"""
Core campaign dispatcher enforcing R1-R9 business rules, priority scheduling,
and resilient error handling.
"""
from __future__ import annotations

import asyncio
import json
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional, Tuple

import httpx

from .config import SystemConfig, IST
from .clock import ProviderClock, in_window, next_open
from .db import Database
from .state_machine import TERMINAL_DISPOSITIONS, apply_call_result, parse_iso_datetime

logger = logging.getLogger("dispatcher")


class CampaignDispatcher:
    def __init__(self, cfg: SystemConfig, db: Database, clock: ProviderClock):
        self.cfg = cfg
        self.db = db
        self.clock = clock

    def is_campaign_open(self, campaign_id: str, now: datetime) -> bool:
        camp = self.cfg.campaigns.get(campaign_id)
        if not camp:
            return False

        # R1: Check regulatory window [09:00, 21:00) IST and campaign window
        # We enforce a safety buffer of simulated seconds to ensure that network latency
        # and clock advancement at high speeds (e.g. 600x - 1500x) never push an in-flight
        # dispatch past the window closing time.
        buffer_seconds = max(180.0, self.clock.speed * 0.4)
        probe_end = now + timedelta(seconds=buffer_seconds)

        rw = self.cfg.regulatory_window
        cw = camp.window

        # Check that now is within windows
        if not in_window(now, rw.start, rw.end, None, grace_seconds=0):
            return False
        if not in_window(now, cw.start, cw.end, cw.days, grace_seconds=0):
            return False

        # Check that probe_end is also within windows (so we don't dial on the verge of closing)
        if not in_window(probe_end, rw.start, rw.end, None, grace_seconds=0):
            return False
        if not in_window(probe_end, cw.start, cw.end, cw.days, grace_seconds=0):
            return False

        return True

    def get_candidate_leads(self, now: datetime) -> List[Dict[str, Any]]:
        """
        Retrieves eligible leads respecting priority:
        1. Due callbacks (callback_at <= now)
        2. Retries (next_eligible_at <= now)
        3. Fresh leads (attempts == 0)

        Filters out campaigns that are currently outside their calling window,
        phones that are currently in active calls, and phones in DND.
        """
        now_iso = now.isoformat()

        # 1. Identify which campaigns are open
        open_campaign_ids = [
            cid for cid in self.cfg.campaigns.keys()
            if self.is_campaign_open(cid, now)
        ]
        if not open_campaign_ids:
            return []

        placeholders = ",".join("?" for _ in open_campaign_ids)

        with self.db.transaction() as conn:
            # 2. Query tenant active calls
            tenant_active = {}
            for t in self.cfg.tenants.values():
                cur = conn.execute(
                    """
                    SELECT count(*) as cnt
                    FROM calls
                    WHERE tenant_id = ? AND state IN ('PLACING', 'ACTIVE')
                    """,
                    (t.tenant_id,),
                )
                tenant_active[t.tenant_id] = cur.fetchone()["cnt"]

            # Filter open campaigns by tenant availability
            eligible_campaign_ids = [
                cid for cid in open_campaign_ids
                if tenant_active[self.cfg.campaigns[cid].tenant_id] < self.cfg.tenants[self.cfg.campaigns[cid].tenant_id].max_concurrent_calls
            ]
            if not eligible_campaign_ids:
                return []

            camp_placeholders = ",".join("?" for _ in eligible_campaign_ids)

            # 3. Query leads with priority ordering:
            # Priority 1: CALLBACK_DUE where callback_at <= now
            # Priority 2: RETRY_DUE where next_eligible_at <= now
            # Priority 3: FRESH
            query = f"""
            SELECT l.campaign_id, l.lead_id, l.phone, l.status, l.attempts,
                   l.callback_at, l.next_eligible_at, c.tenant_id,
                   CASE
                       WHEN l.status = 'CALLBACK_DUE' THEN 1
                       WHEN l.status = 'RETRY_DUE' THEN 2
                       WHEN l.status = 'FRESH' THEN 3
                       ELSE 99
                   END as prio
            FROM leads l
            JOIN campaigns c ON l.campaign_id = c.campaign_id
            WHERE l.campaign_id IN ({camp_placeholders})
              AND l.status IN ('CALLBACK_DUE', 'RETRY_DUE', 'FRESH')
              AND l.attempts < c.max_attempts
              AND (
                  (l.status = 'CALLBACK_DUE' AND (l.callback_at IS NULL OR l.callback_at <= ?))
                  OR (l.status = 'RETRY_DUE' AND (l.next_eligible_at IS NULL OR l.next_eligible_at <= ?))
                  OR (l.status = 'FRESH')
              )
              -- Not in DND
              AND l.phone NOT IN (SELECT phone FROM dnd_registry)
              -- Phone not actively being called
              AND l.phone NOT IN (
                  SELECT phone FROM calls WHERE state IN ('PLACING', 'ACTIVE')
              )
            ORDER BY prio ASC,
                     COALESCE(l.callback_at, l.next_eligible_at, '9999-99-99') ASC,
                     l.lead_id ASC
            """
            params = list(eligible_campaign_ids) + [now_iso, now_iso]
            cur = conn.execute(query, params)
            candidates = [dict(row) for row in cur.fetchall()]

        return candidates

    async def reserve_and_dispatch(
        self,
        candidate: Dict[str, Any],
        client: httpx.AsyncClient,
    ) -> bool:
        """
        Atomically reserves concurrency slot and dials provider API.
        Handles 201, 200, 503, 504, and network errors.
        """
        cid = candidate["campaign_id"]
        lid = candidate["lead_id"]
        phone = candidate["phone"]
        tenant_id = candidate["tenant_id"]
        now = self.clock.now()
        now_iso = now.isoformat()

        # Step 1: Atomic reservation
        attempt_no = candidate["attempts"] + 1
        req_id = f"req_{cid}_{lid}_{attempt_no}"

        with self.db.transaction() as conn:
            # Re-check calling window
            if not self.is_campaign_open(cid, now):
                return False

            # Re-check tenant concurrency limit
            cur = conn.execute(
                "SELECT count(*) as cnt FROM calls WHERE tenant_id = ? AND state IN ('PLACING', 'ACTIVE')",
                (tenant_id,),
            )
            if cur.fetchone()["cnt"] >= self.cfg.tenants[tenant_id].max_concurrent_calls:
                return False

            # Re-check phone concurrency
            cur = conn.execute(
                "SELECT count(*) as cnt FROM calls WHERE phone = ? AND state IN ('PLACING', 'ACTIVE')",
                (phone,),
            )
            if cur.fetchone()["cnt"] > 0:
                return False

            # Re-check DND
            cur = conn.execute("SELECT 1 FROM dnd_registry WHERE phone = ?", (phone,))
            if cur.fetchone() is not None:
                conn.execute(
                    "UPDATE leads SET status = 'DND_BLOCKED', updated_at = ? WHERE campaign_id = ? AND lead_id = ?",
                    (now_iso, cid, lid),
                )
                return False

            # Reserve slot
            conn.execute(
                """
                INSERT INTO calls (
                    request_id, campaign_id, lead_id, tenant_id, phone,
                    attempt_no, state, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, 'PLACING', ?, ?)
                ON CONFLICT(request_id) DO UPDATE SET
                    state = 'PLACING', updated_at = excluded.updated_at
                """,
                (req_id, cid, lid, tenant_id, phone, attempt_no, now_iso, now_iso),
            )
            conn.execute(
                """
                UPDATE leads
                SET status = 'IN_FLIGHT', updated_at = ?
                WHERE campaign_id = ? AND lead_id = ?
                """,
                (now_iso, cid, lid),
            )

        # Step 2: Call provider API
        dial_body = {
            "request_id": req_id,
            "campaign_id": cid,
            "lead_id": lid,
            "phone": phone,
            "callback_url": self.cfg.webhook_url,
        }
        url = f"{self.cfg.provider_url.rstrip('/')}/v1/calls"

        resp = None
        exception_occurred = False
        try:
            resp = await client.post(url, json=dial_body, timeout=8.0)
        except Exception as e:
            logger.warning("Network exception when placing call %s: %s", req_id, e)
            exception_occurred = True

        now_sim = self.clock.now()
        now_sim_iso = now_sim.isoformat()

        # Step 3: Handle response
        if resp is not None and resp.status_code in (201, 200):
            data = resp.json()
            call_sid = data.get("call_sid")

            with self.db.transaction() as conn:
                # Check if call was already ended by a very fast webhook
                call_cur = conn.execute("SELECT state FROM calls WHERE request_id = ?", (req_id,))
                c_state = call_cur.fetchone()
                if c_state and c_state["state"] == "ENDED":
                    # Webhook already ended it, just ensure call_sid and attempt count are updated
                    conn.execute(
                        "UPDATE calls SET call_sid = COALESCE(?, call_sid) WHERE request_id = ?",
                        (call_sid, req_id),
                    )
                    conn.execute(
                        """
                        UPDATE leads
                        SET attempts = ?, last_call_sid = COALESCE(?, last_call_sid), updated_at = ?
                        WHERE campaign_id = ? AND lead_id = ?
                        """,
                        (attempt_no, call_sid, now_sim_iso, cid, lid),
                    )
                else:
                    conn.execute(
                        """
                        UPDATE calls
                        SET call_sid = ?, state = 'ACTIVE', updated_at = ?
                        WHERE request_id = ?
                        """,
                        (call_sid, now_sim_iso, req_id),
                    )
                    conn.execute(
                        """
                        UPDATE leads
                        SET attempts = ?, last_call_sid = ?, updated_at = ?
                        WHERE campaign_id = ? AND lead_id = ?
                        """,
                        (attempt_no, call_sid, now_sim_iso, cid, lid),
                    )
            return True

        elif resp is not None and resp.status_code == 503:
            # 503: Transient, call was NOT placed.
            logger.info("Provider returned 503 for %s; reverting reservation.", req_id)
            with self.db.transaction() as conn:
                conn.execute("DELETE FROM calls WHERE request_id = ? AND state = 'PLACING'", (req_id,))
                conn.execute(
                    """
                    UPDATE leads
                    SET status = CASE WHEN attempts > 0 THEN 'RETRY_DUE' ELSE 'FRESH' END,
                        updated_at = ?
                    WHERE campaign_id = ? AND lead_id = ? AND status = 'IN_FLIGHT'
                    """,
                    (now_sim_iso, cid, lid),
                )
            return False

        elif (resp is not None and resp.status_code == 504) or exception_occurred:
            # 504 or Network Error: Outcome unknown! Immediately reconcile with GET /v1/calls?request_id=...
            logger.info("504 / network timeout for %s; querying provider for status...", req_id)
            try:
                rec_resp = await client.get(url, params={"request_id": req_id}, timeout=5.0)
                if rec_resp.status_code == 200:
                    data = rec_resp.json()
                    call_sid = data.get("call_sid")
                    prov_state = data.get("state")
                    with self.db.transaction() as conn:
                        conn.execute(
                            """
                            UPDATE calls
                            SET call_sid = ?, state = 'ACTIVE', updated_at = ?
                            WHERE request_id = ?
                            """,
                            (call_sid, now_sim_iso, req_id),
                        )
                        conn.execute(
                            """
                            UPDATE leads
                            SET attempts = ?, last_call_sid = ?, updated_at = ?
                            WHERE campaign_id = ? AND lead_id = ?
                            """,
                            (attempt_no, call_sid, now_sim_iso, cid, lid),
                        )
                    if prov_state == "ended" and "result" in data:
                        apply_call_result(self.db, data["result"], self.cfg.retry_policy, now_sim)
                    return True
                elif rec_resp.status_code == 404:
                    # Provider definitely never accepted the call
                    with self.db.transaction() as conn:
                        conn.execute("DELETE FROM calls WHERE request_id = ? AND state = 'PLACING'", (req_id,))
                        conn.execute(
                            """
                            UPDATE leads
                            SET status = CASE WHEN attempts > 0 THEN 'RETRY_DUE' ELSE 'FRESH' END,
                                updated_at = ?
                            WHERE campaign_id = ? AND lead_id = ? AND status = 'IN_FLIGHT'
                            """,
                            (now_sim_iso, cid, lid),
                        )
                    return False
            except Exception as e:
                logger.warning("Reconciliation query failed for %s: %s; watchdog will handle", req_id, e)
                return False

        else:
            # Unhandled status code (e.g. 4xx)
            logger.error("Provider returned unexpected status %s: %s", resp.status_code if resp else "None", resp.text if resp else "")
            with self.db.transaction() as conn:
                conn.execute("DELETE FROM calls WHERE request_id = ? AND state = 'PLACING'", (req_id,))
                conn.execute(
                    """
                    UPDATE leads
                    SET status = CASE WHEN attempts > 0 THEN 'RETRY_DUE' ELSE 'FRESH' END,
                        updated_at = ?
                    WHERE campaign_id = ? AND lead_id = ? AND status = 'IN_FLIGHT'
                    """,
                    (now_sim_iso, cid, lid),
                )
            return False

    def is_finished(self) -> bool:
        """
        Returns True when every single lead has reached a final state
        (COMPLETED, EXHAUSTED, or DND_BLOCKED) and no calls are active/placing.
        """
        with self.db.transaction() as conn:
            cur = conn.execute(
                """
                SELECT count(*) as cnt
                FROM leads
                WHERE status NOT IN ('COMPLETED', 'EXHAUSTED', 'DND_BLOCKED')
                """
            )
            incomplete_leads = cur.fetchone()["cnt"]

            cur = conn.execute(
                "SELECT count(*) as cnt FROM calls WHERE state IN ('PLACING', 'ACTIVE')"
            )
            active_calls = cur.fetchone()["cnt"]

            return incomplete_leads == 0 and active_calls == 0

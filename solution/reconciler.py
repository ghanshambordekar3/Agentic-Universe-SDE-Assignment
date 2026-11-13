"""
Reconciler and watchdog for crash recovery, lost webhooks, and state consistency.
"""
from __future__ import annotations

import logging
from datetime import datetime
from typing import Optional

import httpx

from .config import SystemConfig
from .clock import ProviderClock
from .db import Database
from .state_machine import apply_call_result

logger = logging.getLogger("reconciler")


class Reconciler:
    def __init__(self, cfg: SystemConfig, db: Database, clock: ProviderClock):
        self.cfg = cfg
        self.db = db
        self.clock = clock

    async def recover_on_boot(self, client: httpx.AsyncClient):
        """
        Called once on dispatcher startup to inspect and resolve any calls left in
        PLACING or ACTIVE states (e.g. after a process crash or kill -9).
        """
        logger.info("Running crash recovery on startup...")
        now = self.clock.now()

        with self.db.transaction() as conn:
            # Sync any missed DND blocks first
            conn.execute(
                """
                UPDATE leads
                SET status = 'DND_BLOCKED', updated_at = ?
                WHERE phone IN (SELECT phone FROM dnd_registry)
                  AND status IN ('FRESH', 'RETRY_DUE', 'CALLBACK_DUE')
                """,
                (now.isoformat(),),
            )

            cur = conn.execute(
                """
                SELECT request_id, call_sid, campaign_id, lead_id, state, attempt_no
                FROM calls
                WHERE state IN ('PLACING', 'ACTIVE')
                """
            )
            pending_calls = [dict(row) for row in cur.fetchall()]

        for call in pending_calls:
            req_id = call["request_id"]
            call_sid = call.get("call_sid")
            state = call["state"]

            # Query provider for call status
            try:
                url = f"{self.cfg.provider_url.rstrip('/')}/v1/calls"
                resp = await client.get(url, params={"request_id": req_id})
                if resp.status_code == 200:
                    data = resp.json()
                    prov_state = data.get("state")
                    prov_sid = data.get("call_sid")
                    if prov_state == "ended" and "result" in data:
                        apply_call_result(self.db, data["result"], self.cfg.retry_policy, self.clock.now())
                    elif prov_state == "active":
                        with self.db.transaction() as conn:
                            conn.execute(
                                "UPDATE calls SET call_sid = ?, state = 'ACTIVE' WHERE request_id = ?",
                                (prov_sid, req_id),
                            )
                elif resp.status_code == 404:
                    # Provider never saw this call or call was not placed
                    if state == "PLACING":
                        logger.info("Reverting unplaced call %s for lead %s", req_id, call["lead_id"])
                        with self.db.transaction() as conn:
                            conn.execute("DELETE FROM calls WHERE request_id = ?", (req_id,))
                            # Restore lead status
                            conn.execute(
                                """
                                UPDATE leads
                                SET status = CASE WHEN attempts > 0 THEN 'RETRY_DUE' ELSE 'FRESH' END,
                                    updated_at = ?
                                WHERE campaign_id = ? AND lead_id = ? AND status = 'IN_FLIGHT'
                                """,
                                (self.clock.now().isoformat(), call["campaign_id"], call["lead_id"]),
                            )
            except Exception as e:
                logger.warning("Could not reconcile call %s on boot: %s", req_id, e)

    async def reconcile_active_calls(self, client: httpx.AsyncClient) -> int:
        """
        Polls provider for any ACTIVE calls to recover from lost webhooks.
        Returns the number of resolved calls.
        """
        with self.db.transaction() as conn:
            cur = conn.execute(
                """
                SELECT request_id, call_sid, campaign_id, lead_id, state
                FROM calls
                WHERE state IN ('ACTIVE', 'PLACING')
                """
            )
            active_calls = [dict(row) for row in cur.fetchall()]

        resolved = 0
        for call in active_calls:
            req_id = call["request_id"]
            call_sid = call.get("call_sid")
            try:
                if call_sid:
                    url = f"{self.cfg.provider_url.rstrip('/')}/v1/calls/{call_sid}"
                    resp = await client.get(url)
                else:
                    url = f"{self.cfg.provider_url.rstrip('/')}/v1/calls"
                    resp = await client.get(url, params={"request_id": req_id})

                if resp.status_code == 200:
                    data = resp.json()
                    if data.get("state") == "ended" and "result" in data:
                        logger.info("Watchdog recovered ended call %s (webhook lost)", call_sid or req_id)
                        apply_call_result(self.db, data["result"], self.cfg.retry_policy, self.clock.now())
                        resolved += 1
                elif resp.status_code == 404 and call["state"] == "PLACING":
                    # Was placing, but provider has no record
                    with self.db.transaction() as conn:
                        conn.execute("DELETE FROM calls WHERE request_id = ?", (req_id,))
                        conn.execute(
                            """
                            UPDATE leads
                            SET status = CASE WHEN attempts > 0 THEN 'RETRY_DUE' ELSE 'FRESH' END,
                                updated_at = ?
                            WHERE campaign_id = ? AND lead_id = ? AND status = 'IN_FLIGHT'
                            """,
                            (self.clock.now().isoformat(), call["campaign_id"], call["lead_id"]),
                        )
                        resolved += 1
            except Exception as e:
                logger.debug("Reconcile check failed for %s: %s", req_id, e)

        return resolved

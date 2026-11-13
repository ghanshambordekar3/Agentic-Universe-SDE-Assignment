"""
Lead state machine, rules enforcement, and disposition processing.
"""
from __future__ import annotations

import json
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, Tuple

from .config import IST, RetryPolicyConfig
from .db import Database

TERMINAL_DISPOSITIONS = {"interested", "not_interested", "wrong_number", "dnd"}


def parse_iso_datetime(dt_str: Optional[str]) -> Optional[datetime]:
    if not dt_str:
        return None
    try:
        dt = datetime.fromisoformat(dt_str)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=IST)
        return dt.astimezone(IST)
    except Exception:
        return None


def register_global_dnd(
    conn,
    phone: str,
    sim_time_iso: str,
    source_campaign_id: str,
    source_lead_id: str,
):
    """
    Registers a phone number in the global DND registry and immediately
    marks any other non-terminal, non-in-flight leads for that phone as DND_BLOCKED.
    """
    conn.execute(
        """
        INSERT OR IGNORE INTO dnd_registry (phone, registered_at, source_campaign_id, source_lead_id)
        VALUES (?, ?, ?, ?)
        """,
        (phone, sim_time_iso, source_campaign_id, source_lead_id),
    )

    # Any other lead with this phone that is FRESH, RETRY_DUE, or CALLBACK_DUE becomes DND_BLOCKED
    conn.execute(
        """
        UPDATE leads
        SET status = 'DND_BLOCKED', updated_at = ?
        WHERE phone = ?
          AND status IN ('FRESH', 'RETRY_DUE', 'CALLBACK_DUE')
          AND NOT (campaign_id = ? AND lead_id = ?)
        """,
        (sim_time_iso, phone, source_campaign_id, source_lead_id),
    )


def apply_call_result(
    db: Database,
    result: Dict[str, Any],
    retry_policy: RetryPolicyConfig,
    now: datetime,
) -> bool:
    """
    Atomically updates a call and its corresponding lead based on a call outcome
    (either received from a webhook or fetched via provider reconciliation).
    Returns True if state was updated, False if already ended (idempotent).
    """
    req_id = result.get("request_id")
    call_sid = result.get("call_sid")
    status = result.get("status")  # completed | no_answer | busy | failed
    disposition = result.get("disposition")
    callback_at_str = result.get("callback_at")
    ended_at_str = result.get("ended_at") or now.isoformat()
    duration_sec = int(result.get("duration_sec", 0))

    ended_at_dt = parse_iso_datetime(ended_at_str) or now
    now_iso = now.isoformat()

    with db.transaction() as conn:
        # Locate call by request_id or call_sid
        cursor = conn.execute(
            """
            SELECT request_id, call_sid, campaign_id, lead_id, tenant_id, phone,
                   attempt_no, state
            FROM calls
            WHERE request_id = ? OR call_sid = ?
            """,
            (req_id, call_sid),
        )
        call_row = cursor.fetchone()
        if not call_row:
            # If call record does not exist yet (e.g., webhook arrived before 201 response was written)
            cid = result.get("campaign_id")
            lid = result.get("lead_id")
            tenant_id = result.get("tenant_id")
            phone = result.get("phone")
            attempt_no = 1

            # Check lead attempts
            lead_cur = conn.execute(
                "SELECT attempts FROM leads WHERE campaign_id = ? AND lead_id = ?",
                (cid, lid),
            )
            lead_row = lead_cur.fetchone()
            if lead_row:
                attempt_no = lead_row["attempts"] + 1

            conn.execute(
                """
                INSERT INTO calls (
                    request_id, call_sid, campaign_id, lead_id, tenant_id, phone,
                    attempt_no, state, status, disposition, callback_at,
                    started_at, ended_at, duration_sec, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, 'ENDED', ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    req_id,
                    call_sid,
                    cid,
                    lid,
                    tenant_id,
                    phone,
                    attempt_no,
                    status,
                    disposition,
                    callback_at_str,
                    result.get("started_at") or now_iso,
                    ended_at_str,
                    duration_sec,
                    now_iso,
                    now_iso,
                ),
            )
            campaign_id = cid
            lead_id = lid
        else:
            if call_row["state"] == "ENDED":
                # Already processed
                return False

            campaign_id = call_row["campaign_id"]
            lead_id = call_row["lead_id"]
            phone = call_row["phone"]

            conn.execute(
                """
                UPDATE calls
                SET call_sid = COALESCE(?, call_sid),
                    state = 'ENDED',
                    status = ?,
                    disposition = ?,
                    callback_at = ?,
                    ended_at = ?,
                    duration_sec = ?,
                    updated_at = ?
                WHERE request_id = ?
                """,
                (
                    call_sid,
                    status,
                    disposition,
                    callback_at_str,
                    ended_at_str,
                    duration_sec,
                    now_iso,
                    call_row["request_id"],
                ),
            )

        # Now update the lead record
        lead_cur = conn.execute(
            """
            SELECT l.campaign_id, l.lead_id, l.phone, l.status, l.attempts, c.max_attempts
            FROM leads l
            JOIN campaigns c ON l.campaign_id = c.campaign_id
            WHERE l.campaign_id = ? AND l.lead_id = ?
            """,
            (campaign_id, lead_id),
        )
        lead = lead_cur.fetchone()
        if not lead:
            return True

        phone = lead["phone"]
        max_attempts = lead["max_attempts"]
        attempts = lead["attempts"]

        # Check if phone is in DND
        dnd_cur = conn.execute(
            "SELECT phone FROM dnd_registry WHERE phone = ?",
            (phone,),
        )
        is_phone_dnd = dnd_cur.fetchone() is not None

        # Determine next lead status
        next_status: str
        next_eligible_at: Optional[str] = None
        callback_at_val: Optional[str] = None

        if disposition in TERMINAL_DISPOSITIONS:
            # Rule R7 & R9: completed takes precedence
            next_status = "COMPLETED"
            if disposition == "dnd":
                # Rule R8: Global DND
                register_global_dnd(conn, phone, now_iso, campaign_id, lead_id)
        elif attempts >= max_attempts:
            # Rule R9: exhausted
            next_status = "EXHAUSTED"
        elif is_phone_dnd:
            # Rule R8 & R9: dnd_blocked
            next_status = "DND_BLOCKED"
        elif status == "completed" and disposition == "callback_requested":
            # Rule R6: callback requested
            next_status = "CALLBACK_DUE"
            callback_at_val = callback_at_str
            next_eligible_at = callback_at_str
        elif status in ("no_answer", "busy"):
            # Rule R5: retry after 60 min
            next_status = "RETRY_DUE"
            mins = retry_policy.no_answer_minutes if status == "no_answer" else retry_policy.busy_minutes
            next_eligible_at = (ended_at_dt + timedelta(minutes=mins)).isoformat()
        elif status == "failed":
            # Rule R5: retry after 15 min
            next_status = "RETRY_DUE"
            next_eligible_at = (ended_at_dt + timedelta(minutes=retry_policy.failed_minutes)).isoformat()
        else:
            # Fallback retry
            next_status = "RETRY_DUE"
            next_eligible_at = (ended_at_dt + timedelta(minutes=retry_policy.no_answer_minutes)).isoformat()

        conn.execute(
            """
            UPDATE leads
            SET status = ?,
                last_status = ?,
                last_disposition = ?,
                last_call_sid = COALESCE(?, last_call_sid),
                next_eligible_at = ?,
                callback_at = ?,
                updated_at = ?
            WHERE campaign_id = ? AND lead_id = ?
            """,
            (
                next_status,
                status,
                disposition,
                call_sid,
                next_eligible_at,
                callback_at_val,
                now_iso,
                campaign_id,
                lead_id,
            ),
        )

        return True

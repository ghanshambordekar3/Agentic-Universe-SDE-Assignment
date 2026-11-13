"""
Unit tests for the state machine, lead lifecycle, and DND handling.
"""
import os
import tempfile
from datetime import datetime, timedelta
import pytest

from solution.config import RetryPolicyConfig, IST
from solution.db import Database
from solution.state_machine import apply_call_result, register_global_dnd


@pytest.fixture
def test_db():
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    db = Database(path)
    # Seed minimal tenant, campaign, leads
    with db.transaction() as conn:
        conn.execute("INSERT INTO tenants VALUES ('tenant_1', 2)")
        conn.execute(
            "INSERT INTO campaigns VALUES ('camp_1', 'tenant_1', '10:00', '19:00', '[\"mon\", \"tue\", \"wed\", \"thu\", \"fri\"]', 3)"
        )
        conn.execute(
            "INSERT INTO campaigns VALUES ('camp_2', 'tenant_1', '10:00', '19:00', '[\"mon\", \"tue\", \"wed\", \"thu\", \"fri\"]', 3)"
        )
        # Lead 1 on camp 1
        conn.execute(
            "INSERT INTO leads (campaign_id, lead_id, phone, status, attempts) VALUES ('camp_1', 'l1', '+919999999999', 'FRESH', 0)"
        )
        # Lead 2 on camp 2 with SAME phone
        conn.execute(
            "INSERT INTO leads (campaign_id, lead_id, phone, status, attempts) VALUES ('camp_2', 'l2', '+919999999999', 'FRESH', 0)"
        )
    yield db
    if os.path.exists(path):
        os.remove(path)


def test_terminal_disposition_completed(test_db):
    rp = RetryPolicyConfig()
    now = datetime(2026, 11, 12, 12, 0, 0, tzinfo=IST)

    result = {
        "event": "call.completed",
        "call_sid": "CA12345",
        "request_id": "req_1",
        "tenant_id": "tenant_1",
        "campaign_id": "camp_1",
        "lead_id": "l1",
        "phone": "+919999999999",
        "status": "completed",
        "disposition": "interested",
        "ended_at": now.isoformat(),
        "duration_sec": 120,
    }

    apply_call_result(test_db, result, rp, now)

    with test_db.transaction() as conn:
        lead = conn.execute("SELECT status, last_disposition FROM leads WHERE lead_id = 'l1'").fetchone()
        assert lead["status"] == "COMPLETED"
        assert lead["last_disposition"] == "interested"


def test_retry_intervals_busy_and_failed(test_db):
    rp = RetryPolicyConfig(busy_minutes=60, failed_minutes=15)
    now = datetime(2026, 11, 12, 12, 0, 0, tzinfo=IST)

    # Busy outcome
    result_busy = {
        "event": "call.completed",
        "call_sid": "CA_busy",
        "request_id": "req_busy",
        "tenant_id": "tenant_1",
        "campaign_id": "camp_1",
        "lead_id": "l1",
        "phone": "+919999999999",
        "status": "busy",
        "disposition": None,
        "ended_at": now.isoformat(),
    }
    apply_call_result(test_db, result_busy, rp, now)

    with test_db.transaction() as conn:
        lead = conn.execute("SELECT status, next_eligible_at FROM leads WHERE lead_id = 'l1'").fetchone()
        assert lead["status"] == "RETRY_DUE"
        expected_elig = (now + timedelta(minutes=60)).isoformat()
        assert lead["next_eligible_at"] == expected_elig

    # Failed outcome
    result_failed = {
        "event": "call.completed",
        "call_sid": "CA_fail",
        "request_id": "req_fail",
        "tenant_id": "tenant_1",
        "campaign_id": "camp_1",
        "lead_id": "l1",
        "phone": "+919999999999",
        "status": "failed",
        "disposition": None,
        "ended_at": now.isoformat(),
    }
    apply_call_result(test_db, result_failed, rp, now)

    with test_db.transaction() as conn:
        lead = conn.execute("SELECT status, next_eligible_at FROM leads WHERE lead_id = 'l1'").fetchone()
        assert lead["status"] == "RETRY_DUE"
        expected_elig = (now + timedelta(minutes=15)).isoformat()
        assert lead["next_eligible_at"] == expected_elig


def test_callback_requested(test_db):
    rp = RetryPolicyConfig()
    now = datetime(2026, 11, 12, 12, 0, 0, tzinfo=IST)
    cb_time = (now + timedelta(hours=2)).isoformat()

    result = {
        "event": "call.completed",
        "call_sid": "CA_cb",
        "request_id": "req_cb",
        "tenant_id": "tenant_1",
        "campaign_id": "camp_1",
        "lead_id": "l1",
        "phone": "+919999999999",
        "status": "completed",
        "disposition": "callback_requested",
        "callback_at": cb_time,
        "ended_at": now.isoformat(),
    }
    apply_call_result(test_db, result, rp, now)

    with test_db.transaction() as conn:
        lead = conn.execute("SELECT status, callback_at FROM leads WHERE lead_id = 'l1'").fetchone()
        assert lead["status"] == "CALLBACK_DUE"
        assert lead["callback_at"] == cb_time


def test_max_attempts_exhausted(test_db):
    rp = RetryPolicyConfig()
    now = datetime(2026, 11, 12, 12, 0, 0, tzinfo=IST)

    # Set lead to 3 attempts
    with test_db.transaction() as conn:
        conn.execute("UPDATE leads SET attempts = 3 WHERE lead_id = 'l1'")

    result = {
        "event": "call.completed",
        "call_sid": "CA_last",
        "request_id": "req_last",
        "tenant_id": "tenant_1",
        "campaign_id": "camp_1",
        "lead_id": "l1",
        "phone": "+919999999999",
        "status": "no_answer",
        "disposition": None,
        "ended_at": now.isoformat(),
    }
    apply_call_result(test_db, result, rp, now)

    with test_db.transaction() as conn:
        lead = conn.execute("SELECT status FROM leads WHERE lead_id = 'l1'").fetchone()
        assert lead["status"] == "EXHAUSTED"


def test_global_dnd_propagation(test_db):
    """
    When Lead 1 gets DND:
    - Lead 1's final status is COMPLETED (disposition dnd is terminal).
    - Lead 2 (different campaign, same phone) is marked DND_BLOCKED.
    """
    rp = RetryPolicyConfig()
    now = datetime(2026, 11, 12, 12, 0, 0, tzinfo=IST)

    result_dnd = {
        "event": "call.completed",
        "call_sid": "CA_dnd",
        "request_id": "req_dnd",
        "tenant_id": "tenant_1",
        "campaign_id": "camp_1",
        "lead_id": "l1",
        "phone": "+919999999999",
        "status": "completed",
        "disposition": "dnd",
        "ended_at": now.isoformat(),
    }
    apply_call_result(test_db, result_dnd, rp, now)

    with test_db.transaction() as conn:
        l1 = conn.execute("SELECT status, last_disposition FROM leads WHERE lead_id = 'l1'").fetchone()
        l2 = conn.execute("SELECT status FROM leads WHERE lead_id = 'l2'").fetchone()

        assert l1["status"] == "COMPLETED"
        assert l1["last_disposition"] == "dnd"
        assert l2["status"] == "DND_BLOCKED"

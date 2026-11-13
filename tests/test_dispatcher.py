"""
Unit tests for the Campaign Dispatcher scheduling, constraints, and priorities.
"""
import os
import tempfile
from datetime import datetime, timedelta
import pytest
import httpx

from solution.config import SystemConfig, CallingWindowConfig, RetryPolicyConfig, TenantConfig, CampaignConfig, IST
from solution.clock import ProviderClock
from solution.db import Database
from solution.dispatcher import CampaignDispatcher


@pytest.fixture
def dispatcher_env():
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    db = Database(path)

    # Seed 1 tenant with max_concurrent_calls = 2
    # 1 campaign open Mon-Fri 10:00 - 19:00
    with db.transaction() as conn:
        conn.execute("INSERT INTO tenants VALUES ('tenant_1', 2)")
        conn.execute(
            "INSERT INTO campaigns VALUES ('camp_1', 'tenant_1', '10:00', '19:00', '[\"mon\", \"tue\", \"wed\", \"thu\", \"fri\"]', 3)"
        )

    cfg = SystemConfig(
        sim_start=datetime(2026, 11, 12, 11, 0, tzinfo=IST),  # Thursday 11:00 AM (inside window)
        timezone_name="Asia/Kolkata",
        regulatory_window=CallingWindowConfig(start="09:00", end="21:00"),
        window_grace_seconds=120,
        retry_policy=RetryPolicyConfig(),
        webhook_secret="test_secret",
        tenants={"tenant_1": TenantConfig("tenant_1", 2)},
        campaigns={"camp_1": CampaignConfig("camp_1", "tenant_1", "leads.csv", CallingWindowConfig("10:00", "19:00", ["mon", "tue", "wed", "thu", "fri"]), 3)},
        fixtures_dir="",
        db_path=path,
    )
    clock = ProviderClock(sim_epoch=cfg.sim_start, real_epoch=1000.0, speed=1.0)
    dispatcher = CampaignDispatcher(cfg, db, clock)

    yield dispatcher, db, cfg, clock

    if os.path.exists(path):
        os.remove(path)


def test_priority_ordering_callbacks_over_retries_over_fresh(dispatcher_env):
    dispatcher, db, cfg, clock = dispatcher_env
    now = clock.now()
    past = (now - timedelta(minutes=10)).isoformat()

    with db.transaction() as conn:
        # Fresh lead
        conn.execute("INSERT INTO leads (campaign_id, lead_id, phone, status, attempts) VALUES ('camp_1', 'fresh_1', '+919111111111', 'FRESH', 0)")
        # Retry lead due in the past
        conn.execute("INSERT INTO leads (campaign_id, lead_id, phone, status, attempts, next_eligible_at) VALUES ('camp_1', 'retry_1', '+919222222222', 'RETRY_DUE', 1, ?)", (past,))
        # Callback lead due in the past
        conn.execute("INSERT INTO leads (campaign_id, lead_id, phone, status, attempts, callback_at) VALUES ('camp_1', 'callback_1', '+919333333333', 'CALLBACK_DUE', 1, ?)", (past,))

    candidates = dispatcher.get_candidate_leads(now)
    assert len(candidates) == 3
    # First must be callback, second retry, third fresh
    assert candidates[0]["lead_id"] == "callback_1"
    assert candidates[1]["lead_id"] == "retry_1"
    assert candidates[2]["lead_id"] == "fresh_1"


def test_concurrency_tenant_limit(dispatcher_env):
    dispatcher, db, cfg, clock = dispatcher_env
    now = clock.now()

    with db.transaction() as conn:
        conn.execute("INSERT INTO leads (campaign_id, lead_id, phone, status, attempts) VALUES ('camp_1', 'l1', '+919111111111', 'FRESH', 0)")
        conn.execute("INSERT INTO leads (campaign_id, lead_id, phone, status, attempts) VALUES ('camp_1', 'l2', '+919222222222', 'FRESH', 0)")
        # Occupy both slots for tenant_1 (limit = 2)
        conn.execute("INSERT INTO calls (request_id, campaign_id, lead_id, tenant_id, phone, attempt_no, state, created_at, updated_at) VALUES ('req_occ_1', 'camp_1', 'l_occ1', 'tenant_1', '+919000000001', 1, 'ACTIVE', 'now', 'now')")
        conn.execute("INSERT INTO calls (request_id, campaign_id, lead_id, tenant_id, phone, attempt_no, state, created_at, updated_at) VALUES ('req_occ_2', 'camp_1', 'l_occ2', 'tenant_1', '+919000000002', 1, 'ACTIVE', 'now', 'now')")

    candidates = dispatcher.get_candidate_leads(now)
    # Tenant concurrency reached, so no candidates eligible right now
    assert len(candidates) == 0


def test_one_call_per_phone(dispatcher_env):
    dispatcher, db, cfg, clock = dispatcher_env
    now = clock.now()

    with db.transaction() as conn:
        # Lead with phone that is currently in an active call
        conn.execute("INSERT INTO leads (campaign_id, lead_id, phone, status, attempts) VALUES ('camp_1', 'l1', '+919111111111', 'FRESH', 0)")
        conn.execute("INSERT INTO calls (request_id, campaign_id, lead_id, tenant_id, phone, attempt_no, state, created_at, updated_at) VALUES ('req_occ_1', 'camp_1', 'l_other', 'tenant_1', '+919111111111', 1, 'ACTIVE', 'now', 'now')")

    candidates = dispatcher.get_candidate_leads(now)
    # Phone is in active call, so excluded
    assert len(candidates) == 0

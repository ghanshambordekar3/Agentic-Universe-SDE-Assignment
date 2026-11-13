"""
Unit tests for the webhook receiver.
"""
import hashlib
import hmac
import json
import os
import tempfile
from datetime import datetime
import pytest
from fastapi.testclient import TestClient

from solution.config import SystemConfig, CallingWindowConfig, RetryPolicyConfig, TenantConfig, CampaignConfig, IST
from solution.clock import ProviderClock
from solution.db import Database
from solution.webhook_server import create_webhook_app


@pytest.fixture
def webhook_env():
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    db = Database(path)

    # Seed basic data
    with db.transaction() as conn:
        conn.execute("INSERT INTO tenants VALUES ('tenant_1', 2)")
        conn.execute(
            "INSERT INTO campaigns VALUES ('camp_1', 'tenant_1', '10:00', '19:00', '[\"mon\"]', 3)"
        )
        conn.execute(
            "INSERT INTO leads (campaign_id, lead_id, phone, status, attempts) VALUES ('camp_1', 'l1', '+919876543210', 'IN_FLIGHT', 1)"
        )

    cfg = SystemConfig(
        sim_start=datetime(2026, 11, 12, 17, 30, tzinfo=IST),
        timezone_name="Asia/Kolkata",
        regulatory_window=CallingWindowConfig(start="09:00", end="21:00"),
        window_grace_seconds=120,
        retry_policy=RetryPolicyConfig(),
        webhook_secret="test_secret_key",
        tenants={"tenant_1": TenantConfig("tenant_1", 2)},
        campaigns={"camp_1": CampaignConfig("camp_1", "tenant_1", "leads.csv", CallingWindowConfig("10:00", "19:00"), 3)},
        fixtures_dir="",
        db_path=path,
    )
    clock = ProviderClock(sim_epoch=cfg.sim_start, real_epoch=1000.0, speed=1.0)
    app = create_webhook_app(cfg, db, clock)
    client = TestClient(app)

    yield client, db, cfg

    if os.path.exists(path):
        os.remove(path)


def test_webhook_missing_signature(webhook_env):
    client, _, _ = webhook_env
    resp = client.post("/webhooks/telephony", json={"event": "call.completed"})
    assert resp.status_code == 401


def test_webhook_invalid_signature(webhook_env):
    client, _, _ = webhook_env
    body = json.dumps({"event": "call.completed"}).encode()
    resp = client.post(
        "/webhooks/telephony",
        content=body,
        headers={"X-AU-Signature": "invalid_sig", "Content-Type": "application/json"},
    )
    assert resp.status_code == 403


def test_webhook_valid_signature_and_deduplication(webhook_env):
    client, db, cfg = webhook_env

    payload = {
        "event": "call.completed",
        "call_sid": "CA99999",
        "request_id": "req_test_1",
        "tenant_id": "tenant_1",
        "campaign_id": "camp_1",
        "lead_id": "l1",
        "phone": "+919876543210",
        "status": "completed",
        "disposition": "interested",
        "duration_sec": 45,
    }
    body = json.dumps(payload).encode()
    sig = hmac.new(cfg.webhook_secret.encode(), body, hashlib.sha256).hexdigest()

    # First delivery
    resp1 = client.post(
        "/webhooks/telephony",
        content=body,
        headers={
            "X-AU-Signature": sig,
            "X-AU-Delivery-Id": "del_001",
            "Content-Type": "application/json",
        },
    )
    assert resp1.status_code == 200
    assert resp1.json()["status"] == "accepted"

    # Verify lead updated to COMPLETED
    with db.transaction() as conn:
        lead = conn.execute("SELECT status FROM leads WHERE lead_id = 'l1'").fetchone()
        assert lead["status"] == "COMPLETED"

    # Second delivery (duplicate)
    resp2 = client.post(
        "/webhooks/telephony",
        content=body,
        headers={
            "X-AU-Signature": sig,
            "X-AU-Delivery-Id": "del_001",
            "Content-Type": "application/json",
        },
    )
    assert resp2.status_code == 200
    assert resp2.json()["status"] == "duplicate_delivery_ignored"

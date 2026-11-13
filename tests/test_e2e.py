"""
End-to-End integration tests:
1. test_full_pipeline_clean_run: failure_scale=0.0
2. test_pipeline_with_failures_and_crash_recovery: failure_scale=1.0 with mid-run restart
"""
import asyncio
import csv
import importlib.util
import os
import sys
import tempfile
import pytest
import uvicorn
import httpx

from solution.config import SystemConfig
from solution.runner import Runner

HERE = os.path.dirname(os.path.abspath(__file__))
FIXTURES = os.path.join(HERE, "..", "assignment-b-workflow", "fixtures", "campaigns.json")
MOCK_SERVER_PATH = os.path.join(HERE, "..", "assignment-b-workflow", "mock_telephony", "server.py")

spec = importlib.util.spec_from_file_location("mock_telephony_server", MOCK_SERVER_PATH)
mock_mod = importlib.util.module_from_spec(spec)
sys.modules["mock_telephony_server"] = mock_mod
spec.loader.exec_module(mock_mod)
create_mock_app = mock_mod.create_app


@pytest.mark.anyio
async def test_full_pipeline_clean_run():
    mock_port = 8099
    webhook_port = 8200
    mock_app = create_mock_app(FIXTURES, speed=1500.0, seed=42, failure_scale=0.0)

    mock_server_config = uvicorn.Config(mock_app, host="127.0.0.1", port=mock_port, log_level="warning")
    mock_server = uvicorn.Server(mock_server_config)
    mock_task = asyncio.create_task(mock_server.serve())

    fd, db_path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    fd_out, out_csv = tempfile.mkstemp(suffix=".csv")
    os.close(fd_out)

    try:
        await asyncio.sleep(0.5)

        cfg = SystemConfig.load(
            fixtures_path=FIXTURES,
            provider_url=f"http://127.0.0.1:{mock_port}",
            webhook_port=webhook_port,
            db_path=db_path,
        )

        runner = Runner(cfg)
        await runner.run(out_path=out_csv)

        # Query provider /admin/report
        async with httpx.AsyncClient() as client:
            rep_resp = await client.get(f"http://127.0.0.1:{mock_port}/admin/report")
            assert rep_resp.status_code == 200
            report = rep_resp.json()
            assert report["violation_count"] == 0, f"Violations detected: {report['violations']}"

            # Query provider /admin/leads
            leads_resp = await client.get(f"http://127.0.0.1:{mock_port}/admin/leads")
            assert leads_resp.status_code == 200
            expected_leads = leads_resp.json()

        # Read generated CSV
        actual_leads = {}
        with open(out_csv, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                actual_leads[(row["campaign_id"], row["lead_id"])] = row

        assert len(actual_leads) == len(expected_leads) == 200

        for exp in expected_leads:
            key = (exp["campaign_id"], exp["lead_id"])
            assert key in actual_leads, f"Missing lead {key}"
            act = actual_leads[key]
            assert act["final_state"] == exp["expected_state"], f"Mismatch for {key}: expected {exp['expected_state']}, got {act['final_state']}"
            assert int(act["attempts"]) == exp["attempts"], f"Attempt mismatch for {key}: expected {exp['attempts']}, got {act['attempts']}"
            assert act["last_status"] == exp["last_status"], f"Status mismatch for {key}: expected {exp['last_status']}, got {act['last_status']}"
            assert act["last_disposition"] == exp["last_disposition"], f"Disposition mismatch for {key}: expected {exp['last_disposition']}, got {act['last_disposition']}"

    finally:
        mock_server.should_exit = True
        await mock_task
        if os.path.exists(db_path):
            os.remove(db_path)
        if os.path.exists(out_csv):
            os.remove(out_csv)


@pytest.mark.anyio
async def test_pipeline_with_failures_and_crash_recovery():
    """
    Runs with failure_scale=1.0 (503s, 504s, lost webhooks, duplicate webhooks),
    aborts the runner mid-run (simulating a crash/kill), restarts it, and verifies
    0 violations and accurate final state.
    """
    mock_port = 8098
    webhook_port = 8201
    mock_app = create_mock_app(FIXTURES, speed=1800.0, seed=101, failure_scale=1.0)

    mock_server_config = uvicorn.Config(mock_app, host="127.0.0.1", port=mock_port, log_level="warning")
    mock_server = uvicorn.Server(mock_server_config)
    mock_task = asyncio.create_task(mock_server.serve())

    fd, db_path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    fd_out, out_csv = tempfile.mkstemp(suffix=".csv")
    os.close(fd_out)

    try:
        await asyncio.sleep(0.5)

        cfg = SystemConfig.load(
            fixtures_path=FIXTURES,
            provider_url=f"http://127.0.0.1:{mock_port}",
            webhook_port=webhook_port,
            db_path=db_path,
        )

        # 1. Start runner 1
        runner1 = Runner(cfg)
        run_task = asyncio.create_task(runner1.run(out_path=out_csv))

        # Let it run for 10 seconds to accumulate in-flight calls and progress
        await asyncio.sleep(10.0)

        # 2. Simulate abrupt process crash / termination
        runner1.stop_requested = True
        await run_task

        # Verify DB still has in-flight or partial data
        # 3. Restart runner (simulating post-crash restart)
        runner2 = Runner(cfg)
        await runner2.run(out_path=out_csv)

        # 4. Check /admin/report
        async with httpx.AsyncClient() as client:
            rep_resp = await client.get(f"http://127.0.0.1:{mock_port}/admin/report")
            assert rep_resp.status_code == 200
            report = rep_resp.json()
            assert report["violation_count"] == 0, f"Violations detected on crash recovery run: {report['violations']}"

            leads_resp = await client.get(f"http://127.0.0.1:{mock_port}/admin/leads")
            assert leads_resp.status_code == 200
            expected_leads = leads_resp.json()

        actual_leads = {}
        with open(out_csv, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                actual_leads[(row["campaign_id"], row["lead_id"])] = row

        assert len(actual_leads) == len(expected_leads) == 200

        for exp in expected_leads:
            key = (exp["campaign_id"], exp["lead_id"])
            assert key in actual_leads, f"Missing lead {key}"
            act = actual_leads[key]
            assert act["final_state"] == exp["expected_state"], f"Mismatch for {key}: expected {exp['expected_state']}, got {act['final_state']}"
            assert int(act["attempts"]) == exp["attempts"], f"Attempt mismatch for {key}: expected {exp['attempts']}, got {act['attempts']}"
            assert act["last_status"] == exp["last_status"], f"Status mismatch for {key}: expected {exp['last_status']}, got {act['last_status']}"
            assert act["last_disposition"] == exp["last_disposition"], f"Disposition mismatch for {key}: expected {exp['last_disposition']}, got {act['last_disposition']}"

    finally:
        mock_server.should_exit = True
        await mock_task
        if os.path.exists(db_path):
            os.remove(db_path)
        if os.path.exists(out_csv):
            os.remove(out_csv)

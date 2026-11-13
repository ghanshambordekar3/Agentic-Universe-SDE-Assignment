"""
Main orchestration runner for the Campaign Dispatcher.
"""
from __future__ import annotations

import asyncio
import logging
import signal
import sys
import time
from typing import Optional

import httpx
import uvicorn

from .config import SystemConfig
from .clock import ProviderClock
from .db import Database
from .dispatcher import CampaignDispatcher
from .reconciler import Reconciler
from .webhook_server import create_webhook_app
from .exporter import export_final_state

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("runner")


async def wait_for_provider(provider_url: str, max_retries: int = 30, interval: float = 1.0) -> ProviderClock:
    """Waits until the mock telephony provider is available and syncs the clock."""
    logger.info("Connecting to telephony provider at %s...", provider_url)
    for attempt in range(max_retries):
        try:
            clock = await ProviderClock.sync_from_provider(provider_url)
            logger.info("Connected to provider! Sim now: %s, Speed: %.1fx", clock.now().isoformat(), clock.speed)
            return clock
        except Exception as e:
            if attempt % 5 == 0:
                logger.info("Waiting for provider to be reachable (%s)...", e)
            await asyncio.sleep(interval)
    raise RuntimeError(f"Could not connect to provider at {provider_url} after {max_retries} seconds.")


class Runner:
    def __init__(self, cfg: SystemConfig):
        self.cfg = cfg
        self.db = Database(cfg.db_path)
        self.stop_requested = False

    async def run(self, out_path: str = "final_state.csv"):
        # 1. Sync Clock with provider
        clock = await wait_for_provider(self.cfg.provider_url)

        # 2. Seed database
        logger.info("Seeding fixtures into database (%s)...", self.cfg.db_path)
        self.db.seed_fixtures(self.cfg)

        # 3. Start Webhook Server in background
        app = create_webhook_app(self.cfg, self.db, clock)
        server_config = uvicorn.Config(
            app,
            host=self.cfg.webhook_host,
            port=self.cfg.webhook_port,
            log_level="warning",
            access_log=False,
        )
        server = uvicorn.Server(server_config)
        webhook_task = asyncio.create_task(server.serve())

        # Give server a moment to bind
        await asyncio.sleep(0.5)

        # 4. Instantiate Dispatcher and Reconciler
        dispatcher = CampaignDispatcher(self.cfg, self.db, clock)
        reconciler = Reconciler(self.cfg, self.db, clock)

        # 5. Boot Recovery (crash resilience)
        async with httpx.AsyncClient(timeout=10.0) as http_client:
            await reconciler.recover_on_boot(http_client)

            # 6. Main Orchestration Loop
            logger.info("Starting dispatch loop...")
            last_reconcile_time = time.time()
            last_status_log = time.time()

            try:
                while not self.stop_requested:
                    now = clock.now()

                    # Check completion condition
                    if dispatcher.is_finished():
                        logger.info("All leads have reached a final state! Exiting dispatch loop.")
                        break

                    # Dispatch candidates
                    candidates = dispatcher.get_candidate_leads(now)
                    dispatched_count = 0
                    for candidate in candidates:
                        dispatched = await dispatcher.reserve_and_dispatch(candidate, http_client)
                        if dispatched:
                            dispatched_count += 1

                    # Periodic lost-webhook watchdog (runs every 1.5 seconds)
                    if time.time() - last_reconcile_time > 1.5:
                        await reconciler.reconcile_active_calls(http_client)
                        last_reconcile_time = time.time()

                    # Periodic progress log (every 5 seconds)
                    if time.time() - last_status_log > 5.0:
                        with self.db.transaction() as conn:
                            cur = conn.execute(
                                """
                                SELECT status, count(*) as count
                                FROM leads
                                GROUP BY status
                                """
                            )
                            lead_stats = {r["status"]: r["count"] for r in cur.fetchall()}
                            cur = conn.execute(
                                "SELECT count(*) as cnt FROM calls WHERE state IN ('PLACING', 'ACTIVE')"
                            )
                            active = cur.fetchone()["cnt"]
                        logger.info("Progress: active_calls=%d, lead_states=%s", active, lead_stats)
                        last_status_log = time.time()

                    # Adaptive sleep: sleep shorter if active/candidates, slightly longer if idle
                    if dispatched_count > 0:
                        await asyncio.sleep(0.02)
                    else:
                        await asyncio.sleep(0.1)

            finally:
                # 7. Export output
                exported_file = export_final_state(self.db, out_path)
                logger.info("Exported final state to %s", exported_file)

                # 8. Check provider admin report
                try:
                    rep_resp = await http_client.get(f"{self.cfg.provider_url.rstrip('/')}/admin/report")
                    if rep_resp.status_code == 200:
                        report_data = rep_resp.json()
                        v_count = report_data.get("violation_count", 0)
                        logger.info("Provider /admin/report violation_count: %d", v_count)
                        if v_count > 0:
                            logger.warning("Violations: %s", report_data.get("violations_by_type"))
                        else:
                            logger.info("PERFECT RUN: 0 violations recorded by provider!")
                except Exception as e:
                    logger.warning("Could not fetch /admin/report: %s", e)

                # Shutdown webhook server
                server.should_exit = True
                await webhook_task

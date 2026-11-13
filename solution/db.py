"""
Database layer using SQLite with WAL mode and atomic transaction helpers.
"""
from __future__ import annotations

import csv
import json
import os
import sqlite3
from contextlib import contextmanager
from datetime import datetime
from typing import Generator, List, Dict, Any, Optional

from .config import SystemConfig, IST

SCHEMA_SQL = """
PRAGMA journal_mode = WAL;
PRAGMA synchronous = NORMAL;
PRAGMA busy_timeout = 10000;

CREATE TABLE IF NOT EXISTS tenants (
    tenant_id TEXT PRIMARY KEY,
    max_concurrent_calls INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS campaigns (
    campaign_id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    window_start TEXT NOT NULL,
    window_end TEXT NOT NULL,
    window_days TEXT NOT NULL,
    max_attempts INTEGER NOT NULL,
    FOREIGN KEY(tenant_id) REFERENCES tenants(tenant_id)
);

CREATE TABLE IF NOT EXISTS leads (
    campaign_id TEXT NOT NULL,
    lead_id TEXT NOT NULL,
    phone TEXT NOT NULL,
    customer_name TEXT,
    status TEXT NOT NULL,  -- FRESH, IN_FLIGHT, CALLBACK_DUE, RETRY_DUE, COMPLETED, EXHAUSTED, DND_BLOCKED
    attempts INTEGER NOT NULL DEFAULT 0,
    last_status TEXT,
    last_disposition TEXT,
    last_call_sid TEXT,
    next_eligible_at TEXT,
    callback_at TEXT,
    updated_at TEXT,
    PRIMARY KEY (campaign_id, lead_id),
    FOREIGN KEY(campaign_id) REFERENCES campaigns(campaign_id)
);

CREATE INDEX IF NOT EXISTS idx_leads_phone ON leads(phone);
CREATE INDEX IF NOT EXISTS idx_leads_status ON leads(status);

CREATE TABLE IF NOT EXISTS calls (
    request_id TEXT PRIMARY KEY,
    call_sid TEXT UNIQUE,
    campaign_id TEXT NOT NULL,
    lead_id TEXT NOT NULL,
    tenant_id TEXT NOT NULL,
    phone TEXT NOT NULL,
    attempt_no INTEGER NOT NULL,
    state TEXT NOT NULL,  -- PLACING, ACTIVE, ENDED
    status TEXT,
    disposition TEXT,
    callback_at TEXT,
    started_at TEXT,
    ended_at TEXT,
    duration_sec INTEGER DEFAULT 0,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    FOREIGN KEY(campaign_id, lead_id) REFERENCES leads(campaign_id, lead_id)
);

CREATE INDEX IF NOT EXISTS idx_calls_state ON calls(state);
CREATE INDEX IF NOT EXISTS idx_calls_active_tenant ON calls(tenant_id, state);
CREATE INDEX IF NOT EXISTS idx_calls_active_phone ON calls(phone, state);
CREATE INDEX IF NOT EXISTS idx_calls_lead ON calls(campaign_id, lead_id);

CREATE TABLE IF NOT EXISTS dnd_registry (
    phone TEXT PRIMARY KEY,
    registered_at TEXT NOT NULL,
    source_campaign_id TEXT,
    source_lead_id TEXT
);

CREATE TABLE IF NOT EXISTS processed_webhooks (
    delivery_id TEXT PRIMARY KEY,
    received_at TEXT NOT NULL
);
"""


class Database:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self._init_db()

    def get_connection(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, timeout=10.0, isolation_level=None)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode = WAL;")
        conn.execute("PRAGMA busy_timeout = 10000;")
        return conn

    @contextmanager
    def transaction(self) -> Generator[sqlite3.Connection, None, None]:
        conn = self.get_connection()
        conn.execute("BEGIN IMMEDIATE")
        try:
            yield conn
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise
        finally:
            conn.close()

    def _init_db(self):
        conn = self.get_connection()
        try:
            conn.executescript(SCHEMA_SQL)
        finally:
            conn.close()

    def seed_fixtures(self, cfg: SystemConfig):
        """
        Idempotently seeds tenants, campaigns, and leads from fixtures.
        Existing records and states are preserved.
        """
        with self.transaction() as conn:
            # Seed tenants
            for t in cfg.tenants.values():
                conn.execute(
                    """
                    INSERT INTO tenants (tenant_id, max_concurrent_calls)
                    VALUES (?, ?)
                    ON CONFLICT(tenant_id) DO UPDATE SET max_concurrent_calls = excluded.max_concurrent_calls
                    """,
                    (t.tenant_id, t.max_concurrent_calls),
                )

            # Seed campaigns
            for c in cfg.campaigns.values():
                conn.execute(
                    """
                    INSERT INTO campaigns (campaign_id, tenant_id, window_start, window_end, window_days, max_attempts)
                    VALUES (?, ?, ?, ?, ?, ?)
                    ON CONFLICT(campaign_id) DO UPDATE SET
                        tenant_id = excluded.tenant_id,
                        window_start = excluded.window_start,
                        window_end = excluded.window_end,
                        window_days = excluded.window_days,
                        max_attempts = excluded.max_attempts
                    """,
                    (
                        c.campaign_id,
                        c.tenant_id,
                        c.window.start,
                        c.window.end,
                        json.dumps(c.window.days or []),
                        c.max_attempts,
                    ),
                )

            # Seed leads
            now_iso = datetime.now(IST).isoformat()
            for c in cfg.campaigns.values():
                leads_file_path = os.path.join(cfg.fixtures_dir, c.leads_file)
                if not os.path.exists(leads_file_path):
                    continue
                with open(leads_file_path, "r", encoding="utf-8") as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        conn.execute(
                            """
                            INSERT OR IGNORE INTO leads (
                                campaign_id, lead_id, phone, customer_name, status,
                                attempts, updated_at
                            ) VALUES (?, ?, ?, ?, 'FRESH', 0, ?)
                            """,
                            (
                                c.campaign_id,
                                row["lead_id"],
                                row["phone"],
                                row.get("customer_name"),
                                now_iso,
                            ),
                        )

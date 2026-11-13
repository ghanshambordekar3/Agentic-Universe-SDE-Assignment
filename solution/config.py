"""
Configuration loader for the campaign dispatcher.
"""
from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Any, Optional

IST = timezone(timedelta(hours=5, minutes=30))


@dataclass
class TenantConfig:
    tenant_id: str
    max_concurrent_calls: int


@dataclass
class CallingWindowConfig:
    start: str
    end: str
    days: Optional[List[str]] = None


@dataclass
class CampaignConfig:
    campaign_id: str
    tenant_id: str
    leads_file: str
    window: CallingWindowConfig
    max_attempts: int


@dataclass
class RetryPolicyConfig:
    no_answer_minutes: int = 60
    busy_minutes: int = 60
    failed_minutes: int = 15


@dataclass
class SystemConfig:
    sim_start: datetime
    timezone_name: str
    regulatory_window: CallingWindowConfig
    window_grace_seconds: int
    retry_policy: RetryPolicyConfig
    webhook_secret: str
    tenants: Dict[str, TenantConfig]
    campaigns: Dict[str, CampaignConfig]
    fixtures_dir: str
    provider_url: str = "http://127.0.0.1:8099"
    webhook_host: str = "127.0.0.1"
    webhook_port: int = 8200
    webhook_url: str = "http://127.0.0.1:8200/webhooks/telephony"
    db_path: str = "campaign_dispatcher.db"

    @classmethod
    def load(cls, fixtures_path: str, provider_url: str = "http://127.0.0.1:8099",
             webhook_port: int = 8200, db_path: str = "campaign_dispatcher.db") -> SystemConfig:
        with open(fixtures_path, "r", encoding="utf-8") as f:
            raw = json.load(f)

        fixtures_dir = os.path.dirname(os.path.abspath(fixtures_path))
        sim_start = datetime.fromisoformat(raw["sim_start"]).astimezone(IST)

        reg_win = CallingWindowConfig(
            start=raw["regulatory_window"]["start"],
            end=raw["regulatory_window"]["end"],
            days=None,
        )

        rp = raw.get("retry_policy", {})
        retry_policy = RetryPolicyConfig(
            no_answer_minutes=rp.get("no_answer_minutes", 60),
            busy_minutes=rp.get("busy_minutes", 60),
            failed_minutes=rp.get("failed_minutes", 15),
        )

        tenants = {}
        for t in raw.get("tenants", []):
            tenants[t["tenant_id"]] = TenantConfig(
                tenant_id=t["tenant_id"],
                max_concurrent_calls=int(t["max_concurrent_calls"]),
            )

        campaigns = {}
        for c in raw.get("campaigns", []):
            w = c["window"]
            campaigns[c["campaign_id"]] = CampaignConfig(
                campaign_id=c["campaign_id"],
                tenant_id=c["tenant_id"],
                leads_file=c["leads_file"],
                window=CallingWindowConfig(
                    start=w["start"],
                    end=w["end"],
                    days=w.get("days"),
                ),
                max_attempts=int(c.get("max_attempts", 3)),
            )

        webhook_url = f"http://127.0.0.1:{webhook_port}/webhooks/telephony"

        return cls(
            sim_start=sim_start,
            timezone_name=raw.get("timezone", "Asia/Kolkata"),
            regulatory_window=reg_win,
            window_grace_seconds=int(raw.get("window_grace_seconds", 120)),
            retry_policy=retry_policy,
            webhook_secret=raw["webhook_secret"],
            tenants=tenants,
            campaigns=campaigns,
            fixtures_dir=fixtures_dir,
            provider_url=provider_url,
            webhook_port=webhook_port,
            webhook_url=webhook_url,
            db_path=db_path,
        )

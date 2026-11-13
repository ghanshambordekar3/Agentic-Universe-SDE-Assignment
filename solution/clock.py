"""
Simulated Clock and Calling Window computations.
"""
from __future__ import annotations

import asyncio
import time
from datetime import datetime, timedelta, timezone
from typing import Optional, List, Tuple

import httpx

IST = timezone(timedelta(hours=5, minutes=30))
DAYS = ["mon", "tue", "wed", "thu", "fri", "sat", "sun"]


def parse_hm(s: str) -> Tuple[int, int]:
    h, m = s.split(":")
    return int(h), int(m)


def in_window(
    t: datetime,
    start: str,
    end: str,
    days: Optional[List[str]] = None,
    grace_seconds: float = 0.0,
) -> bool:
    """
    Returns True if t is within [start, end) on an allowed day.
    Checks probe instants within [-grace_seconds, +grace_seconds].
    For conservative dispatcher checks, grace_seconds should be 0.
    """
    probes = [t]
    if grace_seconds > 0:
        probes.extend([t - timedelta(seconds=grace_seconds), t + timedelta(seconds=grace_seconds)])

    sh, sm = parse_hm(start)
    eh, em = parse_hm(end)

    for probe in probes:
        lt = probe.astimezone(IST)
        if days is not None and DAYS[lt.weekday()] not in days:
            continue
        s = lt.replace(hour=sh, minute=sm, second=0, microsecond=0)
        e = lt.replace(hour=eh, minute=em, second=0, microsecond=0)
        if s <= lt < e:
            return True
    return False


def next_open(t: datetime, start: str, end: str, days: Optional[List[str]] = None) -> datetime:
    """
    Computes the earliest instant >= t that is inside [start, end) on an allowed day.
    """
    lt = t.astimezone(IST)
    allowed_days = days if days is not None else DAYS
    sh, sm = parse_hm(start)
    eh, em = parse_hm(end)

    for d in range(0, 14):
        day = (lt + timedelta(days=d)).replace(second=0, microsecond=0)
        if DAYS[day.weekday()] not in allowed_days:
            continue
        s = day.replace(hour=sh, minute=sm)
        e = day.replace(hour=eh, minute=em)
        if d == 0:
            if lt < s:
                return s
            if lt < e:
                return lt
            continue
        return s
    return lt


class ProviderClock:
    """
    Synchronizes with the provider's simulated clock.
    Formula: sim_now = sim_epoch + (time.time() - real_epoch) * speed
    """
    def __init__(self, sim_epoch: datetime, real_epoch: float, speed: float):
        self.sim_epoch = sim_epoch.astimezone(IST)
        self.real_epoch = real_epoch
        self.speed = speed

    def now(self) -> datetime:
        elapsed_real = time.time() - self.real_epoch
        elapsed_sim = elapsed_real * self.speed
        return self.sim_epoch + timedelta(seconds=elapsed_sim)

    async def sleep_sim(self, sim_seconds: float):
        if self.speed <= 0:
            await asyncio.sleep(sim_seconds)
            return
        real_seconds = max(0.001, sim_seconds / self.speed)
        await asyncio.sleep(real_seconds)

    @classmethod
    async def sync_from_provider(cls, provider_url: str, timeout: float = 5.0) -> ProviderClock:
        async with httpx.AsyncClient(timeout=timeout) as client:
            resp = await client.get(f"{provider_url.rstrip('/')}/v1/clock")
            resp.raise_for_status()
            data = resp.json()
            sim_epoch = datetime.fromisoformat(data["sim_epoch"])
            real_epoch = float(data["real_epoch"])
            speed = float(data["speed"])
            return cls(sim_epoch=sim_epoch, real_epoch=real_epoch, speed=speed)

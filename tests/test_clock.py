"""
Unit tests for Clock and Calling Window calculations.
"""
from datetime import datetime, timezone, timedelta
import pytest

from solution.clock import in_window, next_open, parse_hm, ProviderClock, IST


def test_parse_hm():
    assert parse_hm("09:30") == (9, 30)
    assert parse_hm("21:00") == (21, 0)


def test_in_window_within_hours():
    # Thursday 12 Nov 2026 14:00 IST
    t = datetime(2026, 11, 12, 14, 0, 0, tzinfo=IST)
    assert in_window(t, "10:00", "19:00", ["mon", "tue", "wed", "thu", "fri", "sat"]) is True
    assert in_window(t, "09:00", "21:00", None) is True


def test_in_window_outside_hours():
    # Thursday 12 Nov 2026 21:30 IST (past regulatory window)
    t = datetime(2026, 11, 12, 21, 30, 0, tzinfo=IST)
    assert in_window(t, "09:00", "21:00", None, grace_seconds=0) is False
    assert in_window(t, "10:00", "19:00", ["thu"], grace_seconds=0) is False


def test_in_window_disallowed_day():
    # Sunday 15 Nov 2026 12:00 IST
    t = datetime(2026, 11, 15, 12, 0, 0, tzinfo=IST)
    # Campaign only allows Mon-Fri
    assert in_window(t, "09:30", "18:00", ["mon", "tue", "wed", "thu", "fri"], grace_seconds=0) is False


def test_next_open_same_day():
    # Thursday 12 Nov 2026 08:30 IST
    t = datetime(2026, 11, 12, 8, 30, 0, tzinfo=IST)
    nxt = next_open(t, "10:00", "19:00", ["thu"])
    assert nxt == datetime(2026, 11, 12, 10, 0, 0, tzinfo=IST)


def test_next_open_next_day():
    # Thursday 12 Nov 2026 20:00 IST (after 19:00 close)
    t = datetime(2026, 11, 12, 20, 0, 0, tzinfo=IST)
    nxt = next_open(t, "10:00", "19:00", ["thu", "fri"])
    assert nxt == datetime(2026, 11, 13, 10, 0, 0, tzinfo=IST)


def test_provider_clock_sim_speed():
    start = datetime(2026, 11, 12, 17, 30, 0, tzinfo=IST)
    real_epoch = 1000.0
    speed = 600.0  # 1 real sec = 10 sim min = 600 sim sec

    clock = ProviderClock(sim_epoch=start, real_epoch=real_epoch, speed=speed)
    # Mock time.time()
    import time
    orig_time = time.time
    try:
        time.time = lambda: 1001.0  # 1 second later
        now = clock.now()
        expected = start + timedelta(seconds=600)
        assert abs((now - expected).total_seconds()) < 0.001
    finally:
        time.time = orig_time

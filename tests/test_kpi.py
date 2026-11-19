from datetime import datetime, timedelta, timezone

import pytest

from dq_stream.config import load_config
from dq_stream.kpi import WindowAggregator, percentile, window_start
from dq_stream.validator import ValidationResult

CFG = load_config()
T0 = datetime(2026, 11, 19, 8, 0, tzinfo=timezone.utc)


def res(feed="claims", at=0, missing=(), errors=()):
    return ValidationResult(event_id="x", feed=feed, event_time=T0 + timedelta(seconds=at),
                            missing=list(missing), errors=list(errors))


def add(agg, r, lag=1.0):
    agg.add(r, r.event_time + timedelta(seconds=lag))


def test_window_start():
    assert window_start(T0 + timedelta(seconds=59), 60) == T0
    assert window_start(T0 + timedelta(seconds=60), 60) == T0 + timedelta(minutes=1)


def test_percentile():
    assert percentile([], 95) is None
    assert percentile([5], 95) == 5
    assert percentile(list(range(1, 101)), 95) == 95
    assert percentile(list(range(1, 101)), 50) == 50


def test_kpis_for_one_window():
    agg = WindowAggregator(CFG.feeds, window_s=60)
    for i in range(8):
        add(agg, res(at=i))
    add(agg, res(at=10, missing=["status"]))
    add(agg, res(at=20, errors=["claim_amount: -5 below min 0"]))

    (rec,) = agg.flush(force=True)
    assert rec.events == 10
    assert rec.completeness_pct == 90.0
    assert rec.error_rate_pct == 10.0
    assert rec.top_missing == "status"
    assert rec.top_error == "claim_amount"
    assert rec.window_start == T0.isoformat()


def test_lag_and_sla():
    agg = WindowAggregator(CFG.feeds, window_s=60)
    for i in range(19):
        add(agg, res(at=i), lag=2)
    add(agg, res(at=30), lag=500)          # claims SLA is 120s
    (rec,) = agg.flush(force=True)
    assert rec.p50_lag_s == 2
    assert rec.max_lag_s == 500
    assert rec.sla_breach_pct == 5.0


def test_window_only_closes_after_watermark():
    agg = WindowAggregator(CFG.feeds, window_s=60, allowed_lateness_s=120)
    add(agg, res(at=10))
    add(agg, res(at=150))                  # watermark = 150 - 120 = 30s -> window 0-60 still open
    assert agg.flush() == []
    add(agg, res(at=185))                  # watermark = 65s -> first window can close
    closed = agg.flush()
    assert [r.window_start for r in closed] == [T0.isoformat()]


def test_late_event_after_close_is_dropped_and_counted_on_arrival_window():
    agg = WindowAggregator(CFG.feeds, window_s=60, allowed_lateness_s=0)
    add(agg, res(at=10))
    add(agg, res(at=70))
    agg.flush()                            # closes 08:00
    late = res(at=20)                      # belongs to 08:00, which is closed
    agg.add(late, T0 + timedelta(seconds=75))
    (rec,) = agg.flush(force=True)
    assert rec.window_start == (T0 + timedelta(minutes=1)).isoformat()
    assert rec.events == 1
    assert rec.dropped_late == 1


def test_feeds_are_independent():
    agg = WindowAggregator(CFG.feeds, window_s=60)
    add(agg, res(feed="claims", at=5))
    add(agg, res(feed="pharmacy", at=5, missing=["ndc_code"]))
    recs = {r.feed: r for r in agg.flush(force=True)}
    assert recs["claims"].completeness_pct == 100.0
    assert recs["pharmacy"].completeness_pct == 0.0


def test_event_without_time_is_ignored():
    agg = WindowAggregator(CFG.feeds)
    r = res()
    r.event_time = None
    agg.add(r, T0)
    assert agg.flush(force=True) == []


@pytest.mark.parametrize("lateness", [0, 60, 300])
def test_nothing_lost_with_force_flush(lateness):
    agg = WindowAggregator(CFG.feeds, window_s=60, allowed_lateness_s=lateness)
    for s in range(0, 600, 7):
        add(agg, res(at=s))
    total = sum(r.events for r in agg.flush()) + sum(r.events for r in agg.flush(force=True))
    assert total == len(range(0, 600, 7))

"""
Offline simulation - runs the same validate -> window -> KPI path as the
consumer, but feeds it generated events directly (no Kafka needed).

Useful for demos, for building the Power BI report, and for checking that
a known incident (e.g. pharmacy error spike from minute 10 to 15) shows up
in the KPIs where it should.
"""
import random
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

from .events import make_event
from .kpi import WindowAggregator
from .validator import validate_event


@dataclass
class Incident:
    feed: str
    start_min: int
    end_min: int            # exclusive
    error_rate: float = 0.0
    late_rate: float = 0.0
    volume_factor: float = 1.0

    def active(self, feed, minute):
        return feed == self.feed and self.start_min <= minute < self.end_min


def run_simulation(cfg, minutes=30, events_per_min=None, incidents=(), seed=42,
                   start=None, base_error_rate=0.005, base_late_rate=0.01):
    """
    Returns a list of KpiRecord, one per feed per minute.
    events_per_min: dict feed -> rate (defaults to 120/min for every feed)
    """
    rng = random.Random(seed)
    start = start or datetime(2026, 11, 19, 8, 0, tzinfo=timezone.utc)
    rates = events_per_min or {name: 120 for name in cfg.feeds}
    agg = WindowAggregator(cfg.feeds, window_s=cfg.window_seconds)
    records = []

    for minute in range(minutes):
        minute_start = start + timedelta(minutes=minute)
        batch = []
        for feed in cfg.feeds:
            err, late, factor = base_error_rate, base_late_rate, 1.0
            max_late = 90                 # normal stragglers are only a little late
            for inc in incidents:
                if inc.active(feed, minute):
                    err = max(err, inc.error_rate)
                    if inc.late_rate:
                        late = max(late, inc.late_rate)
                        max_late = 600    # an upstream backlog being replayed
                    factor = inc.volume_factor
            n = int(rates.get(feed, 120) * factor)
            for _ in range(n):
                arrival = minute_start + timedelta(seconds=rng.uniform(0, 60))
                event = make_event(feed, rng=rng, error_rate=err, late_rate=late,
                                   max_late_s=max_late, now=arrival)
                # most events are processed within a few seconds of arriving
                processed = arrival + timedelta(seconds=rng.uniform(0.2, 4.0))
                batch.append((processed, event, feed))

        # process in arrival order, like a consumer would
        for processed, event, feed in sorted(batch, key=lambda b: b[0]):
            agg.add(validate_event(event, cfg.feeds[feed], today=processed.date()), processed)
        records.extend(agg.flush())

    records.extend(agg.flush(force=True))
    # late stragglers from minute 0 can open windows before the start - those are
    # only partly filled, so leave them out
    records = [r for r in records if r.window_start >= start.isoformat()]
    return sorted(records, key=lambda r: (r.window_start, r.feed))

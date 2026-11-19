"""
Tumbling-window KPIs per feed.

Events are bucketed by *event time* into fixed windows (default 60s).
A window is closed once the feed's watermark - the newest event_time seen
minus `allowed_lateness_s` - has moved past the window's end. Closed
windows are turned into one KpiRecord each.

An event that arrives after its window was already closed can't be added
any more; it's counted in `dropped_late` on the window it *arrived* in
(by processing time), so the dashboard shows the loss when it happened.

KPIs per window:
  events            - how many events landed in the window
  completeness_pct  - % of events with every required field present
  error_rate_pct    - % of events with at least one rule violation
  p50_lag_s/p95_lag_s/max_lag_s - processing time minus event time
  sla_breach_pct    - % of events whose lag was over the feed's freshness SLA
  dropped_late      - events that came after their window had closed
  top_missing / top_error - the field causing most of the problems
"""
import math
from collections import Counter, defaultdict
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone


def window_start(ts, window_s):
    epoch = int(ts.timestamp())
    return datetime.fromtimestamp(epoch - epoch % window_s, tz=timezone.utc)


def percentile(values, pct):
    """Nearest-rank percentile. Good enough for monitoring and has no numpy dependency."""
    if not values:
        return None
    ordered = sorted(values)
    rank = max(1, math.ceil(pct / 100 * len(ordered)))
    return ordered[rank - 1]


@dataclass
class KpiRecord:
    feed: str
    window_start: str
    window_end: str
    events: int
    completeness_pct: float
    error_rate_pct: float
    p50_lag_s: float | None
    p95_lag_s: float | None
    max_lag_s: float | None
    sla_breach_pct: float
    dropped_late: int
    top_missing: str | None
    top_error: str | None

    def to_dict(self):
        return asdict(self)


@dataclass
class _Window:
    events: int = 0
    complete: int = 0
    invalid: int = 0
    sla_breaches: int = 0
    lags: list = field(default_factory=list)
    missing: Counter = field(default_factory=Counter)
    errors: Counter = field(default_factory=Counter)


class WindowAggregator:
    def __init__(self, feeds, window_s=60, allowed_lateness_s=120):
        self.feeds = feeds                      # name -> FeedConfig
        self.window_s = window_s
        self.lateness = timedelta(seconds=allowed_lateness_s)
        self._open = defaultdict(dict)          # feed -> {window_start: _Window}
        self._closed_upto = {}                  # feed -> newest closed window_start
        self._max_event_time = {}               # feed -> newest event_time seen
        self._pending_dropped = Counter()       # (feed, arrival window) -> dropped late events

    def add(self, result, processed_at):
        """Add one ValidationResult. Events with no usable event_time are skipped."""
        feed = result.feed
        if result.event_time is None:
            return

        ws = window_start(result.event_time, self.window_s)
        closed = self._closed_upto.get(feed)
        if closed is not None and ws <= closed:
            self._pending_dropped[(feed, window_start(processed_at, self.window_s))] += 1
            return

        w = self._open[feed].setdefault(ws, _Window())
        w.events += 1
        if result.is_complete:
            w.complete += 1
        else:
            w.missing.update(result.missing)
        if not result.is_valid:
            w.invalid += 1
            w.errors.update(e.split(":", 1)[0] for e in result.errors)

        lag = max(0.0, (processed_at - result.event_time).total_seconds())
        w.lags.append(lag)
        if lag > self.feeds[feed].freshness_sla_s:
            w.sla_breaches += 1

        prev = self._max_event_time.get(feed)
        if prev is None or result.event_time > prev:
            self._max_event_time[feed] = result.event_time

    def watermark(self, feed):
        newest = self._max_event_time.get(feed)
        return None if newest is None else newest - self.lateness

    def flush(self, force=False):
        """Close and return every window behind the watermark (or all of them with force=True)."""
        out = []
        for feed, windows in self._open.items():
            wm = self.watermark(feed)
            for ws in sorted(windows):
                we = ws + timedelta(seconds=self.window_s)
                if not force and (wm is None or we > wm):
                    break
                out.append(self._to_record(feed, ws, we, windows.pop(ws)))
                self._closed_upto[feed] = ws
        return out

    def _to_record(self, feed, ws, we, w):
        n = w.events                     # always >= 1, windows are only created by add()
        dropped = self._pending_dropped.pop((feed, ws), 0)

        def pct(x):
            return round(100.0 * x / n, 2)

        return KpiRecord(
            feed=feed,
            window_start=ws.isoformat(),
            window_end=we.isoformat(),
            events=n,
            completeness_pct=pct(w.complete),
            error_rate_pct=pct(w.invalid),
            p50_lag_s=_round(percentile(w.lags, 50)),
            p95_lag_s=_round(percentile(w.lags, 95)),
            max_lag_s=_round(max(w.lags) if w.lags else None),
            sla_breach_pct=pct(w.sla_breaches),
            dropped_late=dropped,
            top_missing=w.missing.most_common(1)[0][0] if w.missing else None,
            top_error=w.errors.most_common(1)[0][0] if w.errors else None,
        )


def _round(v):
    return None if v is None else round(v, 2)

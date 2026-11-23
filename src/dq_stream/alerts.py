"""
Alerting on closed KPI windows.

Two kinds of checks per feed:

1. Thresholds (from feeds.yaml)  -> CRITICAL
     completeness below min, error rate above max, p95 lag above max,
     volume more than X% away from the feed's recent average

2. Baseline drift                -> WARN
     the metric is still inside its threshold but far outside what is
     normal for *this* feed (z-score against the last N windows). Catches
     a feed that normally runs at 0.2% errors jumping to 1.5% - under a 2%
     threshold, but clearly something changed.

Alerts are stateful: a metric has to be bad for `confirm_windows` windows in
a row before it fires (one noisy minute isn't an incident), then it fires
once (FIRING) and once more when it recovers (RESOLVED), instead of paging
every minute.
"""
import json
import logging
import os
import statistics
import urllib.request
from collections import defaultdict, deque
from dataclasses import asdict, dataclass

log = logging.getLogger(__name__)

# metric -> (KpiRecord field, which direction is bad)
METRICS = {
    "completeness": ("completeness_pct", "low"),
    "error_rate": ("error_rate_pct", "high"),
    "p95_lag": ("p95_lag_s", "high"),
    "volume": ("events", "both"),
}

# smallest spread used for the z-score. With ~100 events a minute, 1-2 bad
# records move a percentage by 1-2 points - that's noise, not drift.
MIN_SPREAD = {"completeness": 1.0, "error_rate": 1.0, "p95_lag": 5.0}
MIN_SPREAD_PCT_OF_MEAN = {"volume": 0.10}


@dataclass
class Alert:
    feed: str
    metric: str
    status: str          # FIRING | RESOLVED
    severity: str        # CRITICAL | WARN
    window_start: str
    value: float
    expected: str        # threshold or baseline, as readable text
    message: str

    def to_dict(self):
        return asdict(self)


class AlertEngine:
    def __init__(self, feeds, baseline_windows=15, min_history=5, z_limit=3.0, confirm_windows=2):
        self.feeds = feeds
        self.min_history = min_history
        self.z_limit = z_limit
        self.confirm_windows = confirm_windows
        self.streak = defaultdict(int)                                      # (feed, metric) -> bad run
        self.history = defaultdict(lambda: deque(maxlen=baseline_windows))   # (feed, metric) -> values
        self.active = {}                                                    # (feed, metric) -> Alert

    # ----------------------------------------------------------- checks
    def _threshold_breach(self, feed, metric, value):
        t = self.feeds[feed].thresholds
        if metric == "completeness" and value < t.min_completeness_pct:
            return f">= {t.min_completeness_pct}%"
        if metric == "error_rate" and value > t.max_error_rate_pct:
            return f"<= {t.max_error_rate_pct}%"
        if metric == "p95_lag" and value > t.max_p95_lag_s:
            return f"<= {t.max_p95_lag_s}s"
        if metric == "volume":
            hist = self.history[(feed, metric)]
            if len(hist) >= self.min_history:
                avg = statistics.fmean(hist)
                if avg and abs(value - avg) / avg * 100 > t.volume_change_pct:
                    return f"{avg:.0f} +/-{t.volume_change_pct:.0f}%"
        return None

    def _baseline_drift(self, feed, metric, value):
        hist = self.history[(feed, metric)]
        if len(hist) < self.min_history:
            return None
        mean = statistics.fmean(hist)
        # floor on the spread so a perfectly flat history doesn't flag tiny wiggles
        spread = max(statistics.pstdev(hist), MIN_SPREAD.get(metric, 0.0),
                     abs(mean) * MIN_SPREAD_PCT_OF_MEAN.get(metric, 0.0), 1e-9)
        z = (value - mean) / spread
        direction = METRICS[metric][1]
        bad = (direction == "high" and z > self.z_limit) or \
              (direction == "low" and z < -self.z_limit) or \
              (direction == "both" and abs(z) > self.z_limit)
        return f"~{mean:.2f} for this feed (z={z:+.1f})" if bad else None

    # ----------------------------------------------------------- main entry
    def evaluate(self, record):
        """Check one KpiRecord. Returns the alerts that changed state (new or resolved)."""
        out = []
        for metric, (fld, _) in METRICS.items():
            value = getattr(record, fld)
            if value is None:
                continue
            key = (record.feed, metric)

            expected = self._threshold_breach(record.feed, metric, value)
            severity = "CRITICAL" if expected else None
            if not expected:
                expected = self._baseline_drift(record.feed, metric, value)
                severity = "WARN" if expected else None

            self.streak[key] = self.streak[key] + 1 if severity else 0
            confirmed = severity and self.streak[key] >= self.confirm_windows

            current = self.active.get(key)
            if confirmed and (current is None or current.severity != severity):
                alert = Alert(record.feed, metric, "FIRING", severity, record.window_start, value,
                              expected, f"{record.feed} {metric}={value}, expected {expected}")
                self.active[key] = alert
                out.append(alert)
            elif not severity and current is not None:
                out.append(Alert(record.feed, metric, "RESOLVED", current.severity, record.window_start,
                                 value, current.expected, f"{record.feed} {metric} back to normal ({value})"))
                del self.active[key]

            # only learn from healthy windows so an incident doesn't become the new normal
            if not severity:
                self.history[key].append(value)
        return out


# ------------------------------------------------------------------- notifiers
def notify_log(alerts):
    for a in alerts:
        level = logging.WARNING if a.status == "FIRING" else logging.INFO
        log.log(level, "[%s %s] %s @ %s", a.status, a.severity, a.message, a.window_start[11:16])


def notify_slack(alerts, webhook_url=None, timeout=5):
    """Posts FIRING/RESOLVED alerts to a Slack incoming webhook. No-op if no URL is set."""
    url = webhook_url or os.getenv("SLACK_WEBHOOK_URL")
    if not url or not alerts:
        return False
    icon = {"FIRING": ":red_circle:", "RESOLVED": ":large_green_circle:"}
    text = "\n".join(f"{icon[a.status]} *{a.status}* {a.severity} - {a.message} ({a.window_start[11:16]} UTC)"
                     for a in alerts)
    req = urllib.request.Request(url, data=json.dumps({"text": text}).encode(),
                                 headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=timeout):
            return True
    except OSError as exc:
        # a broken webhook must never stop the pipeline
        log.error("slack notify failed: %s", exc)
        return False

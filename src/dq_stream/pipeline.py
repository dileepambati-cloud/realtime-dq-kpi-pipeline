"""
The consumer loop, separated from Kafka so it can be tested with any
iterable of (topic, event, msg) tuples - a real EventConsumer in
production, a list in unit tests.
"""
import logging
from collections import Counter
from datetime import datetime, timezone

from .alerts import AlertEngine, notify_log, notify_slack
from .kpi import WindowAggregator
from .validator import validate_event

log = logging.getLogger(__name__)


class StreamPipeline:
    def __init__(self, cfg, store, flush_every=200, export_csv=False, clock=None):
        self.cfg = cfg
        self.store = store
        self.flush_every = flush_every
        self.export_csv = export_csv
        self.clock = clock or (lambda: datetime.now(timezone.utc))
        self.agg = WindowAggregator(cfg.feeds, window_s=cfg.window_seconds)
        self.alerts = AlertEngine(cfg.feeds)
        self.unreadable = Counter()
        self.processed = 0

    def handle(self, topic, event):
        feed = self.cfg.feed_for_topic(topic)
        now = self.clock()
        if event is None:
            self.unreadable[feed.name] += 1
        else:
            self.agg.add(validate_event(event, feed, today=now.date()), now)
        self.processed += 1

    def flush(self, force=False):
        records = self.agg.flush(force=force)
        if not records:
            return [], []
        self.store.write(records)
        for r in records:
            log.info("%s %s  events=%d complete=%.1f%% errors=%.1f%% p95_lag=%ss dropped=%d",
                     r.feed, r.window_start[11:16], r.events, r.completeness_pct,
                     r.error_rate_pct, r.p95_lag_s, r.dropped_late)
        changed = [a for r in records for a in self.alerts.evaluate(r)]
        if changed:
            self.store.write_alerts(changed)
            notify_log(changed)
            notify_slack(changed)
        if self.export_csv:
            self.store.export_csv()
            self.store.export_alerts_csv()
        return records, changed

    def run(self, messages, commit=None):
        """
        Process (topic, event, msg) tuples until the iterator ends.
        `commit(msg)` is called after each message is handled, so an offset
        is only committed once its event has been counted.
        """
        try:
            for topic, event, msg in messages:
                self.handle(topic, event)
                if commit:
                    commit(msg)
                if self.processed % self.flush_every == 0:
                    self.flush()
        finally:
            self.flush(force=True)      # write whatever is still open before exiting
            if self.unreadable:
                log.warning("unreadable (non-JSON) messages: %s", dict(self.unreadable))

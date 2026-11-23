"""
Reads events from all configured topics, validates each one, rolls them
into per-feed tumbling windows, writes closed windows to the KPI store and
raises alerts (log + Slack if SLACK_WEBHOOK_URL is set) when a feed drifts.

    python consumer.py                  # run until Ctrl+C
    python consumer.py --idle-stop 10   # stop after 10 empty polls (handy for tests/CI)
    python consumer.py --csv            # also refresh output/dq_kpi_window.csv on every flush
"""
import argparse
import logging
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent / "src"))

from dq_stream.alerts import AlertEngine, notify_log, notify_slack  # noqa: E402
from dq_stream.config import load_config  # noqa: E402
from dq_stream.kafka_io import EventConsumer  # noqa: E402
from dq_stream.kpi import WindowAggregator  # noqa: E402
from dq_stream.sink import KpiStore  # noqa: E402
from dq_stream.validator import validate_event  # noqa: E402

log = logging.getLogger("consumer")


def log_windows(records):
    for r in records:
        log.info("%s %s  events=%d complete=%.1f%% errors=%.1f%% p95_lag=%ss dropped=%d",
                 r.feed, r.window_start[11:16], r.events, r.completeness_pct,
                 r.error_rate_pct, r.p95_lag_s, r.dropped_late)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--group", default="dq-kpi")
    ap.add_argument("--idle-stop", type=int, default=None, help="stop after N empty polls")
    ap.add_argument("--db", default="output/dq_kpi.db")
    ap.add_argument("--csv", action="store_true", help="refresh the CSV export after each flush")
    args = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    cfg = load_config()
    consumer = EventConsumer([f.topic for f in cfg.feeds.values()], group_id=args.group)
    agg = WindowAggregator(cfg.feeds, window_s=cfg.window_seconds)
    alerts = AlertEngine(cfg.feeds)
    store = KpiStore(args.db)
    unreadable = Counter()

    def flush(force=False):
        records = agg.flush(force=force)
        if records:
            store.write(records)
            log_windows(records)
            changed = [a for r in records for a in alerts.evaluate(r)]
            if changed:
                store.write_alerts(changed)
                notify_log(changed)
                notify_slack(changed)
            if args.csv:
                store.export_csv()
                store.export_alerts_csv()

    try:
        for i, (topic, event, msg) in enumerate(consumer.messages(max_idle_polls=args.idle_stop), 1):
            feed = cfg.feed_for_topic(topic)
            now = datetime.now(timezone.utc)
            if event is None:
                unreadable[feed.name] += 1
            else:
                agg.add(validate_event(event, feed, today=now.date()), now)
            consumer.commit(msg)
            if i % 200 == 0:
                flush()
    except KeyboardInterrupt:
        pass
    finally:
        flush(force=True)       # write whatever is still open before exiting
        consumer.close()
        store.close()
        if unreadable:
            log.warning("unreadable (non-JSON) messages: %s", dict(unreadable))


if __name__ == "__main__":
    main()

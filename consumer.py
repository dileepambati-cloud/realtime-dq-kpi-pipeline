"""
Reads events from all configured topics, validates each one, rolls them
into per-feed tumbling windows, writes closed windows to the KPI store and
raises alerts (log + Slack if SLACK_WEBHOOK_URL is set) when a feed drifts.

    python consumer.py                  # run until Ctrl+C
    python consumer.py --idle-stop 10   # stop after 10 empty polls (handy for tests/CI)
    python consumer.py --csv            # also refresh the CSV exports on every flush
"""
import argparse
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent / "src"))

from dq_stream.config import load_config  # noqa: E402
from dq_stream.kafka_io import EventConsumer  # noqa: E402
from dq_stream.pipeline import StreamPipeline  # noqa: E402
from dq_stream.sink import KpiStore  # noqa: E402


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--group", default="dq-kpi")
    ap.add_argument("--idle-stop", type=int, default=None, help="stop after N empty polls")
    ap.add_argument("--db", default="output/dq_kpi.db")
    ap.add_argument("--csv", action="store_true", help="refresh the CSV exports after each flush")
    args = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    cfg = load_config()
    consumer = EventConsumer([f.topic for f in cfg.feeds.values()], group_id=args.group)
    store = KpiStore(args.db)
    pipeline = StreamPipeline(cfg, store, export_csv=args.csv)

    try:
        pipeline.run(consumer.messages(max_idle_polls=args.idle_stop), commit=consumer.commit)
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()
        store.close()


if __name__ == "__main__":
    main()

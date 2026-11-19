"""
Reads events from all configured topics and validates each one.

For now this prints running counts per feed. Windowed KPIs and alerts
are built on top of this loop.

    python consumer.py                 # run until Ctrl+C
    python consumer.py --idle-stop 10  # stop after 10 empty polls (handy for tests/CI)
"""
import argparse
import logging
import sys
from collections import Counter
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent / "src"))

from dq_stream.config import load_config  # noqa: E402
from dq_stream.kafka_io import EventConsumer  # noqa: E402
from dq_stream.validator import validate_event  # noqa: E402

log = logging.getLogger("consumer")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--group", default="dq-kpi")
    ap.add_argument("--idle-stop", type=int, default=None, help="stop after N empty polls")
    args = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    cfg = load_config()
    consumer = EventConsumer([f.topic for f in cfg.feeds.values()], group_id=args.group)
    counts = {name: Counter() for name in cfg.feeds}

    try:
        for topic, event, msg in consumer.messages(max_idle_polls=args.idle_stop):
            feed = cfg.feed_for_topic(topic)
            c = counts[feed.name]
            c["events"] += 1
            if event is None:
                c["unreadable"] += 1
            else:
                result = validate_event(event, feed)
                c["incomplete"] += 0 if result.is_complete else 1
                c["invalid"] += 0 if result.is_valid else 1
            consumer.commit(msg)

            if c["events"] % 100 == 0:
                log.info("%s: %s", feed.name, dict(c))
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()
        for name, c in counts.items():
            if c["events"]:
                log.info("final %s: %s", name, dict(c))


if __name__ == "__main__":
    main()

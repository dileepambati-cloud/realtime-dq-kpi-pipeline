"""
Sends synthetic events to Kafka (or prints them with --dry-run).

    python producer.py --feed claims --rate 20 --seconds 60
    python producer.py --feed pharmacy --rate 50 --error-rate 0.08      # a feed going bad
    python producer.py --feed claims --late-rate 0.3                    # lots of late events
    python producer.py --feed eligibility --count 5 --dry-run           # just print
"""
import argparse
import json
import logging
import random
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent / "src"))

from dq_stream.config import load_config  # noqa: E402
from dq_stream.events import make_event  # noqa: E402


def main():
    ap = argparse.ArgumentParser(description="synthetic healthcare event producer")
    ap.add_argument("--feed", required=True, choices=["claims", "eligibility", "pharmacy"])
    ap.add_argument("--rate", type=float, default=10, help="events per second")
    ap.add_argument("--seconds", type=float, default=30, help="how long to run")
    ap.add_argument("--count", type=int, help="send exactly N events (ignores --seconds)")
    ap.add_argument("--error-rate", type=float, default=0.01, help="share of events to corrupt (0-1)")
    ap.add_argument("--late-rate", type=float, default=0.02, help="share of events that arrive late (0-1)")
    ap.add_argument("--seed", type=int, help="random seed for repeatable runs")
    ap.add_argument("--dry-run", action="store_true", help="print events instead of sending")
    args = ap.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    topic = load_config().feeds[args.feed].topic

    rng = random.Random(args.seed)
    total = args.count if args.count is not None else int(args.rate * args.seconds)
    pause = 1.0 / args.rate if args.rate > 0 else 0

    producer = None
    if not args.dry_run:
        from dq_stream.kafka_io import EventProducer
        producer = EventProducer()

    for i in range(total):
        event = make_event(args.feed, rng=rng, error_rate=args.error_rate, late_rate=args.late_rate)
        if producer:
            producer.send(topic, event)
        else:
            print(json.dumps(event))
        if pause and not args.dry_run:
            time.sleep(pause)
        if producer and (i + 1) % 500 == 0:
            logging.info("%s: %d events queued", topic, i + 1)

    if producer:
        producer.flush()
        logging.info("done: %d delivered, %d failed -> %s", producer.sent, producer.failed, topic)


if __name__ == "__main__":
    main()

"""
Thin wrapper around confluent-kafka.

Kept separate so the rest of the code (validation, KPIs, alerts) works on
plain dicts and can be tested without a broker. confluent_kafka is only
imported when a producer/consumer is actually created.
"""
import json
import logging
import os

log = logging.getLogger(__name__)

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")


def _kafka():
    try:
        import confluent_kafka
    except ImportError as exc:
        raise ImportError("confluent-kafka is not installed - pip install confluent-kafka") from exc
    return confluent_kafka


class EventProducer:
    def __init__(self, bootstrap=BOOTSTRAP, producer=None):
        # `producer` lets tests pass a fake object with produce/poll/flush
        self._p = producer or _kafka().Producer({
            "bootstrap.servers": bootstrap,
            "enable.idempotence": True,
            "linger.ms": 20,
        })
        self.sent = 0
        self.failed = 0

    def _on_delivery(self, err, msg):
        if err is not None:
            self.failed += 1
            log.error("delivery failed: %s", err)
        else:
            self.sent += 1

    def send(self, topic, event):
        # key by member so all events for one member land on the same partition
        key = str((event.get("payload") or {}).get("member_id") or event.get("event_id"))
        self._p.produce(topic, key=key.encode(), value=json.dumps(event).encode(),
                        on_delivery=self._on_delivery)
        self._p.poll(0)

    def flush(self, timeout=10):
        self._p.flush(timeout)


def decode(raw_value):
    """bytes from Kafka -> dict, or None if it isn't valid JSON."""
    try:
        return json.loads(raw_value)
    except (TypeError, ValueError):
        return None


class EventConsumer:
    def __init__(self, topics, group_id="dq-kpi", bootstrap=BOOTSTRAP, consumer=None):
        self._c = consumer or _kafka().Consumer({
            "bootstrap.servers": bootstrap,
            "group.id": group_id,
            "auto.offset.reset": "earliest",
            # commit after we've processed the message, not before
            "enable.auto.commit": False,
        })
        self._c.subscribe(list(topics))

    def messages(self, timeout=1.0, max_idle_polls=None):
        """
        Yields (topic, event_or_None, msg). Stops after `max_idle_polls`
        empty polls in a row (None = run forever).
        """
        idle = 0
        while True:
            msg = self._c.poll(timeout)
            if msg is None:
                idle += 1
                if max_idle_polls is not None and idle >= max_idle_polls:
                    return
                continue
            idle = 0
            if msg.error():
                log.warning("consumer error: %s", msg.error())
                continue
            yield msg.topic(), decode(msg.value()), msg

    def commit(self, msg):
        self._c.commit(message=msg, asynchronous=True)

    def close(self):
        self._c.close()

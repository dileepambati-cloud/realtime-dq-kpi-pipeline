import json

from dq_stream.kafka_io import EventConsumer, EventProducer, decode


class FakeProducer:
    def __init__(self):
        self.messages = []

    def produce(self, topic, key, value, on_delivery):
        self.messages.append((topic, key, value))
        on_delivery(None, object())

    def poll(self, timeout):
        return 0

    def flush(self, timeout=None):
        return 0


class FakeMsg:
    def __init__(self, topic, value, err=None):
        self._t, self._v, self._e = topic, value, err

    def topic(self):
        return self._t

    def value(self):
        return self._v

    def error(self):
        return self._e


class FakeConsumer:
    def __init__(self, msgs):
        self.queue = list(msgs)
        self.committed = []
        self.subscribed = None
        self.closed = False

    def subscribe(self, topics):
        self.subscribed = topics

    def poll(self, timeout):
        return self.queue.pop(0) if self.queue else None

    def commit(self, message, asynchronous):
        self.committed.append(message)

    def close(self):
        self.closed = True


def test_producer_keys_by_member():
    fake = FakeProducer()
    p = EventProducer(producer=fake)
    p.send("dq.claims", {"event_id": "e1", "payload": {"member_id": "M00001"}})
    p.send("dq.claims", {"event_id": "e2", "payload": {}})
    assert fake.messages[0][1] == b"M00001"
    assert fake.messages[1][1] == b"e2"          # falls back to event_id
    assert json.loads(fake.messages[0][2])["event_id"] == "e1"
    assert p.sent == 2 and p.failed == 0


def test_decode_handles_garbage():
    assert decode(b'{"a": 1}') == {"a": 1}
    assert decode(b"not json") is None
    assert decode(None) is None


def test_consumer_yields_and_stops_when_idle():
    msgs = [
        FakeMsg("dq.claims", b'{"event_id": "1"}'),
        None,
        FakeMsg("dq.claims", b"", err="broker hiccup"),   # skipped
        FakeMsg("dq.pharmacy", b"garbage"),
    ]
    fake = FakeConsumer(msgs)
    c = EventConsumer(["dq.claims", "dq.pharmacy"], consumer=fake)
    out = list(c.messages(max_idle_polls=2))

    assert fake.subscribed == ["dq.claims", "dq.pharmacy"]
    assert [(t, e) for t, e, _ in out] == [("dq.claims", {"event_id": "1"}), ("dq.pharmacy", None)]

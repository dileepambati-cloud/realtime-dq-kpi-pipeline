"""
Round trip through a real Kafka broker: produce -> consume -> validate ->
KPI windows -> store.

Skipped unless KAFKA_BOOTSTRAP_SERVERS is set (CI starts a broker for it).
Locally: `docker compose up -d` and
    KAFKA_BOOTSTRAP_SERVERS=localhost:9092 python -m pytest tests/integration -v
"""
import os
import random
import uuid

import pytest

pytest.importorskip("confluent_kafka")
BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
pytestmark = pytest.mark.skipif(not BOOTSTRAP, reason="KAFKA_BOOTSTRAP_SERVERS not set")

from confluent_kafka.admin import AdminClient, NewTopic  # noqa: E402

from dq_stream.config import load_config  # noqa: E402
from dq_stream.events import make_event  # noqa: E402
from dq_stream.kafka_io import EventConsumer, EventProducer  # noqa: E402
from dq_stream.pipeline import StreamPipeline  # noqa: E402
from dq_stream.sink import KpiStore  # noqa: E402

CFG = load_config()


@pytest.fixture(scope="module", autouse=True)
def topics():
    admin = AdminClient({"bootstrap.servers": BOOTSTRAP})
    new = [NewTopic(f.topic, num_partitions=3, replication_factor=1) for f in CFG.feeds.values()]
    for fut in admin.create_topics(new).values():
        try:
            fut.result(timeout=30)
        except Exception as exc:   # already exists is fine
            if "TOPIC_ALREADY_EXISTS" not in str(exc):
                raise


def test_produce_consume_kpis(tmp_path):
    rng = random.Random(11)
    producer = EventProducer(bootstrap=BOOTSTRAP)
    sent = {"claims": 0, "pharmacy": 0}
    for _ in range(300):
        producer.send("dq.claims", make_event("claims", rng=rng, error_rate=0.0))
        sent["claims"] += 1
    for _ in range(200):
        producer.send("dq.pharmacy", make_event("pharmacy", rng=rng, error_rate=0.25))
        sent["pharmacy"] += 1
    producer.flush(30)
    assert producer.failed == 0 and producer.sent == 500

    consumer = EventConsumer([f.topic for f in CFG.feeds.values()],
                             group_id=f"it-{uuid.uuid4().hex[:8]}", bootstrap=BOOTSTRAP)
    store = KpiStore(str(tmp_path / "kpi.db"))
    pipeline = StreamPipeline(CFG, store, flush_every=100)
    try:
        # first polls can be empty while the group joins, so allow a generous idle limit
        pipeline.run(consumer.messages(timeout=1.0, max_idle_polls=20), commit=consumer.commit)
    finally:
        consumer.close()

    rows = {r: (n, err) for r, n, err in store.conn.execute(
        "SELECT feed, SUM(events), SUM(events * error_rate_pct) / SUM(events) "
        "FROM dq_kpi_window GROUP BY feed")}
    store.close()

    assert rows["claims"][0] == sent["claims"]
    assert rows["pharmacy"][0] == sent["pharmacy"]
    assert rows["claims"][1] < 1.0            # clean feed
    assert rows["pharmacy"][1] > 5.0          # 25% of events corrupted

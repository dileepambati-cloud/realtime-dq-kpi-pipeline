import random
from datetime import datetime, timedelta, timezone

from dq_stream.config import load_config
from dq_stream.events import make_event
from dq_stream.pipeline import StreamPipeline
from dq_stream.sink import KpiStore

CFG = load_config()
T0 = datetime(2026, 11, 19, 8, 0, tzinfo=timezone.utc)


def stream(minutes=5, per_min=60, bad_feed=None, bad_from=None, seed=1):
    """Events in arrival order plus the matching processing times."""
    rng = random.Random(seed)
    msgs, times = [], []
    step = 60 / per_min
    for m in range(minutes):
        for i in range(per_min):
            for feed in CFG.feeds.values():
                t = T0 + timedelta(minutes=m, seconds=i * step)
                err = 0.3 if (feed.name == bad_feed and m >= bad_from) else 0.0
                msgs.append((feed.topic, make_event(feed.name, rng=rng, error_rate=err, now=t), object()))
                times.append(t + timedelta(seconds=1))
    return msgs, iter(times)


def test_pipeline_end_to_end_without_kafka(tmp_path):
    msgs, times = stream(minutes=8, bad_feed="pharmacy", bad_from=5)
    store = KpiStore(str(tmp_path / "kpi.db"))
    committed = []
    p = StreamPipeline(CFG, store, flush_every=50, clock=lambda: next(times))
    p.run(msgs, commit=committed.append)

    assert len(committed) == len(msgs)                       # every message committed
    assert p.processed == len(msgs)
    assert store.count() == 8 * len(CFG.feeds)               # one window per feed per minute
    total = store.conn.execute("SELECT SUM(events) FROM dq_kpi_window").fetchone()[0]
    assert total == len(msgs)                                # nothing lost

    fired = {(a["feed"], a["metric"]) for a in store.alerts() if a["status"] == "FIRING"}
    assert ("pharmacy", "error_rate") in fired
    assert not any(f == "claims" for f, _ in fired)
    store.close()


def test_unreadable_messages_are_counted_not_fatal(tmp_path):
    store = KpiStore(str(tmp_path / "kpi.db"))
    p = StreamPipeline(CFG, store, clock=lambda: T0)
    p.run([("dq.claims", None, object()), ("dq.claims", None, object())])
    assert p.unreadable["claims"] == 2
    assert store.count() == 0
    store.close()


def test_open_windows_flushed_on_error(tmp_path):
    store = KpiStore(str(tmp_path / "kpi.db"))
    msgs, times = stream(minutes=1, per_min=10)

    def broken():
        yield from msgs[:5]
        raise KeyboardInterrupt

    p = StreamPipeline(CFG, store, clock=lambda: next(times))
    try:
        p.run(broken())
    except KeyboardInterrupt:
        pass
    assert store.conn.execute("SELECT SUM(events) FROM dq_kpi_window").fetchone()[0] == 5
    store.close()

import csv

from dq_stream.config import load_config
from dq_stream.simulate import Incident, run_simulation
from dq_stream.sink import COLUMNS, KpiStore

CFG = load_config()


def by(records, feed):
    return {r.window_start[11:16]: r for r in records if r.feed == feed}


def test_quiet_run_has_one_row_per_feed_per_minute():
    recs = run_simulation(CFG, minutes=5, seed=1)
    assert len(recs) == 5 * len(CFG.feeds)
    assert all(r.completeness_pct > 95 for r in recs)


def test_error_spike_shows_up_only_where_injected():
    recs = run_simulation(CFG, minutes=10, seed=2,
                          incidents=[Incident("pharmacy", 4, 7, error_rate=0.2)])
    ph = by(recs, "pharmacy")
    spike = [ph[m] for m in ("08:04", "08:05", "08:06")]
    calm = [ph[m] for m in ("08:01", "08:02", "08:08")]
    assert min(r.error_rate_pct + (100 - r.completeness_pct) for r in spike) > 8
    assert max(r.error_rate_pct + (100 - r.completeness_pct) for r in calm) < 4
    # other feeds aren't affected
    assert max(r.error_rate_pct for r in by(recs, "claims").values()) < 4


def test_volume_drop():
    recs = run_simulation(CFG, minutes=6, seed=3,
                          incidents=[Incident("eligibility", 2, 4, volume_factor=0.25)])
    el = by(recs, "eligibility")
    assert el["08:02"].events < 40 and el["08:03"].events < 40
    assert el["08:01"].events > 100


def test_late_burst_drops_events():
    recs = run_simulation(CFG, minutes=12, seed=4,
                          incidents=[Incident("claims", 8, 10, late_rate=0.4)])
    cl = by(recs, "claims")
    assert cl["08:08"].dropped_late + cl["08:09"].dropped_late > 30
    assert cl["08:03"].dropped_late == 0


def test_store_upsert_and_csv(tmp_path):
    recs = run_simulation(CFG, minutes=3, seed=5)
    store = KpiStore(str(tmp_path / "kpi.db"))
    store.write(recs)
    store.write(recs)                        # replay -> no duplicates
    assert store.count() == len(recs)

    latest = store.recent("claims", limit=1)[0]
    assert latest["window_start"].startswith("2026-11-19T08:02")

    path, n = store.export_csv(tmp_path / "kpi.csv")
    with open(path) as f:
        rows = list(csv.DictReader(f))
    assert n == len(recs) == len(rows)
    assert list(rows[0]) == COLUMNS
    store.close()


def test_simulation_does_not_depend_on_todays_date():
    # generated dates follow the simulated clock, so a quiet run stays quiet
    # whether it is dated in the past or the future
    from datetime import datetime, timezone
    for year in (2020, 2030):
        recs = run_simulation(CFG, minutes=3, seed=6,
                              start=datetime(year, 1, 15, 9, 0, tzinfo=timezone.utc))
        assert max(r.error_rate_pct for r in recs) < 4, year

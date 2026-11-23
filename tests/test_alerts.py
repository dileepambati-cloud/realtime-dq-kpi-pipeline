import json

import pytest

from dq_stream.alerts import AlertEngine, notify_slack
from dq_stream.config import load_config
from dq_stream.kpi import KpiRecord
from dq_stream.simulate import Incident, run_simulation
from dq_stream.sink import KpiStore

CFG = load_config()


def rec(minute, feed="claims", events=120, complete=100.0, errors=0.0, p95=4.0):
    return KpiRecord(feed=feed, window_start=f"2026-11-19T08:{minute:02d}:00+00:00",
                     window_end=f"2026-11-19T08:{minute + 1:02d}:00+00:00", events=events,
                     completeness_pct=complete, error_rate_pct=errors, p50_lag_s=2.0, p95_lag_s=p95,
                     max_lag_s=p95, sla_breach_pct=0.0, dropped_late=0, top_missing=None, top_error=None)


def feed_in(engine, records):
    return [a for r in records for a in engine.evaluate(r)]


def test_threshold_needs_two_bad_windows_then_fires_once_and_resolves():
    eng = AlertEngine(CFG.feeds)
    out = feed_in(eng, [rec(0, errors=5.0)])
    assert out == []                                    # one bad minute is not an incident

    out = feed_in(eng, [rec(1, errors=6.0), rec(2, errors=7.0)])
    assert [(a.status, a.severity, a.metric) for a in out] == [("FIRING", "CRITICAL", "error_rate")]

    out = feed_in(eng, [rec(3, errors=0.0)])
    assert [(a.status, a.metric) for a in out] == [("RESOLVED", "error_rate")]


def test_feed_specific_threshold():
    # 1.5% is fine for claims (max 2%) but not for pharmacy (max 1%)
    eng = AlertEngine(CFG.feeds)
    out = feed_in(eng, [rec(m, feed="claims", errors=1.5) for m in range(3)])
    out += feed_in(eng, [rec(m, feed="pharmacy", errors=1.5) for m in range(3)])
    assert {a.feed for a in out if a.severity == "CRITICAL"} == {"pharmacy"}


def test_baseline_drift_warns_inside_threshold():
    eng = AlertEngine(CFG.feeds)
    feed_in(eng, [rec(m, p95=4.0 + (m % 3)) for m in range(10)])     # normal lag ~5s
    out = feed_in(eng, [rec(10, p95=90), rec(11, p95=95)])            # far above normal, under 300s limit
    assert [(a.severity, a.metric) for a in out] == [("WARN", "p95_lag")]


def test_volume_drop_against_average():
    eng = AlertEngine(CFG.feeds)
    feed_in(eng, [rec(m, events=120) for m in range(8)])
    out = feed_in(eng, [rec(8, events=30), rec(9, events=25)])
    assert out[0].metric == "volume" and out[0].severity == "CRITICAL"
    assert "120 +/-50%" in out[0].expected


def test_incident_does_not_become_the_new_normal():
    eng = AlertEngine(CFG.feeds)
    feed_in(eng, [rec(m, p95=4.0) for m in range(6)])
    feed_in(eng, [rec(m, p95=120.0) for m in range(6, 20)])          # long incident
    assert max(eng.history[("claims", "p95_lag")]) == 4.0


def test_simulated_incidents_detected_and_quiet_run_is_silent():
    incidents = [Incident("pharmacy", 20, 24, error_rate=0.12),
                 Incident("eligibility", 28, 31, volume_factor=0.2)]
    eng = AlertEngine(CFG.feeds)
    fired = {(a.feed, a.metric) for a in feed_in(eng, run_simulation(CFG, minutes=40, incidents=incidents))
             if a.status == "FIRING"}
    assert ("pharmacy", "error_rate") in fired
    assert ("eligibility", "volume") in fired

    for seed in range(1, 6):
        eng = AlertEngine(CFG.feeds)
        assert feed_in(eng, run_simulation(CFG, minutes=60, seed=seed)) == [], seed


def test_alerts_are_stored(tmp_path):
    eng = AlertEngine(CFG.feeds)
    alerts = feed_in(eng, [rec(m, errors=9.0) for m in range(3)] + [rec(3)])
    store = KpiStore(str(tmp_path / "kpi.db"))
    store.write_alerts(alerts)
    store.write_alerts(alerts)                                        # replay is idempotent
    assert [a["status"] for a in store.alerts("claims")] == ["FIRING", "RESOLVED"]
    path, n = store.export_alerts_csv(tmp_path / "alerts.csv")
    assert n == 2 and path.exists()
    store.close()


class FakeResponse:
    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


def test_slack_payload(monkeypatch):
    sent = {}

    def fake_urlopen(req, timeout):
        sent["body"] = json.loads(req.data)
        return FakeResponse()

    monkeypatch.setattr("urllib.request.urlopen", fake_urlopen)
    eng = AlertEngine(CFG.feeds)
    alerts = feed_in(eng, [rec(m, errors=9.0) for m in range(2)])
    assert notify_slack(alerts, webhook_url="https://hooks.example.com/x") is True
    assert "FIRING" in sent["body"]["text"] and "claims error_rate" in sent["body"]["text"]


def test_slack_skipped_without_url(monkeypatch):
    monkeypatch.delenv("SLACK_WEBHOOK_URL", raising=False)
    assert notify_slack([object()]) is False


def test_slack_failure_does_not_raise(monkeypatch):
    def boom(req, timeout):
        raise OSError("connection refused")

    monkeypatch.setattr("urllib.request.urlopen", boom)
    eng = AlertEngine(CFG.feeds)
    alerts = feed_in(eng, [rec(m, errors=9.0) for m in range(2)])
    assert notify_slack(alerts, webhook_url="https://hooks.example.com/x") is False


@pytest.mark.parametrize("confirm", [1, 3])
def test_confirm_windows_setting(confirm):
    eng = AlertEngine(CFG.feeds, confirm_windows=confirm)
    out = feed_in(eng, [rec(m, errors=9.0) for m in range(confirm)])
    assert len(out) == 1

import pytest

from dq_stream.config import load_config


def test_loads_all_feeds():
    cfg = load_config()
    assert set(cfg.feeds) == {"claims", "eligibility", "pharmacy"}
    assert cfg.window_seconds == 60


def test_feed_threshold_overrides_default():
    cfg = load_config()
    assert cfg.feeds["pharmacy"].thresholds.max_error_rate_pct == 1.0
    assert cfg.feeds["claims"].thresholds.max_error_rate_pct == 2.0
    # untouched values still come from defaults
    assert cfg.feeds["pharmacy"].thresholds.min_completeness_pct == 98.0


def test_feed_for_topic():
    cfg = load_config()
    assert cfg.feed_for_topic("dq.claims").name == "claims"
    with pytest.raises(KeyError):
        cfg.feed_for_topic("dq.unknown")


def test_missing_required_key(tmp_path):
    bad = tmp_path / "feeds.yaml"
    bad.write_text("feeds:\n  x:\n    topic: t\n")
    with pytest.raises(ValueError):
        load_config(bad)

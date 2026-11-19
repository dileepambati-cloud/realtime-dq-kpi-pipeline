from datetime import datetime, timezone

import pytest

from dq_stream.config import load_config
from dq_stream.events import generate, make_event
from dq_stream.validator import parse_ts, validate_event

CFG = load_config()


@pytest.mark.parametrize("feed", ["claims", "eligibility", "pharmacy"])
def test_clean_events_pass_validation(feed):
    events = generate(feed, 200, seed=1, error_rate=0.0, late_rate=0.0)
    results = [validate_event(e, CFG.feeds[feed]) for e in events]
    assert all(r.is_complete and r.is_valid for r in results)


def test_envelope_shape():
    e = make_event("claims")
    assert set(e) == {"event_id", "feed", "event_time", "payload"}
    assert parse_ts(e["event_time"]).tzinfo is not None


def test_same_seed_same_events():
    now = datetime(2026, 11, 19, 12, 0, tzinfo=timezone.utc)
    assert generate("pharmacy", 20, seed=7, now=now) == generate("pharmacy", 20, seed=7, now=now)


def test_error_rate_produces_roughly_that_many_bad_events():
    events = generate("claims", 2000, seed=3, error_rate=0.10, late_rate=0.0)
    bad = sum(1 for e in events
              if not (r := validate_event(e, CFG.feeds["claims"])).is_complete or not r.is_valid)
    # each corruption breaks the record, so expect ~10% (allow for randomness)
    assert 140 <= bad <= 260


def test_late_events_are_in_the_past():
    now = datetime(2026, 11, 19, 12, 0, tzinfo=timezone.utc)
    events = generate("claims", 300, seed=5, late_rate=1.0, now=now)
    assert all(parse_ts(e["event_time"]) < now for e in events)


def test_unknown_feed():
    with pytest.raises(ValueError):
        make_event("lab_results")

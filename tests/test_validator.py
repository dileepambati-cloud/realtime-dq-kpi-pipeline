from datetime import date

from dq_stream.config import load_config
from dq_stream.validator import parse_ts, validate_event

CFG = load_config()
TODAY = date(2026, 11, 19)


def claim(**overrides):
    payload = {
        "claim_id": "C0000001",
        "member_id": "M00042",
        "claim_amount": 250.0,
        "status": "PAID",
        "service_date": "2026-11-01",
    }
    payload.update(overrides)
    return {"event_id": "e1", "feed": "claims", "event_time": "2026-11-19T10:00:00+00:00",
            "payload": payload}


def check(event):
    return validate_event(event, CFG.feeds["claims"], today=TODAY)


def test_good_event():
    r = check(claim())
    assert r.is_complete and r.is_valid
    assert r.missing == [] and r.errors == []


def test_missing_and_blank_count_as_incomplete():
    r = check(claim(member_id=None, status="  "))
    assert sorted(r.missing) == ["member_id", "status"]
    assert r.errors == []           # blanks aren't double counted as errors


def test_number_rules():
    assert "below min" in check(claim(claim_amount=-5)).errors[0]
    assert "above max" in check(claim(claim_amount=250000)).errors[0]
    assert "not a number" in check(claim(claim_amount="abc")).errors[0]
    assert check(claim(claim_amount="99.5")).is_valid      # numeric strings are fine


def test_allowed_values_and_pattern():
    r = check(claim(status="VOID", member_id="MBR-1"))
    assert len(r.errors) == 2
    assert any("status" in e for e in r.errors)
    assert any("member_id" in e for e in r.errors)


def test_dates():
    assert "future" in check(claim(service_date="2027-01-01")).errors[0]
    assert "bad date" in check(claim(service_date="11/01/2026")).errors[0]


def test_bad_event_time_is_an_error():
    e = claim()
    e["event_time"] = "yesterday"
    r = check(e)
    assert not r.is_valid and r.event_time is None


def test_missing_payload_does_not_crash():
    r = check({"event_id": "x", "event_time": "2026-11-19T10:00:00Z"})
    assert len(r.missing) == 5


def test_parse_ts():
    assert parse_ts("2026-11-19T10:00:00Z").utcoffset().total_seconds() == 0
    assert parse_ts("2026-11-19T10:00:00").tzinfo is not None    # naive -> UTC
    assert parse_ts(None) is None
    assert parse_ts("not a time") is None

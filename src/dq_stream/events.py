"""
Synthetic healthcare events for testing the pipeline.

Every event has the same envelope:

    {"event_id": "...", "feed": "claims", "event_time": "2026-11-19T15:04:05+00:00",
     "payload": {...feed specific fields...}}

`error_rate` and `late_rate` control how many bad / delayed events are
produced, so you can simulate a feed going bad and watch the KPIs move.
All values are random - no real member or claim data.
"""
import random
import uuid
from datetime import datetime, timedelta, timezone

PLANS = ["PPO", "HMO", "EPO"]
STATUSES = ["PAID", "PAID", "PAID", "DENIED", "PENDING"]


def utcnow():
    return datetime.now(timezone.utc)


def _member_id(rng):
    return f"M{rng.randint(1, 99999):05d}"


def _recent_date(rng, days=60):
    return (utcnow().date() - timedelta(days=rng.randint(0, days))).isoformat()


def _claims(rng):
    return {
        "claim_id": f"C{rng.randint(1, 9_999_999):07d}",
        "member_id": _member_id(rng),
        "claim_amount": round(rng.uniform(20, 5000), 2),
        "status": rng.choice(STATUSES),
        "service_date": _recent_date(rng),
    }


def _eligibility(rng):
    return {
        "member_id": _member_id(rng),
        "plan_code": rng.choice(PLANS),
        "effective_date": _recent_date(rng, 365),
    }


def _pharmacy(rng):
    return {
        "rx_id": f"RX{rng.randint(1, 9_999_999):07d}",
        "member_id": _member_id(rng),
        "ndc_code": f"{rng.randint(10000, 99999)}-{rng.randint(1000, 9999)}-{rng.randint(10, 99)}",
        "quantity": rng.randint(1, 120),
        "fill_date": _recent_date(rng, 30),
    }


BUILDERS = {"claims": _claims, "eligibility": _eligibility, "pharmacy": _pharmacy}


# ways a record can go wrong - each takes a payload and breaks it in place
def _drop_field(payload, rng):
    payload[rng.choice(list(payload))] = None


def _blank_field(payload, rng):
    payload[rng.choice(list(payload))] = ""


def _bad_value(payload, rng):
    if "claim_amount" in payload:
        payload["claim_amount"] = -abs(payload["claim_amount"])
    elif "quantity" in payload:
        payload["quantity"] = 0
    elif "plan_code" in payload:
        payload["plan_code"] = "XYZ"


def _future_date(payload, rng):
    for key in ("service_date", "fill_date", "effective_date"):
        if key in payload:
            payload[key] = (utcnow().date() + timedelta(days=rng.randint(5, 400))).isoformat()
            return


def _bad_member(payload, rng):
    payload["member_id"] = f"MBR-{rng.randint(1, 999)}"


CORRUPTIONS = [_drop_field, _blank_field, _bad_value, _future_date, _bad_member]


def make_event(feed, rng=None, error_rate=0.0, late_rate=0.0, max_late_s=900, now=None):
    rng = rng or random.Random()
    if feed not in BUILDERS:
        raise ValueError(f"unknown feed {feed}. options: {sorted(BUILDERS)}")

    payload = BUILDERS[feed](rng)
    if rng.random() < error_rate:
        rng.choice(CORRUPTIONS)(payload, rng)

    event_time = now or utcnow()
    if rng.random() < late_rate:
        # event happened a while ago but is only arriving now
        event_time = event_time - timedelta(seconds=rng.randint(60, max_late_s))

    return {
        "event_id": str(uuid.UUID(int=rng.getrandbits(128))),
        "feed": feed,
        "event_time": event_time.isoformat(),
        "payload": payload,
    }


def generate(feed, n, seed=None, **kwargs):
    rng = random.Random(seed)
    return [make_event(feed, rng=rng, **kwargs) for _ in range(n)]

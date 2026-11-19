"""
Per-event checks.

validate_event() looks at one event and returns a ValidationResult with
two kinds of problems:

  missing - required fields that are absent / null / blank  -> completeness KPI
  errors  - fields that are present but wrong               -> error-rate KPI

It never raises on bad data. A broken event is a data quality finding,
not a reason to stop the consumer.
"""
import re
from dataclasses import dataclass, field
from datetime import date, datetime, timezone


@dataclass
class ValidationResult:
    event_id: str
    feed: str
    event_time: datetime | None
    missing: list = field(default_factory=list)
    errors: list = field(default_factory=list)

    @property
    def is_complete(self):
        return not self.missing

    @property
    def is_valid(self):
        return not self.errors and self.event_time is not None


def _blank(v):
    return v is None or (isinstance(v, str) and v.strip() == "")


def parse_ts(value):
    """ISO timestamp -> aware datetime (UTC assumed if no offset). None if unparseable."""
    if not isinstance(value, str):
        return None
    try:
        ts = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    return ts if ts.tzinfo else ts.replace(tzinfo=timezone.utc)


def _check_field(name, value, rule, today):
    """Returns an error string, or None if the value passes."""
    kind = rule.get("type")

    if kind == "number":
        if isinstance(value, bool):
            return f"{name}: not a number"
        try:
            num = float(value)
        except (TypeError, ValueError):
            return f"{name}: not a number ({value!r})"
        if "min" in rule and num < rule["min"]:
            return f"{name}: {num} below min {rule['min']}"
        if "max" in rule and num > rule["max"]:
            return f"{name}: {num} above max {rule['max']}"

    if kind == "date":
        try:
            d = date.fromisoformat(str(value)[:10])
        except ValueError:
            return f"{name}: bad date ({value!r})"
        if rule.get("not_future") and d > today:
            return f"{name}: date {d} is in the future"

    if "allowed" in rule and value not in rule["allowed"]:
        return f"{name}: {value!r} not in {rule['allowed']}"

    if "pattern" in rule and not re.fullmatch(rule["pattern"], str(value)):
        return f"{name}: {value!r} does not match {rule['pattern']}"

    return None


def validate_event(event, feed_cfg, today=None):
    today = today or datetime.now(timezone.utc).date()
    payload = event.get("payload") or {}
    result = ValidationResult(
        event_id=str(event.get("event_id", "")),
        feed=feed_cfg.name,
        event_time=parse_ts(event.get("event_time")),
    )

    if result.event_time is None:
        result.errors.append(f"event_time: missing or unparseable ({event.get('event_time')!r})")

    for name in feed_cfg.required:
        if _blank(payload.get(name)):
            result.missing.append(name)

    for name, rule in feed_cfg.rules.items():
        value = payload.get(name)
        if _blank(value):
            continue  # already counted as missing if it's required
        err = _check_field(name, value, rule, today)
        if err:
            result.errors.append(err)

    return result

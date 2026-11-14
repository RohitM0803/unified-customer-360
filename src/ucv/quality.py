"""Schema, timestamp, decimal, and record-quality validation."""
import re
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from .schema import SOURCE_COLUMNS


def schema_problem(headers, entity):
    expected = SOURCE_COLUMNS[entity]
    if headers is None or len(headers) != len(set(headers)) or set(headers) != set(expected):
        return f"schema mismatch: expected {list(expected)}, got {headers}"
    return None


def parse_timestamp(value):
    raw = (value or "").strip()
    if not raw:
        raise ValueError("timestamp is required")
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError("invalid ISO-8601 timestamp") from exc
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def iso_timestamp(value):
    parsed = parse_timestamp(value) if not isinstance(value, datetime) else value.astimezone(timezone.utc)
    return parsed.isoformat(timespec="microseconds").replace("+00:00", "Z")


def parse_amount(value):
    raw = (value or "").strip()
    if not raw:
        raise ValueError("amount is required")
    try:
        amount = Decimal(raw)
    except InvalidOperation as exc:
        raise ValueError("amount must be a decimal") from exc
    if not amount.is_finite():
        raise ValueError("amount must be finite")
    scale = max(0, -amount.as_tuple().exponent)
    integer_digits = max(0, amount.adjusted() + 1)
    if scale > 9 or integer_digits > 29:
        raise ValueError("amount exceeds supported DECIMAL(38,9) range")
    return format(amount.normalize(), "f")


def required_key(value, field):
    result = (value or "").strip()
    if not result:
        raise ValueError(f"missing required field: {field}")
    return result

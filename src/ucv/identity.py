"""Deterministic customer identity rules and evidence helpers."""
import hashlib
import re

EMAIL_RE = re.compile(r"^[^\s@]+@[^\s@]+\.[^\s@]+$")


def normalize_email(value):
    value = (value or "").strip().casefold()
    return value if value and EMAIL_RE.fullmatch(value) else None


def normalize_name(value):
    return " ".join((value or "").casefold().split())


def stable_id(prefix, value):
    return prefix + "_" + hashlib.sha256(value.encode("utf-8")).hexdigest()[:24]


def payload_hash(row):
    import json
    return hashlib.sha256(json.dumps(row, ensure_ascii=False, sort_keys=True).encode("utf-8")).hexdigest()


def customer_key(source_system, source_customer_id, email):
    if email:
        return "email:" + email, "exact_normalized_email"
    return f"source:{source_system}:{source_customer_id}", "source_system_customer_id"

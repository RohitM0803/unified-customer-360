"""Streaming, fixed-seed synthetic data generator with exact total-row targets."""
import argparse
import csv
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path

from .schema import SOURCE_COLUMNS

PRESETS = {"small": 100, "medium": 10_000, "large": 100_000}
FIRST_NAMES = ("Avery", "Jordan", "Morgan", "Casey", "Taylor", "Riley", "Cameron", "Quinn")
LAST_NAMES = ("Patel", "Kim", "Garcia", "Nguyen", "Brown", "Shah", "Wilson", "Singh")
EVENT_TYPES = ("call", "email", "meeting", "support_case", "webinar")


def entity_distribution(total_rows):
    if total_rows < 4:
        raise ValueError("total_rows must be at least 4 to include all four entities")
    base, remainder = divmod(total_rows, 4)
    counts = {entity: base for entity in SOURCE_COLUMNS}
    for entity in list(SOURCE_COLUMNS)[:remainder]:
        counts[entity] += 1
    if counts["customers"] == 0:
        raise ValueError("dataset must include customers")
    return counts


def _customer(index):
    first = FIRST_NAMES[index % len(FIRST_NAMES)]
    last = LAST_NAMES[(index // len(FIRST_NAMES)) % len(LAST_NAMES)]
    return {
        "customer_id": f"c{index:08d}",
        "name": f"{first} {last} {index:06d}",
        "email": f"customer{index}@example.test",
        "phone": f"+1-555-{index % 1000:03d}-{index % 10000:04d}",
    }


def _make_rows(entity, count, customer_count, rng):
    base_time = datetime(2021, 1, 1, tzinfo=timezone.utc)
    for index in range(count):
        customer_index = index % customer_count
        customer_id = f"c{customer_index:08d}"
        if entity == "customers":
            row = _customer(index)
        elif entity == "accounts":
            row = {"account_id": f"a{index:09d}", "customer_id": customer_id,
                   "account_name": f"{FIRST_NAMES[index % 8]} {LAST_NAMES[(index // 8) % 8]} Business {index:07d}",
                   "status": ("active", "inactive", "pending")[rng.randrange(3)]}
        elif entity == "orders":
            occurred = base_time + timedelta(days=rng.randrange(2200), seconds=rng.randrange(86400))
            row = {"order_id": f"o{index:09d}", "customer_id": customer_id,
                   "order_ts": occurred.isoformat().replace("+00:00", "Z"),
                   "amount": f"{rng.randrange(100, 5_000_000) / 100:.2f}"}
        else:
            occurred = base_time + timedelta(days=rng.randrange(2200), seconds=rng.randrange(86400))
            row = {"event_id": f"e{index:09d}", "customer_id": customer_id,
                   "event_ts": occurred.isoformat().replace("+00:00", "Z"),
                   "event_type": EVENT_TYPES[rng.randrange(len(EVENT_TYPES))],
                   "details": f"Synthetic contact event {index:08d}"}
        yield row


def _apply_messy_cases(entity, rows):
    n = len(rows)
    if n < 8:
        return
    if entity == "customers":
        rows[0]["phone"] = ""
        rows[1] = dict(rows[0])  # exact same-batch replay of customer 0
        rows[2] = dict(rows[0], name="Conflicting Duplicate Customer")  # conflicting key
        rows[3]["email"] = rows[0]["email"]
        rows[3]["name"] = "Different Person With Shared Email"
        rows[-1]["customer_id"] = ""  # missing required key
    elif entity == "accounts":
        rows[-1]["customer_id"] = "missing-customer-reference"
        rows[-2]["account_id"] = ""
    elif entity == "orders":
        rows[-1]["amount"] = "not-a-decimal"
        rows[-2]["order_id"] = rows[0]["order_id"]  # repeated key in same batch
    elif entity == "interactions":
        rows[-1]["event_ts"] = "not-an-iso-timestamp"
        rows[-2]["event_id"] = rows[0]["event_id"]


def generate_dataset(out_dir, total_rows, seed=1729, mode="clean", overwrite=False):
    if mode not in ("clean", "messy", "schema_drift"):
        raise ValueError("mode must be clean, messy, or schema_drift")
    counts = entity_distribution(total_rows)
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    paths = [out_dir / f"{entity}.csv" for entity in SOURCE_COLUMNS]
    existing = [p for p in paths if p.exists()]
    if existing and not overwrite:
        raise FileExistsError(f"Refusing to overwrite generated data: {existing[0]}; choose a new output path or pass --overwrite")
    rng = random.Random(seed)
    # The largest relationship dimension is customer count; all child references are deterministic.
    for entity, columns in SOURCE_COLUMNS.items():
        entity_seed = seed ^ (sum(map(ord, entity)) * 1_000_003)
        entity_rng = random.Random(entity_seed)
        relationship_customer_count = counts["customers"]
        if mode == "messy" and entity != "customers" and counts[entity] >= 8:
            # Keep deliberately malformed child values reachable by their own
            # validators; reference one of the customer's valid source IDs.
            while ((counts[entity] - 1) % relationship_customer_count < 4
                   or (counts[entity] - 1) % relationship_customer_count
                   == counts["customers"] - 1):
                relationship_customer_count -= 1
        rows = list(_make_rows(entity, counts[entity], relationship_customer_count, entity_rng))
        if mode == "messy":
            _apply_messy_cases(entity, rows)
        if entity != "customers":
            rng.shuffle(rows)  # deterministic late and out-of-order child events
        path = out_dir / f"{entity}.csv"
        open_mode = "w" if overwrite else "x"
        with path.open(open_mode, encoding="utf-8", newline="") as stream:
            writer = csv.DictWriter(stream, fieldnames=columns, extrasaction="raise")
            writer.writeheader()
            writer.writerows(rows)
    if mode == "schema_drift":
        path = out_dir / "accounts.csv"
        content = path.read_text(encoding="utf-8")
        content = content.replace("account_id,customer_id,account_name,status",
                                  "account_id,customer_id,unexpected_column,status", 1)
        path.write_text(content, encoding="utf-8")
    manifest = {"seed": seed, "mode": mode, "total_rows": sum(counts.values()),
                "entity_counts": counts, "out_dir": str(out_dir.resolve())}
    manifest_path = out_dir / "manifest.json"
    if manifest_path.exists() and not overwrite:
        raise FileExistsError(f"Refusing to overwrite manifest: {manifest_path}")
    import json
    manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    return manifest


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--size", choices=PRESETS, default="large")
    parser.add_argument("--total-rows", type=int)
    parser.add_argument("--mode", choices=("clean", "messy", "schema_drift"), default="clean")
    parser.add_argument("--seed", type=int, default=1729)
    parser.add_argument("--out", type=Path)
    parser.add_argument("--overwrite", action="store_true")
    args = parser.parse_args(argv)
    total = args.total_rows if args.total_rows is not None else PRESETS[args.size]
    output = args.out or Path("data/generated") / f"{args.size}_{args.mode}"
    print(generate_dataset(output, total, args.seed, args.mode, args.overwrite))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

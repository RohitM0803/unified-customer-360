"""Validation, standardization, identity resolution, and Silver persistence."""
import json
from datetime import datetime, timezone
from .bronze import iter_bronze, validate_bronze_schemas
from ..identity import customer_key, normalize_email, normalize_name, payload_hash, stable_id
from ..quality import iso_timestamp, parse_amount, required_key
from ..schema import SOURCE_COLUMNS


def _reject(conn, run_id, system, file_name, rownum, entity, reason, raw):
    reject_id = stable_id("reject", f"{run_id}|{file_name}|{rownum}|{reason}")
    conn.execute("INSERT INTO rejected_record VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                 [reject_id, run_id, system, file_name, rownum, entity, reason,
                  json.dumps(raw, ensure_ascii=False, sort_keys=True)])


def _issue(conn, run_id, system, file_name, rownum, entity, severity, reason, raw):
    issue_id = stable_id("issue", f"{run_id}|{file_name}|{rownum}|{reason}")
    conn.execute("INSERT INTO quality_issue VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                 [issue_id, run_id, system, file_name, rownum, entity, severity, reason,
                  json.dumps(raw, ensure_ascii=False, sort_keys=True)])


def _count(counts, entity, outcome):
    counts[outcome] += 1
    bucket = counts.setdefault("entity_counts", {}).setdefault(entity, {
        "inserted_rows": 0, "skipped_existing_rows": 0,
        "skipped_duplicate_batch_rows": 0, "rejected_rows": 0,
    })
    bucket[outcome] += 1


def _process_customers(conn, run_id, source_system, counts, seen, invalid_ids,
                       source_map, email_index, name_index, customer_rows, source_rows):
    for file_name, rownum, system, raw_json in iter_bronze(conn, run_id, "customers"):
        raw = json.loads(raw_json)
        try:
            customer_id = required_key(raw.get("customer_id"), "customer_id")
        except ValueError as exc:
            _reject(conn, run_id, system, file_name, rownum, "customers", str(exc), raw)
            _count(counts, "customers", "rejected_rows")
            continue
        fingerprint = payload_hash(raw)
        batch_key = (system, customer_id)
        if batch_key in seen:
            if seen[batch_key] == fingerprint:
                _count(counts, "customers", "skipped_duplicate_batch_rows")
            else:
                _reject(conn, run_id, system, file_name, rownum, "customers",
                        "conflicting duplicate customer key within input batch", raw)
                _count(counts, "customers", "rejected_rows")
                invalid_ids.add(batch_key)
            continue
        seen[batch_key] = fingerprint
        existing = source_map.get(batch_key)
        if existing:
            if existing[0] == fingerprint:
                _count(counts, "customers", "skipped_existing_rows")
            else:
                _reject(conn, run_id, system, file_name, rownum, "customers",
                        "conflicting duplicate customer key already loaded", raw)
                _count(counts, "customers", "rejected_rows")
                invalid_ids.add(batch_key)
            continue

        raw_email = (raw.get("email") or "").strip()
        email = normalize_email(raw_email)
        if raw_email and not email:
            _issue(conn, run_id, system, file_name, rownum, "customers", "warning",
                   "invalid email; source identity fallback used", raw)
        elif not email:
            _issue(conn, run_id, system, file_name, rownum, "customers", "info",
                   "email missing; source identity fallback used", raw)
        phone = (raw.get("phone") or "").strip() or None
        if not phone:
            _issue(conn, run_id, system, file_name, rownum, "customers", "info",
                   "phone missing", raw)
        name = (raw.get("name") or "").strip() or None
        name_norm = normalize_name(name)
        match_key, method = customer_key(system, customer_id, email)
        by_email = email_index.get(email, []) if email else []
        by_name = name_index.get(name_norm, []) if name_norm else []
        matching_sk = None
        candidate_sks = set()
        reason = None
        for candidate in by_email:
            if not name_norm or not candidate[1] or candidate[1] == name_norm:
                matching_sk = candidate[0]
                method = "exact_normalized_email"
                break
            candidate_sks.add(candidate[0])
            reason = "email matches but non-empty normalized names conflict"
        if not matching_sk:
            for candidate in by_name:
                if candidate[0] != matching_sk:
                    candidate_sks.add(candidate[0])
                    reason = reason or "normalized name matches but identity keys differ"
        uncertain = bool(candidate_sks)
        if matching_sk:
            customer_sk = matching_sk
        else:
            if uncertain or method == "source_system_customer_id":
                match_key = f"source:{system}:{customer_id}"
                method = "source_system_customer_id"
            customer_sk = stable_id("cust", match_key)
            customer_rows.append(
                [customer_sk, match_key, name, name_norm, email, phone, method, uncertain,
                 datetime.now(timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z")])
            if email:
                email_index.setdefault(email, []).append((customer_sk, name_norm))
            if name_norm:
                name_index.setdefault(name_norm, []).append((customer_sk, email, name_norm))
        if uncertain:
            conn.execute("UPDATE silver_customer SET uncertain_match=TRUE WHERE customer_sk=?",
                         [customer_sk])
            for candidate_sk in sorted(candidate_sks):
                conn.execute("UPDATE silver_customer SET uncertain_match=TRUE WHERE customer_sk=?",
                             [candidate_sk])
                review_id = stable_id("review", f"{run_id}|{system}|{customer_id}|{candidate_sk}")
                conn.execute("INSERT INTO uncertain_match_review VALUES (?, ?, ?, ?, ?, ?, ?, 'pending', ?)",
                    [review_id, run_id, system, customer_id, customer_sk, candidate_sk,
                     reason or "identity requires review",
                     datetime.now(timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z")])
        source_rows.append(
            [system, customer_id, customer_sk, fingerprint, file_name, rownum, run_id])
        source_map[batch_key] = (fingerprint, customer_sk)
        _count(counts, "customers", "inserted_rows")


def _process_children(conn, run_id, source_system, entity, counts, seen, invalid_ids,
                      customer_refs, existing_hashes, insert_rows):
    names = {
        "accounts": ("account_id", "silver_account", "account_id"),
        "orders": ("order_id", "silver_order", "order_id"),
        "interactions": ("event_id", "silver_interaction", "event_id"),
    }
    keyfield, table, key_column = names[entity]
    for file_name, rownum, system, raw_json in iter_bronze(conn, run_id, entity):
        raw = json.loads(raw_json)
        try:
            key = required_key(raw.get(keyfield), keyfield)
            customer_id = required_key(raw.get("customer_id"), "customer_id")
        except ValueError as exc:
            _reject(conn, run_id, system, file_name, rownum, entity, str(exc), raw)
            _count(counts, entity, "rejected_rows")
            continue
        source_key = (system, customer_id)
        if source_key in invalid_ids:
            _reject(conn, run_id, system, file_name, rownum, entity,
                    "customer source identity is conflicting in this batch", raw)
            _count(counts, entity, "rejected_rows")
            continue
        customer_sk = customer_refs.get(source_key)
        if customer_sk is None:
            _reject(conn, run_id, system, file_name, rownum, entity,
                    "invalid customer reference", raw)
            _count(counts, entity, "rejected_rows")
            continue
        row = dict(raw)
        if entity == "accounts":
            values = [(row.get("account_name") or "").strip() or None,
                      (row.get("status") or "").strip() or None]
        elif entity == "orders":
            try:
                order_ts = iso_timestamp(row.get("order_ts"))
                amount = parse_amount(row.get("amount"))
            except ValueError as exc:
                _reject(conn, run_id, system, file_name, rownum, entity, str(exc), raw)
                _count(counts, entity, "rejected_rows")
                continue
            values = [order_ts, amount]
        else:
            try:
                event_ts = iso_timestamp(row.get("event_ts"))
                event_type = required_key(row.get("event_type"), "event_type")
            except ValueError as exc:
                _reject(conn, run_id, system, file_name, rownum, entity, str(exc), raw)
                _count(counts, entity, "rejected_rows")
                continue
            values = [event_ts, event_type, (row.get("details") or "").strip() or None]

        fingerprint = payload_hash(raw)
        batch_key = (system, key)
        if batch_key in seen[entity]:
            if seen[entity][batch_key] == fingerprint:
                _count(counts, entity, "skipped_duplicate_batch_rows")
            else:
                _reject(conn, run_id, system, file_name, rownum, entity,
                        f"conflicting duplicate {keyfield} within input batch", raw)
                _count(counts, entity, "rejected_rows")
            continue
        seen[entity][batch_key] = fingerprint
        existing = existing_hashes.get(batch_key)
        if existing:
            if existing == fingerprint:
                _count(counts, entity, "skipped_existing_rows")
            else:
                _reject(conn, run_id, system, file_name, rownum, entity,
                        f"conflicting duplicate {keyfield} already loaded", raw)
                _count(counts, entity, "rejected_rows")
            continue
        common = [system, key, customer_sk]
        if entity == "accounts":
            params = common + values + [fingerprint, file_name, rownum, run_id]
        elif entity == "orders":
            params = common + values + [fingerprint, file_name, rownum, run_id]
        else:
            params = common + values + [fingerprint, file_name, rownum, run_id]
        insert_rows.append(params)
        existing_hashes[batch_key] = fingerprint
        _count(counts, entity, "inserted_rows")


def transform_silver(conn, run_id, source_system):
    """Validate the Bronze contract and persist accepted rows to Silver."""
    validate_bronze_schemas(conn, run_id)
    counts = {"input_rows": conn.execute(
        "SELECT count(*) FROM bronze_record WHERE run_id=?", [run_id]).fetchone()[0],
        "inserted_rows": 0, "skipped_existing_rows": 0,
        "skipped_duplicate_batch_rows": 0, "rejected_rows": 0}
    counts["entity_counts"] = {
        entity: {"input_rows": row_count, "inserted_rows": 0,
                 "skipped_existing_rows": 0, "skipped_duplicate_batch_rows": 0,
                 "rejected_rows": 0}
        for entity, row_count in conn.execute(
            "SELECT entity,count(*) FROM bronze_record WHERE run_id=? GROUP BY entity",
            [run_id]).fetchall()
    }
    source_map = {
        (row[0], row[1]): (row[2], row[3])
        for row in conn.execute(
            "SELECT source_system,source_customer_id,payload_hash,customer_sk FROM silver_customer_source").fetchall()
    }
    customer_refs = {key: value[1] for key, value in source_map.items()}
    email_index, name_index = {}, {}
    for row in conn.execute("SELECT customer_sk,email,name_norm FROM silver_customer").fetchall():
        if row[1]:
            email_index.setdefault(row[1], []).append((row[0], row[2]))
        if row[2]:
            name_index.setdefault(row[2], []).append((row[0], row[1], row[2]))
    seen_customers = {}
    invalid_ids = set()
    customer_rows, source_rows = [], []
    _process_customers(conn, run_id, source_system, counts, seen_customers, invalid_ids,
                       source_map, email_index, name_index, customer_rows, source_rows)
    if customer_rows:
        conn.executemany("INSERT INTO silver_customer VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", customer_rows)
    if source_rows:
        conn.executemany("INSERT INTO silver_customer_source VALUES (?, ?, ?, ?, ?, ?, ?)", source_rows)
    customer_refs.update({key: value[1] for key, value in source_map.items()})
    seen_children = {entity: {} for entity in ("accounts", "orders", "interactions")}
    existing_hashes = {}
    for entity, table, key_column in (("accounts","silver_account","account_id"),
                                      ("orders","silver_order","order_id"),
                                      ("interactions","silver_interaction","event_id")):
        existing_hashes[entity] = {
            (row[0],row[1]):row[2] for row in conn.execute(
                f"SELECT source_system,{key_column},payload_hash FROM {table}").fetchall()
        }
    for entity in ("accounts", "orders", "interactions"):
        insert_rows = []
        _process_children(conn, run_id, source_system, entity, counts, seen_children,
                          invalid_ids, customer_refs, existing_hashes[entity], insert_rows)
        if insert_rows:
            width = len(insert_rows[0])
            placeholders = ",".join("?" for _ in range(width))
            table = {"accounts": "silver_account", "orders": "silver_order",
                     "interactions": "silver_interaction"}[entity]
            conn.executemany(f"INSERT INTO {table} VALUES ({placeholders})", insert_rows)
    counts["output_rows"] = (counts["inserted_rows"] + counts["skipped_existing_rows"]
                             + counts["skipped_duplicate_batch_rows"])
    counts["accepted_rows"] = counts["inserted_rows"]
    return counts

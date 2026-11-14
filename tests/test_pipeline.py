import csv
import json
import sys
from pathlib import Path
from decimal import Decimal

import duckdb
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from ucv.generator import entity_distribution, generate_dataset
from ucv.pipeline import run_pipeline
from ucv.quality import parse_amount, parse_timestamp
from ucv.identity import normalize_email, stable_id


def make_input(tmp_path, mode="clean", total=100, seed=1729):
    source = tmp_path / f"source_{mode}_{total}"
    manifest = generate_dataset(source, total, seed=seed, mode=mode)
    return source, manifest


def run_once(tmp_path, source, database=None, raw_root=None, source_system="test-system"):
    database = database or tmp_path / "warehouse.duckdb"
    raw_root = raw_root or tmp_path / "raw_archive"
    return run_pipeline(source, database, raw_root, source_system,
                        tmp_path / "reports"), database, raw_root


def test_total_distribution_and_deterministic_generation(tmp_path):
    assert entity_distribution(100) == {
        "customers": 25, "accounts": 25, "orders": 25, "interactions": 25
    }
    first, manifest = make_input(tmp_path, total=100, seed=23)
    second = tmp_path / "second_source"
    generate_dataset(second, 100, seed=23)
    assert manifest["total_rows"] == 100
    for entity in ("customers", "accounts", "orders", "interactions"):
        assert (first / f"{entity}.csv").read_bytes() == (second / f"{entity}.csv").read_bytes()


def test_bronze_silver_gold_reconciliation(tmp_path):
    source, _ = make_input(tmp_path)
    report, database, raw_root = run_once(tmp_path, source)
    assert report["status"] == "succeeded"
    assert report["input_rows"] == report["inserted_rows"] == report["output_rows"] == 100
    assert report["rejected_rows"] == report["skipped_existing_rows"] == 0
    assert report["input_rows_by_source"] == {
        "customers": 25, "accounts": 25, "orders": 25, "interactions": 25,
    }
    assert report["reconciliation"]["balanced"] is True
    assert report["stage_rows"]["bronze"] == 100
    assert report["stage_rows"]["gold"]["gold_customer_360"] == 25
    con = duckdb.connect(str(database), read_only=True)
    expected = {
        "bronze_record": 100, "silver_customer": 25, "silver_account": 25,
        "silver_order": 25, "silver_interaction": 25, "gold_customer_master": 25,
        "gold_customer_360": 25, "gold_customer_account": 25,
        "gold_customer_order_history": 25, "gold_customer_interaction_history": 25,
    }
    for table, count in expected.items():
        assert con.execute(f"select count(*) from {table}").fetchone()[0] == count
    assert con.execute("""select count(*) from silver_account a left join silver_customer c
                          using(customer_sk) where c.customer_sk is null""").fetchone()[0] == 0
    row = con.execute("""select account_count,order_count,interaction_count,total_order_amount,
                         identity_evidence_json from gold_customer_360 limit 1""").fetchone()
    assert row[:3] == (1, 1, 1) and row[3] > 0
    evidence = json.loads(row[4])
    assert evidence["match_method"] == "exact_normalized_email"
    assert evidence["sources"][0]["source_file"] == "customers.csv"
    event_rows = con.execute("select event_ts from silver_interaction order by source_row").fetchall()
    event_times = [value[0] for value in event_rows]
    assert event_times != sorted(event_times)  # late/out-of-order events remain stored as received
    assert con.execute("select max(event_ts) from gold_customer_interaction_history").fetchone()[0] == max(event_times)
    lineage = con.execute("""select bronze_record.source_file,bronze_record.source_row,
                            bronze_record.source_system,bronze_record.run_id,
                            bronze_record.ingested_at,bronze_file.archived_path,bronze_file.sha256
                            from bronze_record join bronze_file using(run_id,source_file) limit 1""").fetchone()
    assert lineage[0] == "customers.csv" and lineage[1] >= 2
    assert lineage[2] == "test-system" and lineage[3] and lineage[4]
    assert Path(lineage[5]).is_file() and len(lineage[6]) == 64
    con.close()
    assert list((raw_root / "test-system").iterdir())


def test_exact_rerun_is_skipped_and_bronze_is_append_only(tmp_path):
    source, _ = make_input(tmp_path)
    database, raw = tmp_path / "warehouse.duckdb", tmp_path / "raw"
    first, _, _ = run_once(tmp_path, source, database, raw)
    second, _, _ = run_once(tmp_path, source, database, raw)
    assert first["inserted_rows"] == 100
    assert second["input_rows"] == second["output_rows"] == 100
    assert second["inserted_rows"] == 0
    assert second["skipped_existing_rows"] == 100
    assert second["rejected_rows"] == 0
    assert second["reconciliation"]["balanced"] is True
    assert second["accepted_rows"] == 0
    assert second["entity_counts"]["orders"]["skipped_existing_rows"] == 25
    con = duckdb.connect(str(database), read_only=True)
    assert con.execute("select count(*) from bronze_record").fetchone()[0] == 200
    assert con.execute("select count(*) from silver_order").fetchone()[0] == 25
    assert con.execute("select count(*) from run_log where status='succeeded'").fetchone()[0] == 2
    con.close()


def test_same_batch_exact_duplicates_skip_conflicts_reject(tmp_path):
    source, _ = make_input(tmp_path)
    path = source / "accounts.csv"
    with path.open(encoding="utf-8", newline="") as stream:
        rows = list(csv.DictReader(stream))
    with path.open("a", encoding="utf-8", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=rows[0].keys())
        writer.writerow(rows[0])
        writer.writerow(dict(rows[0], status="deliberately-conflicting"))
    report, database, _ = run_once(tmp_path, source)
    assert report["input_rows"] == 102
    assert report["inserted_rows"] == 100
    assert report["skipped_duplicate_batch_rows"] == 1
    assert report["rejected_rows"] == 1
    assert report["output_rows"] == 101
    con = duckdb.connect(str(database), read_only=True)
    assert con.execute("select count(*) from silver_account").fetchone()[0] == 25
    assert con.execute("select count(*) from rejected_record").fetchone()[0] == 1
    con.close()


def test_cross_run_conflict_does_not_overwrite(tmp_path):
    source, _ = make_input(tmp_path)
    database, raw = tmp_path / "warehouse.duckdb", tmp_path / "raw"
    run_once(tmp_path, source, database, raw)
    path = source / "orders.csv"
    with path.open(encoding="utf-8", newline="") as stream:
        rows = list(csv.DictReader(stream))
    original = rows[0]["amount"]
    rows[0]["amount"] = "999999.99"
    with path.open("w", encoding="utf-8", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)
    report, _, _ = run_once(tmp_path, source, database, raw)
    assert report["skipped_existing_rows"] == 99
    assert report["rejected_rows"] == 1
    con = duckdb.connect(str(database), read_only=True)
    stored = con.execute("select amount from silver_order where order_id=?",
                         [rows[0]["order_id"]]).fetchone()[0]
    assert Decimal(stored) == Decimal(original)
    con.close()


def test_messy_quality_identity_and_relationship_checks(tmp_path):
    source, manifest = make_input(tmp_path, mode="messy")
    assert manifest["total_rows"] == 100
    report, database, _ = run_once(tmp_path, source)
    assert report["input_rows"] == 100 and report["rejected_rows"] > 0
    assert report["skipped_duplicate_batch_rows"] >= 1
    assert report["output_rows"] + report["rejected_rows"] == report["input_rows"]
    assert report["reconciliation"]["balanced"] is True
    assert "invalid customer reference" in report["rejection_reasons"]
    assert "amount must be a decimal" in report["rejection_reasons"]
    assert "invalid ISO-8601 timestamp" in report["rejection_reasons"]
    con = duckdb.connect(str(database), read_only=True)
    assert con.execute("select count(*) from rejected_record where source_file is null or source_row is null or raw_record_json is null").fetchone()[0] == 0
    assert con.execute("select count(*) from uncertain_match_review where review_status='pending'").fetchone()[0] >= 1
    assert con.execute("select count(*) from silver_customer where uncertain_match").fetchone()[0] >= 1
    assert con.execute("""select count(*) from silver_account a left join silver_customer c
                          using(customer_sk) where c.customer_sk is null""").fetchone()[0] == 0
    con.close()


def test_schema_drift_preserves_bronze_and_records_failed_run(tmp_path):
    source, _ = make_input(tmp_path, mode="schema_drift")
    report, database, _ = run_once(tmp_path, source)
    assert report["status"] == "failed" and "schema mismatch" in report["failure_reason"]
    con = duckdb.connect(str(database), read_only=True)
    assert con.execute("select count(*) from bronze_record").fetchone()[0] == 100
    assert con.execute("select count(*) from run_log where status='failed'").fetchone()[0] == 1
    assert con.execute("select count(*) from rejected_record").fetchone()[0] == 100
    assert con.execute("select count(*) from silver_customer").fetchone()[0] == 0
    con.close()
    assert sum(report["rejection_reasons"].values()) == 100
    assert report["reconciliation"]["balanced"] is True
    assert report["unprocessed_rows"] == 0


def test_failed_gold_publish_recovers_from_bronze(tmp_path, monkeypatch):
    import ucv.pipeline as pipeline
    source, _ = make_input(tmp_path)
    database, raw, reports = tmp_path / "warehouse.duckdb", tmp_path / "raw", tmp_path / "reports"
    original = pipeline.publish_gold
    monkeypatch.setattr(pipeline, "publish_gold",
                        lambda _conn: (_ for _ in ()).throw(RuntimeError("injected failure")))
    failed = pipeline.run_pipeline(source, database, raw, "test-system", reports)
    assert failed["status"] == "failed"
    assert failed["inserted_rows"] == failed["output_rows"] == 0
    assert failed["unprocessed_rows"] == 100
    con = duckdb.connect(str(database), read_only=True)
    assert con.execute("select count(*) from bronze_record").fetchone()[0] == 100
    assert con.execute("select count(*) from silver_customer").fetchone()[0] == 0
    con.close()
    monkeypatch.setattr(pipeline, "publish_gold", original)
    recovered = pipeline.run_pipeline(source, database, raw, "test-system", reports)
    assert recovered["status"] == "succeeded" and recovered["inserted_rows"] == 100
    con = duckdb.connect(str(database), read_only=True)
    assert con.execute("select count(*) from run_log where status='failed'").fetchone()[0] == 1
    assert con.execute("select count(*) from gold_customer_360").fetchone()[0] == 25
    con.close()


def test_failed_republish_preserves_existing_gold(tmp_path, monkeypatch):
    import ucv.pipeline as pipeline

    source, _ = make_input(tmp_path)
    database, raw, reports = tmp_path / "warehouse.duckdb", tmp_path / "raw", tmp_path / "reports"
    first = pipeline.run_pipeline(source, database, raw, "test-system", reports)
    assert first["status"] == "succeeded"
    original_publish = pipeline.publish_gold

    def delete_gold_then_fail(conn):
        conn.execute("DELETE FROM gold_customer_master")
        raise RuntimeError("injected republish failure")

    monkeypatch.setattr(pipeline, "publish_gold", delete_gold_then_fail)
    failed = pipeline.run_pipeline(source, database, raw, "test-system", reports)
    assert failed["status"] == "failed"
    assert failed["accepted_rows"] == 0
    assert failed["unprocessed_rows"] == 100
    assert failed["reconciliation"]["balanced"] is True

    con = duckdb.connect(str(database), read_only=True)
    assert con.execute("select count(*) from gold_customer_360").fetchone()[0] == 25
    con.close()

    monkeypatch.setattr(pipeline, "publish_gold", original_publish)
    recovered = pipeline.run_pipeline(source, database, raw, "test-system", reports)
    assert recovered["status"] == "succeeded"
    assert recovered["inserted_rows"] == 0
    assert recovered["skipped_existing_rows"] == 100
    con = duckdb.connect(str(database), read_only=True)
    assert con.execute("select count(*) from gold_customer_360").fetchone()[0] == 25
    con.close()


def test_empty_batch_and_validation_helpers(tmp_path):
    source = tmp_path / "empty"
    source.mkdir()
    headers = {
        "customers": "customer_id,name,email,phone\n",
        "accounts": "account_id,customer_id,account_name,status\n",
        "orders": "order_id,customer_id,order_ts,amount\n",
        "interactions": "event_id,customer_id,event_ts,event_type,details\n",
    }
    for entity, header in headers.items():
        (source / f"{entity}.csv").write_text(header, encoding="utf-8")
    report, database, _ = run_once(tmp_path, source)
    assert report["status"] == "succeeded"
    assert report["input_rows"] == report["output_rows"] == 0
    con = duckdb.connect(str(database), read_only=True)
    assert con.execute("select count(*) from gold_customer_360").fetchone()[0] == 0
    con.close()
    assert normalize_email(" USER@Example.test ") == "user@example.test"
    assert normalize_email("not-an-email") is None
    assert stable_id("cust", "same") == stable_id("cust", "same")
    assert parse_amount("12.500") == "12.5"
    assert parse_timestamp("2024-01-01T03:00:00+03:00").isoformat() == "2024-01-01T00:00:00+00:00"
    with pytest.raises(ValueError):
        parse_amount("NaN")
    with pytest.raises(ValueError):
        parse_timestamp("not-a-date")


@pytest.mark.integration
def test_large_clean_100k_total(tmp_path):
    source, manifest = make_input(tmp_path, total=100_000, seed=314159)
    assert manifest["entity_counts"] == {
        "customers": 25_000, "accounts": 25_000, "orders": 25_000,
        "interactions": 25_000,
    }
    report, database, _ = run_once(tmp_path, source)
    assert report["status"] == "succeeded"
    assert report["input_rows"] == report["output_rows"] == report["inserted_rows"] == 100_000
    assert report["rejected_rows"] == 0
    con = duckdb.connect(str(database), read_only=True)
    assert con.execute("select count(*) from bronze_record").fetchone()[0] == 100_000
    assert con.execute("select count(*) from gold_customer_360").fetchone()[0] == 25_000
    assert con.execute("select sum(order_count) from gold_customer_360").fetchone()[0] == 25_000
    assert con.execute("select sum(interaction_count) from gold_customer_360").fetchone()[0] == 25_000
    con.close()


class _ConnectionProxy:
    def __init__(self, inner, fail_execute=None):
        self._inner = inner
        self._fail_execute = fail_execute
        self.closed = False

    def execute(self, query, parameters=None):
        normalized = " ".join(query.lower().split())
        if self._fail_execute and self._fail_execute(normalized):
            raise RuntimeError("injected database recording failure")
        if parameters is None:
            return self._inner.execute(query)
        return self._inner.execute(query, parameters)

    def close(self):
        self.closed = True
        return self._inner.close()

    def __getattr__(self, name):
        return getattr(self._inner, name)


def _track_pipeline_connection(monkeypatch, fail_execute=None):
    import ucv.pipeline as pipeline

    real_connect = pipeline.duckdb.connect
    tracked = {}

    def connect(*args, **kwargs):
        proxy = _ConnectionProxy(
            real_connect(*args, **kwargs), fail_execute=fail_execute
        )
        tracked["connection"] = proxy
        return proxy

    monkeypatch.setattr(pipeline.duckdb, "connect", connect)
    return real_connect, tracked


def test_initialization_failure_closes_connection_and_writes_report(
    tmp_path, monkeypatch
):
    import ucv.pipeline as pipeline

    source, _ = make_input(tmp_path)
    database = tmp_path / "warehouse.duckdb"
    _, tracked = _track_pipeline_connection(monkeypatch)
    monkeypatch.setattr(
        pipeline,
        "initialize",
        lambda _conn: (_ for _ in ()).throw(
            RuntimeError("injected initialization failure")
        ),
    )

    report = pipeline.run_pipeline(
        source, database, tmp_path / "raw", "test-system", tmp_path / "reports"
    )

    assert report["status"] == "failed"
    assert report["failure_reason"] == "RuntimeError: injected initialization failure"
    assert tracked["connection"].closed
    assert (tmp_path / "reports" / f'{report["run_id"]}.json').is_file()


def test_initial_run_log_insert_failure_closes_connection_and_writes_report(
    tmp_path, monkeypatch
):
    import ucv.pipeline as pipeline

    source, _ = make_input(tmp_path)
    database = tmp_path / "warehouse.duckdb"
    real_connect, tracked = _track_pipeline_connection(
        monkeypatch,
        fail_execute=lambda sql: sql.startswith("insert into run_log("),
    )

    report = pipeline.run_pipeline(
        source, database, tmp_path / "raw", "test-system", tmp_path / "reports"
    )

    assert report["status"] == "failed"
    assert "injected database recording failure" in report["failure_reason"]
    assert tracked["connection"].closed
    assert (tmp_path / "reports" / f'{report["run_id"]}.json').is_file()

    con = real_connect(str(database), read_only=True)
    try:
        assert con.execute(
            "select count(*) from run_log where run_id=?", [report["run_id"]]
        ).fetchone()[0] == 0
    finally:
        con.close()


def test_partial_bronze_failure_counts_persisted_rows(tmp_path, monkeypatch):
    import ucv.pipeline as pipeline

    source, _ = make_input(tmp_path)
    original_ingest = pipeline.ingest_bronze

    def ingest_then_fail(conn, *args):
        original_ingest(conn, *args)
        conn.commit()
        raise RuntimeError("injected post-Bronze failure")

    monkeypatch.setattr(pipeline, "ingest_bronze", ingest_then_fail)
    report, database, _ = run_once(tmp_path, source)

    assert report["status"] == "failed"
    assert report["failure_reason"] == "RuntimeError: injected post-Bronze failure"
    assert report["input_rows"] == 100
    assert report["output_rows"] == 0
    assert report["unprocessed_rows"] == 100

    con = duckdb.connect(str(database), read_only=True)
    try:
        assert con.execute(
            "select count(*) from bronze_record where run_id=?", [report["run_id"]]
        ).fetchone()[0] == 100
    finally:
        con.close()


def test_silver_failure_rolls_back_silver_and_gold(tmp_path, monkeypatch):
    import ucv.pipeline as pipeline

    source, _ = make_input(tmp_path)
    original_transform = pipeline.transform_silver

    def transform_then_fail(conn, *args):
        original_transform(conn, *args)
        raise RuntimeError("injected post-Silver failure")

    monkeypatch.setattr(pipeline, "transform_silver", transform_then_fail)
    report, database, _ = run_once(tmp_path, source)

    assert report["status"] == "failed"
    assert report["failure_reason"] == "RuntimeError: injected post-Silver failure"
    assert report["input_rows"] == 100
    assert report["inserted_rows"] == report["output_rows"] == 0
    assert report["unprocessed_rows"] == 100

    con = duckdb.connect(str(database), read_only=True)
    try:
        assert con.execute("select count(*) from bronze_record").fetchone()[0] == 100
        assert con.execute("select count(*) from silver_customer").fetchone()[0] == 0
        assert con.execute("select count(*) from gold_customer_360").fetchone()[0] == 0
    finally:
        con.close()


def test_failure_recording_error_does_not_mask_original_or_report(
    tmp_path, monkeypatch
):
    import ucv.pipeline as pipeline

    source, _ = make_input(tmp_path)
    database = tmp_path / "warehouse.duckdb"
    real_connect, tracked = _track_pipeline_connection(
        monkeypatch,
        fail_execute=lambda sql: (
            sql.startswith("update run_log") and "status='failed'" in sql
        ),
    )
    monkeypatch.setattr(
        pipeline,
        "transform_silver",
        lambda *_args: (_ for _ in ()).throw(RuntimeError("original Silver failure")),
    )

    report = pipeline.run_pipeline(
        source, database, tmp_path / "raw", "test-system", tmp_path / "reports"
    )

    assert report["status"] == "failed"
    assert report["failure_reason"] == "RuntimeError: original Silver failure"
    assert report["failure_stage"] == "Silver processing"
    assert "Traceback (most recent call last):" in report["failure_traceback"]
    assert "RuntimeError: original Silver failure" in report["failure_traceback"]
    assert tracked["connection"].closed
    report_path = tmp_path / "reports" / f'{report["run_id"]}.json'
    saved_report = json.loads(report_path.read_text(encoding="utf-8"))
    assert saved_report["failure_reason"] == "RuntimeError: original Silver failure"
    assert saved_report["failure_stage"] == "Silver processing"
    assert saved_report["failure_traceback"] == report["failure_traceback"]

    con = real_connect(str(database), read_only=True)
    try:
        assert con.execute(
            "select status from run_log where run_id=?", [report["run_id"]]
        ).fetchone()[0] == "running"
    finally:
        con.close()


def test_failure_report_write_error_does_not_mask_original(tmp_path, monkeypatch):
    import ucv.pipeline as pipeline

    source, _ = make_input(tmp_path)
    monkeypatch.setattr(
        pipeline,
        "transform_silver",
        lambda *_args: (_ for _ in ()).throw(RuntimeError("original Silver failure")),
    )
    monkeypatch.setattr(
        pipeline,
        "_save_report",
        lambda *_args: (_ for _ in ()).throw(OSError("injected report disk failure")),
    )

    report, _, _ = run_once(tmp_path, source)

    assert report["status"] == "failed"
    assert report["failure_reason"] == "RuntimeError: original Silver failure"


def test_total_runtime_uses_perf_counter_on_success_and_failure(
    tmp_path, monkeypatch
):
    from types import SimpleNamespace

    import ucv.pipeline as pipeline

    success_dir = tmp_path / "success"
    success_dir.mkdir()
    success_source, _ = make_input(success_dir)
    ticks = iter(index * 0.5 for index in range(20))
    monkeypatch.setattr(
        pipeline, "time", SimpleNamespace(perf_counter=lambda: next(ticks))
    )
    success, _, _ = run_once(success_dir, success_source)

    assert success["status"] == "succeeded"
    assert success["bronze_seconds"] == 0.5
    assert success["silver_seconds"] == 0.5
    assert success["gold_seconds"] == 0.5
    assert success["total_seconds"] == 3.5

    failure_dir = tmp_path / "failure"
    failure_dir.mkdir()
    failure_source, _ = make_input(failure_dir)
    ticks = iter(index * 0.5 for index in range(20))
    monkeypatch.setattr(
        pipeline, "time", SimpleNamespace(perf_counter=lambda: next(ticks))
    )
    monkeypatch.setattr(
        pipeline,
        "transform_silver",
        lambda *_args: (_ for _ in ()).throw(RuntimeError("timed failure")),
    )
    failed, database, _ = run_once(failure_dir, failure_source)

    assert failed["status"] == "failed"
    assert failed["bronze_seconds"] == 0.5
    assert failed["silver_seconds"] == 0.0
    assert failed["total_seconds"] == 2.0
    con = duckdb.connect(str(database), read_only=True)
    try:
        assert con.execute("select count(*) from bronze_record").fetchone()[0] == 100
    finally:
        con.close()

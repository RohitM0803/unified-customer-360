"""DuckDB Medallion pipeline orchestration."""
import json
import logging
import time
import traceback
import uuid
from datetime import datetime, timezone
from pathlib import Path

import duckdb

from .layers.bronze import ingest_bronze
from .layers.gold import publish_gold
from .layers.silver import transform_silver
from .schema import DDL

logger = logging.getLogger("ucv")


def _log(event, **fields):
    logger.info(json.dumps({"event": event, **fields}, default=str, sort_keys=True))


def initialize(conn):
    for statement in DDL.split(";"):
        if statement.strip():
            conn.execute(statement)


def _save_report(path, report):
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        raise FileExistsError(f"Refusing to overwrite run report: {path}")
    path.write_text(json.dumps(report, indent=2, default=str), encoding="utf-8")


def _log_secondary(event, **fields):
    """Best-effort logging that cannot replace the original pipeline error."""
    try:
        _log(event, **fields)
    except Exception:
        pass


def _collect_metrics(conn, run_id, report):
    report["input_rows_by_source"] = dict(conn.execute(
        "SELECT entity,count(*) FROM bronze_record WHERE run_id=? GROUP BY entity",
        [run_id]).fetchall())
    report["rejection_reasons"] = dict(conn.execute(
        "SELECT reason,count(*) FROM rejected_record WHERE run_id=? GROUP BY reason ORDER BY reason",
        [run_id]).fetchall())
    report["stage_rows"] = {
        "bronze": sum(report["input_rows_by_source"].values()),
        "silver": {table: int(conn.execute(f"SELECT count(*) FROM {table}").fetchone()[0])
                   for table in ("silver_customer", "silver_account", "silver_order",
                                 "silver_interaction")},
        "gold": {table: int(conn.execute(f"SELECT count(*) FROM {table}").fetchone()[0])
                 for table in ("gold_customer_master", "gold_customer_account",
                               "gold_customer_order_history", "gold_customer_interaction_history",
                               "gold_customer_360")},
    }
    accounted = (report["inserted_rows"] + report["skipped_existing_rows"]
                 + report["skipped_duplicate_batch_rows"] + report["rejected_rows"]
                 + report["unprocessed_rows"])
    report["reconciliation"] = {
        "input_rows": report["input_rows"], "accounted_rows": accounted,
        "balanced": accounted == report["input_rows"],
        "published_rows": report["output_rows"],
        "silver_entity_counts": report.get("entity_counts", {}),
    }


def run_pipeline(input_dir, database_path="database/customer360.duckdb",
                 raw_root="data/raw/archive", source_system="default",
                 report_dir="output/duckdb_reports"):
    input_dir = Path(input_dir)
    database_path = Path(database_path)
    raw_root = Path(raw_root)
    report_dir = Path(report_dir)
    if database_path.suffix.lower() != ".duckdb":
        raise ValueError("database_path must end with .duckdb")

    run_id = str(uuid.uuid4())
    started = datetime.now(timezone.utc)
    wall_started = time.perf_counter()
    report_path = report_dir / f"{run_id}.json"
    report = {
        "run_id": run_id, "source_system": source_system,
        "started_at": started.isoformat(), "status": "running",
        "input_rows": 0, "inserted_rows": 0, "skipped_existing_rows": 0,
        "skipped_duplicate_batch_rows": 0, "rejected_rows": 0,
        "unprocessed_rows": 0, "output_rows": 0,
        "accepted_rows": 0, "input_rows_by_source": {}, "entity_counts": {},
        "rejection_reasons": {}, "stage_rows": {}, "reconciliation": {},
        "bronze_seconds": 0.0, "silver_seconds": 0.0, "gold_seconds": 0.0,
        "total_seconds": 0.0,
    }
    conn = None
    schema_initialized = False
    run_log_created = False
    current_stage = "initialization"

    try:
        database_path.parent.mkdir(parents=True, exist_ok=True)
        conn = duckdb.connect(str(database_path))
        initialize(conn)
        schema_initialized = True
        conn.execute(
            "INSERT INTO run_log(run_id,source_system,started_at,status) VALUES (?,?,?,'running')",
            [run_id, source_system,
             started.isoformat(timespec="microseconds").replace("+00:00", "Z")],
        )
        conn.commit()
        run_log_created = True
        _log("run_started", run_id=run_id, source_system=source_system,
             database=str(database_path), input_dir=str(input_dir))

        current_stage = "Bronze ingestion"
        begin = time.perf_counter()
        input_rows = ingest_bronze(conn, input_dir, run_id, source_system, raw_root)
        conn.commit()  # Bronze remains a separately committed layer.
        report["bronze_seconds"] = time.perf_counter() - begin
        report["input_rows"] = input_rows
        _log("bronze_completed", run_id=run_id, input_rows=input_rows,
             elapsed_seconds=report["bronze_seconds"])

        current_stage = "Silver processing"
        conn.execute("BEGIN TRANSACTION")
        begin = time.perf_counter()
        silver_counts = transform_silver(conn, run_id, source_system)
        report["silver_seconds"] = time.perf_counter() - begin
        report.update(silver_counts)
        _log("silver_completed", run_id=run_id, **silver_counts,
             elapsed_seconds=report["silver_seconds"])

        current_stage = "Gold publishing"
        begin = time.perf_counter()
        gold_customers = publish_gold(conn)
        report["gold_seconds"] = time.perf_counter() - begin
        report["gold_customer_count"] = gold_customers
        _log("gold_completed", run_id=run_id, gold_customer_count=gold_customers,
             elapsed_seconds=report["gold_seconds"])

        report["unprocessed_rows"] = 0
        current_stage = "reconciliation"
        _collect_metrics(conn, run_id, report)
        report["total_seconds"] = time.perf_counter() - wall_started
        report["status"] = "succeeded"
        report["ended_at"] = datetime.now(timezone.utc).isoformat()
        current_stage = "run logging"
        conn.execute("""UPDATE run_log SET ended_at=?,status='succeeded',input_rows=?,inserted_rows=?,
            skipped_existing_rows=?,skipped_duplicate_batch_rows=?,rejected_rows=?,unprocessed_rows=?,output_rows=?,
            bronze_seconds=?,silver_seconds=?,gold_seconds=?,total_seconds=? WHERE run_id=?""",
            [report["ended_at"], report["input_rows"], report["inserted_rows"],
             report["skipped_existing_rows"], report["skipped_duplicate_batch_rows"],
             report["rejected_rows"], report["unprocessed_rows"], report["output_rows"],
             report["bronze_seconds"], report["silver_seconds"], report["gold_seconds"],
             report["total_seconds"], run_id])
        conn.commit()

    except Exception as exc:
        reason = f"{type(exc).__name__}: {exc}"
        report["status"] = "failed"
        report["failure_reason"] = reason
        report["failure_stage"] = current_stage
        report["failure_traceback"] = traceback.format_exc()
        report["ended_at"] = datetime.now(timezone.utc).isoformat()
        report["total_seconds"] = time.perf_counter() - wall_started
        # Silver and Gold share a transaction; rolled-back outputs are zero.
        report["inserted_rows"] = 0
        report["skipped_existing_rows"] = 0
        report["skipped_duplicate_batch_rows"] = 0
        report["rejected_rows"] = 0
        report["output_rows"] = 0
        report["accepted_rows"] = 0
        for entity_counts in report.get("entity_counts", {}).values():
            for key in ("inserted_rows", "skipped_existing_rows",
                        "skipped_duplicate_batch_rows", "rejected_rows"):
                entity_counts[key] = 0

        if conn is not None:
            try:
                conn.rollback()
            except Exception as rollback_exc:
                _log_secondary("failure_rollback_error", run_id=run_id,
                               failure_reason=str(rollback_exc))

        # Count only rows actually persisted to Bronze, even when ingestion
        # raised before returning its count.
        if conn is not None and schema_initialized:
            try:
                report["input_rows"] = int(conn.execute(
                    "SELECT count(*) FROM bronze_record WHERE run_id=?", [run_id]
                ).fetchone()[0])
            except Exception as count_exc:
                _log_secondary("bronze_count_error", run_id=run_id,
                               failure_reason=str(count_exc))

        # Preserve the existing batch schema-drift quarantine, but make it
        # explicit-column and best-effort so it cannot mask the original error.
        if (conn is not None and schema_initialized and isinstance(exc, ValueError)
                and "schema mismatch" in str(exc)):
            bad_file = next((f"{entity}.csv" for entity in
                             ("customers", "accounts", "orders", "interactions")
                             if f"{entity}.csv:" in str(exc)), None)
            if bad_file:
                try:
                    rows = conn.execute(
                        "SELECT source_file,source_row,entity,source_system,raw_record_json "
                        "FROM bronze_record WHERE run_id=?", [run_id]
                    ).fetchall()
                    conn.execute("BEGIN TRANSACTION")
                    for file_name, rownum, entity, system, raw in rows:
                        reject_id = uuid.uuid5(
                            uuid.NAMESPACE_URL,
                            f"{run_id}|{file_name}|{rownum}|{reason}",
                        ).hex
                        conn.execute(
                            "INSERT INTO rejected_record "
                            "(reject_id,run_id,source_system,source_file,source_row,entity,"
                            "reason,raw_record_json) VALUES (?,?,?,?,?,?,?,?)",
                            [reject_id, run_id, system, file_name, rownum, entity,
                             f"batch schema failure ({bad_file}): {reason}; row not published", raw],
                        )
                    conn.commit()
                    report["rejected_rows"] = int(conn.execute(
                        "SELECT count(*) FROM rejected_record WHERE run_id=?", [run_id]
                    ).fetchone()[0])
                except Exception as quarantine_exc:
                    try:
                        conn.rollback()
                    except Exception as rollback_exc:
                        _log_secondary("quarantine_rollback_error", run_id=run_id,
                                       failure_reason=str(rollback_exc))
                    _log_secondary("schema_quarantine_error", run_id=run_id,
                                   original_failure=reason,
                                   failure_reason=str(quarantine_exc))

        report["unprocessed_rows"] = max(
            0, report["input_rows"] - report["output_rows"] - report["rejected_rows"]
        )
        if conn is not None and schema_initialized:
            try:
                _collect_metrics(conn, run_id, report)
            except Exception as metrics_exc:
                _log_secondary("failure_metrics_error", run_id=run_id,
                               failure_reason=str(metrics_exc))

        # Do not assume this row exists if initialization or its insert failed.
        if conn is not None and run_log_created:
            try:
                conn.execute("BEGIN TRANSACTION")
                conn.execute("""UPDATE run_log SET ended_at=?,status='failed',input_rows=?,inserted_rows=?,
                    skipped_existing_rows=?,skipped_duplicate_batch_rows=?,rejected_rows=?,unprocessed_rows=?,output_rows=?,
                    bronze_seconds=?,silver_seconds=?,gold_seconds=?,total_seconds=?,failure_reason=? WHERE run_id=?""",
                    [report["ended_at"], report["input_rows"], report["inserted_rows"],
                     report["skipped_existing_rows"], report["skipped_duplicate_batch_rows"],
                     report["rejected_rows"], report["unprocessed_rows"], report["output_rows"],
                     report["bronze_seconds"], report["silver_seconds"], report["gold_seconds"],
                     report["total_seconds"], reason, run_id])
                conn.commit()
            except Exception as record_exc:
                try:
                    conn.rollback()
                except Exception as rollback_exc:
                    _log_secondary("failed_run_log_rollback_error", run_id=run_id,
                                   failure_reason=str(rollback_exc))
                _log_secondary("failed_run_log_update_error", run_id=run_id,
                               original_failure=reason, failure_reason=str(record_exc))
        _log_secondary("run_failed", run_id=run_id, failure_reason=reason)

    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception as close_exc:
                _log_secondary("connection_close_error", run_id=run_id,
                               failure_reason=str(close_exc))

    try:
        report["database_bytes"] = database_path.stat().st_size if database_path.exists() else 0
    except Exception as size_exc:
        report["database_bytes"] = 0
        _log_secondary("database_size_error", run_id=run_id,
                       failure_reason=str(size_exc))

    if report["status"] == "failed":
        try:
            _save_report(report_path, report)
        except Exception as report_exc:
            _log_secondary("failed_run_report_write_error", run_id=run_id,
                           original_failure=report.get("failure_reason"),
                           failure_reason=str(report_exc))
    else:
        _save_report(report_path, report)

    _log_secondary("run_finished", run_id=run_id, status=report["status"],
                   report_path=str(report_path), total_seconds=report["total_seconds"])
    return report

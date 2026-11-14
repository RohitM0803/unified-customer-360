"""Reproducible CSV/Parquet and DuckDB Medallion benchmark."""
import argparse
import ctypes
import json
import os
import platform
import statistics
import time
from pathlib import Path

import duckdb

from .generator import PRESETS, generate_dataset
from .pipeline import run_pipeline
from .schema import SOURCE_COLUMNS


def _sql_path(path):
    return Path(path).resolve().as_posix().replace("'", "''")


def _read_seconds(conn, query):
    started = time.perf_counter()
    cursor = conn.execute(query)
    rows = 0
    while batch := cursor.fetchmany(8192):
        rows += len(batch)
    return time.perf_counter() - started, rows


def _windows_memory():
    if os.name != "nt":
        return None

    class Counters(ctypes.Structure):
        _fields_ = [
            ("cb", ctypes.c_ulong), ("PageFaultCount", ctypes.c_ulong),
            ("PeakWorkingSetSize", ctypes.c_size_t), ("WorkingSetSize", ctypes.c_size_t),
            ("QuotaPeakPagedPoolUsage", ctypes.c_size_t), ("QuotaPagedPoolUsage", ctypes.c_size_t),
            ("QuotaPeakNonPagedPoolUsage", ctypes.c_size_t), ("QuotaNonPagedPoolUsage", ctypes.c_size_t),
            ("PagefileUsage", ctypes.c_size_t), ("PeakPagefileUsage", ctypes.c_size_t),
            ("PrivateUsage", ctypes.c_size_t),
        ]
    value = Counters()
    value.cb = ctypes.sizeof(value)
    ok = ctypes.windll.psapi.GetProcessMemoryInfo(
        ctypes.windll.kernel32.GetCurrentProcess(), ctypes.byref(value), value.cb)
    return {"working_set_bytes": value.WorkingSetSize,
            "peak_working_set_bytes": value.PeakWorkingSetSize} if ok else None


def _total_memory_bytes():
    if os.name != "nt":
        return None

    class MemoryStatus(ctypes.Structure):
        _fields_ = [("dwLength", ctypes.c_ulong), ("dwMemoryLoad", ctypes.c_ulong),
                    ("ullTotalPhys", ctypes.c_ulonglong), ("ullAvailPhys", ctypes.c_ulonglong),
                    ("ullTotalPageFile", ctypes.c_ulonglong), ("ullAvailPageFile", ctypes.c_ulonglong),
                    ("ullTotalVirtual", ctypes.c_ulonglong), ("ullAvailVirtual", ctypes.c_ulonglong),
                    ("ullAvailExtendedVirtual", ctypes.c_ulonglong)]
    status = MemoryStatus()
    status.dwLength = ctypes.sizeof(status)
    return status.ullTotalPhys if ctypes.windll.kernel32.GlobalMemoryStatusEx(ctypes.byref(status)) else None


def _query_for(entity, source):
    if entity == "customers":
        return f"SELECT count(*) FROM {source}"
    if entity == "accounts":
        return f"SELECT status, count(*) FROM {source} GROUP BY status"
    if entity == "orders":
        return f"SELECT customer_id, sum(try_cast(amount AS DECIMAL(18,2))) FROM {source} GROUP BY customer_id"
    return f"SELECT event_type, count(*) FROM {source} GROUP BY event_type"


def benchmark_size(total_rows, seed, root, repeats=3, mode="messy"):
    benchmark_started = time.perf_counter()
    size_name = next((name for name, value in PRESETS.items() if value == total_rows), f"{total_rows}_rows")
    case_dir = Path(root) / size_name
    if case_dir.exists() and any(case_dir.iterdir()):
        raise FileExistsError(f"Benchmark output already exists: {case_dir}; choose a new root")
    input_dir = case_dir / "input"
    csv_export_dir = case_dir / "csv_export"
    parquet_dir = case_dir / "parquet"
    result_dir = case_dir / "results"
    for directory in (input_dir, csv_export_dir, parquet_dir, result_dir):
        directory.mkdir(parents=True, exist_ok=True)

    mem_before = _windows_memory()
    generation_started = time.perf_counter()
    manifest = generate_dataset(input_dir, total_rows, seed=seed, mode=mode)
    generation_seconds = time.perf_counter() - generation_started
    csv_paths = {e: input_dir / f"{e}.csv" for e in SOURCE_COLUMNS}
    csv_export_paths = {e: csv_export_dir / f"{e}.csv" for e in SOURCE_COLUMNS}
    parquet_paths = {e: parquet_dir / f"{e}.parquet" for e in SOURCE_COLUMNS}
    source_csv_bytes = sum(p.stat().st_size for p in csv_paths.values())

    con = duckdb.connect()
    csv_write_samples, parquet_write_samples = [], []
    for entity in SOURCE_COLUMNS:
        source = _sql_path(csv_paths[entity])
        csv_target, parquet_target = _sql_path(csv_export_paths[entity]), _sql_path(parquet_paths[entity])
        con.execute(f"CREATE TABLE bench_{entity} AS SELECT * FROM read_csv_auto('{source}', header=true, all_varchar=true)")
        started = time.perf_counter()
        con.execute(f"COPY bench_{entity} TO '{csv_target}' (HEADER, DELIMITER ',')")
        csv_write_samples.append(time.perf_counter() - started)
        started = time.perf_counter()
        con.execute(f"COPY bench_{entity} TO '{parquet_target}' (FORMAT PARQUET, COMPRESSION ZSTD)")
        parquet_write_samples.append(time.perf_counter() - started)
    csv_bytes = sum(p.stat().st_size for p in csv_export_paths.values())
    parquet_bytes = sum(p.stat().st_size for p in parquet_paths.values())

    measurements = {"full_read": {"csv": [], "parquet": []},
                    "selected_columns": {"csv": [], "parquet": []},
                    "analytical_query": {"csv": [], "parquet": []}}
    for _ in range(repeats):
        for entity in SOURCE_COLUMNS:
            for label, path in (("csv", csv_export_paths[entity]), ("parquet", parquet_paths[entity])):
                source = f"read_csv_auto('{_sql_path(path)}', header=true, all_varchar=true)" if label == "csv" else f"read_parquet('{_sql_path(path)}')"
                elapsed, count = _read_seconds(con, f"SELECT * FROM {source}")
                measurements["full_read"][label].append(elapsed)
                if count != manifest["entity_counts"][entity]:
                    raise AssertionError(f"{label} full read count mismatch for {entity}: {count}")
                if entity == "customers":
                    selected_query = f"SELECT sum(length(customer_id))+sum(length(email)) FROM {source}"
                elif entity == "accounts":
                    selected_query = f"SELECT sum(length(account_id))+sum(length(status)) FROM {source}"
                elif entity == "orders":
                    selected_query = f"SELECT sum(length(order_id))+sum(length(CAST(amount AS VARCHAR))) FROM {source}"
                else:
                    selected_query = f"SELECT sum(length(event_id))+sum(length(event_type)) FROM {source}"
                started = time.perf_counter()
                con.execute(selected_query).fetchone()
                measurements["selected_columns"][label].append(time.perf_counter() - started)
                started = time.perf_counter()
                query_result = con.execute(_query_for(entity, source)).fetchall()
                measurements["analytical_query"][label].append(time.perf_counter() - started)
                if label == "csv":
                    csv_result = query_result
                elif sorted(map(repr, query_result)) != sorted(map(repr, csv_result)):
                    raise AssertionError(f"CSV/Parquet analytical results differ for {entity}")
    con.close()

    pipeline_report = run_pipeline(input_dir, case_dir / "customer360.duckdb",
                                  case_dir / "raw_archive", "benchmark",
                                  result_dir / "pipeline_reports")
    if pipeline_report["status"] != "succeeded":
        raise RuntimeError(f"Benchmark pipeline failed: {pipeline_report.get('failure_reason')}")
    expected_rows = manifest["total_rows"]
    if (pipeline_report["input_rows"] != expected_rows
            or pipeline_report["output_rows"] + pipeline_report["rejected_rows"]
            != expected_rows
            or not pipeline_report["reconciliation"].get("balanced")
            or pipeline_report["stage_rows"]["bronze"] != expected_rows):
        raise AssertionError("Initial benchmark pipeline counts did not reconcile")
    rerun_report = run_pipeline(input_dir, case_dir / "customer360.duckdb",
                                case_dir / "raw_archive", "benchmark",
                                result_dir / "pipeline_reports")
    if rerun_report["status"] != "succeeded":
        raise RuntimeError(f"Identical benchmark rerun failed: {rerun_report.get('failure_reason')}")
    if (rerun_report["input_rows"] != expected_rows
            or rerun_report["inserted_rows"] != 0
            or rerun_report["skipped_existing_rows"] != pipeline_report["inserted_rows"]
            or rerun_report["skipped_duplicate_batch_rows"] != pipeline_report["skipped_duplicate_batch_rows"]
            or rerun_report["rejected_rows"] != pipeline_report["rejected_rows"]
            or not rerun_report["reconciliation"].get("balanced")
            or rerun_report["stage_rows"]["gold"] != pipeline_report["stage_rows"]["gold"]):
        raise AssertionError("Identical benchmark rerun did not reconcile without changing Gold")
    mem_after = _windows_memory()
    summary = {
        "dataset": manifest, "repeats": repeats,
        "machine": {"platform": platform.platform(), "processor": platform.processor(),
                    "logical_cpus": os.cpu_count(), "python": platform.python_version(),
                    "duckdb": duckdb.__version__, "total_memory_bytes": _total_memory_bytes(),
                    "memory_before": mem_before,
                    "memory_after_peak": mem_after},
        "csv": {"source_bytes": source_csv_bytes, "export_bytes": csv_bytes,
                "export_write_seconds": sum(csv_write_samples),
                "export_write_samples": csv_write_samples,
                "generation_and_source_write_seconds": generation_seconds},
        "parquet": {"bytes": parquet_bytes, "write_seconds": sum(parquet_write_samples),
                    "write_samples": parquet_write_samples,
                    "compression": "ZSTD"},
        "measurements_seconds": {
            name: {fmt: {"samples": values, "median": statistics.median(values)}
                   for fmt, values in formats.items()}
            for name, formats in measurements.items()
        },
        "pipeline": {"initial": pipeline_report, "identical_rerun": rerun_report,
                     "identical_rerun_verified": True},
        "benchmark_seconds": time.perf_counter() - benchmark_started,
        "csv_to_parquet_size_ratio": round(csv_bytes / parquet_bytes, 3) if parquet_bytes else None,
        "parquet_storage_reduction_fraction": round(1 - parquet_bytes / csv_bytes, 4) if csv_bytes else None,
    }
    output = result_dir / "benchmark.json"
    if output.exists() and not overwrite:
        raise FileExistsError(f"Refusing to overwrite benchmark result: {output}")
    output.write_text(json.dumps(summary, indent=2, default=str), encoding="utf-8")
    return summary


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sizes", nargs="+", choices=(*PRESETS.keys(),))
    parser.add_argument("--total-rows", nargs="+", type=int)
    parser.add_argument("--seed", type=int, default=1729)
    parser.add_argument("--out", type=Path, default=Path("data/benchmarks"))
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--mode", choices=("clean", "messy"), default="messy")
    args = parser.parse_args(argv)
    totals = args.total_rows or [PRESETS[x] for x in (args.sizes or ["medium", "large"])]
    if args.repeats < 1:
        parser.error("--repeats must be at least one")
    results = [benchmark_size(n, args.seed, args.out, args.repeats, args.mode) for n in totals]
    for result in results:
        print(json.dumps(result, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

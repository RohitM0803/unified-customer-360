# Storage-format and pipeline benchmarks

`benchmark.py` creates a fixed-seed clean dataset, loads each CSV into an in-memory DuckDB table, exports the same table to CSV and ZSTD Parquet, then compares storage size, export time, full scan time, selected-column scan time, and one representative analytical aggregate. It also runs the complete DuckDB Medallion pipeline and records stage timings, throughput, database bytes, process memory (Windows), platform, CPU, Python, DuckDB version, seed, and per-entity counts.

Use `--sizes medium large` for 10,000- and 100,000-total-row runs; `--total-rows N` supports larger local trials. Each non-empty benchmark case directory is protected against accidental reuse. Results are JSON under the case's `results/benchmark.json`, with raw samples and medians. Dataset generation time is reported separately from export timing.

No CSV-versus-Parquet performance claim is valid until the command has been run on this machine. Parquet remains a benchmark artifact and optional interchange format; the working layers stay in DuckDB unless measured downstream needs justify persistent Parquet snapshots.

A benchmark is local evidence only. It does not establish distributed performance, multi-writer behavior, or production service capacity.

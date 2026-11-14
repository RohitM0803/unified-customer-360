# Unified Customer View

A local DuckDB analytics project that turns four CSV sources into an auditable Bronze → Silver → Gold Customer 360 view. The pipeline is designed for reproducible local batch work; it does not claim distributed-scale operation.

## Technology choices

- Python 3.13 and SQL implement generation, validation, orchestration, and reporting.
- DuckDB is the only runtime database. It stores the Medallion layers and supports analytical SQL and Parquet directly.
- pytest provides unit, integration, recovery, and large-dataset tests.
- Python standard-library JSON logging records structured stage/run events.
- Parquet is measured against CSV by the benchmark command. It is an optional output format, not duplicated into each layer by default.
- Polars is not a runtime dependency: current transformations are relational SQL and custom validation. Add it only if benchmarks identify a dataframe task that benefits.
- Great Expectations is not included; explicit schemas and domain checks are small, stable, and directly tested.
- Dagster/Prefect are not included; a local one-shot batch command has no scheduling or service dependency to orchestrate.

## Setup (PowerShell)

```powershell
py -3.13 -m venv .venv
.venv\Scripts\python.exe -m pip install -e ".[dev]"
```

All project commands below use the local `.venv` interpreter explicitly.

## Generate reproducible data

Counts are **total rows across all four files**, not customer counts. The distribution is balanced and written to `manifest.json`.

```powershell
.venv\Scripts\python.exe generate_data.py --size small --mode clean --out data/generated/small_clean
.venv\Scripts\python.exe generate_data.py --size medium --mode clean --out data/generated/medium_clean
.venv\Scripts\python.exe generate_data.py --size large --mode clean --out data/generated/large_clean
.venv\Scripts\python.exe generate_data.py --size large --mode messy --out data/generated/large_messy
```

Presets are 100, 10,000, and 100,000 total rows, split equally when divisible by four. The default size is `large` (100,000 rows); use `--total-rows N` for another exact total, `--seed 314159` to select a reproducible seed, and `--mode schema_drift` for a deliberately malformed header. The messy profile keeps the requested total while introducing missing keys/attributes, exact and conflicting duplicate keys, invalid references/amounts/timestamps, conflicting email identity, and shuffled late/out-of-order child events. Generation refuses to overwrite existing files unless `--overwrite` is explicit.

## Run the pipeline

```powershell
.venv\Scripts\python.exe pipeline.py --input data/generated/small_clean --database database/customer360.duckdb --raw-root data/raw/archive --source-system demo
```

Each run gets a unique JSON report in `output/duckdb_reports`. Bronze archives original input files under source system/run ID and retains parsed original row values in DuckDB. Reports are never silently overwritten. Reports include input counts by entity, accepted inserts, in-batch duplicates, previously loaded rows, rejected rows and reasons, unprocessed rows, Bronze/Silver/Gold counts, stage durations, and row-accounting reconciliation. `output_rows` includes inserted and safely skipped rows; `accepted_rows` counts newly inserted Silver records. Successful batches reconcile input as inserted + previously loaded + in-batch duplicates + rejected.

## Layers and metrics

Bronze → Silver → Gold table names, inputs, outputs, responsibilities, failure behavior, and lineage are in [docs/architecture.md](docs/architecture.md). The field-level schema is in [docs/data_dictionary.md](docs/data_dictionary.md).

Every run reports `input_rows`, `inserted_rows`, `skipped_existing_rows`, `skipped_duplicate_batch_rows`, `rejected_rows`, `unprocessed_rows`, and `output_rows`, plus Bronze/Silver/Gold/total durations and target database size. Exact loaded replays are skipped; exact in-batch repeats are separately counted; conflicts and invalid rows are quarantined. Successful batches reconcile `input_rows = inserted_rows + skipped_existing_rows + skipped_duplicate_batch_rows + rejected_rows`. Failed runs retain Bronze and record unprocessed rows; transactional Silver/Gold changes roll back.

## Tests

```powershell
.venv\Scripts\python.exe -m pytest -v
```

The full suite includes a clean 100,000-row integration run and tests replay, conflict, schema drift, malformed/invalid rows, relationship reconciliation, Gold output, lineage, out-of-order events, and retry after an injected Gold failure. Run only the large integration test with:

```powershell
.venv\Scripts\python.exe -m pytest tests/test_pipeline.py::test_large_clean_100k_total -q
```

## CSV / Parquet and pipeline benchmarks

```powershell
.venv\Scripts\python.exe benchmark.py --sizes large --mode messy --seed 1729 --repeats 1 --out data/benchmarks/run_100k
```

The benchmark defaults to mixed messy data. It processes all generated input through Bronze/Silver/Gold, checks input/accepted/rejected/duplicate reconciliation, then processes the identical input again and verifies new rows are not inserted, prior rows are skipped, the same batch duplicates and rejects are reported, and Gold counts remain unchanged. Both run reports and actual stage counts are recorded. The benchmark also measures generation separately; CSV and Parquet export size/write times from the same DuckDB tables; full scans, selected-column scans, representative aggregates, stage durations, database size, and process memory where supported. It records platform, processor, CPU count, Python, DuckDB, seed, entity distribution, compression, and samples/medians as JSON. It refuses to reuse a non-empty benchmark case directory. Results cover only this machine and do not imply distributed scalability.

## Migration and backups

The prior SQLite databases are retained as offline backups under `backups/legacy_sqlite/`; the new runtime imports no SQLite module and writes a separate `.duckdb` database. Existing source datasets and historical reports remain preserved. The new DuckDB database is rebuilt from the preserved source CSVs; it does not claim to recreate legacy run-history rows that were only present in SQLite.

## Limitations

DuckDB is embedded and intended here for local analytics and one writer at a time. Benchmark results cover only this machine and tested dataset sizes. Source files are archived per run, so raw archive storage grows with each ingestion. Exact email matching and source-ID fallback are deterministic; conflicting names/emails go to manual review, with no fuzzy auto-merge.

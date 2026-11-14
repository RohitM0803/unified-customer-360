# Medallion architecture

The runnable pipeline is implemented under `src/ucv` and uses one DuckDB database. Its orchestrator calls three separable layer functions:

```text
CSV input -> Bronze -> Silver -> Gold -> JSON run report
```

## Bronze

`ingest_bronze` first verifies that all four input files exist, then copies each original file to `data/raw/archive/<source_system>/<run_id>/`. `bronze_file` records the original header, schema-valid flag, SHA-256, archived path, source system, run ID, and UTC ingestion time. `bronze_record` stores parsed source strings, entity, source file, CSV row number, source system, run ID, and ingestion time. New run IDs append history; the code never updates or deletes prior raw records/files.

A schema-drifted file and its raw rows are archived before Silver validates the recorded header. If validation fails, Bronze remains available and the run is marked failed.

## Silver

`transform_silver` validates the expected columns and applies required-key, foreign-key, timestamp, decimal, and identity rules. Records are stored in `silver_customer`, `silver_customer_source`, `silver_account`, `silver_order`, and `silver_interaction`, each with source lineage on source-level rows.

Email matching trims and case-folds syntactically valid email addresses. Missing/invalid email uses `(source_system, source_customer_id)` and emits a quality issue. Exact repeated rows in a batch are counted as batch skips; exact records from previous runs are counted as existing skips. Changed payloads for a stable key, invalid required fields, and invalid references are written to `rejected_record` with the raw JSON and lineage. Conflicting identity evidence creates `uncertain_match_review` rows with pending status; no fuzzy match is made. `quality_issue` captures non-fatal missing/invalid optional values.

Silver and Gold are committed as one DuckDB transaction. A failed transform or publish rolls back Silver/Gold changes; Bronze persists for investigation and a new-run retry.

## Gold

`publish_gold` rebuilds derived outputs from Silver after a successful transform. `gold_customer_master` contains one stable customer identity, match method, uncertainty/review status, and JSON evidence linking source identities and candidate reviews. `gold_customer_account`, `gold_customer_order_history`, and `gold_customer_interaction_history` are lineage-preserving relationship/history tables. `gold_customer_360` is a useful aggregate view with account/order/interaction counts, lifetime order amount, and latest order/interaction times, alongside identity evidence.

The Gold entities are derived and replaced transactionally on each successful publication; Bronze and Silver provide the historical/raw records.

## Run metrics and recovery

`run_log` records run status, input/inserted/existing-skip/in-batch-skip/reject/unprocessed/output counts, stage times, total time, and failure reason. For successful runs, `input_rows = inserted_rows + skipped_existing_rows + skipped_duplicate_batch_rows + rejected_rows`. On failure, transactional outputs are reported as zero for the failed transaction; Bronze input that was not rejected is reported as `unprocessed_rows`.

## Module map

- `src/ucv/layers/bronze.py`: source archive, row retention, and schema metadata.
- `src/ucv/layers/silver.py`: validation, deduplication, identity mapping, quarantine.
- `src/ucv/identity.py`, `quality.py`: pure normalization and rule functions.
- `src/ucv/layers/gold.py`: master, links/history, and Customer 360 view.
- `src/ucv/pipeline.py`: stage timing, transactions, run/report orchestration.
- `src/ucv/generator.py`, `benchmark.py`: reproducible data and format/performance measurement.

## Scope and compatibility

DuckDB is a local analytical database with a single-writer model; it is not distributed. The old SQLite databases are retained offline as requested and are not opened or modified by the new runtime. The fresh DuckDB target is built from preserved CSV source files; previous database-only run history is not reconstructed.

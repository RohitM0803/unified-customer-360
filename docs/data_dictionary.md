# Data dictionary

All user-facing source column names are snake_case. Source files are UTF-8 CSV. Source row numbers are one-based, with the header on line 1. Event and order timestamps are stored as UTC ISO-8601 text with microseconds. Amounts are finite decimal text; Gold aggregates cast values to `DECIMAL(38,9)`.

| Layer/table | Grain and key | Important columns / constraints |
|---|---|---|
| Bronze `bronze_file` | One source file per run; `(run_id, source_file)` | `source_system`, `columns_json`, `schema_valid`, `sha256`, `archived_path`, `ingested_at` |
| Bronze `bronze_record` | One input data row; `bronze_id` | `entity`, `source_file`, `source_row`, `source_system`, `run_id`, `ingested_at`, `raw_record_json` |
| Silver `silver_customer` | One resolved customer; `customer_sk` | `match_key`, `name_norm`, normalized `email`, `match_method`, `uncertain_match` |
| Silver `silver_customer_source` | One source customer; `(source_system, source_customer_id)` | Link to `customer_sk`, payload hash, source lineage |
| Silver `silver_account` | One account; `(source_system, account_id)` | `customer_sk`, name/status, raw payload hash, source lineage |
| Silver `silver_order` | One order; `(source_system, order_id)` | `customer_sk`, UTC `order_ts`, finite decimal `amount`, source lineage |
| Silver `silver_interaction` | One event; `(source_system, event_id)` | `customer_sk`, UTC `event_ts`, type/details, source lineage |
| Silver `rejected_record` | One rejected source row; `reject_id` | Reason, original JSON values, source file/row/system/run |
| Silver `quality_issue` | One non-fatal quality warning; `issue_id` | Severity, reason, source lineage, original values |
| Silver `uncertain_match_review` | One candidate relationship; `review_id` | Subject/candidate customer keys, reason, pending review status |
| Gold `gold_customer_master` | One canonical customer; `customer_sk` | Match evidence, identity resolution method, uncertainty/review flags |
| Gold `gold_customer_account` | One customer-account link | Account key and full source lineage |
| Gold `gold_customer_order_history` | One customer-order link | Order data and source lineage |
| Gold `gold_customer_interaction_history` | One customer-event link | Event data and source lineage |
| Gold `gold_customer_360` | One row per canonical customer | Counts, spend aggregate, latest activity, identity evidence |
| Monitoring `run_log` | One pipeline attempt; `run_id` | Status, counts, stage durations, total duration, failure reason |

## Source files

- `customers.csv`: `customer_id`, `name`, `email`, `phone`. `customer_id` is required; email and phone may be absent.
- `accounts.csv`: `account_id`, `customer_id`, `account_name`, `status`. Account ID and a resolvable customer reference are required.
- `orders.csv`: `order_id`, `customer_id`, `order_ts`, `amount`. ID/reference, ISO timestamp, finite decimal are required.
- `interactions.csv`: `event_id`, `customer_id`, `event_ts`, `event_type`, `details`. ID/reference, ISO timestamp, and event type are required.

Matching is deterministic: valid trimmed/case-folded exact email first; otherwise source system plus source customer ID. A non-empty syntactically invalid email is not used as an identity key and creates a quality issue. Conflicting identity evidence is flagged for manual review rather than auto-merged.

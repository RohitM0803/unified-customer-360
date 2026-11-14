# Recorded validation results

These are measured results from the local benchmark run below, not estimates. The benchmark generated messy-mode CSV inputs with seed `314159`, processed all rows through Bronze, Silver, and Gold, then processed the identical files a second time against the same DuckDB database.

```powershell
.venv\Scripts\python.exe benchmark.py --sizes large --mode messy --seed 314159 --repeats 1 --out data/benchmarks/acceptance_100k_timed
```

The benchmark completed in **287.19 seconds** on Python 3.13.5 and DuckDB 2.0.0. `benchmark.json` is generated locally; the large database and data files are intentionally not checked in.

## Input counts

| File | Rows |
|---|---:|
| customers.csv | 25,000 |
| accounts.csv | 25,000 |
| orders.csv | 25,000 |
| interactions.csv | 25,000 |
| **Total** | **100,000** |

## First run

| Category | Rows |
|---|---:|
| Input / Bronze | 100,000 |
| Inserted into Silver | 99,975 |
| Duplicate within batch | 1 |
| Previously loaded | 0 |
| Rejected | 24 |
| Unprocessed | 0 |
| Accounted | 100,000 (balanced) |

## Second identical run

| Category | Rows |
|---|---:|
| Input / Bronze | 100,000 |
| Newly inserted | 0 |
| Duplicate within batch | 1 |
| Previously loaded | 99,975 |
| Rejected | 24 |
| Unprocessed | 0 |
| Accounted | 100,000 (balanced) |

Both runs published the same physical Silver and Gold contents. `output_rows` is 99,976 on each run because it includes the one exact in-batch duplicate as accounted/skipped input; it is not a count of newly inserted physical rows.

## Persisted table counts after both runs

The database contains two append-only Bronze ingestions and two sets of rejection records. Silver and Gold remain deduplicated:

| Table | Rows |
|---|---:|
| bronze_record | 200,000 |
| rejected_record | 48 |
| silver_customer | 24,997 |
| silver_account | 24,992 |
| silver_order | 24,993 |
| silver_interaction | 24,993 |
| gold_customer_master | 24,997 |
| gold_customer_account | 24,992 |
| gold_customer_order_history | 24,993 |
| gold_customer_interaction_history | 24,993 |
| gold_customer_360 | 24,997 |

`gold_customer_360` has one row per published customer; its count is not the total number of source records. The account, order, and interaction records are published in their corresponding Gold history tables.

## Rejections across both runs

The same invalid input rows are rejected again on replay, so these are totals across both runs:

| Reason | Rows |
|---|---:|
| amount must be a decimal | 2 |
| conflicting duplicate customer key within input batch | 2 |
| customer source identity is conflicting in this batch | 12 |
| invalid ISO-8601 timestamp | 2 |
| invalid customer reference | 26 |
| missing required field: account_id | 2 |
| missing required field: customer_id | 2 |
| **Total** | **48** |

## Test results and known issue

The full suite most recently completed with **18 passed in 148.99 seconds**, including the clean 100K integration test. An intermittent `RuntimeError: Query interrupted` has occurred during Bronze ingestion in another run; later isolated and full-suite runs passed. Its trigger has not been established, so this result does not claim that intermittent issue has been diagnosed.

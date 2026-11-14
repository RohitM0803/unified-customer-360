"""Shared source schemas and DuckDB DDL."""

SOURCE_COLUMNS = {
    "customers": ("customer_id", "name", "email", "phone"),
    "accounts": ("account_id", "customer_id", "account_name", "status"),
    "orders": ("order_id", "customer_id", "order_ts", "amount"),
    "interactions": ("event_id", "customer_id", "event_ts", "event_type", "details"),
}

DDL = """
CREATE TABLE IF NOT EXISTS run_log (
  run_id VARCHAR PRIMARY KEY, source_system VARCHAR NOT NULL, started_at VARCHAR NOT NULL,
  ended_at VARCHAR, status VARCHAR NOT NULL,
  input_rows BIGINT DEFAULT 0, inserted_rows BIGINT DEFAULT 0,
  skipped_existing_rows BIGINT DEFAULT 0, skipped_duplicate_batch_rows BIGINT DEFAULT 0,
  rejected_rows BIGINT DEFAULT 0, unprocessed_rows BIGINT DEFAULT 0,
  output_rows BIGINT DEFAULT 0,
  bronze_seconds DOUBLE DEFAULT 0, silver_seconds DOUBLE DEFAULT 0,
  gold_seconds DOUBLE DEFAULT 0, total_seconds DOUBLE DEFAULT 0,
  failure_reason VARCHAR
);
CREATE TABLE IF NOT EXISTS bronze_file (
  run_id VARCHAR NOT NULL, source_system VARCHAR NOT NULL, source_file VARCHAR NOT NULL,
  columns_json VARCHAR NOT NULL, schema_valid BOOLEAN NOT NULL, sha256 VARCHAR NOT NULL,
  archived_path VARCHAR NOT NULL, ingested_at VARCHAR NOT NULL,
  PRIMARY KEY(run_id, source_file)
);
CREATE TABLE IF NOT EXISTS bronze_record (
  bronze_id VARCHAR PRIMARY KEY, entity VARCHAR NOT NULL, source_file VARCHAR NOT NULL,
  source_row BIGINT NOT NULL, source_system VARCHAR NOT NULL, run_id VARCHAR NOT NULL,
  ingested_at VARCHAR NOT NULL, raw_record_json VARCHAR NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_bronze_run_entity ON bronze_record(run_id, entity, source_row);
CREATE TABLE IF NOT EXISTS silver_customer (
  customer_sk VARCHAR PRIMARY KEY, match_key VARCHAR NOT NULL UNIQUE, name VARCHAR,
  name_norm VARCHAR, email VARCHAR, phone VARCHAR, match_method VARCHAR NOT NULL,
  uncertain_match BOOLEAN NOT NULL DEFAULT FALSE, created_at VARCHAR NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_silver_customer_email ON silver_customer(email);
CREATE INDEX IF NOT EXISTS ix_silver_customer_name_norm ON silver_customer(name_norm);
CREATE TABLE IF NOT EXISTS silver_customer_source (
  source_system VARCHAR NOT NULL, source_customer_id VARCHAR NOT NULL,
  customer_sk VARCHAR NOT NULL, payload_hash VARCHAR NOT NULL, source_file VARCHAR NOT NULL,
  source_row BIGINT NOT NULL, run_id VARCHAR NOT NULL,
  PRIMARY KEY(source_system, source_customer_id)
);
CREATE TABLE IF NOT EXISTS silver_account (
  source_system VARCHAR NOT NULL, account_id VARCHAR NOT NULL, customer_sk VARCHAR NOT NULL,
  account_name VARCHAR, status VARCHAR, payload_hash VARCHAR NOT NULL,
  source_file VARCHAR NOT NULL, source_row BIGINT NOT NULL, run_id VARCHAR NOT NULL,
  PRIMARY KEY(source_system, account_id)
);
CREATE TABLE IF NOT EXISTS silver_order (
  source_system VARCHAR NOT NULL, order_id VARCHAR NOT NULL, customer_sk VARCHAR NOT NULL,
  order_ts VARCHAR NOT NULL, amount VARCHAR NOT NULL, payload_hash VARCHAR NOT NULL,
  source_file VARCHAR NOT NULL, source_row BIGINT NOT NULL, run_id VARCHAR NOT NULL,
  PRIMARY KEY(source_system, order_id)
);
CREATE TABLE IF NOT EXISTS silver_interaction (
  source_system VARCHAR NOT NULL, event_id VARCHAR NOT NULL, customer_sk VARCHAR NOT NULL,
  event_ts VARCHAR NOT NULL, event_type VARCHAR NOT NULL, details VARCHAR,
  payload_hash VARCHAR NOT NULL, source_file VARCHAR NOT NULL, source_row BIGINT NOT NULL,
  run_id VARCHAR NOT NULL, PRIMARY KEY(source_system, event_id)
);
CREATE INDEX IF NOT EXISTS ix_silver_account_customer ON silver_account(customer_sk);
CREATE INDEX IF NOT EXISTS ix_silver_order_customer ON silver_order(customer_sk);
CREATE INDEX IF NOT EXISTS ix_silver_interaction_customer ON silver_interaction(customer_sk);
CREATE TABLE IF NOT EXISTS quality_issue (
  issue_id VARCHAR PRIMARY KEY, run_id VARCHAR NOT NULL, source_system VARCHAR NOT NULL,
  source_file VARCHAR NOT NULL, source_row BIGINT NOT NULL, entity VARCHAR NOT NULL,
  severity VARCHAR NOT NULL, reason VARCHAR NOT NULL, raw_record_json VARCHAR NOT NULL
);
CREATE TABLE IF NOT EXISTS rejected_record (
  reject_id VARCHAR PRIMARY KEY, run_id VARCHAR NOT NULL, source_system VARCHAR NOT NULL,
  source_file VARCHAR NOT NULL, source_row BIGINT NOT NULL, entity VARCHAR NOT NULL,
  reason VARCHAR NOT NULL, raw_record_json VARCHAR NOT NULL
);
CREATE TABLE IF NOT EXISTS uncertain_match_review (
  review_id VARCHAR PRIMARY KEY, run_id VARCHAR NOT NULL, source_system VARCHAR NOT NULL,
  source_customer_id VARCHAR NOT NULL, customer_sk VARCHAR NOT NULL,
  candidate_customer_sk VARCHAR NOT NULL, reason VARCHAR NOT NULL,
  review_status VARCHAR NOT NULL DEFAULT 'pending', created_at VARCHAR NOT NULL
);
CREATE TABLE IF NOT EXISTS gold_customer_master (
  customer_sk VARCHAR PRIMARY KEY, match_key VARCHAR NOT NULL, name VARCHAR, email VARCHAR,
  phone VARCHAR, match_method VARCHAR NOT NULL, uncertain_match BOOLEAN NOT NULL,
  review_status VARCHAR NOT NULL, identity_evidence_json VARCHAR NOT NULL,
  published_at VARCHAR NOT NULL
);
CREATE TABLE IF NOT EXISTS gold_customer_account (
  source_system VARCHAR NOT NULL, account_id VARCHAR NOT NULL, customer_sk VARCHAR NOT NULL,
  account_name VARCHAR, status VARCHAR, source_file VARCHAR NOT NULL,
  source_row BIGINT NOT NULL, run_id VARCHAR NOT NULL,
  PRIMARY KEY(source_system, account_id)
);
CREATE TABLE IF NOT EXISTS gold_customer_order_history (
  source_system VARCHAR NOT NULL, order_id VARCHAR NOT NULL, customer_sk VARCHAR NOT NULL,
  order_ts VARCHAR NOT NULL, amount VARCHAR NOT NULL, source_file VARCHAR NOT NULL,
  source_row BIGINT NOT NULL, run_id VARCHAR NOT NULL,
  PRIMARY KEY(source_system, order_id)
);
CREATE TABLE IF NOT EXISTS gold_customer_interaction_history (
  source_system VARCHAR NOT NULL, event_id VARCHAR NOT NULL, customer_sk VARCHAR NOT NULL,
  event_ts VARCHAR NOT NULL, event_type VARCHAR NOT NULL, details VARCHAR,
  source_file VARCHAR NOT NULL, source_row BIGINT NOT NULL, run_id VARCHAR NOT NULL,
  PRIMARY KEY(source_system, event_id)
);
CREATE OR REPLACE VIEW gold_customer_360 AS
SELECT m.customer_sk, m.name, m.email, m.phone, m.match_method, m.uncertain_match,
       m.review_status, COALESCE(a.account_count, 0) AS account_count,
       COALESCE(o.order_count, 0) AS order_count,
       COALESCE(o.total_order_amount, 0) AS total_order_amount,
       o.latest_order_ts, COALESCE(i.interaction_count, 0) AS interaction_count,
       i.latest_interaction_ts, m.identity_evidence_json
FROM gold_customer_master m
LEFT JOIN (SELECT customer_sk, count(*) AS account_count FROM gold_customer_account GROUP BY customer_sk) a USING(customer_sk)
LEFT JOIN (SELECT customer_sk, count(*) AS order_count,
                  sum(try_cast(amount AS DECIMAL(38, 9))) AS total_order_amount,
                  max(order_ts) AS latest_order_ts
           FROM gold_customer_order_history GROUP BY customer_sk) o USING(customer_sk)
LEFT JOIN (SELECT customer_sk, count(*) AS interaction_count,
                  max(event_ts) AS latest_interaction_ts
           FROM gold_customer_interaction_history GROUP BY customer_sk) i USING(customer_sk);
"""

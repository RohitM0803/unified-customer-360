"""Customer master, linked entity history, and Customer 360 publication."""
import json
from collections import defaultdict
from datetime import datetime, timezone


def publish_gold(conn):
    published_at = datetime.now(timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z")
    conn.execute("DELETE FROM gold_customer_account")
    conn.execute("DELETE FROM gold_customer_order_history")
    conn.execute("DELETE FROM gold_customer_interaction_history")
    conn.execute("DELETE FROM gold_customer_master")

    sources = defaultdict(list)
    for row in conn.execute(
        "SELECT customer_sk,source_system,source_customer_id,source_file,source_row,run_id "
        "FROM silver_customer_source ORDER BY customer_sk,source_system,source_customer_id").fetchall():
        sources[row[0]].append({"source_system": row[1], "source_customer_id": row[2],
                                "source_file": row[3], "source_row": row[4], "run_id": row[5]})
    reviews = defaultdict(list)
    for row in conn.execute(
        "SELECT customer_sk,candidate_customer_sk,source_system,source_customer_id,reason,review_status,run_id "
        "FROM uncertain_match_review ORDER BY created_at,review_id").fetchall():
        evidence = {"customer_sk": row[0], "candidate_customer_sk": row[1],
                    "source_system": row[2], "source_customer_id": row[3],
                    "reason": row[4], "review_status": row[5], "run_id": row[6]}
        reviews[row[0]].append(evidence)
        reviews[row[1]].append(evidence)

    master_rows = []
    for row in conn.execute(
        "SELECT customer_sk,match_key,name,email,phone,match_method,uncertain_match "
        "FROM silver_customer ORDER BY customer_sk").fetchall():
        evidence = json.dumps({
            "match_key": row[1], "match_method": row[5],
            "sources": sources[row[0]], "uncertain_matches": reviews[row[0]],
        }, ensure_ascii=False, sort_keys=True)
        review_status = "pending" if row[6] else "not_required"
        master_rows.append((row[0], row[1], row[2], row[3], row[4], row[5],
                            row[6], review_status, evidence, published_at))
    if master_rows:
        conn.executemany("INSERT INTO gold_customer_master VALUES (?,?,?,?,?,?,?,?,?,?)", master_rows)
    conn.execute("""INSERT INTO gold_customer_account
        SELECT source_system,account_id,customer_sk,account_name,status,source_file,source_row,run_id
        FROM silver_account""")
    conn.execute("""INSERT INTO gold_customer_order_history
        SELECT source_system,order_id,customer_sk,order_ts,amount,source_file,source_row,run_id
        FROM silver_order""")
    conn.execute("""INSERT INTO gold_customer_interaction_history
        SELECT source_system,event_id,customer_sk,event_ts,event_type,details,source_file,source_row,run_id
        FROM silver_interaction""")
    return len(master_rows)

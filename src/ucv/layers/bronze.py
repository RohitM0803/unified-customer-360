"""Immutable source-file and raw-row ingestion."""
import csv
import hashlib
import json
import re
import shutil
from datetime import datetime, timezone
from pathlib import Path
from ..schema import SOURCE_COLUMNS


def sha256_file(path):
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def ingest_bronze(conn, input_dir, run_id, source_system, raw_root):
    """Archive all source files and append their unmodified CSV string values."""
    input_dir, raw_root = Path(input_dir), Path(raw_root)
    if not re.fullmatch(r"[A-Za-z0-9_.-]+", source_system):
        raise ValueError("source_system may contain only letters, numbers, dot, underscore, and hyphen")
    files = {}
    for entity in SOURCE_COLUMNS:
        path = input_dir / f"{entity}.csv"
        if not path.is_file():
            raise FileNotFoundError(f"required source file not found: {path}")
        files[entity] = path

    archive_dir = raw_root / source_system / run_id
    archive_dir.mkdir(parents=True, exist_ok=False)
    batch = []
    metadata = []
    ingested_at = datetime.now(timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z")
    total = 0
    for entity, path in files.items():
        archived = archive_dir / path.name
        shutil.copyfile(path, archived)
        digest = sha256_file(archived)
        with path.open("r", encoding="utf-8-sig", newline="") as stream:
            reader = csv.DictReader(stream)
            headers = reader.fieldnames
            from ..quality import schema_problem
            problem = schema_problem(headers, entity)
            metadata.append((run_id, source_system, path.name, json.dumps(headers or []),
                             problem is None, digest, str(archived.resolve()), ingested_at))
            for rownum, row in enumerate(reader, start=2):
                raw = json.dumps(row, ensure_ascii=False, separators=(",", ":"))
                bronze_id = hashlib.sha256(f"{run_id}\0{path.name}\0{rownum}".encode()).hexdigest()
                batch.append((bronze_id, entity, path.name, rownum, source_system, run_id,
                              ingested_at, raw))
                total += 1
    conn.execute("BEGIN TRANSACTION")
    conn.executemany("INSERT INTO bronze_file VALUES (?, ?, ?, ?, ?, ?, ?, ?)", metadata)
    if batch:
        conn.executemany("INSERT INTO bronze_record VALUES (?, ?, ?, ?, ?, ?, ?, ?)", batch)
    return total


def validate_bronze_schemas(conn, run_id):
    from ..quality import schema_problem
    for entity, expected in SOURCE_COLUMNS.items():
        row = conn.execute("SELECT columns_json FROM bronze_file WHERE run_id=? AND source_file=?",
                           [run_id, entity + ".csv"]).fetchone()
        if row is None:
            raise ValueError(f"Bronze metadata missing for {entity}.csv")
        issue = schema_problem(json.loads(row[0]), entity)
        if issue:
            raise ValueError(f"{entity}.csv: {issue}")


def iter_bronze(conn, run_id, entity, batch_size=4096):
    cursor = conn.cursor()
    cursor.execute(
        "SELECT source_file,source_row,source_system,raw_record_json "
        "FROM bronze_record WHERE run_id=? AND entity=? ORDER BY source_row,bronze_id",
        [run_id, entity])
    while rows := cursor.fetchmany(batch_size):
        for row in rows:
            yield row

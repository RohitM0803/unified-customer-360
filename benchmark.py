"""Reproducible CSV/Parquet and DuckDB performance benchmark CLI."""
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent / "src"))
from ucv.benchmark import main

if __name__ == "__main__":
    raise SystemExit(main())

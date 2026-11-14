"""Command-line entry point for the DuckDB Customer 360 pipeline."""
import argparse
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent / "src"))
from ucv.pipeline import run_pipeline


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--database", type=Path, default=Path("database/customer360.duckdb"))
    parser.add_argument("--raw-root", type=Path, default=Path("data/raw/archive"))
    parser.add_argument("--source-system", default="default")
    parser.add_argument("--report-dir", type=Path, default=Path("output/duckdb_reports"))
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    report = run_pipeline(args.input, args.database, args.raw_root,
                          args.source_system, args.report_dir)
    print(report)
    return 0 if report["status"] == "succeeded" else 1


if __name__ == "__main__":
    raise SystemExit(main())

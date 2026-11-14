"""CLI for reproducible clean and messy dataset generation."""
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent / "src"))
from ucv.generator import main

if __name__ == "__main__":
    raise SystemExit(main())

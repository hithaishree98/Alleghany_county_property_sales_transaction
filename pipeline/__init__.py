"""Allegheny County real-estate-sales pipeline: landing -> bronze -> silver -> gold, on DuckDB."""

from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data"
DB_PATH = DATA_DIR / "warehouse.duckdb"

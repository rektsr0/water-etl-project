#!/usr/bin/env python3
"""
Export ETL tables to CSV for Power BI Desktop (Get Data > Text/CSV).

Reads from data/water.db (default) or PostgreSQL when WATER_ETL_USE_POSTGRES=1.
Run after: python main.py
"""

from __future__ import annotations

import os
import sqlite3
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import pandas as pd

from etl.config import DatabaseConfigError, psycopg2_kwargs


def _connect():
    use_pg = os.environ.get("WATER_ETL_USE_POSTGRES", "").lower() in ("1", "true", "yes")
    if use_pg:
        import psycopg2

        return psycopg2.connect(**psycopg2_kwargs())
    db = PROJECT_ROOT / "data" / "water.db"
    if not db.is_file():
        raise SystemExit(
            f"Database not found at {db}. Run `python main.py` first (or set WATER_ETL_USE_POSTGRES=1 with DB env vars)."
        )
    return sqlite3.connect(db)


def main() -> int:
    out_dir = PROJECT_ROOT / "data" / "exports"
    out_dir.mkdir(parents=True, exist_ok=True)

    try:
        conn = _connect()
    except DatabaseConfigError as e:
        print(e, file=sys.stderr)
        return 1

    try:
        pd.read_sql_query("SELECT * FROM sensor_data", conn).to_csv(
            out_dir / "sensor_data.csv", index=False
        )
        pd.read_sql_query("SELECT * FROM sensor_agg_by_location", conn).to_csv(
            out_dir / "sensor_agg_by_location.csv", index=False
        )
        pd.read_sql_query(
            """
            SELECT location, COUNT(*) AS leak_count
            FROM sensor_data
            WHERE is_leak = 1
            GROUP BY location
            """,
            conn,
        ).to_csv(out_dir / "leaks_by_location.csv", index=False)
    finally:
        conn.close()

    print(f"Exported CSVs to {out_dir.resolve()}")
    print("  - sensor_data.csv")
    print("  - sensor_agg_by_location.csv")
    print("  - leaks_by_location.csv")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

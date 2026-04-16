"""
Smart Water Infrastructure ETL — orchestrates extract, transform, load, and optional SQL analytics.
"""

from __future__ import annotations

import argparse
import logging
import os
import sqlite3
import sys
from pathlib import Path

from pyspark.sql import SparkSession

from etl.config import DatabaseConfigError, psycopg2_kwargs
from etl.extract import build_spark_session, extract
from etl.load import load
from etl.transform import transform

logger = logging.getLogger(__name__)
PROJECT_ROOT = Path(__file__).resolve().parent


def _configure_logging(verbose: bool) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    # Use stdout so log lines interleave correctly with print() (Spark also uses stderr for JVM logs).
    logging.basicConfig(
        level=level,
        format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        stream=sys.stdout,
    )


def run_pipeline(spark: SparkSession | None = None) -> Path | None:
    """Extract → transform → load. Stops Spark if this function created the session."""
    own_spark = spark is None
    if own_spark:
        spark = build_spark_session()

    try:
        logger.info("Starting water sensor ETL pipeline")
        df = extract(spark)
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug("Sample raw rows:")
            df.show(5, truncate=False)

        df_clean, df_agg = transform(df)
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug("Sample cleaned rows:")
            df_clean.show(10, truncate=False)
            logger.debug("Aggregates by location:")
            df_agg.show(truncate=False)

        db_path = load(df_clean, df_agg)
        logger.info("Loaded data successfully - pipeline steps complete")
        return db_path
    finally:
        if own_spark:
            spark.stop()
            logger.info("Spark session stopped")


def _read_sql_queries(sql_path: Path) -> list[str]:
    lines: list[str] = []
    for line in sql_path.read_text(encoding="utf-8").splitlines():
        if line.strip().startswith("--"):
            continue
        lines.append(line)
    text = "\n".join(lines)
    return [s.strip() for s in text.split(";") if s.strip()]


def run_analytics_sql(
    *,
    sqlite_path: Path | None = None,
    queries_path: Path | None = None,
) -> None:
    """Execute analytical queries from sql/queries.sql (default: SQLite after ETL)."""
    qp = queries_path or PROJECT_ROOT / "sql" / "queries.sql"
    statements = _read_sql_queries(qp)
    if not statements:
        logger.warning("No SQL statements found in %s", qp)
        return

    use_pg = os.environ.get("WATER_ETL_USE_POSTGRES", "").lower() in ("1", "true", "yes")
    if use_pg:
        try:
            import psycopg2  # type: ignore[import-untyped]
        except ImportError:
            logger.error("Install psycopg2-binary to run analytics against PostgreSQL, or unset WATER_ETL_USE_POSTGRES")
            return
        try:
            conn = psycopg2.connect(**psycopg2_kwargs())
        except DatabaseConfigError as e:
            logger.error("%s", e)
            return
        try:
            cur = conn.cursor()
            for stmt in statements:
                logger.info("Running query:\n%s", stmt)
                cur.execute(stmt)
                rows = cur.fetchall()
                colnames = [d[0] for d in cur.description] if cur.description else []
                print(_format_result(colnames, rows), end="", flush=True)
        finally:
            conn.close()
        return

    db = sqlite_path or PROJECT_ROOT / "data" / "water.db"
    if not db.is_file():
        logger.error("SQLite database not found at %s - run the pipeline first", db)
        return

    with sqlite3.connect(db) as conn:
        for stmt in statements:
            logger.info("Running query:\n%s", stmt)
            cur = conn.execute(stmt)
            colnames = [d[0] for d in cur.description] if cur.description else []
            rows = cur.fetchall()
            print(_format_result(colnames, rows), end="", flush=True)


def _format_result(colnames: list[str], rows: list) -> str:
    if not colnames:
        return "(no result)\n"
    header = " | ".join(colnames)
    sep = "-+-".join("-" * max(len(c), 3) for c in colnames)
    lines = [header, sep]
    for row in rows:
        lines.append(" | ".join(str(v) for v in row))
    return "\n".join(lines) + "\n"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Water sensor ETL and analytics")
    parser.add_argument("--analytics-only", action="store_true", help="Run sql/queries.sql only (no ETL)")
    parser.add_argument("-v", "--verbose", action="store_true", help="Debug logging")
    args = parser.parse_args(argv)

    _configure_logging(args.verbose)

    if args.analytics_only:
        run_analytics_sql()
        return 0

    db_path = run_pipeline()
    logger.info("")
    logger.info("--- Analytics (from sql/queries.sql) ---")
    run_analytics_sql(sqlite_path=db_path)
    return 0


if __name__ == "__main__":
    sys.exit(main())

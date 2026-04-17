"""
Smart Water Infrastructure ETL — medallion pipeline: bronze → silver → gold → SQL analytics.
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
from etl.load import load_medallion
from etl.transform import build_gold, build_silver

logger = logging.getLogger(__name__)
PROJECT_ROOT = Path(__file__).resolve().parent


def _configure_logging(verbose: bool) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        stream=sys.stdout,
    )


def run_pipeline(spark: SparkSession | None = None) -> Path | None:
    """
    extract (bronze) → build_silver → build_gold → load_medallion (bronze + silver + all gold tables to SQL).
    """
    own_spark = spark is None
    if own_spark:
        spark = build_spark_session()

    try:
        logger.info("Starting water sensor medallion pipeline")

        logger.info("Stage 1: Extract → bronze (raw + ingestion metadata)")
        df_bronze = extract(spark)
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug("Sample bronze rows:")
            df_bronze.show(5, truncate=False)

        logger.info("Stage 2: Transform bronze → silver (cleanse, types, is_leak)")
        df_silver = build_silver(df_bronze)
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug("Sample silver rows:")
            df_silver.show(10, truncate=False)

        logger.info("Stage 3: Aggregate silver → gold (reporting tables)")
        df_gold_agg, df_gold_leaks, df_gold_daily = build_gold(df_silver)
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug("Gold aggregates:")
            df_gold_agg.show(truncate=False)

        logger.info("Stage 4: Load bronze, silver, and gold to SQL")
        db_path = load_medallion(
            df_bronze,
            df_silver,
            df_gold_agg,
            df_gold_leaks,
            df_gold_daily,
        )
        logger.info("Pipeline complete - all medallion layers written")
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
    """Execute analytical queries from sql/queries.sql (gold-layer consumers)."""
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
    logger.info("--- Analytics (from sql/queries.sql, gold layer) ---")
    run_analytics_sql(sqlite_path=db_path)
    return 0


if __name__ == "__main__":
    sys.exit(main())

"""Load: persist cleaned and aggregate DataFrames to SQL (PostgreSQL via JDBC or local SQLite)."""

from __future__ import annotations

import logging
import os
import sqlite3
from pathlib import Path

from pyspark.sql import DataFrame

from etl.config import DatabaseConfigError, db_password, db_user, jdbc_url

logger = logging.getLogger(__name__)


def _project_root() -> Path:
    return Path(__file__).resolve().parent.parent


def load_jdbc_postgres(df: DataFrame, table: str, url: str, user: str, password: str, mode: str = "overwrite") -> None:
    """Write a DataFrame to PostgreSQL using Spark JDBC (resume-style pattern)."""
    logger.info("Writing table %s to PostgreSQL via JDBC", table)
    (
        df.write.format("jdbc")
        .option("url", url)
        .option("dbtable", table)
        .option("user", user)
        .option("password", password)
        .option("driver", "org.postgresql.Driver")
        .mode(mode)
        .save()
    )
    logger.info("Saved to %s", table)


def load_sqlite_pandas(df: DataFrame, db_path: Path, table: str) -> None:
    """Write to SQLite using Pandas (no extra JDBC jar; good for local demos)."""
    import pandas as pd

    db_path.parent.mkdir(parents=True, exist_ok=True)
    pdf = df.toPandas()
    with sqlite3.connect(db_path) as conn:
        pdf.to_sql(table, conn, if_exists="replace", index=False)
    logger.info("Wrote %s rows to SQLite %s :: %s", len(pdf), db_path, table)


def load(
    df_clean: DataFrame,
    df_agg: DataFrame,
    *,
    use_postgres: bool | None = None,
) -> Path | None:
    """
    Load cleaned sensor rows and aggregates into SQL.

    If ``WATER_ETL_USE_POSTGRES=1`` (or ``use_postgres=True``), uses JDBC to PostgreSQL.
    Otherwise writes SQLite files under ``data/water.db``.
    """
    postgres = use_postgres if use_postgres is not None else os.environ.get("WATER_ETL_USE_POSTGRES", "").lower() in (
        "1",
        "true",
        "yes",
    )

    if postgres:
        try:
            url = jdbc_url()
            user = db_user()
            password = db_password()
        except DatabaseConfigError as e:
            logger.error("%s", e)
            raise
        load_jdbc_postgres(df_clean, "sensor_data", url, user, password)
        load_jdbc_postgres(df_agg, "sensor_agg_by_location", url, user, password)
        return None

    db_path = _project_root() / "data" / "water.db"
    load_sqlite_pandas(df_clean, db_path, "sensor_data")
    load_sqlite_pandas(df_agg, db_path, "sensor_agg_by_location")
    logger.info("SQLite database ready at %s", db_path.resolve())
    return db_path

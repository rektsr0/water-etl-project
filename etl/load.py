"""Load: persist bronze, silver, and gold tables to SQL (PostgreSQL JDBC or SQLite)."""

from __future__ import annotations

import logging
import os
import sqlite3
from pathlib import Path

from pyspark.sql import DataFrame

from etl.config import DatabaseConfigError, db_password, db_user, jdbc_url

logger = logging.getLogger(__name__)

TABLE_BRONZE = "bronze_sensor_data"
TABLE_SILVER = "silver_sensor_data"
TABLE_GOLD_AGG = "gold_sensor_agg_by_location"
TABLE_GOLD_LEAKS = "gold_leaks_by_location"
TABLE_GOLD_DAILY = "gold_daily_pressure_summary"


def _project_root() -> Path:
    return Path(__file__).resolve().parent.parent


def load_jdbc_postgres(df: DataFrame, table: str, url: str, user: str, password: str, mode: str = "overwrite") -> None:
    """Write a DataFrame to PostgreSQL using Spark JDBC."""
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
    """Write to SQLite using Pandas."""
    import pandas as pd

    db_path.parent.mkdir(parents=True, exist_ok=True)
    pdf = df.toPandas()
    with sqlite3.connect(db_path) as conn:
        pdf.to_sql(table, conn, if_exists="replace", index=False)
    logger.info("Wrote %s rows to SQLite %s :: %s", len(pdf), db_path, table)


def _write_all_tables(
    df_bronze: DataFrame,
    df_silver: DataFrame,
    df_gold_agg: DataFrame,
    df_gold_leaks: DataFrame,
    df_gold_daily: DataFrame,
    *,
    postgres: bool,
    url: str | None,
    user: str | None,
    password: str | None,
) -> Path | None:
    if postgres:
        assert url and user and password
        load_jdbc_postgres(df_bronze, TABLE_BRONZE, url, user, password)
        load_jdbc_postgres(df_silver, TABLE_SILVER, url, user, password)
        load_jdbc_postgres(df_gold_agg, TABLE_GOLD_AGG, url, user, password)
        load_jdbc_postgres(df_gold_leaks, TABLE_GOLD_LEAKS, url, user, password)
        load_jdbc_postgres(df_gold_daily, TABLE_GOLD_DAILY, url, user, password)
        return None

    db_path = _project_root() / "data" / "water.db"
    load_sqlite_pandas(df_bronze, db_path, TABLE_BRONZE)
    load_sqlite_pandas(df_silver, db_path, TABLE_SILVER)
    load_sqlite_pandas(df_gold_agg, db_path, TABLE_GOLD_AGG)
    load_sqlite_pandas(df_gold_leaks, db_path, TABLE_GOLD_LEAKS)
    load_sqlite_pandas(df_gold_daily, db_path, TABLE_GOLD_DAILY)
    logger.info("SQLite medallion tables ready at %s", db_path.resolve())
    return db_path


def load_medallion(
    df_bronze: DataFrame,
    df_silver: DataFrame,
    df_gold_agg: DataFrame,
    df_gold_leaks: DataFrame,
    df_gold_daily: DataFrame,
    *,
    use_postgres: bool | None = None,
) -> Path | None:
    """
    Write bronze → silver → gold tables.

    Table names: ``bronze_sensor_data``, ``silver_sensor_data``,
    ``gold_sensor_agg_by_location``, ``gold_leaks_by_location``,
    ``gold_daily_pressure_summary``.
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
        return _write_all_tables(
            df_bronze,
            df_silver,
            df_gold_agg,
            df_gold_leaks,
            df_gold_daily,
            postgres=True,
            url=url,
            user=user,
            password=password,
        )

    return _write_all_tables(
        df_bronze,
        df_silver,
        df_gold_agg,
        df_gold_leaks,
        df_gold_daily,
        postgres=False,
        url=None,
        user=None,
        password=None,
    )

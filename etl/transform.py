"""Transform: silver (cleansed + typed + leak flag) and gold (reporting aggregates)."""

from __future__ import annotations

import logging

from pyspark.sql import DataFrame
from pyspark.sql.functions import avg, col, count, to_date

logger = logging.getLogger(__name__)


def build_silver(df_bronze: DataFrame) -> DataFrame:
    """
    Silver layer: validated sensor readings with consistent types and ``is_leak``.

    - Drops rows missing pressure, flow_rate, or location
    - Casts timestamp, pressure, flow_rate
    - Adds ``is_leak`` when pressure < 30 (demo threshold)
    - Preserves bronze lineage columns ``ingested_at``, ``source_file`` when present
    """
    df = df_bronze.dropna(subset=["pressure", "flow_rate", "location"])

    df = df.withColumn("timestamp", col("timestamp").cast("timestamp"))
    df = df.withColumn("flow_rate", col("flow_rate").cast("float"))
    df = df.withColumn("pressure", col("pressure").cast("float"))

    df = df.withColumn("is_leak", (col("pressure") < 30.0).cast("int"))

    row_count = df.count()
    leak_rows = df.filter(col("is_leak") == 1).count()
    logger.info("Silver: %s rows; is_leak=1 on %s rows", row_count, leak_rows)

    return df


def build_gold(df_silver: DataFrame) -> tuple[DataFrame, DataFrame, DataFrame]:
    """
    Gold layer: business-ready tables for SQL / Power BI.

    Returns
    -------
    gold_sensor_agg_by_location
        Per-site avg pressure/flow and ``reading_count`` (for weighted global averages).
    gold_leaks_by_location
        Count of leak-flagged readings per location.
    gold_daily_pressure_summary
        Calendar-day averages by location for trend / operational reporting.
    """
    df_agg = df_silver.groupBy("location").agg(
        avg("pressure").alias("avg_pressure"),
        avg("flow_rate").alias("avg_flow"),
        count("*").alias("reading_count"),
    )
    logger.info("Gold agg: %s locations", df_agg.count())

    df_leaks = (
        df_silver.filter(col("is_leak") == 1)
        .groupBy("location")
        .agg(count("*").alias("leak_count"))
    )
    logger.info("Gold leaks: %s locations with >=1 leak reading", df_leaks.count())

    df_daily = (
        df_silver.withColumn("reading_date", to_date(col("timestamp")))
        .groupBy("reading_date", "location")
        .agg(
            avg("pressure").alias("avg_pressure"),
            avg("flow_rate").alias("avg_flow"),
        )
    )
    logger.info("Gold daily: %s date/location buckets", df_daily.count())

    return df_agg, df_leaks, df_daily

"""Transform: silver (cleansed + typed + leak flag) and gold (reporting aggregates)."""

from __future__ import annotations

import logging

from pyspark.sql import DataFrame
from pyspark.sql.functions import avg, col, count, to_date

logger = logging.getLogger(__name__)


def build_silver(df_bronze: DataFrame) -> DataFrame:
    """Drop incomplete rows; cast types; ``is_leak`` = 1 when pressure < 30 psi."""
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
    """Returns ``(agg_by_location, leaks_by_location, daily_by_date_location)``."""
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

"""Transform: clean rows and engineer leak + aggregate features."""

from __future__ import annotations

import logging

from pyspark.sql import DataFrame
from pyspark.sql.functions import avg, col

logger = logging.getLogger(__name__)


def transform(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """
    Drop incomplete rows, cast flow_rate, flag low-pressure (possible leak) events,
    and aggregate averages by location.
    """
    df_clean = df.dropna(subset=["pressure", "flow_rate", "location"])

    df_clean = df_clean.withColumn("flow_rate", col("flow_rate").cast("float"))
    df_clean = df_clean.withColumn("pressure", col("pressure").cast("float"))

    # Possible leak: sustained or sudden low pressure (threshold from ops / engineering)
    df_clean = df_clean.withColumn("is_leak", (col("pressure") < 30.0).cast("int"))

    row_count = df_clean.count()
    leak_count = df_clean.filter(col("is_leak") == 1).count()
    logger.info("After cleaning: %s rows; flagged is_leak=%s on %s rows", row_count, leak_count, leak_count)

    df_agg = df_clean.groupBy("location").agg(
        avg("pressure").alias("avg_pressure"),
        avg("flow_rate").alias("avg_flow"),
    )
    logger.info("Built aggregates for %s locations", df_agg.count())

    return df_clean, df_agg

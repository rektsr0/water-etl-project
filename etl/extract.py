"""Extract: read raw sensor CSV into the bronze layer (minimal change from source)."""

from __future__ import annotations

import logging
import os
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import current_timestamp, lit

logger = logging.getLogger(__name__)


def _project_root() -> Path:
    return Path(__file__).resolve().parent.parent


def build_spark_session(app_name: str = "WaterETL") -> SparkSession:
    """Create a Spark session tuned for local batch ETL."""
    builder = (
        SparkSession.builder.appName(app_name)
        .master(os.environ.get("SPARK_MASTER", "local[*]"))
        .config("spark.sql.shuffle.partitions", os.environ.get("SPARK_SHUFFLE_PARTITIONS", "8"))
    )
    packages = os.environ.get("SPARK_JARS_PACKAGES", "").strip()
    if os.environ.get("WATER_ETL_USE_POSTGRES", "").lower() in ("1", "true", "yes") and not packages:
        packages = os.environ.get(
            "WATER_ETL_PG_JDBC_PACKAGE",
            "org.postgresql:postgresql:42.7.4",
        )
    if packages:
        builder = builder.config("spark.jars.packages", packages)
    return builder.getOrCreate()


def extract(spark: SparkSession, csv_path: str | os.PathLike[str] | None = None) -> DataFrame:
    """
    Bronze extract: load ``water_sensor.csv`` as-is (header + inferred types) plus ingestion metadata.

    No cleansing here beyond what Spark infers from the file. Downstream ``build_silver`` applies rules.
    """
    path = Path(csv_path) if csv_path is not None else _project_root() / "data" / "water_sensor.csv"
    resolved = path.resolve()
    resolved_str = str(resolved)
    logger.info("Bronze extract reading %s", resolved_str)

    df = spark.read.csv(resolved_str, header=True, inferSchema=True)
    df = df.withColumn("ingested_at", current_timestamp()).withColumn("source_file", lit(resolved_str))

    count = df.count()
    logger.info("Bronze: loaded %s raw rows", count)
    return df

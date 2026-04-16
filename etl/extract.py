"""Extract: read raw sensor CSV into a Spark DataFrame."""

from __future__ import annotations

import logging
import os
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession

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
    # JDBC to PostgreSQL needs the driver JAR (downloaded on first run when using Maven coords).
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
    Load water_sensor.csv with header and inferred types.

    Parameters
    ----------
    spark : SparkSession
    csv_path : optional path; defaults to ``data/water_sensor.csv`` under project root.
    """
    path = Path(csv_path) if csv_path is not None else _project_root() / "data" / "water_sensor.csv"
    resolved = path.resolve()
    logger.info("Reading sensor data from %s", resolved)

    df = spark.read.csv(str(resolved), header=True, inferSchema=True)
    count = df.count()
    logger.info("Loaded %s rows from extract", count)
    return df

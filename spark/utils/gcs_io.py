"""Spark ↔ GCS I/O helpers — session creation, partitioned reads, Parquet writes."""

import os
import logging

from pyspark.sql import SparkSession, DataFrame

logger = logging.getLogger(__name__)

GCS_BUCKET = os.environ.get("GCS_BUCKET", "eth-bigdata-project")


def create_spark_session(app_name: str) -> SparkSession:
    """Create a SparkSession configured with GCS connector and PostgreSQL JDBC."""
    gcs_project = os.environ.get("GCS_PROJECT_ID", "")
    gcs_creds = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", "")

    builder = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.jars", "/opt/bitnami/spark/jars/gcs-connector-hadoop3-latest.jar,"
                              "/opt/bitnami/spark/jars/postgresql-42.7.1.jar")
        .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
        .config("spark.hadoop.fs.AbstractFileSystem.gs.impl",
                "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
        .config("spark.hadoop.google.cloud.auth.service.account.enable", "true")
    )

    if gcs_project:
        builder = builder.config("spark.hadoop.fs.gs.project.id", gcs_project)
    if gcs_creds:
        builder = builder.config(
            "spark.hadoop.google.cloud.auth.service.account.json.keyfile", gcs_creds
        )

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    logger.info("SparkSession created: %s", app_name)
    return spark


def read_raw_partition(spark: SparkSession, date_str: str, bucket: str = GCS_BUCKET) -> DataFrame:
    """Read raw transaction Parquet for a single date partition."""
    path = f"gs://{bucket}/raw/transactions/dt={date_str}/"
    logger.info("Reading raw partition: %s", path)
    return spark.read.parquet(path)


def read_edge_aggregate(
    spark: SparkSession, run_date: str, bucket: str = GCS_BUCKET
) -> DataFrame | None:
    """Read previously computed edge aggregate for a given run_date.

    Returns None if the partition does not exist (first run).
    """
    path = f"gs://{bucket}/processed/graph_edges/run_date={run_date}/"
    try:
        df = spark.read.parquet(path)
        if df.head(1):
            logger.info("Read edge aggregate: %s (%d rows)", path, df.count())
            return df
    except Exception:
        pass
    logger.info("No edge aggregate found at %s", path)
    return None


def write_parquet(
    df: DataFrame,
    sub_path: str,
    bucket: str = GCS_BUCKET,
    mode: str = "overwrite",
) -> None:
    """Write DataFrame as Parquet to GCS."""
    path = f"gs://{bucket}/{sub_path}"
    df.write.mode(mode).parquet(path)
    logger.info("Wrote Parquet to %s (mode=%s)", path, mode)

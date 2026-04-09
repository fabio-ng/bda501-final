"""
SparkSession factory for GCS-enabled Spark jobs.
"""

import logging
import os
from typing import Dict, Optional

from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)


def create_spark_session(
    app_name: str,
    config: Optional[Dict[str, str]] = None,
    credentials_path: Optional[str] = None,
) -> SparkSession:
    """
    Create a SparkSession configured for GCS access.

    Args:
        app_name: Name of the Spark application
        config: Optional dictionary of additional Spark configurations
        credentials_path: Path to Google Cloud service account JSON
                         (defaults to GOOGLE_APPLICATION_CREDENTIALS env var)

    Returns:
        Configured SparkSession
    """
    if config is None:
        config = {}

    # Determine credentials path
    if credentials_path is None:
        credentials_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")

    # Base configuration
    spark_config = {
        # GCS connector via hadoop-gcs
        "spark.jars.packages": "com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.2.20",
        # GCS authentication
        "spark.hadoop.google.cloud.auth.service.account.enable": "true",
        # Merge with user-provided config
        **config,
    }

    # Add credentials path if available
    if credentials_path:
        spark_config["spark.hadoop.google.cloud.auth.service.account.json.keyfile"] = (
            credentials_path
        )
        logger.info(f"Using service account credentials from: {credentials_path}")

    # Build SparkSession
    builder = SparkSession.builder.appName(app_name)
    for key, value in spark_config.items():
        builder = builder.config(key, value)

    spark = builder.getOrCreate()
    logger.info(f"Created SparkSession: {app_name}")
    return spark


def stop_spark(spark: SparkSession) -> None:
    """
    Stop a SparkSession gracefully.

    Args:
        spark: SparkSession to stop
    """
    if spark:
        spark.stop()
        logger.info("SparkSession stopped")

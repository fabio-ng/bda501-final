"""Spark Job 2 — Incremental Edge Aggregation: 180-day sliding window.

Algorithm:
    edge_new = edge_prev + day_entering - day_exiting

This reads only 2 days of raw data instead of rescanning the full 180-day window,
reducing daily scan volume by ~90x.
"""

import argparse
import logging
import sys
from datetime import datetime, timedelta

from pyspark.sql import SparkSession, DataFrame, functions as F
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, LongType

from utils.gcs_io import (
    create_spark_session,
    read_raw_partition,
    read_edge_aggregate,
    write_parquet,
    GCS_BUCKET,
)
from utils.pg_writer import atomic_swap_edges

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger("incremental_edges")

# Schema for empty edge DataFrames (first run or missing partitions)
EDGE_SCHEMA = StructType([
    StructField("from_wallet", StringType(), False),
    StructField("to_wallet", StringType(), False),
    StructField("total_volume", DecimalType(38, 18), False),
    StructField("tx_count", LongType(), False),
])


def _aggregate_day(spark: SparkSession, date_str: str) -> DataFrame:
    """Read raw data for a single day and aggregate into (from, to) edge pairs."""
    try:
        raw = read_raw_partition(spark, date_str)
        if raw.head(1) is None:
            return spark.createDataFrame([], EDGE_SCHEMA)
    except Exception:
        logger.info("No raw partition for %s — returning empty edges", date_str)
        return spark.createDataFrame([], EDGE_SCHEMA)

    return (
        raw.groupBy(
            F.col("`from`").alias("from_wallet"),
            F.col("`to`").alias("to_wallet"),
        )
        .agg(
            F.sum(F.col("value_eth").cast("decimal(38,18)")).alias("total_volume"),
            F.count("*").alias("tx_count"),
        )
    )


def main(target_date: str):
    spark = create_spark_session("eth_incremental_edges")

    try:
        target = datetime.strptime(target_date, "%Y-%m-%d").date()
        prev_date = (target - timedelta(days=1)).isoformat()
        exit_date = (target - timedelta(days=180)).isoformat()
        period_start = (target - timedelta(days=179)).isoformat()

        logger.info(
            "Incremental edges: target=%s, prev_aggregate=%s, exiting=%s, window=[%s, %s]",
            target_date, prev_date, exit_date, period_start, target_date,
        )

        # ── Step 1: Read previous edge aggregate ─────
        prev = read_edge_aggregate(spark, prev_date)
        if prev is None:
            logger.info("No previous aggregate — starting from empty state")
            prev = spark.createDataFrame([], EDGE_SCHEMA)
        else:
            # Normalize column names (drop period metadata for merge)
            prev = prev.select(
                F.col("from_wallet"),
                F.col("to_wallet"),
                F.col("total_volume"),
                F.col("tx_count"),
            )

        # ── Step 2: Aggregate day entering window ─────
        entering = _aggregate_day(spark, target_date)
        logger.info("Day entering (%s): %d edge pairs", target_date, entering.count())

        # ── Step 3: Aggregate day exiting window ──────
        exiting = _aggregate_day(spark, exit_date)
        logger.info("Day exiting (%s): %d edge pairs", exit_date, exiting.count())

        # ── Step 4: Merge — prev + entering - exiting ─
        join_cols = ["from_wallet", "to_wallet"]

        merged = (
            prev.alias("p")
            .join(entering.alias("e"), on=join_cols, how="full_outer")
            .join(exiting.alias("x"), on=join_cols, how="full_outer")
            .select(
                F.col("from_wallet"),
                F.col("to_wallet"),
                (
                    F.coalesce(F.col("p.total_volume"), F.lit(0).cast("decimal(38,18)"))
                    + F.coalesce(F.col("e.total_volume"), F.lit(0).cast("decimal(38,18)"))
                    - F.coalesce(F.col("x.total_volume"), F.lit(0).cast("decimal(38,18)"))
                ).alias("total_volume"),
                (
                    F.coalesce(F.col("p.tx_count"), F.lit(0))
                    + F.coalesce(F.col("e.tx_count"), F.lit(0))
                    - F.coalesce(F.col("x.tx_count"), F.lit(0))
                ).alias("tx_count"),
            )
        )

        # ── Step 5: Remove edges with tx_count <= 0 ──
        result = merged.filter(F.col("tx_count") > 0)

        # ── Step 6: Add period metadata ───────────────
        result = (
            result
            .withColumn("period_start", F.lit(period_start).cast("date"))
            .withColumn("period_end", F.lit(target_date).cast("date"))
            .withColumn("updated_at", F.current_timestamp())
        )

        result_count = result.count()
        logger.info("Edge aggregate result: %d edges", result_count)

        # ── Step 7: Write to GCS (versioned) ──────────
        write_parquet(result, f"processed/graph_edges/run_date={target_date}")

        # ── Step 8: Write to PostgreSQL (atomic swap) ─
        # Select only the columns that match the PG table schema
        pg_df = result.select(
            "from_wallet", "to_wallet", "total_volume", "tx_count",
            "period_start", "period_end", "updated_at",
        )
        atomic_swap_edges(pg_df)

        logger.info("Incremental edge aggregation complete for %s", target_date)

    finally:
        spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Incremental 180-day edge aggregation")
    parser.add_argument("--target-date", required=True, help="Date to process (YYYY-MM-DD)")
    args = parser.parse_args()

    datetime.strptime(args.target_date, "%Y-%m-%d")
    main(args.target_date)

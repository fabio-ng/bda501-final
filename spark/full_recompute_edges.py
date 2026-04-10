"""Spark Job 3 — Full Recompute: rebuild 180-day edge aggregate from raw data.

Used for:
  - Initial bootstrap after BigQuery export
  - Disaster recovery if incremental state is corrupted
  - Correctness validation against incremental results
"""

import argparse
import logging
import sys
from datetime import datetime, timedelta

from pyspark.sql import functions as F

from utils.gcs_io import create_spark_session, write_parquet, GCS_BUCKET
from utils.pg_writer import atomic_swap_edges

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger("full_recompute_edges")


def main(end_date: str):
    spark = create_spark_session("eth_full_recompute_edges")

    try:
        end = datetime.strptime(end_date, "%Y-%m-%d").date()
        start = end - timedelta(days=179)

        logger.info("Full recompute: window [%s, %s] (180 days)", start, end)

        # ── Read all raw partitions in the 180-day window ──
        # Build list of partition paths (some may not exist)
        paths = []
        for day_offset in range(180):
            dt = start + timedelta(days=day_offset)
            paths.append(f"gs://{GCS_BUCKET}/raw/transactions/dt={dt.isoformat()}/")

        # Read with mergeSchema to handle minor schema differences
        # Use spark.read with a path list — Spark skips missing paths gracefully
        raw = spark.read.option("mergeSchema", "true").parquet(*paths)
        raw_count = raw.count()
        logger.info("Read %d total transactions across 180-day window", raw_count)

        if raw_count == 0:
            logger.warning("No data found in window — aborting recompute")
            return

        # ── Aggregate all edges ───────────────────────────
        edges = (
            raw.groupBy(
                F.col("`from`").alias("from_wallet"),
                F.col("`to`").alias("to_wallet"),
            )
            .agg(
                F.sum(F.col("value_eth").cast("decimal(38,18)")).alias("total_volume"),
                F.count("*").alias("tx_count"),
            )
            .filter(F.col("tx_count") > 0)
        )

        # ── Add period metadata ───────────────────────────
        result = (
            edges
            .withColumn("period_start", F.lit(start.isoformat()).cast("date"))
            .withColumn("period_end", F.lit(end_date).cast("date"))
            .withColumn("updated_at", F.current_timestamp())
        )

        result_count = result.count()
        logger.info("Full recompute result: %d edges", result_count)

        # ── Write to GCS ──────────────────────────────────
        write_parquet(result, f"processed/graph_edges/run_date={end_date}")

        # ── Write to PostgreSQL (atomic swap) ─────────────
        pg_df = result.select(
            "from_wallet", "to_wallet", "total_volume", "tx_count",
            "period_start", "period_end", "updated_at",
        )
        atomic_swap_edges(pg_df)

        logger.info("Full recompute complete for window ending %s", end_date)

    finally:
        spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Full 180-day edge recompute from raw data")
    parser.add_argument("--end-date", required=True, help="End date of 180-day window (YYYY-MM-DD)")
    args = parser.parse_args()

    datetime.strptime(args.end_date, "%Y-%m-%d")
    main(args.end_date)

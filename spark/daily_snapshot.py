"""Spark Job 1 — Daily Snapshot: compute top-100 wallets by transaction count."""

import argparse
import logging
import sys
from datetime import datetime

from pyspark.sql import functions as F
from pyspark.sql.window import Window

from utils.gcs_io import create_spark_session, read_raw_partition, write_parquet
from utils.pg_writer import upsert_snapshot

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger("daily_snapshot")


def main(target_date: str):
    spark = create_spark_session("eth_daily_snapshot")

    try:
        # Read raw transactions for target date
        raw = read_raw_partition(spark, target_date)
        raw_count = raw.count()
        logger.info("Raw transactions for %s: %d rows", target_date, raw_count)

        if raw_count == 0:
            logger.warning("No data for %s — skipping snapshot", target_date)
            return

        # ── Sent stats: aggregate by sender ───────────
        sent = (
            raw.groupBy(F.col("`from`").alias("wallet_address"))
            .agg(
                F.sum(F.col("value_eth").cast("decimal(38,18)")).alias("sent_eth"),
                F.count("*").alias("sent_count"),
            )
        )

        # ── Recv stats: aggregate by receiver ─────────
        recv = (
            raw.groupBy(F.col("`to`").alias("wallet_address"))
            .agg(
                F.sum(F.col("value_eth").cast("decimal(38,18)")).alias("recv_eth"),
                F.count("*").alias("recv_count"),
            )
        )

        # ── Full outer join + compute totals ──────────
        combined = (
            sent.join(recv, on="wallet_address", how="full_outer")
            .fillna(0, subset=["sent_eth", "recv_eth", "sent_count", "recv_count"])
            .withColumn("total_volume", F.col("sent_eth") + F.col("recv_eth"))
            .withColumn("total_txns", F.col("sent_count") + F.col("recv_count"))
        )

        # ── Rank by total_txns (DENSE_RANK) ──────────
        window = Window.orderBy(F.col("total_txns").desc())
        ranked = (
            combined
            .withColumn("rank", F.dense_rank().over(window))
            .filter(F.col("rank") <= 100)
        )

        # ── Add metadata columns ─────────────────────
        snapshot = (
            ranked
            .withColumn("snapshot_date", F.lit(target_date).cast("date"))
            .select(
                "wallet_address",
                "snapshot_date",
                "rank",
                "total_volume",
                "total_txns",
                "sent_eth",
                "recv_eth",
                "sent_count",
                "recv_count",
            )
        )

        result_count = snapshot.count()
        logger.info("Snapshot for %s: %d wallets", target_date, result_count)

        # ── Write to GCS ─────────────────────────────
        write_parquet(snapshot, f"processed/snapshots/dt={target_date}")

        # ── Write to PostgreSQL (upsert) ─────────────
        upsert_snapshot(snapshot)

        logger.info("Daily snapshot complete for %s", target_date)

    finally:
        spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Compute daily top-100 wallet snapshot")
    parser.add_argument("--target-date", required=True, help="Date to process (YYYY-MM-DD)")
    args = parser.parse_args()

    # Validate date format
    datetime.strptime(args.target_date, "%Y-%m-%d")
    main(args.target_date)

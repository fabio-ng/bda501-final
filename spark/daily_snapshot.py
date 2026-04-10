"""Spark Job 1 — Daily Snapshot: compute top-100 wallets by transaction count."""

import argparse
import logging
import sys
from datetime import datetime

from pyspark.sql import functions as F
from pyspark.sql.window import Window

from utils.gcs_io import (
    create_spark_session,
    list_raw_partition_dates,
    read_raw_partition,
    write_parquet,
)
from utils.pg_writer import existing_snapshot_dates, upsert_snapshot

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger("daily_snapshot")


def process_one_date(spark, target_date: str) -> None:
    """Compute snapshot for one date: GCS processed + Postgres upsert."""
    raw = read_raw_partition(spark, target_date)
    raw_count = raw.count()
    logger.info("Raw transactions for %s: %d rows", target_date, raw_count)

    if raw_count == 0:
        logger.warning("No data for %s — skipping snapshot", target_date)
        return

    sent = (
        raw.groupBy(F.col("`from`").alias("wallet_address"))
        .agg(
            F.sum(F.col("value_eth").cast("decimal(38,18)")).alias("sent_eth"),
            F.count("*").alias("sent_count"),
        )
    )

    recv = (
        raw.groupBy(F.col("`to`").alias("wallet_address"))
        .agg(
            F.sum(F.col("value_eth").cast("decimal(38,18)")).alias("recv_eth"),
            F.count("*").alias("recv_count"),
        )
    )

    combined = (
        sent.join(recv, on="wallet_address", how="full_outer")
        .fillna(0, subset=["sent_eth", "recv_eth", "sent_count", "recv_count"])
        .withColumn("total_volume", F.col("sent_eth") + F.col("recv_eth"))
        .withColumn("total_txns", F.col("sent_count") + F.col("recv_count"))
    )

    window = Window.orderBy(F.col("total_txns").desc())
    ranked = (
        combined.withColumn("rank", F.dense_rank().over(window)).filter(F.col("rank") <= 100)
    )

    snapshot = (
        ranked.withColumn("snapshot_date", F.lit(target_date).cast("date")).select(
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

    write_parquet(snapshot, f"processed/snapshots/dt={target_date}")
    upsert_snapshot(snapshot)
    logger.info("Daily snapshot complete for %s", target_date)


def main_single(target_date: str) -> None:
    spark = create_spark_session("eth_daily_snapshot")
    try:
        process_one_date(spark, target_date)
    finally:
        spark.stop()


def main_backfill(*, all_partitions: bool) -> None:
    """Process every raw dt=* on GCS, optionally skipping dates already in Postgres."""
    spark = create_spark_session("eth_daily_snapshot_backfill")
    try:
        dates = list_raw_partition_dates(spark)
        if not dates:
            logger.warning("No raw partitions found — nothing to do")
            return

        if not all_partitions:
            existing = existing_snapshot_dates()
            before = len(dates)
            dates = [d for d in dates if d not in existing]
            logger.info(
                "Backfill missing: %d of %d partition(s) need snapshot (skipping %d already in Postgres)",
                len(dates),
                before,
                before - len(dates),
            )
        else:
            logger.info("Backfill all: %d partition(s)", len(dates))

        failed: list[tuple[str, str]] = []
        for i, d in enumerate(dates, 1):
            logger.info("[%d/%d] Processing %s", i, len(dates), d)
            try:
                process_one_date(spark, d)
            except Exception as e:
                logger.exception("Failed snapshot for %s", d)
                failed.append((d, str(e)))

        if failed:
            for d, msg in failed:
                logger.error("%s: %s", d, msg)
            raise SystemExit(1)
    finally:
        spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Compute daily top-100 wallet snapshot (single day or backfill)"
    )
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--target-date", help="Single date (YYYY-MM-DD)")
    mode.add_argument(
        "--backfill-all",
        action="store_true",
        help="Recompute snapshot for every raw partition on GCS (upsert overwrites Postgres)",
    )
    mode.add_argument(
        "--backfill-missing",
        action="store_true",
        help="Process only raw partitions that have no rows yet in wallet_daily_snapshot",
    )
    args = parser.parse_args()

    if args.target_date:
        datetime.strptime(args.target_date, "%Y-%m-%d")
        main_single(args.target_date)
    elif args.backfill_all:
        main_backfill(all_partitions=True)
    else:
        main_backfill(all_partitions=False)

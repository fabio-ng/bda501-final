"""Spark Job 3 — Full Recompute: rebuild 180-day edge aggregate from raw data.

Used for:
  - Initial bootstrap after BigQuery export
  - Disaster recovery if incremental state is corrupted
  - Correctness validation against incremental results
"""

import argparse
import logging
import sys
import time
from datetime import datetime, timedelta

from pyspark.sql import functions as F
from py4j.java_gateway import java_import

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
    t0 = time.perf_counter()

    def elapsed() -> float:
        return time.perf_counter() - t0

    try:
        end = datetime.strptime(end_date, "%Y-%m-%d").date()
        start = end - timedelta(days=179)

        logger.info(
            "Full recompute: window [%s, %s] (180 days). "
            "Follow [1/7]…[7/7] steps below; Spark Master UI at http://localhost:8082 "
            "shows cluster apps only if you submit with --master spark://spark-master:7077.",
            start,
            end,
        )

        # ── Read existing raw partitions in the 180-day window ──
        # Spark fails if any provided path is missing, so probe first.
        logger.info(
            "[1/7] Probing GCS for existing partitions under gs://%s/raw/transactions/dt=*/ ...",
            GCS_BUCKET,
        )
        t_probe = time.perf_counter()
        jvm = spark._jvm
        hconf = spark._jsc.hadoopConfiguration()
        java_import(jvm, "org.apache.hadoop.fs.Path")
        java_import(jvm, "org.apache.hadoop.fs.FileSystem")

        existing_paths = []
        missing_dates = []
        for day_offset in range(180):
            dt = start + timedelta(days=day_offset)
            path = f"gs://{GCS_BUCKET}/raw/transactions/dt={dt.isoformat()}/"
            jpath = jvm.Path(path)
            fs = jvm.FileSystem.get(jpath.toUri(), hconf)
            if fs.exists(jpath):
                existing_paths.append(path)
            else:
                missing_dates.append(dt.isoformat())

        logger.info(
            "[1/7] Probe done: %d paths exist, %d missing (%.1fs).",
            len(existing_paths),
            len(missing_dates),
            time.perf_counter() - t_probe,
        )

        if missing_dates:
            logger.warning(
                "Missing %d partitions in window; skipping them. First few: %s",
                len(missing_dates),
                ", ".join(missing_dates[:10]),
            )

        if not existing_paths:
            logger.warning("No existing raw partitions found in window — aborting recompute")
            return

        # Read only existing partitions, keeping schema-merge for compatibility.
        logger.info(
            "[2/7] Parquet read plan built for %d partition(s) (lazy until count).",
            len(existing_paths),
        )
        t_read = time.perf_counter()
        raw = spark.read.option("mergeSchema", "true").parquet(*existing_paths)

        logger.info(
            "[3/7] Counting raw transactions (full GCS scan for selected days) — often the slowest step..."
        )
        raw_count = raw.count()
        logger.info(
            "[3/7] Raw rows: %d (step %.1fs, total %.1fs).",
            raw_count,
            time.perf_counter() - t_read,
            elapsed(),
        )

        if raw_count == 0:
            logger.warning("No data found in window — aborting recompute")
            return

        # ── Aggregate all edges ───────────────────────────
        logger.info("[4/7] Aggregating edges (groupBy + shuffle)...")
        t_agg = time.perf_counter()
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
        logger.info("[5/7] Counting distinct edges after aggregation...")
        result = (
            edges
            .withColumn("period_start", F.lit(start.isoformat()).cast("date"))
            .withColumn("period_end", F.lit(end_date).cast("date"))
            .withColumn("updated_at", F.current_timestamp())
        )

        result_count = result.count()
        logger.info(
            "[5/7] Edge rows: %d (aggregation step %.1fs, total %.1fs).",
            result_count,
            time.perf_counter() - t_agg,
            elapsed(),
        )

        # ── Write to GCS ──────────────────────────────────
        logger.info(
            "[6/7] Writing Parquet to gs://%s/processed/graph_edges/run_date=%s/ ...",
            GCS_BUCKET,
            end_date,
        )
        t_gcs = time.perf_counter()
        write_parquet(result, f"processed/graph_edges/run_date={end_date}")
        logger.info("[6/7] GCS write finished (%.1fs, total %.1fs).", time.perf_counter() - t_gcs, elapsed())

        # ── Write to PostgreSQL (atomic swap) ─────────────
        logger.info("[7/7] Writing to PostgreSQL (atomic_swap_edges)...")
        t_pg = time.perf_counter()
        pg_df = result.select(
            "from_wallet", "to_wallet", "total_volume", "tx_count",
            "period_start", "period_end", "updated_at",
        )
        atomic_swap_edges(pg_df)
        logger.info(
            "[7/7] PostgreSQL swap finished (%.1fs). Full recompute complete for %s in %.1fs total.",
            time.perf_counter() - t_pg,
            end_date,
            elapsed(),
        )

    finally:
        spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Full 180-day edge recompute from raw data")
    parser.add_argument("--end-date", required=True, help="End date of 180-day window (YYYY-MM-DD)")
    args = parser.parse_args()

    datetime.strptime(args.end_date, "%Y-%m-%d")
    main(args.end_date)

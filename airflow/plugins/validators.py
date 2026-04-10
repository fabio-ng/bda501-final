"""Airflow validation plugin — pre-Spark data quality checks on GCS raw partitions."""

import logging
from google.cloud import storage

logger = logging.getLogger(__name__)

# Expected daily row count bounds for ETH mainnet
MIN_ROWS_PER_DAY = 50_000
MAX_ROWS_PER_DAY = 500_000


def validate_raw_partition(bucket_name: str, date_str: str) -> dict:
    """Check that a GCS raw partition exists, is non-empty, and has plausible row counts.

    Args:
        bucket_name: GCS bucket name.
        date_str: Partition date (YYYY-MM-DD).

    Returns:
        {"passed": bool, "row_count": int | None, "message": str}
    """
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    prefix = f"raw/transactions/dt={date_str}/"

    blobs = list(bucket.list_blobs(prefix=prefix))
    parquet_blobs = [b for b in blobs if b.name.endswith(".parquet")]

    # Check 1: partition exists with Parquet files
    if not parquet_blobs:
        msg = f"No Parquet files found in gs://{bucket_name}/{prefix}"
        logger.error(msg)
        return {"passed": False, "row_count": None, "message": msg}

    # Check 2: no zero-byte files
    empty_files = [b.name for b in parquet_blobs if b.size == 0]
    if empty_files:
        msg = f"Found {len(empty_files)} empty Parquet files in partition {date_str}"
        logger.warning(msg)

    # Check 3: estimate row count from file sizes
    # Typical compressed Parquet row: ~150-250 bytes for ETH txns
    total_bytes = sum(b.size for b in parquet_blobs)
    estimated_rows = total_bytes // 200  # rough estimate

    if estimated_rows < MIN_ROWS_PER_DAY:
        msg = (
            f"Partition {date_str}: estimated {estimated_rows} rows "
            f"(< {MIN_ROWS_PER_DAY} minimum). Total size: {total_bytes} bytes, "
            f"{len(parquet_blobs)} files."
        )
        logger.warning(msg)
        return {"passed": False, "row_count": estimated_rows, "message": msg}

    if estimated_rows > MAX_ROWS_PER_DAY:
        msg = (
            f"Partition {date_str}: estimated {estimated_rows} rows "
            f"(> {MAX_ROWS_PER_DAY} maximum). Possible data anomaly."
        )
        logger.warning(msg)
        # Still passes — high count is suspicious but not blocking
        return {"passed": True, "row_count": estimated_rows, "message": msg}

    msg = (
        f"Partition {date_str}: OK — {len(parquet_blobs)} files, "
        f"~{estimated_rows} estimated rows, {total_bytes} bytes."
    )
    logger.info(msg)
    return {"passed": True, "row_count": estimated_rows, "message": msg}

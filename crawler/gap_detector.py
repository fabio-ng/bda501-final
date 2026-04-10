"""Gap detector — scans GCS raw partitions to find missing block ranges."""

import logging
from datetime import datetime, timedelta, timezone

import pyarrow.parquet as pq
from google.cloud import storage

logger = logging.getLogger(__name__)

# Only scan recent partitions to avoid full-history scan
DEFAULT_LOOKBACK_DAYS = 7


class GapDetector:
    """Identifies missing block ranges in GCS raw zone.

    On crawler startup, compares blocks in GCS against the expected
    continuous range [checkpoint, chain_head] and returns gaps to backfill.
    """

    def __init__(self, gcs_bucket: str, prefix: str = "raw/transactions"):
        self.client = storage.Client()
        self.bucket = self.client.bucket(gcs_bucket)
        self.prefix = prefix

    def detect_gaps(
        self,
        checkpoint_block: int,
        chain_head: int,
        lookback_days: int = DEFAULT_LOOKBACK_DAYS,
    ) -> list[tuple[int, int]]:
        """Find missing block ranges between checkpoint and chain_head.

        Only scans partitions within lookback_days of today to avoid
        scanning the entire GCS history on every startup.

        Returns:
            List of (start_block, end_block) tuples representing gaps.
        """
        if checkpoint_block >= chain_head:
            logger.info("No gap detection needed — checkpoint is at chain head")
            return []

        # Determine date range to scan
        today = datetime.now(tz=timezone.utc).date()
        start_date = today - timedelta(days=lookback_days)

        # Collect all block numbers from recent partitions
        covered_blocks = set()
        dates_scanned = 0

        for day_offset in range(lookback_days + 1):
            dt = start_date + timedelta(days=day_offset)
            date_str = dt.isoformat()
            partition_prefix = f"{self.prefix}/dt={date_str}/"

            blobs = list(self.bucket.list_blobs(prefix=partition_prefix))
            if not blobs:
                continue

            dates_scanned += 1
            for blob in blobs:
                if not blob.name.endswith(".parquet"):
                    continue
                try:
                    # Read only block_number column from Parquet metadata
                    uri = f"gs://{self.bucket.name}/{blob.name}"
                    pf = pq.ParquetFile(uri, filesystem=_get_gcs_fs())
                    table = pf.read(columns=["block_number"])
                    covered_blocks.update(table["block_number"].to_pylist())
                except Exception as e:
                    logger.warning("Failed to read %s: %s", blob.name, e)

        logger.info(
            "Gap detector scanned %d dates, found %d unique blocks",
            dates_scanned,
            len(covered_blocks),
        )

        # Find gaps in the range [checkpoint+1, chain_head]
        gaps = []
        gap_start = None

        for block_num in range(checkpoint_block + 1, chain_head + 1):
            if block_num not in covered_blocks:
                if gap_start is None:
                    gap_start = block_num
            else:
                if gap_start is not None:
                    gaps.append((gap_start, block_num - 1))
                    gap_start = None

        # Close trailing gap
        if gap_start is not None:
            gaps.append((gap_start, chain_head))

        if gaps:
            total_missing = sum(end - start + 1 for start, end in gaps)
            logger.warning(
                "Detected %d gaps (%d missing blocks): %s",
                len(gaps),
                total_missing,
                gaps[:5],  # log first 5 for brevity
            )
        else:
            logger.info("No gaps detected")

        return gaps


def _get_gcs_fs():
    """Lazy-load GCS filesystem for PyArrow."""
    import pyarrow.fs as pafs
    return pafs.GcsFileSystem()

"""Checkpoint manager — persists last_processed_block to local file + GCS."""

import os
import logging

from google.cloud import storage

logger = logging.getLogger(__name__)


class CheckpointManager:
    """Persists the crawler's last processed block number.

    Dual-write to local file (fast reads on restart) and GCS (disaster recovery).
    """

    def __init__(
        self,
        local_path: str = "checkpoints/last_block.txt",
        gcs_bucket: str | None = None,
        gcs_key: str = "checkpoints/ingest/last_block.txt",
    ):
        self.local_path = local_path
        self.gcs_bucket = gcs_bucket
        self.gcs_key = gcs_key
        self._gcs_client = None
        self._bucket = None

        # Ensure local directory exists
        os.makedirs(os.path.dirname(local_path), exist_ok=True)

        if gcs_bucket:
            self._gcs_client = storage.Client()
            self._bucket = self._gcs_client.bucket(gcs_bucket)

    def save(self, block_number: int) -> None:
        """Persist block number to local file and GCS."""
        block_str = str(block_number)

        # Local write
        with open(self.local_path, "w") as f:
            f.write(block_str)

        # GCS write
        if self._bucket:
            blob = self._bucket.blob(self.gcs_key)
            blob.upload_from_string(block_str)

        logger.debug("Checkpoint saved: block %d", block_number)

    def load(self) -> int:
        """Load last processed block. Try local → GCS → 0."""
        # Try local
        if os.path.exists(self.local_path):
            with open(self.local_path) as f:
                content = f.read().strip()
                if content.isdigit():
                    block = int(content)
                    logger.info("Checkpoint loaded from local: block %d", block)
                    return block

        # Try GCS
        if self._bucket:
            blob = self._bucket.blob(self.gcs_key)
            if blob.exists():
                content = blob.download_as_text().strip()
                if content.isdigit():
                    block = int(content)
                    logger.info("Checkpoint loaded from GCS: block %d", block)
                    # Sync to local for next restart
                    self.save(block)
                    return block

        logger.warning("No checkpoint found — starting from block 0")
        return 0

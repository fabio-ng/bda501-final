"""
Google Cloud Storage utilities for ETL pipeline.
"""

import logging
import os
from typing import List, Optional

import pandas as pd
import pyarrow.parquet as pq
from google.cloud import storage

logger = logging.getLogger(__name__)


class GCSClient:
    """Wrapper around Google Cloud Storage client."""

    def __init__(
        self,
        bucket_name: str,
        credentials_path: Optional[str] = None,
    ):
        """
        Initialize GCS client.

        Args:
            bucket_name: GCS bucket name (without 'gs://' prefix)
            credentials_path: Path to service account JSON
                             (defaults to GOOGLE_APPLICATION_CREDENTIALS env var)
        """
        self.bucket_name = bucket_name
        self.credentials_path = credentials_path or os.environ.get(
            "GOOGLE_APPLICATION_CREDENTIALS"
        )

        try:
            self.client = storage.Client()
            self.bucket = self.client.bucket(bucket_name)
            logger.info(f"Initialized GCSClient for bucket: {bucket_name}")
        except Exception as e:
            logger.error(f"Failed to initialize GCSClient: {e}")
            raise

    def upload_file(self, local_path: str, gcs_path: str) -> str:
        """
        Upload a local file to GCS.

        Args:
            local_path: Path to local file
            gcs_path: GCS path (without 'gs://' prefix)

        Returns:
            Full GCS URI (gs://bucket/path)
        """
        try:
            blob = self.bucket.blob(gcs_path)
            blob.upload_from_filename(local_path)
            full_uri = f"gs://{self.bucket_name}/{gcs_path}"
            logger.info(f"Uploaded {local_path} to {full_uri}")
            return full_uri
        except Exception as e:
            logger.error(f"Failed to upload {local_path}: {e}")
            raise

    def download_file(self, gcs_path: str, local_path: str) -> str:
        """
        Download a file from GCS.

        Args:
            gcs_path: GCS path (without 'gs://' prefix)
            local_path: Destination local path

        Returns:
            local_path
        """
        try:
            blob = self.bucket.blob(gcs_path)
            blob.download_to_filename(local_path)
            logger.info(f"Downloaded gs://{self.bucket_name}/{gcs_path} to {local_path}")
            return local_path
        except Exception as e:
            logger.error(f"Failed to download {gcs_path}: {e}")
            raise

    def list_blobs(self, prefix: str = "") -> List[str]:
        """
        List blobs in GCS bucket with optional prefix.

        Args:
            prefix: GCS path prefix to filter by

        Returns:
            List of blob names (paths)
        """
        try:
            blobs = self.client.list_blobs(self.bucket_name, prefix=prefix)
            blob_names = [blob.name for blob in blobs]
            logger.info(f"Listed {len(blob_names)} blobs with prefix '{prefix}'")
            return blob_names
        except Exception as e:
            logger.error(f"Failed to list blobs: {e}")
            raise

    def upload_dataframe_as_parquet(
        self,
        df: pd.DataFrame,
        gcs_path: str,
    ) -> str:
        """
        Write a pandas DataFrame as Parquet to GCS.

        Args:
            df: Pandas DataFrame
            gcs_path: GCS destination path (without 'gs://' prefix)

        Returns:
            Full GCS URI
        """
        try:
            # Write to temporary local file first
            temp_file = "/tmp/temp_parquet.parquet"
            df.to_parquet(temp_file, index=False, engine="pyarrow")

            # Upload to GCS
            full_uri = self.upload_file(temp_file, gcs_path)

            # Clean up
            os.remove(temp_file)
            logger.info(
                f"Uploaded DataFrame ({len(df)} rows) to {full_uri}"
            )
            return full_uri
        except Exception as e:
            logger.error(f"Failed to upload DataFrame: {e}")
            raise

    def read_parquet_from_gcs(self, gcs_path: str) -> pd.DataFrame:
        """
        Read a Parquet file from GCS into a pandas DataFrame.

        Args:
            gcs_path: GCS path (with or without 'gs://' prefix)

        Returns:
            Pandas DataFrame
        """
        try:
            # Normalize path
            if gcs_path.startswith("gs://"):
                gcs_path = gcs_path[5:]  # Remove 'gs://' prefix

            # Read directly from GCS
            full_uri = f"gs://{self.bucket_name}/{gcs_path}"
            df = pd.read_parquet(full_uri)
            logger.info(f"Read {len(df)} rows from {full_uri}")
            return df
        except Exception as e:
            logger.error(f"Failed to read Parquet from GCS: {e}")
            raise

    def blob_exists(self, gcs_path: str) -> bool:
        """
        Check if a blob exists in GCS.

        Args:
            gcs_path: GCS path (without 'gs://' prefix)

        Returns:
            True if blob exists, False otherwise
        """
        try:
            blob = self.bucket.blob(gcs_path)
            exists = blob.exists()
            logger.debug(f"Blob {gcs_path} exists: {exists}")
            return exists
        except Exception as e:
            logger.error(f"Failed to check blob existence: {e}")
            raise

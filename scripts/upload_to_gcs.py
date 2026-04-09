#!/usr/bin/env python3
"""
Upload local files to GCS buckets.

Usage:
    python scripts/upload_to_gcs.py data/raw/xblock/ gs://eth-phishing-raw/xblock/
    python scripts/upload_to_gcs.py model_artifacts/current/ gs://eth-phishing-models/models/current/
    python scripts/upload_to_gcs.py file.csv gs://eth-phishing-raw/transactions/ --no-overwrite
"""

import os
import sys
import argparse
import logging
from pathlib import Path
from typing import Optional

try:
    from google.cloud import storage
    from google.api_core.exceptions import GoogleAPIError
    HAS_GCS = True
except ImportError:
    HAS_GCS = False

try:
    from tqdm import tqdm
except ImportError:
    tqdm = None

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class GCSUploader:
    """Upload files to Google Cloud Storage."""

    def __init__(self, credentials_path: Optional[str] = None):
        """
        Initialize GCS uploader.

        Args:
            credentials_path: Path to GCP service account JSON
        """
        if not HAS_GCS:
            raise ImportError("google-cloud-storage not installed. Install with: pip install google-cloud-storage")

        if credentials_path:
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = credentials_path

        self.client = storage.Client()
        logger.info("GCS client initialized")

    def _parse_gcs_uri(self, uri: str) -> tuple:
        """
        Parse GCS URI to bucket and prefix.

        Args:
            uri: GCS URI (e.g., gs://bucket/prefix/)

        Returns:
            Tuple of (bucket_name, prefix)
        """
        if not uri.startswith("gs://"):
            raise ValueError(f"Invalid GCS URI: {uri}")

        # Remove gs://
        parts = uri[5:].rstrip("/").split("/", 1)
        bucket = parts[0]
        prefix = parts[1] if len(parts) > 1 else ""

        return bucket, prefix

    def _get_bucket(self, bucket_name: str) -> storage.Bucket:
        """Get or create bucket reference."""
        return self.client.bucket(bucket_name)

    def upload_file(
        self,
        local_path: str,
        gcs_uri: str,
        no_overwrite: bool = False,
    ) -> bool:
        """
        Upload a single file to GCS.

        Args:
            local_path: Local file path
            gcs_uri: GCS destination (gs://bucket/path/ or gs://bucket/object)
            no_overwrite: Skip if file exists

        Returns:
            bool: True if successful
        """
        if not os.path.exists(local_path):
            logger.error(f"Local file not found: {local_path}")
            return False

        bucket_name, prefix = self._parse_gcs_uri(gcs_uri)
        bucket = self._get_bucket(bucket_name)

        # Determine blob name
        if gcs_uri.endswith("/"):
            # Directory target
            filename = os.path.basename(local_path)
            blob_name = f"{prefix}/{filename}".lstrip("/")
        else:
            # File target
            blob_name = prefix

        blob = bucket.blob(blob_name)

        # Check if exists
        if no_overwrite and blob.exists():
            logger.info(f"Skipping (already exists): gs://{bucket_name}/{blob_name}")
            return True

        try:
            logger.info(f"Uploading: {local_path} → gs://{bucket_name}/{blob_name}")
            blob.upload_from_filename(local_path)
            logger.info(f"✓ Uploaded: gs://{bucket_name}/{blob_name}")
            return True
        except GoogleAPIError as e:
            logger.error(f"Failed to upload: {e}")
            return False

    def upload_directory(
        self,
        local_dir: str,
        gcs_uri: str,
        no_overwrite: bool = False,
        recursive: bool = True,
    ) -> int:
        """
        Upload directory contents to GCS.

        Args:
            local_dir: Local directory path
            gcs_uri: GCS destination (should end with /)
            no_overwrite: Skip files that exist
            recursive: Include subdirectories

        Returns:
            int: Number of files uploaded
        """
        if not os.path.isdir(local_dir):
            logger.error(f"Local directory not found: {local_dir}")
            return 0

        if not gcs_uri.endswith("/"):
            gcs_uri += "/"

        bucket_name, prefix = self._parse_gcs_uri(gcs_uri)
        bucket = self._get_bucket(bucket_name)

        # Get list of files
        local_path_obj = Path(local_dir)
        if recursive:
            files = list(local_path_obj.rglob("*"))
        else:
            files = list(local_path_obj.glob("*"))

        files = [f for f in files if f.is_file()]
        logger.info(f"Found {len(files)} files to upload")

        if not files:
            logger.warning("No files found to upload")
            return 0

        uploaded = 0
        failed = 0

        # Progress bar if tqdm available
        iterator = tqdm(files, unit="file") if tqdm else files

        for local_file in iterator:
            # Compute relative path
            rel_path = local_file.relative_to(local_path_obj)
            blob_name = f"{prefix.rstrip('/')}/{str(rel_path).replace(chr(92), '/')}"  # Handle Windows paths
            blob_name = blob_name.lstrip("/")

            blob = bucket.blob(blob_name)

            # Check if exists
            if no_overwrite and blob.exists():
                if not tqdm:
                    logger.info(f"Skipping (already exists): gs://{bucket_name}/{blob_name}")
                continue

            try:
                blob.upload_from_filename(str(local_file))
                uploaded += 1
                if not tqdm:
                    logger.info(f"✓ Uploaded: gs://{bucket_name}/{blob_name}")
            except GoogleAPIError as e:
                logger.error(f"Failed to upload {local_file}: {e}")
                failed += 1

        logger.info(f"\nUpload summary:")
        logger.info(f"  Uploaded: {uploaded} files")
        logger.info(f"  Failed: {failed} files")
        logger.info(f"  Destination: gs://{bucket_name}/{prefix}")

        return uploaded


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Upload files to Google Cloud Storage",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )

    parser.add_argument(
        "source_path",
        type=str,
        help="Local file or directory to upload",
    )
    parser.add_argument(
        "gcs_uri",
        type=str,
        help="GCS destination (gs://bucket/path/)",
    )
    parser.add_argument(
        "--no-overwrite",
        action="store_true",
        help="Skip files that already exist in GCS",
    )
    parser.add_argument(
        "--credentials",
        type=str,
        help="Path to GCP service account JSON",
    )

    args = parser.parse_args()

    # Initialize uploader
    try:
        uploader = GCSUploader(credentials_path=args.credentials)
    except ImportError as e:
        logger.error(f"Failed to initialize GCS client: {e}")
        sys.exit(1)

    # Upload file or directory
    if os.path.isfile(args.source_path):
        success = uploader.upload_file(
            args.source_path,
            args.gcs_uri,
            no_overwrite=args.no_overwrite,
        )
        sys.exit(0 if success else 1)
    elif os.path.isdir(args.source_path):
        uploaded = uploader.upload_directory(
            args.source_path,
            args.gcs_uri,
            no_overwrite=args.no_overwrite,
        )
        sys.exit(0 if uploaded > 0 else 1)
    else:
        logger.error(f"Source path not found: {args.source_path}")
        sys.exit(1)


if __name__ == "__main__":
    main()

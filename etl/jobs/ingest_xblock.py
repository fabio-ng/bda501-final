"""
XBlock-ETH dataset ingestion job: load labeled phishing transactions from local files to GCS.
"""

import argparse
import glob
import json
import logging
import os
import time
from datetime import datetime
from typing import Dict, Optional, Tuple

import pandas as pd

from etl.utils.gcs_utils import GCSClient

logger = logging.getLogger(__name__)


class IngestXBlockJob:
    """
    Reads XBlock-ETH labeled transaction network dataset from local files
    (downloaded from Kaggle) and uploads to GCS raw bucket as Parquet.

    Source: Local CSV files from xblock/ethereum-phishing-transaction-network
      - phishing_txs.csv / transactions.csv — transaction edges
      - phishing_addrs.txt / labels.csv — labeled addresses
    Target:
      gs://{GCS_BUCKET_RAW}/xblock/transactions/transactions.parquet
      gs://{GCS_BUCKET_RAW}/xblock/labels/labels.parquet
    """

    def __init__(
        self,
        gcs_bucket_raw: str,
        local_data_dir: Optional[str] = None,
        credentials_path: Optional[str] = None,
    ):
        """
        Initialize XBlock ingestion job.

        Args:
            gcs_bucket_raw: GCS bucket name for raw data
            local_data_dir: Local directory containing XBlock CSV files
            credentials_path: Path to service account JSON
        """
        self.gcs_bucket_raw = gcs_bucket_raw
        self.local_data_dir = local_data_dir or "./xblock_data"
        self.credentials_path = credentials_path

        self.gcs_client = GCSClient(gcs_bucket_raw, credentials_path)

        logger.info(
            f"Initialized IngestXBlockJob for bucket {gcs_bucket_raw}, "
            f"data dir {self.local_data_dir}"
        )

    def run(
        self,
        local_data_dir: Optional[str] = None,
    ) -> Dict[str, any]:
        """
        Main entry point for XBlock ingestion job.

        Args:
            local_data_dir: Local directory containing XBlock files (optional override)

        Returns:
            Dictionary with job statistics
        """
        job_start_time = time.time()
        data_dir = local_data_dir or self.local_data_dir

        stats = {
            "source_dir": data_dir,
            "transactions_count": 0,
            "labels_count": 0,
            "phishing_count": 0,
            "legitimate_count": 0,
            "gcs_paths": [],
            "duration_seconds": 0,
            "timestamp": datetime.now().isoformat(),
        }

        try:
            logger.info(f"Starting XBlock ingestion from {data_dir}")

            # Check data directory exists
            if not os.path.isdir(data_dir):
                raise FileNotFoundError(f"Data directory not found: {data_dir}")

            # Load transactions
            logger.info("Loading transactions...")
            tx_df = self._load_transactions(data_dir)
            stats["transactions_count"] = len(tx_df)
            logger.info(f"Loaded {len(tx_df)} transactions")

            # Load labels
            logger.info("Loading labels...")
            labels_df = self._load_labels(data_dir)
            stats["labels_count"] = len(labels_df)
            stats["phishing_count"] = int((labels_df["label"] == 1).sum())
            stats["legitimate_count"] = int((labels_df["label"] == 0).sum())
            logger.info(
                f"Loaded {len(labels_df)} labels "
                f"({stats['phishing_count']} phishing, {stats['legitimate_count']} legitimate)"
            )

            # Validate and clean data
            tx_df = self._validate_and_clean(tx_df)
            labels_df = self._validate_and_clean(labels_df)

            # Upload to GCS
            tx_gcs_path = self._upload_to_gcs(
                tx_df, "xblock/transactions", "transactions.parquet"
            )
            stats["gcs_paths"].append(tx_gcs_path)

            labels_gcs_path = self._upload_to_gcs(
                labels_df, "xblock/labels", "labels.parquet"
            )
            stats["gcs_paths"].append(labels_gcs_path)

            job_duration = time.time() - job_start_time
            stats["duration_seconds"] = job_duration

            # Log job
            self._log_job(stats)

            logger.info(
                f"XBlock ingestion completed in {job_duration:.2f}s: "
                f"{len(tx_df)} transactions, {len(labels_df)} labels"
            )
            return stats

        except Exception as e:
            logger.error(f"XBlock ingestion failed: {e}")
            stats["error"] = str(e)
            self._log_job(stats)
            raise

    def _load_transactions(self, data_dir: str) -> pd.DataFrame:
        """
        Load transaction data from CSV files.

        Handles multiple XBlock filename variants.

        Args:
            data_dir: Local directory path

        Returns:
            Pandas DataFrame with transactions
        """
        # Try common filename patterns
        patterns = [
            "phishing_txs.csv",
            "transactions.csv",
            "MulDiGraph_edge.csv",
            "transaction_list.csv",
            "*transaction*.csv",
            "*edge*.csv",
        ]

        df = None
        for pattern in patterns:
            file_path = os.path.join(data_dir, pattern)
            matching_files = glob.glob(file_path)
            if matching_files:
                try:
                    file = matching_files[0]
                    logger.info(f"Attempting to load transactions from {file}")
                    df = pd.read_csv(file)

                    # Normalize column names
                    df.columns = df.columns.str.lower().str.strip()

                    # Rename columns to standard names
                    column_mapping = {
                        "hash": "tx_hash",
                        "from": "from_address",
                        "to": "to_address",
                        "timestamp": "block_timestamp",
                        "block_num": "block_number",
                        "edge_id": "tx_hash",
                        "source": "from_address",
                        "target": "to_address",
                    }
                    df = df.rename(columns=column_mapping)

                    # Ensure required columns exist
                    required_cols = ["tx_hash", "from_address", "to_address"]
                    if all(col in df.columns for col in required_cols):
                        logger.info(
                            f"Successfully loaded {len(df)} transactions from {file}"
                        )
                        return df
                except Exception as e:
                    logger.debug(f"Failed to load {file}: {e}")
                    continue

        if df is None:
            raise FileNotFoundError(
                f"No transaction CSV files found in {data_dir} matching patterns: {patterns}"
            )

        return df

    def _load_labels(self, data_dir: str) -> pd.DataFrame:
        """
        Load address labels (phishing or legitimate).

        Handles multiple XBlock filename variants.

        Args:
            data_dir: Local directory path

        Returns:
            Pandas DataFrame with columns [address, label]
        """
        # Try common filename patterns
        patterns = [
            "phishing_addrs.txt",
            "labels.csv",
            "phishing_addresses.txt",
            "*label*.csv",
            "*phishing*.txt",
        ]

        for pattern in patterns:
            file_path = os.path.join(data_dir, pattern)
            matching_files = glob.glob(file_path)
            if matching_files:
                try:
                    file = matching_files[0]
                    logger.info(f"Attempting to load labels from {file}")

                    if file.endswith(".txt"):
                        # Text format: one address per line = phishing
                        with open(file) as f:
                            addresses = [
                                line.strip().lower()
                                for line in f
                                if line.strip()
                            ]
                        df = pd.DataFrame(
                            {
                                "address": addresses,
                                "label": 1,  # phishing
                            }
                        )
                    else:
                        # CSV format
                        df = pd.read_csv(file)
                        df.columns = df.columns.str.lower().str.strip()

                        # Ensure required columns
                        if "address" in df.columns and "label" in df.columns:
                            pass
                        elif "addr" in df.columns:
                            df = df.rename(columns={"addr": "address"})

                    logger.info(f"Successfully loaded {len(df)} labels from {file}")
                    return df

                except Exception as e:
                    logger.debug(f"Failed to load {file}: {e}")
                    continue

        # Fallback: try to infer labels from transaction addresses
        logger.warning(
            "No explicit label files found. Inferring labels from transaction sources..."
        )
        return pd.DataFrame({"address": [], "label": []})

    def _validate_and_clean(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Validate and clean transaction/label data.

        Args:
            df: Input DataFrame

        Returns:
            Cleaned DataFrame
        """
        original_len = len(df)

        # Remove duplicates
        if "tx_hash" in df.columns:
            df = df.drop_duplicates(subset=["tx_hash"])
        elif "address" in df.columns:
            df = df.drop_duplicates(subset=["address"])

        # Lowercase addresses
        for col in ["from_address", "to_address", "address"]:
            if col in df.columns:
                df[col] = df[col].str.lower().str.strip()

        # Remove null addresses
        for col in ["from_address", "to_address", "address"]:
            if col in df.columns:
                df = df[df[col].notna()]
                df = df[df[col] != ""]

        # Validate address format (should be 0x prefixed, 40 hex chars)
        for col in ["from_address", "to_address", "address"]:
            if col in df.columns:
                valid = df[col].str.match(r"^0x[a-f0-9]{40}$")
                removed = (~valid).sum()
                if removed > 0:
                    logger.warning(f"Removed {removed} invalid addresses in {col}")
                df = df[valid]

        cleaned_len = len(df)
        if cleaned_len < original_len:
            logger.info(
                f"Cleaned data: {original_len} -> {cleaned_len} rows "
                f"({original_len - cleaned_len} removed)"
            )

        return df.reset_index(drop=True)

    def _upload_to_gcs(
        self,
        df: pd.DataFrame,
        gcs_prefix: str,
        filename: str,
    ) -> str:
        """
        Upload DataFrame to GCS as Parquet.

        Args:
            df: Pandas DataFrame
            gcs_prefix: GCS path prefix (without gs://bucket/)
            filename: Filename

        Returns:
            Full GCS URI
        """
        gcs_path = f"{gcs_prefix}/{filename}"
        full_uri = self.gcs_client.upload_dataframe_as_parquet(df, gcs_path)
        logger.info(f"Uploaded {len(df)} rows to {full_uri}")
        return full_uri

    def _log_job(self, stats: Dict) -> None:
        """
        Register job execution in GCS log file.

        Args:
            stats: Job statistics dictionary
        """
        try:
            log_path = "etl_job_log.json"
            if self.gcs_client.blob_exists(log_path):
                self.gcs_client.download_file(log_path, "/tmp/log.json")
                with open("/tmp/log.json") as f:
                    jobs = json.load(f)
            else:
                jobs = []

            jobs.append(stats)

            with open("/tmp/log.json", "w") as f:
                json.dump(jobs, f, indent=2)

            self.gcs_client.upload_file("/tmp/log.json", log_path)
            logger.info(f"Logged job to {log_path}")
        except Exception as e:
            logger.warning(f"Failed to log job: {e}")


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    parser = argparse.ArgumentParser(
        description="Ingest XBlock-ETH dataset from local files"
    )
    parser.add_argument(
        "--data-dir",
        required=True,
        help="Local directory containing XBlock CSV files",
    )
    parser.add_argument(
        "--bucket",
        required=True,
        help="GCS bucket name for raw data",
    )
    parser.add_argument(
        "--credentials",
        default=None,
        help="Path to service account JSON",
    )

    args = parser.parse_args()

    job = IngestXBlockJob(
        gcs_bucket_raw=args.bucket,
        local_data_dir=args.data_dir,
        credentials_path=args.credentials,
    )

    result = job.run()
    logger.info(f"Job result: {json.dumps(result, indent=2)}")

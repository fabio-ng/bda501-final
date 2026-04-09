"""
Feature engineering job: compute address-level features from raw transactions.
"""

import argparse
import json
import logging
import os
import pickle
import time
from datetime import datetime
from typing import Dict, Optional, Tuple

import numpy as np
import pandas as pd
from sklearn.preprocessing import StandardScaler

from etl.schemas.feature_schema import NODE_FEATURE_NAMES
from etl.utils.gcs_utils import GCSClient

logger = logging.getLogger(__name__)

# Wei to ETH conversion
WEI_TO_ETH = 1e-18


class ProcessFeaturesJob:
    """
    Reads raw transactions from GCS, computes 12-dimensional node feature vectors
    per Ethereum address, joins with labels, and saves to GCS processed bucket.

    Source: gs://{GCS_BUCKET_RAW}/
    Target:
      gs://{GCS_BUCKET_PROCESSED}/features/node_features.parquet
      gs://{GCS_BUCKET_PROCESSED}/features/labels.parquet
      gs://{GCS_BUCKET_PROCESSED}/features/edge_index.parquet
    """

    def __init__(
        self,
        gcs_bucket_raw: str,
        gcs_bucket_processed: str,
        credentials_path: Optional[str] = None,
    ):
        """
        Initialize feature processing job.

        Args:
            gcs_bucket_raw: GCS bucket with raw data
            gcs_bucket_processed: GCS bucket for processed data
            credentials_path: Path to service account JSON
        """
        self.gcs_bucket_raw = gcs_bucket_raw
        self.gcs_bucket_processed = gcs_bucket_processed
        self.credentials_path = credentials_path

        self.gcs_raw = GCSClient(gcs_bucket_raw, credentials_path)
        self.gcs_processed = GCSClient(gcs_bucket_processed, credentials_path)

        logger.info(
            f"Initialized ProcessFeaturesJob: raw={gcs_bucket_raw}, "
            f"processed={gcs_bucket_processed}"
        )

    def run(
        self,
        source: str = "xblock",
        use_spark: bool = False,
    ) -> Dict[str, any]:
        """
        Main entry point for feature processing job.

        Args:
            source: Data source ('xblock', 'bigquery', or 'both')
            use_spark: Whether to use Spark for large datasets (default False)

        Returns:
            Dictionary with job statistics
        """
        job_start_time = time.time()

        stats = {
            "source": source,
            "use_spark": use_spark,
            "addresses_count": 0,
            "transactions_count": 0,
            "phishing_count": 0,
            "legitimate_count": 0,
            "unknown_count": 0,
            "gcs_paths": [],
            "duration_seconds": 0,
            "timestamp": datetime.now().isoformat(),
        }

        try:
            logger.info(f"Starting feature processing from {source}")

            # Load transactions
            logger.info("Loading transactions...")
            tx_df = self._load_transactions(source)
            stats["transactions_count"] = len(tx_df)
            logger.info(f"Loaded {len(tx_df)} transactions")

            # Load labels
            logger.info("Loading labels...")
            labels_df = self._load_labels(source)
            logger.info(f"Loaded {len(labels_df)} labels")

            # Compute address features
            logger.info("Computing address features...")
            features_df = self._compute_address_features(tx_df)
            stats["addresses_count"] = len(features_df)
            logger.info(f"Computed features for {len(features_df)} unique addresses")

            # Join with labels
            features_df = features_df.merge(
                labels_df[["address", "label"]],
                on="address",
                how="left",
            )
            stats["phishing_count"] = int((features_df["label"] == 1).sum())
            stats["legitimate_count"] = int((features_df["label"] == 0).sum())
            stats["unknown_count"] = int(features_df["label"].isna().sum())

            # Build address-to-id mapping
            node_to_id = {addr: idx for idx, addr in enumerate(features_df["address"])}

            # Build edge index
            logger.info("Building edge index...")
            edge_index = self._build_edge_index(tx_df, node_to_id)
            logger.info(f"Built edge index with shape {edge_index.shape}")

            # Normalize features
            logger.info("Normalizing features...")
            features_normalized, scaler = self._normalize_features(features_df)

            # Save to GCS
            logger.info("Saving to GCS...")
            self._save_numpy_arrays(
                features_normalized,
                edge_index,
                features_df["label"].values,
                node_to_id,
                scaler,
            )

            # Also save as Parquet for easier inspection
            features_df.to_parquet("/tmp/features.parquet", index=False)
            features_gcs = self.gcs_processed.upload_file(
                "/tmp/features.parquet",
                "features/node_features.parquet",
            )
            stats["gcs_paths"].append(features_gcs)

            # Save edge index as Parquet
            edge_df = pd.DataFrame({
                "source": edge_index[0],
                "target": edge_index[1],
            })
            edge_df.to_parquet("/tmp/edges.parquet", index=False)
            edges_gcs = self.gcs_processed.upload_file(
                "/tmp/edges.parquet",
                "features/edge_index.parquet",
            )
            stats["gcs_paths"].append(edges_gcs)

            # Save labels as Parquet
            labels_df.to_parquet("/tmp/labels.parquet", index=False)
            labels_gcs = self.gcs_processed.upload_file(
                "/tmp/labels.parquet",
                "features/labels.parquet",
            )
            stats["gcs_paths"].append(labels_gcs)

            job_duration = time.time() - job_start_time
            stats["duration_seconds"] = job_duration

            logger.info(
                f"Feature processing completed in {job_duration:.2f}s: "
                f"{len(features_df)} addresses with {edge_index.shape[1]} edges"
            )
            return stats

        except Exception as e:
            logger.error(f"Feature processing failed: {e}")
            stats["error"] = str(e)
            raise

    def _load_transactions(self, source: str) -> pd.DataFrame:
        """
        Load transactions from GCS raw bucket.

        Args:
            source: Data source ('xblock', 'bigquery', or 'both')

        Returns:
            Pandas DataFrame with transactions
        """
        dfs = []

        if source in ("xblock", "both"):
            try:
                logger.info("Loading XBlock transactions...")
                tx_xblock = self.gcs_raw.read_parquet_from_gcs(
                    "xblock/transactions/transactions.parquet"
                )
                dfs.append(tx_xblock)
                logger.info(f"Loaded {len(tx_xblock)} XBlock transactions")
            except Exception as e:
                logger.warning(f"Failed to load XBlock transactions: {e}")

        if source in ("bigquery", "both"):
            try:
                logger.info("Loading BigQuery transactions...")
                # List all parquet files in BigQuery directory
                blobs = self.gcs_raw.list_blobs("transactions/bigquery/")
                parquet_files = [b for b in blobs if b.endswith(".parquet")]
                if parquet_files:
                    tx_bq = self.gcs_raw.read_parquet_from_gcs(parquet_files[0])
                    dfs.append(tx_bq)
                    logger.info(f"Loaded {len(tx_bq)} BigQuery transactions")
            except Exception as e:
                logger.warning(f"Failed to load BigQuery transactions: {e}")

        if not dfs:
            raise ValueError(
                f"No transactions found for source '{source}'"
            )

        # Concatenate all transaction sources
        tx_df = pd.concat(dfs, ignore_index=True)
        tx_df = tx_df.drop_duplicates(subset=["tx_hash"])
        logger.info(f"Total unique transactions: {len(tx_df)}")

        return tx_df

    def _load_labels(self, source: str) -> pd.DataFrame:
        """
        Load address labels from GCS raw bucket.

        Args:
            source: Data source

        Returns:
            Pandas DataFrame with columns [address, label]
        """
        try:
            labels_df = self.gcs_raw.read_parquet_from_gcs(
                "xblock/labels/labels.parquet"
            )
            logger.info(f"Loaded {len(labels_df)} labels")
            return labels_df
        except Exception as e:
            logger.warning(f"Failed to load labels: {e}")
            return pd.DataFrame({"address": [], "label": []})

    def _compute_address_features(self, tx_df: pd.DataFrame) -> pd.DataFrame:
        """
        Compute 12 node features per address.

        Features:
        1. in_degree: count of incoming transactions
        2. out_degree: count of outgoing transactions
        3. total_eth_received: sum of value where to_address = address (wei -> ETH)
        4. total_eth_sent: sum of value where from_address = address
        5. avg_tx_value_in: average of incoming transaction values
        6. avg_tx_value_out: average of outgoing transaction values
        7. max_tx_value: maximum transaction value across all transactions
        8. unique_in_neighbors: count of unique senders
        9. unique_out_neighbors: count of unique receivers
        10. account_lifetime: max_timestamp - min_timestamp (seconds)
        11. failed_tx_ratio: fraction of transactions with receipt_status=0
        12. avg_gas_used: mean of gas column

        Args:
            tx_df: Transaction DataFrame

        Returns:
            DataFrame with address and 12 feature columns
        """
        # Ensure value is numeric and in wei
        tx_df["value"] = pd.to_numeric(tx_df["value"], errors="coerce").fillna(0)

        # Ensure gas is numeric
        tx_df["gas"] = pd.to_numeric(tx_df["gas"], errors="coerce").fillna(0)

        # Convert timestamps to datetime
        if "block_timestamp" in tx_df.columns:
            tx_df["block_timestamp"] = pd.to_datetime(
                tx_df["block_timestamp"], errors="coerce"
            )

        # Ensure receipt_status is numeric (0=failed, 1=success, null=unknown)
        tx_df["receipt_status"] = pd.to_numeric(
            tx_df["receipt_status"], errors="coerce"
        )

        # Get all unique addresses
        all_addresses = set(tx_df["from_address"]) | set(tx_df["to_address"])
        addresses = sorted(list(all_addresses))

        features_data = []

        for address in addresses:
            # Incoming transactions (to_address == address)
            incoming = tx_df[tx_df["to_address"] == address]
            in_degree = len(incoming)
            total_eth_received = (incoming["value"].sum() * WEI_TO_ETH)
            avg_tx_value_in = (
                incoming["value"].mean() * WEI_TO_ETH
                if in_degree > 0
                else 0
            )
            unique_in_neighbors = incoming["from_address"].nunique()

            # Outgoing transactions (from_address == address)
            outgoing = tx_df[tx_df["from_address"] == address]
            out_degree = len(outgoing)
            total_eth_sent = (outgoing["value"].sum() * WEI_TO_ETH)
            avg_tx_value_out = (
                outgoing["value"].mean() * WEI_TO_ETH
                if out_degree > 0
                else 0
            )
            unique_out_neighbors = outgoing["to_address"].nunique()

            # All transactions involving address
            all_tx = pd.concat([incoming, outgoing]).drop_duplicates(
                subset=["tx_hash"]
            )

            # Value features
            max_tx_value = (
                tx_df["value"].max() * WEI_TO_ETH if len(tx_df) > 0 else 0
            )

            # Temporal features
            account_lifetime = 0
            if "block_timestamp" in tx_df.columns and len(all_tx) > 0:
                timestamps = all_tx["block_timestamp"].dropna()
                if len(timestamps) > 1:
                    account_lifetime = (
                        (timestamps.max() - timestamps.min()).total_seconds()
                    )

            # Behavior features
            failed_tx_ratio = 0
            if len(all_tx) > 0:
                failed_count = (all_tx["receipt_status"] == 0).sum()
                failed_tx_ratio = failed_count / len(all_tx)

            # Gas features
            avg_gas_used = (
                all_tx["gas"].mean() if len(all_tx) > 0 else 0
            )

            features_data.append({
                "address": address,
                "in_degree": in_degree,
                "out_degree": out_degree,
                "total_eth_received": float(total_eth_received),
                "total_eth_sent": float(total_eth_sent),
                "avg_tx_value_in": float(avg_tx_value_in),
                "avg_tx_value_out": float(avg_tx_value_out),
                "max_tx_value": float(max_tx_value),
                "unique_in_neighbors": int(unique_in_neighbors),
                "unique_out_neighbors": int(unique_out_neighbors),
                "account_lifetime": float(account_lifetime),
                "failed_tx_ratio": float(failed_tx_ratio),
                "avg_gas_used": float(avg_gas_used),
            })

        features_df = pd.DataFrame(features_data)
        logger.info(f"Computed features for {len(features_df)} addresses")
        return features_df

    def _build_edge_index(
        self,
        tx_df: pd.DataFrame,
        node_to_id: Dict[str, int],
    ) -> np.ndarray:
        """
        Build edge index (source, target) pairs for graph.

        Args:
            tx_df: Transaction DataFrame
            node_to_id: Mapping from address to node ID

        Returns:
            Array of shape (2, num_edges)
        """
        edges = []

        for _, row in tx_df.iterrows():
            from_addr = row["from_address"]
            to_addr = row["to_address"]

            if from_addr in node_to_id and to_addr in node_to_id:
                edges.append([node_to_id[from_addr], node_to_id[to_addr]])

        edge_index = np.array(edges, dtype=np.int64).T

        if edge_index.size == 0:
            # Empty edge index
            edge_index = np.zeros((2, 0), dtype=np.int64)

        logger.info(f"Built edge index with {edge_index.shape[1]} edges")
        return edge_index

    def _normalize_features(
        self,
        features_df: pd.DataFrame,
    ) -> Tuple[pd.DataFrame, StandardScaler]:
        """
        Normalize feature vectors using StandardScaler.

        Args:
            features_df: Features DataFrame

        Returns:
            Tuple of (normalized_df, scaler)
        """
        # Extract feature columns only (exclude address and label)
        feature_cols = [col for col in NODE_FEATURE_NAMES if col in features_df.columns]

        X = features_df[feature_cols].values
        scaler = StandardScaler()
        X_normalized = scaler.fit_transform(X)

        # Create normalized DataFrame
        df_normalized = features_df.copy()
        for i, col in enumerate(feature_cols):
            df_normalized[col] = X_normalized[:, i]

        logger.info(f"Normalized {len(feature_cols)} features")
        return df_normalized, scaler

    def _save_numpy_arrays(
        self,
        node_features: pd.DataFrame,
        edge_index: np.ndarray,
        labels: np.ndarray,
        node_to_id: Dict[str, int],
        scaler: StandardScaler,
    ) -> None:
        """
        Save features as numpy arrays and scaler as pickle to GCS.

        Args:
            node_features: Normalized features DataFrame
            edge_index: Edge index array
            labels: Label array
            node_to_id: Node ID mapping
            scaler: StandardScaler object
        """
        # Extract feature arrays
        feature_cols = NODE_FEATURE_NAMES
        X = node_features[feature_cols].values

        # Save numpy arrays
        np.save("/tmp/node_features.npy", X)
        np.save("/tmp/edge_index.npy", edge_index)
        np.save("/tmp/labels.npy", labels)

        # Save node mapping as JSON
        with open("/tmp/node_to_id.json", "w") as f:
            json.dump(node_to_id, f)

        # Save scaler
        with open("/tmp/scaler.pkl", "wb") as f:
            pickle.dump(scaler, f)

        # Upload to GCS
        self.gcs_processed.upload_file("/tmp/node_features.npy", "features/node_features.npy")
        self.gcs_processed.upload_file("/tmp/edge_index.npy", "features/edge_index.npy")
        self.gcs_processed.upload_file("/tmp/labels.npy", "features/labels.npy")
        self.gcs_processed.upload_file("/tmp/node_to_id.json", "features/node_to_id.json")
        self.gcs_processed.upload_file("/tmp/scaler.pkl", "features/scaler.pkl")

        logger.info("Saved numpy arrays and scaler to GCS")


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    parser = argparse.ArgumentParser(
        description="Process features from raw transactions"
    )
    parser.add_argument(
        "--raw-bucket",
        required=True,
        help="GCS bucket with raw data",
    )
    parser.add_argument(
        "--processed-bucket",
        required=True,
        help="GCS bucket for processed data",
    )
    parser.add_argument(
        "--source",
        choices=["xblock", "bigquery", "both"],
        default="xblock",
        help="Data source",
    )
    parser.add_argument(
        "--use-spark",
        action="store_true",
        help="Use Spark for processing",
    )
    parser.add_argument(
        "--credentials",
        default=None,
        help="Path to service account JSON",
    )

    args = parser.parse_args()

    job = ProcessFeaturesJob(
        gcs_bucket_raw=args.raw_bucket,
        gcs_bucket_processed=args.processed_bucket,
        credentials_path=args.credentials,
    )

    result = job.run(source=args.source, use_spark=args.use_spark)
    logger.info(f"Job result: {json.dumps(result, indent=2)}")

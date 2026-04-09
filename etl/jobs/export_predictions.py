"""
Batch inference and predictions export job.
"""

import argparse
import json
import logging
import time
from datetime import datetime
from typing import Dict, Optional

import numpy as np
import pandas as pd

from etl.utils.gcs_utils import GCSClient

logger = logging.getLogger(__name__)


class ExportPredictionsJob:
    """
    Runs batch inference on all known addresses from GCS processed data
    and exports prediction results to GCS.
    """

    def __init__(
        self,
        gcs_bucket_processed: str,
        credentials_path: Optional[str] = None,
    ):
        """
        Initialize predictions export job.

        Args:
            gcs_bucket_processed: GCS bucket with processed data
            credentials_path: Path to service account JSON
        """
        self.gcs_bucket_processed = gcs_bucket_processed
        self.credentials_path = credentials_path

        self.gcs_processed = GCSClient(gcs_bucket_processed, credentials_path)

        logger.info(
            f"Initialized ExportPredictionsJob for bucket {gcs_bucket_processed}"
        )

    def run(
        self,
        model_path: Optional[str] = None,
        predictor: str = "mock",
    ) -> Dict[str, any]:
        """
        Main entry point for batch predictions export.

        Args:
            model_path: Path to trained model (if using real predictor)
            predictor: Type of predictor ('mock', 'rf', 'gbm', 'nn')

        Returns:
            Dictionary with job statistics
        """
        job_start_time = time.time()

        stats = {
            "model_path": model_path,
            "predictor_type": predictor,
            "predictions_count": 0,
            "phishing_predictions": 0,
            "legitimate_predictions": 0,
            "gcs_paths": [],
            "duration_seconds": 0,
            "timestamp": datetime.now().isoformat(),
        }

        try:
            logger.info(f"Starting batch predictions with predictor: {predictor}")

            # Load features and node mapping
            logger.info("Loading features...")
            node_features = np.load("/tmp/node_features.npy")
            with open("/tmp/node_to_id.json") as f:
                node_to_id = json.load(f)

            # Reverse mapping
            id_to_node = {v: k for k, v in node_to_id.items()}

            logger.info(f"Loaded {len(node_to_id)} addresses")

            # Run inference
            logger.info(f"Running inference with {predictor}...")
            predictions = self._predict(node_features, model_path, predictor)

            # Create predictions DataFrame
            predictions_df = pd.DataFrame({
                "address": [id_to_node[i] for i in range(len(predictions))],
                "prediction": predictions[:, 0],  # probability of phishing
                "phishing": (predictions[:, 0] > 0.5).astype(int),
                "confidence": np.maximum(
                    predictions[:, 0],
                    1 - predictions[:, 0]
                ),
            })

            stats["predictions_count"] = len(predictions_df)
            stats["phishing_predictions"] = int(
                (predictions_df["phishing"] == 1).sum()
            )
            stats["legitimate_predictions"] = int(
                (predictions_df["phishing"] == 0).sum()
            )

            logger.info(
                f"Generated predictions: "
                f"{stats['phishing_predictions']} phishing, "
                f"{stats['legitimate_predictions']} legitimate"
            )

            # Save predictions to GCS
            predictions_df.to_parquet("/tmp/predictions.parquet", index=False)
            pred_gcs = self.gcs_processed.upload_file(
                "/tmp/predictions.parquet",
                "predictions/predictions.parquet",
            )
            stats["gcs_paths"].append(pred_gcs)

            # Also save as CSV for easier inspection
            predictions_df.to_csv("/tmp/predictions.csv", index=False)
            csv_gcs = self.gcs_processed.upload_file(
                "/tmp/predictions.csv",
                "predictions/predictions.csv",
            )
            stats["gcs_paths"].append(csv_gcs)

            # Save top phishing addresses
            top_phishing = predictions_df.nlargest(1000, "prediction")
            top_phishing.to_csv("/tmp/top_phishing.csv", index=False)
            top_gcs = self.gcs_processed.upload_file(
                "/tmp/top_phishing.csv",
                "predictions/top_phishing_addresses.csv",
            )
            stats["gcs_paths"].append(top_gcs)

            job_duration = time.time() - job_start_time
            stats["duration_seconds"] = job_duration

            logger.info(
                f"Predictions export completed in {job_duration:.2f}s: "
                f"{len(predictions_df)} addresses scored"
            )
            return stats

        except Exception as e:
            logger.error(f"Predictions export failed: {e}")
            stats["error"] = str(e)
            raise

    def _predict(
        self,
        X: np.ndarray,
        model_path: Optional[str] = None,
        predictor: str = "mock",
    ) -> np.ndarray:
        """
        Run inference on feature matrix.

        Args:
            X: Feature matrix (n_samples, n_features)
            model_path: Path to trained model
            predictor: Type of predictor

        Returns:
            Array of predictions (probabilities)
        """
        if predictor == "mock":
            # Mock predictor: random probabilities
            logger.info("Using mock predictor (random probabilities)")
            return np.random.rand(X.shape[0], 2)

        elif predictor == "rf":
            # Random Forest
            try:
                import joblib
                logger.info(f"Loading Random Forest model from {model_path}")
                model = joblib.load(model_path)
                proba = model.predict_proba(X)
                return proba
            except ImportError:
                logger.warning("joblib not available, using mock predictor")
                return np.random.rand(X.shape[0], 2)

        elif predictor == "gbm":
            # Gradient Boosting Machine
            try:
                import joblib
                logger.info(f"Loading GBM model from {model_path}")
                model = joblib.load(model_path)
                proba = model.predict_proba(X)
                return proba
            except ImportError:
                logger.warning("joblib not available, using mock predictor")
                return np.random.rand(X.shape[0], 2)

        elif predictor == "nn":
            # Neural Network
            try:
                import tensorflow as tf
                logger.info(f"Loading NN model from {model_path}")
                model = tf.keras.models.load_model(model_path)
                proba = model.predict(X)
                return proba
            except ImportError:
                logger.warning("TensorFlow not available, using mock predictor")
                return np.random.rand(X.shape[0], 2)

        else:
            logger.warning(f"Unknown predictor {predictor}, using mock")
            return np.random.rand(X.shape[0], 2)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    parser = argparse.ArgumentParser(
        description="Export batch predictions"
    )
    parser.add_argument(
        "--processed-bucket",
        required=True,
        help="GCS bucket with processed data",
    )
    parser.add_argument(
        "--model-path",
        default=None,
        help="Path to trained model",
    )
    parser.add_argument(
        "--predictor",
        choices=["mock", "rf", "gbm", "nn"],
        default="mock",
        help="Type of predictor",
    )
    parser.add_argument(
        "--credentials",
        default=None,
        help="Path to service account JSON",
    )

    args = parser.parse_args()

    job = ExportPredictionsJob(
        gcs_bucket_processed=args.processed_bucket,
        credentials_path=args.credentials,
    )

    result = job.run(model_path=args.model_path, predictor=args.predictor)
    logger.info(f"Job result: {json.dumps(result, indent=2)}")

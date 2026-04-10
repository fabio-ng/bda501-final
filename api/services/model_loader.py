"""
Model loading and management service.
Handles loading model metadata and artifacts.
"""
import os
import json
import logging
from typing import Optional, Dict, Any
from datetime import datetime
from api.config import settings

logger = logging.getLogger(__name__)


class ModelLoader:
    """Load and manage model artifacts."""

    def __init__(self):
        """Initialize model loader."""
        self.artifacts_dir = settings.MODEL_ARTIFACTS_DIR
        self.model_checkpoint = settings.MODEL_CHECKPOINT_NAME
        self._metadata: Optional[Dict[str, Any]] = None
        self._metadata_loaded = False

    def load_metadata(self) -> Dict[str, Any]:
        """
        Load model metadata from JSON file.
        Returns empty dict if file doesn't exist (graceful degradation).
        """
        if self._metadata_loaded:
            return self._metadata or {}

        metadata_path = os.path.join(self.artifacts_dir, "metadata.json")

        if not os.path.exists(metadata_path):
            logger.warning(f"Metadata file not found at {metadata_path}, using defaults")
            self._metadata = self._get_default_metadata()
            self._metadata_loaded = True
            return self._metadata

        try:
            with open(metadata_path, "r") as f:
                self._metadata = json.load(f)
            logger.info(f"Loaded metadata from {metadata_path}")
            self._metadata_loaded = True
            return self._metadata
        except Exception as e:
            logger.error(f"Error loading metadata: {e}, using defaults")
            self._metadata = self._get_default_metadata()
            self._metadata_loaded = True
            return self._metadata

    def model_file_exists(self) -> bool:
        """Check if model checkpoint file exists."""
        checkpoint_path = os.path.join(
            self.artifacts_dir,
            f"{self.model_checkpoint}.pt"
        )
        return os.path.exists(checkpoint_path)

    def load_real_predictor(self):
        """
        Load the real GraphSAGE predictor.
        Requires model artifacts to exist.
        """
        if not self.model_file_exists():
            logger.warning("Model checkpoint not found, falling back to mock")
            return None

        try:
            # Import here to avoid import errors if torch not available
            import torch
            from model_runtime.gnn_predictor import GNNPredictor as RealGraphSAGEPredictor

            checkpoint_path = os.path.join(
                self.artifacts_dir,
                f"{self.model_checkpoint}.pt"
            )
            device = "cuda" if settings.MODEL_DEVICE == "cuda" else "cpu"

            predictor = RealGraphSAGEPredictor(
                checkpoint_path=checkpoint_path,
                device=device,
            )
            logger.info(f"Loaded real predictor from {checkpoint_path}")
            return predictor
        except Exception as e:
            logger.warning(f"Failed to load real predictor: {e}, using mock")
            return None

    def load_mock_predictor(self):
        """Load the mock predictor for testing without model artifacts."""
        try:
            from model_runtime import MockPredictor

            predictor = MockPredictor()
            logger.info("Loaded mock predictor")
            return predictor
        except Exception as e:
            logger.error(f"Failed to load mock predictor: {e}")
            raise

    @staticmethod
    def _get_default_metadata() -> Dict[str, Any]:
        """Get default metadata when file not found."""
        return {
            "name": "GraphSAGE Phishing Detector",
            "version": "1.0.0",
            "architecture": "GraphSAGE with GCN aggregator",
            "training_date": datetime.utcnow().isoformat(),
            "accuracy": None,
            "precision": None,
            "recall": None,
            "f1_score": None,
            "roc_auc": None,
            "threshold": settings.PHISHING_SCORE_THRESHOLD,
            "graph_sample_size": None,
        }

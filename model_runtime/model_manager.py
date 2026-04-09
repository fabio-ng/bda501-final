"""
Model Manager
Handles model loading, version management, and hot-swapping.
"""

import json
import logging
from pathlib import Path
from typing import Optional

from .base_predictor import BasePredictor
from .mock_predictor import MockPredictor

logger = logging.getLogger(__name__)


class ModelManager:
    """
    Manages model loading, versioning, and hot-swapping.

    Responsibilities:
    - Load model metadata from metadata.json
    - Load model files from disk or GCS
    - Switch between mock and real predictors
    - Support hot-swapping without API restart
    """

    def __init__(self, artifacts_dir: str = "./model_artifacts"):
        """
        Initialize model manager.

        Args:
            artifacts_dir (str): Path to model_artifacts directory
        """
        self.artifacts_dir = Path(artifacts_dir)
        self.current_dir = self.artifacts_dir / "current"
        self.metadata_file = "metadata.json"
        self.model_file = "model.pt"

        self.current_predictor: Optional[BasePredictor] = None
        self.current_metadata: Optional[dict] = None

    def load_metadata(self) -> Optional[dict]:
        """
        Load model metadata from current/metadata.json.

        Returns:
            dict: Metadata if found, None otherwise
        """
        metadata_path = self.current_dir / self.metadata_file

        if not metadata_path.exists():
            logger.warning(f"Metadata file not found: {metadata_path}")
            return None

        try:
            with open(metadata_path, "r") as f:
                self.current_metadata = json.load(f)
            logger.info(
                f"Loaded metadata: {self.current_metadata.get('model_version')}"
            )
            return self.current_metadata
        except Exception as e:
            logger.error(f"Failed to load metadata: {e}")
            return None

    def model_file_exists(self) -> bool:
        """
        Check if model.pt exists in current directory.

        Returns:
            bool: True if file exists
        """
        model_path = self.current_dir / self.model_file
        exists = model_path.exists()
        logger.debug(f"Model file exists: {exists} ({model_path})")
        return exists

    def load_mock_predictor(self) -> MockPredictor:
        """
        Load mock predictor.

        Returns:
            MockPredictor: Mock predictor instance
        """
        logger.info("Loading mock predictor")
        predictor = MockPredictor(metadata=self.current_metadata)
        self.current_predictor = predictor
        return predictor

    def load_real_predictor(self, metadata: dict) -> Optional[BasePredictor]:
        """
        Load real predictor (GNN model).

        Args:
            metadata (dict): Model metadata

        Returns:
            BasePredictor: Real predictor instance, or None if loading fails
        """
        model_type = metadata.get("model_type", "MockPredictor")

        if model_type == "GraphSAGE":
            logger.warning(
                "GraphSAGE model loading not yet implemented. "
                "Falling back to mock predictor."
            )
            return self.load_mock_predictor()

        logger.warning(f"Unknown model type: {model_type}. Using mock predictor.")
        return self.load_mock_predictor()

    def get_current_predictor(self) -> BasePredictor:
        """
        Get current predictor (loaded or mock).

        Returns:
            BasePredictor: Current predictor instance
        """
        if self.current_predictor is None:
            logger.warning("No predictor loaded. Returning mock.")
            return self.load_mock_predictor()
        return self.current_predictor

    def reload_model(self) -> bool:
        """
        Reload model from disk (hot-swap support).

        Returns:
            bool: True if reload successful
        """
        logger.info("Reloading model...")

        # Load metadata
        metadata = self.load_metadata()
        if not metadata:
            logger.warning("Failed to load metadata. Using mock.")
            self.current_predictor = self.load_mock_predictor()
            return False

        # Try to load real model
        if self.model_file_exists():
            predictor = self.load_real_predictor(metadata)
            if predictor:
                self.current_predictor = predictor
                logger.info("Model reloaded successfully")
                return True

        # Fall back to mock
        logger.warning("Model file not found. Falling back to mock.")
        self.current_predictor = self.load_mock_predictor()
        return False

    def initialize(self) -> bool:
        """
        Initialize model manager on startup.

        Returns:
            bool: True if initialization successful
        """
        logger.info(f"Initializing model manager (artifacts_dir={self.artifacts_dir})")

        # Verify artifacts directory
        if not self.artifacts_dir.exists():
            logger.error(f"Artifacts directory not found: {self.artifacts_dir}")
            return False

        # Load metadata
        metadata = self.load_metadata()
        if not metadata:
            logger.warning("No metadata found. Will use mock predictor.")

        # Try to load model
        if self.model_file_exists():
            logger.info("Model file found. Attempting to load real model...")
            predictor = self.load_real_predictor(metadata) if metadata else None
            if predictor:
                self.current_predictor = predictor
                logger.info("Real model loaded successfully")
                return True

        # Fall back to mock
        logger.info("No model file or loading failed. Using mock predictor.")
        self.current_predictor = self.load_mock_predictor()
        return True

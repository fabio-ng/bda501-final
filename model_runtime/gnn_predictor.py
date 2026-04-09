"""
GraphSAGE GNN Predictor Implementation
Placeholder for real model integration.

TODO: Implement real GraphSAGE model loading and inference
- Load PyTorch model from model_artifacts/current/model.pt
- Implement graph loading and node embedding
- Implement batch prediction with graph context
- Handle graph updates and model hot-swapping
"""

from .base_predictor import BasePredictor


class GNNPredictor(BasePredictor):
    """
    Placeholder for real GraphSAGE GNN predictor.

    This class will be implemented in Phase 3 when the model is trained.
    Currently raises NotImplementedError.
    """

    def __init__(self, metadata: dict = None, model_path: str = None):
        """
        Initialize GNN predictor (not yet implemented).

        Args:
            metadata (dict, optional): Model metadata
            model_path (str, optional): Path to model.pt file

        Raises:
            NotImplementedError: GNN model not yet implemented
        """
        raise NotImplementedError(
            "GNN Predictor not yet implemented. "
            "This will be available after Phase 3 (Model Training)."
        )

    def predict(self, address: str) -> dict:
        """Not implemented."""
        raise NotImplementedError()

    def predict_batch(self, addresses: list[str]) -> list[dict]:
        """Not implemented."""
        raise NotImplementedError()

    def get_model_info(self) -> dict:
        """Not implemented."""
        raise NotImplementedError()

    def health_check(self) -> bool:
        """Not implemented."""
        raise NotImplementedError()

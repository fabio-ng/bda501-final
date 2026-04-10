"""
Predictor service wrapping model_runtime predictors.
Provides a unified interface for predictions.
"""
import logging
import time
from typing import Optional, Dict, Any, List
from api.config import settings
from api.services.model_loader import ModelLoader

logger = logging.getLogger(__name__)

# Global predictor instance
_predictor_instance = None
_model_loader_instance = None


class Predictor:
    """Unified predictor interface."""

    def __init__(self, predictor, model_loader: ModelLoader):
        """Initialize predictor."""
        self.predictor = predictor
        self.model_loader = model_loader
        self.metadata = model_loader.load_metadata()

    # Map string confidence labels (from predictors) to numeric values so the
    # API schema (which wants a float) stays happy.
    _CONFIDENCE_MAP = {"high": 0.9, "medium": 0.65, "low": 0.4}

    def _coerce_confidence(self, raw) -> float:
        if isinstance(raw, (int, float)):
            return float(raw)
        if isinstance(raw, str):
            return self._CONFIDENCE_MAP.get(raw.lower(), 0.5)
        return 0.0

    def predict(self, address: str) -> Dict[str, Any]:
        """
        Predict phishing probability for a single address.
        Returns prediction dict with score, confidence, and risk factors.
        """
        start = time.time()
        try:
            result = self.predictor.predict(address)
            elapsed = (time.time() - start) * 1000

            # Support both naming conventions from Mock/Real predictors.
            score = float(
                result.get("phishing_probability",
                           result.get("phishing_score",
                                      result.get("score", 0.0)))
            )
            return {
                "address": address,
                "phishing_score": score,
                "is_phishing": bool(
                    result.get(
                        "is_phishing",
                        score >= float(
                            self.metadata.get("threshold", settings.PHISHING_SCORE_THRESHOLD)
                        ),
                    )
                ),
                "confidence": self._coerce_confidence(result.get("confidence", 0.5)),
                "prediction": result.get(
                    "prediction",
                    "phishing" if score >= 0.7 else "legitimate",
                ),
                "risk_factors": result.get("risk_factors", []),
                "model_version": self.metadata.get("version", "unknown"),
                "inference_time_ms": float(
                    result.get("inference_time_ms", elapsed)
                ),
                "threshold_used": float(
                    result.get(
                        "threshold_used",
                        self.metadata.get("threshold", settings.PHISHING_SCORE_THRESHOLD),
                    )
                ),
            }
        except Exception as e:
            logger.error(f"Prediction error for {address}: {e}")
            # Return safe default on error
            return {
                "address": address,
                "phishing_score": 0.0,
                "is_phishing": False,
                "confidence": 0.0,
                "prediction": "legitimate",
                "risk_factors": [],
                "model_version": self.metadata.get("version", "unknown"),
                "inference_time_ms": (time.time() - start) * 1000,
                "threshold_used": float(
                    self.metadata.get("threshold", settings.PHISHING_SCORE_THRESHOLD)
                ),
                "error": str(e),
            }

    def predict_batch(self, addresses: List[str]) -> Dict[str, Any]:
        """
        Predict phishing probability for multiple addresses.
        """
        start = time.time()
        predictions = []

        for address in addresses:
            pred = self.predict(address)
            predictions.append(pred)

        elapsed = (time.time() - start) * 1000

        return {
            "predictions": predictions,
            "processing_time_ms": elapsed,
        }

    def get_metadata(self) -> Dict[str, Any]:
        """Get model metadata."""
        return self.metadata


def get_predictor() -> Predictor:
    """
    Get or initialize the global predictor instance.
    Supports both real and mock modes based on config.
    """
    global _predictor_instance, _model_loader_instance

    if _predictor_instance is not None:
        return _predictor_instance

    # Initialize model loader
    if _model_loader_instance is None:
        _model_loader_instance = ModelLoader()

    # Try to load based on inference mode
    predictor = None

    if settings.INFERENCE_MODE == "real":
        predictor = _model_loader_instance.load_real_predictor()
        if predictor is None:
            logger.warning("Real mode requested but failed, falling back to mock")

    if predictor is None:
        predictor = _model_loader_instance.load_mock_predictor()

    _predictor_instance = Predictor(predictor, _model_loader_instance)
    return _predictor_instance


def reset_predictor() -> None:
    """Reset the global predictor instance (useful for testing)."""
    global _predictor_instance
    _predictor_instance = None

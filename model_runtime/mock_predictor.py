"""
Mock Predictor Implementation
Deterministic mock for testing and development without real model.
"""

import hashlib
from .base_predictor import BasePredictor


class MockPredictor(BasePredictor):
    """
    Deterministic mock predictor for development and testing.

    Behavior:
    - Same address always produces the same score
    - Score is based on SHA256 hash of address (deterministic)
    - Known phishing addresses return high scores
    - Unknown addresses: max score = 0.4 (most are legitimate)
    """

    # Known phishing addresses (from seed data)
    KNOWN_PHISHING = {
        "0x0000000000000000000000000000000000000bad": 0.95,
        "0xdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef": 0.88,
        "0xcafebabecafebabecafebabecafebabecafebabe": 0.92,
        "0x6b175474e89094c44da98b954eedeac495271d0f": 0.03,  # Legit: USDC
        "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48": 0.02,  # Legit: USDT
    }

    def __init__(self, metadata: dict = None):
        """
        Initialize mock predictor.

        Args:
            metadata (dict, optional): Model metadata (ignored for mock)
        """
        self.metadata = metadata or {}
        self.model_version = "mock-0.0.0"
        self.inference_mode = "mock"
        self.default_threshold = 0.7

    def predict(self, address: str) -> dict:
        """
        Predict phishing probability for a single address.

        Args:
            address (str): Ethereum address

        Returns:
            dict: Prediction result
        """
        # Normalize address
        address = address.lower()

        # Validate format
        if not self._validate_address(address):
            return {
                "address": address,
                "phishing_probability": 0.0,
                "prediction": "error",
                "confidence": "low",
                "inference_mode": "mock",
                "error": "Invalid address format",
            }

        # Check if known phishing
        if address in self.KNOWN_PHISHING:
            score = self.KNOWN_PHISHING[address]
        else:
            # Deterministic score from address hash
            hash_val = int(hashlib.sha256(address.encode()).hexdigest(), 16)
            # Scale hash to 0.0-1.0, then cap at 0.4 for unknown addresses
            raw_score = (hash_val % 1000) / 1000.0
            score = raw_score * 0.4

        # Determine prediction
        prediction = "phishing" if score >= self.default_threshold else "legitimate"
        confidence = self._confidence(score)
        is_known = address in self.KNOWN_PHISHING

        return {
            "address": address,
            "phishing_probability": round(score, 4),
            "prediction": prediction,
            "confidence": confidence,
            "threshold_used": self.default_threshold,
            "model_version": self.model_version,
            "inference_mode": self.inference_mode,
            "inference_time_ms": 0.5,
            "is_known_address": is_known,
            "risk_factors": self._mock_risk_factors(score),
        }

    def predict_batch(self, addresses: list[str]) -> list[dict]:
        """
        Predict phishing probabilities for multiple addresses.

        Args:
            addresses (list[str]): List of addresses

        Returns:
            list[dict]: List of prediction results
        """
        return [self.predict(addr) for addr in addresses]

    def get_model_info(self) -> dict:
        """
        Get mock model metadata.

        Returns:
            dict: Model information
        """
        return {
            "model_id": "mock-predictor",
            "model_version": self.model_version,
            "model_type": "MockPredictor",
            "inference_mode": self.inference_mode,
            "threshold": self.default_threshold,
            "feature_count": 0,
            "graph_nodes": 0,
            "graph_edges": 0,
            "training_dataset": "none",
            "loaded_at": "2026-04-09T00:00:00Z",
            "compatibility_status": "ok",
        }

    def health_check(self) -> bool:
        """
        Health check - always True for mock.

        Returns:
            bool: Always True
        """
        return True

    def _mock_risk_factors(self, score: float) -> list[str]:
        """
        Generate mock risk factors based on score.

        Args:
            score (float): Phishing score

        Returns:
            list[str]: List of risk factors
        """
        factors = []

        if score > 0.8:
            factors.append("Pattern matches known phishing behavior (mock)")
        if score > 0.6:
            factors.append("Unusual transaction pattern detected (mock)")
        if score > 0.4:
            factors.append("Moderate anomaly score (mock)")
        if score < 0.3:
            factors.append("Low-risk address (mock)")

        # Always add a note about mock mode
        factors.append(f"Score: {score:.2f} (deterministic hash-based)")

        return factors

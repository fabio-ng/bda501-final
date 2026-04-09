"""
Abstract Base Predictor Class
Defines the interface all predictors must implement.
"""

from abc import ABC, abstractmethod
from typing import Optional


class BasePredictor(ABC):
    """
    Abstract base class for all predictor implementations.
    Defines the interface that model loaders expect.
    """

    @abstractmethod
    def predict(self, address: str) -> dict:
        """
        Predict phishing probability for a single Ethereum address.

        Args:
            address (str): Ethereum address (0x-prefixed, 42 characters)

        Returns:
            dict: Prediction result with keys:
                - address (str): Input address
                - phishing_probability (float): Score between 0 and 1
                - prediction (str): 'phishing' or 'legitimate'
                - confidence (str): 'high', 'medium', or 'low'
                - threshold_used (float): Threshold for classification
                - model_version (str): Version of model used
                - inference_mode (str): 'mock' or 'real'
                - inference_time_ms (float): Latency in milliseconds
                - is_known_address (bool): Whether address is in training set
                - risk_factors (list): List of risk factor explanations
        """
        ...

    @abstractmethod
    def predict_batch(self, addresses: list[str]) -> list[dict]:
        """
        Predict phishing probabilities for multiple addresses.

        Args:
            addresses (list[str]): List of Ethereum addresses

        Returns:
            list[dict]: List of prediction results (same format as predict())
        """
        ...

    @abstractmethod
    def get_model_info(self) -> dict:
        """
        Get metadata about the currently loaded model.

        Returns:
            dict: Model metadata with keys:
                - model_id (str)
                - model_version (str)
                - model_type (str)
                - inference_mode (str)
                - threshold (float)
                - feature_count (int)
                - training_dataset (str)
        """
        ...

    @abstractmethod
    def health_check(self) -> bool:
        """
        Verify that the predictor is functional.

        Returns:
            bool: True if healthy, False otherwise
        """
        ...

    def _confidence(self, score: float) -> str:
        """
        Determine confidence level from score.

        Args:
            score (float): Phishing probability between 0 and 1

        Returns:
            str: 'high', 'medium', or 'low'
        """
        if score >= 0.8 or score <= 0.2:
            return "high"
        if score >= 0.5 or score <= 0.4:
            return "medium"
        return "low"

    def _validate_address(self, address: str) -> bool:
        """
        Validate Ethereum address format.

        Args:
            address (str): Address to validate

        Returns:
            bool: True if valid, False otherwise
        """
        if not isinstance(address, str):
            return False
        if len(address) != 42:
            return False
        if not address.startswith("0x"):
            return False
        try:
            int(address[2:], 16)
            return True
        except ValueError:
            return False

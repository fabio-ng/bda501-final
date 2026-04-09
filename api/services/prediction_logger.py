"""
Prediction logging service.
Handles storing predictions and alerts to the database.
"""
import logging
from typing import Optional
from api.database import get_db
from api.config import settings

logger = logging.getLogger(__name__)


class PredictionLogger:
    """Log predictions and create alerts."""

    def __init__(self):
        """Initialize prediction logger."""
        self.db = get_db()

    def ensure_address(self, address: str) -> None:
        """
        Ensure address exists in the addresses table.
        No-op if already exists.
        """
        try:
            self.db.ensure_address(address)
        except Exception as e:
            logger.warning(f"Failed to ensure address {address}: {e}")
            # Don't raise - this is non-critical

    def log_prediction(
        self,
        address: str,
        phishing_score: float,
        is_phishing: bool,
        model_version: str,
        inference_mode: str,
        threshold: float,
        inference_time_ms: float,
    ) -> Optional[str]:
        """
        Log a prediction to the database.
        Returns: prediction_id or None on error
        """
        try:
            prediction_id = self.db.log_prediction(
                address=address,
                phishing_score=phishing_score,
                is_phishing=is_phishing,
                model_version=model_version,
                inference_mode=inference_mode,
                threshold=threshold,
                inference_time_ms=inference_time_ms,
            )
            logger.info(
                f"Logged prediction {prediction_id} for {address}: "
                f"score={phishing_score}, is_phishing={is_phishing}"
            )
            return prediction_id
        except Exception as e:
            logger.error(f"Failed to log prediction for {address}: {e}")
            return None

    def create_alert(
        self,
        address: str,
        phishing_score: float,
        alert_type: str = "THRESHOLD_EXCEEDED",
        severity: str = "HIGH",
        message: Optional[str] = None,
    ) -> Optional[str]:
        """
        Create a high-risk alert for an address.
        Returns: alert_id or None on error
        """
        if message is None:
            message = f"Address flagged with phishing score {phishing_score:.2%}"

        try:
            alert_id = self.db.create_alert(
                address=address,
                phishing_score=phishing_score,
                alert_type=alert_type,
                severity=severity,
                message=message,
            )
            logger.info(
                f"Created alert {alert_id} for {address}: "
                f"type={alert_type}, severity={severity}"
            )
            return alert_id
        except Exception as e:
            logger.error(f"Failed to create alert for {address}: {e}")
            return None

    def should_create_alert(self, phishing_score: float) -> bool:
        """
        Determine if an alert should be created based on score.
        """
        return (
            settings.AUTO_ALERT_ENABLED
            and phishing_score >= settings.AUTO_ALERT_THRESHOLD
        )

    def get_alert_severity(self, phishing_score: float) -> str:
        """Determine alert severity based on score."""
        if phishing_score >= settings.HIGH_RISK_THRESHOLD:
            return "CRITICAL"
        elif phishing_score >= settings.AUTO_ALERT_THRESHOLD:
            return "HIGH"
        else:
            return "MEDIUM"


def get_prediction_logger() -> PredictionLogger:
    """Get prediction logger instance."""
    return PredictionLogger()

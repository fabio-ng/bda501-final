"""
Configuration management for the FastAPI backend.
Uses environment variables with sensible defaults.
"""
import os
from typing import Optional
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """Application configuration from environment variables."""

    # API Configuration
    API_VERSION: str = "1.0.0"
    API_TITLE: str = "Ethereum Phishing Detection API"
    API_DESCRIPTION: str = "GraphSAGE-based phishing detection for Ethereum addresses"
    DEBUG: bool = os.getenv("DEBUG", "false").lower() == "true"

    # Database Configuration
    DATABASE_URL: str = os.getenv(
        "DATABASE_URL",
        "postgresql://postgres:password@localhost:5432/phishing_db"
    )
    DATABASE_POOL_SIZE: int = int(os.getenv("DATABASE_POOL_SIZE", "5"))
    DATABASE_MAX_OVERFLOW: int = int(os.getenv("DATABASE_MAX_OVERFLOW", "10"))

    # Model Configuration
    INFERENCE_MODE: str = os.getenv("INFERENCE_MODE", "mock")  # "mock" or "real"
    MODEL_ARTIFACTS_DIR: str = os.getenv(
        "MODEL_ARTIFACTS_DIR",
        "/app/model_runtime/artifacts"
    )
    MODEL_DEVICE: str = os.getenv("MODEL_DEVICE", "cpu")  # "cpu" or "cuda"
    MODEL_CHECKPOINT_NAME: str = os.getenv(
        "MODEL_CHECKPOINT_NAME",
        "graphsage_model_v1"
    )

    # GCS Configuration
    GCS_BUCKET_MODELS: str = os.getenv("GCS_BUCKET_MODELS", "phishing-models")
    GCS_BUCKET_DATA: str = os.getenv("GCS_BUCKET_DATA", "phishing-data")
    GCS_PROJECT_ID: Optional[str] = os.getenv("GCS_PROJECT_ID")
    GCS_CREDENTIALS_PATH: Optional[str] = os.getenv("GCS_CREDENTIALS_PATH")

    # Kafka Configuration
    KAFKA_BOOTSTRAP_SERVERS: str = os.getenv(
        "KAFKA_BOOTSTRAP_SERVERS",
        "localhost:9092"
    )
    KAFKA_TOPIC_PREDICTIONS: str = os.getenv(
        "KAFKA_TOPIC_PREDICTIONS",
        "phishing-predictions"
    )
    KAFKA_ENABLED: bool = os.getenv("KAFKA_ENABLED", "false").lower() == "true"

    # Logging Configuration
    LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO")
    LOG_FORMAT: str = os.getenv("LOG_FORMAT", "json")  # "json" or "text"

    # Prediction Thresholds
    PHISHING_SCORE_THRESHOLD: float = float(
        os.getenv("PHISHING_SCORE_THRESHOLD", "0.5")
    )
    HIGH_RISK_THRESHOLD: float = float(
        os.getenv("HIGH_RISK_THRESHOLD", "0.8")
    )

    # Alert Configuration
    AUTO_ALERT_ENABLED: bool = os.getenv("AUTO_ALERT_ENABLED", "true").lower() == "true"
    AUTO_ALERT_THRESHOLD: float = float(
        os.getenv("AUTO_ALERT_THRESHOLD", "0.75")
    )

    # Batch Processing
    MAX_BATCH_SIZE: int = int(os.getenv("MAX_BATCH_SIZE", "100"))

    # CORS Configuration
    CORS_ORIGINS: list = [
        "http://localhost:3000",
        "http://localhost:8080",
        "http://localhost",
    ]

    # API Rate Limiting (for future use)
    RATE_LIMIT_ENABLED: bool = os.getenv("RATE_LIMIT_ENABLED", "false").lower() == "true"
    RATE_LIMIT_REQUESTS: int = int(os.getenv("RATE_LIMIT_REQUESTS", "100"))
    RATE_LIMIT_WINDOW_SECONDS: int = int(
        os.getenv("RATE_LIMIT_WINDOW_SECONDS", "60")
    )

    class Config:
        env_file = ".env"
        case_sensitive = True
        extra = "ignore"


def get_settings() -> Settings:
    """Get the application settings."""
    return Settings()


# Global settings instance
settings = get_settings()

"""
Pydantic request/response schemas for the FastAPI backend.
"""
from typing import Optional, List, Dict, Any
from datetime import datetime
from pydantic import BaseModel, Field, field_validator
import re


class PredictAddressRequest(BaseModel):
    """Request schema for single address prediction."""
    address: str = Field(
        ...,
        description="Ethereum address to predict (0x format)"
    )
    include_risk_factors: bool = Field(
        default=True,
        description="Include detailed risk factors in response"
    )
    threshold_override: Optional[float] = Field(
        default=None,
        description="Override default phishing threshold for this prediction"
    )

    @field_validator("address")
    @classmethod
    def validate_address(cls, v: str) -> str:
        """Validate Ethereum address format."""
        if not re.match(r"^0x[a-fA-F0-9]{40}$", v):
            raise ValueError("Invalid Ethereum address format. Must be 0x followed by 40 hex characters.")
        return v.lower()


class RiskFactor(BaseModel):
    """Individual risk factor information."""
    name: str
    value: float
    description: Optional[str] = None


class PredictAddressResponse(BaseModel):
    """Response schema for single address prediction."""
    address: str
    phishing_score: float = Field(
        description="Phishing probability score (0-1)"
    )
    is_phishing: bool = Field(
        description="Whether address exceeds threshold"
    )
    confidence: float = Field(
        description="Confidence in prediction (0-1)"
    )
    risk_factors: Optional[List[RiskFactor]] = None
    model_version: str
    inference_time_ms: float
    timestamp: datetime
    correlation_id: str


class PredictBatchRequest(BaseModel):
    """Request schema for batch address predictions."""
    addresses: List[str] = Field(
        ...,
        min_items=1,
        max_items=100,
        description="List of Ethereum addresses to predict"
    )
    include_risk_factors: bool = Field(
        default=False,
        description="Include risk factors for each prediction"
    )

    @field_validator("addresses")
    @classmethod
    def validate_addresses(cls, v: List[str]) -> List[str]:
        """Validate all addresses in the list."""
        validated = []
        for addr in v:
            if not re.match(r"^0x[a-fA-F0-9]{40}$", addr):
                raise ValueError(f"Invalid address format: {addr}")
            validated.append(addr.lower())
        return validated


class BatchPrediction(BaseModel):
    """Single prediction in batch response."""
    address: str
    phishing_score: float
    is_phishing: bool
    confidence: float
    risk_factors: Optional[List[RiskFactor]] = None


class PredictBatchResponse(BaseModel):
    """Response schema for batch predictions."""
    predictions: List[BatchPrediction]
    batch_size: int
    processing_time_ms: float
    timestamp: datetime
    correlation_id: str


class AlertResponse(BaseModel):
    """Alert information response."""
    alert_id: str
    address: str
    phishing_score: float
    alert_type: str  # "HIGH_RISK", "MEDIUM_RISK", "THRESHOLD_EXCEEDED"
    severity: str  # "CRITICAL", "HIGH", "MEDIUM"
    message: str
    created_at: datetime
    reviewed: bool = False
    reviewed_at: Optional[datetime] = None
    notes: Optional[str] = None


class PredictionHistoryResponse(BaseModel):
    """Prediction history entry."""
    prediction_id: str
    address: str
    phishing_score: float
    is_phishing: bool
    model_version: str
    inference_mode: str
    predicted_at: datetime
    threshold_used: float


class PredictionHistoryListResponse(BaseModel):
    """Paginated prediction history response."""
    predictions: List[PredictionHistoryResponse]
    total_count: int
    limit: int
    offset: int
    has_more: bool


class AlertListResponse(BaseModel):
    """Paginated alerts list response."""
    alerts: List[AlertResponse]
    total_count: int
    limit: int
    offset: int
    has_more: bool


class DashboardSummaryResponse(BaseModel):
    """Dashboard summary statistics."""
    total_predictions: int
    total_phishing_detected: int
    phishing_detection_rate: float  # percentage
    total_alerts: int
    unreviewed_alerts: int
    high_risk_alerts: int
    avg_phishing_score: float
    predictions_last_24h: int
    alerts_last_24h: int
    model_version: str
    last_model_update: datetime
    system_status: str  # "healthy", "degraded", "down"
    uptime_hours: float


class HealthCheckDatabase(BaseModel):
    """Database health check result."""
    status: str  # "connected", "disconnected", "error"
    message: Optional[str] = None
    response_time_ms: Optional[float] = None


class HealthCheckGCS(BaseModel):
    """GCS health check result."""
    status: str  # "connected", "disconnected", "error", "skipped"
    message: Optional[str] = None
    response_time_ms: Optional[float] = None


class HealthCheckKafka(BaseModel):
    """Kafka health check result."""
    status: str  # "connected", "disconnected", "error", "skipped"
    message: Optional[str] = None
    response_time_ms: Optional[float] = None


class HealthResponse(BaseModel):
    """API health check response."""
    status: str  # "healthy", "degraded", "unhealthy"
    timestamp: datetime
    api_version: str
    database: HealthCheckDatabase
    gcs: HealthCheckGCS
    kafka: HealthCheckKafka
    inference_mode: str


class ModelMetadata(BaseModel):
    """Model metadata information."""
    name: str
    version: str
    architecture: str
    training_date: datetime
    accuracy: Optional[float] = None
    precision: Optional[float] = None
    recall: Optional[float] = None
    f1_score: Optional[float] = None
    roc_auc: Optional[float] = None
    threshold: float
    graph_sample_size: Optional[int] = None
    device: str


class ModelInfoResponse(BaseModel):
    """Current model information response."""
    model: ModelMetadata
    inference_mode: str
    artifacts_directory: str
    last_loaded: datetime
    correlation_id: str


class ModelMetricsResponse(BaseModel):
    """Model evaluation metrics response."""
    metrics: Dict[str, Any]
    evaluation_date: datetime
    dataset_size: Optional[int] = None
    correlation_id: str


class IngestMockRequest(BaseModel):
    """Request to ingest mock transaction data."""
    num_transactions: int = Field(
        default=100,
        ge=1,
        le=10000,
        description="Number of mock transactions to generate"
    )
    num_phishing_addresses: int = Field(
        default=10,
        ge=1,
        description="Number of phishing addresses to include"
    )
    background: bool = Field(
        default=True,
        description="Run as background task"
    )


class IngestMockResponse(BaseModel):
    """Response from mock data ingestion."""
    task_id: str
    status: str  # "queued", "processing", "completed", "failed"
    message: str
    num_transactions: int
    num_phishing_addresses: int
    created_at: datetime


class ErrorResponse(BaseModel):
    """Standard error response."""
    error: str
    detail: str
    status_code: int
    timestamp: datetime
    correlation_id: str

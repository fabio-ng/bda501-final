"""
Model information and metrics endpoints.
"""
import logging
from datetime import datetime
from typing import Dict, Any
from fastapi import APIRouter, Request
from api.models import ModelInfoResponse, ModelMetricsResponse, ModelMetadata
from api.services.predictor import get_predictor
from api.config import settings

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/model", tags=["model"])


@router.get("/info", response_model=ModelInfoResponse)
async def get_model_info(request: Request) -> ModelInfoResponse:
    """
    Get information about the currently loaded model.
    """
    correlation_id = getattr(request.state, "correlation_id", "unknown")

    predictor = get_predictor()
    metadata = predictor.get_metadata()

    # Build model metadata response
    model = ModelMetadata(
        name=metadata.get("name", "Unknown Model"),
        version=metadata.get("version", "unknown"),
        architecture=metadata.get("architecture", "Unknown"),
        training_date=datetime.fromisoformat(
            metadata.get("training_date", datetime.utcnow().isoformat())
        ),
        accuracy=metadata.get("accuracy"),
        precision=metadata.get("precision"),
        recall=metadata.get("recall"),
        f1_score=metadata.get("f1_score"),
        roc_auc=metadata.get("roc_auc"),
        threshold=metadata.get("threshold", settings.PHISHING_SCORE_THRESHOLD),
        graph_sample_size=metadata.get("graph_sample_size"),
        device=settings.MODEL_DEVICE,
    )

    return ModelInfoResponse(
        model=model,
        inference_mode=settings.INFERENCE_MODE,
        artifacts_directory=settings.MODEL_ARTIFACTS_DIR,
        last_loaded=datetime.utcnow(),
        correlation_id=correlation_id,
    )


@router.get("/metrics", response_model=ModelMetricsResponse)
async def get_model_metrics(request: Request) -> ModelMetricsResponse:
    """
    Get model evaluation metrics.
    """
    correlation_id = getattr(request.state, "correlation_id", "unknown")

    predictor = get_predictor()
    metadata = predictor.get_metadata()

    # Build metrics dictionary
    metrics: Dict[str, Any] = {
        "accuracy": metadata.get("accuracy"),
        "precision": metadata.get("precision"),
        "recall": metadata.get("recall"),
        "f1_score": metadata.get("f1_score"),
        "roc_auc": metadata.get("roc_auc"),
        "threshold": metadata.get("threshold", settings.PHISHING_SCORE_THRESHOLD),
    }

    return ModelMetricsResponse(
        metrics=metrics,
        evaluation_date=datetime.fromisoformat(
            metadata.get("training_date", datetime.utcnow().isoformat())
        ),
        dataset_size=metadata.get("graph_sample_size"),
        correlation_id=correlation_id,
    )

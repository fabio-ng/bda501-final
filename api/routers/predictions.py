"""
Prediction history endpoints.
"""
import logging
from datetime import datetime
from typing import Optional
from fastapi import APIRouter, Request, Query
from api.models import PredictionHistoryListResponse, PredictionHistoryResponse
from api.database import get_db

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/predictions", tags=["predictions"])


@router.get("/history", response_model=PredictionHistoryListResponse)
async def get_prediction_history(
    request: Request,
    address: Optional[str] = Query(None, description="Filter by address"),
    min_score: Optional[float] = Query(None, description="Minimum phishing score"),
    limit: int = Query(20, ge=1, le=100, description="Results per page"),
    offset: int = Query(0, ge=0, description="Pagination offset"),
    sort: str = Query("created_at", description="Sort field"),
) -> PredictionHistoryListResponse:
    """
    Get paginated prediction history with optional filters.

    Query parameters:
    - address: Filter by specific Ethereum address
    - min_score: Filter by minimum phishing score (0-1)
    - limit: Number of results per page (1-100)
    - offset: Pagination offset
    - sort: Sort field (created_at, phishing_score, address)
    """
    db = get_db()

    # Normalize address if provided
    if address:
        address = address.lower()

    # Validate sort field
    valid_sorts = ["created_at", "phishing_score", "address"]
    if sort not in valid_sorts:
        sort = "created_at"

    # Fetch paginated results
    predictions, total_count = db.get_predictions_history(
        address=address,
        limit=limit,
        offset=offset,
        min_score=min_score,
        sort=sort,
    )

    # Convert to response models
    prediction_responses = [
        PredictionHistoryResponse(
            prediction_id=str(p.get("id")),
            address=p.get("address", ""),
            phishing_score=float(p.get("phishing_score", 0.0)),
            is_phishing=bool(p.get("is_phishing", False)),
            model_version=p.get("model_version", "unknown"),
            inference_mode=p.get("inference_mode", "unknown"),
            predicted_at=p.get("created_at", datetime.utcnow()),
            threshold_used=float(p.get("threshold", 0.5)),
        )
        for p in predictions
    ]

    has_more = (offset + limit) < total_count

    return PredictionHistoryListResponse(
        predictions=prediction_responses,
        total_count=total_count,
        limit=limit,
        offset=offset,
        has_more=has_more,
    )

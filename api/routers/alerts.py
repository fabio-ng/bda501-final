"""
Alerts endpoints.
"""
import logging
from datetime import datetime
from typing import Optional
from fastapi import APIRouter, Request, Query
from api.models import AlertListResponse, AlertResponse
from api.database import get_db

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/alerts", tags=["alerts"])


@router.get("", response_model=AlertListResponse)
async def get_alerts(
    request: Request,
    since: Optional[str] = Query(None, description="Alerts since timestamp"),
    min_score: Optional[float] = Query(None, description="Minimum phishing score"),
    reviewed: Optional[bool] = Query(None, description="Filter by reviewed status"),
    limit: int = Query(20, ge=1, le=100, description="Results per page"),
    offset: int = Query(0, ge=0, description="Pagination offset"),
) -> AlertListResponse:
    """
    Get paginated alerts with optional filters.

    Query parameters:
    - since: ISO timestamp to get alerts after (e.g., 2024-01-01T00:00:00Z)
    - min_score: Minimum phishing score threshold (0-1)
    - reviewed: Filter by reviewed status (true/false)
    - limit: Number of results per page (1-100)
    - offset: Pagination offset
    """
    db = get_db()

    # Fetch paginated results
    alerts, total_count = db.get_alerts(
        since=since,
        min_score=min_score,
        reviewed=reviewed,
        limit=limit,
        offset=offset,
    )

    # Convert to response models
    alert_responses = [
        AlertResponse(
            alert_id=str(a.get("id")),
            address=a.get("address", ""),
            phishing_score=float(a.get("phishing_score", 0.0)),
            alert_type=a.get("alert_type", "THRESHOLD_EXCEEDED"),
            severity=a.get("severity", "HIGH"),
            message=a.get("message", ""),
            created_at=a.get("created_at", datetime.utcnow()),
            reviewed=bool(a.get("reviewed", False)),
            reviewed_at=a.get("reviewed_at"),
            notes=a.get("notes"),
        )
        for a in alerts
    ]

    has_more = (offset + limit) < total_count

    return AlertListResponse(
        alerts=alert_responses,
        total_count=total_count,
        limit=limit,
        offset=offset,
        has_more=has_more,
    )

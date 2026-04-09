"""
Dashboard endpoints.
"""
import logging
from datetime import datetime
from fastapi import APIRouter, Request
from api.models import DashboardSummaryResponse
from api.database import get_db
from api.services.predictor import get_predictor
from api.config import settings

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/dashboard", tags=["dashboard"])


@router.get("/summary", response_model=DashboardSummaryResponse)
async def get_dashboard_summary(request: Request) -> DashboardSummaryResponse:
    """
    Get aggregated dashboard summary statistics.
    Includes prediction counts, alert status, and system health.
    """
    db = get_db()
    predictor = get_predictor()

    # Get database statistics
    summary = db.get_dashboard_summary()

    # Get model info
    metadata = predictor.get_metadata()

    # Build response
    return DashboardSummaryResponse(
        total_predictions=summary.get("total_predictions", 0),
        total_phishing_detected=summary.get("total_phishing_detected", 0),
        phishing_detection_rate=summary.get("phishing_detection_rate", 0.0),
        total_alerts=summary.get("total_alerts", 0),
        unreviewed_alerts=summary.get("unreviewed_alerts", 0),
        high_risk_alerts=summary.get("high_risk_alerts", 0),
        avg_phishing_score=summary.get("avg_phishing_score", 0.0),
        predictions_last_24h=summary.get("predictions_last_24h", 0),
        alerts_last_24h=summary.get("alerts_last_24h", 0),
        model_version=metadata.get("version", "unknown"),
        last_model_update=datetime.fromisoformat(
            metadata.get("training_date", datetime.utcnow().isoformat())
        ),
        system_status="healthy",  # Would be computed from health checks
        uptime_hours=24.0,  # Would be computed from startup time
    )

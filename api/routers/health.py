"""
Health check endpoints.
"""
import logging
import asyncio
from datetime import datetime
from fastapi import APIRouter, Request
from api.models import (
    HealthResponse,
    HealthCheckDatabase,
    HealthCheckGCS,
    HealthCheckKafka,
)
from api.database import get_db
from api.config import settings

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/health", tags=["health"])


@router.get("", response_model=HealthResponse)
async def health_check(request: Request) -> HealthResponse:
    """
    Check API health and connectivity.
    Tests database, GCS, and Kafka connectivity.
    """
    correlation_id = getattr(request.state, "correlation_id", "unknown")
    db = get_db()

    # Check database
    db_ok, db_msg, db_time = db.health_check()
    db_status = HealthCheckDatabase(
        status="connected" if db_ok else "disconnected",
        message=db_msg,
        response_time_ms=db_time,
    )

    # Check GCS (graceful degradation if not configured)
    gcs_status = check_gcs_health()

    # Check Kafka (graceful degradation if not enabled)
    kafka_status = check_kafka_health()

    # Determine overall status
    overall_status = "healthy"
    if not db_ok:
        overall_status = "unhealthy"
    elif gcs_status.status == "error" or kafka_status.status == "error":
        overall_status = "degraded"

    return HealthResponse(
        status=overall_status,
        timestamp=datetime.utcnow(),
        api_version=settings.API_VERSION,
        database=db_status,
        gcs=gcs_status,
        kafka=kafka_status,
        inference_mode=settings.INFERENCE_MODE,
    )


def check_gcs_health() -> HealthCheckGCS:
    """Check GCS connectivity."""
    if not settings.GCS_PROJECT_ID:
        return HealthCheckGCS(
            status="skipped",
            message="GCS not configured",
        )

    try:
        from google.cloud import storage
        import time

        start = time.time()
        client = storage.Client(project=settings.GCS_PROJECT_ID)
        # Simple operation to check connectivity
        list(client.list_buckets(max_results=1))
        elapsed = (time.time() - start) * 1000

        return HealthCheckGCS(
            status="connected",
            response_time_ms=elapsed,
        )
    except Exception as e:
        logger.warning(f"GCS health check failed: {e}")
        return HealthCheckGCS(
            status="error",
            message=str(e),
        )


def check_kafka_health() -> HealthCheckKafka:
    """Check Kafka connectivity."""
    if not settings.KAFKA_ENABLED:
        return HealthCheckKafka(
            status="skipped",
            message="Kafka not enabled",
        )

    try:
        from kafka import KafkaAdminClient
        import time

        start = time.time()
        admin = KafkaAdminClient(
            bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
            request_timeout_ms=5000,
        )
        # Simple operation to check connectivity
        admin.list_topics()
        elapsed = (time.time() - start) * 1000
        admin.close()

        return HealthCheckKafka(
            status="connected",
            response_time_ms=elapsed,
        )
    except Exception as e:
        logger.warning(f"Kafka health check failed: {e}")
        return HealthCheckKafka(
            status="error",
            message=str(e),
        )

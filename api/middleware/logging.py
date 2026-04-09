"""
Structured logging middleware using structlog.
"""
import time
import logging
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response
import structlog

logger = structlog.get_logger(__name__)


class StructuredLoggingMiddleware(BaseHTTPMiddleware):
    """Middleware for structured request/response logging."""

    async def dispatch(self, request: Request, call_next) -> Response:
        """Log structured request and response information."""
        start_time = time.time()
        correlation_id = getattr(request.state, "correlation_id", "unknown")

        # Log request
        logger.info(
            "request_started",
            method=request.method,
            path=request.url.path,
            query_params=dict(request.query_params),
            correlation_id=correlation_id,
        )

        try:
            response = await call_next(request)
            process_time = time.time() - start_time

            # Log response
            logger.info(
                "request_completed",
                method=request.method,
                path=request.url.path,
                status_code=response.status_code,
                process_time_ms=process_time * 1000,
                correlation_id=correlation_id,
            )

            return response
        except Exception as e:
            process_time = time.time() - start_time
            logger.error(
                "request_failed",
                method=request.method,
                path=request.url.path,
                error=str(e),
                process_time_ms=process_time * 1000,
                correlation_id=correlation_id,
            )
            raise

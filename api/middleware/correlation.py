"""
Correlation ID middleware for request tracking.
"""
import uuid
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response


class CorrelationIDMiddleware(BaseHTTPMiddleware):
    """Middleware that assigns a correlation ID to each request."""

    async def dispatch(self, request: Request, call_next) -> Response:
        """Process request and assign correlation ID."""
        # Check for existing correlation ID
        correlation_id = request.headers.get("X-Correlation-ID")
        if not correlation_id:
            correlation_id = str(uuid.uuid4())

        # Store in request state for access in handlers
        request.state.correlation_id = correlation_id

        # Process request
        response = await call_next(request)

        # Add correlation ID to response header
        response.headers["X-Correlation-ID"] = correlation_id

        return response

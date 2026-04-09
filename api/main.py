"""
FastAPI main application with lifespan and router registration.
"""
import logging
import sys
from contextlib import asynccontextmanager
from datetime import datetime
from typing import AsyncGenerator

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.exceptions import RequestValidationError

import structlog

from api.config import settings
from api.database import DatabasePool, get_db
from api.services.predictor import get_predictor
from api.middleware.correlation import CorrelationIDMiddleware
from api.middleware.logging import StructuredLoggingMiddleware
from api.routers import (
    health,
    predict,
    model,
    predictions,
    alerts,
    ingest,
    dashboard,
)
from api.models import ErrorResponse

# Configure structured logging
structlog.configure(
    processors=[
        structlog.stdlib.filter_by_level,
        structlog.stdlib.add_logger_name,
        structlog.stdlib.add_log_level,
        structlog.stdlib.PositionalArgumentsFormatter(),
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
        structlog.processors.UnicodeDecoder(),
        structlog.processors.JSONRenderer(),
    ],
    context_class=dict,
    logger_factory=structlog.stdlib.LoggerFactory(),
    cache_logger_on_first_use=True,
)

# Configure standard logging
logging.basicConfig(
    format="%(message)s",
    stream=sys.stdout,
    level=getattr(logging, settings.LOG_LEVEL),
)

logger = structlog.get_logger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator:
    """
    Application lifespan context manager.
    Handles startup and shutdown tasks.
    """
    # Startup
    logger.info(
        "application_startup",
        api_version=settings.API_VERSION,
        inference_mode=settings.INFERENCE_MODE,
        debug=settings.DEBUG,
    )

    try:
        # Initialize database pool
        db_pool = DatabasePool.get_instance()
        logger.info("database_pool_initialized")

        # Load model
        predictor = get_predictor()
        logger.info(
            "model_loaded",
            model_version=predictor.get_metadata().get("version"),
        )

        # Test database connection
        db = get_db()
        is_healthy, msg, _ = db.health_check()
        if is_healthy:
            logger.info("database_health_check_passed")
        else:
            logger.warning("database_health_check_failed", message=msg)

    except Exception as e:
        logger.error("startup_error", error=str(e), exc_info=True)
        raise

    yield

    # Shutdown
    logger.info("application_shutdown")
    try:
        DatabasePool.close_all()
        logger.info("database_pool_closed")
    except Exception as e:
        logger.error("shutdown_error", error=str(e))


def create_app() -> FastAPI:
    """Create and configure the FastAPI application."""
    app = FastAPI(
        title=settings.API_TITLE,
        description=settings.API_DESCRIPTION,
        version=settings.API_VERSION,
        lifespan=lifespan,
    )

    # Add middleware
    app.add_middleware(StructuredLoggingMiddleware)
    app.add_middleware(CorrelationIDMiddleware)

    app.add_middleware(
        CORSMiddleware,
        allow_origins=settings.CORS_ORIGINS,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # Register routers
    app.include_router(health.router)
    app.include_router(predict.router)
    app.include_router(model.router)
    app.include_router(predictions.router)
    app.include_router(alerts.router)
    app.include_router(ingest.router)
    app.include_router(dashboard.router)

    # Exception handlers
    @app.exception_handler(RequestValidationError)
    async def validation_exception_handler(request: Request, exc: RequestValidationError):
        """Handle Pydantic validation errors."""
        correlation_id = getattr(request.state, "correlation_id", "unknown")
        return JSONResponse(
            status_code=422,
            content={
                "error": "Validation Error",
                "detail": exc.errors(),
                "status_code": 422,
                "timestamp": datetime.utcnow().isoformat(),
                "correlation_id": correlation_id,
            },
        )

    @app.exception_handler(Exception)
    async def general_exception_handler(request: Request, exc: Exception):
        """Handle general exceptions."""
        correlation_id = getattr(request.state, "correlation_id", "unknown")
        logger.error(
            "unhandled_exception",
            error=str(exc),
            path=request.url.path,
            method=request.method,
            correlation_id=correlation_id,
            exc_info=True,
        )
        return JSONResponse(
            status_code=500,
            content={
                "error": "Internal Server Error",
                "detail": "An unexpected error occurred",
                "status_code": 500,
                "timestamp": datetime.utcnow().isoformat(),
                "correlation_id": correlation_id,
            },
        )

    # Root endpoint
    @app.get("/")
    async def root(request: Request):
        """Root endpoint."""
        correlation_id = getattr(request.state, "correlation_id", "unknown")
        return {
            "name": settings.API_TITLE,
            "version": settings.API_VERSION,
            "status": "active",
            "correlation_id": correlation_id,
        }

    return app


# Create the application
app = create_app()

if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "api.main:app",
        host="0.0.0.0",
        port=8000,
        reload=settings.DEBUG,
    )

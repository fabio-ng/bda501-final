"""FastAPI application — ETH Transaction Analytics API."""

import logging
import os
from contextlib import asynccontextmanager

import asyncpg
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded

from db.connection import init_pool, close_pool
from routes.top100 import router as top100_router
from routes.wallet_graph import router as wallet_graph_router
from routes.health import router as health_router

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ── Rate limiter ──────────────────────────────
limiter = Limiter(key_func=get_remote_address, default_limits=["60/minute"])


# ── App lifecycle ─────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    await init_pool()
    logger.info("API started")
    yield
    # Shutdown
    await close_pool()
    logger.info("API stopped")


# ── App ───────────────────────────────────────
app = FastAPI(
    title="ETH Transaction Analytics API",
    description="Daily top-100 wallet rankings and 180-day transaction graph",
    version="1.0.0",
    lifespan=lifespan,
)

# Rate limiting
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

# ── CORS — read allowed origins from env, fall back to dev defaults ──
_default_origins = "http://localhost:3000,http://localhost:5173"
_cors_origins = [
    o.strip()
    for o in os.environ.get("CORS_ALLOWED_ORIGINS", _default_origins).split(",")
    if o.strip()
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=_cors_origins,
    allow_credentials=True,
    allow_methods=["GET"],
    allow_headers=["*"],
)


# ── Centralized DB error handler ─────────────
@app.exception_handler(asyncpg.PostgresError)
async def postgres_error_handler(request: Request, exc: asyncpg.PostgresError):
    logger.error("Database error on %s %s: %s", request.method, request.url.path, exc)
    return JSONResponse(
        status_code=503,
        content={"detail": "Database temporarily unavailable. Please retry shortly."},
    )


# Routes
app.include_router(top100_router)
app.include_router(wallet_graph_router)
app.include_router(health_router)

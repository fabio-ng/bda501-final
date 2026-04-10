"""FastAPI application — ETH Transaction Analytics API."""

import logging
from contextlib import asynccontextmanager

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

# CORS — allow frontend origins
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:3000",
        "http://localhost:5173",  # Vite dev server
    ],
    allow_credentials=True,
    allow_methods=["GET"],
    allow_headers=["*"],
)

# Routes
app.include_router(top100_router)
app.include_router(wallet_graph_router)
app.include_router(health_router)

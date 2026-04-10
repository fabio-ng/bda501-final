"""Async PostgreSQL connection pool using asyncpg."""

import os
import logging
from contextlib import asynccontextmanager

import asyncpg

logger = logging.getLogger(__name__)

_pool: asyncpg.Pool | None = None


async def init_pool() -> asyncpg.Pool:
    """Create the connection pool. Called on FastAPI startup."""
    global _pool
    _pool = await asyncpg.create_pool(
        host=os.environ.get("POSTGRES_HOST", "postgres"),
        port=int(os.environ.get("POSTGRES_PORT", "5432")),
        database=os.environ.get("POSTGRES_DB", "ethdb"),
        user=os.environ.get("POSTGRES_USER", "ethuser"),
        password=os.environ.get("POSTGRES_PASSWORD", "ethpass"),
        min_size=2,
        max_size=10,
    )
    logger.info("PostgreSQL connection pool created")
    return _pool


async def close_pool() -> None:
    """Close the connection pool. Called on FastAPI shutdown."""
    global _pool
    if _pool:
        await _pool.close()
        _pool = None
        logger.info("PostgreSQL connection pool closed")


def get_pool() -> asyncpg.Pool:
    """Return the current pool. Raises if not initialized."""
    if _pool is None:
        raise RuntimeError("Database pool not initialized — call init_pool() first")
    return _pool

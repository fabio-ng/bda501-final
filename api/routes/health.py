"""GET /api/health — service health + latest data timestamps."""

from fastapi import APIRouter, Request
from slowapi import Limiter
from slowapi.util import get_remote_address

from db.connection import get_pool
from db.models import get_health
from schemas.responses import HealthResponse

router = APIRouter()
limiter = Limiter(key_func=get_remote_address)


@router.get("/api/health", response_model=HealthResponse)
@limiter.limit("60/minute")
async def health(request: Request):
    pool = get_pool()
    info = await get_health(pool)

    return HealthResponse(
        status="ok",
        last_snapshot_date=info["last_snapshot_date"],
        edge_period_end=info["edge_period_end"],
    )

"""GET /api/health — service health + latest data timestamps."""

from fastapi import APIRouter

from db.connection import get_pool
from db.models import get_health
from schemas.responses import HealthResponse

router = APIRouter()


@router.get("/api/health", response_model=HealthResponse)
async def health():
    pool = get_pool()
    info = await get_health(pool)

    return HealthResponse(
        status="ok",
        last_snapshot_date=info["last_snapshot_date"],
        edge_period_end=info["edge_period_end"],
    )

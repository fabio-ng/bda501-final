"""GET /api/top100 — paginated daily wallet snapshot."""

from datetime import date as Date

from fastapi import APIRouter, Query

from db.connection import get_pool
from db.models import get_top100
from schemas.responses import Top100Response, WalletSnapshotItem

router = APIRouter()


@router.get("/api/top100", response_model=Top100Response)
async def top100(
    snap_date: Date = Query(
        ...,
        alias="date",
        description="Snapshot date (YYYY-MM-DD)",
    ),
    page: int = Query(1, ge=1, description="Page number"),
    page_size: int = Query(20, ge=1, le=100, description="Items per page"),
):
    pool = get_pool()
    rows, total_count = await get_top100(pool, snap_date, page, page_size)

    items = [
        WalletSnapshotItem(
            wallet_address=r["wallet_address"],
            snapshot_date=str(r["snapshot_date"]),
            rank=r["rank"],
            total_volume=str(r["total_volume"]),
            total_txns=r["total_txns"],
            sent_eth=str(r["sent_eth"]),
            recv_eth=str(r["recv_eth"]),
            sent_count=r["sent_count"],
            recv_count=r["recv_count"],
        )
        for r in rows
    ]

    return Top100Response(
        date=snap_date.isoformat(),
        page=page,
        page_size=page_size,
        total_count=total_count,
        items=items,
    )

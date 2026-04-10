"""GET /api/wallet/{address}/graph — wallet transaction graph edges + nodes."""

import re

from fastapi import APIRouter, Path, Query, HTTPException

from db.connection import get_pool
from db.models import get_wallet_graph
from schemas.responses import WalletGraphResponse, GraphEdge, GraphNode

router = APIRouter()

ADDRESS_RE = re.compile(r"^0x[0-9a-fA-F]{40}$")


@router.get("/api/wallet/{address}/graph", response_model=WalletGraphResponse)
async def wallet_graph(
    address: str = Path(..., description="ETH wallet address (0x...)"),
    min_volume: float = Query(0.1, ge=0, description="Minimum volume filter (ETH)"),
    limit: int = Query(500, ge=1, le=500, description="Max edges returned"),
):
    if not ADDRESS_RE.match(address):
        raise HTTPException(
            status_code=400,
            detail="Invalid ETH address. Must be 42-char hex starting with 0x.",
        )

    pool = get_pool()
    rows = await get_wallet_graph(pool, address, min_volume, limit)

    # Build edges
    edges = [
        GraphEdge(
            from_wallet=r["from_wallet"],
            to_wallet=r["to_wallet"],
            total_volume=str(r["total_volume"]),
            tx_count=r["tx_count"],
        )
        for r in rows
    ]

    # Build unique node set
    addr_lower = address.lower()
    node_addrs = set()
    for r in rows:
        node_addrs.add(r["from_wallet"])
        node_addrs.add(r["to_wallet"])

    nodes = [
        GraphNode(address=addr, is_center=(addr == addr_lower))
        for addr in sorted(node_addrs)
    ]

    # Period metadata from first row (all rows share the same window)
    period_start = str(rows[0]["period_start"]) if rows else None
    period_end = str(rows[0]["period_end"]) if rows else None

    return WalletGraphResponse(
        wallet=address,
        period_start=period_start,
        period_end=period_end,
        nodes=nodes,
        edges=edges,
    )

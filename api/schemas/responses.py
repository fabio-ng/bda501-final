"""Pydantic response models for API endpoints."""

from pydantic import BaseModel


class WalletSnapshotItem(BaseModel):
    wallet_address: str
    snapshot_date: str
    rank: int
    total_volume: str  # NUMERIC as string to preserve precision
    total_txns: int
    sent_eth: str
    recv_eth: str
    sent_count: int
    recv_count: int


class Top100Response(BaseModel):
    date: str
    page: int
    page_size: int
    total_count: int
    items: list[WalletSnapshotItem]


class GraphEdge(BaseModel):
    from_wallet: str
    to_wallet: str
    total_volume: str
    tx_count: int


class GraphNode(BaseModel):
    address: str
    is_center: bool


class WalletGraphResponse(BaseModel):
    wallet: str
    period_start: str | None
    period_end: str | None
    nodes: list[GraphNode]
    edges: list[GraphEdge]


class HealthResponse(BaseModel):
    status: str
    last_snapshot_date: str | None
    edge_period_end: str | None

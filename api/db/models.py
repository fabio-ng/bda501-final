"""Database query helpers for wallet snapshots and graph edges."""

import asyncpg


async def get_top100(
    pool: asyncpg.Pool,
    date: str,
    page: int,
    page_size: int,
) -> tuple[list[dict], int]:
    """Fetch paginated top-100 snapshot for a given date.

    Returns:
        (items, total_count)
    """
    offset = (page - 1) * page_size

    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT wallet_address, snapshot_date, rank, total_volume,
                   total_txns, sent_eth, recv_eth, sent_count, recv_count
            FROM wallet_daily_snapshot
            WHERE snapshot_date = $1
            ORDER BY rank
            LIMIT $2 OFFSET $3
            """,
            date,
            page_size,
            offset,
        )

        count_row = await conn.fetchrow(
            "SELECT COUNT(*) AS cnt FROM wallet_daily_snapshot WHERE snapshot_date = $1",
            date,
        )
        total_count = count_row["cnt"] if count_row else 0

    return [dict(r) for r in rows], total_count


async def get_wallet_graph(
    pool: asyncpg.Pool,
    address: str,
    min_volume: float,
    limit: int,
) -> list[dict]:
    """Fetch graph edges involving a wallet address.

    Returns edges where the wallet is sender or receiver,
    filtered by minimum volume and capped at limit.
    """
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT from_wallet, to_wallet, total_volume, tx_count,
                   period_start, period_end
            FROM wallet_graph_edge
            WHERE (from_wallet = $1 OR to_wallet = $1)
              AND total_volume >= $2
            ORDER BY total_volume DESC
            LIMIT $3
            """,
            address.lower(),
            min_volume,
            limit,
        )

    return [dict(r) for r in rows]


async def get_health(pool: asyncpg.Pool) -> dict:
    """Fetch latest snapshot date and edge period end."""
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT
                (SELECT MAX(snapshot_date) FROM wallet_daily_snapshot) AS last_snapshot_date,
                (SELECT MAX(period_end) FROM wallet_graph_edge) AS edge_period_end
            """
        )

    return {
        "last_snapshot_date": str(row["last_snapshot_date"]) if row["last_snapshot_date"] else None,
        "edge_period_end": str(row["edge_period_end"]) if row["edge_period_end"] else None,
    }

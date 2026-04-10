"""Spark → PostgreSQL writer — upsert snapshots and atomic-swap edges."""

import os
import logging

import psycopg2
from pyspark.sql import DataFrame

logger = logging.getLogger(__name__)


def _jdbc_url() -> str:
    host = os.environ.get("POSTGRES_HOST", "postgres")
    port = os.environ.get("POSTGRES_PORT", "5432")
    db = os.environ.get("POSTGRES_DB", "ethdb")
    return f"jdbc:postgresql://{host}:{port}/{db}"


def _jdbc_properties() -> dict:
    return {
        "user": os.environ.get("POSTGRES_USER", "ethuser"),
        "password": os.environ.get("POSTGRES_PASSWORD", "ethpass"),
        "driver": "org.postgresql.Driver",
    }


def _pg_connect():
    """Raw psycopg2 connection for DDL operations."""
    return psycopg2.connect(
        host=os.environ.get("POSTGRES_HOST", "postgres"),
        port=os.environ.get("POSTGRES_PORT", "5432"),
        dbname=os.environ.get("POSTGRES_DB", "ethdb"),
        user=os.environ.get("POSTGRES_USER", "ethuser"),
        password=os.environ.get("POSTGRES_PASSWORD", "ethpass"),
    )


def upsert_snapshot(df: DataFrame) -> None:
    """Write daily snapshot to PostgreSQL via temp table + upsert.

    Uses INSERT ... ON CONFLICT (wallet_address, snapshot_date) DO UPDATE
    to handle Spark re-runs idempotently.
    """
    url = _jdbc_url()
    props = _jdbc_properties()
    temp_table = "wallet_daily_snapshot_tmp"

    # Write to temp table
    df.write.jdbc(url, temp_table, mode="overwrite", properties=props)

    # Upsert from temp into main table
    conn = _pg_connect()
    try:
        with conn.cursor() as cur:
            cur.execute(f"""
                INSERT INTO wallet_daily_snapshot
                    (wallet_address, snapshot_date, rank, total_volume, total_txns,
                     sent_eth, recv_eth, sent_count, recv_count, created_at)
                SELECT
                    wallet_address, snapshot_date, rank, total_volume, total_txns,
                    sent_eth, recv_eth, sent_count, recv_count, NOW()
                FROM {temp_table}
                ON CONFLICT (wallet_address, snapshot_date)
                DO UPDATE SET
                    rank         = EXCLUDED.rank,
                    total_volume = EXCLUDED.total_volume,
                    total_txns   = EXCLUDED.total_txns,
                    sent_eth     = EXCLUDED.sent_eth,
                    recv_eth     = EXCLUDED.recv_eth,
                    sent_count   = EXCLUDED.sent_count,
                    recv_count   = EXCLUDED.recv_count,
                    created_at   = NOW();
            """)
            cur.execute(f"DROP TABLE IF EXISTS {temp_table};")
        conn.commit()
        logger.info("Upserted snapshot to wallet_daily_snapshot")
    finally:
        conn.close()


def atomic_swap_edges(df: DataFrame) -> None:
    """Write edge aggregate to PostgreSQL via staging table + atomic swap.

    Sequence:
      1. Truncate staging table
      2. Spark writes full edge set to staging
      3. BEGIN; RENAME main → old; RENAME staging → main; DROP old; COMMIT;
      4. Recreate empty staging table
    """
    url = _jdbc_url()
    props = _jdbc_properties()

    conn = _pg_connect()
    try:
        # Step 1: Truncate staging
        with conn.cursor() as cur:
            cur.execute("TRUNCATE TABLE wallet_graph_edge_staging;")
        conn.commit()
    finally:
        conn.close()

    # Step 2: Write to staging
    df.write.jdbc(url, "wallet_graph_edge_staging", mode="append", properties=props)
    logger.info("Wrote %d edge rows to staging table", df.count())

    # Step 3: Atomic swap
    conn = _pg_connect()
    try:
        conn.autocommit = False
        with conn.cursor() as cur:
            cur.execute("ALTER TABLE wallet_graph_edge RENAME TO wallet_graph_edge_old;")
            cur.execute("ALTER TABLE wallet_graph_edge_staging RENAME TO wallet_graph_edge;")
            cur.execute("DROP TABLE wallet_graph_edge_old;")
        conn.commit()
        logger.info("Atomic swap complete: staging → wallet_graph_edge")

        # Step 4: Recreate empty staging table
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE wallet_graph_edge_staging (
                    id              SERIAL PRIMARY KEY,
                    from_wallet     VARCHAR(42) NOT NULL,
                    to_wallet       VARCHAR(42) NOT NULL,
                    total_volume    NUMERIC(38,18) NOT NULL DEFAULT 0,
                    tx_count        BIGINT NOT NULL DEFAULT 0,
                    period_start    DATE NOT NULL,
                    period_end      DATE NOT NULL,
                    updated_at      TIMESTAMP NOT NULL DEFAULT NOW()
                );
            """)
        logger.info("Recreated empty wallet_graph_edge_staging")
    finally:
        conn.close()

-- ============================================
-- ETH Transaction Analytics — PostgreSQL Schema
-- ============================================
-- Serving-only tables: no raw transactions stored.
-- Spark is the sole writer via JDBC.
-- ============================================

-- wallet_daily_snapshot: one row per wallet per day (top 100)
CREATE TABLE IF NOT EXISTS wallet_daily_snapshot (
    id              SERIAL PRIMARY KEY,
    wallet_address  VARCHAR(42) NOT NULL,
    snapshot_date   DATE NOT NULL,
    rank            INT NOT NULL,
    total_volume    NUMERIC(38,18) NOT NULL DEFAULT 0,
    total_txns      BIGINT NOT NULL DEFAULT 0,
    sent_eth        NUMERIC(38,18) NOT NULL DEFAULT 0,
    recv_eth        NUMERIC(38,18) NOT NULL DEFAULT 0,
    sent_count      BIGINT NOT NULL DEFAULT 0,
    recv_count      BIGINT NOT NULL DEFAULT 0,
    created_at      TIMESTAMP NOT NULL DEFAULT NOW()
);

-- Prevent duplicates on Spark re-runs
CREATE UNIQUE INDEX IF NOT EXISTS idx_snapshot_wallet_date
    ON wallet_daily_snapshot (wallet_address, snapshot_date);

-- Primary query pattern: get top 100 for a given date
CREATE INDEX IF NOT EXISTS idx_snapshot_date_rank
    ON wallet_daily_snapshot (snapshot_date, rank);


-- wallet_graph_edge: one row per directed wallet pair (180-day rolling window)
CREATE TABLE IF NOT EXISTS wallet_graph_edge (
    id              SERIAL PRIMARY KEY,
    from_wallet     VARCHAR(42),
    to_wallet       VARCHAR(42),
    total_volume    NUMERIC(38,18) NOT NULL DEFAULT 0,
    tx_count        BIGINT NOT NULL DEFAULT 0,
    period_start    DATE NOT NULL,
    period_end      DATE NOT NULL,
    updated_at      TIMESTAMP NOT NULL DEFAULT NOW()
);

-- One row per directed pair (single rolling window)
CREATE UNIQUE INDEX IF NOT EXISTS idx_edge_pair
    ON wallet_graph_edge (from_wallet, to_wallet);

-- Query: all edges where wallet is sender
CREATE INDEX IF NOT EXISTS idx_edge_from
    ON wallet_graph_edge (from_wallet);

-- Query: all edges where wallet is receiver
CREATE INDEX IF NOT EXISTS idx_edge_to
    ON wallet_graph_edge (to_wallet);


-- Staging table for atomic swap (identical schema, indexes not needed)
CREATE TABLE IF NOT EXISTS wallet_graph_edge_staging (
    id              SERIAL PRIMARY KEY,
    from_wallet     VARCHAR(42),
    to_wallet       VARCHAR(42),
    total_volume    NUMERIC(38,18) NOT NULL DEFAULT 0,
    tx_count        BIGINT NOT NULL DEFAULT 0,
    period_start    DATE NOT NULL,
    period_end      DATE NOT NULL,
    updated_at      TIMESTAMP NOT NULL DEFAULT NOW()
);

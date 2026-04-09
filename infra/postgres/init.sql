-- ============================================================
-- ETHEREUM PHISHING DETECTION PLATFORM - DATABASE SCHEMA
-- PostgreSQL initialization script for Cloud SQL
-- ============================================================

-- ============================================================
-- CORE TABLES (serve website + API)
-- ============================================================

-- 1. addresses: Known Ethereum addresses with labels
CREATE TABLE IF NOT EXISTS addresses (
    address         VARCHAR(42) PRIMARY KEY,   -- 0x + 40 hex chars
    label           SMALLINT,                  -- 0=legit, 1=phishing, NULL=unknown
    label_source    VARCHAR(100),              -- 'xblock-eth', 'etherscan', 'analyst'
    first_seen_at   TIMESTAMP,
    last_seen_at    TIMESTAMP,
    total_tx_count  INTEGER DEFAULT 0,
    is_contract     BOOLEAN DEFAULT FALSE,
    created_at      TIMESTAMP DEFAULT NOW(),
    updated_at      TIMESTAMP DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_addresses_label ON addresses(label);
CREATE INDEX IF NOT EXISTS idx_addresses_updated ON addresses(updated_at);

-- 2. predictions: Every prediction made by the system
CREATE TABLE IF NOT EXISTS predictions (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    address         VARCHAR(42) NOT NULL REFERENCES addresses(address)
                        ON DELETE CASCADE,
    phishing_score  REAL NOT NULL CHECK (phishing_score BETWEEN 0 AND 1),
    prediction      VARCHAR(20) NOT NULL,      -- 'phishing' | 'legitimate'
    confidence      VARCHAR(10) NOT NULL,       -- 'high' | 'medium' | 'low'
    threshold_used  REAL NOT NULL DEFAULT 0.7,
    model_version_id INTEGER REFERENCES model_versions(id),
    inference_mode  VARCHAR(10) NOT NULL DEFAULT 'mock',  -- 'mock' | 'real'
    inference_time_ms REAL,
    source          VARCHAR(20) NOT NULL DEFAULT 'api',  -- 'api' | 'batch' | 'streaming'
    is_known_address BOOLEAN DEFAULT FALSE,
    risk_factors    JSONB,                     -- ["factor1", "factor2"]
    request_metadata JSONB,                    -- {correlation_id, ip, user_agent}
    created_at      TIMESTAMP DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_predictions_address ON predictions(address);
CREATE INDEX IF NOT EXISTS idx_predictions_score ON predictions(phishing_score DESC);
CREATE INDEX IF NOT EXISTS idx_predictions_created ON predictions(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_predictions_source ON predictions(source);
CREATE INDEX IF NOT EXISTS idx_predictions_model ON predictions(model_version_id);

-- 3. model_versions: Registered model versions
CREATE TABLE IF NOT EXISTS model_versions (
    id              SERIAL PRIMARY KEY,
    version         VARCHAR(50) UNIQUE NOT NULL,   -- '1.0.0', '1.1.0'
    model_type      VARCHAR(50) NOT NULL,           -- 'GraphSAGE', 'GAT'
    model_path      VARCHAR(500),                   -- file path or S3 URI
    is_active       BOOLEAN DEFAULT FALSE,
    feature_count   INTEGER,
    threshold       REAL DEFAULT 0.7,
    training_dataset VARCHAR(100),
    graph_nodes     BIGINT,
    graph_edges     BIGINT,
    trained_at      TIMESTAMP,
    deployed_at     TIMESTAMP,
    retired_at      TIMESTAMP,
    created_at      TIMESTAMP DEFAULT NOW()
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_model_active ON model_versions(is_active)
    WHERE is_active = TRUE;

-- 4. model_metrics: Evaluation metrics per model version
CREATE TABLE IF NOT EXISTS model_metrics (
    id              SERIAL PRIMARY KEY,
    model_version_id INTEGER NOT NULL REFERENCES model_versions(id)
                        ON DELETE CASCADE,
    metric_name     VARCHAR(50) NOT NULL,        -- 'test_f1', 'test_precision', etc.
    metric_value    REAL NOT NULL,
    dataset_split   VARCHAR(20) DEFAULT 'test',  -- 'train', 'val', 'test'
    created_at      TIMESTAMP DEFAULT NOW(),
    UNIQUE(model_version_id, metric_name, dataset_split)
);
CREATE INDEX IF NOT EXISTS idx_metrics_model ON model_metrics(model_version_id);

-- 5. alerts: High-confidence phishing alerts
CREATE TABLE IF NOT EXISTS alerts (
    id              SERIAL PRIMARY KEY,
    address         VARCHAR(42) NOT NULL,
    phishing_score  REAL NOT NULL,
    trigger_source  VARCHAR(20) NOT NULL,         -- 'streaming', 'batch', 'api'
    trigger_tx_hash VARCHAR(66),                  -- transaction that triggered alert
    model_version_id INTEGER REFERENCES model_versions(id),
    reviewed        BOOLEAN DEFAULT FALSE,
    analyst_label   SMALLINT,                     -- NULL until reviewed
    reviewer_notes  TEXT,
    created_at      TIMESTAMP DEFAULT NOW(),
    reviewed_at     TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_alerts_address ON alerts(address);
CREATE INDEX IF NOT EXISTS idx_alerts_score ON alerts(phishing_score DESC);
CREATE INDEX IF NOT EXISTS idx_alerts_created ON alerts(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_alerts_unreviewed ON alerts(reviewed) WHERE reviewed = FALSE;

-- ============================================================
-- OPERATIONAL TABLES (observability + job tracking)
-- ============================================================

-- 6. ingestion_jobs: Track data ingestion runs
CREATE TABLE IF NOT EXISTS ingestion_jobs (
    id              SERIAL PRIMARY KEY,
    job_type        VARCHAR(30) NOT NULL,         -- 'mock', 'etherscan', 'kafka', 'file'
    status          VARCHAR(20) NOT NULL DEFAULT 'pending',
                                                  -- 'pending','running','completed','failed'
    records_ingested INTEGER DEFAULT 0,
    records_failed  INTEGER DEFAULT 0,
    error_message   TEXT,
    started_at      TIMESTAMP,
    completed_at    TIMESTAMP,
    created_at      TIMESTAMP DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_ingest_status ON ingestion_jobs(status);

-- 7. etl_jobs: Track ETL pipeline runs
CREATE TABLE IF NOT EXISTS etl_jobs (
    id              SERIAL PRIMARY KEY,
    job_name        VARCHAR(100) NOT NULL,        -- 'ingest_raw', 'process_features'
    status          VARCHAR(20) NOT NULL DEFAULT 'pending',
    input_records   BIGINT DEFAULT 0,
    output_records  BIGINT DEFAULT 0,
    error_message   TEXT,
    spark_app_id    VARCHAR(100),
    started_at      TIMESTAMP,
    completed_at    TIMESTAMP,
    duration_seconds REAL,
    created_at      TIMESTAMP DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_etl_status ON etl_jobs(status);
CREATE INDEX IF NOT EXISTS idx_etl_name ON etl_jobs(job_name);

-- 8. api_requests_log: Audit trail for API calls
CREATE TABLE IF NOT EXISTS api_requests_log (
    id              BIGSERIAL PRIMARY KEY,
    correlation_id  VARCHAR(36),                  -- UUID correlation
    method          VARCHAR(10) NOT NULL,
    endpoint        VARCHAR(200) NOT NULL,
    status_code     INTEGER,
    request_body    JSONB,                        -- sanitized request
    response_time_ms REAL,
    client_ip       VARCHAR(45),
    user_agent      VARCHAR(500),
    error_message   TEXT,
    created_at      TIMESTAMP DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_api_log_endpoint ON api_requests_log(endpoint);
CREATE INDEX IF NOT EXISTS idx_api_log_created ON api_requests_log(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_api_log_correlation ON api_requests_log(correlation_id);

-- ============================================================
-- OPTIONAL TABLES (extend as needed)
-- ============================================================

-- 9. transactions_raw: Raw transaction data (optional — GCS preferred)
CREATE TABLE IF NOT EXISTS transactions_raw (
    tx_hash         VARCHAR(66) PRIMARY KEY,
    from_address    VARCHAR(42) NOT NULL,
    to_address      VARCHAR(42),
    value_eth       NUMERIC(30, 18),
    gas             BIGINT,
    gas_price       BIGINT,
    block_number    BIGINT,
    block_timestamp TIMESTAMP,
    input_data      TEXT,
    is_error        BOOLEAN DEFAULT FALSE,
    ingestion_job_id INTEGER REFERENCES ingestion_jobs(id),
    created_at      TIMESTAMP DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_txraw_from ON transactions_raw(from_address);
CREATE INDEX IF NOT EXISTS idx_txraw_to ON transactions_raw(to_address);
CREATE INDEX IF NOT EXISTS idx_txraw_block ON transactions_raw(block_number);

-- 10. transactions_processed: Cleaned + enriched transactions (optional)
CREATE TABLE IF NOT EXISTS transactions_processed (
    tx_hash         VARCHAR(66) PRIMARY KEY,
    from_address    VARCHAR(42) NOT NULL,
    to_address      VARCHAR(42),
    value_eth       NUMERIC(30, 18),
    from_label      SMALLINT,
    to_label        SMALLINT,
    etl_job_id      INTEGER REFERENCES etl_jobs(id),
    processed_at    TIMESTAMP DEFAULT NOW()
);

-- 11. feature_snapshots: Pre-computed feature vectors (optional)
CREATE TABLE IF NOT EXISTS feature_snapshots (
    address         VARCHAR(42) NOT NULL,
    snapshot_version VARCHAR(50) NOT NULL,         -- matches model version
    features        JSONB NOT NULL,               -- {"in_degree": 5, "out_degree": 3, ...}
    created_at      TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (address, snapshot_version)
);
CREATE INDEX IF NOT EXISTS idx_features_version ON feature_snapshots(snapshot_version);

-- ============================================================
-- VIEWS (for dashboard queries)
-- ============================================================

CREATE OR REPLACE VIEW v_dashboard_summary AS
SELECT
    (SELECT COUNT(*) FROM predictions) AS total_predictions,
    (SELECT COUNT(*) FROM predictions WHERE prediction = 'phishing') AS total_phishing,
    (SELECT COUNT(*) FROM alerts) AS total_alerts,
    (SELECT COUNT(*) FROM alerts WHERE reviewed = FALSE) AS unreviewed_alerts,
    (SELECT COUNT(*) FROM predictions
     WHERE created_at >= NOW() - INTERVAL '24 hours') AS predictions_24h,
    (SELECT AVG(inference_time_ms) FROM predictions
     WHERE created_at >= NOW() - INTERVAL '24 hours') AS avg_inference_ms;

CREATE OR REPLACE VIEW v_daily_predictions AS
SELECT
    DATE(created_at) AS prediction_date,
    source,
    COUNT(*) AS total,
    SUM(CASE WHEN prediction = 'phishing' THEN 1 ELSE 0 END) AS phishing_count,
    AVG(phishing_score) AS avg_score,
    AVG(inference_time_ms) AS avg_inference_ms
FROM predictions
GROUP BY DATE(created_at), source
ORDER BY prediction_date DESC;

CREATE OR REPLACE VIEW v_top_risky AS
SELECT
    address,
    MAX(phishing_score) AS max_score,
    COUNT(*) AS detection_count,
    MAX(created_at) AS last_detected
FROM predictions
WHERE prediction = 'phishing'
GROUP BY address
ORDER BY max_score DESC
LIMIT 100;

CREATE OR REPLACE VIEW v_alert_queue AS
SELECT
    a.id, a.address, a.phishing_score, a.trigger_source,
    a.trigger_tx_hash, a.created_at,
    mv.version AS model_version
FROM alerts a
LEFT JOIN model_versions mv ON a.model_version_id = mv.id
WHERE a.reviewed = FALSE
ORDER BY a.phishing_score DESC;

-- ============================================================
-- GRANTS (if using separate user)
-- ============================================================
-- GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO eth_app_user;
-- GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO eth_app_user;
-- GRANT SELECT ON ALL VIEWS IN SCHEMA public TO eth_app_user;

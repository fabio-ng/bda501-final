-- ============================================================
-- Phase 4: PostgreSQL Schema for Ethereum Phishing Detection
-- ============================================================

-- Database
-- CREATE DATABASE eth_phishing;

-- 1. Phishing labels (known ground truth)
CREATE TABLE IF NOT EXISTS phishing_labels (
    address         VARCHAR(42) PRIMARY KEY,  -- Ethereum address (0x...)
    label           SMALLINT NOT NULL,         -- 1 = phishing, 0 = legitimate
    source          VARCHAR(100),              -- e.g., 'etherscan', 'xblock-eth'
    created_at      TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_labels_label ON phishing_labels(label);

-- 2. Detection results (batch + streaming predictions)
CREATE TABLE IF NOT EXISTS detection_results (
    id              SERIAL PRIMARY KEY,
    address         VARCHAR(42) NOT NULL,
    phishing_score  REAL NOT NULL,             -- 0.0 to 1.0
    predicted_label SMALLINT NOT NULL,         -- 0 or 1
    model_version   VARCHAR(100),              -- e.g., 'graphsage_v1_20260404'
    source          VARCHAR(20) NOT NULL,      -- 'batch' or 'streaming'
    detected_at     TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_results_address ON detection_results(address);
CREATE INDEX idx_results_score ON detection_results(phishing_score DESC);
CREATE INDEX idx_results_detected ON detection_results(detected_at DESC);
CREATE INDEX idx_results_source ON detection_results(source);

-- 3. Streaming alerts (high-risk addresses flagged in real-time)
CREATE TABLE IF NOT EXISTS phishing_alerts (
    id              SERIAL PRIMARY KEY,
    address         VARCHAR(42) NOT NULL,
    phishing_score  REAL NOT NULL,
    tx_hash         VARCHAR(66),               -- triggering transaction hash
    from_address    VARCHAR(42),
    to_address      VARCHAR(42),
    tx_value        NUMERIC(30, 18),           -- ETH value
    model_version   VARCHAR(100),
    alerted_at      TIMESTAMP DEFAULT NOW(),
    reviewed        BOOLEAN DEFAULT FALSE,
    analyst_label   SMALLINT                   -- NULL until reviewed, then 0 or 1
);

CREATE INDEX idx_alerts_score ON phishing_alerts(phishing_score DESC);
CREATE INDEX idx_alerts_time ON phishing_alerts(alerted_at DESC);
CREATE INDEX idx_alerts_reviewed ON phishing_alerts(reviewed);

-- 4. Model registry (tracks deployed model versions)
CREATE TABLE IF NOT EXISTS model_registry (
    id              SERIAL PRIMARY KEY,
    version         VARCHAR(100) UNIQUE NOT NULL,
    gcs_path        VARCHAR(500) NOT NULL,
    test_f1         REAL,
    test_auc_roc    REAL,
    test_auc_pr     REAL,
    is_active       BOOLEAN DEFAULT FALSE,     -- currently deployed model
    trained_at      TIMESTAMP,
    deployed_at     TIMESTAMP DEFAULT NOW()
);

-- Only one active model at a time
CREATE UNIQUE INDEX idx_active_model ON model_registry(is_active) WHERE is_active = TRUE;

-- 5. Views for Grafana dashboards

-- Daily detection counts
CREATE OR REPLACE VIEW v_daily_detections AS
SELECT
    DATE(detected_at) AS detection_date,
    source,
    COUNT(*) AS total_detections,
    SUM(CASE WHEN predicted_label = 1 THEN 1 ELSE 0 END) AS phishing_count,
    AVG(phishing_score) AS avg_score
FROM detection_results
GROUP BY DATE(detected_at), source
ORDER BY detection_date DESC;

-- Top risky addresses
CREATE OR REPLACE VIEW v_top_risky_addresses AS
SELECT
    address,
    MAX(phishing_score) AS max_score,
    COUNT(*) AS detection_count,
    MAX(detected_at) AS last_detected
FROM detection_results
WHERE predicted_label = 1
GROUP BY address
ORDER BY max_score DESC
LIMIT 100;

-- Alert review queue
CREATE OR REPLACE VIEW v_alert_review_queue AS
SELECT
    id, address, phishing_score, tx_hash,
    from_address, to_address, tx_value,
    alerted_at
FROM phishing_alerts
WHERE reviewed = FALSE
ORDER BY phishing_score DESC;

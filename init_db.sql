-- Create tables for the phishing detection system
CREATE TABLE IF NOT EXISTS addresses (
    id SERIAL PRIMARY KEY,
    address VARCHAR(42) UNIQUE NOT NULL,
    first_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_addresses_address ON addresses(address);

CREATE TABLE IF NOT EXISTS predictions (
    id SERIAL PRIMARY KEY,
    address VARCHAR(42) NOT NULL,
    phishing_score FLOAT NOT NULL,
    is_phishing BOOLEAN NOT NULL,
    model_version VARCHAR(50),
    inference_mode VARCHAR(20),
    threshold FLOAT,
    inference_time_ms FLOAT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_predictions_address ON predictions(address);
CREATE INDEX idx_predictions_created_at ON predictions(created_at DESC);
CREATE INDEX idx_predictions_is_phishing ON predictions(is_phishing);
CREATE INDEX idx_predictions_score ON predictions(phishing_score);

CREATE TABLE IF NOT EXISTS alerts (
    id SERIAL PRIMARY KEY,
    address VARCHAR(42) NOT NULL,
    phishing_score FLOAT NOT NULL,
    alert_type VARCHAR(50),
    severity VARCHAR(20),
    message TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    reviewed BOOLEAN DEFAULT FALSE,
    reviewed_at TIMESTAMP,
    notes TEXT
);

CREATE INDEX idx_alerts_address ON alerts(address);
CREATE INDEX idx_alerts_created_at ON alerts(created_at DESC);
CREATE INDEX idx_alerts_reviewed ON alerts(reviewed);
CREATE INDEX idx_alerts_severity ON alerts(severity);

-- Grant permissions
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO postgres;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO postgres;

"""
Shared configuration for the GNN Ethereum Phishing Detection pipeline.
All phases (4-6) import from this module.
"""

import os

# ──────────────────────────────────────────────
# Google Cloud Platform
# ──────────────────────────────────────────────
GCP_PROJECT = os.getenv("GCP_PROJECT", "eth-phishing-detection")
GCP_REGION = os.getenv("GCP_REGION", "us-central1")

# GCS buckets
GCS_BUCKET = os.getenv("GCS_BUCKET", "eth-phishing-data")
GCS_MODELS_PATH = f"gs://{GCS_BUCKET}/models/"
GCS_PROCESSED_PATH = f"gs://{GCS_BUCKET}/processed/"
GCS_RAW_PATH = f"gs://{GCS_BUCKET}/raw/"
GCS_CHECKPOINTS_PATH = f"gs://{GCS_BUCKET}/checkpoints/"

# Cloud SQL (PostgreSQL)
DB_HOST = os.getenv("DB_HOST", "127.0.0.1")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
DB_NAME = os.getenv("DB_NAME", "eth_phishing")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "")

DB_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
DB_JDBC_URL = f"jdbc:postgresql://{DB_HOST}:{DB_PORT}/{DB_NAME}"

# ──────────────────────────────────────────────
# Kafka
# ──────────────────────────────────────────────
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC_NEW_TX = "new_transactions"
KAFKA_TOPIC_ALERTS = "phishing_alerts"
KAFKA_CONSUMER_GROUP = "phishing-detector"

# ──────────────────────────────────────────────
# Model
# ──────────────────────────────────────────────
MODEL_FILENAME = "graphsage_phishing.pt"
MODEL_LOCAL_PATH = os.getenv("MODEL_LOCAL_PATH", f"/tmp/{MODEL_FILENAME}")
MODEL_GCS_PATH = f"{GCS_MODELS_PATH}{MODEL_FILENAME}"

# GraphSAGE hyperparameters (must match training)
IN_FEATS = 12
HIDDEN_FEATS = 128
OUT_FEATS = 2
DROPOUT = 0.5
FANOUTS = [15, 10]

# ──────────────────────────────────────────────
# Inference
# ──────────────────────────────────────────────
PHISHING_THRESHOLD = 0.7
INFERENCE_BATCH_SIZE = 256
MODEL_REFRESH_INTERVAL_HOURS = 24

# ──────────────────────────────────────────────
# Etherscan API
# ──────────────────────────────────────────────
ETHERSCAN_API_KEY = os.getenv("ETHERSCAN_API_KEY", "")
ETHERSCAN_API_URL = "https://api.etherscan.io/api"

# ──────────────────────────────────────────────
# FastAPI
# ──────────────────────────────────────────────
API_HOST = os.getenv("API_HOST", "0.0.0.0")
API_PORT = int(os.getenv("API_PORT", "8000"))

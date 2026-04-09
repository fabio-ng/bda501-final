# Ethereum Phishing Detection Platform

> **BDA501 Final Project** — GNN-based Ethereum Phishing Detection with Full Platform Infrastructure  
> Architecture: **Local Compute (Rancher Desktop)** + **Cloud Storage (Google Cloud Platform)**

---

## Overview

This platform detects phishing addresses on the Ethereum blockchain using Graph Neural Networks (GraphSAGE). The system is designed as a **hybrid architecture**:

- **Compute** runs locally via Docker containers on Rancher Desktop (API, dashboard, Kafka, Spark)
- **Storage** lives on Google Cloud Platform (Cloud SQL PostgreSQL, Google Cloud Storage)
- **Model** can be plugged in later — the platform runs in **mock mode** until a trained model is available

```
┌──────────────────────────────────┐     ┌─────────────────────────┐
│  LOCAL (Rancher Desktop)         │     │  GCP (Cloud)            │
│                                  │     │                         │
│  FastAPI ──┐                     │     │  Cloud SQL PostgreSQL   │
│  Streamlit ┤── docker-compose    │◄───▶│  GCS Buckets:           │
│  Kafka     ┤                     │     │    - raw data           │
│  MLflow    ┘                     │     │    - processed data     │
│  (optional: Spark, Grafana)      │     │    - model artifacts    │
└──────────────────────────────────┘     └─────────────────────────┘
```

---

## Data sources & ingestion policy

Data for this project comes from the **Ethereum blockchain** (as ground truth), exposed through several gateways. Use each source for what it does best: **bulk history** from BigQuery, **curated training graphs** from XBlock-ETH on Kaggle, **incremental / per-address** calls via Etherscan, and **real-time** feeds via a node or throttled API for streaming.

### Source matrix


| Source                                                                                                                                                 | Role                                                                              | When to use                                                                                                  |
| ------------------------------------------------------------------------------------------------------------------------------------------------------ | --------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------ |
| **XBlock-ETH (Kaggle)** `[xblock/ethereum-phishing-transaction-network](https://www.kaggle.com/datasets/xblock/ethereum-phishing-transaction-network)` | Labeled transaction network for **offline training and evaluation** (Phases 1–3)  | Kaggle Notebook; no full-chain crawl needed for the training MVP                                             |
| **Google BigQuery** `[bigquery-public-data.crypto_ethereum](https://console.cloud.google.com/marketplace/product/google/crypto-ethereum-blockchain)`   | **Large-scale** historical transactions, batch ETL, graph supplements, retraining | Filter by `block_timestamp` / `block_number`; export Parquet → GCS; monitor bytes processed                  |
| **Etherscan API**                                                                                                                                      | **Labels**, per-address tx lists, recent activity                                 | After setting `ETHERSCAN_API_KEY`; respect **rate limits**; do **not** use as a bulk substitute for BigQuery |
| **Ethereum chain** (node WebSocket or API)                                                                                                             | **Canonical** blocks and transactions for **streaming** detection                 | Kafka producer: self-hosted node or **throttled** Etherscan polling with cache                               |


### Fetch policy (efficiency and cost)

- **Tiering:** Primary training uses the **XBlock-ETH snapshot on Kaggle**. Add or refresh large history with **BigQuery batch** jobs. Use **Etherscan** for incremental enrichment, address metadata, and small windows—not for scanning the whole chain.
- **BigQuery:** Avoid `SELECT `*; restrict columns and time/block ranges; export results to GCS; prefer **scheduled** daily/weekly jobs over repeated ad-hoc queries; watch **bytes processed** for billing.
- **Etherscan:** Set `ETHERSCAN_API_KEY` (see [Configuration](#5-configuration) and `RUN_GUIDE.md`); use **exponential backoff** on HTTP 429; **cache** responses keyed by `address` / `tx_hash`; batch logically where the API allows.
- **Kaggle:** Attach the dataset in the notebook UI—data appears under `/kaggle/input/`; avoid redundant downloads unless running [locally](RUN_GUIDE.md) with the Kaggle CLI.

### Storage policy (aligned with this repo)

- `**eth-phishing-raw` (GCS):** Raw Parquet/CSV from BigQuery export or ingest; partition by `block_number` or date for cheaper partial reads.
- `**eth-phishing-processed` (GCS):** Spark-processed graphs/features and intermediate tables synced from training or ETL.
- `**eth-phishing-models` (GCS):** Checkpoints and `current/` model artifacts (see [§10 Model Integration](#10-model-integration)).
- **Cloud SQL:** Labels, `detection_results`, alerts, and API-facing metadata—low-latency rows for dashboard and FastAPI.
- **Lifecycle:** Optional lifecycle rule on raw buckets (e.g. delete objects older than 90 days) to control storage cost—see [§2.3 Create GCS Buckets](#23-create-gcs-buckets).

```mermaid
flowchart LR
  subgraph sources [Sources]
    XBlock[XBlock_ETH_Kaggle]
    BQ[BigQuery_crypto_ethereum]
    ES[Etherscan_API]
    Chain[Chain_node_or_API]
  end
  subgraph policy [Fetch_policy]
    Tier[Tier_training_vs_batch_vs_stream]
    Limit[Rate_limits_and_caching]
  end
  subgraph stores [Stores]
    GCSR[GCS_raw_processed]
    GCSM[GCS_models]
    PG[Cloud_SQL]
  end
  XBlock --> Tier
  BQ --> Tier
  ES --> Limit
  Chain --> Limit
  Tier --> GCSR
  Limit --> GCSR
  Tier --> GCSM
  GCSR --> PG
```



For deeper architecture and ETL patterns, see `[gnn_ethereum_phishing_detection.md](gnn_ethereum_phishing_detection.md)`.

### ETL pipeline (implemented) ✅

The data pipeline is fully implemented in `[etl/](etl/)`. Each job can run standalone or via the orchestrator:

```
XBlock-ETH (Kaggle)  ──► etl/jobs/ingest_xblock.py  ──► gs://eth-phishing-raw/xblock/
                                                              │
BigQuery crypto_eth  ──► etl/jobs/ingest_bigquery.py ──► gs://eth-phishing-raw/bigquery/
                                                              │
                                                    etl/jobs/process_features.py
                                                              │
                                         ┌────────────────────┴────────────────────┐
                                         ▼                                         ▼
                              gs://eth-phishing-processed/               notebooks/02 (training)
                              features/node_features.parquet
                              features/edge_index.parquet
                              features/labels.parquet
                              features/node_to_id.pkl
                              features/feature_scaler.pkl
                                         │
                              ┌──────────┴────────────┐
                              ▼                       ▼
                         Model training         Streamlit dashboard
                    (notebooks/03, Phase 3)  (pages/0_Data_Sources.py)
```

**Quick run:**

```bash
# Install ETL deps
pip install -r etl/requirements.txt

# Option A — Full pipeline (XBlock + BigQuery + feature engineering)
python scripts/run_etl.py --source all \
  --data-dir data/raw/xblock \
  --bucket-raw eth-phishing-raw \
  --bucket-processed eth-phishing-processed

# Option B — XBlock only (no BigQuery cost)
python scripts/run_etl.py --source xblock --data-dir data/raw/xblock

# Option C — BigQuery only (specify date range)
python scripts/run_etl.py --source bigquery \
  --start-date 2024-01-01 --end-date 2024-01-31 \
  --limit 500000

# Option D — Feature engineering only (data already in GCS)
python scripts/run_etl.py --source process

# Dry run — estimate BigQuery cost without executing
python scripts/run_etl.py --source all --dry-run

# Or via Makefile
make etl-xblock         # XBlock → GCS raw
make etl-bigquery       # BigQuery → GCS raw  (set BIGQUERY_START_DATE / END_DATE)
make etl-process        # GCS raw → GCS processed features
make etl-all            # Full pipeline
make etl-dry-run        # Cost estimate
```

**Job descriptions:**


| Job                | File                             | Source → Target                                                           |
| ------------------ | -------------------------------- | ------------------------------------------------------------------------- |
| Ingest XBlock-ETH  | `etl/jobs/ingest_xblock.py`      | Kaggle CSV → `gs://…-raw/xblock/` (Parquet)                               |
| Ingest BigQuery    | `etl/jobs/ingest_bigquery.py`    | `bigquery-public-data.crypto_ethereum` → `gs://…-raw/bigquery/` (Parquet) |
| Process Features   | `etl/jobs/process_features.py`   | GCS raw → 12-feature vectors + edge_index → `gs://…-processed/features/`  |
| Export Predictions | `etl/jobs/export_predictions.py` | Feature store → batch predictions → Cloud SQL + GCS                       |


---

## Table of Contents

1. [Data sources & ingestion policy](#data-sources--ingestion-policy)
2. [Prerequisites](#1-prerequisites)
3. [GCP Setup](#2-gcp-setup)
4. [Local Environment Setup](#3-local-environment-setup)
5. [Project Scaffold](#4-project-scaffold)
6. [Configuration](#5-configuration)
7. [Database Initialization](#6-database-initialization)
8. [Build & Run](#7-build--run)
9. [Verify Installation](#8-verify-installation)
10. [Development Workflow](#9-development-workflow)
11. [Model Integration](#10-model-integration)
12. [Project Structure](#11-project-structure)
13. [Troubleshooting](#12-troubleshooting)
14. [Project phases (roadmap)](#project-phases-roadmap)

---

## 1. Prerequisites

### Required Tools


| Tool                 | Version | Purpose                                     | Install                                                           |
| -------------------- | ------- | ------------------------------------------- | ----------------------------------------------------------------- |
| **Rancher Desktop**  | Latest  | Container runtime (replaces Docker Desktop) | [rancher-desktop.io](https://rancherdesktop.io/)                  |
| **Docker CLI**       | 20.10+  | Container management                        | Included with Rancher Desktop                                     |
| **docker-compose**   | v2+     | Multi-container orchestration               | Included with Rancher Desktop                                     |
| **Python**           | 3.10+   | Local scripts, seeding data                 | [python.org](https://www.python.org/)                             |
| **Google Cloud SDK** | Latest  | `gcloud`, `gsutil` CLI tools                | [cloud.google.com/sdk](https://cloud.google.com/sdk/docs/install) |
| **Git**              | 2.0+    | Version control                             | Pre-installed on most OS                                          |


### Required Accounts


| Account                   | Purpose                                  | Free Tier?                         |
| ------------------------- | ---------------------------------------- | ---------------------------------- |
| **Google Cloud Platform** | Cloud SQL + GCS storage                  | Yes ($300 credit for new accounts) |
| **Kaggle**                | Model training with free GPU (Phase 1-3) | Yes                                |


### System Requirements


| Profile      | RAM                            | CPU     | Recommended For      |
| ------------ | ------------------------------ | ------- | -------------------- |
| **Minimal**  | 8 GB total (3 GB to Rancher)   | 2 cores | API + dashboard only |
| **Standard** | 16 GB total (6 GB to Rancher)  | 4 cores | Full MVP stack       |
| **Full**     | 32 GB total (10 GB to Rancher) | 6 cores | Everything + Spark   |


---

## 2. GCP Setup

### 2.1 Create GCP Project

```bash
# Authenticate with Google Cloud
gcloud auth login
gcloud auth application-default login

# Create project (or use existing)
export GCP_PROJECT_ID="bda501-eth-phishing"
gcloud projects create $GCP_PROJECT_ID --name="BDA501 Ethereum Phishing Detection"
gcloud config set project $GCP_PROJECT_ID

# Enable billing (required for Cloud SQL)
# Go to: https://console.cloud.google.com/billing
# Link billing account to project

# Enable required APIs
gcloud services enable sqladmin.googleapis.com
gcloud services enable storage.googleapis.com
gcloud services enable iam.googleapis.com
```

### 2.2 Create Cloud SQL Instance

```bash
# Create PostgreSQL instance (db-f1-micro = smallest, ~$7/month)
gcloud sql instances create ethphish-db \
  --database-version=POSTGRES_16 \
  --tier=db-f1-micro \
  --region=asia-southeast1 \
  --storage-size=10GB \
  --storage-auto-increase \
  --availability-type=zonal

# Set root password
gcloud sql users set-password postgres \
  --instance=ethphish-db \
  --password=YOUR_SECURE_PASSWORD_HERE

# Create application database
gcloud sql databases create eth_phishing --instance=ethphish-db
gcloud sql databases create mlflow --instance=ethphish-db

# Authorize your local IP (for direct connection during development)
MY_IP=$(curl -s ifconfig.me)
gcloud sql instances patch ethphish-db \
  --authorized-networks="${MY_IP}/32"

# Get the Cloud SQL public IP
gcloud sql instances describe ethphish-db --format="value(ipAddresses[0].ipAddress)"
# Save this IP as CLOUD_SQL_HOST in your .env file
```

### 2.3 Create GCS Buckets

```bash
# Set region
export GCS_REGION="asia-southeast1"

# Create buckets (names must be globally unique)
gsutil mb -l $GCS_REGION gs://eth-phishing-raw
gsutil mb -l $GCS_REGION gs://eth-phishing-processed
gsutil mb -l $GCS_REGION gs://eth-phishing-models

# Set lifecycle policy (auto-delete raw data after 90 days to save cost)
cat > /tmp/lifecycle.json << 'EOF'
{
  "lifecycle": {
    "rule": [
      {
        "action": {"type": "Delete"},
        "condition": {"age": 90}
      }
    ]
  }
}
EOF
gsutil lifecycle set /tmp/lifecycle.json gs://eth-phishing-raw

# Verify buckets
gsutil ls
```

### 2.4 Create Service Account

```bash
# Create service account for the application
gcloud iam service-accounts create ethphish-sa \
  --display-name="Ethereum Phishing Detection Platform"

export SA_EMAIL="ethphish-sa@${GCP_PROJECT_ID}.iam.gserviceaccount.com"

# Grant required roles
gcloud projects add-iam-policy-binding $GCP_PROJECT_ID \
  --member="serviceAccount:${SA_EMAIL}" \
  --role="roles/cloudsql.client"

gcloud projects add-iam-policy-binding $GCP_PROJECT_ID \
  --member="serviceAccount:${SA_EMAIL}" \
  --role="roles/storage.objectAdmin"

# Download service account key
mkdir -p credentials
gcloud iam service-accounts keys create credentials/gcp-service-account.json \
  --iam-account=$SA_EMAIL

# IMPORTANT: Never commit this file to git!
echo "credentials/" >> .gitignore
```

### 2.5 Verify GCP Setup

```bash
# Test Cloud SQL connection
CLOUD_SQL_HOST=$(gcloud sql instances describe ethphish-db \
  --format="value(ipAddresses[0].ipAddress)")
psql "postgresql://postgres:YOUR_PASSWORD@${CLOUD_SQL_HOST}:5432/eth_phishing"

# Test GCS access
echo "test" | gsutil cp - gs://eth-phishing-raw/test.txt
gsutil cat gs://eth-phishing-raw/test.txt
gsutil rm gs://eth-phishing-raw/test.txt

# Test service account
export GOOGLE_APPLICATION_CREDENTIALS="./credentials/gcp-service-account.json"
gcloud auth activate-service-account --key-file=$GOOGLE_APPLICATION_CREDENTIALS
gsutil ls
```

---

## 3. Local Environment Setup

### 3.1 Rancher Desktop Configuration

1. **Install Rancher Desktop** from [rancherdesktop.io](https://rancherdesktop.io/)
2. **Configure resources** (Preferences → Virtual Machine):
  - Memory: **6 GB** (standard) or **3 GB** (minimal)
  - CPUs: **4 cores** (standard) or **2 cores** (minimal)
  - Container Engine: **dockerd (moby)** — required for docker-compose compatibility
3. **Verify installation**:

```bash
docker version
docker-compose version
# Both should output version info without errors
```

### 3.2 Clone Repository

```bash
git clone <repository-url>
cd bda501-final
```

### 3.3 Python Virtual Environment (for local scripts)

```bash
python -m venv .venv
source .venv/bin/activate  # macOS/Linux
# .venv\Scripts\activate   # Windows

pip install -r requirements.txt
```

---

## 4. Project Scaffold

Create the full directory structure:

```bash
# Application services
mkdir -p api/routers api/services api/middleware
mkdir -p dashboard/pages dashboard/components
mkdir -p model_runtime
mkdir -p etl/jobs etl/schemas etl/utils
mkdir -p streaming

# Model artifacts (local cache for GCS models)
mkdir -p model_artifacts/current model_artifacts/mock

# Infrastructure
mkdir -p infra/gcp infra/postgres infra/kafka
mkdir -p infra/grafana/provisioning/datasources
mkdir -p infra/grafana/provisioning/dashboards
mkdir -p infra/grafana/dashboards
mkdir -p infra/prometheus

# Configuration & secrets
mkdir -p configs
mkdir -p credentials

# Development support
mkdir -p scripts tests
mkdir -p data/sample
mkdir -p logs/api logs/etl logs/streaming
mkdir -p notebooks
```

---

## 5. Configuration

### 5.1 Environment Variables

```bash
# Copy template and edit
cp .env.example .env
```

Edit `.env` with your GCP values:

```bash
# ── GCP Configuration (REQUIRED) ──
GCP_PROJECT_ID=bda501-eth-phishing
GOOGLE_APPLICATION_CREDENTIALS=./credentials/gcp-service-account.json

# ── Cloud SQL PostgreSQL (from step 2.2) ──
CLOUD_SQL_HOST=34.xxx.xxx.xxx          # Cloud SQL public IP
CLOUD_SQL_CONNECTION_NAME=bda501-eth-phishing:asia-southeast1:ethphish-db
POSTGRES_DB=eth_phishing
POSTGRES_USER=postgres
POSTGRES_PASSWORD=YOUR_SECURE_PASSWORD

# ── GCS Buckets (from step 2.3) ──
GCS_BUCKET_RAW=eth-phishing-raw
GCS_BUCKET_PROCESSED=eth-phishing-processed
GCS_BUCKET_MODELS=eth-phishing-models

# ── Local Services ──
KAFKA_PORT=9092
MLFLOW_PORT=5000
API_PORT=8000
DASHBOARD_PORT=8501
INFERENCE_MODE=auto
LOG_LEVEL=INFO
```

### 5.2 Gitignore

Ensure these entries exist in `.gitignore`:

```bash
# Secrets
.env
credentials/
*.json.key

# Local data
data/raw/
logs/
model_artifacts/current/model.pt
model_artifacts/v*/

# Python
.venv/
__pycache__/
*.pyc

# OS
.DS_Store
```

---

## 6. Database Initialization

Run the schema against Cloud SQL:

```bash
# Option A: Direct psql connection
export CLOUD_SQL_HOST=$(grep CLOUD_SQL_HOST .env | cut -d= -f2)
export PGPASSWORD=$(grep POSTGRES_PASSWORD .env | cut -d= -f2)

psql -h $CLOUD_SQL_HOST -U postgres -d eth_phishing -f infra/postgres/init.sql

# Option B: Using gcloud sql connect
gcloud sql connect ethphish-db --user=postgres --database=eth_phishing < infra/postgres/init.sql
```

Verify tables were created:

```bash
psql -h $CLOUD_SQL_HOST -U postgres -d eth_phishing -c "\dt"
# Should show: addresses, predictions, model_versions, model_metrics,
#              alerts, ingestion_jobs, etl_jobs, api_requests_log, etc.
```

---

## 7. Build & Run

### 7.1 Minimal Stack (api + dashboard only)

```bash
docker-compose --profile minimal up -d --build

# Services started:
#   - api         → http://localhost:8000
#   - dashboard   → http://localhost:8501
# Storage: Cloud SQL + GCS (no local containers needed)
```

### 7.2 MVP Stack (recommended for development)

```bash
docker-compose up -d --build

# Services started:
#   - api         → http://localhost:8000
#   - dashboard   → http://localhost:8501
#   - kafka       → localhost:9092
#   - zookeeper   → localhost:2181
#   - mlflow      → http://localhost:5000
# Storage: Cloud SQL + GCS
```

### 7.3 Full Stack (includes Spark + monitoring)

```bash
docker-compose --profile full --profile monitoring up -d --build

# Additional services:
#   - spark-master  → http://localhost:8080 (Spark UI)
#   - spark-worker  → (connected to master)
#   - prometheus    → http://localhost:9090
#   - grafana       → http://localhost:3000 (admin/admin)
```

### 7.4 Stop Services

```bash
# Stop all services
docker-compose down

# Stop and remove volumes (clean slate)
docker-compose down -v
```

---

## 8. Verify Installation

### 8.1 Check Container Health

```bash
docker-compose ps
# All containers should show "Up" or "healthy"
```

### 8.2 Test API

```bash
# Health check
curl http://localhost:8000/health | python -m json.tool

# Expected response:
# {
#   "status": "healthy",
#   "inference_mode": "mock",
#   "database": "connected",
#   "gcs": "connected",
#   ...
# }

# Test prediction (mock mode)
curl -X POST http://localhost:8000/predict/address \
  -H "Content-Type: application/json" \
  -d '{"address": "0x742d35cc6634c0532925a3b844bc9e7595f2bd18"}' \
  | python -m json.tool

# Model info
curl http://localhost:8000/model/info | python -m json.tool
```

### 8.3 Test Dashboard

Open in browser: **[http://localhost:8501](http://localhost:8501)**

You should see the Streamlit dashboard with:

- Overview page with summary metrics
- Mock mode banner (yellow)
- Address lookup functionality

### 8.4 Seed Sample Data

```bash
# Run from host (with .venv activated)
python scripts/seed_data.py

# Or run inside the API container
docker-compose exec api python scripts/seed_data.py
```

### 8.5 Test GCS Connectivity

```bash
# From host
gsutil ls gs://eth-phishing-models/

# Upload mock model metadata
gsutil cp model_artifacts/mock/metadata.json \
  gs://eth-phishing-models/models/current/metadata.json
```

---

## 9. Development Workflow

### 9.1 Daily Workflow

```bash
# Start services
docker-compose up -d

# Watch API logs
docker-compose logs -f api

# Make code changes (hot-reload for API with volume mount)
# Edit files in api/, dashboard/, etc.

# Rebuild after Dockerfile changes
docker-compose up -d --build api

# Stop when done
docker-compose down
```

### 9.2 Running Tests

```bash
# Unit tests
python -m pytest tests/ -v

# Test against running stack
python -m pytest tests/test_api.py -v --live
```

### 9.3 Useful Commands

```bash
# View all service logs
docker-compose logs --tail=50

# Enter API container shell
docker-compose exec api bash

# Restart single service
docker-compose restart api

# Check Cloud SQL from host
psql "postgresql://postgres:PASSWORD@CLOUD_SQL_HOST:5432/eth_phishing" \
  -c "SELECT COUNT(*) FROM predictions;"

# Upload file to GCS
gsutil cp data/sample/transactions_sample.csv gs://eth-phishing-raw/samples/
```

---

## 10. Model Integration

When the ML team completes Phase 1-3 (training), follow these steps to integrate the model:

### 10.1 Upload Model to GCS

```bash
# Upload trained model artifacts
gsutil cp model.pt gs://eth-phishing-models/models/v1/model.pt
gsutil cp metadata.json gs://eth-phishing-models/models/v1/metadata.json
gsutil cp node_features.npy gs://eth-phishing-models/models/v1/graph/
gsutil cp edge_index.npy gs://eth-phishing-models/models/v1/graph/
gsutil cp node_to_id.pkl gs://eth-phishing-models/models/v1/graph/

# Set as current model
gsutil -m rsync -r \
  gs://eth-phishing-models/models/v1/ \
  gs://eth-phishing-models/models/current/
```

### 10.2 Switch from Mock to Real

```bash
# Restart API to pick up new model
docker-compose restart api

# Verify
curl http://localhost:8000/health
# inference_mode should now be "real"

curl http://localhost:8000/model/info
# Should show real model version, type, metrics
```

### 10.3 Rollback if Needed

```bash
# Revert to mock model
gsutil -m rsync -r \
  gs://eth-phishing-models/models/mock/ \
  gs://eth-phishing-models/models/current/
docker-compose restart api
```

See `PLATFORM_DESIGN.md` Section 20 for the full integration checklist.

---

## 11. Project Structure

```
bda501-final/
├── docker-compose.yml          # MVP + optional Spark/monitoring profiles
├── .env.example                # Environment template (all GCP + service vars)
├── .env                        # Local env (gitignored)
├── Makefile                    # Convenience commands (make up, down, seed, etc.)
├── README.md                   # This file
├── PLATFORM_DESIGN.md          # Full system design document
├── RUN_GUIDE.md                # Training + deployment guide
│
├── api/                        # ✅ FastAPI backend (Phase 5)
│   ├── Dockerfile
│   ├── requirements.txt
│   ├── main.py                 # App entrypoint, lifespan, CORS, error handlers
│   ├── config.py               # Settings via env vars
│   ├── database.py             # PostgreSQL connection pool
│   ├── models.py               # Pydantic request/response schemas
│   ├── routers/                # 7 router modules (health, predict, model, etc.)
│   ├── services/               # predictor, model_loader, prediction_logger
│   └── middleware/             # correlation IDs, structured logging
│
├── dashboard/                  # ✅ Streamlit frontend (Phase 6)
│   ├── Dockerfile
│   ├── app.py                  # Home/overview page
│   ├── pages/                  # 6 pages (lookup, model info, metrics, alerts, history, admin)
│   └── components/             # Reusable widgets (prediction_card, metric_display)
│
├── model_artifacts/            # ✅ Model files (Phase 4)
│   ├── current/metadata.json   # Mock metadata (replace with real model)
│   └── mock/metadata.json      # Development mock
│
├── model_runtime/              # ✅ Inference logic (Phase 4)
│   ├── base_predictor.py       # Abstract interface
│   ├── mock_predictor.py       # Deterministic hash-based mock
│   ├── gnn_predictor.py        # Real GNN predictor (placeholder)
│   └── model_manager.py        # Version management, hot-swap
│
├── notebooks/                  # ✅ Jupyter notebooks (Phases 1-2)
│   ├── 01_data_exploration.ipynb    # EDA on XBlock-ETH dataset
│   └── 02_feature_engineering.ipynb # Graph build + 12 features
│
├── infra/                      # ✅ Infrastructure configs (Phase 4)
│   ├── postgres/init.sql       # 11 tables + 4 views
│   ├── kafka/create-topics.sh  # 3 Kafka topics
│   ├── prometheus/prometheus.yml
│   └── grafana/provisioning/   # Datasources + dashboards
│
├── configs/config.yaml         # ✅ Full platform configuration
├── scripts/                    # ✅ Utility scripts
│   ├── seed_data.py            # Seeds mock data into PostgreSQL
│   ├── download_data.py        # Downloads XBlock-ETH from Kaggle
│   └── check_health.sh         # Verifies all services
│
├── data/sample/                # ✅ Sample data (Phase 1)
│   ├── transactions_sample.csv # 100 sample transactions
│   ├── addresses_sample.csv    # 50 sample addresses
│   └── phishing_labels.csv     # Known phishing labels
│
├── pipeline/                   # Legacy pipeline code (reference)
│   ├── docker-compose.yml      # Old compose (local postgres)
│   ├── api/app.py              # Earlier API iteration
│   ├── inference/              # Original inference service
│   ├── streaming/              # Kafka producer + Spark streaming
│   └── sql/schema.sql          # Earlier schema
│
├── credentials/                # GCP service account (gitignored!)
├── logs/                       # Application logs (gitignored)
└── data/raw/                   # Raw ingested data (gitignored)
```

---

## 12. Troubleshooting

### Common Issues


| Issue                             | Cause                        | Fix                                                                       |
| --------------------------------- | ---------------------------- | ------------------------------------------------------------------------- |
| `docker-compose up` fails         | Rancher Desktop not running  | Start Rancher Desktop, wait 30s                                           |
| API can't connect to Cloud SQL    | IP not authorized            | `gcloud sql instances patch ethphish-db --authorized-networks=YOUR_IP/32` |
| GCS permission denied             | Service account missing role | Grant `roles/storage.objectAdmin` to SA                                   |
| Container OOM killed              | Not enough RAM allocated     | Increase Rancher Desktop VM memory                                        |
| Port already in use               | Another service on same port | Change port in `.env` or stop conflicting service                         |
| MLflow can't connect to Cloud SQL | Firewall / password issue    | Check Cloud SQL authorized networks + password                            |


### Checking Logs

```bash
# All services
docker-compose logs --tail=100

# Specific service
docker-compose logs -f api
docker-compose logs -f dashboard

# Cloud SQL logs
gcloud sql instances list
gcloud logging read "resource.type=cloudsql_database" --limit=20
```

### Resetting Everything

```bash
# Reset local containers
docker-compose down -v

# Reset Cloud SQL data (destructive!)
gcloud sql connect ethphish-db --user=postgres < infra/postgres/init.sql

# Reset GCS data (destructive!)
gsutil -m rm -r gs://eth-phishing-raw/**
gsutil -m rm -r gs://eth-phishing-processed/**
```

---

## Quick Reference


| Service         | URL                                                                                  | Notes                             |
| --------------- | ------------------------------------------------------------------------------------ | --------------------------------- |
| **API**         | [http://localhost:8000](http://localhost:8000)                                       | FastAPI with auto-docs at `/docs` |
| **Dashboard**   | [http://localhost:8501](http://localhost:8501)                                       | Streamlit UI                      |
| **API Docs**    | [http://localhost:8000/docs](http://localhost:8000/docs)                             | Swagger UI                        |
| **MLflow**      | [http://localhost:5000](http://localhost:5000)                                       | Model tracking UI                 |
| **Spark UI**    | [http://localhost:8080](http://localhost:8080)                                       | Only with `--profile full`        |
| **Grafana**     | [http://localhost:3000](http://localhost:3000)                                       | Only with `--profile monitoring`  |
| **Cloud SQL**   | `$CLOUD_SQL_HOST:5432`                                                               | PostgreSQL on GCP                 |
| **GCS Console** | [https://console.cloud.google.com/storage](https://console.cloud.google.com/storage) | GCP Storage browser               |


---

## Cost Estimation (GCP)


| Resource             | Tier        | Estimated Cost    |
| -------------------- | ----------- | ----------------- |
| Cloud SQL PostgreSQL | db-f1-micro | ~$7–10/month      |
| GCS Storage          | Standard    | ~$0.02/GB/month   |
| GCS Operations       | Standard    | ~$0.004/10k ops   |
| **Total (dev/demo)** | —           | **~$10–15/month** |


> Use `gcloud sql instances patch ethphish-db --activation-policy=NEVER` to stop the Cloud SQL instance when not in use (saves ~70% cost).

---

## Project phases (roadmap)

This repository uses **two phase schemes**: a **README / platform** breakdown (Phases 1–8 below) and a consolidated **implementation plan** (`[implementation_plan.md](implementation_plan.md)`) with six phases for training + production. Use the mapping table so names line up when you read either document.

### README platform phases ↔ `implementation_plan.md`


| README (this file)                             | `[implementation_plan.md](implementation_plan.md)`               | Notes                                              |
| ---------------------------------------------- | ---------------------------------------------------------------- | -------------------------------------------------- |
| Phases **1–3** (Kaggle)                        | Phases **1–3**                                                   | Dataset, preprocessing, GNN training on XBlock-ETH |
| Phase **4** (scaffold / infra)                 | Phase **4** (partial): GCP, GCS, model upload, pipeline skeleton | Foundation before feature-complete streaming       |
| Phases **5–6** (API + Streamlit)               | Phase **5**: dashboard + alert API                               | REST + UI on top of Cloud SQL / mocks              |
| Phase **7** (Kafka streaming)                  | Phase **4** (partial): Kafka producer/consumer, streaming path   | Same production phase, different slice in README   |
| Phase **8** (auth, rate limits, observability) | Phase **6**: hardening, optimization, retraining hooks           | Security and ops maturity                          |


**Docker Compose path:** The root-level `[docker-compose.yml](docker-compose.yml)` is the primary compose file. Run `docker compose up` from the repository root. The `[pipeline/](pipeline/)` directory contains legacy reference code from an earlier iteration; the authoritative stack definition is now at root.

### Phase 1 — Dataset / EDA / Feature Exploration ✅ IMPLEMENTED

> **Status**: Implemented — data loading, EDA notebook, sample data, download script all created.

- **Notebook**: `[notebooks/01_data_exploration.ipynb](notebooks/01_data_exploration.ipynb)` — loads the XBlock-ETH dataset from Kaggle, explores structure, analyzes class distribution (phishing vs legitimate), generates transaction and graph statistics, produces data quality report with visualizations.
- **Download script**: `[scripts/download_data.py](scripts/download_data.py)` — downloads XBlock-ETH dataset via kagglehub/Kaggle CLI with graceful fallback to sample data.
- **Sample data** (for development without Kaggle):
  - `[data/sample/transactions_sample.csv](data/sample/transactions_sample.csv)` — 100 realistic Ethereum transactions
  - `[data/sample/addresses_sample.csv](data/sample/addresses_sample.csv)` — 50 sample addresses with statistics
  - `[data/sample/phishing_labels.csv](data/sample/phishing_labels.csv)` — 50 labeled addresses (20% phishing, 80% legitimate)

#### How to run Phase 1

```bash
# Option A: On Kaggle (recommended for full dataset)
# Upload 01_data_exploration.ipynb to Kaggle, attach xblock/ethereum-phishing-transaction-network dataset

# Option B: Locally with Kaggle CLI
pip install kagglehub
python scripts/download_data.py
jupyter notebook notebooks/01_data_exploration.ipynb

# Option C: Use sample data (no download needed)
# The notebook auto-detects and uses data/sample/ if Kaggle data is unavailable
```

### Phase 2 — Preprocessing / Graph Build / Feature Engineering ✅ IMPLEMENTED

> **Status**: Implemented — preprocessing, graph construction, 12-feature computation, normalization.

- **Notebook**: `[notebooks/02_feature_engineering.ipynb](notebooks/02_feature_engineering.ipynb)` — cleans and validates data, builds directed transaction graph via NetworkX, computes 12 node features per address (in_degree, out_degree, total_eth_received, total_eth_sent, avg_tx_value_in, avg_tx_value_out, max_tx_value, unique_in_neighbors, unique_out_neighbors, account_lifetime, failed_tx_ratio, avg_gas_used), normalizes with StandardScaler.
- **Outputs** (saved to `data/processed/`):
  - `node_features.npy` — N × 12 normalized feature matrix
  - `edge_index.npy` — 2 × E edge array (PyTorch/DGL format)
  - `labels.npy` — node labels (0=legit, 1=phishing, -1=unknown)
  - `node_to_id.pkl` — address → integer mapping
  - `feature_scaler.pkl` — fitted StandardScaler for inference

### Phase 3 — Model Training / Evaluation (TODO)

> **Status**: TODO — awaiting Phase 2 outputs on full dataset.

- **Goal**: Train GraphSAGE on the XBlock-ETH graph, evaluate with precision/recall/F1/AUC, export `model.pt` + `metadata.json`.
- **Notebook placeholder**: `[notebooks/03_model_training.ipynb](notebooks/)` (create when Phase 2 on full data is complete).
- **Reference**: `[gnn_ethereum_phishing_detection.ipynb](gnn_ethereum_phishing_detection.ipynb)` contains the full training pipeline.

### Phase 4 — System Foundation ✅ IMPLEMENTED

> **Status**: Implemented — full infrastructure: Docker Compose, schema, configs, model runtime, scripts.

- **Docker Compose**: `[docker-compose.yml](docker-compose.yml)` — MVP stack (api + dashboard + kafka + zookeeper + mlflow) with optional `--profile full` (Spark) and `--profile monitoring` (Prometheus + Grafana). No local Postgres — database is on Cloud SQL (GCP).
- **Database schema**: `[infra/postgres/init.sql](infra/postgres/init.sql)` — 11 tables (addresses, predictions, model_versions, model_metrics, alerts, ingestion_jobs, etl_jobs, api_requests_log, transactions_raw, transactions_processed, feature_snapshots) + 4 views (v_dashboard_summary, v_daily_predictions, v_top_risky, v_alert_queue).
- **Environment config**: `[.env.example](.env.example)` — complete template with all GCP, database, Kafka, and service variables.
- **App config**: `[configs/config.yaml](configs/config.yaml)` — full platform configuration.
- **Kafka topics**: `[infra/kafka/create-topics.sh](infra/kafka/create-topics.sh)` — eth.transactions, eth.alerts, eth.dead-letter.
- **Model runtime**: `[model_runtime/](model_runtime/)` — `base_predictor.py` (abstract interface), `mock_predictor.py` (deterministic hash-based), `gnn_predictor.py` (placeholder), `model_manager.py` (version management + hot-swap).
- **Mock model artifacts**: `[model_artifacts/current/metadata.json](model_artifacts/current/metadata.json)` — enables API to run in mock mode without trained model.
- **Scripts**: `[scripts/seed_data.py](scripts/seed_data.py)` (seeds 50 addresses, 200 predictions, 10 alerts, 1 model version, 6 metrics), `[scripts/check_health.sh](scripts/check_health.sh)` (verifies all services).
- **Monitoring**: `[infra/prometheus/prometheus.yml](infra/prometheus/prometheus.yml)`, `[infra/grafana/provisioning/](infra/grafana/provisioning/)`.
- **Makefile**: `[Makefile](Makefile)` — convenience commands (make up, down, build, logs, seed, health, etc.).

#### Steps to activate Phase 4

1. Complete GCP: project, APIs, Cloud SQL, GCS buckets, service account key — [§2 GCP Setup](#2-gcp-setup).
2. `cp .env.example .env` and fill in Cloud SQL host, password, GCS bucket names — [§5 Configuration](#5-configuration).
3. Apply schema: `psql -h $CLOUD_SQL_HOST -U postgres -d eth_phishing -f infra/postgres/init.sql` — [§6 Database Initialization](#6-database-initialization).
4. `docker-compose up -d --build` — starts API + Dashboard + Kafka + MLflow.
5. Seed data: `python scripts/seed_data.py` — populates tables with mock data.
6. Verify: `curl http://localhost:8000/health` — [§8 Verify Installation](#8-verify-installation).

### Phase 5 — Inference-ready Backend ✅ IMPLEMENTED

> **Status**: Implemented — full FastAPI backend with 10+ endpoints, model loader, prediction logging, middleware.

- **API code**: `[api/](api/)` — complete FastAPI application.
  - **Routers** (7 files): `health.py`, `predict.py`, `model.py`, `predictions.py`, `alerts.py`, `ingest.py`, `dashboard.py`
  - **Services** (3 files): `predictor.py` (predictor factory), `model_loader.py` (loads from model_artifacts/), `prediction_logger.py` (logs to PostgreSQL)
  - **Middleware** (2 files): `correlation.py` (UUID correlation IDs), `logging.py` (structured JSON logging via structlog)
  - **Core**: `main.py` (lifespan, CORS, error handlers), `config.py` (settings), `database.py` (connection pool), `models.py` (Pydantic schemas)
- **Endpoints**:
  - `GET /health` — service health + dependency status
  - `POST /predict/address` — single address classification with risk factors
  - `POST /predict/batch` — batch prediction (max 100 addresses)
  - `GET /model/info` — current model metadata
  - `GET /model/metrics` — evaluation metrics + confusion matrix
  - `GET /predictions/history` — paginated history with filters
  - `GET /alerts` — alert queue with filtering
  - `POST /ingest/mock-transactions` — generate test data
  - `GET /dashboard/summary` — aggregated stats for dashboard
- **Dockerfile**: `[api/Dockerfile](api/Dockerfile)` — builds from project root context, includes model_runtime.

#### Key design points

- **Mock mode**: API runs fully without trained model — `MockPredictor` returns deterministic scores based on address hash.
- **Auto-detection**: When `INFERENCE_MODE=auto`, checks for `model_artifacts/current/model.pt` — uses real predictor if found, mock otherwise.
- **Prediction logging**: Every prediction is persisted to Cloud SQL with correlation_id, inference_time, model_version.
- **Auto-alerting**: Predictions above `auto_alert_threshold` (0.85) automatically create alert records.

### Phase 6 — Streamlit Dashboard ✅ IMPLEMENTED

> **Status**: Implemented — 7-page Streamlit app with full API integration, charts, and mock mode support.

- **Dashboard code**: `[dashboard/](dashboard/)` — multi-page Streamlit application.
  - `app.py` — Home/Overview: 4 metric cards, daily trend chart, top risky addresses, mock mode banner
  - `pages/1_Address_Lookup.py` — Ethereum address classification with score gauge, risk factors, history
  - `pages/2_Model_Info.py` — Model architecture, graph stats, configuration, training info
  - `pages/3_Metrics.py` — Precision/Recall/F1/AUC cards, confusion matrix heatmap, threshold analysis
  - `pages/4_Alerts.py` — Alert queue with severity filtering, color-coded risk indicators
  - `pages/5_History.py` — Paginated prediction history with advanced filters, CSV/JSON export
  - `pages/6_Admin.py` — System health monitoring, mock data generation, configuration view
- **Reusable components**: `[dashboard/components/](dashboard/components/)` — `prediction_card.py`, `metric_display.py`
- **Dockerfile**: `[dashboard/Dockerfile](dashboard/Dockerfile)`

#### Features

- Interactive Plotly visualizations (line charts, heatmaps, gauges)
- Mock mode fallback when API is unavailable (displays sample data for demonstration)
- Ethereum address validation with real-time feedback
- Color-coded risk levels (red/yellow/green)
- Responsive layout with sidebar navigation

### Phase 7 — Kafka streaming prototype

- **Goal:** `streaming/` producer and consumer: topics for transactions and alerts, wiring toward Spark or lightweight processors as designed.
- **Data:** **Streaming** path from **Ethereum via node or Etherscan** into Kafka per [fetch policy](#data-sources--ingestion-policy)—throttle, cache, avoid bulk history here; **BigQuery** remains for batch backfills.
- **Deliverables:** `mock_producer.py`, `consumer.py`, `alert_processor.py`, Kafka topics in `infra/kafka/`.
- **Existing reference**: `[pipeline/streaming/](pipeline/streaming/)` contains `kafka_producer.py` (Etherscan poller) and `spark_streaming.py` (Spark Structured Streaming consumer).
- **See also:** `[implementation_plan.md](implementation_plan.md)` Phase 4 tasks 4.5–4.11; `[gnn_ethereum_phishing_detection.md](gnn_ethereum_phishing_detection.md)` streaming diagram.

#### Steps

1. Ensure Kafka (and Zookeeper) are up: `docker-compose up -d` — topics auto-create or run `bash infra/kafka/create-topics.sh`.
2. Configure the producer: start with a mock producer or wire `ETHERSCAN_API_KEY` for live polling per [Data sources & ingestion policy](#data-sources--ingestion-policy).
3. Run consumer and alert processor; confirm messages flow and alerts align with the DB schema.
4. (Optional) Attach Spark Structured Streaming — `[RUN_GUIDE.md](RUN_GUIDE.md)` Spark section and `[implementation_plan.md](implementation_plan.md)` tasks 4.7–4.11.

### Phase 7 — Kafka streaming prototype

- **Goal:** `streaming/` producer and consumer: topics for transactions and alerts, wiring toward Spark or lightweight processors as designed.
- **Data:** **Streaming** path from **Ethereum via node or Etherscan** into Kafka per [fetch policy](#data-sources--ingestion-policy)—throttle, cache, avoid bulk history here; **BigQuery** remains for batch backfills.
- **Deliverables:** `mock_producer.py`, `consumer.py`, `alert_processor.py`, Kafka topics in `infra/kafka/`.
- **See also:** `[implementation_plan.md](implementation_plan.md)` Phase 4 tasks 4.5–4.11; `[gnn_ethereum_phishing_detection.md](gnn_ethereum_phishing_detection.md)` streaming diagram.

#### Steps

1. Ensure Kafka (and Zookeeper if required) are up with your compose stack — [§7](#7-build--run); create or verify topics (`new_transactions`, `phishing_alerts`) — `[RUN_GUIDE.md](RUN_GUIDE.md)` (Phase 4 continued: Kafka topics) or scripts under `infra/kafka/`.
2. Configure the producer: start with `streaming/mock_producer.py` or wire `ETHERSCAN_API_KEY` for live polling per [Data sources & ingestion policy](#data-sources--ingestion-policy).
3. Run `streaming/consumer.py` and `alert_processor.py`; confirm messages flow and alerts align with your DB schema.
4. (Optional) Attach Spark Structured Streaming — `[RUN_GUIDE.md](RUN_GUIDE.md)` Spark section and `[implementation_plan.md](implementation_plan.md)` tasks 4.7–4.11.

### Phase 8 — Auth, rate limiting, observability

- **Goal:** Production hardening: API authentication, rate limits (align with **Etherscan-style** throttling patterns), structured logging, metrics/tracing hooks.
- **Data:** No new sources—protects **outbound** fetches (Etherscan) and **inbound** API abuse; audit trails in **Cloud SQL** where applicable.
- **Deliverables:** Middleware under `api/middleware/`, config, optional Grafana/Prometheus from Compose profiles.
- **See also:** `[implementation_plan.md](implementation_plan.md)` Phase 6 (monitoring, Delta/Spark optimizations, retraining loop).

#### Steps

1. Add authentication and per-client rate limiting in `api/middleware/`; document required headers or API keys in [§5](#5-configuration).
2. Enable structured logging and correlation IDs (see `PLATFORM_DESIGN.md` for conventions) so requests trace to `api_requests_log` or equivalent.
3. Turn on observability: Prometheus/Grafana via [§7.3](#73-full-stack-includes-spark--monitoring) and/or GCP Cloud Logging; define alerts for inference latency, Kafka lag, and model drift per `[implementation_plan.md](implementation_plan.md)` Phase 6.


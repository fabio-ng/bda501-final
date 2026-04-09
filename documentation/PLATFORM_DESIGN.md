# Ethereum Phishing Detection Platform — System Design Document

> **Audience**: Development team ready to build  
> **Author**: Senior Big Data Engineer / Backend Architect / MLOps / Full-stack Integration Lead  
> **Date**: April 2026  
> **Runtime**: Rancher Desktop + docker-compose (local compute) | Google Cloud Platform (data storage)  
> **Storage**: Google Cloud Storage (GCS) + Cloud SQL (PostgreSQL)  
> **Primary Language**: Python  

---

## Table of Contents

1. [Scope](#1-scope)
2. [Kiến trúc tổng thể](#2-kiến-trúc-tổng-thể)
3. [Thiết kế theo Phase](#3-thiết-kế-theo-phase)
4. [Tech Stack](#4-tech-stack)
5. [Kiến trúc Local trên Rancher Desktop](#5-kiến-trúc-local-trên-rancher-desktop)
6. [Danh sách Service](#6-danh-sách-service)
7. [Cấu trúc thư mục Project](#7-cấu-trúc-thư-mục-project)
8. [Model Integration Contract](#8-model-integration-contract)
9. [API Design](#9-api-design)
10. [Database Schema](#10-database-schema)
11. [ETL / Processing Design](#11-etl--processing-design)
12. [Mock Mode](#12-mock-mode)
13. [Website / Dashboard](#13-website--dashboard)
14. [Streaming Prototype](#14-streaming-prototype)
15. [Logging, Monitoring, Observability](#15-logging-monitoring-observability)
16. [Security & Hardening](#16-security--hardening)
17. [Docker-compose Skeleton](#17-docker-compose-skeleton)
18. [File cấu hình mẫu](#18-file-cấu-hình-mẫu)
19. [Pseudocode / Skeleton Code](#19-pseudocode--skeleton-code)
20. [Luồng tích hợp sau khi Model hoàn tất](#20-luồng-tích-hợp-sau-khi-model-hoàn-tất)
21. [Deliverables](#21-deliverables)
22. [Kết luận và Khuyến nghị](#22-kết-luận-và-khuyến-nghị)

---

## 1. Scope

### 1.1 Project Positioning

Đây **không phải** một research project. Đây là một **platform engineering project** với mục tiêu:

- Xây hệ thống sẵn sàng nhận model artifact
- Demo được end-to-end trước khi model hoàn tất
- Compute services chạy local trên Rancher Desktop (API, dashboard, Kafka, Spark)
- Data storage trên Google Cloud Platform (GCS buckets, Cloud SQL PostgreSQL)
- Kiến trúc hybrid: **local compute + cloud storage** — dễ demo, dễ scale lên production

### 1.2 Phase Breakdown


| Phase | Tên                                        | Trạng thái      | Ai làm        | Khi nào              |
| ----- | ------------------------------------------ | --------------- | ------------- | -------------------- |
| 1     | Dataset / EDA / Feature Exploration        | **TODO**        | Data team     | Sau khi platform sẵn |
| 2     | Preprocessing / Graph Build / Feature Eng. | **TODO**        | Data team     | Sau Phase 1          |
| 3     | Model Training / Evaluation                | **TODO**        | ML team       | Sau Phase 2          |
| **4** | **System Foundation**                      | **BUILD NOW**   | Platform team | Tuần 1               |
| **5** | **Inference-ready Backend**                | **BUILD NOW**   | Backend team  | Tuần 1–2             |
| **6** | **Website & Dashboard**                    | **BUILD NOW**   | Full-stack    | Tuần 2               |
| **7** | **Streaming & Alert Prototype**            | **BUILD NOW**   | Data eng.     | Tuần 2–3             |
| **8** | **Hardening**                              | **BUILD LATER** | DevOps        | Tuần 3–4             |


### 1.3 Dependency Map

```
Phase 1–3 (TODO: model team)
    │
    │  output: model.pt + metadata.json + metrics.json
    │
    ▼
┌─────────────────────────────────────────────────────────────┐
│  Phase 4: System Foundation (no model dependency)           │
│  ├── Cloud SQL PostgreSQL (GCP) + schema                    │
│  ├── GCS buckets (raw-data, processed, model-artifacts)     │
│  ├── Kafka + Zookeeper (local)                              │
│  ├── MLflow tracking server (local, artifacts → GCS)        │
│  ├── API skeleton (FastAPI, local)                          │
│  ├── Dashboard skeleton (Streamlit, local)                  │
│  └── Model interface contract (mock predictor)              │
│                                                             │
│  Phase 5: Inference Backend (uses mock until model ready)   │
│  ├── POST /predict/address                                  │
│  ├── POST /predict/batch                                    │
│  ├── Model loader (file-based, hot-swappable)               │
│  └── Prediction logging to PostgreSQL                       │
│                                                             │
│  Phase 6: Website & Dashboard (uses mock data)              │
│  ├── Address lookup UI                                      │
│  ├── Prediction result display                              │
│  ├── Model metrics page                                     │
│  └── History & alerts pages                                 │
│                                                             │
│  Phase 7: Streaming (independent of model)                  │
│  ├── Mock Kafka producer                                    │
│  ├── Consumer + enrichment                                  │
│  └── Alert pipeline                                         │
└─────────────────────────────────────────────────────────────┘
    │
    │  When model is ready: copy artifact → restart API → done
    │
    ▼
  Production-ready demo
```

### 1.4 What Depends on What


| Component            | Depends on Model?               | Can Run with Mock?   |
| -------------------- | ------------------------------- | -------------------- |
| Cloud SQL PostgreSQL | No                              | N/A                  |
| GCS storage          | No                              | N/A                  |
| Kafka + streaming    | No                              | Yes                  |
| FastAPI backend      | **Only for real predictions**   | Yes (mock predictor) |
| Streamlit dashboard  | No                              | Yes (mock data)      |
| MLflow tracking      | No                              | Yes (mock metrics)   |
| ETL pipeline (Spark) | No                              | Yes (sample data)    |
| Inference service    | **Yes, when switching to real** | Yes (mock)           |


---

## 2. Kiến trúc tổng thể

### 2.1 End-to-End Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    ETHEREUM PHISHING DETECTION PLATFORM                      │
│              (Local Compute — Rancher Desktop | Cloud Storage — GCP)         │
│                                                                             │
│  ┌──────────────┐     ┌──────────────┐     ┌──────────────────────────┐    │
│  │ DATA SOURCES │     │  INGESTION   │     │   RAW STORAGE (GCP)      │    │
│  │              │     │  (local)     │     │                          │    │
│  │ • Mock       │────▶│ • Kafka      │────▶│ • GCS bucket:           │    │
│  │   Generator  │     │   Producer   │     │   gs://eth-phishing/raw/ │    │
│  │ • CSV/JSON   │     │ • REST API   │     │ • Cloud SQL PostgreSQL  │    │
│  │   files      │     │   ingest     │     │   (transactions_raw)    │    │
│  │ • Etherscan  │     │              │     │                          │    │
│  │   API (opt)  │     │              │     │                          │    │
│  └──────────────┘     └──────────────┘     └──────────┬───────────────┘    │
│                                                        │                    │
│                                            ┌───────────▼───────────┐       │
│                                            │ ETL / PROCESSING      │       │
│                                            │ (local Spark/pandas)  │       │
│                                            │                       │       │
│                                            │ • PySpark jobs        │       │
│                                            │ • validate / clean    │       │
│                                            │ • normalize           │       │
│                                            │ • enrich features     │       │
│                                            │ • write processed     │       │
│                                            └───────────┬───────────┘       │
│                                                        │                    │
│  ┌──────────────────┐                     ┌────────────▼──────────┐        │
│  │  MODEL REGISTRY  │                     │ PROCESSED STORAGE     │        │
│  │  (GCP + local)   │                     │ (GCP)                 │        │
│  │                  │                     │                       │        │
│  │ • GCS bucket:    │                     │ • GCS bucket:         │        │
│  │   gs://eth-      │                     │   gs://eth-phishing/  │        │
│  │   phishing/      │                     │   processed/          │        │
│  │   models/        │                     │ • Cloud SQL           │        │
│  │ • MLflow         │                     │   (feature_snapshots) │        │
│  │   (local server) │                     │                       │        │
│  └────────┬─────────┘                     └───────────────────────┘        │
│           │                                                                │
│  ┌────────▼──────────────────────────────────────────────────────────┐     │
│  │                     INFERENCE SERVICE                              │     │
│  │                                                                    │     │
│  │  ┌─────────────┐    ┌──────────────┐    ┌──────────────────┐     │     │
│  │  │ Model       │    │ Predictor    │    │ Prediction       │     │     │
│  │  │ Loader      │───▶│ (Mock/Real)  │───▶│ Logger           │     │     │
│  │  │             │    │              │    │ (→ PostgreSQL)    │     │     │
│  │  └─────────────┘    └──────────────┘    └──────────────────┘     │     │
│  └───────────────────────────┬────────────────────────────────────────┘     │
│                              │                                             │
│  ┌───────────────────────────▼────────────────────────────────────────┐    │
│  │                      BACKEND API (FastAPI)                         │    │
│  │                                                                    │    │
│  │  GET /health          GET /model/info       GET /model/metrics     │    │
│  │  POST /predict/addr   POST /predict/batch   GET /predictions/hist  │    │
│  │  GET /alerts          POST /ingest/mock     GET /dashboard/summary │    │
│  └───────────────────────────┬────────────────────────────────────────┘    │
│                              │                                             │
│  ┌───────────────────────────▼────────────────────────────────────────┐    │
│  │                    WEBSITE / DASHBOARD (Streamlit)                  │    │
│  │                                                                    │    │
│  │  • Home / Overview        • Address Lookup                         │    │
│  │  • Prediction Result      • Model Info & Metrics                   │    │
│  │  • Alerts Page            • Prediction History                     │    │
│  └────────────────────────────────────────────────────────────────────┘    │
│                                                                            │
│  ┌────────────────────────────────────────────────────────────────────┐    │
│  │                    OBSERVABILITY (Optional)                         │    │
│  │  • Prometheus metrics   • Grafana dashboards   • Structured logs   │    │
│  └────────────────────────────────────────────────────────────────────┘    │
└────────────────────────────────────────────────────────────────────────────┘
```

### 2.2 Streaming Extension (Phase 7)

```
┌────────────────────────────────────────────────────────────────────┐
│                    STREAMING LAYER (Phase 7)                        │
│                                                                    │
│  Mock Producer ───▶ Kafka [eth.transactions] ───▶ Python Consumer  │
│  (simulated txns)           │                         │            │
│                             │                    enrich + score     │
│                             │                         │            │
│                             │                    ┌────▼────┐       │
│                             │                    │ score   │       │
│                             │                    │ > 0.7?  │       │
│                             │                    └────┬────┘       │
│                             │                    yes  │  no        │
│                             │                    ┌────▼────┐       │
│                             │                    │ Kafka   │       │
│                             │                    │ [alerts]│       │
│                             │                    └────┬────┘       │
│                             │                         │            │
│                             │                    PostgreSQL        │
│                             │                    (alerts table)    │
│                             │                         │            │
│                             └─────────────────▶ Dashboard update   │
└────────────────────────────────────────────────────────────────────┘
```

### 2.3 Component-to-Service Mapping


| Component             | Location        | Service / Resource                  | Port / Endpoint              | Network               |
| --------------------- | --------------- | ----------------------------------- | ---------------------------- | --------------------- |
| Cloud SQL PostgreSQL  | **GCP**         | Cloud SQL instance                  | GCP private IP / public IP   | Internet → GCP        |
| GCS Buckets           | **GCP**         | gs://eth-phishing-{env}/            | GCS API                      | Internet → GCP        |
| Kafka                 | **Local**       | `kafka`                             | 9092                         | `backend`             |
| Zookeeper             | **Local**       | `zookeeper`                         | 2181                         | `backend`             |
| Spark Master          | **Local**       | `spark-master`                      | 8080 (UI), 7077              | `backend`             |
| Spark Worker          | **Local**       | `spark-worker`                      | 8081                         | `backend`             |
| MLflow                | **Local**       | `mlflow`                            | 5000                         | `backend`             |
| FastAPI               | **Local**       | `api`                               | 8000                         | `backend`, `frontend` |
| Streamlit             | **Local**       | `dashboard`                         | 8501                         | `frontend`            |
| Grafana (opt)         | **Local**       | `grafana`                           | 3000                         | `frontend`            |
| Prometheus (opt)      | **Local**       | `prometheus`                        | 9090                         | `backend`             |


---

## 3. Thiết kế theo Phase

### Phase 1 — TODO: Dataset / EDA / Feature Exploration

> **Status**: TODO — Chờ data team thực hiện

- **Mục tiêu**: Load dataset XBlock-ETH, khám phá cấu trúc dữ liệu, phân tích phân phối
- **Input**: Kaggle dataset `xblock/ethereum-phishing-transaction-network`
- **Output**: EDA notebook, data quality report, feature candidates
- **Placeholder**: `notebooks/01_data_exploration.ipynb` (empty)

---

### Phase 2 — TODO: Preprocessing / Graph Build / Feature Engineering

> **Status**: TODO — Chờ Phase 1 xong

- **Mục tiêu**: Clean data, build transaction graph, compute node features
- **Input**: Raw CSV/Parquet từ Phase 1
- **Output**: `node_features.npy`, `edge_index.npy`, `labels.npy`, `node_to_id.pkl`
- **Placeholder**: `notebooks/02_feature_engineering.ipynb` (empty)

---

### Phase 3 — TODO: Model Training / Evaluation

> **Status**: TODO — Chờ Phase 2 xong

- **Mục tiêu**: Train GraphSAGE, evaluate, export model artifact
- **Input**: Processed numpy arrays từ Phase 2
- **Output**: `model.pt`, `metadata.json`, `metrics.json`
- **Placeholder**: `notebooks/03_model_training.ipynb` (empty)

---

### Phase 4 — System Foundation


| Attribute         | Value                                                                                     |
| ----------------- | ----------------------------------------------------------------------------------------- |
| **Mục tiêu**      | Dựng local compute stack + GCP storage, database schema, API skeleton, dashboard skeleton                  |
| **Input**         | docker-compose.yml, schema.sql, config files, GCP service account                                          |
| **Output**        | Running local platform kết nối GCS/Cloud SQL với mock data                                                 |
| **Services**      | Cloud SQL (GCP), GCS (GCP), kafka, zookeeper, spark, mlflow, api, dashboard (local)                       |
| **Done criteria** | `docker-compose up -d` chạy xong, API kết nối Cloud SQL, GCS accessible, dashboard hiển thị mock data      |


**Việc cần làm:**

- Setup GCP project + enable APIs (Cloud SQL, Cloud Storage)
- Tạo Cloud SQL PostgreSQL instance + schema (12+ tables)
- Tạo GCS buckets (eth-phishing-raw, eth-phishing-processed, eth-phishing-models)
- Tạo GCP service account + download key JSON
- Viết `docker-compose.yml` cho Rancher Desktop (local compute services)
- Setup Kafka topics (eth.transactions, eth.alerts, eth.dead-letter)
- Viết API skeleton FastAPI (kết nối Cloud SQL + GCS)
- Viết Dashboard skeleton Streamlit
- Tạo model integration contract (metadata.json schema)
- Viết mock predictor
- Seed sample data vào Cloud SQL
- Viết `.env.example` và `config.yaml` (bao gồm GCP configs)

---

### Phase 5 — Inference-ready Backend


| Attribute         | Value                                                                           |
| ----------------- | ------------------------------------------------------------------------------- |
| **Mục tiêu**      | REST API hoàn chỉnh, model loader, prediction logging                           |
| **Input**         | Mock predictor hoặc real model artifact                                         |
| **Output**        | API chạy tại `localhost:8000`, predictions lưu PostgreSQL                       |
| **Services**      | api, postgres                                                                   |
| **Done criteria** | `POST /predict/address` trả result (mock hoặc real), prediction log có trong DB |


**Việc cần làm:**

- Implement tất cả API endpoints (9 endpoints)
- Implement model loader (load from `model_artifacts/current/`)
- Implement mock predictor (random score, deterministic by address hash)
- Implement real predictor interface (abstract class)
- Implement prediction logging middleware
- Implement request validation (Ethereum address format)
- Implement model version management
- Implement model hot-swap (reload on SIGHUP hoặc API call)
- Write integration tests

---

### Phase 6 — Website & Dashboard Integration


| Attribute         | Value                                                                   |
| ----------------- | ----------------------------------------------------------------------- |
| **Mục tiêu**      | Dashboard hiển thị đầy đủ: lookup, prediction, metrics, history, alerts |
| **Input**         | API responses, mock data                                                |
| **Output**        | Streamlit app tại `localhost:8501`                                      |
| **Services**      | dashboard, api                                                          |
| **Done criteria** | Tất cả 7 màn hình render đúng, loading/error states hoạt động           |


**Việc cần làm:**

- Home / overview page
- Address lookup page
- Prediction result display
- Model info page
- Metrics visualization page
- Alerts page
- Prediction history page
- Error handling và loading states
- Responsive layout

---

### Phase 7 — Streaming & Alert Prototype


| Attribute         | Value                                                                     |
| ----------------- | ------------------------------------------------------------------------- |
| **Mục tiêu**      | Near real-time detection via Kafka → Consumer → Alert                     |
| **Input**         | Mock transaction events                                                   |
| **Output**        | Alerts trong PostgreSQL và Kafka topic                                    |
| **Services**      | kafka, api, postgres, spark (optional)                                    |
| **Done criteria** | Mock producer gửi events, consumer xử lý, alerts xuất hiện trên dashboard |


**Việc cần làm:**

- Mock Kafka producer (simulated transactions)
- Python Kafka consumer (hoặc Spark Structured Streaming)
- Enrichment logic
- Inference call (mock hoặc real)
- Alert generation (score > threshold)
- Write alerts to PostgreSQL
- Publish alerts to Kafka `eth.alerts` topic
- Dead-letter topic cho failed messages
- Dashboard real-time update

---

### Phase 8 — Hardening


| Attribute         | Value                                                         |
| ----------------- | ------------------------------------------------------------- |
| **Mục tiêu**      | Auth, rate limiting, observability, config management         |
| **Input**         | Running platform từ Phase 4–7                                 |
| **Output**        | Production-ready platform                                     |
| **Services**      | All + prometheus, grafana                                     |
| **Done criteria** | API có auth, rate limit, structured logging, health dashboard |


**Việc cần làm:**

- API key authentication
- Rate limiting (slowapi)
- CORS configuration
- Structured JSON logging
- Request correlation IDs
- Prometheus metrics endpoint
- Grafana operational dashboard
- Config management (environment-based)
- Retry logic cho Kafka consumer
- Dead-letter queue processing
- API versioning (/v1/ prefix)
- Deployment documentation

---

## 4. Tech Stack

### 4.1 Core Stack


| Layer                 | Technology           | Version           | Vai trò                                                  | Bắt buộc?                 | Local/Prod | Lý do chọn                                                   | Nhược điểm                                |
| --------------------- | -------------------- | ----------------- | -------------------------------------------------------- | ------------------------- | ---------- | ------------------------------------------------------------ | ----------------------------------------- |
| **Container Runtime** | Rancher Desktop      | Latest            | Container engine thay Docker Desktop                     | **Bắt buộc**              | Both       | Free, open-source, tương thích Docker API, không cần license | UI ít trực quan hơn Docker Desktop        |
| **Orchestration**     | docker-compose       | v2+               | Định nghĩa và chạy multi-container                       | **Bắt buộc**              | Local      | Declarative, portable, đủ cho local dev                      | Không có auto-scaling                     |
| **Relational DB**     | Cloud SQL PostgreSQL | 16                | Primary data store: predictions, alerts, metrics, logs   | **Bắt buộc**              | GCP        | Managed, auto-backup, HA, ACID, JSON support, scaling        | Monthly cost (~$7-50/mo), network latency |
| **Object Storage**    | Google Cloud Storage | N/A               | Raw data, processed data, model artifacts, MLflow store  | **Bắt buộc**              | GCP        | Scalable, durable (11 nines), integrated with GCP ecosystem  | Cần GCP account + credentials             |
| **Message Broker**    | Apache Kafka         | 7.6.0 (Confluent) | Transaction streaming, alert publishing                  | **Bắt buộc**              | Both       | High throughput, durable, partitioned                        | Heavy memory footprint (~1GB)             |
| **Kafka Dep.**        | Zookeeper            | 7.6.0             | Kafka metadata management                                | **Bắt buộc** (with Kafka) | Both       | Required by Kafka (pre-KRaft)                                | Thêm 1 service nữa                        |
| **Processing**        | Apache Spark         | 3.5               | Distributed ETL, feature engineering                     | **Optional**              | Both       | Handles large-scale data, PySpark API                        | Heavy (~2GB RAM), overkill cho small data |
| **Backend API**       | FastAPI              | 0.111+            | REST API cho frontend, inference endpoint                | **Bắt buộc**              | Both       | Async, auto-docs, Pydantic validation, fast                  | Python GIL limits concurrency             |
| **Model Serving**     | Python (custom)      | 3.11+             | Load model, run inference, manage versions               | **Bắt buộc**              | Both       | Full control, simple, matches training stack                 | Không có model caching như TF Serving     |
| **Dashboard**         | Streamlit            | 1.35+             | Interactive web dashboard                                | **Bắt buộc**              | Both       | Rapid prototyping, Python-native, rich widgets               | Limited customization vs React            |
| **Model Tracking**    | MLflow               | 2.14+             | Experiment tracking, model registry                      | **Optional**              | Both       | Standard MLOps tool, UI, artifact store                      | Thêm 1 service, setup artifact backend    |
| **Monitoring**        | Prometheus + Grafana | Latest            | Metrics collection + visualization                       | **Optional**              | Both       | Industry standard, extensive ecosystem                       | Thêm 2 services                           |
| **Reverse Proxy**     | Nginx                | Latest            | SSL termination, routing, static assets                  | **Optional**              | Prod only  | Efficient, well-known                                        | Thêm config layer                         |


### 4.2 Python Dependencies

```
# Core API
fastapi>=0.111.0
uvicorn[standard]>=0.30.0
pydantic>=2.7
python-multipart

# Database
psycopg2-binary>=2.9
sqlalchemy>=2.0

# Google Cloud Storage
google-cloud-storage>=2.16

# Kafka
confluent-kafka>=2.4
# or kafka-python>=2.0.2

# ML / Inference
torch>=2.3
numpy>=1.26
pandas>=2.2

# Dashboard
streamlit>=1.35
plotly>=5.22
requests

# ETL
pyspark>=3.5

# MLflow
mlflow>=2.14

# Monitoring (optional)
prometheus-client>=0.20
prometheus-fastapi-instrumentator

# Utilities
python-dotenv
pyyaml
structlog
httpx
```

---

## 5. Kiến trúc Local trên Rancher Desktop

### How to Run This Platform on Rancher Desktop Using docker-compose

#### 5.1 Vì sao dùng Rancher Desktop?


| Lý do                    | Giải thích                                                                   |
| ------------------------ | ---------------------------------------------------------------------------- |
| **Free & open-source**   | Không cần Docker Desktop license (bắt buộc trả tiền cho org >250 người)      |
| **Docker-compatible**    | Hỗ trợ Docker CLI, docker-compose, Dockerfiles — không cần thay đổi workflow |
| **containerd + nerdctl** | Backend linh hoạt, hỗ trợ cả dockerd (moby)                                  |
| **Kubernetes built-in**  | Có k3s sẵn nếu muốn migrate lên k8s sau                                      |
| **Cross-platform**       | macOS (Intel + Apple Silicon), Linux, Windows                                |


#### 5.2 CLI-first Mindset

```bash
# Verify Rancher Desktop is running and docker CLI works
docker version
docker-compose version

# Verify GCP authentication
gcloud auth application-default print-access-token
gcloud config get-value project

# Start the platform (local compute services)
cd /path/to/bda501-final
docker-compose up -d

# Check service health
docker-compose ps
docker-compose logs -f api

# Verify GCS connectivity from API
curl http://localhost:8000/health

# Stop platform
docker-compose down

# Full cleanup including volumes
docker-compose down -v
```

#### 5.3 Service Classification

**GCP Services (always required):**

- Cloud SQL PostgreSQL — primary data store (managed)
- GCS buckets — object storage for raw data, processed data, model artifacts

**Must-run locally (core compute):**

- `api` — backend REST API (connects to Cloud SQL + GCS)
- `dashboard` — Streamlit UI

**Should-run locally (full demo):**

- `kafka` + `zookeeper` — streaming pipeline
- `mlflow` — model tracking (artifacts → GCS)

**Optional (can skip for light demo):**

- `spark-master` + `spark-worker` — ETL processing
- `grafana` + `prometheus` — monitoring
- `nginx` — reverse proxy

#### 5.4 Volume & Bind Mount Strategy

```yaml
volumes:
  # Named volumes for local services (survives container restart)
  mlflow_data:      # MLflow local tracking DB
  grafana_data:     # Grafana dashboards + settings

  # NO pg_data or minio_data — storage is on GCP!
  # PostgreSQL → Cloud SQL (managed by GCP)
  # Object storage → GCS buckets (managed by GCP)

# Bind mounts for development (editable from host)
# ./model_artifacts:/app/model_artifacts    # Local model cache (synced from GCS)
# ./configs:/app/configs                     # Config files — editable
# ./logs:/app/logs                           # Log output — inspectable
# ./data/sample:/app/data/sample             # Sample/seed data
# ./credentials:/app/credentials:ro          # GCP service account key (read-only)
```

#### 5.5 Resource Allocation


| Machine Profile      | RAM                       | CPU     | Recommended docker-compose profile                                      |
| -------------------- | ------------------------- | ------- | ----------------------------------------------------------------------- |
| **Minimal** (8 GB)   | Allocate 3 GB to Rancher  | 2 cores | `docker-compose --profile minimal up` (api + dashboard only, DB on GCP)   |
| **Standard** (16 GB) | Allocate 6 GB to Rancher  | 4 cores | `docker-compose up` (api + dashboard + kafka + mlflow, DB on GCP)         |
| **Full** (32+ GB)    | Allocate 10 GB to Rancher | 6 cores | `docker-compose --profile full up` (everything local + Spark, DB on GCP)  |


**Rancher Desktop Settings:**

- Preferences → Virtual Machine → Memory: set per table above
- Preferences → Virtual Machine → CPUs: set per table above
- Container Engine: dockerd (moby) — for docker-compose compatibility

#### 5.6 `.env` Organization

```
.env                    # Main environment file (gitignored)
.env.example            # Template for team (committed)
credentials/
  gcp-service-account.json  # GCP service account key (gitignored, NEVER commit!)
configs/
  config.yaml           # Application config (committed)
  config.local.yaml     # Local overrides (gitignored)
```

#### 5.7 Network Design

```yaml
networks:
  backend:
    driver: bridge
    # Internal local services: kafka, spark, mlflow
  frontend:
    driver: bridge
    # User-facing local: api, dashboard, grafana

# GCP services (Cloud SQL, GCS) are accessed via internet/public IP
# from local containers — no Docker network needed
```

Separation rationale:

- `backend` network: local services that should not be directly accessible from browser
- `frontend` network: local services with ports exposed to host
- GCP services: accessed via internet from any container with `GOOGLE_APPLICATION_CREDENTIALS`
- `api` is on both networks — it bridges backend data and frontend display

#### 5.8 Keeping the Stack Light


| Strategy                 | How                                                 |
| ------------------------ | --------------------------------------------------- |
| Offload storage to GCP   | DB + object storage on cloud — less local resources |
| Slim Python images       | Use `python:3.11-slim` for all containers           |
| Docker compose profiles  | Group optional services under `--profile full`      |
| Lazy loading             | Spark only starts when ETL job is triggered         |
| Shared Python base image | One Dockerfile base for api + dashboard             |
| Volume pruning           | `docker volume prune` to reclaim space              |
| Log rotation             | Limit container log size via compose logging config |


#### 5.9 Model Artifacts (GCS + Local Cache)

```yaml
api:
  volumes:
    - ./model_artifacts:/app/model_artifacts:ro  # Local cache of GCS model
  environment:
    GCS_MODEL_BUCKET: ${GCS_MODEL_BUCKET:-eth-phishing-models}
    GCS_MODEL_PREFIX: models/current/
```

**Workflow for model team:**

1. Train model → export `model.pt` + `metadata.json`
2. Upload files to GCS: `gsutil cp model.pt gs://eth-phishing-models/models/v1/`
3. API downloads model from GCS at startup (cached locally in `model_artifacts/`)
4. Alternative: local bind mount for faster dev iteration — just drop files in `model_artifacts/current/`
5. No rebuild needed — upload to GCS + restart API, or drop files locally + restart

---

## 6. Danh sách Service

### 6.1 Minimum Viable Platform Stack

```
┌─────────────────────────────────────────────────┐
│  GCP Services (always on, managed)               │
│                                                  │
│  Cloud SQL ─────── PostgreSQL primary data store │
│  GCS buckets ───── Raw data, processed, models   │
└─────────────────────────────────────────────────┘
         │ (internet connection)
┌─────────────────────────────────────────────────┐
│  MVP Local Stack (must have for demo)            │
│                                                  │
│  kafka ─────────── Transaction streaming         │
│  zookeeper ─────── Kafka dependency              │
│  mlflow ────────── Model registry + metrics      │
│  api ───────────── FastAPI backend               │
│  dashboard ─────── Streamlit frontend            │
│                                                  │
│  Total: 5 local containers + 2 GCP services      │
│  Est. local RAM: ~3 GB                           │
└─────────────────────────────────────────────────┘
```

### 6.2 Full Stack

```
┌─────────────────────────────────────────────────┐
│  GCP Services (managed)                          │
│  Cloud SQL + GCS buckets                         │
└─────────────────────────────────────────────────┘
         │
┌─────────────────────────────────────────────────┐
│  Full Local Stack (everything)                   │
│                                                  │
│  MVP Stack (5 containers)                        │
│  + spark-master ── ETL processing coordinator    │
│  + spark-worker ── ETL processing executor       │
│  + grafana ─────── Operational monitoring UI     │
│  + prometheus ──── Metrics collection            │
│  + nginx ───────── Reverse proxy                 │
│                                                  │
│  Total: 10 local containers + 2 GCP services     │
│  Est. local RAM: ~6–8 GB                         │
└─────────────────────────────────────────────────┘
```

### 6.3 Service Decision Matrix


| Service               | Location    | Bỏ được nếu chỉ demo website? | Giữ để scale dễ? | Lý do                                           |
| --------------------- | ----------- | ----------------------------- | ---------------- | ----------------------------------------------- |
| Cloud SQL PostgreSQL  | **GCP**     | **Không**                     | Bắt buộc         | Lưu mọi thứ: predictions, alerts, metrics, logs |
| GCS buckets           | **GCP**     | **Không**                     | Bắt buộc         | Raw/processed data, model artifacts             |
| `kafka`               | Local       | Có, nếu bỏ streaming          | **Nên giữ**      | Streaming là core feature, Kafka là standard    |
| `zookeeper`           | Local       | Có, nếu bỏ Kafka              | Đi theo Kafka    | Kafka dependency                                |
| `mlflow`              | Local       | Có, dùng file-based tracking  | Nên giữ          | Model versioning + metrics UI                   |
| `api`                 | Local       | **Không**                     | Bắt buộc         | Backend cho mọi thứ                             |
| `dashboard`           | Local       | **Không**                     | Bắt buộc         | UI cho demo                                     |
| `spark-master/worker` | Local       | **Có**                        | Nên giữ          | ETL demo, Big Data proof                        |
| `grafana`             | Local       | **Có**                        | Nice-to-have     | Operational metrics                             |
| `prometheus`          | Local       | **Có**                        | Nice-to-have     | Metrics collection                              |


### 6.4 Minimal Demo Stack (3 containers only)

Nếu máy local yếu (8GB RAM), chỉ cần:

```bash
docker-compose --profile minimal up -d
# Starts: api + dashboard (2 containers only)
# Database: Cloud SQL on GCP (no local postgres needed)
# Storage: GCS on GCP (no local storage needed)
# Everything else is mocked or file-based
```

---

## 7. Cấu trúc thư mục Project

```
bda501-final/
│
├── docker-compose.yml              # Main compose file
├── .env.example                    # Environment template
├── .env                            # Local env (gitignored)
├── Makefile                        # Convenience commands
├── README.md                       # Project overview
├── PLATFORM_DESIGN.md              # This document
│
├── api/                            # FastAPI backend service
│   ├── Dockerfile
│   ├── requirements.txt
│   ├── main.py                     # App entrypoint, lifespan, router registration
│   ├── config.py                   # Settings via pydantic-settings
│   ├── models.py                   # Pydantic request/response schemas
│   ├── database.py                 # PostgreSQL connection pool
│   ├── routers/
│   │   ├── health.py               # GET /health
│   │   ├── predict.py              # POST /predict/address, /predict/batch
│   │   ├── model.py                # GET /model/info, /model/metrics
│   │   ├── predictions.py          # GET /predictions/history
│   │   ├── alerts.py               # GET /alerts
│   │   ├── ingest.py               # POST /ingest/mock-transactions
│   │   └── dashboard.py            # GET /dashboard/summary
│   ├── services/
│   │   ├── predictor.py            # Abstract predictor + mock + real
│   │   ├── model_loader.py         # Load model from artifacts folder
│   │   └── prediction_logger.py    # Log predictions to PostgreSQL
│   └── middleware/
│       ├── correlation.py          # Request correlation ID
│       └── logging.py              # Structured request logging
│
├── dashboard/                      # Streamlit frontend
│   ├── Dockerfile
│   ├── requirements.txt
│   ├── app.py                      # Main Streamlit app
│   ├── pages/
│   │   ├── 1_Address_Lookup.py
│   │   ├── 2_Model_Info.py
│   │   ├── 3_Metrics.py
│   │   ├── 4_Alerts.py
│   │   ├── 5_History.py
│   │   └── 6_Admin.py
│   └── components/
│       ├── prediction_card.py      # Reusable prediction result widget
│       └── metric_display.py       # Reusable metric display widget
│
├── model_artifacts/                # Model files (bind mounted into api)
│   ├── current/                    # ← Active model symlink/folder
│   │   ├── model.pt                # PLACEHOLDER — replace with real model
│   │   └── metadata.json           # Model metadata (version, schema, threshold)
│   ├── v1/                         # Version folders (future)
│   │   ├── model.pt
│   │   └── metadata.json
│   └── mock/                       # Mock model for development
│       └── metadata.json           # Mock metadata
│
├── model_runtime/                  # Model inference logic
│   ├── __init__.py
│   ├── base_predictor.py           # Abstract base class
│   ├── mock_predictor.py           # Mock implementation
│   ├── gnn_predictor.py            # Real GNN predictor (PLACEHOLDER)
│   └── model_manager.py            # Version management, hot-swap
│
├── etl/                            # ETL pipeline jobs
│   ├── Dockerfile                  # PySpark image
│   ├── requirements.txt
│   ├── jobs/
│   │   ├── ingest_raw.py           # Read raw → validate → store
│   │   ├── process_features.py     # Feature engineering
│   │   └── export_predictions.py   # Batch inference results export
│   ├── schemas/
│   │   ├── transaction_schema.py   # PySpark schema definitions
│   │   └── feature_schema.py
│   └── utils/
│       ├── spark_session.py        # Spark session factory
│       └── storage.py              # GCS read/write helpers
│
├── streaming/                      # Kafka streaming pipeline
│   ├── mock_producer.py            # Simulated transaction generator
│   ├── consumer.py                 # Transaction consumer + inference
│   ├── alert_processor.py          # Alert generation logic
│   └── schemas.py                  # Avro/JSON event schemas
│
├── mlflow/                         # MLflow configuration
│   ├── Dockerfile                  # MLflow server image
│   └── mlflow.env                  # MLflow-specific env vars
│
├── credentials/                    # GCP credentials (gitignored!)
│   └── gcp-service-account.json    # Service account key (NEVER commit)
│
├── infra/                          # Infrastructure configs
│   ├── gcp/
│   │   ├── setup-cloud-sql.sh      # Cloud SQL instance creation
│   │   ├── setup-gcs-buckets.sh    # GCS bucket creation
│   │   └── setup-service-account.sh # IAM + SA setup
│   ├── postgres/
│   │   └── init.sql                # DB initialization script (run against Cloud SQL)
│   ├── kafka/
│   │   └── create-topics.sh        # Topic creation script
│   ├── grafana/
│   │   ├── provisioning/           # Auto-provisioned datasources + dashboards
│   │   │   ├── datasources/
│   │   │   │   └── postgres.yml
│   │   │   └── dashboards/
│   │   │       └── dashboard.yml
│   │   └── dashboards/
│   │       └── platform.json       # Operational dashboard JSON
│   ├── prometheus/
│   │   └── prometheus.yml          # Scrape config
│   └── nginx/
│       └── nginx.conf              # Reverse proxy config
│
├── configs/                        # Application configuration
│   ├── config.yaml                 # Main config (committed)
│   └── config.local.yaml           # Local overrides (gitignored)
│
├── scripts/                        # Utility scripts
│   ├── seed_data.py                # Seed mock data into PostgreSQL
│   ├── seed_metrics.py             # Seed mock model metrics
│   ├── check_health.sh             # Verify all services are up
│   ├── reset_db.sh                 # Drop and recreate all tables
│   └── swap_model.sh               # Swap model version script
│
├── tests/                          # Test suite
│   ├── test_api.py                 # API endpoint tests
│   ├── test_predictor.py           # Predictor unit tests
│   ├── test_model_loader.py        # Model loading tests
│   ├── test_streaming.py           # Kafka consumer tests
│   └── conftest.py                 # pytest fixtures
│
├── data/                           # Local data (gitignored except samples)
│   ├── sample/
│   │   ├── transactions_sample.csv # 100 sample transactions
│   │   ├── addresses_sample.csv    # 50 sample addresses with labels
│   │   └── phishing_labels.csv     # Known phishing address list
│   └── raw/                        # Raw ingested data (gitignored)
│
├── logs/                           # Application logs (gitignored)
│   ├── api/
│   ├── etl/
│   └── streaming/
│
└── notebooks/                      # Jupyter notebooks (Phase 1–3)
    ├── 01_data_exploration.ipynb    # PLACEHOLDER
    ├── 02_feature_engineering.ipynb # PLACEHOLDER
    └── 03_model_training.ipynb     # PLACEHOLDER
```

### Thư mục chi tiết


| Thư mục            | Mục đích                | Files chờ model           | Ghi chú                                |
| ------------------ | ----------------------- | ------------------------- | -------------------------------------- |
| `api/`             | FastAPI backend code    | Không                     | Build ngay                             |
| `dashboard/`       | Streamlit frontend      | Không                     | Build ngay                             |
| `model_artifacts/` | Chứa model files        | `current/model.pt`        | **PLACEHOLDER** — thay bằng model thật |
| `model_runtime/`   | Inference logic         | `gnn_predictor.py`        | Skeleton có sẵn, fill khi model xong   |
| `etl/`             | Spark ETL jobs          | Feature engineering logic | Skeleton sẵn                           |
| `streaming/`       | Kafka producer/consumer | Không                     | Build ngay với mock                    |
| `mlflow/`          | MLflow server config    | Không                     | Build ngay                             |
| `infra/`           | Docker/infra configs    | Không                     | Build ngay                             |
| `configs/`         | App configuration       | Threshold tuning          | Build ngay                             |
| `scripts/`         | DevOps utilities        | Không                     | Build ngay                             |
| `tests/`           | Test suite              | Predictor tests           | Build ngay                             |
| `data/sample/`     | Sample data             | Không                     | Build ngay                             |
| `notebooks/`       | Jupyter notebooks       | All 3 notebooks           | **PLACEHOLDER** — Phase 1–3            |


---

## 8. Model Integration Contract

### 8.1 Artifact Directory Structure

```
model_artifacts/
├── current/                    # ← API always loads from here
│   ├── model.pt                # PyTorch model weights
│   └── metadata.json           # Model metadata (REQUIRED)
├── v1/                         # Historical versions
│   ├── model.pt
│   └── metadata.json
├── v2/
│   ├── model.pt
│   └── metadata.json
└── mock/                       # Development mock
    └── metadata.json
```

### 8.2 Metadata Schema (`metadata.json`)

```json
{
  "model_id": "graphsage-phishing-v1",
  "model_version": "1.0.0",
  "model_type": "GraphSAGE",
  "created_at": "2026-04-09T10:00:00Z",
  "created_by": "ml-team",

  "input_schema": {
    "type": "address_features",
    "feature_count": 12,
    "feature_names": [
      "in_degree", "out_degree", "total_eth_received", "total_eth_sent",
      "avg_tx_value_in", "avg_tx_value_out", "max_tx_value",
      "unique_in_neighbors", "unique_out_neighbors", "account_lifetime",
      "failed_tx_ratio", "avg_gas_used"
    ],
    "feature_dtypes": "float32",
    "normalization": {
      "method": "standard_scaler",
      "mean": [10.5, 8.2, 1.3, 0.9, 0.15, 0.12, 0.8, 5.1, 3.2, 86400, 0.02, 21000],
      "scale": [25.1, 18.7, 5.6, 3.1, 0.45, 0.38, 2.1, 12.3, 8.7, 172800, 0.08, 15000]
    }
  },

  "output_schema": {
    "type": "binary_classification",
    "classes": ["legitimate", "phishing"],
    "output_dim": 2,
    "output_format": "softmax_probabilities"
  },

  "inference_config": {
    "default_threshold": 0.7,
    "batch_size": 256,
    "device": "cpu",
    "requires_graph": true,
    "num_hops": 2,
    "neighbor_samples": [15, 10]
  },

  "training_info": {
    "dataset": "xblock-eth",
    "dataset_version": "2026-04",
    "train_samples": 1800000,
    "val_samples": 600000,
    "test_samples": 570000,
    "epochs": 50,
    "optimizer": "AdamW",
    "learning_rate": 0.001,
    "class_weights": [1.0, 100.0]
  },

  "metrics": {
    "test_precision": 0.912,
    "test_recall": 0.867,
    "test_f1": 0.889,
    "test_roc_auc": 0.945,
    "test_pr_auc": 0.823,
    "test_accuracy": 0.987,
    "threshold_used": 0.7,
    "confusion_matrix": {
      "true_positive": 1010,
      "false_positive": 97,
      "true_negative": 568000,
      "false_negative": 153
    }
  },

  "compatibility": {
    "python_version": ">=3.10",
    "torch_version": ">=2.0",
    "dgl_version": ">=1.1",
    "api_version": ">=1.0.0"
  },

  "graph_data": {
    "node_features_file": "node_features.npy",
    "edge_index_file": "edge_index.npy",
    "node_mapping_file": "node_to_id.pkl",
    "labels_file": "labels.npy",
    "total_nodes": 2970000,
    "total_edges": 13550000
  }
}
```

### 8.3 Input Schema (Inference Request)

```python
# Single address prediction
{
    "address": "0x742d35cc6634c0532925a3b844bc9e7595f2bd18",
    "include_features": false,      # optional: return computed features
    "include_risk_factors": true,   # optional: return risk explanation
    "threshold_override": null      # optional: custom threshold
}

# Batch prediction
{
    "addresses": [
        "0x742d35cc6634c0532925a3b844bc9e7595f2bd18",
        "0xdac17f958d2ee523a2206206994597c13d831ec7"
    ],
    "threshold_override": null
}
```

### 8.4 Output Schema (Inference Response)

```python
# Single address response
{
    "address": "0x742d35cc6634c0532925a3b844bc9e7595f2bd18",
    "prediction": "phishing",          # "phishing" | "legitimate"
    "phishing_probability": 0.847,     # 0.0 – 1.0
    "confidence": "high",             # "high" | "medium" | "low"
    "threshold_used": 0.7,
    "model_version": "1.0.0",
    "model_type": "GraphSAGE",
    "inference_mode": "real",          # "real" | "mock"
    "inference_time_ms": 23.4,
    "is_known_address": true,          # found in training graph
    "risk_factors": [
        "Very new account (< 7 days old)",
        "High fan-in pattern (receives from many unique addresses)"
    ],
    "timestamp": "2026-04-09T10:30:00Z"
}
```

### 8.5 Threshold Configuration

```yaml
# configs/config.yaml
model:
  default_threshold: 0.7
  confidence_bands:
    high: 0.8       # score >= 0.8 → high confidence phishing
    medium: 0.5      # 0.5 <= score < 0.8 → medium
    low: 0.0         # score < 0.5 → low (likely legitimate)
  auto_alert_threshold: 0.85  # auto-generate alert above this
```

### 8.6 Fallback When No Model

```python
# When model_artifacts/current/model.pt does not exist:
# 1. API starts in MOCK MODE
# 2. MockPredictor returns deterministic scores based on address hash
# 3. All responses include "inference_mode": "mock"
# 4. Dashboard shows a banner: "Running in mock mode — no model loaded"
# 5. All other features work normally (predictions are logged, alerts generated)
```

### 8.7 Model Loading at Startup

```
API Startup Sequence:
1. Read MODEL_ARTIFACTS_DIR env var (default: /app/model_artifacts)
2. If GCS_SYNC enabled:
   a. Check GCS bucket for models/{current}/metadata.json
   b. Download model artifacts from GCS → local cache
3. Check if {MODEL_ARTIFACTS_DIR}/current/metadata.json exists (local or synced)
4. If YES:
   a. Validate metadata.json schema
   b. Check compatibility (Python version, torch version, API version)
   c. Load model.pt into memory
   d. Set inference_mode = "real"
   e. Log: "Model loaded: v1.0.0, GraphSAGE, threshold=0.7"
5. If NO:
   a. Set inference_mode = "mock"
   b. Initialize MockPredictor
   c. Log: "WARNING: No model found. Running in mock mode."
6. Register model metadata in Cloud SQL model_versions table
7. API ready to serve
```

### 8.8 Hot-swap Model Version

```
Option A — GCS upload (recommended):
1. Upload new model files to GCS: gs://eth-phishing-models/models/v2/
2. Update GCS "current" pointer (copy v2 → current/)
3. Call POST /admin/reload-model
4. API syncs from GCS and reloads model without restart

Option B — Local file + API restart:
1. Copy new model files to model_artifacts/current/
2. docker-compose restart api
3. API picks up new model on startup

Option C — Automated (MLflow + GCS):
1. Register new model version in MLflow
2. Mark as "Production" stage
3. API polls MLflow every MODEL_REFRESH_INTERVAL_HOURS
4. Auto-downloads from GCS and loads new production model
```

### 8.9 Compatibility Validation

```python
def validate_model_compatibility(metadata: dict) -> list[str]:
    errors = []

    # Check feature count matches API expectation
    if metadata["input_schema"]["feature_count"] != EXPECTED_FEATURES:
        errors.append(f"Feature count mismatch: model expects "
                      f"{metadata['input_schema']['feature_count']}, "
                      f"API expects {EXPECTED_FEATURES}")

    # Check torch version
    import torch
    model_torch = metadata["compatibility"]["torch_version"]
    if not version_satisfies(torch.__version__, model_torch):
        errors.append(f"Torch version mismatch: {torch.__version__} "
                      f"vs required {model_torch}")

    # Check API version
    model_api = metadata["compatibility"]["api_version"]
    if not version_satisfies(API_VERSION, model_api):
        errors.append(f"API version mismatch: {API_VERSION} "
                      f"vs required {model_api}")

    return errors  # empty list = compatible
```

---

## 9. API Design

### 9.1 Base URL

```
Local:      http://localhost:8000
Container:  http://api:8000
```

### 9.2 Endpoints

---

#### `GET /health`


| Field          | Value                                   |
| -------------- | --------------------------------------- |
| **Purpose**    | Service health check, dependency status |
| **Auth**       | None                                    |
| **Rate limit** | None                                    |


**Response 200:**

```json
{
  "status": "healthy",
  "version": "1.0.0",
  "inference_mode": "mock",
  "model_loaded": false,
  "database": "connected",
  "kafka": "connected",
  "gcs": "connected",
  "uptime_seconds": 3421,
  "timestamp": "2026-04-09T10:30:00Z"
}
```

**Status codes:** `200 OK`, `503 Service Unavailable` (when critical dependency down)

**Data source:** Live checks against Cloud SQL PostgreSQL, Kafka, GCS

---

#### `GET /model/info`


| Field          | Value                                   |
| -------------- | --------------------------------------- |
| **Purpose**    | Current model metadata, version, config |
| **Auth**       | None                                    |
| **Rate limit** | None                                    |


**Response 200:**

```json
{
  "model_id": "graphsage-phishing-v1",
  "model_version": "1.0.0",
  "model_type": "GraphSAGE",
  "inference_mode": "real",
  "threshold": 0.7,
  "loaded_at": "2026-04-09T08:00:00Z",
  "feature_count": 12,
  "graph_nodes": 2970000,
  "graph_edges": 13550000,
  "training_dataset": "xblock-eth",
  "compatibility_status": "ok"
}
```

**Empty state:** Returns `{"inference_mode": "mock", "model_version": "mock-0.0.0", ...}`

**Data source:** In-memory model metadata + `model_artifacts/current/metadata.json`

---

#### `GET /model/metrics`


| Field          | Value                                    |
| -------------- | ---------------------------------------- |
| **Purpose**    | Model evaluation metrics (from training) |
| **Auth**       | None                                     |
| **Rate limit** | None                                     |


**Response 200:**

```json
{
  "model_version": "1.0.0",
  "metrics": {
    "test_precision": 0.912,
    "test_recall": 0.867,
    "test_f1": 0.889,
    "test_roc_auc": 0.945,
    "test_pr_auc": 0.823,
    "test_accuracy": 0.987
  },
  "confusion_matrix": {
    "true_positive": 1010,
    "false_positive": 97,
    "true_negative": 568000,
    "false_negative": 153
  },
  "threshold_used": 0.7,
  "source": "metadata.json"
}
```

**Empty state:** Returns seeded mock metrics when no real model is loaded

**Data source:** `model_artifacts/current/metadata.json` → `metrics` field, or `model_metrics` table

---

#### `POST /predict/address`


| Field          | Value                              |
| -------------- | ---------------------------------- |
| **Purpose**    | Classify a single Ethereum address |
| **Auth**       | API key (Phase 8)                  |
| **Rate limit** | 100 req/min (Phase 8)              |


**Request body:**

```json
{
  "address": "0x742d35cc6634c0532925a3b844bc9e7595f2bd18",
  "include_risk_factors": true,
  "threshold_override": null
}
```

**Validation:**

- `address` must match `^0x[a-fA-F0-9]{40}$`
- `threshold_override` must be `null` or `0.0–1.0`

**Response 200:**

```json
{
  "address": "0x742d35cc6634c0532925a3b844bc9e7595f2bd18",
  "prediction": "phishing",
  "phishing_probability": 0.847,
  "confidence": "high",
  "threshold_used": 0.7,
  "model_version": "1.0.0",
  "inference_mode": "real",
  "inference_time_ms": 23.4,
  "is_known_address": true,
  "risk_factors": [
    "Very new account (< 7 days old)",
    "High fan-in pattern"
  ],
  "prediction_id": "pred_abc123",
  "timestamp": "2026-04-09T10:30:00Z"
}
```

**Status codes:** `200 OK`, `400 Bad Request` (invalid address), `503 Model Unavailable`

**Common errors:**

- `400`: `"Invalid Ethereum address format"`
- `503`: `"Model service temporarily unavailable"`

**Data source:** Model inference → log to `predictions` table

---

#### `POST /predict/batch`


| Field          | Value                                   |
| -------------- | --------------------------------------- |
| **Purpose**    | Classify multiple addresses in one call |
| **Auth**       | API key (Phase 8)                       |
| **Rate limit** | 20 req/min, max 100 addresses per batch |


**Request body:**

```json
{
  "addresses": [
    "0x742d35cc6634c0532925a3b844bc9e7595f2bd18",
    "0xdac17f958d2ee523a2206206994597c13d831ec7"
  ],
  "threshold_override": null
}
```

**Validation:**

- `addresses` array: min 1, max 100 items
- Each address must match Ethereum format

**Response 200:**

```json
{
  "results": [
    {
      "address": "0x742d35cc6634c0532925a3b844bc9e7595f2bd18",
      "prediction": "phishing",
      "phishing_probability": 0.847,
      "confidence": "high"
    },
    {
      "address": "0xdac17f958d2ee523a2206206994597c13d831ec7",
      "prediction": "legitimate",
      "phishing_probability": 0.023,
      "confidence": "high"
    }
  ],
  "total": 2,
  "phishing_count": 1,
  "model_version": "1.0.0",
  "inference_mode": "real",
  "total_inference_time_ms": 45.2,
  "batch_id": "batch_xyz789",
  "timestamp": "2026-04-09T10:30:00Z"
}
```

**Status codes:** `200`, `400` (invalid addresses or too many), `503`

**Data source:** Model batch inference → log all to `predictions` table

---

#### `GET /predictions/history`


| Field          | Value                            |
| -------------- | -------------------------------- |
| **Purpose**    | Retrieve past prediction results |
| **Auth**       | None                             |
| **Rate limit** | 50 req/min                       |


**Query parameters:**

- `address` (optional) — filter by specific address
- `prediction` (optional) — `"phishing"` or `"legitimate"`
- `min_score` (optional, float) — minimum phishing probability
- `limit` (int, default 50, max 500)
- `offset` (int, default 0)
- `sort` (string, default `"-created_at"`) — field to sort by

**Response 200:**

```json
{
  "predictions": [
    {
      "prediction_id": "pred_abc123",
      "address": "0x742d...",
      "prediction": "phishing",
      "phishing_probability": 0.847,
      "model_version": "1.0.0",
      "inference_mode": "real",
      "source": "api",
      "created_at": "2026-04-09T10:30:00Z"
    }
  ],
  "total": 1247,
  "limit": 50,
  "offset": 0
}
```

**Data source:** `predictions` table in PostgreSQL

---

#### `GET /alerts`


| Field          | Value                          |
| -------------- | ------------------------------ |
| **Purpose**    | List high-risk phishing alerts |
| **Auth**       | None                           |
| **Rate limit** | 50 req/min                     |


**Query parameters:**

- `since` (ISO timestamp, optional)
- `min_score` (float, default 0.7)
- `reviewed` (bool, optional)
- `limit` (int, default 50, max 500)

**Response 200:**

```json
{
  "alerts": [
    {
      "alert_id": 42,
      "address": "0xabc...",
      "phishing_score": 0.94,
      "trigger": "streaming",
      "tx_hash": "0xdef...",
      "model_version": "1.0.0",
      "created_at": "2026-04-09T10:25:00Z",
      "reviewed": false,
      "analyst_label": null
    }
  ],
  "total": 23,
  "unreviewed_count": 18
}
```

**Data source:** `alerts` table in PostgreSQL

---

#### `POST /ingest/mock-transactions`


| Field          | Value                                                  |
| -------------- | ------------------------------------------------------ |
| **Purpose**    | Generate and ingest simulated transactions for testing |
| **Auth**       | None                                                   |
| **Rate limit** | 10 req/min                                             |


**Request body:**

```json
{
  "count": 100,
  "phishing_ratio": 0.05,
  "publish_to_kafka": true
}
```

**Response 200:**

```json
{
  "ingested": 100,
  "phishing_simulated": 5,
  "kafka_published": true,
  "job_id": "ingest_job_456"
}
```

**Data source:** Random generation → Kafka topic / PostgreSQL

---

#### `GET /dashboard/summary`


| Field          | Value                                    |
| -------------- | ---------------------------------------- |
| **Purpose**    | Aggregated stats for dashboard home page |
| **Auth**       | None                                     |
| **Rate limit** | 100 req/min                              |


**Response 200:**

```json
{
  "total_predictions": 12450,
  "total_phishing_detected": 342,
  "total_alerts": 89,
  "alerts_last_24h": 7,
  "predictions_last_24h": 156,
  "model_version": "1.0.0",
  "inference_mode": "real",
  "avg_inference_time_ms": 18.3,
  "top_risky_addresses": [
    {"address": "0xabc...", "max_score": 0.97, "detection_count": 3}
  ],
  "daily_trend": [
    {"date": "2026-04-08", "predictions": 120, "phishing": 8},
    {"date": "2026-04-09", "predictions": 156, "phishing": 12}
  ]
}
```

**Data source:** Aggregation queries against `predictions`, `alerts` tables

---

## 10. Database Schema

### 10.1 Entity-Relationship Overview

```
┌──────────────┐     ┌──────────────┐     ┌──────────────────┐
│  addresses   │     │  predictions │     │  model_versions  │
│──────────────│     │──────────────│     │──────────────────│
│ PK address   │◄────│ FK address   │     │ PK id            │
│    label     │     │ FK model_ver │────▶│    version        │
│    source    │     │    score     │     │    is_active      │
│    ...       │     │    ...       │     │    ...            │
└──────────────┘     └──────────────┘     └──────────────────┘
                          │
                     ┌────▼──────────┐     ┌──────────────────┐
                     │    alerts     │     │  model_metrics   │
                     │──────────────│     │──────────────────│
                     │ PK id        │     │ PK id            │
                     │ FK address   │     │ FK model_ver     │
                     │    score     │     │    metric_name   │
                     │    ...       │     │    metric_value   │
                     └──────────────┘     └──────────────────┘

┌──────────────┐     ┌──────────────┐     ┌──────────────────┐
│ingestion_jobs│     │   etl_jobs   │     │api_requests_log  │
│──────────────│     │──────────────│     │──────────────────│
│ PK id        │     │ PK id        │     │ PK id            │
│    status    │     │    status    │     │    endpoint      │
│    ...       │     │    ...       │     │    ...           │
└──────────────┘     └──────────────┘     └──────────────────┘
```

### 10.2 Table Definitions

```sql
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
CREATE INDEX idx_addresses_label ON addresses(label);
CREATE INDEX idx_addresses_updated ON addresses(updated_at);

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
CREATE INDEX idx_predictions_address ON predictions(address);
CREATE INDEX idx_predictions_score ON predictions(phishing_score DESC);
CREATE INDEX idx_predictions_created ON predictions(created_at DESC);
CREATE INDEX idx_predictions_source ON predictions(source);
CREATE INDEX idx_predictions_model ON predictions(model_version_id);

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
CREATE UNIQUE INDEX idx_model_active ON model_versions(is_active)
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
CREATE INDEX idx_metrics_model ON model_metrics(model_version_id);

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
CREATE INDEX idx_alerts_address ON alerts(address);
CREATE INDEX idx_alerts_score ON alerts(phishing_score DESC);
CREATE INDEX idx_alerts_created ON alerts(created_at DESC);
CREATE INDEX idx_alerts_unreviewed ON alerts(reviewed) WHERE reviewed = FALSE;

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
CREATE INDEX idx_ingest_status ON ingestion_jobs(status);

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
CREATE INDEX idx_etl_status ON etl_jobs(status);
CREATE INDEX idx_etl_name ON etl_jobs(job_name);

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
CREATE INDEX idx_api_log_endpoint ON api_requests_log(endpoint);
CREATE INDEX idx_api_log_created ON api_requests_log(created_at DESC);
CREATE INDEX idx_api_log_correlation ON api_requests_log(correlation_id);

-- Partition by month for large-scale deployments (optional)
-- CREATE TABLE api_requests_log_2026_04 PARTITION OF api_requests_log
--     FOR VALUES FROM ('2026-04-01') TO ('2026-05-01');

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
CREATE INDEX idx_txraw_from ON transactions_raw(from_address);
CREATE INDEX idx_txraw_to ON transactions_raw(to_address);
CREATE INDEX idx_txraw_block ON transactions_raw(block_number);

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
CREATE INDEX idx_features_version ON feature_snapshots(snapshot_version);

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
```

### 10.3 Table Classification


| Table                    | Phục vụ Website?      | Phục vụ Observability? | Optional?                    |
| ------------------------ | --------------------- | ---------------------- | ---------------------------- |
| `addresses`              | Yes (lookup)          | No                     | **Required**                 |
| `predictions`            | Yes (history, result) | Yes (audit)            | **Required**                 |
| `model_versions`         | Yes (model info)      | Yes (tracking)         | **Required**                 |
| `model_metrics`          | Yes (metrics page)    | No                     | **Required**                 |
| `alerts`                 | Yes (alerts page)     | Yes (audit)            | **Required**                 |
| `ingestion_jobs`         | No                    | Yes                    | Optional                     |
| `etl_jobs`               | No                    | Yes                    | Optional                     |
| `api_requests_log`       | No                    | Yes                    | Optional                     |
| `transactions_raw`       | No                    | No                     | Optional (use GCS instead)   |
| `transactions_processed` | No                    | No                     | Optional                     |
| `feature_snapshots`      | No                    | No                     | Optional                     |


---

## 11. ETL / Processing Design

### 11.1 Pipeline Overview

```
Data Source                   Validate          Normalize         Enrich
────────────              ──────────────    ──────────────    ──────────────
CSV/JSON file     ───▶    • Schema check    • Lowercase       • Compute node
Kafka topic       ───▶    • Type coercion     addresses         features
Mock generator    ───▶    • Null handling   • Wei → ETH       • Join labels
                          • Dedup by        • Timestamp        • PageRank
                            tx_hash           parse            • Degree stats
                              │                  │                  │
                              ▼                  ▼                  ▼
                          Store Raw          Store Processed   Feature Store
                          ──────────────    ──────────────    ──────────────
                          • GCS bucket      • GCS bucket      • Cloud SQL
                            gs://eth-        gs://eth-           PostgreSQL
                            phishing/        phishing/           feature_snapshots
                            raw/             processed/        • Format: JSON
                          • Format:         • Format:
                            Parquet           Parquet
                          • Partition:      • Partition:
                            by block_num      by date
```

### 11.2 Spark Job Structure

```python
# etl/jobs/ingest_raw.py (skeleton)

class IngestRawJob:
    """Read raw transactions, validate, store to GCS."""

    def __init__(self, spark, config):
        self.spark = spark
        self.config = config
        self.job_id = None

    def run(self, source_path: str, source_type: str = "csv"):
        # 1. Register job in Cloud SQL PostgreSQL
        self.job_id = self._register_job()

        try:
            # 2. Read source data
            raw_df = self._read_source(source_path, source_type)

            # 3. Validate schema
            validated_df = self._validate(raw_df)

            # 4. Deduplicate
            deduped_df = validated_df.dropDuplicates(["tx_hash"])

            # 5. Store to GCS as Parquet
            partition_col = "block_number_partition"
            deduped_df = deduped_df.withColumn(
                partition_col,
                F.floor(F.col("block_number") / 1000000)
            )
            deduped_df.write \
                .mode("append") \
                .partitionBy(partition_col) \
                .parquet("gs://eth-phishing-raw/transactions/")

            # 6. Update job status
            self._complete_job(deduped_df.count())

        except Exception as e:
            self._fail_job(str(e))
            raise
```

### 11.3 File Formats


| Stage         | Format            | Compression | Partition Key       | Lý do                            |
| ------------- | ----------------- | ----------- | ------------------- | -------------------------------- |
| Raw ingestion | Parquet           | Snappy      | `block_number / 1M` | Columnar, compressed, splittable |
| Processed     | Parquet           | Snappy      | `date`              | Time-based access pattern        |
| Features      | JSON (PostgreSQL) | N/A         | by address          | Fast random lookup               |
| Model input   | NumPy (.npy)      | None        | N/A                 | Direct torch tensor conversion   |


### 11.4 Idempotency & Retry

```
IDEMPOTENCY STRATEGY:
1. Every job has a unique job_id
2. Raw data is deduped by tx_hash before write
3. Parquet writes use "append" mode with partition pruning
4. Failed jobs can be safely re-run — duplicates are caught
5. Each ETL job logs to etl_jobs table with status tracking

RETRY STRATEGY:
1. Spark job failures: auto-retry up to 3 times (Spark config)
2. Kafka consumer: commit offset only after successful processing
3. Database writes: wrapped in transaction with rollback on error
4. GCS uploads: retry with exponential backoff (3 attempts)
```

### 11.5 Local Dev Simplification

```
FOR LOCAL DEVELOPMENT:
- Skip Spark entirely for small datasets (< 1M rows)
- Use pandas for ETL on sample data
- Read from local CSV or download from GCS
- Write processed data directly to Cloud SQL PostgreSQL
- Use scripts/seed_data.py to populate tables with sample data

WHEN TO USE SPARK:
- Dataset > 1M rows
- Need to demonstrate Big Data processing for the project
- ETL involves complex joins or graph analytics
- Spark reads/writes directly to GCS via gs:// connector
```

---

## 12. Mock Mode

### 12.1 Why Mock Mode is Critical


| Reason                   | Explanation                                                           |
| ------------------------ | --------------------------------------------------------------------- |
| **Parallel development** | Frontend, backend, streaming can all be built while model is training |
| **Demo readiness**       | Show the full system working before model is done                     |
| **Testing**              | Integration tests don't depend on a 50MB model file                   |
| **Onboarding**           | New team members can run the platform immediately                     |
| **CI/CD**                | Automated tests run without model artifacts                           |


### 12.2 Mock Predictor Implementation

```python
# model_runtime/mock_predictor.py

import hashlib
from .base_predictor import BasePredictor

class MockPredictor(BasePredictor):
    """
    Deterministic mock predictor.
    Same address always produces same score (based on address hash).
    Known phishing addresses from seed data return high scores.
    """

    KNOWN_PHISHING = {
        "0x0000000000000000000000000000000000000bad": 0.95,
        "0xdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef": 0.88,
    }

    def predict(self, address: str) -> dict:
        address = address.lower()

        # Known phishing addresses
        if address in self.KNOWN_PHISHING:
            score = self.KNOWN_PHISHING[address]
        else:
            # Deterministic score from address hash
            hash_val = int(hashlib.sha256(address.encode()).hexdigest(), 16)
            score = (hash_val % 1000) / 1000.0
            # Skew distribution: most addresses are legitimate
            score = score * 0.4  # max mock score for unknown = 0.4

        return {
            "address": address,
            "phishing_probability": round(score, 4),
            "prediction": "phishing" if score >= 0.7 else "legitimate",
            "confidence": self._confidence(score),
            "inference_mode": "mock",
            "model_version": "mock-0.0.0",
            "inference_time_ms": 0.5,
            "is_known_address": address in self.KNOWN_PHISHING,
            "risk_factors": self._mock_risk_factors(score),
        }

    def predict_batch(self, addresses: list[str]) -> list[dict]:
        return [self.predict(addr) for addr in addresses]

    def _confidence(self, score: float) -> str:
        if score >= 0.8 or score <= 0.2:
            return "high"
        if score >= 0.5 or score <= 0.4:
            return "medium"
        return "low"

    def _mock_risk_factors(self, score: float) -> list[str]:
        factors = []
        if score > 0.8:
            factors.append("Pattern matches known phishing behavior (mock)")
        if score > 0.6:
            factors.append("Unusual transaction pattern detected (mock)")
        return factors
```

### 12.3 Mock Metrics

```python
# scripts/seed_metrics.py

MOCK_METRICS = {
    "test_precision": 0.912,
    "test_recall": 0.867,
    "test_f1": 0.889,
    "test_roc_auc": 0.945,
    "test_pr_auc": 0.823,
    "test_accuracy": 0.987,
}

MOCK_CONFUSION_MATRIX = {
    "true_positive": 1010,
    "false_positive": 97,
    "true_negative": 568000,
    "false_negative": 153,
}
```

### 12.4 Seed Data

```python
# scripts/seed_data.py seeds:
# - 50 sample addresses (5 phishing, 45 legitimate) into addresses table
# - 200 sample predictions into predictions table
# - 10 sample alerts into alerts table
# - 1 mock model version into model_versions table
# - 6 mock metrics into model_metrics table
# - 100 sample transactions into transactions_raw table
```

### 12.5 Switching from Mock to Real

```
SWITCH PROCESS:
1. Copy model.pt + metadata.json to model_artifacts/current/
2. Set environment variable: INFERENCE_MODE=real (or auto-detect from file)
3. Restart API: docker-compose restart api
4. API detects model.pt → loads real predictor → inference_mode = "real"
5. Dashboard banner disappears
6. All new predictions use real model
7. Old mock predictions remain in history (tagged inference_mode=mock)

NO CODE CHANGES REQUIRED — only file copy + restart.
```

---

## 13. Website / Dashboard

### 13.1 Technology: Streamlit

Streamlit is chosen for rapid prototyping with Python-native development.

### 13.2 Screen Specifications

---

#### Screen 1: Home / Overview

```
┌─────────────────────────────────────────────────────────────┐
│  🛡️ Ethereum Phishing Detection Platform                    │
│                                                              │
│  ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────┐      │
│  │ 12,450   │ │ 342      │ │ 89       │ │ 18.3ms   │      │
│  │ Total    │ │ Phishing │ │ Alerts   │ │ Avg      │      │
│  │ Predict. │ │ Detected │ │ Total    │ │ Latency  │      │
│  └──────────┘ └──────────┘ └──────────┘ └──────────┘      │
│                                                              │
│  Model: GraphSAGE v1.0.0 | Mode: real | Threshold: 0.7     │
│  ⚠️ [MOCK MODE BANNER - shown only when no real model]      │
│                                                              │
│  ┌─────────────────────────────────┐                        │
│  │ Daily Detection Trend (7 days)  │                        │
│  │ ▓▓▓▓▓▓▓░░░ 120                 │                        │
│  │ ▓▓▓▓▓▓▓▓░░ 156                 │                        │
│  │ ▓▓▓▓▓▓░░░░ 98                  │                        │
│  └─────────────────────────────────┘                        │
│                                                              │
│  ┌─────────────────────────────────┐                        │
│  │ Top Risky Addresses             │                        │
│  │ 1. 0xabc... Score: 0.97        │                        │
│  │ 2. 0xdef... Score: 0.94        │                        │
│  │ 3. 0x123... Score: 0.91        │                        │
│  └─────────────────────────────────┘                        │
└─────────────────────────────────────────────────────────────┘
```

- **API called**: `GET /dashboard/summary`
- **Loading state**: Skeleton placeholders for metrics cards
- **Error state**: "Unable to connect to API" with retry button
- **Empty state**: "No predictions yet. Try the Address Lookup page."
- **Mock mode**: Yellow banner: "Running in mock mode — predictions are simulated"

---

#### Screen 2: Address Lookup

```
┌─────────────────────────────────────────────────────────────┐
│  🔍 Address Lookup                                           │
│                                                              │
│  ┌──────────────────────────────────────────────┐           │
│  │ Enter Ethereum Address                        │           │
│  │ 0x742d35cc6634c0532925a3b844bc9e7595f2bd18   │           │
│  └──────────────────────────────────────────────┘           │
│  [🔎 Classify Address]                                      │
│                                                              │
│  ── Result ──────────────────────────────────────            │
│                                                              │
│  🚨 PHISHING DETECTED                                       │
│                                                              │
│  Score: 0.847 (84.7%)          Confidence: HIGH              │
│  Model: GraphSAGE v1.0.0      Mode: real                    │
│  Inference time: 23.4ms        Known address: Yes            │
│                                                              │
│  ┌─────────────────────────────┐                            │
│  │ Risk Gauge                  │                            │
│  │ [████████████░░░░] 84.7%    │                            │
│  │ ───────────┬────            │                            │
│  │       threshold: 70%        │                            │
│  └─────────────────────────────┘                            │
│                                                              │
│  ⚠️ Risk Factors:                                            │
│  • Very new account (< 7 days old)                          │
│  • High fan-in pattern (receives from many addresses)       │
│                                                              │
│  ── Previous Predictions for this Address ──                 │
│  [Table of past predictions]                                │
└─────────────────────────────────────────────────────────────┘
```

- **API called**: `POST /predict/address`, then `GET /predictions/history?address=...`
- **Loading state**: Spinner with "Extracting subgraph and running inference..."
- **Error state**: Red box with error message (invalid address, API down)
- **Empty state**: "Enter an Ethereum address above to check for phishing"
- **Confidence display**: Color-coded — red (high phishing), yellow (medium), green (low)
- **Threshold display**: Dotted line on gauge showing the decision boundary

---

#### Screen 3: Model Info

```
┌─────────────────────────────────────────────────────────────┐
│  🤖 Model Information                                        │
│                                                              │
│  Model ID:        graphsage-phishing-v1                     │
│  Version:         1.0.0                                     │
│  Type:            GraphSAGE (2-layer, 128 hidden)           │
│  Status:          ✅ Active                                  │
│  Mode:            real                                       │
│  Loaded at:       2026-04-09 08:00:00 UTC                   │
│                                                              │
│  ── Graph Stats ──                                           │
│  Nodes:           2,970,000                                 │
│  Edges:           13,550,000                                │
│  Features:        12 dimensions                             │
│                                                              │
│  ── Configuration ──                                         │
│  Threshold:       0.7                                       │
│  Batch size:      256                                       │
│  Device:          cpu                                       │
│  Neighbor samples: [15, 10] (2-hop)                         │
│                                                              │
│  ── Training Info ──                                         │
│  Dataset:         xblock-eth                                │
│  Train samples:   1,800,000                                 │
│  Epochs:          50                                        │
│  Optimizer:       AdamW (lr=0.001)                          │
│                                                              │
│  ── Version History ──                                       │
│  [Table: version | deployed_at | status | f1_score]         │
└─────────────────────────────────────────────────────────────┘
```

- **API called**: `GET /model/info`
- **Empty state**: "No model loaded. System is running in mock mode."

---

#### Screen 4: Metrics Page

```
┌─────────────────────────────────────────────────────────────┐
│  📊 Model Metrics                                            │
│                                                              │
│  ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────┐      │
│  │ 0.912    │ │ 0.867    │ │ 0.889    │ │ 0.945    │      │
│  │Precision │ │ Recall   │ │ F1-Score │ │ ROC-AUC  │      │
│  └──────────┘ └──────────┘ └──────────┘ └──────────┘      │
│                                                              │
│  PR-AUC: 0.823     Accuracy: 0.987                          │
│                                                              │
│  ── Confusion Matrix ──                                      │
│  ┌────────────────────────────────┐                         │
│  │              Predicted         │                         │
│  │           Legit    Phishing    │                         │
│  │  Actual                       │                         │
│  │  Legit   568,000    97        │                         │
│  │  Phish     153    1,010       │                         │
│  └────────────────────────────────┘                         │
│                                                              │
│  False Positive Rate: 0.017%                                │
│  False Negative Rate: 13.2%                                 │
│                                                              │
│  ── Threshold Analysis ──                                    │
│  [Slider: adjust threshold, see impact on precision/recall] │
└─────────────────────────────────────────────────────────────┘
```

- **API called**: `GET /model/metrics`
- **Empty state**: Shows seeded mock metrics with disclaimer

---

#### Screen 5: Alerts Page

- **API called**: `GET /alerts`
- **Data**: Table of unreviewed alerts, sortable by score
- **Actions**: Mark as reviewed, confirm/reject label
- **Loading state**: "Loading alerts..."
- **Empty state**: "No active alerts."

---

#### Screen 6: Prediction History

- **API called**: `GET /predictions/history`
- **Data**: Paginated table of all predictions
- **Filters**: address, date range, prediction type, score range
- **Loading state**: Skeleton table
- **Empty state**: "No predictions recorded yet."

---

#### Screen 7: Admin / Config (optional)

- **Features**: Reload model, view system health, adjust threshold
- **API called**: `GET /health`, `POST /admin/reload-model`
- **Access**: Consider restricting in production

---

## 14. Streaming Prototype

### 14.1 Event Schema

```json
{
  "event_type": "transaction",
  "tx_hash": "0xabc123...",
  "from_address": "0x742d35cc6634c0532925a3b844bc9e7595f2bd18",
  "to_address": "0xdac17f958d2ee523a2206206994597c13d831ec7",
  "value_eth": 1.5,
  "gas": 21000,
  "gas_price": 30000000000,
  "block_number": 19500001,
  "block_timestamp": 1712678400,
  "source": "mock"
}
```

### 14.2 Kafka Topics


| Topic              | Purpose                             | Partitions | Retention |
| ------------------ | ----------------------------------- | ---------- | --------- |
| `eth.transactions` | New transaction events              | 3          | 7 days    |
| `eth.alerts`       | Phishing alerts (score > threshold) | 3          | 30 days   |
| `eth.dead-letter`  | Failed processing events            | 1          | 30 days   |


### 14.3 Mock Producer

```python
# streaming/mock_producer.py (skeleton)

import json, time, random, hashlib
from confluent_kafka import Producer

def generate_mock_transaction():
    """Generate a realistic-looking mock Ethereum transaction."""
    is_phishing = random.random() < 0.05  # 5% phishing rate

    if is_phishing:
        from_addr = random.choice(KNOWN_PHISHING_ADDRESSES)
    else:
        from_addr = "0x" + hashlib.sha256(
            str(random.random()).encode()
        ).hexdigest()[:40]

    return {
        "event_type": "transaction",
        "tx_hash": "0x" + hashlib.sha256(
            str(time.time()).encode()
        ).hexdigest(),
        "from_address": from_addr,
        "to_address": "0x" + hashlib.sha256(
            str(random.random()).encode()
        ).hexdigest()[:40],
        "value_eth": round(random.uniform(0.001, 10.0), 4),
        "gas": 21000,
        "gas_price": random.randint(10_000_000_000, 100_000_000_000),
        "block_number": 19500000 + int(time.time()) % 100000,
        "block_timestamp": int(time.time()),
        "source": "mock",
    }

def run_producer(rate_per_second=5):
    producer = Producer({"bootstrap.servers": "localhost:9092"})
    while True:
        for _ in range(rate_per_second):
            tx = generate_mock_transaction()
            producer.produce(
                "eth.transactions",
                key=tx["from_address"],
                value=json.dumps(tx),
            )
        producer.flush()
        time.sleep(1)
```

### 14.4 Consumer Flow

```
Consumer Loop:
1. Poll Kafka [eth.transactions] topic
2. Deserialize JSON event
3. Validate event schema
4. Extract from_address and to_address
5. Call predictor.predict(address)
6. If score > threshold:
   a. Create alert record
   b. Publish to [eth.alerts] topic
   c. Insert into alerts table
7. Log prediction to predictions table
8. Commit Kafka offset

ERROR HANDLING:
- Deserialization error → publish to [eth.dead-letter], skip
- Inference error → retry up to 3 times, then dead-letter
- Database error → retry with exponential backoff
- Kafka publish error → retry, log, continue
```

### 14.5 Retry Strategy

```
RETRY CONFIG:
  max_retries: 3
  initial_delay_ms: 100
  max_delay_ms: 5000
  backoff_multiplier: 2.0

DEAD-LETTER:
  Topic: eth.dead-letter
  Contains: original event + error message + retry count + timestamp
  Processing: manual review or periodic retry job
```

---

## 15. Logging, Monitoring, Observability

### 15.1 Structured Logging

```python
import structlog

logger = structlog.get_logger()

# Every log entry includes:
logger.info("prediction_completed",
    correlation_id="req-abc-123",
    address="0x742d...",
    phishing_score=0.847,
    prediction="phishing",
    model_version="1.0.0",
    inference_mode="real",
    inference_time_ms=23.4,
    source="api",
)
```

**Log format**: JSON lines (one JSON object per line)

### 15.2 Correlation ID

```
Every API request gets a UUID correlation ID:
- Assigned in middleware at request entry
- Passed through all downstream calls
- Included in all log entries for that request
- Returned in response header: X-Correlation-ID
- Stored in predictions table for audit
```

### 15.3 Log Categories


| Log Type           | What is logged                                | Where                                |
| ------------------ | --------------------------------------------- | ------------------------------------ |
| **Request log**    | method, endpoint, status, duration, client_ip | `api_requests_log` table + stdout    |
| **Prediction log** | address, score, model_version, mode           | `predictions` table + stdout         |
| **Model load log** | version, load_time, status, error             | stdout + file                        |
| **Alert log**      | address, score, trigger                       | `alerts` table + stdout              |
| **Job status log** | job_id, status, records, duration             | `etl_jobs`/`ingestion_jobs` + stdout |
| **Error log**      | exception, stack trace, context               | stderr + file                        |


### 15.4 Metrics to Monitor (Prometheus)


| Metric                         | Type      | Description                      |
| ------------------------------ | --------- | -------------------------------- |
| `api_request_total`            | Counter   | Total API requests by endpoint   |
| `api_request_duration_seconds` | Histogram | Request latency distribution     |
| `prediction_total`             | Counter   | Total predictions by mode/result |
| `prediction_latency_seconds`   | Histogram | Inference time                   |
| `alert_total`                  | Counter   | Alerts generated                 |
| `model_load_timestamp`         | Gauge     | When model was last loaded       |
| `kafka_consumer_lag`           | Gauge     | Messages behind in consumer      |
| `kafka_messages_processed`     | Counter   | Events processed                 |
| `db_connection_pool_active`    | Gauge     | Active DB connections            |


### 15.5 Health Checks


| Service    | Check                               | Interval |
| ---------- | ----------------------------------- | -------- |
| API        | `GET /health` returns 200           | 10s      |
| Cloud SQL  | `pg_isready` / TCP connect          | 5s       |
| Kafka      | Broker API versions check           | 10s      |
| GCS        | `storage.Client().list_buckets()`   | 30s      |
| Model      | Model file exists (GCS or local)    | 60s      |


---

## 16. Security & Hardening

### 16.1 Classification


| Feature                       | Must-have (demo) | Should-have (internal) | Future (production)    |
| ----------------------------- | ---------------- | ---------------------- | ---------------------- |
| Input validation              | ✅                | ✅                      | ✅                      |
| Ethereum address format check | ✅                | ✅                      | ✅                      |
| CORS (allow localhost)        | ✅                | ✅                      | ✅                      |
| API key auth                  | ❌                | ✅                      | ✅                      |
| JWT auth                      | ❌                | ❌                      | ✅                      |
| Rate limiting                 | ❌                | ✅                      | ✅                      |
| HTTPS/TLS                     | ❌                | ❌                      | ✅                      |
| RBAC                          | ❌                | ❌                      | ✅                      |
| Secrets management            | `.env` + GCP SA  | `.env` + GCP SA        | Vault / Secret Manager |
| Data masking                  | ❌                | Log truncation         | Full PII handling      |
| Backup                        | ❌                | pg_dump manual         | Automated daily        |
| Data retention                | No policy        | 90-day API logs        | Configurable per table |


### 16.2 Immediate Security (Phase 4)

```python
# Input validation
import re
ETHEREUM_ADDRESS_RE = re.compile(r"^0x[a-fA-F0-9]{40}$")

def validate_address(address: str) -> bool:
    return bool(ETHEREUM_ADDRESS_RE.match(address))

# CORS (FastAPI)
from fastapi.middleware.cors import CORSMiddleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:8501"],  # Streamlit
    allow_methods=["*"],
    allow_headers=["*"],
)
```

### 16.3 Phase 8 Security Additions

```python
# API Key middleware
from fastapi import Security, HTTPException
from fastapi.security import APIKeyHeader

api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)

async def verify_api_key(api_key: str = Security(api_key_header)):
    if api_key != settings.API_KEY:
        raise HTTPException(status_code=403, detail="Invalid API key")
    return api_key

# Rate limiting
from slowapi import Limiter
limiter = Limiter(key_func=get_remote_address)
app.state.limiter = limiter

@app.post("/predict/address")
@limiter.limit("100/minute")
async def predict_address(request: Request, ...):
    ...
```

---

## 17. Docker-compose Skeleton

```yaml
# docker-compose.yml
# Ethereum Phishing Detection Platform
# Runtime: Rancher Desktop (local compute) + GCP (storage: Cloud SQL + GCS)
# Usage: docker-compose up -d
# NOTE: PostgreSQL is on Cloud SQL (GCP) — no local postgres container needed
#       Object storage is on GCS — no local MinIO container needed

version: "3.8"

x-common-env: &common-env
  TZ: UTC
  GOOGLE_APPLICATION_CREDENTIALS: /app/credentials/gcp-service-account.json

x-gcp-env: &gcp-env
  GCP_PROJECT_ID: ${GCP_PROJECT_ID}
  GCS_BUCKET_RAW: ${GCS_BUCKET_RAW:-eth-phishing-raw}
  GCS_BUCKET_PROCESSED: ${GCS_BUCKET_PROCESSED:-eth-phishing-processed}
  GCS_BUCKET_MODELS: ${GCS_BUCKET_MODELS:-eth-phishing-models}
  CLOUD_SQL_CONNECTION_NAME: ${CLOUD_SQL_CONNECTION_NAME}
  DATABASE_URL: postgresql://${POSTGRES_USER:-postgres}:${POSTGRES_PASSWORD}@${CLOUD_SQL_HOST}:5432/${POSTGRES_DB:-eth_phishing}

services:
  # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  # NOTE: DATA LAYER IS ON GCP (no local containers)
  #
  #   Cloud SQL PostgreSQL → ${CLOUD_SQL_HOST}:5432
  #   GCS buckets:
  #     gs://${GCS_BUCKET_RAW}/         → raw transaction data
  #     gs://${GCS_BUCKET_PROCESSED}/   → processed/enriched data
  #     gs://${GCS_BUCKET_MODELS}/      → model artifacts + MLflow
  # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

  # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  # MESSAGING LAYER (local)
  # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

  zookeeper:
    image: confluentinc/cp-zookeeper:7.6.0
    container_name: ethphish-zookeeper
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
      ZOOKEEPER_TICK_TIME: 2000
    networks:
      - backend
    restart: unless-stopped
    deploy:
      resources:
        limits:
          memory: 256M

  kafka:
    image: confluentinc/cp-kafka:7.6.0
    container_name: ethphish-kafka
    depends_on:
      - zookeeper
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:29092,PLAINTEXT_HOST://localhost:9092
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT
      KAFKA_INTER_BROKER_LISTENER_NAME: PLAINTEXT
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: "true"
    ports:
      - "${KAFKA_PORT:-9092}:9092"
    networks:
      - backend
    restart: unless-stopped
    deploy:
      resources:
        limits:
          memory: 1G

  # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  # ML TRACKING (local server, artifacts → GCS)
  # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

  mlflow:
    image: ghcr.io/mlflow/mlflow:v2.14.0
    container_name: ethphish-mlflow
    environment:
      <<: *common-env
      <<: *gcp-env
      MLFLOW_BACKEND_STORE_URI: postgresql://${POSTGRES_USER:-postgres}:${POSTGRES_PASSWORD}@${CLOUD_SQL_HOST}:5432/mlflow
      MLFLOW_DEFAULT_ARTIFACT_ROOT: gs://${GCS_BUCKET_MODELS:-eth-phishing-models}/mlflow-artifacts/
    command: >
      mlflow server
      --host 0.0.0.0
      --port 5000
      --backend-store-uri postgresql://${POSTGRES_USER:-postgres}:${POSTGRES_PASSWORD}@${CLOUD_SQL_HOST}:5432/mlflow
      --default-artifact-root gs://${GCS_BUCKET_MODELS:-eth-phishing-models}/mlflow-artifacts/
    ports:
      - "${MLFLOW_PORT:-5000}:5000"
    volumes:
      - ./credentials:/app/credentials:ro
    networks:
      - backend
      - frontend
    restart: unless-stopped
    deploy:
      resources:
        limits:
          memory: 512M

  # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  # APPLICATION LAYER (local compute, GCP storage)
  # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

  api:
    build:
      context: ./api
      dockerfile: Dockerfile
    container_name: ethphish-api
    environment:
      <<: *common-env
      <<: *gcp-env
      KAFKA_BOOTSTRAP_SERVERS: kafka:29092
      MODEL_ARTIFACTS_DIR: /app/model_artifacts
      INFERENCE_MODE: ${INFERENCE_MODE:-auto}
      LOG_LEVEL: ${LOG_LEVEL:-INFO}
    ports:
      - "${API_PORT:-8000}:8000"
    volumes:
      - ./model_artifacts:/app/model_artifacts:ro
      - ./credentials:/app/credentials:ro
      - ./configs:/app/configs:ro
      - ./logs/api:/app/logs
    networks:
      - backend
      - frontend
    restart: unless-stopped
    deploy:
      resources:
        limits:
          memory: 1G

  dashboard:
    build:
      context: ./dashboard
      dockerfile: Dockerfile
    container_name: ethphish-dashboard
    environment:
      <<: *common-env
      API_URL: http://api:8000
    ports:
      - "${DASHBOARD_PORT:-8501}:8501"
    depends_on:
      - api
    networks:
      - frontend
    restart: unless-stopped
    deploy:
      resources:
        limits:
          memory: 512M

  # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  # OPTIONAL: SPARK (profile: full)
  # Reads/writes GCS via gs:// connector
  # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

  spark-master:
    image: bitnami/spark:3.5
    container_name: ethphish-spark-master
    environment:
      - SPARK_MODE=master
      - SPARK_MASTER_HOST=spark-master
      - GOOGLE_APPLICATION_CREDENTIALS=/app/credentials/gcp-service-account.json
    ports:
      - "${SPARK_MASTER_UI_PORT:-8080}:8080"
      - "7077:7077"
    volumes:
      - ./credentials:/app/credentials:ro
    networks:
      - backend
    profiles:
      - full
    deploy:
      resources:
        limits:
          memory: 1G

  spark-worker:
    image: bitnami/spark:3.5
    container_name: ethphish-spark-worker
    environment:
      - SPARK_MODE=worker
      - SPARK_MASTER_URL=spark://spark-master:7077
      - SPARK_WORKER_MEMORY=2g
      - SPARK_WORKER_CORES=2
      - GOOGLE_APPLICATION_CREDENTIALS=/app/credentials/gcp-service-account.json
    depends_on:
      - spark-master
    volumes:
      - ./credentials:/app/credentials:ro
    networks:
      - backend
    profiles:
      - full
    deploy:
      resources:
        limits:
          memory: 2G

  # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  # OPTIONAL: MONITORING (profile: monitoring)
  # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

  prometheus:
    image: prom/prometheus:latest
    container_name: ethphish-prometheus
    volumes:
      - ./infra/prometheus/prometheus.yml:/etc/prometheus/prometheus.yml:ro
    ports:
      - "${PROMETHEUS_PORT:-9090}:9090"
    networks:
      - backend
    profiles:
      - monitoring
    deploy:
      resources:
        limits:
          memory: 256M

  grafana:
    image: grafana/grafana:10.4.0
    container_name: ethphish-grafana
    environment:
      GF_SECURITY_ADMIN_USER: ${GRAFANA_USER:-admin}
      GF_SECURITY_ADMIN_PASSWORD: ${GRAFANA_PASSWORD:-admin}
    ports:
      - "${GRAFANA_PORT:-3000}:3000"
    volumes:
      - grafana_data:/var/lib/grafana
      - ./infra/grafana/provisioning:/etc/grafana/provisioning:ro
    depends_on:
      - prometheus
    networks:
      - frontend
      - backend
    profiles:
      - monitoring
    deploy:
      resources:
        limits:
          memory: 256M

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# NETWORKS
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

networks:
  backend:
    driver: bridge
  frontend:
    driver: bridge

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# VOLUMES (minimal — storage is on GCP!)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

volumes:
  grafana_data:
```

---

## 18. File cấu hình mẫu

### 18.1 `.env.example`

```bash
# ============================================================
# Ethereum Phishing Detection Platform — Environment Variables
# Copy to .env and customize
# ============================================================

# ── GCP Configuration (REQUIRED) ──
GCP_PROJECT_ID=your-gcp-project-id
GOOGLE_APPLICATION_CREDENTIALS=./credentials/gcp-service-account.json

# ── Cloud SQL PostgreSQL (GCP) ──
CLOUD_SQL_HOST=34.xxx.xxx.xxx          # Cloud SQL public IP
CLOUD_SQL_CONNECTION_NAME=project:region:instance
POSTGRES_DB=eth_phishing
POSTGRES_USER=postgres
POSTGRES_PASSWORD=your-secure-password  # Set during Cloud SQL creation

# ── GCS Buckets (GCP) ──
GCS_BUCKET_RAW=eth-phishing-raw
GCS_BUCKET_PROCESSED=eth-phishing-processed
GCS_BUCKET_MODELS=eth-phishing-models

# ── Kafka (local) ──
KAFKA_PORT=9092

# ── MLflow (local server, artifacts → GCS) ──
MLFLOW_PORT=5000

# ── API (local) ──
API_PORT=8000
INFERENCE_MODE=auto        # auto | mock | real
LOG_LEVEL=INFO
# API_KEY=your-api-key-here  # Uncomment for Phase 8

# ── Dashboard (local) ──
DASHBOARD_PORT=8501

# ── Spark (optional, local) ──
SPARK_MASTER_UI_PORT=8080

# ── Monitoring (optional, local) ──
GRAFANA_USER=admin
GRAFANA_PASSWORD=admin
GRAFANA_PORT=3000
PROMETHEUS_PORT=9090

# ── External APIs (optional) ──
# ETHERSCAN_API_KEY=your-key-here
```

### 18.2 `configs/config.yaml`

```yaml
# ============================================================
# Platform Configuration
# ============================================================

app:
  name: "Ethereum Phishing Detection Platform"
  version: "1.0.0"
  environment: "local"  # local | staging | production

gcp:
  project_id: "${GCP_PROJECT_ID}"
  credentials_file: "/app/credentials/gcp-service-account.json"

storage:
  type: "gcs"  # gcs | local
  gcs:
    buckets:
      raw: "${GCS_BUCKET_RAW}"
      processed: "${GCS_BUCKET_PROCESSED}"
      models: "${GCS_BUCKET_MODELS}"
    model_prefix: "models/"
    mlflow_prefix: "mlflow-artifacts/"

database:
  type: "cloud_sql"  # cloud_sql | local_postgres
  cloud_sql:
    host: "${CLOUD_SQL_HOST}"
    port: 5432
    database: "${POSTGRES_DB}"
    user: "${POSTGRES_USER}"
    pool_size: 10
    max_overflow: 20

model:
  artifacts_dir: "/app/model_artifacts"
  current_dir: "current"
  metadata_file: "metadata.json"
  model_file: "model.pt"
  default_threshold: 0.7
  refresh_interval_hours: 24
  gcs_sync: true  # sync model artifacts from GCS at startup
  confidence_bands:
    high: 0.8
    medium: 0.5
    low: 0.0
  auto_alert_threshold: 0.85

inference:
  batch_size: 256
  max_batch_addresses: 100
  timeout_seconds: 30
  mode: "auto"  # auto | mock | real

kafka:
  topics:
    transactions: "eth.transactions"
    alerts: "eth.alerts"
    dead_letter: "eth.dead-letter"
  consumer:
    group_id: "phishing-detector"
    auto_offset_reset: "latest"
    max_poll_records: 500
  producer:
    acks: "all"
    retries: 3

streaming:
  mock_producer:
    rate_per_second: 5
    phishing_ratio: 0.05
  consumer:
    processing_interval_seconds: 10
    retry:
      max_retries: 3
      initial_delay_ms: 100
      max_delay_ms: 5000
      backoff_multiplier: 2.0

logging:
  level: "INFO"
  format: "json"  # json | text
  correlation_id_header: "X-Correlation-ID"

api:
  rate_limit:
    enabled: false  # Enable in Phase 8
    default: "100/minute"
    predict: "100/minute"
    batch: "20/minute"
  cors:
    origins:
      - "http://localhost:8501"
      - "http://localhost:3000"
```

### 18.3 `model_artifacts/current/metadata.json` (Mock)

```json
{
  "model_id": "mock-predictor",
  "model_version": "mock-0.0.0",
  "model_type": "MockPredictor",
  "created_at": "2026-04-09T00:00:00Z",
  "created_by": "platform-team",

  "input_schema": {
    "type": "address_string",
    "feature_count": 0,
    "feature_names": [],
    "normalization": null
  },

  "output_schema": {
    "type": "binary_classification",
    "classes": ["legitimate", "phishing"],
    "output_dim": 2,
    "output_format": "deterministic_hash"
  },

  "inference_config": {
    "default_threshold": 0.7,
    "batch_size": 256,
    "device": "cpu",
    "requires_graph": false
  },

  "metrics": {
    "test_precision": null,
    "test_recall": null,
    "test_f1": null,
    "test_roc_auc": null,
    "test_pr_auc": null,
    "note": "Mock predictor — no real metrics available"
  },

  "compatibility": {
    "python_version": ">=3.10",
    "api_version": ">=1.0.0"
  }
}
```

---

## 19. Pseudocode / Skeleton Code

### 19.1 App Startup — Load Model

```python
# api/main.py

from contextlib import asynccontextmanager
from fastapi import FastAPI
from .services.model_loader import ModelLoader
from .services.predictor import get_predictor
from .config import settings

@asynccontextmanager
async def lifespan(app: FastAPI):
    # STARTUP
    loader = ModelLoader(settings.MODEL_ARTIFACTS_DIR)
    metadata = loader.load_metadata()

    if metadata and loader.model_file_exists():
        predictor = loader.load_real_predictor(metadata)
        app.state.inference_mode = "real"
        logger.info("Model loaded", version=metadata["model_version"])
    else:
        predictor = loader.load_mock_predictor()
        app.state.inference_mode = "mock"
        logger.warning("No model found — running in mock mode")

    app.state.predictor = predictor
    app.state.model_metadata = metadata

    yield  # App runs here

    # SHUTDOWN
    logger.info("Shutting down")

app = FastAPI(title="Ethereum Phishing Detection API", lifespan=lifespan)
```

### 19.2 Base Predictor Interface

```python
# model_runtime/base_predictor.py

from abc import ABC, abstractmethod

class BasePredictor(ABC):
    @abstractmethod
    def predict(self, address: str) -> dict:
        """Predict phishing probability for a single address."""
        ...

    @abstractmethod
    def predict_batch(self, addresses: list[str]) -> list[dict]:
        """Predict phishing probability for multiple addresses."""
        ...

    @abstractmethod
    def get_model_info(self) -> dict:
        """Return model metadata."""
        ...

    def health_check(self) -> bool:
        """Check if predictor is ready to serve."""
        return True
```

### 19.3 Real Predictor Interface (Skeleton)

```python
# model_runtime/gnn_predictor.py

import torch
import numpy as np
from .base_predictor import BasePredictor

class GNNPredictor(BasePredictor):
    """
    Real GNN predictor — loaded when model.pt is available.
    PLACEHOLDER: Fill in when model training (Phase 3) is complete.
    """

    def __init__(self, model_path: str, metadata: dict, graph_data_dir: str):
        self.metadata = metadata
        self.device = torch.device(
            metadata.get("inference_config", {}).get("device", "cpu")
        )

        # Load model weights
        checkpoint = torch.load(model_path, map_location=self.device,
                                weights_only=False)
        self.model = self._build_model(metadata)
        self.model.load_state_dict(checkpoint["model_state_dict"])
        self.model.eval()

        # Load graph data
        self.node_features = np.load(f"{graph_data_dir}/node_features.npy")
        self.edge_index = np.load(f"{graph_data_dir}/edge_index.npy")
        # ... load node_to_id mapping, etc.

    def _build_model(self, metadata):
        """Build model architecture from metadata."""
        # TODO: Implement based on model_type in metadata
        raise NotImplementedError("Implement when model architecture is finalized")

    def predict(self, address: str) -> dict:
        address = address.lower()
        # TODO: Implement real inference
        # 1. Look up address in node_to_id mapping
        # 2. Extract k-hop subgraph
        # 3. Run model forward pass
        # 4. Return prediction dict
        raise NotImplementedError("Implement when model is available")

    def predict_batch(self, addresses: list[str]) -> list[dict]:
        return [self.predict(addr) for addr in addresses]

    def get_model_info(self) -> dict:
        return self.metadata
```

### 19.4 `/predict/address` Endpoint

```python
# api/routers/predict.py

from fastapi import APIRouter, Request, HTTPException
from ..models import PredictAddressRequest, PredictAddressResponse
from ..services.prediction_logger import PredictionLogger

router = APIRouter()

@router.post("/predict/address", response_model=PredictAddressResponse)
async def predict_address(
    request: Request,
    body: PredictAddressRequest,
):
    predictor = request.app.state.predictor
    logger = PredictionLogger(request.app.state.db_pool)

    # Validate Ethereum address format
    if not is_valid_ethereum_address(body.address):
        raise HTTPException(status_code=400,
                            detail="Invalid Ethereum address format")

    # Run prediction
    result = predictor.predict(body.address)

    # Override threshold if requested
    if body.threshold_override is not None:
        result["threshold_used"] = body.threshold_override
        result["prediction"] = (
            "phishing" if result["phishing_probability"] >= body.threshold_override
            else "legitimate"
        )

    # Ensure address exists in addresses table
    await logger.ensure_address(body.address)

    # Log prediction to database
    prediction_id = await logger.log_prediction(
        result,
        source="api",
        correlation_id=request.state.correlation_id,
    )
    result["prediction_id"] = prediction_id

    # Auto-generate alert if score exceeds alert threshold
    if result["phishing_probability"] >= settings.AUTO_ALERT_THRESHOLD:
        await logger.create_alert(result, trigger_source="api")

    return PredictAddressResponse(**result)
```

### 19.5 Background Job — Ingest Mock Data

```python
# api/routers/ingest.py

import asyncio
import random
import hashlib
from fastapi import APIRouter, BackgroundTasks

router = APIRouter()

async def ingest_mock_transactions(count: int, phishing_ratio: float, db_pool):
    """Background task: generate and insert mock transactions."""
    for i in range(count):
        is_phishing = random.random() < phishing_ratio
        addr = generate_mock_address(is_phishing)
        tx = generate_mock_transaction(addr)

        # Insert into transactions_raw
        await db_pool.execute(
            "INSERT INTO transactions_raw (tx_hash, from_address, to_address, "
            "value_eth, gas, block_number, block_timestamp) "
            "VALUES ($1, $2, $3, $4, $5, $6, NOW())",
            tx["tx_hash"], tx["from_address"], tx["to_address"],
            tx["value_eth"], tx["gas"], tx["block_number"],
        )

@router.post("/ingest/mock-transactions")
async def create_mock_transactions(
    count: int = 100,
    phishing_ratio: float = 0.05,
    background_tasks: BackgroundTasks = None,
):
    background_tasks.add_task(
        ingest_mock_transactions, count, phishing_ratio, db_pool
    )
    return {"status": "accepted", "count": count}
```

### 19.6 Kafka Consumer

```python
# streaming/consumer.py

from confluent_kafka import Consumer, KafkaError
import json

def run_consumer(predictor, db_conn, config):
    consumer = Consumer({
        "bootstrap.servers": config["kafka"]["bootstrap_servers"],
        "group.id": config["kafka"]["consumer"]["group_id"],
        "auto.offset.reset": config["kafka"]["consumer"]["auto_offset_reset"],
    })
    consumer.subscribe([config["kafka"]["topics"]["transactions"]])

    while True:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            handle_consumer_error(msg.error())
            continue

        try:
            event = json.loads(msg.value().decode("utf-8"))
            process_transaction_event(event, predictor, db_conn, config)
            consumer.commit(msg)
        except Exception as e:
            publish_to_dead_letter(msg, str(e))
            consumer.commit(msg)

def process_transaction_event(event, predictor, db_conn, config):
    address = event["from_address"]
    result = predictor.predict(address)

    # Log prediction
    log_prediction(db_conn, result, source="streaming")

    # Generate alert if above threshold
    threshold = config["model"]["auto_alert_threshold"]
    if result["phishing_probability"] >= threshold:
        create_alert(db_conn, result, trigger_tx_hash=event.get("tx_hash"))
        publish_alert(result)
```

### 19.7 Alert Generation

```python
# streaming/alert_processor.py

def create_alert(db_conn, prediction_result, trigger_tx_hash=None):
    """Create an alert record and publish to Kafka."""
    with db_conn.cursor() as cur:
        cur.execute(
            "INSERT INTO alerts (address, phishing_score, trigger_source, "
            "trigger_tx_hash, model_version_id) "
            "VALUES (%s, %s, %s, %s, "
            " (SELECT id FROM model_versions WHERE is_active = TRUE)) "
            "RETURNING id",
            (
                prediction_result["address"],
                prediction_result["phishing_probability"],
                "streaming",
                trigger_tx_hash,
            ),
        )
        alert_id = cur.fetchone()[0]
        db_conn.commit()
    return alert_id
```

### 19.8 Dashboard Summary Aggregation

```python
# api/routers/dashboard.py

@router.get("/dashboard/summary")
async def dashboard_summary(request: Request):
    db = request.app.state.db_pool

    summary = await db.fetchrow("SELECT * FROM v_dashboard_summary")
    daily = await db.fetch(
        "SELECT * FROM v_daily_predictions ORDER BY prediction_date DESC LIMIT 7"
    )
    top_risky = await db.fetch("SELECT * FROM v_top_risky LIMIT 5")

    model_meta = request.app.state.model_metadata or {}

    return {
        "total_predictions": summary["total_predictions"],
        "total_phishing_detected": summary["total_phishing"],
        "total_alerts": summary["total_alerts"],
        "unreviewed_alerts": summary["unreviewed_alerts"],
        "predictions_last_24h": summary["predictions_24h"],
        "avg_inference_time_ms": round(summary["avg_inference_ms"] or 0, 1),
        "model_version": model_meta.get("model_version", "unknown"),
        "inference_mode": request.app.state.inference_mode,
        "top_risky_addresses": [dict(r) for r in top_risky],
        "daily_trend": [dict(r) for r in daily],
    }
```

---

## 20. Luồng tích hợp sau khi Model hoàn tất

### 20.1 Checklist tích hợp model

```
PRE-REQUISITES (từ ML team):
□ File model.pt đã export (torch.save format)
□ File metadata.json đã tạo theo schema ở Section 8.2
□ File metrics.json hoặc metrics nằm trong metadata.json
□ Graph data files: node_features.npy, edge_index.npy, node_to_id.pkl
□ Scaler parameters (mean, scale) nếu có normalization
□ Model architecture code (hoặc mô tả đủ để rebuild)

INTEGRATION STEPS:

1. UPLOAD ARTIFACT TO GCS:
   □ gsutil cp model.pt gs://eth-phishing-models/models/v1/model.pt
   □ gsutil cp metadata.json gs://eth-phishing-models/models/v1/metadata.json
   □ gsutil cp node_features.npy gs://eth-phishing-models/models/v1/graph/
   □ gsutil cp edge_index.npy gs://eth-phishing-models/models/v1/graph/
   □ gsutil cp node_to_id.pkl gs://eth-phishing-models/models/v1/graph/
   □ gsutil -m rsync gs://eth-phishing-models/models/v1/ gs://eth-phishing-models/models/current/

2. UPDATE METADATA:
   □ Verify metadata.json has all required fields (Section 8.2)
   □ Set correct feature_count
   □ Set correct threshold
   □ Fill in training_info and metrics sections
   □ Set compatibility versions

3. VERIFY SCHEMA COMPATIBILITY:
   □ Run: python scripts/validate_model.py
   □ Check: feature_count matches API expectation
   □ Check: torch version compatible
   □ Check: input/output schema matches API contract

4. IMPLEMENT REAL PREDICTOR:
   □ Fill in model_runtime/gnn_predictor.py
   □ Implement _build_model() matching model architecture
   □ Implement predict() with proper graph lookup
   □ Implement predict_batch() for efficiency
   □ Run: pytest tests/test_predictor.py

5. IMPORT METRICS:
   □ Run: python scripts/import_metrics.py model_artifacts/v1/metadata.json
   □ Verify: GET /model/metrics returns real metrics
   □ Verify: Dashboard metrics page shows real values

6. TEST ENDPOINTS:
   □ Restart API: docker-compose restart api
   □ Test: GET /health → inference_mode should be "real"
   □ Test: GET /model/info → shows real model version
   □ Test: POST /predict/address → returns real prediction
   □ Test: POST /predict/batch → batch predictions work
   □ Test: GET /predictions/history → predictions logged

7. TEST UI:
   □ Open dashboard: http://localhost:8501
   □ Verify: No mock mode banner
   □ Test: Address lookup returns real predictions
   □ Verify: Metrics page shows real training metrics
   □ Verify: Model info page shows real model details

8. ROLLBACK PLAN:
   □ If model fails to load:
     - Revert GCS: gsutil -m rsync gs://eth-phishing-models/models/mock/ gs://eth-phishing-models/models/current/
     - Restart API: docker-compose restart api
     - API falls back to mock mode automatically
   □ If predictions are wrong:
     - Check threshold in metadata.json
     - Check feature normalization parameters
     - Compare with training evaluation results
   □ If performance is too slow:
     - Check device setting (cpu vs cuda)
     - Reduce neighbor_samples in metadata
     - Enable batch mode for API
```

### 20.2 Validation Script

```python
# scripts/validate_model.py

import json
import sys
import torch

def validate(metadata_path: str):
    with open(metadata_path) as f:
        meta = json.load(f)

    errors = []

    # Required fields
    required = ["model_id", "model_version", "model_type",
                "input_schema", "output_schema", "inference_config"]
    for field in required:
        if field not in meta:
            errors.append(f"Missing required field: {field}")

    # Feature count
    fc = meta.get("input_schema", {}).get("feature_count")
    if fc is None or fc < 1:
        errors.append(f"Invalid feature_count: {fc}")

    # Threshold
    thresh = meta.get("inference_config", {}).get("default_threshold")
    if thresh is None or not (0 < thresh < 1):
        errors.append(f"Invalid threshold: {thresh}")

    # Model file exists
    model_dir = metadata_path.rsplit("/", 1)[0]
    model_file = f"{model_dir}/model.pt"
    try:
        checkpoint = torch.load(model_file, map_location="cpu", weights_only=False)
        if "model_state_dict" not in checkpoint:
            errors.append("model.pt missing 'model_state_dict' key")
    except FileNotFoundError:
        errors.append(f"Model file not found: {model_file}")

    if errors:
        print("VALIDATION FAILED:")
        for e in errors:
            print(f"  ✗ {e}")
        sys.exit(1)
    else:
        print("VALIDATION PASSED ✓")
        print(f"  Model: {meta['model_id']} v{meta['model_version']}")
        print(f"  Type: {meta['model_type']}")
        print(f"  Features: {fc}")
        print(f"  Threshold: {thresh}")
```

---

## 21. Deliverables

### 21.1 Checklist hoàn chỉnh


| #   | Deliverable                                    | Phase | Status                                |
| --- | ---------------------------------------------- | ----- | ------------------------------------- |
| 1   | `docker-compose.yml` (full stack)              | 4     | **Build now**                         |
| 2   | Folder structure (as in Section 7)             | 4     | **Build now**                         |
| 3   | `infra/postgres/init.sql` (full schema)        | 4     | **Build now**                         |
| 4   | `.env.example` + `configs/config.yaml`         | 4     | **Build now**                         |
| 5   | `api/` FastAPI skeleton (all 9 endpoints)      | 5     | **Build now**                         |
| 6   | `model_runtime/base_predictor.py`              | 5     | **Build now**                         |
| 7   | `model_runtime/mock_predictor.py`              | 5     | **Build now**                         |
| 8   | `model_runtime/gnn_predictor.py` (skeleton)    | 5     | **Build now** (fill when model ready) |
| 9   | `model_runtime/model_manager.py`               | 5     | **Build now**                         |
| 10  | `model_artifacts/current/metadata.json` (mock) | 5     | **Build now**                         |
| 11  | `dashboard/` Streamlit app (7 pages)           | 6     | **Build now**                         |
| 12  | `streaming/mock_producer.py`                   | 7     | **Build now**                         |
| 13  | `streaming/consumer.py`                        | 7     | **Build now**                         |
| 14  | `streaming/alert_processor.py`                 | 7     | **Build now**                         |
| 15  | `etl/jobs/ingest_raw.py` (skeleton)            | 7     | **Build now**                         |
| 16  | `scripts/seed_data.py`                         | 4     | **Build now**                         |
| 17  | `scripts/seed_metrics.py`                      | 4     | **Build now**                         |
| 18  | `scripts/validate_model.py`                    | 5     | **Build now**                         |
| 19  | `scripts/swap_model.sh`                        | 5     | **Build now**                         |
| 20  | `data/sample/` (CSV files)                     | 4     | **Build now**                         |
| 21  | `tests/` (pytest suite)                        | 5     | **Build now**                         |
| 22  | `infra/kafka/create-topics.sh`                 | 7     | **Build now**                         |
| 23  | `infra/gcp/setup-*.sh` (Cloud SQL, GCS, SA)    | 4     | **Build now**                         |
| 24  | `Makefile` (convenience commands)              | 4     | **Build now**                         |
| 25  | Integration checklist (Section 20)             | —     | **Done** (this document)              |


### 21.2 What NOT to Build Now


| Item                                | When to build | Why wait                          |
| ----------------------------------- | ------------- | --------------------------------- |
| Real GNN predictor implementation   | After Phase 3 | Depends on model architecture     |
| Feature engineering pipeline (full) | After Phase 2 | Depends on feature decisions      |
| Etherscan API integration (real)    | After demo    | Needs API key, rate limits        |
| HTTPS/TLS setup                     | Phase 8       | Not needed for local demo         |
| CI/CD pipeline                      | Phase 8       | Focus on functionality first      |
| Kubernetes manifests                | Post-demo     | docker-compose sufficient for now |
| Production deployment scripts       | Post-demo     | Local-first approach              |


---

## 22. Kết luận và Khuyến nghị

### 22.1 Stack tối ưu — Build ngay tuần này

```
GCP: Cloud SQL + GCS buckets (managed, always on)
Local: kafka + zookeeper + mlflow + api + dashboard
= 5 local containers + 2 GCP services, ~3 GB local RAM
```

Đây là MVP stack cho phép:

- Demo full website flow
- Mock predictions
- Prediction history
- Model metrics display
- Ready for model plug-in
- Data persists on GCP (không mất khi restart local)

### 22.2 Stack tối giản — Máy local yếu (8 GB RAM)

```
GCP: Cloud SQL + GCS buckets (managed)
Local: api + dashboard
= 2 local containers, ~1 GB local RAM
```

- Bỏ Kafka (mock data qua API endpoint thay vì streaming)
- Bỏ MLflow (dùng file-based metadata trên GCS)
- DB trên Cloud SQL — không tốn RAM local cho PostgreSQL
- Mọi thứ vẫn hoạt động nhưng không có streaming demo

### 22.3 Stack đầy đủ — Demo với website + Big Data proof

```
GCP: Cloud SQL + GCS buckets (managed)
Local: kafka + zookeeper + mlflow + api + dashboard
     + spark-master + spark-worker + grafana + prometheus
= 10 local containers + 2 GCP services, ~6-8 GB local RAM
```

- Full Big Data stack demonstration
- ETL qua Spark (reads/writes GCS)
- Monitoring via Grafana
- Cần máy 16+ GB RAM (nhưng nhẹ hơn so với full local vì DB trên GCP)

### 22.4 Những gì nên POSTPONE cho đến khi model xong


| Item                                   | Lý do postpone                        |
| -------------------------------------- | ------------------------------------- |
| `gnn_predictor.py` full implementation | Cần biết model architecture chính xác |
| Feature normalization logic (scaler)   | Cần scaler từ training pipeline       |
| Graph data loading optimization        | Cần biết graph size thực tế           |
| Real Etherscan producer                | Cần API key + phải hiểu rate limits   |
| Performance tuning inference           | Cần benchmark với real model          |


### 22.5 Ba bước đầu tiên cần làm ngay

**Bước 1 — GCP Setup + Scaffold (ngày 1)**

```bash
# 1. Setup GCP resources
gcloud projects create bda501-eth-phishing --name="BDA501 Final"
gcloud sql instances create ethphish-db --database-version=POSTGRES_16 \
  --tier=db-f1-micro --region=asia-southeast1
gsutil mb -l asia-southeast1 gs://eth-phishing-raw
gsutil mb -l asia-southeast1 gs://eth-phishing-processed
gsutil mb -l asia-southeast1 gs://eth-phishing-models

# 2. Create service account + download key
gcloud iam service-accounts create ethphish-sa
gcloud iam service-accounts keys create credentials/gcp-service-account.json \
  --iam-account=ethphish-sa@${PROJECT_ID}.iam.gserviceaccount.com

# 3. Create local folder structure
mkdir -p api/routers api/services api/middleware
mkdir -p dashboard/pages dashboard/components
mkdir -p model_artifacts/current model_artifacts/mock
mkdir -p model_runtime etl/jobs streaming
mkdir -p infra/{gcp,postgres,kafka,grafana,prometheus}
mkdir -p configs scripts tests data/sample logs credentials

# 4. Write config files
# Write docker-compose.yml (from Section 17)
# Write infra/postgres/init.sql → run against Cloud SQL
# Write .env (from .env.example, Section 18.1)
# Write configs/config.yaml (from Section 18.2)
```

**Bước 2 — API + Mock Predictor (ngày 2–3)**

```bash
# Implement api/ with all endpoints (connects to Cloud SQL + GCS)
# Implement model_runtime/mock_predictor.py
# Write scripts/seed_data.py (seeds Cloud SQL)
# Test: docker-compose up -d → curl http://localhost:8000/health
# Test: POST /predict/address with mock address
```

**Bước 3 — Dashboard + Streaming (ngày 4–5)**

```bash
# Implement dashboard/ with Streamlit
# Implement streaming/mock_producer.py
# Implement streaming/consumer.py
# Test: Full flow from dashboard → API → prediction → history
# Test: Mock producer → Kafka → consumer → alert → dashboard
```

---

### Summary

Hệ thống này được thiết kế theo nguyên tắc **"build the platform, plug in the model later"**:

1. **Toàn bộ infrastructure, API, dashboard, streaming chạy được ngay** mà không cần model
2. **Mock mode** cho phép demo full flow end-to-end
3. **Model integration contract** rõ ràng — khi model xong chỉ cần **upload GCS + restart**
4. **Không thay đổi code** khi chuyển từ mock sang real model
5. **Hybrid architecture**: local compute (Rancher Desktop) + cloud storage (GCP) — best of both worlds
6. **Data durability**: dữ liệu trên GCP không mất khi restart/rebuild local
7. **Mỗi service tách rời** — team backend, frontend, ML có thể làm song song

> **Handoff**: Document này đủ chi tiết để một team kỹ thuật bắt tay vào build ngay. Tất cả API contracts, database schemas, config formats, GCP setup steps, và integration checklist đã được định nghĩa. Chỉ cần follow theo thứ tự: **GCP Setup → Phase 4 → 5 → 6 → 7 → 8**.


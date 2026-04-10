# Running the ETH Transaction Analytics Platform

## Prerequisites

| Tool | Version | Check |
|------|---------|-------|
| Docker + Docker Compose | 24+ / v2+ | `docker compose version` |
| Python | 3.11+ | `python3 --version` |
| Google Cloud SDK | Latest | `gcloud --version` |
| Node.js (optional, for local dev) | 20+ | `node --version` |

## 1. Clone & Configure Environment

```bash
cp .env.example .env
```

Edit `.env` with your actual credentials:

| Variable | Where to get it |
|----------|----------------|
| `INFURA_PROJECT_ID` | [infura.io](https://infura.io) — create a free project, copy the Project ID |
| `GCS_PROJECT_ID` | Google Cloud Console → project selector → copy project ID |
| `AIRFLOW__CORE__FERNET_KEY` | Generate with: `python3 -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"` |
| `SLACK_WEBHOOK_URL` | (Optional) Slack App → Incoming Webhooks → copy URL |

## 2. Set Up GCS Credentials

```bash
# Create a GCS service account with Storage Admin role
gcloud iam service-accounts create eth-bigdata \
    --display-name="ETH BigData Pipeline"

gcloud projects add-iam-policy-binding $GCS_PROJECT_ID \
    --member="serviceAccount:eth-bigdata@$GCS_PROJECT_ID.iam.gserviceaccount.com" \
    --role="roles/storage.admin"

# Download the key
mkdir -p credentials
gcloud iam service-accounts keys create credentials/gcs-key.json \
    --iam-account="eth-bigdata@$GCS_PROJECT_ID.iam.gserviceaccount.com"
```

## 3. Create GCS Bucket

```bash
bash scripts/setup_gcs.sh
```

This creates the bucket with folder prefixes (`raw/`, `processed/`, `checkpoints/`, `archive/`) and sets lifecycle policies.

## 4. Bootstrap Historical Data (Optional)

If you want 180 days of historical ETH data before going live:

```bash
# Run in Google BigQuery console or via bq CLI
bq query --use_legacy_sql=false < bigquery/bootstrap_export.sql

# Verify export
gsutil ls gs://eth-bigdata-project/raw/transactions/ | head -10
```

> Cost: ~$1 for 180 days of data (~50-100 GB scanned).

If skipping this step, the crawler will start from scratch with live data only.

## 5. Start All Services

```bash
docker compose up -d
```

Wait ~60 seconds for all services to initialize, then verify:

```bash
docker compose ps
```

All services should show `running` or `healthy`. Expected output:

| Container | Port | Status |
|-----------|------|--------|
| eth-zookeeper | 2181 | healthy |
| eth-kafka | 9092, 29092 | healthy |
| eth-kafka-init | — | exited (0) |
| eth-postgres | 5432 | healthy |
| eth-crawler | 8001 | healthy |
| eth-spark-master | 7077, 8082 | running |
| eth-spark-worker | — | running |
| eth-airflow-init | — | exited (0) |
| eth-airflow-webserver | 8080 | healthy |
| eth-airflow-scheduler | — | running |
| eth-api | 8000 | healthy |
| eth-web | 3000 | healthy |

> `kafka-init` and `airflow-init` are one-shot containers — `exited (0)` is correct.

## 6. Verify the Stack

Run the smoke test:

```bash
bash scripts/smoke_test.sh
```

Or verify manually:

```bash
# Crawler health
curl http://localhost:8001/healthz

# API health
curl http://localhost:8000/api/health

# PostgreSQL tables
docker compose exec postgres psql -U ethuser -d ethdb -c '\dt'

# Kafka topic
docker compose exec kafka kafka-topics --list --bootstrap-server localhost:9092
```

## 7. Run Spark Jobs (First Time)

If you bootstrapped historical data (Step 4), run the initial processing:

```bash
# Option A: Full recompute (builds 180-day edge aggregate from scratch)
docker compose exec spark-master spark-submit /app/full_recompute_edges.py \
    --end-date 2025-03-31

# Option B: Process a single day
docker compose exec spark-master spark-submit /app/daily_snapshot.py \
    --target-date 2025-03-31

docker compose exec spark-master spark-submit /app/incremental_edges.py \
    --target-date 2025-03-31
```

If using test data instead:

```bash
# Seed synthetic data (writes to GCS)
python3 scripts/seed_test_data.py

# Process the seeded data
docker compose exec spark-master spark-submit /app/daily_snapshot.py \
    --target-date 2025-03-25

docker compose exec spark-master spark-submit /app/incremental_edges.py \
    --target-date 2025-03-25
```

After Spark completes, verify data landed in PostgreSQL:

```bash
docker compose exec postgres psql -U ethuser -d ethdb \
    -c "SELECT rank, wallet_address, total_txns FROM wallet_daily_snapshot ORDER BY rank LIMIT 5"
```

## 8. Access the Web Interfaces

| Interface | URL | Credentials |
|-----------|-----|-------------|
| Web App (Top 100 + Graph) | http://localhost:3000 | — |
| FastAPI Docs (Swagger) | http://localhost:8000/docs | — |
| Airflow Dashboard | http://localhost:8080 | admin / admin |
| Spark Master UI | http://localhost:8082 | — |

## 9. Daily Operations

Once running, the pipeline is automated:

- **Crawler** runs continuously, polling ETH blocks every ~12 seconds
- **Airflow** triggers daily at 00:05 UTC:
  1. Validates raw data partition
  2. Runs Spark daily snapshot (top 100)
  3. Runs Spark incremental edge aggregation
  4. Cleans up old edge partitions (>30 days)
- **API + Web** serve the latest data on demand

### Monitor via Airflow

Open http://localhost:8080 → DAGs → `eth_daily_pipeline` to see run history, task durations, and any failures.

### Manual Airflow Trigger

```bash
docker compose exec airflow-webserver \
    airflow dags trigger eth_daily_pipeline --conf '{}' -e 2025-03-31
```

## Troubleshooting

### Crawler not processing blocks

```bash
# Check health and lag
curl http://localhost:8001/healthz | python3 -m json.tool

# Check logs
docker compose logs -f crawler --tail 50
```

Common causes: invalid `INFURA_PROJECT_ID`, missing GCS credentials, Kafka not ready.

### Spark job fails

```bash
# Check Spark master UI for error details
open http://localhost:8082

# Run manually with verbose output
docker compose exec spark-master spark-submit --verbose /app/daily_snapshot.py \
    --target-date 2025-03-31
```

Common causes: GCS partition empty for target date, missing JDBC driver, PostgreSQL connection refused.

### API returns empty results

```bash
# Verify data exists in PostgreSQL
docker compose exec postgres psql -U ethuser -d ethdb \
    -c "SELECT snapshot_date, COUNT(*) FROM wallet_daily_snapshot GROUP BY snapshot_date"

# Check API logs
docker compose logs api --tail 20
```

Spark must complete before the API has data to serve.

### Airflow DAGs not visible

```bash
# Check scheduler logs
docker compose logs airflow-scheduler --tail 30

# Verify DAG files are mounted
docker compose exec airflow-webserver ls /opt/airflow/dags/
```

### Reset everything

```bash
# Stop all services and remove volumes (loses all data)
docker compose down -v

# Rebuild images after code changes
docker compose build --no-cache

# Start fresh
docker compose up -d
```

## Service Architecture Quick Reference

```
Browser :3000 → nginx → FastAPI :8000 → PostgreSQL :5432
                                              ↑
ETH RPC → Crawler :8001 → Kafka :9092 → GCS (Parquet)
                                              ↓
                        Airflow :8080 → Spark :7077 → GCS + PostgreSQL
```

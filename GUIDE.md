# Running the ETH Transaction Analytics Platform

## Prerequisites


| Tool                              | Version   | Check                    |
| --------------------------------- | --------- | ------------------------ |
| Docker + Docker Compose           | 24+ / v2+ | `docker compose version` |
| Python                            | 3.11+     | `python3 --version`      |
| Google Cloud SDK                  | Latest    | `gcloud --version`       |
| Node.js (optional, for local dev) | 20+       | `node --version`         |


Throughout this guide, `**GCS_BUCKET**` means the bucket name in `.env` (default `eth-bigdata-project`). Replace `eth-bigdata-project` in `gsutil` examples if yours differs.

## Platform structure and pipeline flow

### Repository layout


| Path            | Purpose                                                                                              |
| --------------- | ---------------------------------------------------------------------------------------------------- |
| `crawler/`      | ETH block polling, Kafka + GCS raw writes                                                            |
| `kafka/`        | Topic bootstrap script used by `kafka-init`                                                          |
| `spark/`        | PySpark jobs (`daily_snapshot`, `incremental_edges`, `full_recompute_edges`, helpers under `utils/`) |
| `airflow/dags/` | `eth_daily_pipeline`, `full_recompute`, etc.                                                         |
| `sql/`          | `init.sql` (schema on first Postgres start), `migrations/` (one-off ALTERs for existing DBs)         |
| `api/`, `web/`  | FastAPI + React/D3 serving layer                                                                     |
| `scripts/`      | GCS setup, smoke tests, optional seed data                                                           |


### GCS layout (zones)


| Prefix                                                        | Content                                                           |
| ------------------------------------------------------------- | ----------------------------------------------------------------- |
| `gs://$GCS_BUCKET/raw/transactions/dt=YYYY-MM-DD/`            | Immutable raw transaction Parquet (crawler / BigQuery bootstrap)  |
| `gs://$GCS_BUCKET/processed/snapshots/dt=YYYY-MM-DD/`         | Daily top-100 snapshot Parquet (Spark job 1)                      |
| `gs://$GCS_BUCKET/processed/graph_edges/run_date=YYYY-MM-DD/` | 180-day rolling edge aggregate for that run date (Spark jobs 2–3) |


GCS is the **source of truth** for raw and processed files; PostgreSQL holds only **aggregated** tables for the API.

### End-to-end flow

```text
ETH RPC / BigQuery bootstrap
        → Crawler → Kafka → GCS raw Parquet (dt=…)
        → Airflow → Spark batch → GCS processed Parquet + JDBC → PostgreSQL
        → FastAPI → Web (top 100 + graph)
```

1. **Ingest:** Crawler writes each day’s transactions under `raw/transactions/dt=…` (append-style partitions).
2. **Daily batch (Airflow):** Validates the partition → `daily_snapshot.py` (top 100 + upsert `wallet_daily_snapshot`) → `incremental_edges.py` (sliding 180-day window + atomic swap `wallet_graph_edge`).
3. **Full recompute (manual / DAG):** `full_recompute_edges.py` rescans up to 180 existing raw days, writes `processed/graph_edges/run_date=…`, then swaps edges in PostgreSQL.
4. **Serve:** API reads PostgreSQL only; the web app calls the API.

### PostgreSQL (serving)

- `**wallet_daily_snapshot`** — one row per wallet per day in the top 100, keyed by `(wallet_address, snapshot_date)`.
- `**wallet_graph_edge**` — directed edges for the current 180-day window; Spark loads via `**wallet_graph_edge_staging**` then renames tables (atomic swap in `spark/utils/pg_writer.py`).

`from_wallet` / `to_wallet` may be **NULL** in edge rows (e.g. contract-creation–style txs with no `to` in raw data). New databases get this from `sql/init.sql`; existing DBs can run `sql/migrations/001_wallet_graph_edge_nullable_endpoints.sql`. The Postgres image is configured with higher `**max_wal_size`** (see `docker-compose.yml`) to reduce checkpoint churn during large JDBC writes.

## 1. Clone & Configure Environment

```bash
cp .env.example .env
```

Edit `.env` with your actual credentials:


| Variable                    | Where to get it                                                                                                |
| --------------------------- | -------------------------------------------------------------------------------------------------------------- |
| `INFURA_PROJECT_ID`         | [infura.io](https://infura.io) — create a free project, copy the Project ID                                    |
| `GCS_PROJECT_ID`            | Set to `eth-bigdata-project` (or your own GCP project ID)                                                      |
| `AIRFLOW__CORE__FERNET_KEY` | Generate with: `python3 -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"`    |
| `SLACK_WEBHOOK_URL`         | Slack App → Incoming Webhooks → create webhook URL for alerts (leave empty only if you don't use Slack alerts) |


### Runtime selection (Docker Desktop vs Rancher Desktop)

Set the Docker runtime variables in `.env` so `docker-compose.yml` can work with either engine:

```bash
# Option A: Docker Desktop (default)
DOCKER_SOCK_PATH=/var/run/docker.sock
DOCKER_HOST=unix:///Users/<your-user>/.docker/run/docker.sock

# Option B: Rancher Desktop (moby mode)
# Replace <your-user> with your local macOS username
DOCKER_SOCK_PATH=/var/run/docker.sock
DOCKER_HOST=unix:///Users/<your-user>/.rd/docker.sock
```

> `DOCKER_SOCK_PATH` is for bind-mount inside containers and should stay `/var/run/docker.sock`.  
> `DOCKER_HOST` is for your local Docker CLI endpoint on macOS.

## 2. Set Up GCS Credentials

```bash
# Create/select GCP project
gcloud projects create eth-bigdata-project --name="ETH BigData Project"
gcloud config set project eth-bigdata-project
export GCS_PROJECT_ID=eth-bigdata-project

# Enable required APIs
gcloud services enable storage.googleapis.com iam.googleapis.com

# Create a service account for the pipeline
gcloud iam service-accounts create eth-bigdata \
    --display-name="ETH BigData Pipeline"

# Grant project-level admin permissions needed by setup scripts/operations
gcloud projects add-iam-policy-binding $GCS_PROJECT_ID \
    --member="serviceAccount:eth-bigdata@$GCS_PROJECT_ID.iam.gserviceaccount.com" \
    --role="roles/resourcemanager.projectIamAdmin"

# Grant storage admin for GCS bucket and objects management
gcloud projects add-iam-policy-binding $GCS_PROJECT_ID \
    --member="serviceAccount:eth-bigdata@$GCS_PROJECT_ID.iam.gserviceaccount.com" \
    --role="roles/storage.admin"

# Download the key
mkdir -p credentials
gcloud iam service-accounts keys create credentials/gcs-key.json \
    --iam-account="eth-bigdata@$GCS_PROJECT_ID.iam.gserviceaccount.com"
```

> If the project already exists, skip `gcloud projects create` and only run `gcloud config set project eth-bigdata-project`.

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

# Verify date partitions were created (dt=YYYY-MM-DD); use your bucket from .env
export GCS_BUCKET="${GCS_BUCKET:-eth-bigdata-project}"
gsutil ls "gs://${GCS_BUCKET}/raw/transactions/" | head -10

# Verify files inside one day partition
gsutil ls "gs://${GCS_BUCKET}/raw/transactions/dt=2025-03-31/" | head -10
```

> Cost: ~~$1 for 180 days of data (~~50-100 GB scanned).

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


| Container             | Port        | Status     |
| --------------------- | ----------- | ---------- |
| eth-zookeeper         | 2181        | healthy    |
| eth-kafka             | 9092, 29092 | healthy    |
| eth-kafka-init        | —           | exited (0) |
| eth-postgres          | 5432        | healthy    |
| eth-crawler           | 8001        | healthy    |
| eth-spark-master      | 7077, 8082  | running    |
| eth-spark-worker      | —           | running    |
| eth-airflow-init      | —           | exited (0) |
| eth-airflow-webserver | 8080        | healthy    |
| eth-airflow-scheduler | —           | running    |
| eth-api               | 8000        | healthy    |
| eth-web               | 3000        | healthy    |


> `kafka-init` and `airflow-init` are one-shot containers — `exited (0)` is correct.

## 6. Create / Reset Airflow Admin Account

If `admin / admin` login does not work (common when volumes already exist), recreate the `admin` user:

```bash
docker compose exec airflow-webserver airflow users delete --username admin

docker compose exec airflow-webserver airflow users create \
    --role Admin \
    --username admin \
    --password admin \
    --firstname Admin \
    --lastname User \
    --email admin@example.com
```

Or create a new admin user:

```bash
docker compose exec airflow-webserver airflow users create \
    --role Admin \
    --username admin2 \
    --password admin2 \
    --firstname Admin \
    --lastname User \
    --email admin2@example.com
```

List users:

```bash
docker compose exec airflow-webserver airflow users list
```

## 7. Verify the Stack

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

## 8. Run Spark Jobs (First Time)

Use this section after raw partitions exist on GCS (Step 4) or synthetic seed data.

**Shell note (zsh):** If you pass `--master local[*]` to `spark-submit`, **quote** it as `'local[*]'` so zsh does not treat `[*]` as a glob.

**Image note:** Spark application code is **baked into the image** at build time (`spark/Dockerfile` copies `/app`). After adding or changing scripts under `spark/`, run `docker compose build spark-master spark-worker` (or copy a single file: `docker compose cp spark/<script>.py spark-master:/app/`).

```bash
# Option A: Full recompute (builds 180-day edge aggregate from scratch)
docker compose exec spark-master spark-submit /app/full_recompute_edges.py \
    --end-date 2025-03-31

# Option B: Process a single day
docker compose exec spark-master spark-submit /app/daily_snapshot.py \
    --target-date 2025-03-31

docker compose exec spark-master spark-submit /app/incremental_edges.py \
    --target-date 2025-03-31

# Option C: Backfill snapshots from all raw GCS partitions (re-upserts Postgres)
docker compose exec spark-master spark-submit /app/daily_snapshot.py --backfill-all

# Option D: Backfill only dates missing from wallet_daily_snapshot
docker compose exec spark-master spark-submit /app/daily_snapshot.py --backfill-missing
```

### Retry PostgreSQL edge load only (Parquet already on GCS)

If step `[6/7]` wrote `processed/graph_edges/run_date=YYYY-MM-DD/` but `[7/7]` JDBC failed, you can reload **without** rescanning raw data (script must exist in the container — rebuild or `docker compose cp`):

```bash
docker compose exec spark-master /opt/bitnami/spark/bin/spark-submit \
    --master 'local[*]' \
    /app/write_graph_edges_to_postgres_only.py \
    --run-date 2025-03-31
```

Use the same `**--run-date**` as the Parquet folder under `processed/graph_edges/`.

### Long-running jobs: progress logs and Spark UI

`full_recompute_edges.py` prints step logs `[1/7]` … `[7/7]` with elapsed seconds (GCS partition probe → Parquet read plan → raw row count → edge aggregation → GCS write → PostgreSQL swap). Save full output with:

```bash
docker compose exec spark-master spark-submit /app/full_recompute_edges.py \
    --end-date 2025-03-31 2>&1 | tee full_recompute.log
```

**Spark Master UI** ([http://localhost:8082](http://localhost:8082)): useful for **standalone cluster** jobs. Jobs started with `spark-submit` in this repo usually run in **local (driver-only)** mode, so you may see **Drivers: 0 Running** on the master UI while the terminal is still busy — that is expected. Rely on the `[n/7]` log lines (and `tee`) as the main progress signal. If you later submit with `--master spark://spark-master:7077`, the same UI can show **Running Applications** and **Stages** for that run.

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

After Spark completes, verify data (see **Verify data on GCS and PostgreSQL** below), or quickly:

```bash
docker compose exec postgres psql -U ethuser -d ethdb \
    -c "SELECT rank, wallet_address, total_txns FROM wallet_daily_snapshot ORDER BY rank LIMIT 5"
```

## Verify data on GCS and PostgreSQL

Use these checks after bootstrap, Spark runs, or when debugging empty APIs.

### GCS (`gsutil`)

Set a variable to match `.env` (optional):

```bash
export GCS_BUCKET="${GCS_BUCKET:-eth-bigdata-project}"
```

**Raw partitions (one folder per day):**

```bash
# List transaction partitions (dates)
gsutil ls "gs://${GCS_BUCKET}/raw/transactions/" | head -20

# Objects under one day
gsutil ls "gs://${GCS_BUCKET}/raw/transactions/dt=2025-03-31/" | head -20

# Approximate size of one raw day
gsutil du -sh "gs://${GCS_BUCKET}/raw/transactions/dt=2025-03-31/"
```

**Processed snapshots (Spark daily_snapshot):**

```bash
gsutil ls "gs://${GCS_BUCKET}/processed/snapshots/" | tail -20
gsutil ls "gs://${GCS_BUCKET}/processed/snapshots/dt=2025-03-31/" | head -10
```

**Processed graph edges (Spark incremental / full recompute):**

```bash
gsutil ls "gs://${GCS_BUCKET}/processed/graph_edges/" | tail -20
gsutil du -sh "gs://${GCS_BUCKET}/processed/graph_edges/run_date=2025-03-31/"
```

**Quick existence check from host without listing everything:**

```bash
gsutil -q stat "gs://${GCS_BUCKET}/raw/transactions/dt=2025-03-31/" && echo "raw partition exists"
```

### PostgreSQL (`psql` in Docker)

```bash
# Tables
docker compose exec postgres psql -U ethuser -d ethdb -c '\dt'

# Snapshot coverage: row count and date range
docker compose exec postgres psql -U ethuser -d ethdb -c "
SELECT COUNT(*) AS rows,
       MIN(snapshot_date) AS first_day,
       MAX(snapshot_date) AS last_day
FROM wallet_daily_snapshot;
"

# Sample top wallets for a date
docker compose exec postgres psql -U ethuser -d ethdb -c "
SELECT rank, wallet_address, total_txns, snapshot_date
FROM wallet_daily_snapshot
WHERE snapshot_date = '2025-03-31'
ORDER BY rank
LIMIT 10;
"

# Graph edges: size and window metadata
docker compose exec postgres psql -U ethuser -d ethdb -c "
SELECT COUNT(*) AS edges,
       MIN(period_start) AS window_start,
       MAX(period_end) AS window_end,
       MAX(updated_at) AS last_refresh
FROM wallet_graph_edge;
"

# Optional: how many edges have NULL endpoint (contract-creation–style aggregates)
docker compose exec postgres psql -U ethuser -d ethdb -c "
SELECT
  COUNT(*) FILTER (WHERE to_wallet IS NULL) AS null_to,
  COUNT(*) FILTER (WHERE from_wallet IS NULL) AS null_from
FROM wallet_graph_edge;
"
```

## 9. Access the Web Interfaces


| Interface                 | URL                                                      | Credentials   |
| ------------------------- | -------------------------------------------------------- | ------------- |
| Web App (Top 100 + Graph) | [http://localhost:3000](http://localhost:3000)           | —             |
| FastAPI Docs (Swagger)    | [http://localhost:8000/docs](http://localhost:8000/docs) | —             |
| Airflow Dashboard         | [http://localhost:8080](http://localhost:8080)           | admin / admin |
| Spark Master UI           | [http://localhost:8082](http://localhost:8082)           | —             |


## 10. Daily Operations

Once running, the pipeline is automated:

- **Crawler** runs continuously, polling ETH blocks every ~12 seconds
- **Airflow** triggers daily at 00:05 UTC:
  1. Validates raw data partition
  2. Runs Spark daily snapshot (top 100)
  3. Runs Spark incremental edge aggregation
  4. Cleans up old edge partitions (>30 days)
- **API + Web** serve the latest data on demand

### Monitor via Airflow

Open [http://localhost:8080](http://localhost:8080) → DAGs → `eth_daily_pipeline` to see run history, task durations, and any failures.

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

Common causes: GCS partition empty for target date, missing JDBC driver, PostgreSQL connection refused, `**to_wallet` / `from_wallet` NULL** vs old NOT NULL schema (run `sql/migrations/001_wallet_graph_edge_nullable_endpoints.sql` or recreate DB from current `sql/init.sql`).

### PostgreSQL logs: checkpoints too frequently / `max_wal_size`

Large Spark JDBC inserts generate a lot of WAL. This stack raises `**max_wal_size`** (and related settings) via the `postgres` service `command` in `docker-compose.yml`. After changing compose, recreate Postgres: `docker compose up -d --force-recreate postgres`. The messages are tuning hints, not hard failures.

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

## Service architecture quick reference

Same flow as **Platform structure and pipeline flow** above, compressed:

```
Browser :3000 → nginx → FastAPI :8000 → PostgreSQL :5432
                                              ↑
ETH RPC → Crawler :8001 → Kafka :9092 → GCS (Parquet)
                                              ↓
                        Airflow :8080 → Spark :7077 → GCS + PostgreSQL
```


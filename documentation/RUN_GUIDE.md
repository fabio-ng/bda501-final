# Run Guide: GNN Ethereum Phishing Detection

Step-by-step instructions to run the full pipeline from training to production.

## Prerequisites

| Tool | Version | Purpose |
|------|---------|---------|
| Python | 3.10+ | All scripts |
| Docker + Docker Compose | 20.10+ / 2.20+ | Local infrastructure stack |
| Kaggle Account | — | Free GPU training |
| Google Cloud SDK (`gcloud`, `gsutil`) | Latest | GCS upload (Phase 4+) |
| Apache Spark | 3.5+ | Streaming pipeline |
| Node.js / npm | — | Not required |

---

## Phase 1–3: Train on Kaggle (Free GPU)

### Option A: Upload notebook to Kaggle (recommended)

1. **Go to** [kaggle.com/kernels](https://www.kaggle.com/kernels) and click **New Notebook**

2. **Add the dataset:**
   - Click **Add Data** (right sidebar)
   - Search: `xblock/ethereum-phishing-transaction-network`
   - Click **Add**

3. **Enable GPU:**
   - Click **Settings** (right sidebar)
   - Set **Accelerator** → `GPU T4 x2` or `GPU P100`
   - Set **Internet** → `On`

4. **Upload the notebook:**
   - Click **File → Import Notebook**
   - Select `gnn_ethereum_phishing_detection.ipynb` from this project

5. **Run all cells** (Shift+Enter through each, or **Run All**)
   - Expected runtime: **1–3 hours** total
   - Cell 2 (feature engineering): ~5–10 min
   - Cell 6 (training loop): ~30–90 min depending on epochs

6. **Download outputs** when complete:
   - Click **Output** tab in the notebook
   - Download all files:
     ```
     graphsage_phishing.pt    (~50 MB)   — trained model
     node_features.npy        (~135 MB)  — node feature matrix
     edge_index.npy           (~50 MB)   — graph edges
     labels.npy               (~12 MB)   — node labels
     node_to_id.pkl           (~100 MB)  — address-to-ID mapping
     test_predictions.csv     (~1 MB)    — test set predictions
     training_curves.png                 — loss/F1 plots
     evaluation_results.png              — ROC/PR/confusion matrix
     ```

### Option B: Run locally (requires GPU)

```bash
# Install dependencies
pip install torch dgl numpy pandas scikit-learn matplotlib seaborn mlflow

# Download dataset manually from Kaggle
kaggle datasets download -d xblock/ethereum-phishing-transaction-network
unzip ethereum-phishing-transaction-network.zip -d data/

# Open and run the notebook
jupyter notebook gnn_ethereum_phishing_detection.ipynb
```

### Verify training succeeded

Check the notebook output for:
```
TEST SET EVALUATION
====================
  F1 Score (phishing):    > 0.85   (target)
  AUC-ROC:                > 0.90
  AUC-PR:                 > 0.80
```

---

## Phase 4: Deploy to Production

### Step 1: Start local infrastructure

```bash
cd pipeline

# Start PostgreSQL, Kafka, Grafana, and API
docker-compose up -d

# Verify all services are running
docker-compose ps
```

Expected output:
```
NAME                     STATUS
eth-phishing-db          running (healthy)
eth-phishing-kafka       running (healthy)
eth-phishing-zookeeper   running
eth-phishing-grafana     running
eth-phishing-api         running
```

### Step 2: Verify database schema

```bash
# Connect to PostgreSQL and check tables
docker exec -it eth-phishing-db psql -U postgres -d eth_phishing -c "\dt"
```

Expected tables:
```
 detection_results
 model_registry
 phishing_alerts
 phishing_labels
```

### Step 3: Place model files

Create directories and copy Kaggle outputs:

```bash
# Create local model/data directories
mkdir -p pipeline/models pipeline/data

# Copy Kaggle outputs
cp /path/to/kaggle/output/graphsage_phishing.pt  pipeline/models/
cp /path/to/kaggle/output/node_features.npy       pipeline/data/
cp /path/to/kaggle/output/edge_index.npy          pipeline/data/
cp /path/to/kaggle/output/labels.npy              pipeline/data/
cp /path/to/kaggle/output/node_to_id.pkl          pipeline/data/

# Restart API to pick up model files
docker-compose restart api
```

### Step 4: Load batch predictions into database

```bash
# Import test predictions from Kaggle
docker exec -i eth-phishing-db psql -U postgres -d eth_phishing -c "
  COPY detection_results(address, phishing_score, predicted_label, source, model_version)
  FROM STDIN WITH CSV HEADER
" < /path/to/kaggle/output/test_predictions.csv
```

Or use the convenience script (requires formatted CSV):

```bash
python -c "
import pandas as pd
import psycopg2

df = pd.read_csv('/path/to/kaggle/output/test_predictions.csv')
conn = psycopg2.connect(host='localhost', port=5432, dbname='eth_phishing', user='postgres', password='postgres')
cur = conn.cursor()

for _, row in df.iterrows():
    cur.execute(
        'INSERT INTO detection_results (address, phishing_score, predicted_label, source, model_version) VALUES (%s, %s, %s, %s, %s)',
        (row['address'], row['phishing_score'], row['predicted_label'], 'batch', 'graphsage_v1')
    )

conn.commit()
cur.close()
conn.close()
print(f'Loaded {len(df)} predictions.')
"
```

### Step 5 (Optional): Upload to GCS

For cloud deployment:

```bash
export GCS_BUCKET=eth-phishing-data
export DB_HOST=<your-cloud-sql-ip>
export DB_USER=postgres
export DB_PASSWORD=<password>

./pipeline/scripts/upload_to_gcs.sh /path/to/kaggle/output/
```

---

## Phase 5: Access Dashboard & API

### Grafana Dashboard

1. Open [http://localhost:3000](http://localhost:3000)
2. Login: `admin` / `admin`
3. Add PostgreSQL data source:
   - **Host**: `postgres:5432`
   - **Database**: `eth_phishing`
   - **User**: `postgres` / **Password**: `postgres`
   - **SSL Mode**: disable
4. Import dashboard:
   - Go to **Dashboards → Import**
   - Upload `pipeline/dashboard/grafana_dashboard.json`
   - Select the PostgreSQL data source

### FastAPI Alert Service

1. Open Swagger docs: [http://localhost:8000/docs](http://localhost:8000/docs)

2. Test endpoints:

```bash
# Health check
curl http://localhost:8000/health

# Get recent alerts (high-risk addresses)
curl "http://localhost:8000/alerts?min_score=0.7&limit=10"

# Look up a specific address
curl http://localhost:8000/alerts/0x1234...abcd

# Get summary stats
curl http://localhost:8000/stats

# Submit analyst feedback
curl -X POST http://localhost:8000/feedback \
  -H "Content-Type: application/json" \
  -d '{"alert_id": 1, "analyst_label": 1}'
```

---

## Phase 4 (continued): Start Streaming Pipeline

### Step 1: Create Kafka topics

```bash
# Create topics (auto-created by docker-compose, but explicit is safer)
docker exec eth-phishing-kafka kafka-topics \
  --create --topic new_transactions \
  --bootstrap-server localhost:9092 \
  --partitions 3 --replication-factor 1

docker exec eth-phishing-kafka kafka-topics \
  --create --topic phishing_alerts \
  --bootstrap-server localhost:9092 \
  --partitions 3 --replication-factor 1

# Verify
docker exec eth-phishing-kafka kafka-topics \
  --list --bootstrap-server localhost:9092
```

### Step 2: Start Kafka producer (Ethereum transaction ingestion)

```bash
# Set your Etherscan API key (free at etherscan.io)
export ETHERSCAN_API_KEY=<your-key>

# Install kafka-python
pip install kafka-python requests

# Start producer (polls new Ethereum blocks every ~15 seconds)
cd pipeline
python streaming/kafka_producer.py
```

You should see:
```
2026-04-04 12:00:00 INFO Starting producer from block 19500001
2026-04-04 12:00:15 INFO Block 19500002: published 142 transactions
2026-04-04 12:00:30 INFO Block 19500003: published 98 transactions
```

### Step 3: Start Spark Streaming inference

```bash
# In a new terminal
cd pipeline

# Set environment variables
export GRAPH_DATA_DIR=$(pwd)/data
export MODEL_LOCAL_PATH=$(pwd)/models/graphsage_phishing.pt

# Submit Spark job
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.7.1 \
  streaming/spark_streaming.py
```

You should see:
```
Spark session created. Starting streaming pipeline...
Streaming query started. Waiting for termination...
Processing batch 0: 142 transactions
Batch 0: 87 addresses scored, 2 ALERTS (score >= 0.7)
```

### Step 4: Monitor alerts

```bash
# Watch alerts in real-time via Kafka consumer
docker exec eth-phishing-kafka kafka-console-consumer \
  --topic phishing_alerts \
  --bootstrap-server localhost:9092 \
  --from-beginning

# Or via the API
curl "http://localhost:8000/alerts?min_score=0.7"
```

---

## Phase 6: Retraining (Airflow)

### Option A: Manual retraining

1. Retrain on Kaggle with updated labels
2. Download new model
3. Replace model file:

```bash
cp new_graphsage_phishing.pt pipeline/models/graphsage_phishing.pt
docker-compose restart api
# Spark streaming auto-reloads model every 24h (or restart it)
```

### Option B: Automated retraining with Airflow

```bash
# Install Airflow (if not already)
pip install apache-airflow apache-airflow-providers-google apache-airflow-providers-postgres kaggle

# Copy DAG file
cp pipeline/airflow/retrain_dag.py ~/airflow/dags/

# Configure Airflow connections
airflow connections add eth_phishing_db \
  --conn-type postgres \
  --conn-host localhost \
  --conn-port 5432 \
  --conn-schema eth_phishing \
  --conn-login postgres \
  --conn-password postgres

# Start Airflow
airflow standalone

# The DAG runs weekly (Sunday 2:00 AM UTC)
# To trigger manually:
airflow dags trigger eth_phishing_retrain
```

---

## Stopping Everything

```bash
# Stop all Docker services
cd pipeline
docker-compose down

# Stop with volume cleanup (deletes database data)
docker-compose down -v

# Stop Kafka producer: Ctrl+C in its terminal
# Stop Spark streaming: Ctrl+C in its terminal
```

---

## Troubleshooting

| Problem | Solution |
|---------|----------|
| Kaggle notebook OOM | Reduce graph size: sample 500K nodes instead of full 2.97M |
| Kaggle GPU unavailable | Switch to P100, or wait and retry (GPU allocation is shared) |
| Docker `port already in use` | Change ports in `docker-compose.yml` or stop conflicting services |
| Kafka producer `ConnectionError` | Ensure Kafka container is healthy: `docker-compose ps` |
| Spark `ClassNotFoundException: kafka` | Add `--packages` flag with Kafka connector JAR |
| API returns `500` | Check model file exists at `pipeline/models/graphsage_phishing.pt` |
| Grafana shows no data | Verify PostgreSQL data source connection and that predictions are loaded |
| Low F1 score (< 0.7) | Check class weights, try focal loss, increase epochs, verify feature normalization |

---

## Project File Map

```
exercise2/
├── gnn_ethereum_phishing_detection.ipynb   ← Kaggle notebook (Phases 1-3)
├── gnn_ethereum_phishing_detection.md      ← System design document
├── implementation_plan.md                  ← Full implementation plan
├── architecture_diagram.svg               ← System architecture diagram
├── RUN_GUIDE.md                           ← This file
│
└── pipeline/                              ← Production code (Phases 4-6)
    ├── config.py                          ← Shared configuration
    ├── docker-compose.yml                 ← Local infrastructure stack
    │
    ├── sql/
    │   └── schema.sql                     ← PostgreSQL tables + views
    │
    ├── scripts/
    │   └── upload_to_gcs.sh               ← Upload Kaggle → GCS
    │
    ├── inference/
    │   └── inference_service.py           ← GNN model loading + scoring
    │
    ├── streaming/
    │   ├── kafka_producer.py              ← Ethereum tx → Kafka
    │   └── spark_streaming.py             ← Kafka → inference → alerts
    │
    ├── api/
    │   ├── app.py                         ← FastAPI REST endpoints
    │   ├── requirements.txt
    │   └── Dockerfile
    │
    ├── dashboard/
    │   └── grafana_dashboard.json         ← Grafana 10-panel dashboard
    │
    └── airflow/
        └── retrain_dag.py                 ← Weekly retraining DAG
```

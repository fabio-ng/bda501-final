# Ethereum Phishing Detection ETL Pipeline

Complete end-to-end ETL pipeline for the BDA501 Final Project.

## Directory Structure

```
etl/
├── __init__.py                 # Package init
├── requirements.txt            # Python dependencies
├── run_pipeline.py             # Pipeline orchestrator
│
├── utils/                      # Utility modules
│   ├── __init__.py
│   ├── spark_session.py        # Spark configuration + GCS setup
│   ├── bq_utils.py             # BigQuery client helpers
│   └── gcs_utils.py            # GCS client helpers
│
├── schemas/                    # Data schema definitions
│   ├── __init__.py
│   ├── transaction_schema.py   # Transaction data schemas (Spark, Pydantic, SQL)
│   └── feature_schema.py       # Address feature schemas
│
└── jobs/                       # ETL jobs
    ├── __init__.py
    ├── ingest_bigquery.py      # BigQuery → GCS raw
    ├── ingest_xblock.py        # XBlock CSV → GCS raw
    ├── process_features.py     # Raw transactions → Address features
    └── export_predictions.py   # Feature vectors → Batch predictions

configs/
└── config.yaml                 # Pipeline configuration
```

## Pipeline Overview

The pipeline runs 4 jobs in sequence:

### 1. Ingest XBlock Data (Mandatory)
**Job:** `IngestXBlockJob` (`etl/jobs/ingest_xblock.py`)

Reads labeled phishing transaction network from local CSV files and uploads to GCS.

**Source:** Local files (Kaggle xblock/ethereum-phishing-transaction-network)
- `phishing_txs.csv` / `transactions.csv` — transaction edges
- `phishing_addrs.txt` / `labels.csv` — labeled addresses

**Target:**
- `gs://eth-phishing-raw/xblock/transactions/transactions.parquet`
- `gs://eth-phishing-raw/xblock/labels/labels.parquet`

**Usage:**
```bash
python -m etl.jobs.ingest_xblock \
  --data-dir /path/to/xblock/data \
  --bucket eth-phishing-raw \
  --credentials /path/to/service-account.json
```

### 2. Ingest BigQuery Data (Optional)
**Job:** `IngestBigQueryJob` (`etl/jobs/ingest_bigquery.py`)

Fetches historical Ethereum transactions from BigQuery public dataset and exports to GCS.

**Source:** `bigquery-public-data.crypto_ethereum`
- transactions table (with optional token_transfers)

**Target:**
- `gs://eth-phishing-raw/transactions/bigquery/year=YYYY/month=MM/*.parquet`
- `gs://eth-phishing-raw/transfers/bigquery/year=YYYY/month=MM/*.parquet` (optional)

**Features:**
- Filters by date range (start_date, end_date)
- Cost estimation via dry-run
- Direct GCS export (avoids memory overload)
- Job logging to `etl_job_log.json`

**Usage:**
```bash
python -m etl.jobs.ingest_bigquery \
  --start-date 2020-01-01 \
  --end-date 2020-01-31 \
  --project my-gcp-project \
  --bucket eth-phishing-raw \
  --credentials /path/to/service-account.json \
  --include-transfers
```

### 3. Process Features (Mandatory)
**Job:** `ProcessFeaturesJob` (`etl/jobs/process_features.py`)

Computes 12-dimensional address-level feature vectors from raw transactions.

**Source:**
- `gs://eth-phishing-raw/xblock/transactions/` (or BigQuery)
- `gs://eth-phishing-raw/xblock/labels/`

**Target:**
- `gs://eth-phishing-processed/features/node_features.parquet`
- `gs://eth-phishing-processed/features/node_features.npy` (numpy array)
- `gs://eth-phishing-processed/features/edge_index.parquet`
- `gs://eth-phishing-processed/features/edge_index.npy`
- `gs://eth-phishing-processed/features/labels.parquet`
- `gs://eth-phishing-processed/features/labels.npy`
- `gs://eth-phishing-processed/features/node_to_id.json` (address → node ID mapping)
- `gs://eth-phishing-processed/features/scaler.pkl` (StandardScaler for normalization)

**12 Computed Features per Address:**

| Feature | Definition |
|---------|-----------|
| `in_degree` | Count of incoming transactions |
| `out_degree` | Count of outgoing transactions |
| `total_eth_received` | Sum of ETH received (wei → ETH) |
| `total_eth_sent` | Sum of ETH sent |
| `avg_tx_value_in` | Average incoming transaction value |
| `avg_tx_value_out` | Average outgoing transaction value |
| `max_tx_value` | Maximum transaction value |
| `unique_in_neighbors` | Count of unique senders |
| `unique_out_neighbors` | Count of unique receivers |
| `account_lifetime` | Time from first to last transaction (seconds) |
| `failed_tx_ratio` | Fraction of failed transactions (receipt_status=0) |
| `avg_gas_used` | Mean gas per transaction |

**Features are normalized** using StandardScaler.

**Usage:**
```bash
python -m etl.jobs.process_features \
  --raw-bucket eth-phishing-raw \
  --processed-bucket eth-phishing-processed \
  --source xblock \
  --credentials /path/to/service-account.json
```

### 4. Export Predictions (Optional)
**Job:** `ExportPredictionsJob` (`etl/jobs/export_predictions.py`)

Runs batch inference on all addresses and exports predictions.

**Source:**
- `gs://eth-phishing-processed/features/`

**Target:**
- `gs://eth-phishing-processed/predictions/predictions.parquet`
- `gs://eth-phishing-processed/predictions/predictions.csv`
- `gs://eth-phishing-processed/predictions/top_phishing_addresses.csv`

**Predictor Types:**
- `mock` — random probabilities (for testing)
- `rf` — Random Forest (requires trained model)
- `gbm` — Gradient Boosting Machine
- `nn` — Neural Network (TensorFlow)

**Usage:**
```bash
python -m etl.jobs.export_predictions \
  --processed-bucket eth-phishing-processed \
  --predictor mock \
  --credentials /path/to/service-account.json
```

---

## Running the Pipeline

### Option 1: Full Pipeline Orchestrator

```bash
# Install dependencies
pip install -r etl/requirements.txt

# Set up credentials (if not already set)
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/service-account.json

# Run full pipeline (with defaults from config.yaml)
python -m etl.run_pipeline

# Dry run (plan without executing)
python -m etl.run_pipeline --dry-run

# Skip optional jobs
python -m etl.run_pipeline --skip-bigquery --skip-predictions

# Use custom config
python -m etl.run_pipeline --config configs/config.yaml
```

### Option 2: Individual Jobs

```bash
# XBlock only
python -m etl.jobs.ingest_xblock --data-dir /path/to/xblock --bucket eth-phishing-raw

# BigQuery only
python -m etl.jobs.ingest_bigquery --start-date 2020-01-01 --end-date 2020-01-31 \
  --project my-project --bucket eth-phishing-raw

# Feature processing
python -m etl.jobs.process_features --raw-bucket eth-phishing-raw \
  --processed-bucket eth-phishing-processed --source xblock

# Predictions
python -m etl.jobs.export_predictions --processed-bucket eth-phishing-processed \
  --predictor mock
```

---

## Configuration

Edit `configs/config.yaml` to customize pipeline behavior:

```yaml
# GCS Buckets
gcs_bucket_raw: eth-phishing-raw
gcs_bucket_processed: eth-phishing-processed

# GCP Project
gcp_project: eth-phishing-project
credentials_path: null  # Uses GOOGLE_APPLICATION_CREDENTIALS env var

# XBlock (mandatory)
xblock_data_dir: ./xblock_data

# BigQuery (optional)
bigquery_enabled: false
bigquery_start_date: "2020-01-01"
bigquery_end_date: "2020-12-31"
bigquery_limit: null
bigquery_include_transfers: false

# Features
feature_source: xblock  # 'xblock', 'bigquery', or 'both'
use_spark: false

# Predictions
predictions_enabled: false
model_path: null
predictor_type: mock
```

---

## Data Schemas

### Transaction Schema
```python
EthTransaction(
    tx_hash: str,
    from_address: str,
    to_address: str,
    value: int,              # in wei
    gas: int,
    gas_price: int,
    block_number: int,
    block_timestamp: str,
    receipt_status: Optional[int],  # 0=failed, 1=success
    input: Optional[str]
)
```

### Address Features Schema
```python
AddressFeatures(
    address: str,
    in_degree: int,
    out_degree: int,
    total_eth_received: float,
    total_eth_sent: float,
    avg_tx_value_in: float,
    avg_tx_value_out: float,
    max_tx_value: float,
    unique_in_neighbors: int,
    unique_out_neighbors: int,
    account_lifetime: float,
    failed_tx_ratio: float,
    avg_gas_used: float,
    label: Optional[int]  # 1=phishing, 0=legitimate, None=unknown
)
```

---

## GCS Utilities

### GCSClient

```python
from etl.utils.gcs_utils import GCSClient

gcs = GCSClient("my-bucket")

# Upload file
gcs.upload_file("/local/path", "gcs/path")

# Download file
gcs.download_file("gcs/path", "/local/path")

# Upload DataFrame as Parquet
gcs.upload_dataframe_as_parquet(df, "data/file.parquet")

# Read Parquet
df = gcs.read_parquet_from_gcs("data/file.parquet")

# List blobs
blobs = gcs.list_blobs("prefix/")

# Check existence
exists = gcs.blob_exists("path")
```

---

## BigQuery Utilities

### BQClient

```python
from etl.utils.bq_utils import BQClient

bq = BQClient("my-project")

# Query to DataFrame
df = bq.query_to_dataframe("SELECT * FROM table LIMIT 100")

# Query to GCS (direct export)
bq.query_to_gcs(
    "SELECT * FROM transactions",
    "gs://bucket/path/*.parquet",
    format="PARQUET"
)

# Get table schema
schema = bq.get_table_schema("dataset", "table")

# Estimate query cost
bytes = bq.estimate_query_cost("SELECT COUNT(*) FROM table")
```

---

## Spark Configuration

### SparkSession Factory

```python
from etl.utils.spark_session import create_spark_session, stop_spark

spark = create_spark_session(
    app_name="ETL",
    config={"spark.sql.shuffle.partitions": "200"},
    credentials_path="/path/to/service-account.json"
)

# GCS is automatically configured
spark.read.parquet("gs://bucket/path/*.parquet")

stop_spark(spark)
```

---

## Error Handling & Logging

All jobs use Python's standard `logging` module. Configure logging level:

```bash
# Set via environment variable
export LOGLEVEL=DEBUG
python -m etl.run_pipeline

# Or programmatically
import logging
logging.basicConfig(level=logging.DEBUG)
```

All job execution is logged to `gs://eth-phishing-raw/etl_job_log.json` for auditing.

---

## Performance Notes

### Data Volume
- XBlock: ~2M transactions, ~500K unique addresses
- BigQuery: Billions of transactions (date-range filtered)

### Processing Time
- XBlock ingestion: ~1-2 minutes
- BigQuery ingestion: 5-30 minutes (depends on date range)
- Feature processing: 5-10 minutes
- Predictions: 1-2 minutes

### Memory Requirements
- XBlock only: ~2GB RAM
- Full pipeline (with BigQuery): 8-16GB RAM recommended
- Use Spark for large BigQuery exports

---

## Dependencies

See `etl/requirements.txt`:
- google-cloud-bigquery
- google-cloud-storage
- google-cloud-bigquery-storage
- pandas
- pyarrow
- numpy
- scikit-learn
- pydantic
- pyyaml
- python-dotenv
- tqdm
- pyspark
- kagglehub

---

## Testing & Validation

### Validate XBlock CSV files
```python
from etl.jobs.ingest_xblock import IngestXBlockJob
job = IngestXBlockJob("bucket", "./data")
# Checks column names, address format, deduplication
```

### Estimate BigQuery query cost
```python
from etl.jobs.ingest_bigquery import IngestBigQueryJob
job = IngestBigQueryJob("project", "bucket")
bytes = job.estimate_query_cost(sql)
# Dry run query to estimate bytes scanned
```

### Verify feature computation
```python
from etl.jobs.process_features import ProcessFeaturesJob
job = ProcessFeaturesJob("raw-bucket", "processed-bucket")
# Feature shapes, normalization validation
```

---

## Next Steps

1. **Download XBlock data** from Kaggle:
   ```bash
   kagglehub dataset download xblock/ethereum-phishing-transaction-network
   ```

2. **Set up GCS buckets** (if not already created):
   ```bash
   gsutil mb gs://eth-phishing-raw/
   gsutil mb gs://eth-phishing-processed/
   ```

3. **Configure credentials:**
   ```bash
   export GOOGLE_APPLICATION_CREDENTIALS=/path/to/service-account.json
   ```

4. **Run the pipeline:**
   ```bash
   python -m etl.run_pipeline
   ```

5. **Train models** on processed features (next phase)

---

**Pipeline Status:** Complete and ready for production
**Last Updated:** 2026-04-09

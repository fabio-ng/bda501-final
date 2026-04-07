# Implementation Plan: GNN-Based Ethereum Phishing Detection

## Overview

This plan breaks the system into **6 phases**, ordered by dependency. **Phases 1–3 run entirely on Kaggle Notebooks** using the XBlock-ETH dataset and free GPU acceleration — no cloud infrastructure needed for the training MVP. Phases 4–5 add **streaming and dashboard** on GCP. Phase 6 covers **hardening and optimization**.

### Why Kaggle for Training?

| Advantage | Detail |
|-----------|--------|
| **Zero cost** | Free NVIDIA T4 GPU (16 GB VRAM), 30 hrs/week |
| **Dataset co-location** | XBlock-ETH is hosted on Kaggle — no download/upload needed |
| **Pre-installed libraries** | PyTorch, DGL, scikit-learn, pandas already available |
| **Reproducibility** | Notebooks are versioned and shareable |
| **Scale fit** | ~2.97M nodes / ~13.55M edges fits comfortably in Kaggle's 16 GB RAM + T4 GPU |

---

## Phase 1: Dataset Setup on Kaggle

**Goal:** Load the XBlock-ETH dataset and perform initial exploration in a Kaggle Notebook.

| # | Task | Tool | Details |
|---|------|------|---------|
| 1.1 | Create Kaggle Notebook | Kaggle | New notebook, enable GPU accelerator (NVIDIA T4), set Internet to ON |
| 1.2 | Add XBlock-ETH dataset | Kaggle | Add dataset `xblock/ethereum-phishing-transaction-network` to notebook inputs — data appears at `/kaggle/input/ethereum-phishing-transaction-network/` |
| 1.3 | Install extra libraries | `!pip install` | `dgl`, `mlflow`, `torch-geometric` (if needed), `scikit-learn` (already installed) |
| 1.4 | Explore dataset structure | pandas | Load CSV files, inspect columns, row counts, label distribution |
| 1.5 | Verify dataset stats | pandas | Confirm: ~2.97M unique addresses, ~13.55M edges, ~1,165 phishing labels |
| 1.6 | EDA notebook | matplotlib/seaborn | Plot: label distribution, degree distribution, transaction value distribution, phishing vs. legitimate feature comparison |

**Input:** Kaggle dataset `xblock/ethereum-phishing-transaction-network`
**Output:** Loaded DataFrames, EDA visualizations, confirmed dataset statistics
**Done when:** Dataset loads without errors, label count matches expected ~1,165 phishing addresses

```python
# Quick start in Kaggle Notebook
import pandas as pd
import os

data_dir = "/kaggle/input/ethereum-phishing-transaction-network/"
print(os.listdir(data_dir))  # See available files

# Load edge list and labels
edges_df = pd.read_csv(f"{data_dir}/...")   # transaction edges
labels_df = pd.read_csv(f"{data_dir}/...")  # phishing labels
print(f"Edges: {len(edges_df):,}, Labels: {len(labels_df):,}")
```

---

## Phase 2: Data Preprocessing & Graph Construction (Kaggle Notebook)

**Goal:** Clean data and build the transaction graph with node/edge features — all in-memory on Kaggle.

| # | Task | Tool | Details |
|---|------|------|---------|
| 2.1 | Data cleaning | pandas | Normalize addresses to lowercase, drop duplicates, handle missing values |
| 2.2 | Label joining | pandas `merge` | Left-join edge list with phishing labels on `from_address` and `to_address` to propagate known labels to nodes |
| 2.3 | Node feature engineering | pandas `groupby` | Compute 12 node features: `in_degree`, `out_degree`, `total_eth_received`, `total_eth_sent`, `avg_tx_value_in`, `avg_tx_value_out`, `max_tx_value`, `unique_in_neighbors`, `unique_out_neighbors`, `account_lifetime`, `failed_tx_ratio`, `avg_gas_used` |
| 2.4 | Edge feature aggregation | pandas `groupby` | Aggregate multi-edges between same address pair: `edge_count`, `total_value`, `avg_value`, `time_span` |
| 2.5 | Node ID mapping | pandas | Create address → integer `node_id` mapping via `factorize()` or dictionary |
| 2.6 | Build edge index | numpy | Map `from_address` / `to_address` to `(src_id, dst_id)` integer pairs |
| 2.7 | Feature normalization | scikit-learn | `StandardScaler` on all 12 node features to zero-mean, unit-variance |
| 2.8 | Save processed data | Kaggle output | Save `node_features.npy`, `edge_index.npy`, `labels.npy` to `/kaggle/working/` for reuse |

**Input:** Raw CSVs from XBlock-ETH dataset
**Output:** `node_features.npy` (N×12), `edge_index.npy` (2×E), `labels.npy` (N,)
**Done when:** Node count matches unique address count; edge index contains no out-of-bounds IDs

### Key validation checks
- No duplicate `node_id` values
- Edge index `src_id` and `dst_id` all within `[0, num_nodes)`
- Feature distributions are reasonable (no NaN after normalization)
- Label array: confirm ~1,165 positives, rest either 0 or -1 (unlabeled)

```python
# Example: Node feature engineering
node_in = edges_df.groupby("to_address").agg(
    in_degree=("value", "count"),
    total_eth_received=("value", "sum"),
    avg_tx_value_in=("value", "mean"),
    unique_in_neighbors=("from_address", "nunique"),
)
node_out = edges_df.groupby("from_address").agg(
    out_degree=("value", "count"),
    total_eth_sent=("value", "sum"),
    avg_tx_value_out=("value", "mean"),
    unique_out_neighbors=("to_address", "nunique"),
)
node_features = node_in.join(node_out, how="outer").fillna(0)
```

---

## Phase 3: GNN Model Training & Evaluation (Kaggle Notebook — GPU)

**Goal:** Train GraphSAGE on Kaggle's free T4 GPU and evaluate performance.

| # | Task | Tool | Details |
|---|------|------|---------|
| 3.1 | Construct DGL graph | DGL | `dgl.graph((src_ids, dst_ids))`, attach `ndata["feat"]` (N×12 float tensor) and `ndata["label"]` |
| 3.2 | Train/val/test split | scikit-learn | `train_test_split` on labeled node indices only: 60/20/20, stratified by label |
| 3.3 | Define GraphSAGE model | PyTorch + DGL | 2 GraphSAGE layers, hidden_dim=128, mean aggregator, ReLU, dropout=0.5, 2-class softmax output |
| 3.4 | Configure mini-batch sampler | DGL | `NeighborSampler([15, 10])`, `NodeDataLoader` with batch_size=1024 |
| 3.5 | Handle class imbalance | PyTorch | Compute class weights (~1:100), apply weighted cross-entropy loss; optionally implement focal loss |
| 3.6 | Training loop | PyTorch | Adam optimizer (lr=0.001, weight_decay=5e-4), epochs=50–100, early stopping on validation F1 (patience=10) |
| 3.7 | Evaluate on test set | scikit-learn | Compute Precision, Recall, F1, AUC-ROC, AUC-PR on held-out test nodes |
| 3.8 | Visualize results | matplotlib | Plot: training loss curve, ROC curve, PR curve, confusion matrix |
| 3.9 | Save trained model | PyTorch | `torch.save(model.state_dict(), "/kaggle/working/graphsage_phishing.pt")` — download from Kaggle output |
| 3.10 | Batch inference | DGL | Run forward pass on all ~2.97M nodes, save predictions CSV to `/kaggle/working/predictions.csv` |

**Input:** Processed numpy arrays from Phase 2
**Output:** Trained model `.pt` file, predictions CSV, evaluation metrics
**Done when:** Test F1 > 0.85 (target), model and predictions saved to Kaggle output

### Kaggle GPU training infrastructure
- **GPU**: NVIDIA T4 (16 GB VRAM) — free, 30 hrs/week quota
- **RAM**: 16 GB system memory — sufficient for ~2.97M node graph
- **Mini-batch sampling**: Keeps GPU memory usage bounded (batch_size=1024 nodes with 2-hop sampling ≈ 150K–200K nodes per subgraph, ~2–4 GB VRAM)
- **Session limit**: Kaggle notebooks auto-stop after 12 hours — training should complete well within this (typically 1–3 hours for 50–100 epochs)
- **Fallback**: If T4 is unavailable, Kaggle also offers P100 (16 GB) as an alternative GPU

### Kaggle Notebook structure (recommended cells)

```
Cell 1:  Install dependencies (!pip install dgl)
Cell 2:  Load dataset from /kaggle/input/
Cell 3:  EDA + visualizations
Cell 4:  Preprocessing + feature engineering
Cell 5:  Build DGL graph
Cell 6:  Train/val/test split
Cell 7:  Define GraphSAGE model
Cell 8:  Training loop with validation
Cell 9:  Evaluation metrics + plots
Cell 10: Save model + predictions
```

```python
# Example: DGL graph construction + training on Kaggle GPU
import dgl
import torch
import torch.nn.functional as F
import numpy as np

# Load processed data
node_features = np.load("/kaggle/working/node_features.npy")
edge_index = np.load("/kaggle/working/edge_index.npy")
labels = np.load("/kaggle/working/labels.npy")

# Build graph
graph = dgl.graph((edge_index[0], edge_index[1]))
graph.ndata["feat"] = torch.tensor(node_features, dtype=torch.float32)
graph.ndata["label"] = torch.tensor(labels, dtype=torch.long)

# Move to GPU
device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
print(f"Training on: {device}")  # Should print "cuda"

# Train (see Phase 3 tasks for full loop)
```

---

## Phase 4: Model Export & Streaming Pipeline (GCP)

**Goal:** Export trained model from Kaggle to GCP, then set up real-time phishing detection.

| # | Task | Tool | Details |
|---|------|------|---------|
| 4.1 | Download model from Kaggle | Kaggle output | Download `graphsage_phishing.pt`, `node_features.npy`, `edge_index.npy`, `predictions.csv` from notebook output |
| 4.2 | Upload to GCS | `gsutil` | Upload model to `gs://eth-phishing-data/models/`, processed graph data to `gs://eth-phishing-data/processed/` |
| 4.3 | Set up GCP infrastructure | GCP Console / Terraform | Create GCS buckets, Cloud SQL (PostgreSQL), enable Dataproc APIs |
| 4.4 | Load batch predictions to Cloud SQL | Python / `psql` | Import `predictions.csv` into `detection_results` table |
| 4.5 | Deploy Kafka cluster | Confluent Cloud / self-managed | Create topics: `new_transactions`, `phishing_alerts` |
| 4.6 | Build Kafka producer | Python | Subscribe to Ethereum node WebSocket or poll Etherscan API for new blocks, publish transactions to `new_transactions` topic |
| 4.7 | Spark Structured Streaming consumer | PySpark | Read from `new_transactions`, parse transaction fields |
| 4.8 | Feature enrichment | PySpark | For each new address: query stored node feature table, merge with new transaction to update features incrementally |
| 4.9 | Subgraph extraction | PySpark + DGL | Extract 2-hop neighborhood of target address from stored edge table |
| 4.10 | GNN inference service | Python / DGL | Load model from GCS, run forward pass on local subgraph, output phishing score |
| 4.11 | Alerting | PySpark | If score > 0.7 threshold: publish to `phishing_alerts` Kafka topic, write to Cloud SQL `detection_results` |
| 4.12 | Model refresh mechanism | Cron / Airflow | Retrain on Kaggle → upload new `.pt` to GCS → inference service reloads |

**Input:** Kaggle model output + live Ethereum transactions via Kafka
**Output:** Model deployed on GCS, alerts on `phishing_alerts` topic, results in Cloud SQL
**Done when:** Model loads from GCS; end-to-end streaming latency < 30 seconds

### Streaming considerations
- **Checkpoint**: Spark Structured Streaming checkpoints to GCS for fault tolerance
- **Backpressure**: Configure Kafka consumer `maxOffsetsPerTrigger` to handle bursts
- **Cold addresses**: New addresses with no history get default feature values; model still classifies via neighborhood structure
- **Retraining cycle**: Retrain on Kaggle with updated data → download `.pt` → upload to GCS → inference service picks up new model

---

## Phase 5: Output & Dashboard

**Goal:** Visualize results and expose alerts to downstream consumers.

| # | Task | Tool | Details |
|---|------|------|---------|
| 5.1 | Set up Grafana | Grafana (Docker / GCE) | Connect to Cloud SQL PostgreSQL as data source |
| 5.2 | Phishing detection dashboard | Grafana | Panels: detected addresses table (sorted by risk score), time-series of daily detection count, model F1/Precision/Recall gauges |
| 5.3 | Graph visualization panel | Grafana + plugin / custom | Visualize transaction subgraph around flagged addresses (e.g., using Node Graph panel or external tool like Cytoscape.js) |
| 5.4 | Alert REST API | Flask / FastAPI | Endpoint `GET /alerts?since=<timestamp>` reads from `detection_results` table; `POST /feedback` for analysts to confirm/reject predictions |
| 5.5 | Downstream integration | Kafka consumer | Wallets/exchanges consume from `phishing_alerts` topic to block suspicious addresses |

**Input:** Cloud SQL `detection_results`, Kafka `phishing_alerts`
**Output:** Live dashboard, REST API, Kafka consumer integration
**Done when:** Dashboard shows real-time updates; API returns valid responses

---

## Phase 6: Scalability, Optimization & Hardening

**Goal:** Make the system production-ready and resilient at scale.

| # | Task | Details |
|---|------|---------|
| 6.1 | Delta Lake optimization | Run `OPTIMIZE` and `Z-ORDER` on `from_address`, `to_address` columns for faster lookups during streaming enrichment |
| 6.2 | Spark caching | Persist node feature DataFrame in memory (`MEMORY_AND_DISK`) for reuse across streaming micro-batches |
| 6.3 | Incremental batch processing | Modify batch ETL to only process new blocks since last run (`block_number > last_processed`), merge incrementally into existing Delta tables |
| 6.4 | Model quantization | Quantize trained GraphSAGE model (INT8) for faster inference in the streaming pipeline |
| 6.5 | Graph pruning policy | Define TTL: remove nodes/edges with no activity in last N months to keep working graph manageable |
| 6.6 | 10x scale test | Increase Spark executors, Kafka partitions, and GNN batch size; verify system handles 10x data volume |
| 6.7 | Monitoring & alerting | Set up Stackdriver / Prometheus alerts on: Spark job failures, Kafka consumer lag, inference latency > 30s, model drift (F1 degradation) |
| 6.8 | Periodic model retraining | Airflow DAG: weekly trigger → batch ETL → retrain GNN → evaluate → promote model if F1 improves |
| 6.9 | Active learning loop | Route low-confidence predictions (score 0.3–0.7) to analyst review queue; feed confirmed labels back into Cloud SQL for next training cycle |

---

## Dependency Graph

```
        ┌─────────────────────────────────────────┐
        │            KAGGLE (Free GPU)             │
        │                                          │
        │  Phase 1 (Dataset + EDA)                 │
        │      │                                   │
        │      ▼                                   │
        │  Phase 2 (Preprocessing + Graph)         │
        │      │                                   │
        │      ▼                                   │
        │  Phase 3 (GNN Training + Evaluation)     │
        │      │                                   │
        └──────┼───────────────────────────────────┘
               │  Export model (.pt) + predictions
               ▼
        ┌─────────────────────────────────────────┐
        │         GCP (Production Deploy)          │
        │                                          │
        │  Phase 4 (Model Deploy + Streaming)      │
        │      │                                   │
        │      ▼                                   │
        │  Phase 5 (Dashboard + API)               │
        │      │                                   │
        │      ▼                                   │
        │  Phase 6 (Optimization + Retraining)     │
        │                                          │
        └─────────────────────────────────────────┘
```

- **Phases 1–3 are self-contained on Kaggle** — no cloud setup needed, can start immediately
- **Phase 4** bridges Kaggle → GCP by uploading the trained model and setting up production infrastructure
- **Retraining loop**: Phase 6 feeds back to Kaggle — retrain with new data, re-upload model to GCS

---

## Technology Stack Summary

| Layer | Technology | Phase | Purpose |
|-------|-----------|-------|---------|
| **Training Platform** | **Kaggle Notebooks (T4 GPU)** | **1–3** | **Free GPU for dataset loading, preprocessing, GNN training** |
| Dataset | XBlock-ETH (Kaggle) | 1 | Pre-hosted Ethereum phishing transaction network |
| Preprocessing | pandas + numpy + scikit-learn | 2 | Feature engineering, normalization (fits in 16 GB RAM) |
| GNN Framework | DGL + PyTorch | 2–3 | Graph construction, GraphSAGE training, inference |
| Cloud Platform | Google Cloud Platform | 4–6 | Production hosting |
| Object Storage | Google Cloud Storage | 4–6 | Model artifacts, processed graph data |
| Relational DB | Cloud SQL (PostgreSQL) | 4–6 | Detection results, audit log |
| Stream Broker | Apache Kafka | 4 | Real-time transaction ingestion + alerts |
| Stream Processing | Apache Spark (Dataproc) | 4 | Streaming inference pipeline |
| Dashboard | Grafana | 5 | Visualization of results and metrics |
| API | FastAPI | 5 | Alert REST endpoint |
| Orchestration | Apache Airflow (optional) | 6 | Scheduling retraining + model upload |

---

## Risk Mitigation

| Risk | Impact | Mitigation |
|------|--------|------------|
| Kaggle GPU quota exhausted (30 hrs/week) | Phase 3 delayed | Training takes ~1–3 hrs; schedule runs early in the week. Fallback: use Google Colab Pro |
| Kaggle session timeout (12 hr limit) | Training interrupted | Save checkpoints every 5 epochs to `/kaggle/working/`; resume from checkpoint in new session |
| Class imbalance hurts recall | Phase 3 poor model | Weighted loss + focal loss + oversampling; monitor AUC-PR specifically |
| Graph too large for Kaggle RAM (16 GB) | Phase 2 OOM | XBlock-ETH (~2.97M nodes) fits; if supplementing with more data, downsample or switch to Colab Pro (25 GB) |
| Subgraph extraction too slow for streaming | Phase 4 latency miss | Pre-compute and cache 2-hop neighborhoods; Z-ORDER on address columns |
| Model drift over time | Accuracy degrades | Periodic retraining on Kaggle + F1 monitoring + Airflow automation (Phase 6) |

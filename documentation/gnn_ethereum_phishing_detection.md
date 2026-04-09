# GNN-Based Ethereum Phishing Detection on Big Data Infrastructure

## 1. Problem Formalization

### Problem Statement

Ethereum phishing attacks are a growing threat in the blockchain ecosystem. Malicious accounts trick users into sending funds to fraudulent addresses through fake websites, social engineering, and deceptive smart contracts. Detecting these phishing accounts is critical to protecting users and maintaining trust in the Ethereum network.

This system uses **Graph Neural Networks (GNNs)** to classify Ethereum accounts as **phishing** or **legitimate** by analyzing the transaction graph structure, leveraging the insight that phishing accounts exhibit distinct topological and behavioral patterns in the transaction network.

### Why This Is a Big Data Problem

| Dimension | Scale | Why Traditional Tools Fail |
|-----------|-------|--------------------------|
| **Volume** | ~2 billion+ transactions, ~300 million+ unique addresses, ~1 TB+ raw data | Cannot fit in a single machine's memory; requires distributed storage (GCS/HDFS) and processing (Spark) |
| **Velocity** | ~15 new blocks/minute, ~150–200 transactions/block (~200K tx/day) | Requires streaming ingestion (Kafka) and near real-time inference to detect phishing before funds are moved |
| **Variety** | Transaction records, smart contract interactions, token transfers, labeled phishing lists from multiple sources | ETL pipeline must join heterogeneous data (BigQuery, Etherscan API, community blacklists) into a unified graph |
| **Graph complexity** | 300M+ nodes × 2B+ edges → adjacency matrix is ~10^17 entries | Graph does not fit in memory; requires mini-batch neighborhood sampling (DGL) and distributed graph partitioning |
| **Veracity** | Only ~5,600 verified phishing labels out of 300M+ addresses (0.002%) | Extreme class imbalance demands specialized loss functions and semi-supervised learning at scale |

### Input Data

| Field | Type | Description |
|-------|------|-------------|
| `from_address` | string (hex) | Sender's Ethereum address |
| `to_address` | string (hex) | Receiver's Ethereum address |
| `value` | float (ETH/Wei) | Transaction amount |
| `gas` | integer | Gas limit |
| `gas_price` | integer (Wei) | Gas price |
| `block_number` | integer | Block in which the transaction was mined |
| `timestamp` | integer (Unix) | Transaction timestamp |
| `input_data` | string (hex) | Smart contract interaction data |
| `is_error` | boolean | Whether the transaction failed |
| `label` | integer (0/1) | Phishing label (from known phishing databases) |

- **Source**: Ethereum blockchain via public APIs (Etherscan, Google BigQuery `bigquery-public-data.crypto_ethereum`), and labeled phishing datasets (e.g., Etherscan phishing label list, XBlock-ETH dataset)
- **Format**: Raw data in JSON/CSV; processed data in Parquet
- **Scale**: ~2 billion+ transactions, ~300 million+ unique addresses

### Expected Output

- **Prediction**: Binary classification per address (phishing = 1, legitimate = 0) with a confidence score
- **Alert**: Real-time flagging of newly detected phishing addresses
- **Visualization**: Dashboard showing phishing clusters, high-risk addresses, and network topology
- **Insight**: Behavioral patterns common to phishing accounts (e.g., fan-out transactions, short account lifespan)

### Processing Type

**Hybrid (Batch + Streaming)**:
- **Batch**: Periodic retraining of the GNN model on the full historical transaction graph
- **Streaming**: Near real-time inference on new transactions to detect emerging phishing accounts

---

## 2. Architecture Design

### System Architecture Overview

```
Ethereum Blockchain → Data Ingestion → Storage → Processing (GNN) → Output
       |                    |              |            |               |
   Etherscan API       Apache Kafka     GCS         Apache Spark    Dashboard /
   Google BigQuery     Spark Streaming  Delta Lake   + DGL/PyG      Alert API
```

### Data Ingestion

- **Batch Ingestion**: Google BigQuery public Ethereum dataset queried periodically (daily/weekly) using scheduled Spark jobs to pull historical transaction data
- **Streaming Ingestion**: Apache Kafka consumes new transactions from Ethereum node WebSocket subscriptions or Etherscan API streaming endpoints
- **Label Ingestion**: Phishing address labels pulled from Etherscan's labeled address database and community-maintained blacklists via REST API

**Tool**: Apache Kafka (streaming), Apache Spark (batch ETL)

### Storage Layer

- **Raw Data**: Stored in **Google Cloud Storage (GCS)** buckets in **Parquet** format, partitioned by `block_number` range for efficient querying (e.g., `gs://eth-phishing-data/raw/transactions/`)
- **Processed Graph Data**: Stored in **Delta Lake** on top of GCS for ACID transactions and versioned graph snapshots
  - Node table: address features (in-degree, out-degree, total ETH sent/received, avg transaction value, account age, etc.)
  - Edge table: transaction edges with attributes (value, timestamp, gas)
- **Model Artifacts**: Stored in a model registry (MLflow) on GCS (`gs://eth-phishing-data/models/`)
- **Label Store**: Known phishing labels stored in **Cloud SQL (PostgreSQL)** on Google Cloud

**Formats**: Parquet (columnar, compressed), Delta Lake (versioned on GCS)

### Processing Layer

The processing layer is divided into four stages: data preprocessing, graph construction, GNN model training, and real-time inference.

#### Stage 1: Data Preprocessing (Apache Spark on Dataproc)

Raw transaction records from GCS are cleaned and transformed using PySpark:

- **Data Cleaning**:
  - Remove duplicate transactions (same `tx_hash`)
  - Filter out internal transactions with zero value (optional, depending on analysis scope)
  - Handle missing fields (`input_data`, `gas_price`) with default values
  - Convert `value` from Wei to ETH (`value / 1e18`)
  - Parse `timestamp` into date components for temporal analysis
- **Address Normalization**: Convert all addresses to lowercase checksummed format to avoid duplicates
- **Label Joining**: Join transaction data with the phishing label table (from Cloud SQL) on `from_address` and `to_address` to propagate known labels

```python
# PySpark preprocessing example
raw_tx = spark.read.parquet("gs://eth-phishing-data/raw/transactions/")
labels = spark.read.jdbc(url=cloudsql_url, table="phishing_labels")

cleaned_tx = (
    raw_tx
    .dropDuplicates(["tx_hash"])
    .withColumn("value_eth", F.col("value") / F.lit(1e18))
    .withColumn("from_address", F.lower(F.col("from_address")))
    .withColumn("to_address", F.lower(F.col("to_address")))
)

labeled_tx = cleaned_tx.join(
    labels, cleaned_tx.from_address == labels.address, "left"
)
```

#### Stage 2: Graph Construction & Feature Engineering (Apache Spark)

Transactions are aggregated into a directed graph where nodes are Ethereum addresses and edges represent transaction relationships.

- **Node Feature Computation**: Spark `groupBy` on addresses to compute aggregated features:

| Feature | Computation | Rationale |
|---------|-------------|-----------|
| `in_degree` | Count of incoming transactions | Phishing accounts often receive from many victims |
| `out_degree` | Count of outgoing transactions | Phishing accounts quickly move funds out |
| `total_eth_received` | Sum of incoming ETH | Total volume of funds collected |
| `total_eth_sent` | Sum of outgoing ETH | How quickly funds are drained |
| `avg_tx_value_in` | Mean of incoming ETH values | Phishing tends to have uniform small amounts |
| `avg_tx_value_out` | Mean of outgoing ETH values | Large outgoing transfers indicate fund consolidation |
| `max_tx_value` | Max single transaction value | Outlier detection |
| `unique_in_neighbors` | Count distinct senders | Fan-in pattern typical of phishing |
| `unique_out_neighbors` | Count distinct receivers | Fan-out pattern for money laundering |
| `account_lifetime` | Last tx timestamp - first tx timestamp | Phishing accounts tend to have short lifespans |
| `failed_tx_ratio` | Failed tx count / total tx count | Higher failure rate may indicate automated attack scripts |
| `avg_gas_used` | Mean gas consumed per transaction | Smart contract interaction patterns |

- **Edge Feature Computation**: Aggregate multiple transactions between the same address pair:
  - `edge_count`: Number of transactions between the pair
  - `total_value`: Sum of ETH transferred on this edge
  - `avg_value`: Average ETH per transaction
  - `time_span`: Duration between first and last transaction on this edge

- **Graph Assembly**: Build the adjacency list (edge index) by mapping addresses to integer node IDs:

```python
# Build node ID mapping
all_addresses = (
    cleaned_tx.select("from_address").union(cleaned_tx.select("to_address"))
    .distinct()
    .withColumn("node_id", F.monotonically_increasing_id())
)

# Build edge index
edges = (
    cleaned_tx
    .join(all_addresses.alias("src"), F.col("from_address") == F.col("src.from_address"))
    .join(all_addresses.alias("dst"), F.col("to_address") == F.col("dst.from_address"))
    .select(F.col("src.node_id").alias("src_id"), F.col("dst.node_id").alias("dst_id"))
)
```

#### Stage 3: GNN Model Training (DGL + PyTorch on GPU)

The constructed graph and features are loaded into **DGL (Deep Graph Library)** for GNN training on GPU-enabled nodes (e.g., Google Cloud Dataproc with NVIDIA T4/A100 GPUs or Vertex AI).

- **Model Architecture — GraphSAGE**:
  - **Why GraphSAGE**: Inductive learning — can generalize to unseen nodes (new phishing addresses) without retraining on the full graph, unlike transductive models (e.g., GCN)
  - **Layers**: 2 GraphSAGE layers with `mean` aggregation
  - **Hidden dimension**: 128 per layer
  - **Activation**: ReLU between layers, softmax at output
  - **Dropout**: 0.5 between layers to prevent overfitting
  - **Output**: 2-class classification (phishing vs. legitimate)

- **Training Strategy**:
  - **Mini-batch neighborhood sampling**: For each target node, sample 15 neighbors at hop-1 and 10 neighbors at hop-2, creating manageable subgraphs that fit in GPU memory
  - **Semi-supervised learning**: Only a small fraction of nodes have labels (known phishing/legitimate); the model learns from both labeled and unlabeled nodes through neighborhood aggregation
  - **Class imbalance handling**: Phishing accounts are rare (~0.1% of all addresses); addressed via:
    - Weighted cross-entropy loss (higher weight for phishing class)
    - Oversampling phishing nodes in training batches
    - Focal loss as an alternative to standard cross-entropy
  - **Train/validation/test split**: 60% / 20% / 20% of labeled nodes, stratified to preserve class distribution

- **Hyperparameters**:

| Parameter | Value | Notes |
|-----------|-------|-------|
| Learning rate | 0.001 | Adam optimizer with weight decay 5e-4 |
| Batch size | 1024 nodes | Per mini-batch |
| Epochs | 50-100 | With early stopping (patience=10) |
| Neighbor samples | [15, 10] | Hop-1, Hop-2 |
| Hidden dimension | 128 | Per GraphSAGE layer |
| Dropout | 0.5 | Between layers |
| Loss | Weighted cross-entropy | Weight ratio ~1:100 (legitimate:phishing) |

- **Evaluation Metrics**:
  - **Precision**: Minimize false positives (legitimate accounts wrongly flagged)
  - **Recall**: Maximize detection of actual phishing accounts
  - **F1-score**: Harmonic mean as the primary metric
  - **AUC-ROC**: Overall discriminative ability across thresholds
  - **AUC-PR**: More informative than ROC under class imbalance

- **Model Versioning**: Each trained model is logged to **MLflow** on GCS with:
  - Model weights, hyperparameters, training metrics
  - Graph snapshot version (Delta Lake timestamp)
  - Evaluation results on the test set

#### Stage 4: Real-Time Inference (Spark Structured Streaming + Trained Model)

New transactions arriving via Kafka are scored in near real-time:

1. **Consume**: Spark Structured Streaming reads from the `new_transactions` Kafka topic
2. **Enrich**: For each new address, compute features by querying the latest node feature table in Delta Lake and merging with the new transaction data
3. **Construct local subgraph**: Extract the 2-hop neighborhood of the target address from the stored edge table
4. **Infer**: Load the latest trained GraphSAGE model and run forward pass on the local subgraph to produce a phishing probability score
5. **Threshold & Alert**: If the score exceeds a configurable threshold (e.g., 0.7), publish the address to the `phishing_alerts` Kafka topic and write the result to Cloud SQL

```python
# Streaming inference pipeline (simplified)
stream_df = (
    spark.readStream
    .format("kafka")
    .option("subscribe", "new_transactions")
    .load()
)

def score_new_address(batch_df, batch_id):
    # Extract addresses from new transactions
    new_addresses = batch_df.select("from_address", "to_address")

    # Fetch 2-hop subgraph from Delta Lake
    subgraph = extract_subgraph(new_addresses, hop=2)

    # Run GNN inference
    model = load_latest_model("gs://eth-phishing-data/models/")
    predictions = model.predict(subgraph)

    # Alert on high-risk addresses
    alerts = predictions.filter(F.col("phishing_score") > 0.7)
    alerts.write.format("kafka").option("topic", "phishing_alerts").save()
    alerts.write.jdbc(url=cloudsql_url, table="detection_results", mode="append")

stream_df.writeStream.foreachBatch(score_new_address).start()
```

- **Latency target**: < 30 seconds from transaction appearing on-chain to alert generation
- **Model refresh**: The inference service reloads the latest model from MLflow/GCS every 24 hours (or on-demand after retraining)

**Frameworks**: Apache Spark (preprocessing, feature engineering, streaming), DGL + PyTorch (GNN training & inference), Google Cloud Dataproc (managed Spark/GPU cluster)

### Output Layer

- **Dashboard**: Grafana or custom web dashboard displaying:
  - Detected phishing addresses and their risk scores
  - Transaction graph visualization of flagged clusters
  - Model performance metrics (precision, recall, F1)
- **Alert System**: Flagged addresses pushed to a REST API / Kafka topic for downstream consumers (wallets, exchanges)
- **Database**: Detection results stored in PostgreSQL for querying and audit

---

## 3. Data Flow Explanation

### Batch Pipeline

```
Ethereum BigQuery Dataset
        ↓
  Spark Batch ETL (extract transactions, filter, clean)
        ↓
  HDFS / Delta Lake (raw Parquet, partitioned by block range)
        ↓
  Spark Feature Engineering (compute node & edge features)
        ↓
  Delta Lake (node feature table + edge table)
        ↓
  GNN Training with DGL/PyG (GraphSAGE on sampled subgraphs)
        ↓
  Model Registry (MLflow) — versioned model artifacts
        ↓
  Batch Inference (score all addresses)
        ↓
  PostgreSQL (predictions table) → Grafana Dashboard
```

### Streaming Pipeline

```
Ethereum Node / Etherscan API
        ↓
  Kafka Topic (new_transactions)
        ↓
  Spark Structured Streaming (parse, enrich with features)
        ↓
  GNN Inference Service (load latest model, classify address)
        ↓
  Kafka Topic (phishing_alerts)
        ↓
  Alert API / Dashboard (real-time notifications)
```

### Key Transformations

1. **Raw transactions** → **Graph edges**: Each transaction becomes a directed edge from sender to receiver
2. **Addresses** → **Graph nodes**: Each unique address becomes a node with aggregated features
3. **Node features + Graph structure** → **GNN embeddings**: GNN learns structural representations via message passing
4. **GNN embeddings** → **Classification**: Final layer outputs phishing probability per node

---

## 4. Technology Selection and Justification

| Component | Choice | Big Data Justification |
|-----------|--------|----------------------|
| **Batch Ingestion** | Google BigQuery + Spark | BigQuery indexes the full Ethereum dataset (~1 TB); single-machine queries would take hours. Spark distributes the ETL across a cluster for parallel extraction of 2B+ rows |
| **Stream Ingestion** | Apache Kafka | Handles ~200K transactions/day with fault tolerance and exactly-once semantics. A simple REST poller cannot guarantee delivery or handle burst traffic during high-gas periods |
| **Storage** | Google Cloud Storage + Delta Lake | GCS scales to petabytes with no capacity planning. Delta Lake adds ACID transactions and time-travel — critical when multiple Spark jobs read/write the graph concurrently |
| **Data Format** | Parquet | Columnar compression reduces ~1 TB of raw JSON to ~200 GB. Predicate pushdown lets Spark read only needed columns (e.g., `from_address`, `value`) without full scans |
| **Processing** | Apache Spark (Dataproc) | Feature engineering requires `groupBy` over 300M+ addresses across 2B+ edges — a single-machine pandas job would take days. Spark parallelizes this across 10–100 executors in minutes |
| **GNN Framework** | DGL (Deep Graph Library) | The full graph (300M nodes × 2B edges) is ~48 GB as an adjacency list — cannot fit on a single GPU. DGL's `NeighborSampler` creates mini-batch subgraphs that fit in 16 GB VRAM, and `DistGraph` partitions across machines for graphs that exceed single-node RAM |
| **GNN Model** | GraphSAGE | **Inductive** — can classify new phishing addresses without retraining on the full graph. Transductive models (GCN, Trans2Vec) must retrain when new nodes appear, which is infeasible at scale with 300M+ nodes |
| **Model Management** | MLflow | Tracks model versions, hyperparameters, and metrics across retraining cycles. Without this, comparing weekly retraining runs on TB-scale data becomes unmanageable |
| **Dashboard** | Grafana | Connects directly to PostgreSQL for real-time queries over detection results, avoiding the need to export and transform data for visualization |

### Why Not Simpler Alternatives?

| Alternative | Why It Fails at This Scale |
|-------------|---------------------------|
| **pandas + scikit-learn on a single machine** | 2B transactions × 10 columns ≈ 80 GB CSV. pandas cannot load this into memory (typical machine has 16–64 GB RAM). Even with chunked reading, `groupBy` for 300M addresses requires distributed computing |
| **Neo4j for graph storage + queries** | Neo4j handles graph traversal well but does not support GNN training (no gradient computation). Also, loading 2B edges into Neo4j takes 10+ hours and requires 500+ GB RAM for the index |
| **Traditional ML (Random Forest, XGBoost)** | These models use only per-address tabular features and miss the structural patterns (fan-in/fan-out topology) that distinguish phishing accounts. Studies show GNNs outperform tabular ML by 15–20% F1 on this task (Trans2Vec, TSGN) |
| **Apache Flink instead of Spark** | Flink offers lower latency (~ms) but Spark's unified batch + streaming API avoids maintaining two separate systems. For phishing detection, 10–30 second latency is acceptable — phishing funds typically aren't moved for hours |
| **Single-GPU training without sampling** | The full graph adjacency matrix (300M × 300M) is ~10^17 entries — storing it would require ~90 PB. Mini-batch neighborhood sampling reduces each training step to ~200K nodes, fitting in 4 GB VRAM |

---

## 5. Data Modeling

### Graph Schema

**Nodes (Addresses)**:

| Feature | Type | Description |
|---------|------|-------------|
| `address` | string | Ethereum address (node ID) |
| `in_degree` | integer | Number of incoming transactions |
| `out_degree` | integer | Number of outgoing transactions |
| `total_eth_received` | float | Total ETH received |
| `total_eth_sent` | float | Total ETH sent |
| `avg_tx_value_in` | float | Average incoming transaction value |
| `avg_tx_value_out` | float | Average outgoing transaction value |
| `max_tx_value` | float | Maximum single transaction value |
| `unique_neighbors` | integer | Number of unique addresses interacted with |
| `account_lifetime` | integer | Seconds between first and last transaction |
| `failed_tx_ratio` | float | Ratio of failed transactions |
| `avg_gas_used` | float | Average gas consumed |
| `label` | integer | 0 = legitimate, 1 = phishing (if known) |

**Edges (Transactions)**:

| Feature | Type | Description |
|---------|------|-------------|
| `from_address` | string | Source node |
| `to_address` | string | Target node |
| `value` | float | Transaction value in ETH |
| `timestamp` | integer | Unix timestamp |
| `gas_used` | integer | Gas consumed |
| `edge_weight` | float | Normalized transaction value (for GNN aggregation) |

### GNN Feature Design

- **Input features**: 12-dimensional node feature vector (all numerical features above, normalized)
- **GNN layers**: 2-3 GraphSAGE/GAT layers with hidden dimension 128
- **Neighborhood sampling**: 15 neighbors at hop-1, 10 neighbors at hop-2 (for scalable mini-batch training)
- **Output**: 2-class softmax (phishing vs. legitimate)
- **Loss**: Cross-entropy with class weighting (phishing addresses are a small minority)

---

## 6. Scalability and Optimization

### Horizontal Scaling Strategy

| Layer | Current Scale | Scaling Mechanism | Cost Model |
|-------|--------------|-------------------|------------|
| **Kafka** | 3 partitions, 1 broker | Add partitions + brokers linearly | ~200K msgs/day → 2M msgs/day with 3 brokers |
| **GCS** | ~200 GB Parquet | Automatic — no capacity planning | Pay-per-GB stored ($0.02/GB/month) |
| **Spark (Dataproc)** | 4 workers, 16 vCPUs each | Add workers; scale executors horizontally | 4 workers: ~$4/hr → 40 workers: ~$40/hr |
| **DGL Training** | 1× T4 GPU (16 GB) | Multi-GPU with `DistGraph` | 1× A100: ~$3/hr, scales to 8× across nodes |
| **PostgreSQL** | Single instance | Read replicas for dashboard queries | Separate write (streaming) from read (Grafana) |

### Partitioning Strategy

| Data | Partition Key | Why This Key | Parallelism |
|------|--------------|-------------|-------------|
| **Raw transactions** | `block_number` range (1M blocks/partition) | Temporal locality — batch ETL processes block ranges independently | 20 partitions = 20 parallel Spark tasks |
| **Node feature table** | Hash of `address` (mod 256) | Even distribution — avoids hotspots from high-volume addresses (exchanges) | 256 partitions across Spark executors |
| **Edge table** | Hash of `from_address` | Collocates all outgoing edges of an address for fast neighbor lookups during subgraph extraction | Enables partition-local `groupBy` |
| **Kafka topic** | `from_address` key | Same address always goes to same partition → ordering guarantee per sender | 3–12 partitions based on throughput |

### Optimization Techniques

| Technique | Problem It Solves | Impact |
|-----------|------------------|--------|
| **Delta Lake Z-ORDER** on `from_address`, `to_address` | Full table scan during subgraph extraction for streaming inference | 10–50× faster point lookups by clustering co-accessed addresses |
| **Spark broadcast join** for label table | Small label table (~5,600 rows) joined against 2B transactions | Avoids shuffle — label table is broadcast to all executors |
| **Spark `.cache()`** on node features | Feature DataFrame re-read on every streaming micro-batch | Keeps ~2 GB feature matrix in executor memory |
| **Incremental ETL** | Reprocessing all 2B transactions on every batch run | Only process new blocks (`block_number > last_processed`), merge into existing Delta tables |
| **Model quantization** (INT8) | GNN inference latency in streaming pipeline | 2–3× faster inference with < 1% accuracy loss |
| **Graph pruning** (TTL) | Graph grows unboundedly as blockchain grows | Remove nodes/edges with no activity in 12+ months — reduces working graph by ~60% |

### What Happens if Data Volume Increases by 10x?

Current: ~2B transactions, ~300M addresses. At 10×: ~20B transactions, ~3B addresses.

| Component | Current | At 10× | Action Required |
|-----------|---------|--------|----------------|
| **Raw storage** | ~200 GB Parquet | ~2 TB | GCS auto-scales. No action. |
| **Kafka** | 3 partitions, ~200K msgs/day | 12 partitions, ~2M msgs/day | Add 3 brokers, increase partitions |
| **Spark ETL** | 4 workers, ~30 min batch | 40 workers, ~30 min batch | 10× workers = same runtime (linear scaling) |
| **Feature engineering** | `groupBy` over 300M addresses | `groupBy` over 3B addresses | Increase executor memory to 32 GB; add partitions |
| **GNN graph** | 300M nodes, fits in 1-node RAM | 3B nodes, ~48 GB adjacency | Switch to DGL `DistGraph` across 4 machines |
| **GNN training** | 1× T4, batch_size=1024 | 4× A100, batch_size=4096 | Distributed data-parallel training |
| **Streaming inference** | 1 Spark Streaming job | 3 replicas behind load balancer | Horizontal scaling of inference workers |
| **PostgreSQL** | Single instance | Read replicas + partitioned tables | Partition `detection_results` by `detected_at` |

### Latency vs. Throughput Trade-off

| Pipeline | Priority | Metric | Acceptable Range |
|----------|----------|--------|-----------------|
| **Batch** | Throughput | Process 2B+ transactions per run | 1–4 hours per weekly retraining cycle |
| **Streaming** | Latency | Time from on-chain transaction to alert | < 30 seconds |
| **Dashboard** | Freshness | Time from detection to visible on Grafana | < 5 seconds (PostgreSQL → Grafana refresh) |

This trade-off is managed by **decoupling training (batch) from inference (streaming)**. The batch pipeline can run for hours retraining the model on the full graph without affecting real-time detection. The streaming pipeline uses the last trained model and operates independently.

---

## 7. Bonus

### Pseudo-code: GNN Training Pipeline

```python
# 1. Load graph data from Delta Lake
nodes_df = spark.read.format("delta").load("/data/nodes")
edges_df = spark.read.format("delta").load("/data/edges")

# 2. Construct DGL graph
src = edges_df.select("from_idx").collect()
dst = edges_df.select("to_idx").collect()
graph = dgl.graph((src, dst))
graph.ndata["feat"] = torch.tensor(node_features)  # [N, 12]
graph.ndata["label"] = torch.tensor(labels)         # [N]

# 3. Define GraphSAGE model
model = GraphSAGE(
    in_feats=12,
    hidden_feats=128,
    out_feats=2,
    num_layers=2,
    aggregator="mean"
)

# 4. Mini-batch training with neighborhood sampling
sampler = dgl.dataloading.NeighborSampler([15, 10])
dataloader = dgl.dataloading.NodeDataLoader(
    graph, train_nids, sampler, batch_size=1024, shuffle=True
)

optimizer = torch.optim.Adam(model.parameters(), lr=0.001)
for epoch in range(50):
    for input_nodes, output_nodes, blocks in dataloader:
        logits = model(blocks, blocks[0].srcdata["feat"])
        loss = F.cross_entropy(
            logits, labels[output_nodes], weight=class_weights
        )
        optimizer.zero_grad()
        loss.backward()
        optimizer.step()

# 5. Save model to MLflow
mlflow.pytorch.log_model(model, "graphsage_phishing")
```

### Advanced Techniques

- **Temporal Graph Networks (TGN)**: Incorporate transaction timestamps into the GNN to capture evolving phishing behavior over time, as demonstrated by GrabPhisher (2024, 95% Recall)
- **Transaction Subgraph Networks (TSGN)**: Transform ego-networks into line graphs that preserve transaction flow direction and temporal order for more expressive graph classification
- **Ensemble approach**: Combine GNN predictions with traditional ML features (XGBoost on node features) for improved robustness
- **Active learning**: Route low-confidence predictions (score 0.3–0.7) to analyst review queue, iteratively improving the labeled dataset for each retraining cycle

### Big Data Techniques Summary

| Big Data Technique | Where Applied | Why It Matters |
|-------------------|---------------|---------------|
| **Distributed ETL** (Spark) | Feature engineering over 2B+ transactions | Single-machine pandas cannot `groupBy` 300M addresses in memory |
| **Columnar storage** (Parquet) | Raw and processed data on GCS | 5× compression + predicate pushdown = 80% less I/O |
| **Stream processing** (Kafka + Spark Streaming) | Real-time phishing detection | Detect phishing in seconds, not hours |
| **Mini-batch graph sampling** (DGL) | GNN training on 300M-node graph | Full graph doesn't fit in GPU memory; sampling creates trainable subgraphs |
| **Data versioning** (Delta Lake) | Reproducible graph snapshots | Time-travel enables comparing model accuracy across different graph versions |
| **Horizontal scaling** (Dataproc autoscaling) | Handling 10× data growth | Add workers linearly — no code changes required |
| **Data partitioning** (block_number, address hash) | Parallel processing at every layer | Avoid data skew from high-volume addresses (exchanges) |
| **Lambda architecture** (batch + streaming) | Decoupled training from inference | Batch retraining doesn't block real-time detection |

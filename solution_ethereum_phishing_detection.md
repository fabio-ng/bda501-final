# GNN-Based Ethereum Phishing Detection on Big Data Infrastructure

## Full Technical Solution

---

## Table of Contents

1. [Solution Overview](#1-solution-overview)
2. [Phase 1: Data Crawling & Cluster Storage](#2-phase-1-data-crawling--cluster-storage)
3. [Phase 2: Data Processing with Spark](#3-phase-2-data-processing-with-spark)
4. [Phase 3: GNN Model Training (Distributed)](#4-phase-3-gnn-model-training-distributed)
5. [Phase 4: Web Application — Real-time Phishing Classifier](#5-phase-4-web-application--real-time-phishing-classifier)
6. [Deployment Architecture](#6-deployment-architecture)
7. [Directory Structure](#7-directory-structure)

---

## 1. Solution Overview

### 1.1 Problem

Detect phishing accounts on Ethereum blockchain using Graph Neural Networks, deployed on a Big Data infrastructure that handles the 3Vs: Volume (2B+ transactions), Velocity (new block every ~12s), and Variety (ETH transfers, token transfers, contract calls).

### 1.2 End-to-End Pipeline

```
┌─────────────────────────────────────────────────────────────────────────┐
│                        END-TO-END PIPELINE                              │
│                                                                         │
│  [BigQuery/Etherscan]  →  [HDFS Cluster]  →  [Spark ETL]               │
│         │                      │                   │                    │
│     Crawl raw txns        Multi-node           Feature eng.             │
│     + phishing labels     storage (3 nodes)    Graph build              │
│                                                    │                    │
│                                                    ▼                    │
│  [Web App] ← [FastAPI] ← [Trained Model] ← [Spark + DistDGL]          │
│   Streamlit    REST API    GNN weights       Distributed training       │
│   dashboard    /predict    + embeddings      on Spark cluster           │
└─────────────────────────────────────────────────────────────────────────┘
```

### 1.3 Tech Stack

| Layer | Technology | Purpose |
|-------|-----------|---------|
| **Storage** | HDFS (3-node cluster) | Distributed raw data storage |
| **Cluster DB** | Apache Cassandra | Graph node/edge feature store |
| **Processing** | Apache Spark (PySpark) | Distributed ETL & feature engineering |
| **Graph Framework** | PySpark GraphFrames | Graph analytics at scale |
| **GNN Training** | PyTorch Geometric + DGL | GNN model implementation |
| **Distributed DL** | Spark TorchDistributor | Multi-GPU distributed training |
| **Serving** | FastAPI | REST API for real-time prediction |
| **Frontend** | Streamlit | Interactive web dashboard |
| **Data Source** | Google BigQuery + Etherscan API | Ethereum blockchain data |

---

## 2. Phase 1: Data Crawling & Cluster Storage

### 2.1 Architecture — Multi-Node Cluster

```
┌──────────────────────────────────────────────────────────┐
│                   HDFS CLUSTER (3 nodes)                  │
│                                                           │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐      │
│  │  NameNode    │  │  DataNode 1 │  │  DataNode 2 │      │
│  │  (Master)    │  │  (Worker)   │  │  (Worker)   │      │
│  │             │  │             │  │             │      │
│  │  HDFS Meta  │  │  Block A    │  │  Block A'   │      │
│  │  Spark Master│  │  Block B    │  │  Block C    │      │
│  │  Cassandra  │  │  Cassandra  │  │  Cassandra  │      │
│  └─────────────┘  └─────────────┘  └─────────────┘      │
│                                                           │
│  Replication Factor = 2                                   │
│  Storage: /data/ethereum/raw/                             │
│           /data/ethereum/processed/                        │
│           /data/ethereum/models/                           │
└──────────────────────────────────────────────────────────┘
```

### 2.2 Step 1 — Crawl Phishing Labels

Collect known phishing addresses from multiple authoritative sources.

```python
# crawl_labels.py
import requests
import pandas as pd
from cassandra.cluster import Cluster

# --- Source 1: Etherscan labeled accounts ---
ETHERSCAN_API_KEY = "YOUR_KEY"

def crawl_etherscan_labels():
    """Crawl labeled phishing addresses from Etherscan."""
    url = "https://api.etherscan.io/api"
    # Etherscan provides labeled accounts via their website
    # We also use their API for transaction data
    phishing_addresses = []
    
    # Load from academic datasets (XBlock, DA-HGNN)
    xblock = pd.read_csv("data/xblock_phishing_labels.csv")
    dahgnn = pd.read_csv("data/dahgnn_phishing_labels.csv")
    
    # Load from CryptoScamDB / ChainAbuse
    scamdb = pd.read_csv("data/cryptoscamdb_addresses.csv")
    
    # Merge and deduplicate
    all_labels = pd.concat([
        xblock[["address", "label"]],
        dahgnn[["address", "label"]],
        scamdb[["address", "label"]],
    ]).drop_duplicates(subset="address")
    
    return all_labels  # ~5K-10K phishing + ~50K normal addresses

# --- Source 2: Sample normal addresses ---
def crawl_normal_addresses(n=50000):
    """Sample normal addresses from recent active accounts."""
    # Query BigQuery for recently active addresses NOT in any scam list
    query = """
    SELECT DISTINCT from_address as address
    FROM `bigquery-public-data.crypto_ethereum.transactions`
    WHERE block_timestamp > TIMESTAMP('2024-01-01')
      AND from_address NOT IN (SELECT address FROM phishing_labels)
    ORDER BY RAND()
    LIMIT {n}
    """
    # Execute via BigQuery client
    return normal_addresses

# --- Store labels in Cassandra ---
def store_labels_cassandra(labels_df):
    cluster = Cluster(["node1", "node2", "node3"])
    session = cluster.connect()
    
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS ethereum
        WITH replication = {'class': 'SimpleStrategy', 
                           'replication_factor': 3}
    """)
    session.execute("USE ethereum")
    
    session.execute("""
        CREATE TABLE IF NOT EXISTS address_labels (
            address text PRIMARY KEY,
            label int,          -- 1 = phishing, 0 = normal
            source text,        -- where the label came from
            crawled_at timestamp
        )
    """)
    
    prepared = session.prepare(
        "INSERT INTO address_labels (address, label, source, crawled_at) "
        "VALUES (?, ?, ?, toTimestamp(now()))"
    )
    for _, row in labels_df.iterrows():
        session.execute(prepared, (row.address, row.label, row.source))
    
    print(f"Stored {len(labels_df)} labels in Cassandra")
```

### 2.3 Step 2 — Crawl Transaction Data via BigQuery

Extract large-scale transaction data and store on HDFS.

```python
# crawl_transactions.py
from google.cloud import bigquery
import subprocess

client = bigquery.Client(project="your-project-id")

# --- Query 1: ETH Transactions (2-hop around labeled addresses) ---
ETH_TRANSACTIONS_QUERY = """
WITH labeled AS (
    SELECT address FROM `your-project.ethereum.address_labels`
),
-- 1-hop: direct transactions with labeled addresses
hop1 AS (
    SELECT hash, from_address, to_address, value, gas, gas_price,
           block_timestamp, block_number, input
    FROM `bigquery-public-data.crypto_ethereum.transactions`
    WHERE (from_address IN (SELECT address FROM labeled)
        OR to_address IN (SELECT address FROM labeled))
      AND block_timestamp BETWEEN '2022-01-01' AND '2025-12-31'
),
-- Collect 1-hop neighbor addresses
hop1_addresses AS (
    SELECT DISTINCT from_address AS address FROM hop1
    UNION DISTINCT
    SELECT DISTINCT to_address AS address FROM hop1
),
-- 2-hop: transactions between 1-hop neighbors
hop2 AS (
    SELECT hash, from_address, to_address, value, gas, gas_price,
           block_timestamp, block_number, input
    FROM `bigquery-public-data.crypto_ethereum.transactions`
    WHERE (from_address IN (SELECT address FROM hop1_addresses)
        OR to_address IN (SELECT address FROM hop1_addresses))
      AND block_timestamp BETWEEN '2022-01-01' AND '2025-12-31'
)
SELECT * FROM hop1
UNION ALL
SELECT * FROM hop2
"""

# --- Query 2: ERC-20 Token Transfers ---
TOKEN_TRANSFERS_QUERY = """
SELECT token_address, from_address, to_address, value,
       transaction_hash, block_timestamp, block_number
FROM `bigquery-public-data.crypto_ethereum.token_transfers`
WHERE (from_address IN (SELECT address FROM labeled_addresses)
    OR to_address IN (SELECT address FROM labeled_addresses))
  AND block_timestamp BETWEEN '2022-01-01' AND '2025-12-31'
"""

# --- Query 3: Internal Transactions (Contract Calls) ---
TRACES_QUERY = """
SELECT from_address, to_address, value, gas, gas_used,
       call_type, block_timestamp, block_number, trace_type
FROM `bigquery-public-data.crypto_ethereum.traces`
WHERE (from_address IN (SELECT address FROM labeled_addresses)
    OR to_address IN (SELECT address FROM labeled_addresses))
  AND block_timestamp BETWEEN '2022-01-01' AND '2025-12-31'
  AND status = 1
"""

def extract_and_store_hdfs(query, hdfs_path, table_name):
    """Execute BigQuery and store result to HDFS as Parquet."""
    print(f"Executing query for {table_name}...")
    
    # Export to GCS first, then copy to HDFS
    destination_uri = f"gs://your-bucket/ethereum/{table_name}/*.parquet"
    
    job_config = bigquery.QueryJobConfig(
        destination=f"your-project.ethereum_temp.{table_name}",
        write_disposition="WRITE_TRUNCATE",
    )
    query_job = client.query(query, job_config=job_config)
    query_job.result()
    
    # Export to GCS
    extract_job = client.extract_table(
        f"your-project.ethereum_temp.{table_name}",
        destination_uri,
        job_config=bigquery.ExtractJobConfig(
            destination_format="PARQUET"
        ),
    )
    extract_job.result()
    
    # Copy from GCS to HDFS
    subprocess.run([
        "hadoop", "distcp",
        destination_uri,
        f"hdfs:///data/ethereum/raw/{table_name}/"
    ], check=True)
    
    print(f"Stored {table_name} to HDFS: {hdfs_path}")

# Execute all extractions
extract_and_store_hdfs(ETH_TRANSACTIONS_QUERY, 
                       "hdfs:///data/ethereum/raw/eth_transactions/",
                       "eth_transactions")
extract_and_store_hdfs(TOKEN_TRANSFERS_QUERY,
                       "hdfs:///data/ethereum/raw/token_transfers/",
                       "token_transfers")
extract_and_store_hdfs(TRACES_QUERY,
                       "hdfs:///data/ethereum/raw/traces/",
                       "traces")
```

### 2.4 Step 3 — Store in Cassandra (Cluster DB)

```python
# store_cassandra.py
"""
Cassandra schema for fast random-access lookups during GNN training.
Cassandra cluster runs on same 3 nodes as HDFS.
"""

CASSANDRA_SCHEMA = """
-- Address features (node features)
CREATE TABLE ethereum.address_features (
    address text PRIMARY KEY,
    label int,
    total_tx_count int,
    total_tx_in int,
    total_tx_out int,
    total_value_in double,
    total_value_out double,
    avg_value_in double,
    avg_value_out double,
    unique_counterparties int,
    account_age_days int,
    is_contract boolean,
    token_diversity int,
    avg_gas_price double,
    max_single_tx_value double,
    tx_frequency_per_day double
);

-- Transaction edges (edge features)
CREATE TABLE ethereum.transaction_edges (
    from_address text,
    to_address text,
    tx_hash text,
    value double,
    gas_used int,
    block_timestamp timestamp,
    edge_type text,       -- 'eth_transfer', 'token_transfer', 'contract_call'
    token_address text,   -- null for ETH transfers
    PRIMARY KEY ((from_address), block_timestamp, tx_hash)
) WITH CLUSTERING ORDER BY (block_timestamp DESC);

-- Neighbor index for fast subgraph extraction
CREATE TABLE ethereum.neighbor_index (
    address text,
    neighbor text,
    edge_count int,
    total_value double,
    last_interaction timestamp,
    PRIMARY KEY (address, neighbor)
);
"""
```

### 2.5 Expected Data Scale

| Table | Estimated Rows | Storage (HDFS) | Cassandra |
|-------|---------------|----------------|-----------|
| ETH Transactions | 10M-50M | 5-20 GB | Index only |
| Token Transfers | 5M-20M | 3-10 GB | Index only |
| Traces | 2M-10M | 1-5 GB | Index only |
| Address Features | 500K-2M nodes | - | ~2 GB |
| Transaction Edges | 5M-20M edges | - | ~10 GB |
| Neighbor Index | 500K-2M rows | - | ~1 GB |

---

## 3. Phase 2: Data Processing with Spark

### 3.1 Spark Cluster Configuration

```yaml
# spark-defaults.conf
spark.master                     spark://master-node:7077
spark.executor.memory            8g
spark.executor.cores             4
spark.driver.memory              4g
spark.sql.shuffle.partitions     200
spark.serializer                 org.apache.spark.serializer.KryoSerializer
spark.kryoserializer.buffer.max  512m

# For distributed GNN training
spark.jars.packages              graphframes:graphframes:0.8.3-spark3.5-s_2.12
```

### 3.2 Feature Engineering Pipeline

```python
# spark_etl.py
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from graphframes import GraphFrame

spark = SparkSession.builder \
    .appName("Ethereum Phishing ETL") \
    .config("spark.cassandra.connection.host", "node1,node2,node3") \
    .config("spark.sql.extensions", 
            "com.datastax.spark.connector.CassandraSparkExtensions") \
    .getOrCreate()

# =============================================
# STEP 1: Load raw data from HDFS
# =============================================
eth_tx = spark.read.parquet("hdfs:///data/ethereum/raw/eth_transactions/")
token_tx = spark.read.parquet("hdfs:///data/ethereum/raw/token_transfers/")
traces = spark.read.parquet("hdfs:///data/ethereum/raw/traces/")
labels = spark.read \
    .format("org.apache.spark.sql.cassandra") \
    .options(table="address_labels", keyspace="ethereum") \
    .load()

print(f"ETH Transactions: {eth_tx.count():,}")
print(f"Token Transfers:  {token_tx.count():,}")
print(f"Traces:           {traces.count():,}")
print(f"Labels:           {labels.count():,}")

# =============================================
# STEP 2: Compute Node Features (per address)
# =============================================

# --- Outgoing transaction features ---
out_features = eth_tx.groupBy("from_address").agg(
    F.count("*").alias("tx_out_count"),
    F.sum("value").alias("total_value_out"),
    F.avg("value").alias("avg_value_out"),
    F.max("value").alias("max_value_out"),
    F.avg("gas_price").alias("avg_gas_price_out"),
    F.countDistinct("to_address").alias("unique_receivers"),
    F.min("block_timestamp").alias("first_tx_time"),
    F.max("block_timestamp").alias("last_tx_time"),
).withColumnRenamed("from_address", "address")

# --- Incoming transaction features ---
in_features = eth_tx.groupBy("to_address").agg(
    F.count("*").alias("tx_in_count"),
    F.sum("value").alias("total_value_in"),
    F.avg("value").alias("avg_value_in"),
    F.max("value").alias("max_value_in"),
    F.countDistinct("from_address").alias("unique_senders"),
).withColumnRenamed("to_address", "address")

# --- Token diversity (how many different tokens used) ---
token_diversity = token_tx.groupBy("from_address").agg(
    F.countDistinct("token_address").alias("token_diversity"),
    F.count("*").alias("token_tx_count"),
).withColumnRenamed("from_address", "address")

# --- Contract interaction features ---
contract_features = traces.groupBy("from_address").agg(
    F.count("*").alias("contract_call_count"),
    F.countDistinct("to_address").alias("unique_contracts_called"),
).withColumnRenamed("from_address", "address")

# --- Merge all features ---
node_features = out_features \
    .join(in_features, "address", "full_outer") \
    .join(token_diversity, "address", "left") \
    .join(contract_features, "address", "left") \
    .join(labels.select("address", "label"), "address", "left") \
    .fillna(0)

# --- Derived features ---
node_features = node_features.withColumn(
    "total_tx_count", F.col("tx_out_count") + F.col("tx_in_count")
).withColumn(
    "in_out_ratio", 
    F.when(F.col("tx_out_count") > 0, 
           F.col("tx_in_count") / F.col("tx_out_count")).otherwise(0)
).withColumn(
    "account_age_days",
    F.datediff(F.col("last_tx_time"), F.col("first_tx_time"))
).withColumn(
    "avg_tx_per_day",
    F.when(F.col("account_age_days") > 0,
           F.col("total_tx_count") / F.col("account_age_days")).otherwise(0)
).withColumn(
    "value_concentration",  # Gini-like: max_value / total_value
    F.when(F.col("total_value_out") > 0,
           F.col("max_value_out") / F.col("total_value_out")).otherwise(0)
)

print(f"Node features computed: {node_features.count():,} addresses")
print(f"  Phishing: {node_features.filter(F.col('label')==1).count():,}")
print(f"  Normal:   {node_features.filter(F.col('label')==0).count():,}")

# =============================================
# STEP 3: Build Edge List
# =============================================

# ETH transfer edges
eth_edges = eth_tx.select(
    F.col("from_address").alias("src"),
    F.col("to_address").alias("dst"),
    F.col("value").alias("edge_value"),
    F.col("gas_price").alias("edge_gas"),
    F.col("block_timestamp").alias("edge_timestamp"),
    F.lit("eth_transfer").alias("edge_type"),
)

# Token transfer edges
token_edges = token_tx.select(
    F.col("from_address").alias("src"),
    F.col("to_address").alias("dst"),
    F.col("value").alias("edge_value"),
    F.lit(0).alias("edge_gas"),
    F.col("block_timestamp").alias("edge_timestamp"),
    F.lit("token_transfer").alias("edge_type"),
)

# Contract call edges
call_edges = traces.select(
    F.col("from_address").alias("src"),
    F.col("to_address").alias("dst"),
    F.col("value").alias("edge_value"),
    F.col("gas_used").alias("edge_gas"),
    F.col("block_timestamp").alias("edge_timestamp"),
    F.lit("contract_call").alias("edge_type"),
)

# Union all edge types
all_edges = eth_edges.union(token_edges).union(call_edges)

# Aggregate multi-edges into weighted single edges
edge_features = all_edges.groupBy("src", "dst", "edge_type").agg(
    F.count("*").alias("tx_count"),
    F.sum("edge_value").alias("total_value"),
    F.avg("edge_value").alias("avg_value"),
    F.min("edge_timestamp").alias("first_tx"),
    F.max("edge_timestamp").alias("last_tx"),
    F.avg("edge_gas").alias("avg_gas"),
)

print(f"Edge features computed: {edge_features.count():,} edges")

# =============================================
# STEP 4: Graph Analytics with GraphFrames
# =============================================

vertices = node_features.select(
    F.col("address").alias("id"), "*"
)
edges_gf = edge_features.select(
    F.col("src"), F.col("dst"), "*"
)

g = GraphFrame(vertices, edges_gf)

# PageRank — phishing accounts often have abnormal PageRank
pagerank = g.pageRank(resetProbability=0.15, maxIter=10)
pr_scores = pagerank.vertices.select("id", "pagerank")

# Triangle count — legitimate accounts have more triangles
triangles = g.triangleCount()
tri_counts = triangles.select("id", "count").withColumnRenamed("count", "triangle_count")

# Degree distribution
in_degrees = g.inDegrees.withColumnRenamed("id", "address") \
    .withColumnRenamed("inDegree", "in_degree")
out_degrees = g.outDegrees.withColumnRenamed("id", "address") \
    .withColumnRenamed("outDegree", "out_degree")

# Merge graph features back
node_features_enriched = node_features \
    .join(pr_scores.withColumnRenamed("id", "address"), "address", "left") \
    .join(tri_counts.withColumnRenamed("id", "address"), "address", "left") \
    .join(in_degrees, "address", "left") \
    .join(out_degrees, "address", "left") \
    .fillna(0)

# =============================================
# STEP 5: Save processed data
# =============================================

# Save to HDFS (Parquet) for Spark training
node_features_enriched.write \
    .mode("overwrite") \
    .parquet("hdfs:///data/ethereum/processed/node_features/")

edge_features.write \
    .mode("overwrite") \
    .parquet("hdfs:///data/ethereum/processed/edge_features/")

# Save to Cassandra for fast lookups (serving)
node_features_enriched.write \
    .format("org.apache.spark.sql.cassandra") \
    .options(table="address_features", keyspace="ethereum") \
    .mode("append") \
    .save()

edge_features.write \
    .format("org.apache.spark.sql.cassandra") \
    .options(table="transaction_edges", keyspace="ethereum") \
    .mode("append") \
    .save()

print("ETL pipeline complete!")
print(f"  Nodes: {node_features_enriched.count():,}")
print(f"  Edges: {edge_features.count():,}")
print(f"  Features per node: {len(node_features_enriched.columns)}")
```

### 3.3 Train/Test Split Strategy

```python
# split_data.py
from pyspark.sql import functions as F

node_features = spark.read.parquet(
    "hdfs:///data/ethereum/processed/node_features/"
)

# Temporal split: train on older data, test on newer
# This prevents data leakage and simulates real deployment
labeled = node_features.filter(F.col("label").isNotNull())

# 70% train / 15% val / 15% test by temporal order
train_cutoff = "2024-06-01"
val_cutoff = "2024-10-01"

train = labeled.filter(F.col("first_tx_time") < train_cutoff)
val = labeled.filter(
    (F.col("first_tx_time") >= train_cutoff) & 
    (F.col("first_tx_time") < val_cutoff)
)
test = labeled.filter(F.col("first_tx_time") >= val_cutoff)

# Handle class imbalance info
for name, df in [("Train", train), ("Val", val), ("Test", test)]:
    total = df.count()
    phishing = df.filter(F.col("label") == 1).count()
    print(f"{name}: {total:,} total, {phishing:,} phishing "
          f"({phishing/total*100:.1f}%)")

# Save splits
train.write.mode("overwrite").parquet("hdfs:///data/ethereum/processed/train/")
val.write.mode("overwrite").parquet("hdfs:///data/ethereum/processed/val/")
test.write.mode("overwrite").parquet("hdfs:///data/ethereum/processed/test/")
```

---

## 4. Phase 3: GNN Model Training (Distributed)

### 4.1 Convert to PyG Graph Format

```python
# build_pyg_graph.py
import torch
import numpy as np
import pandas as pd
from torch_geometric.data import HeteroData
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("BuildGraph").getOrCreate()

# Load processed data from HDFS
nodes_df = spark.read.parquet(
    "hdfs:///data/ethereum/processed/node_features/"
).toPandas()
edges_df = spark.read.parquet(
    "hdfs:///data/ethereum/processed/edge_features/"
).toPandas()

# Create address-to-index mapping
addr2idx = {addr: idx for idx, addr in enumerate(nodes_df["address"])}

# Node feature columns (exclude address, label, timestamps)
FEATURE_COLS = [
    "tx_out_count", "tx_in_count", "total_value_out", "total_value_in",
    "avg_value_out", "avg_value_in", "unique_receivers", "unique_senders",
    "token_diversity", "token_tx_count", "contract_call_count",
    "total_tx_count", "in_out_ratio", "account_age_days", 
    "avg_tx_per_day", "value_concentration", "pagerank", 
    "triangle_count", "in_degree", "out_degree",
    "avg_gas_price_out", "max_value_out",
]

# Build node features tensor
x = torch.tensor(
    nodes_df[FEATURE_COLS].values, dtype=torch.float32
)
# Normalize features
x = (x - x.mean(dim=0)) / (x.std(dim=0) + 1e-8)

# Build labels
y = torch.tensor(nodes_df["label"].fillna(-1).values, dtype=torch.long)

# Build edge index per edge type (for heterogeneous graph)
edge_types = ["eth_transfer", "token_transfer", "contract_call"]
edge_indices = {}
edge_attrs = {}

for etype in edge_types:
    mask = edges_df["edge_type"] == etype
    subset = edges_df[mask]
    
    src_indices = subset["src"].map(addr2idx).dropna().astype(int).values
    dst_indices = subset["dst"].map(addr2idx).dropna().astype(int).values
    
    valid = ~(np.isnan(src_indices) | np.isnan(dst_indices))
    edge_indices[etype] = torch.tensor(
        np.stack([src_indices[valid], dst_indices[valid]]), 
        dtype=torch.long
    )
    
    # Edge features: tx_count, total_value, avg_value, avg_gas
    edge_attrs[etype] = torch.tensor(
        subset[valid][["tx_count", "total_value", "avg_value", "avg_gas"]].values,
        dtype=torch.float32
    )

# Build HeteroData object
data = HeteroData()
data["address"].x = x
data["address"].y = y

# Train/val/test masks
train_mask = y >= 0  # will be refined by temporal split
data["address"].train_mask = train_mask

for etype in edge_types:
    data["address", etype, "address"].edge_index = edge_indices[etype]
    data["address", etype, "address"].edge_attr = edge_attrs[etype]

# Save
torch.save(data, "hdfs:///data/ethereum/processed/pyg_graph.pt")
print(f"Graph: {data}")
print(f"  Nodes: {x.shape[0]:,}")
print(f"  Features: {x.shape[1]}")
print(f"  ETH edges: {edge_indices['eth_transfer'].shape[1]:,}")
print(f"  Token edges: {edge_indices['token_transfer'].shape[1]:,}")
print(f"  Call edges: {edge_indices['contract_call'].shape[1]:,}")
```

### 4.2 GNN Model Definition

```python
# models.py
import torch
import torch.nn.functional as F
from torch_geometric.nn import (
    SAGEConv, GATConv, GCNConv, RGCNConv,
    HeteroConv, Linear, global_mean_pool
)

class PhishingGNN(torch.nn.Module):
    """
    Heterogeneous GNN for Ethereum phishing detection.
    Supports multiple edge types: eth_transfer, token_transfer, contract_call.
    """
    def __init__(self, in_channels, hidden_channels=128, out_channels=2, 
                 num_layers=2, edge_types=None, dropout=0.3):
        super().__init__()
        self.num_layers = num_layers
        self.dropout = dropout
        
        if edge_types is None:
            edge_types = [
                ("address", "eth_transfer", "address"),
                ("address", "token_transfer", "address"),
                ("address", "contract_call", "address"),
            ]
        
        self.convs = torch.nn.ModuleList()
        self.norms = torch.nn.ModuleList()
        
        for i in range(num_layers):
            in_c = in_channels if i == 0 else hidden_channels
            conv_dict = {}
            for etype in edge_types:
                # Use GraphSAGE for scalability (sampling-friendly)
                conv_dict[etype] = SAGEConv(in_c, hidden_channels)
            self.convs.append(HeteroConv(conv_dict, aggr="mean"))
            self.norms.append(torch.nn.BatchNorm1d(hidden_channels))
        
        # Classification head
        self.classifier = torch.nn.Sequential(
            Linear(hidden_channels, hidden_channels // 2),
            torch.nn.ReLU(),
            torch.nn.Dropout(dropout),
            Linear(hidden_channels // 2, out_channels),
        )
    
    def forward(self, x_dict, edge_index_dict):
        for i, (conv, norm) in enumerate(zip(self.convs, self.norms)):
            x_dict = conv(x_dict, edge_index_dict)
            x_dict = {key: norm(F.relu(x)) for key, x in x_dict.items()}
            if i < self.num_layers - 1:
                x_dict = {
                    key: F.dropout(x, p=self.dropout, training=self.training)
                    for key, x in x_dict.items()
                }
        
        out = self.classifier(x_dict["address"])
        return out
    
    def get_embeddings(self, x_dict, edge_index_dict):
        """Extract embeddings before classification head."""
        for conv, norm in zip(self.convs, self.norms):
            x_dict = conv(x_dict, edge_index_dict)
            x_dict = {key: norm(F.relu(x)) for key, x in x_dict.items()}
        return x_dict["address"]


class PhishingGraphSAGE(torch.nn.Module):
    """Simple homogeneous GraphSAGE baseline."""
    def __init__(self, in_channels, hidden_channels=128, out_channels=2,
                 num_layers=2, dropout=0.3):
        super().__init__()
        self.convs = torch.nn.ModuleList()
        self.convs.append(SAGEConv(in_channels, hidden_channels))
        for _ in range(num_layers - 1):
            self.convs.append(SAGEConv(hidden_channels, hidden_channels))
        self.classifier = Linear(hidden_channels, out_channels)
        self.dropout = dropout

    def forward(self, x, edge_index):
        for conv in self.convs:
            x = conv(x, edge_index)
            x = F.relu(x)
            x = F.dropout(x, p=self.dropout, training=self.training)
        return self.classifier(x)
```

### 4.3 Distributed Training with Spark

```python
# train_distributed.py
"""
Distributed GNN training using Spark TorchDistributor.
Runs on Spark cluster with multi-GPU support.
"""
from pyspark.sql import SparkSession
from pyspark.ml.torch.distributor import TorchDistributor
import torch
import torch.nn.functional as F
from torch_geometric.loader import NeighborLoader
from sklearn.metrics import (
    f1_score, precision_score, recall_score, 
    roc_auc_score, average_precision_score
)
import mlflow

def train_gnn(checkpoint_dir="/data/ethereum/models/"):
    """Main training function — executed on each Spark worker."""
    import torch.distributed as dist
    from models import PhishingGNN
    
    # Setup distributed
    dist.init_process_group(backend="nccl")
    local_rank = dist.get_rank()
    device = torch.device(f"cuda:{local_rank}")
    
    # Load graph data
    data = torch.load("/data/ethereum/processed/pyg_graph.pt")
    
    # Create NeighborLoader for mini-batch training (scalable)
    train_loader = NeighborLoader(
        data,
        num_neighbors=[15, 10],    # Sample 15 neighbors at hop-1, 10 at hop-2
        batch_size=1024,
        input_nodes=("address", data["address"].train_mask),
        shuffle=True,
        num_workers=4,
    )
    
    val_loader = NeighborLoader(
        data,
        num_neighbors=[15, 10],
        batch_size=2048,
        input_nodes=("address", data["address"].val_mask),
        num_workers=4,
    )
    
    # Model
    model = PhishingGNN(
        in_channels=data["address"].x.shape[1],
        hidden_channels=128,
        out_channels=2,
        num_layers=2,
        dropout=0.3,
    ).to(device)
    
    # Distributed Data Parallel
    model = torch.nn.parallel.DistributedDataParallel(
        model, device_ids=[local_rank]
    )
    
    # Class imbalance: compute class weights
    labels = data["address"].y[data["address"].train_mask]
    n_normal = (labels == 0).sum().float()
    n_phishing = (labels == 1).sum().float()
    class_weights = torch.tensor(
        [1.0, n_normal / n_phishing], device=device
    )
    criterion = torch.nn.CrossEntropyLoss(weight=class_weights)
    
    optimizer = torch.optim.AdamW(model.parameters(), lr=1e-3, weight_decay=1e-4)
    scheduler = torch.optim.lr_scheduler.CosineAnnealingLR(optimizer, T_max=50)
    
    # Training loop
    best_f1 = 0
    for epoch in range(50):
        model.train()
        total_loss = 0
        
        for batch in train_loader:
            batch = batch.to(device)
            optimizer.zero_grad()
            
            out = model(batch.x_dict, batch.edge_index_dict)
            mask = batch["address"].train_mask
            loss = criterion(out[mask], batch["address"].y[mask])
            
            loss.backward()
            torch.nn.utils.clip_grad_norm_(model.parameters(), 1.0)
            optimizer.step()
            total_loss += loss.item()
        
        scheduler.step()
        
        # Validation
        if local_rank == 0 and epoch % 5 == 0:
            model.eval()
            all_preds, all_labels, all_probs = [], [], []
            
            with torch.no_grad():
                for batch in val_loader:
                    batch = batch.to(device)
                    out = model(batch.x_dict, batch.edge_index_dict)
                    mask = batch["address"].val_mask
                    
                    probs = F.softmax(out[mask], dim=1)[:, 1]
                    preds = out[mask].argmax(dim=1)
                    
                    all_preds.extend(preds.cpu().tolist())
                    all_labels.extend(batch["address"].y[mask].cpu().tolist())
                    all_probs.extend(probs.cpu().tolist())
            
            f1 = f1_score(all_labels, all_preds)
            prec = precision_score(all_labels, all_preds)
            rec = recall_score(all_labels, all_preds)
            auroc = roc_auc_score(all_labels, all_probs)
            auprc = average_precision_score(all_labels, all_probs)
            
            print(f"Epoch {epoch}: Loss={total_loss:.4f} "
                  f"F1={f1:.4f} Prec={prec:.4f} Rec={rec:.4f} "
                  f"AUROC={auroc:.4f} AUPRC={auprc:.4f}")
            
            # Log to MLflow
            mlflow.log_metrics({
                "val_f1": f1, "val_precision": prec,
                "val_recall": rec, "val_auroc": auroc,
                "val_auprc": auprc, "train_loss": total_loss,
            }, step=epoch)
            
            # Save best model
            if f1 > best_f1:
                best_f1 = f1
                torch.save({
                    "model_state_dict": model.module.state_dict(),
                    "epoch": epoch,
                    "f1": f1,
                    "auroc": auroc,
                }, f"{checkpoint_dir}/best_model.pt")
                print(f"  Saved best model (F1={f1:.4f})")
    
    dist.destroy_process_group()
    return best_f1

# === Launch distributed training on Spark ===
spark = SparkSession.builder \
    .appName("GNN Distributed Training") \
    .config("spark.executor.resource.gpu.amount", "1") \
    .getOrCreate()

# Run training across 2 workers, each with 1 GPU
distributor = TorchDistributor(
    num_processes=2,         # Number of worker processes
    local_mode=False,        # Run on cluster (not driver)
    use_gpu=True,            # Use GPUs on executors
)

best_f1 = distributor.run(train_gnn, "/data/ethereum/models/")
print(f"Training complete! Best F1: {best_f1:.4f}")
```

### 4.4 Model Evaluation

```python
# evaluate.py
"""Run after training. Loads best model and evaluates on test set."""
import torch
from models import PhishingGNN
from sklearn.metrics import classification_report, confusion_matrix
import matplotlib.pyplot as plt
import seaborn as sns

# Load model
checkpoint = torch.load("/data/ethereum/models/best_model.pt")
model = PhishingGNN(in_channels=22, hidden_channels=128, out_channels=2)
model.load_state_dict(checkpoint["model_state_dict"])
model.eval()

# Load test data
data = torch.load("/data/ethereum/processed/pyg_graph.pt")

# Inference
with torch.no_grad():
    out = model(data.x_dict, data.edge_index_dict)
    test_mask = data["address"].test_mask
    
    probs = torch.softmax(out[test_mask], dim=1)[:, 1].numpy()
    preds = out[test_mask].argmax(dim=1).numpy()
    labels = data["address"].y[test_mask].numpy()

# Classification report
print(classification_report(
    labels, preds, 
    target_names=["Normal", "Phishing"],
    digits=4
))

# Confusion matrix
cm = confusion_matrix(labels, preds)
print(f"\nConfusion Matrix:\n{cm}")
print(f"\nFalse Positive Rate: {cm[0,1]/(cm[0,0]+cm[0,1]):.4f}")
print(f"False Negative Rate: {cm[1,0]/(cm[1,0]+cm[1,1]):.4f}")
```

---

## 5. Phase 4: Web Application — Real-time Phishing Classifier

### 5.1 FastAPI Backend

```python
# api/main.py
"""
REST API for real-time phishing classification.
Accepts an Ethereum address, extracts subgraph, runs GNN inference.
"""
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import torch
import numpy as np
from cassandra.cluster import Cluster
from models import PhishingGNN
import time

app = FastAPI(title="Ethereum Phishing Detector API")

# --- Load model at startup ---
MODEL_PATH = "/data/ethereum/models/best_model.pt"
checkpoint = torch.load(MODEL_PATH, map_location="cpu")
model = PhishingGNN(in_channels=22, hidden_channels=128, out_channels=2)
model.load_state_dict(checkpoint["model_state_dict"])
model.eval()

# --- Connect to Cassandra ---
cassandra_cluster = Cluster(["node1", "node2", "node3"])
session = cassandra_cluster.connect("ethereum")

# --- Schemas ---
class PredictionRequest(BaseModel):
    address: str

class PredictionResponse(BaseModel):
    address: str
    prediction: str           # "phishing" or "normal"
    phishing_probability: float
    confidence: str           # "high", "medium", "low"
    inference_time_ms: float
    num_neighbors: int
    risk_factors: list

# --- Helper: Extract subgraph from Cassandra ---
def extract_subgraph(target_address: str, max_neighbors: int = 50):
    """
    Extract 1-hop subgraph around target address from Cassandra.
    Returns node features and edge index for GNN inference.
    """
    # Get target node features
    target_row = session.execute(
        "SELECT * FROM address_features WHERE address = %s",
        (target_address,)
    ).one()
    
    if target_row is None:
        return None, None, None
    
    # Get neighbors
    neighbors = session.execute(
        "SELECT neighbor, edge_count, total_value "
        "FROM neighbor_index WHERE address = %s LIMIT %s",
        (target_address, max_neighbors)
    ).all()
    
    # Get neighbor features
    all_addresses = [target_address] + [n.neighbor for n in neighbors]
    features_list = []
    
    for addr in all_addresses:
        row = session.execute(
            "SELECT * FROM address_features WHERE address = %s",
            (addr,)
        ).one()
        if row:
            features_list.append(extract_feature_vector(row))
        else:
            features_list.append(np.zeros(22))  # default features
    
    # Build mini-graph
    x = torch.tensor(np.array(features_list), dtype=torch.float32)
    
    # Edges: target (idx=0) connected to all neighbors
    src = [0] * len(neighbors) + list(range(1, len(neighbors) + 1))
    dst = list(range(1, len(neighbors) + 1)) + [0] * len(neighbors)
    edge_index = torch.tensor([src, dst], dtype=torch.long)
    
    return x, edge_index, len(neighbors)

def extract_feature_vector(row):
    """Extract 22-dim feature vector from Cassandra row."""
    return np.array([
        row.total_tx_count or 0, row.total_tx_in or 0,
        row.total_tx_out or 0, row.total_value_in or 0,
        row.total_value_out or 0, row.avg_value_in or 0,
        row.avg_value_out or 0, row.unique_counterparties or 0,
        row.account_age_days or 0, 1 if row.is_contract else 0,
        row.token_diversity or 0, row.avg_gas_price or 0,
        row.max_single_tx_value or 0, row.tx_frequency_per_day or 0,
        # ... remaining features
    ], dtype=np.float32)

def identify_risk_factors(features, prob):
    """Identify why the model flagged this address."""
    factors = []
    if features[8] < 7:  # account_age_days
        factors.append("Very new account (< 7 days old)")
    if features[0] > 100 and features[8] < 30:
        factors.append("High transaction count in short period")
    if features[7] < 3:
        factors.append("Very few unique counterparties")
    if features[4] > 0 and features[3] / features[4] > 10:
        factors.append("Extreme value in/out imbalance")
    if prob > 0.9:
        factors.append("Pattern strongly matches known phishing behavior")
    return factors

# --- API Endpoints ---
@app.post("/predict", response_model=PredictionResponse)
async def predict_address(req: PredictionRequest):
    start = time.time()
    
    address = req.address.lower()
    x, edge_index, n_neighbors = extract_subgraph(address)
    
    if x is None:
        raise HTTPException(
            status_code=404, 
            detail=f"Address {address} not found in database"
        )
    
    # Run GNN inference
    with torch.no_grad():
        # For homogeneous inference
        out = model.forward_homogeneous(x, edge_index)
        prob = torch.softmax(out[0], dim=0)[1].item()
    
    prediction = "phishing" if prob > 0.5 else "normal"
    confidence = "high" if abs(prob - 0.5) > 0.3 else \
                 "medium" if abs(prob - 0.5) > 0.15 else "low"
    
    risk_factors = identify_risk_factors(x[0].numpy(), prob)
    inference_time = (time.time() - start) * 1000
    
    return PredictionResponse(
        address=address,
        prediction=prediction,
        phishing_probability=round(prob, 4),
        confidence=confidence,
        inference_time_ms=round(inference_time, 2),
        num_neighbors=n_neighbors,
        risk_factors=risk_factors,
    )

@app.get("/health")
async def health():
    return {"status": "ok", "model_loaded": True}

@app.get("/stats")
async def stats():
    """Return dataset statistics."""
    total = session.execute(
        "SELECT COUNT(*) FROM address_features"
    ).one()[0]
    phishing = session.execute(
        "SELECT COUNT(*) FROM address_features WHERE label = 1 ALLOW FILTERING"
    ).one()[0]
    return {
        "total_addresses": total,
        "phishing_addresses": phishing,
        "model_f1": checkpoint.get("f1", "N/A"),
        "model_auroc": checkpoint.get("auroc", "N/A"),
    }
```

### 5.2 Streamlit Frontend Dashboard

```python
# app/dashboard.py
"""
Streamlit dashboard for real-time Ethereum phishing detection.
"""
import streamlit as st
import requests
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import networkx as nx
import time

API_URL = "http://localhost:8000"

# --- Page Config ---
st.set_page_config(
    page_title="Ethereum Phishing Detector",
    page_icon="🛡️",
    layout="wide"
)

st.title("🛡️ Ethereum Phishing Detection System")
st.caption("GNN-based real-time phishing classifier | Big Data Pipeline")

# --- Sidebar ---
st.sidebar.header("🔍 Check an Address")
address_input = st.sidebar.text_input(
    "Ethereum Address",
    placeholder="0x742d35Cc6634C0532925a3b844Bc9e7595f2bD18",
)
check_btn = st.sidebar.button("🔎 Classify Address", type="primary")

# --- Main area: tabs ---
tab1, tab2, tab3 = st.tabs([
    "📊 Live Classification", 
    "📈 Model Performance", 
    "🗂️ Recent Transactions"
])

with tab1:
    if check_btn and address_input:
        with st.spinner("Extracting subgraph and running GNN inference..."):
            try:
                resp = requests.post(
                    f"{API_URL}/predict",
                    json={"address": address_input}
                )
                result = resp.json()
                
                # Display result
                col1, col2, col3 = st.columns(3)
                
                with col1:
                    if result["prediction"] == "phishing":
                        st.error(f"🚨 **PHISHING DETECTED**")
                    else:
                        st.success(f"✅ **NORMAL ACCOUNT**")
                
                with col2:
                    st.metric(
                        "Phishing Probability", 
                        f"{result['phishing_probability']:.1%}"
                    )
                
                with col3:
                    st.metric(
                        "Inference Time", 
                        f"{result['inference_time_ms']:.1f} ms"
                    )
                
                # Confidence gauge
                fig = go.Figure(go.Indicator(
                    mode="gauge+number",
                    value=result["phishing_probability"] * 100,
                    title={"text": "Phishing Risk Score"},
                    gauge={
                        "axis": {"range": [0, 100]},
                        "bar": {"color": "darkred" 
                                if result["phishing_probability"] > 0.5 
                                else "green"},
                        "steps": [
                            {"range": [0, 30], "color": "#d4edda"},
                            {"range": [30, 70], "color": "#fff3cd"},
                            {"range": [70, 100], "color": "#f8d7da"},
                        ],
                    }
                ))
                fig.update_layout(height=300)
                st.plotly_chart(fig, use_container_width=True)
                
                # Risk factors
                if result["risk_factors"]:
                    st.subheader("⚠️ Risk Factors")
                    for factor in result["risk_factors"]:
                        st.warning(f"• {factor}")
                
                # Address details
                st.subheader("📋 Address Details")
                st.json({
                    "address": result["address"],
                    "prediction": result["prediction"],
                    "confidence": result["confidence"],
                    "neighbors_analyzed": result["num_neighbors"],
                })
                
            except Exception as e:
                st.error(f"Error: {str(e)}")
    
    else:
        st.info("👆 Enter an Ethereum address in the sidebar to classify it.")

with tab2:
    st.subheader("Model Performance Metrics")
    
    # Fetch model stats
    try:
        stats = requests.get(f"{API_URL}/stats").json()
        
        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Total Addresses", f"{stats['total_addresses']:,}")
        col2.metric("Phishing Labels", f"{stats['phishing_addresses']:,}")
        col3.metric("Model F1-Score", f"{stats['model_f1']:.4f}")
        col4.metric("Model AUROC", f"{stats['model_auroc']:.4f}")
    except:
        st.warning("API not available. Showing placeholder metrics.")

with tab3:
    st.subheader("Recent Transaction Stream")
    st.caption("Simulated real-time stream of Ethereum transactions")
    
    # Placeholder for streaming transactions
    # In production, this connects to Spark Streaming / Kafka
    placeholder = st.empty()
    
    if st.button("▶️ Start Stream"):
        for i in range(20):
            with placeholder.container():
                fake_addr = f"0x{''.join(['abcdef0123456789'[i%16] for _ in range(40)])}"
                is_phishing = i % 7 == 0
                
                cols = st.columns([3, 1, 1])
                cols[0].code(fake_addr)
                if is_phishing:
                    cols[1].error("🚨 Phishing")
                else:
                    cols[1].success("✅ Normal")
                cols[2].caption(f"{time.strftime('%H:%M:%S')}")
            
            time.sleep(0.5)
```

### 5.3 Running the Application

```bash
# Terminal 1: Start FastAPI backend
cd api/
uvicorn main:app --host 0.0.0.0 --port 8000 --workers 4

# Terminal 2: Start Streamlit frontend
cd app/
streamlit run dashboard.py --server.port 8501

# Access at: http://localhost:8501
```

---

## 6. Deployment Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                    PRODUCTION DEPLOYMENT                         │
│                                                                  │
│  ┌──────────┐    ┌──────────┐    ┌──────────┐                  │
│  │  Node 1   │    │  Node 2   │    │  Node 3   │                │
│  │           │    │           │    │           │                │
│  │  HDFS NN  │    │  HDFS DN  │    │  HDFS DN  │                │
│  │  Spark M  │    │  Spark W  │    │  Spark W  │                │
│  │  Cassandra│    │  Cassandra│    │  Cassandra│                │
│  │  GPU: A100│    │  GPU: A100│    │  GPU: A100│                │
│  └─────┬─────┘    └─────┬─────┘    └─────┬─────┘               │
│        │                │                │                      │
│        └────────────────┼────────────────┘                      │
│                         │                                        │
│              ┌──────────┴──────────┐                             │
│              │   Load Balancer      │                             │
│              │   (Nginx)            │                             │
│              └──────────┬──────────┘                             │
│                         │                                        │
│         ┌───────────────┼───────────────┐                       │
│         │               │               │                       │
│   ┌─────┴─────┐  ┌─────┴─────┐  ┌─────┴─────┐                │
│   │ FastAPI    │  │ FastAPI    │  │ Streamlit  │                │
│   │ Worker 1   │  │ Worker 2   │  │ Dashboard  │                │
│   │ :8000      │  │ :8001      │  │ :8501      │                │
│   └───────────┘  └───────────┘  └───────────┘                  │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

### Docker Compose (Development)

```yaml
# docker-compose.yml
version: "3.8"
services:
  cassandra:
    image: cassandra:4.1
    ports: ["9042:9042"]
    volumes: ["cassandra_data:/var/lib/cassandra"]
    environment:
      CASSANDRA_CLUSTER_NAME: ethereum_cluster

  api:
    build: ./api
    ports: ["8000:8000"]
    depends_on: [cassandra]
    volumes: ["./models:/data/ethereum/models"]
    deploy:
      resources:
        reservations:
          devices:
            - driver: nvidia
              count: 1
              capabilities: [gpu]

  dashboard:
    build: ./app
    ports: ["8501:8501"]
    depends_on: [api]
    environment:
      API_URL: http://api:8000
```

---

## 7. Directory Structure

```
ethereum-phishing-gnn/
├── README.md
├── docker-compose.yml
├── requirements.txt
│
├── crawl/                          # Phase 1: Data Crawling
│   ├── crawl_labels.py             # Crawl phishing labels
│   ├── crawl_transactions.py       # BigQuery extraction
│   ├── store_cassandra.py          # Store to cluster DB
│   └── config.yaml                 # API keys, cluster config
│
├── etl/                            # Phase 2: Spark Processing
│   ├── spark_etl.py                # Main ETL pipeline
│   ├── split_data.py               # Train/val/test split
│   ├── build_pyg_graph.py          # Convert to PyG format
│   └── spark-defaults.conf         # Spark cluster config
│
├── training/                       # Phase 3: Model Training
│   ├── models.py                   # GNN model definitions
│   ├── train_distributed.py        # Spark distributed training
│   ├── train_single.py             # Single-GPU training
│   ├── evaluate.py                 # Model evaluation
│   └── baselines.py                # XGBoost, RF baselines
│
├── api/                            # Phase 4: Serving
│   ├── main.py                     # FastAPI backend
│   ├── Dockerfile
│   └── requirements.txt
│
├── app/                            # Phase 4: Frontend
│   ├── dashboard.py                # Streamlit app
│   ├── Dockerfile
│   └── requirements.txt
│
├── notebooks/                      # Exploration
│   ├── 01_data_exploration.ipynb
│   ├── 02_feature_analysis.ipynb
│   └── 03_model_comparison.ipynb
│
├── models/                         # Saved models
│   └── best_model.pt
│
└── data/                           # Local data (dev only)
    ├── xblock_phishing_labels.csv
    └── dahgnn_phishing_labels.csv
```

---

## Summary

| Phase | Input | Output | Big Data Component |
|-------|-------|--------|-------------------|
| **Phase 1: Crawl** | BigQuery + Etherscan + Academic datasets | Raw Parquet on HDFS + Labels in Cassandra | HDFS (3-node cluster), Cassandra |
| **Phase 2: Process** | Raw data on HDFS | Node features + Edge features + PyG graph | PySpark ETL, GraphFrames, Cassandra |
| **Phase 3: Train** | PyG graph + Labels | Trained GNN model (best_model.pt) | Spark TorchDistributor, DistDGL |
| **Phase 4: Serve** | New address input | Phishing/Normal classification + probability | FastAPI + Streamlit + Cassandra lookups |

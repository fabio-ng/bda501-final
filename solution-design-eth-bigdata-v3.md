# Solution Design: ETH Transaction Analytics Platform

**Course:** Big Data
**Tech Stack:** GCS · Apache Kafka · Apache Spark · Cassandra · PostgreSQL · Python
**Version:** 3.0

---

## Table of Contents

1. [System Overview](#1-system-overview)
2. [Architecture](#2-architecture)
3. [Layer Breakdown](#3-layer-breakdown)
   - [3.1 Source](#31-source)
   - [3.1.1 Historical Bootstrap from BigQuery](#311-historical-bootstrap-from-google-bigquery)
   - [3.2 Ingestion](#32-ingestion)
   - [3.3 Processing](#33-processing)
   - [3.4 Machine Learning Pipeline](#34-machine-learning-pipeline)
   - [3.5 Serving](#35-serving)
4. [GCS Bucket Structure](#4-gcs-bucket-structure)
5. [Cassandra Data Model](#5-cassandra-data-model)
6. [PostgreSQL Data Model](#6-postgresql-data-model)
7. [Data Flow](#7-data-flow)
8. [Web Application](#8-web-application)
9. [Fault Tolerance & Monitoring](#9-fault-tolerance--monitoring)
10. [Tech Stack Summary](#10-tech-stack-summary)
11. [Design Decisions & Trade-offs](#11-design-decisions--trade-offs)

---

## 1. System Overview

This platform collects Ethereum on-chain transaction data, computes daily top-100 wallet rankings by transaction count, builds a 180-day transaction graph, classifies wallets by behavioral clustering, and exposes all of it through a REST API and interactive web visualization.

**Four functional requirements drive the design:**

1. **Crawl** — ingest ETH transactions in near real-time and persist them in a distributed object store.
2. **Snapshot** — every day, compute the top 100 wallets by transaction volume and store a ranked snapshot.
3. **Classify** — use distributed ML (Spark MLlib) to cluster wallets by behavioral features and flag anomalous wallets.
4. **Visualize** — serve a website that lists the daily top 100 with cluster labels, and on wallet click, renders a force-directed graph showing transaction relationships over the past 180 days, with anomalous wallets highlighted.

**Core design principles:**

- GCS acts as the **data lake** (raw + processed Parquet files, ML features, trained models). It is the single source of truth.
- Cassandra acts as the **distributed transaction store** — serves real-time wallet transaction history queries, optimized for high write throughput and partition-key lookups.
- PostgreSQL acts as the **serving database** (aggregated snapshots, graph edges, cluster results). It never stores raw transactions.
- Spark is the only component that writes to PostgreSQL — from batch jobs triggered by Airflow.
- Kafka decouples the crawler from storage, enabling replay and fan-out to multiple consumers.
- All ETH monetary values use `NUMERIC` / `DECIMAL` precision — never floating-point.

---

## 2. Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│  SOURCE                                                             │
│  ┌────────────────────────────────────┐                             │
│  │  ETH Blockchain  (Mainnet RPC)     │                             │
│  └────────────────────────────────────┘                             │
└──────────────────────────┬──────────────────────────────────────────┘
                           │ JSON-RPC (~12s/block)
┌──────────────────────────▼──────────────────────────────────────────┐
│  INGESTION                                                           │
│  ┌──────────────────┐    ┌─────────────┐    ┌────────────────────┐  │
│  │  Python Crawler  │───▶│    Kafka    │──┬▶│  GCS raw zone      │  │
│  │  web3.py/Infura  │    │  eth-txns   │  │ │  Parquet/date      │  │
│  └──────────────────┘    └─────────────┘  │ └────────────────────┘  │
│  ┌──────────────────────────────────┐     │ ┌────────────────────┐  │
│  │  Gap Detector: scan raw/ for     │     └▶│  Cassandra         │  │
│  │  missing blocks → backfill RPC  │        │  wallet_transactions│  │
│  └──────────────────────────────────┘       └────────────────────┘  │
└─────────────────────────────────────────────────────────────────────┘
                           │ Spark reads GCS raw
┌──────────────────────────▼──────────────────────────────────────────┐
│  PROCESSING                                                          │
│  ┌──────────────┐   ┌─────────────────────┐  ┌───────────────────┐  │
│  │   Airflow    │──▶│  Spark Job 1        │─▶│ GCS processed     │  │
│  │  (triggers)  │   │  Daily Snapshot     │  │ snapshots/        │  │
│  │              │   └─────────┬───────────┘  └───────────────────┘  │
│  │              │             │                                      │
│  │              │   ┌─────────▼───────────┐  ┌───────────────────┐  │
│  │              │──▶│  Spark Job 2        │─▶│ GCS processed     │  │
│  │              │   │  Incremental Edges  │  │ graph_edges/      │  │
│  │              │   └─────────┬───────────┘  └───────────────────┘  │
│  │              │             │                                      │
│  │              │   ┌─────────▼───────────┐  ┌───────────────────┐  │
│  │              │──▶│  Spark Job 3        │─▶│ GCS processed     │  │
│  │              │   │  Feature Engineer.  │  │ ml_features/      │  │
│  │              │   └─────────┬───────────┘  └───────────────────┘  │
│  │              │             │                                      │
│  │              │   ┌─────────▼───────────┐  ┌───────────────────┐  │
│  │              │──▶│  Spark Job 4        │─▶│ GCS models/       │  │
│  │              │   │  ML Training        │  │ + PostgreSQL      │  │
│  │              │   │  (K-Means + Anomaly)│  │ wallet_cluster    │  │
│  │              │   └─────────┬───────────┘  └───────────────────┘  │
│  │              │             │                                      │
│  │  Validation  │◀────────────┘  writes aggregated to PG            │
│  └──────────────┘                                                    │
└──────────────────────────────────────────────────────────────────────┘
                              │
┌─────────────────────────────▼───────────────────────────────────────┐
│  SERVING                                                             │
│  ┌──────────────┐   ┌──────────────┐                                │
│  │  PostgreSQL  │──▶│              │    ┌─────────────────────────┐ │
│  │  top100+graph│   │   FastAPI    │───▶│    Web + D3.js          │ │
│  │  +clusters   │   │   REST API  │    │  React, force graph     │ │
│  │              │   │              │    │  cluster colors         │ │
│  ├──────────────┤   │              │    │  anomaly highlights     │ │
│  │  Cassandra   │──▶│              │    └─────────────────────────┘ │
│  │  tx history  │   └──────────────┘                                │
│  └──────────────┘                                                    │
└─────────────────────────────────────────────────────────────────────┘
```

---

## 3. Layer Breakdown

### 3.1 Source

| Item | Detail |
|---|---|
| Network | Ethereum Mainnet |
| Interface | JSON-RPC via Infura or Alchemy |
| Block rate | ~12 seconds per block |
| Tx per block | ~100–200 average |
| Fields captured | `tx_hash`, `block_number`, `timestamp`, `from`, `to`, `value_eth`, `gas`, `gas_price` |

---

### 3.1.1 Historical Bootstrap from Google BigQuery

Crawling from genesis block (~block 0, July 2015) via RPC would take weeks to months due to rate limits and sheer volume (~2B+ transactions to date). Instead, the platform bootstraps historical data directly from Google's public BigQuery dataset.

**Data source:** `bigquery-public-data.crypto_ethereum.transactions` — a maintained, complete mirror of all Ethereum mainnet transactions, updated daily by Google.

**Bootstrap procedure (one-time, before pipeline goes live):**

```
Step 1: Export from BigQuery → GCS (as Parquet, partitioned by date)
────────────────────────────────────────────────────────────────────
  BigQuery SQL:
    EXPORT DATA OPTIONS (
      uri = 'gs://eth-bigdata-project/raw/transactions/dt=*/part-*.parquet',
      format = 'PARQUET',
      overwrite = true
    ) AS
    SELECT
      transaction_hash  AS tx_hash,
      block_number,
      block_timestamp   AS timestamp,
      from_address      AS `from`,
      to_address        AS `to`,
      CAST(value AS NUMERIC) / 1e18  AS value_eth,
      gas,
      gas_price
    FROM `bigquery-public-data.crypto_ethereum.transactions`
    WHERE block_timestamp >= TIMESTAMP('2024-10-01')
      AND block_timestamp <  TIMESTAMP('2025-04-01')

Step 2: Verify exported partitions
────────────────────────────────────────────────────────────────────
  - Check GCS: each dt=YYYY-MM-DD/ folder should have non-empty Parquet files
  - Spot-check row counts against BigQuery: SELECT DATE(block_timestamp), COUNT(*)

Step 3: Backfill Cassandra from GCS
────────────────────────────────────────────────────────────────────
  - Run a one-time Spark job that reads GCS raw Parquet and writes
    to Cassandra wallet_transactions table (denormalized: 2 rows per tx)
  - Use spark-cassandra-connector with batch writes

Step 4: Run initial Spark jobs
────────────────────────────────────────────────────────────────────
  - Trigger `spark_full_recompute_edges` to build the initial 180-day edge aggregation
  - Trigger `spark_daily_snapshot` for each historical date (backfill mode)
  - Trigger ML pipeline (Job 3 + Job 4) for initial wallet clustering

Step 5: Set crawler checkpoint
────────────────────────────────────────────────────────────────────
  - Set `last_processed_block` to the latest block in the exported data
  - Start the live crawler — it continues from this block forward
```

**Why BigQuery over other sources:**

| Option | Drawback |
|---|---|
| Etherscan CSV export | Limited to 5,000 rows/export, rate-limited API |
| Third-party APIs (Moralis, Alchemy) | Rate limits, pagination complexity, cost at scale |
| Running own archive node | Requires 2TB+ disk, days to sync, operational overhead |
| **BigQuery (chosen)** | Free tier covers small exports, native Parquet export to GCS, SQL-based filtering, no rate limits |

> **Cost note:** BigQuery charges ~$6.25/TB scanned. A 180-day window of ETH transactions is approximately 50–100 GB, costing under $1 for the initial export. Subsequent daily data comes from the live crawler, not BigQuery.

---

### 3.2 Ingestion

**Python Crawler** is the sole Kafka producer. It polls the ETH RPC for each new block, parses all transactions, and publishes one message per transaction to the `eth-txns` Kafka topic. Every 10 blocks (~2 minutes), it also flushes a Parquet file directly to GCS raw zone as a persistence safety net — independent of Kafka.

The crawler persists a `last_processed_block` checkpoint to a local file (and GCS). On restart, it resumes from this checkpoint. A **gap detector** runs at startup: it scans existing GCS raw partitions, identifies any missing block ranges, and backfills them from RPC before resuming live polling.

**Kafka** acts as a durable buffer and decoupling layer. Two consumer groups read from the `eth-txns` topic:

1. **GCS writer** — flushes Parquet files to GCS raw zone (existing)
2. **Cassandra writer** — writes denormalized rows to Cassandra in near real-time (new)

| Kafka Config | Value |
|---|---|
| Topic | `eth-txns` |
| Partitions | 6 |
| Replication factor | 2 |
| Retention | 7 days |
| Consumer groups | `gcs-writer`, `cassandra-writer` |
| Message format | JSON (schema enforced at application level) |

> **Note on JSON format:** JSON is used for simplicity in a course context. The crawler validates every message against a fixed schema before publishing. In production, Avro + Confluent Schema Registry would be preferred for schema evolution and compact encoding.

**GCS raw zone** is the immutable source of truth for batch processing. Data is written once and never modified. Partitioned by date (`dt=YYYY-MM-DD`) to enable partition pruning when Spark reads a specific day.

**Cassandra** receives every transaction in near real-time via the `cassandra-writer` consumer. Each transaction is written as 2 rows (one per wallet role: sender and receiver) for partition-key-based lookup. See [Section 5: Cassandra Data Model](#5-cassandra-data-model) for details.

---

### 3.3 Processing

**Airflow** orchestrates all batch jobs. Daily DAG triggered at 00:05 UTC with sequential task dependencies:

```
validation_gate → spark_daily_snapshot → spark_incremental_edges → spark_feature_engineering → spark_ml_training
```

On failure, Airflow retries up to 3 times before alerting via Slack webhook.

**Validation gate:**
Before Spark runs, Airflow runs a lightweight validation task:

- Check that the target GCS raw partition (`dt=yesterday`) exists and is non-empty.
- Check row count is within expected bounds (e.g., 50K–200K transactions/day).
- If validation fails → skip all Spark jobs, alert, prevent bad data from entering PostgreSQL.

---

**Spark Batch** performs four jobs:

**Job 1 — Daily Snapshot:**

- Read GCS raw partition for `TARGET_DATE`
- Group transactions by sender (`from`) and receiver (`to`) wallet
- Compute `total_volume` (sent ETH + received ETH) and `total_txns` (sent count + received count)
- Apply `DENSE_RANK` ordered by `total_txns` descending
- Filter top 100 wallets
- Write result to: GCS processed snapshot + PostgreSQL `wallet_daily_snapshot` table

**Job 2 — Incremental Edge Aggregation (180-day rolling window):**

Instead of re-scanning 180 days of raw data every day, Job 2 uses an **incremental sliding window** approach:

```
Day N processing:
  1. Read current edge aggregation from GCS  (edge_aggregate_prev)
  2. Read raw data for day N               (day_entering)
     → aggregate into (from, to) → SUM(value), COUNT(*)
  3. Read raw data for day N-180           (day_exiting)
     → aggregate into (from, to) → SUM(value), COUNT(*)
  4. Merge:
     edge_aggregate_new = edge_aggregate_prev
                        + day_entering
                        - day_exiting
  5. Remove edges where tx_count <= 0
  6. Write edge_aggregate_new → GCS processed (new partition)
  7. Write edge_aggregate_new → PostgreSQL wallet_graph_edge (staging → atomic swap)
```

This reduces daily scan from **180 days (~18M rows)** to **2 days (~200K rows)** — a ~90x reduction.

**Edge aggregate state on GCS** is stored with date-versioned partitions (`run_date=YYYY-MM-DD/`), so previous states are preserved. If an incremental run produces bad results, we can revert to a prior version and recompute.

> **Full recomputation fallback:** A separate Airflow DAG (`spark_full_recompute_edges`) can be triggered manually to rebuild the entire 180-day aggregation from raw data. This is used for initial bootstrap or disaster recovery, not daily runs.

---

### 3.4 Machine Learning Pipeline

The ML pipeline runs as Spark Jobs 3 and 4, scheduled daily after edge aggregation completes. It uses **unsupervised learning** to cluster wallets by behavioral patterns and detect anomalies — no labeled data required.

**Job 3 — Feature Engineering:**

Reads the last 30 days of raw transactions from GCS and computes a feature vector per wallet.

```
Input:  GCS raw/transactions/dt=[today-30 .. yesterday]
Output: GCS processed/ml_features/dt=YYYY-MM-DD/

Feature groups:

  Frequency features (5):
    - tx_count_sent            number of outbound transactions
    - tx_count_recv            number of inbound transactions
    - tx_ratio                 sent / (sent + recv), range [0, 1]
    - active_days              number of distinct days with ≥1 tx
    - avg_tx_per_active_day    total_tx / active_days

  Volume features (5):
    - total_volume_sent        sum of ETH sent
    - total_volume_recv        sum of ETH received
    - avg_tx_value             mean ETH per transaction
    - max_tx_value             largest single transaction
    - std_tx_value             standard deviation of tx values

  Graph features (3):
    - unique_counterparties    count of distinct (from, to) partners
    - top_counterparty_pct     % of volume concentrated in top 1 counterparty
    - in_out_degree_ratio      unique receivers / unique senders

  Time pattern features (3):
    - peak_hour                hour (0–23) with most transactions
    - hour_entropy             Shannon entropy of hourly tx distribution
                               (high = spread evenly, low = concentrated)
    - weekend_ratio            % of transactions on Saturday/Sunday

  Total: 16 features per wallet
```

**Minimum activity filter:** Only wallets with ≥10 transactions in the 30-day window are included. This removes dust wallets and reduces noise.

**Job 4 — Model Training & Scoring (Spark MLlib):**

```
Input:  GCS processed/ml_features/dt=yesterday
Output: GCS models/kmeans/version=YYYY-MM-DD/
        GCS processed/wallet_clusters/dt=YYYY-MM-DD/
        PostgreSQL wallet_cluster table

Pipeline steps:
  1. Load feature vectors from GCS
  2. VectorAssembler → combine 16 features into single vector
  3. StandardScaler → normalize (zero mean, unit variance)
  4. K-Means clustering:
     - k selected via silhouette score evaluation (k = 4 to 10)
     - Best k and trained model saved to GCS models/
  5. For each wallet:
     - Assign cluster_id (0 to k-1)
     - Compute anomaly_score = Euclidean distance to assigned centroid
     - Normalize anomaly_score to [0, 1] range (min-max within cluster)
  6. Flag wallets with anomaly_score > 0.85 as is_anomaly = true
  7. Write results → GCS + PostgreSQL
```

**Expected cluster interpretation:**

| Cluster | Behavioral pattern | Typical wallets |
|---|---|---|
| 0 | Low frequency, low volume, many counterparties | Individual users |
| 1 | Very high frequency, high volume, few counterparties | Exchanges / hot wallets |
| 2 | Uniform 24/7 activity, low hour_entropy | Bots / automated systems |
| 3 | Burst activity in 1–2 days then inactive | Airdrop / campaign wallets |
| 4 | High volume, top_counterparty_pct > 80% | Wash trading suspects |

> **Note:** Cluster labels are assigned manually after inspecting centroids. The model outputs numeric IDs only. Label assignment is stored in a config file (`cluster_labels.json`) and can be updated without retraining.

**Model retraining schedule:**

| Scenario | Trigger | Action |
|---|---|---|
| Daily scoring | Airflow daily DAG | Run Job 3 + Job 4 with latest model |
| Weekly retrain | Airflow weekly DAG (Sunday 02:00 UTC) | Re-evaluate k, retrain K-Means, save new model version |
| Manual retrain | Airflow trigger | Full retrain from any date range |

**Why K-Means over other algorithms:**

| Algorithm | Reason not chosen |
|---|---|
| DBSCAN | Not available in Spark MLlib distributed mode |
| Gaussian Mixture | Higher computational cost, harder to interpret |
| Isolation Forest | Spark MLlib lacks native support |
| **K-Means (chosen)** | Native Spark MLlib, distributed, interpretable centroids, fast on large datasets |

---

### 3.5 Serving

**PostgreSQL** is the relational serving database. It stores aggregated, query-ready data:

- Daily top-100 snapshots with cluster labels
- 180-day rolling graph edges
- Wallet cluster assignments and anomaly scores
- Data volume is small: ~100 rows/day for snapshots, bounded edge and cluster counts

**Cassandra** is the distributed transaction store. It serves real-time wallet transaction history:

- Optimized for point queries by wallet address + time range
- Handles high write throughput from the Kafka consumer
- Scales horizontally as transaction volume grows

**FastAPI** queries both databases:

- PostgreSQL for snapshots, graph edges, cluster data
- Cassandra for wallet transaction history

Rate limiting: 60 requests/minute per IP via `slowapi` middleware.

**Web + D3.js (React)** renders three views:

- **Table view** — top 100 wallets with cluster labels and anomaly flags
- **Graph view** — force-directed graph with nodes colored by cluster
- **Wallet detail view** — transaction history from Cassandra + cluster info + feature radar chart

---

## 4. GCS Bucket Structure

```
gs://eth-bigdata-project/
│
├── raw/
│   └── transactions/
│       └── dt=YYYY-MM-DD/
│           ├── part-{timestamp}-0.parquet
│           └── part-{timestamp}-1.parquet
│
├── processed/
│   ├── snapshots/
│   │   └── dt=YYYY-MM-DD/
│   │       └── part-00000.parquet
│   ├── graph_edges/
│   │   └── run_date=YYYY-MM-DD/
│   │       └── part-00000.parquet
│   ├── ml_features/
│   │   └── dt=YYYY-MM-DD/
│   │       └── part-00000.parquet
│   └── wallet_clusters/
│       └── dt=YYYY-MM-DD/
│           └── part-00000.parquet
│
├── models/
│   └── kmeans/
│       └── version=YYYY-MM-DD/
│           ├── model/                    ← Spark MLlib saved model
│           ├── scaler/                   ← StandardScaler saved model
│           ├── metadata.json             ← k, silhouette score, feature list
│           └── cluster_labels.json       ← human-assigned labels per cluster_id
│
├── checkpoints/
│   ├── ingest/
│   └── spark/
│
└── archive/
    └── transactions/
        └── dt=YYYY-MM-DD/
```

**Access pattern rules:**

| Zone | Written by | Read by | Mutability |
|---|---|---|---|
| `raw/` | Crawler, BigQuery export | Spark Batch | Immutable (append-only) |
| `processed/snapshots/` | Spark Job 1 | FastAPI (fallback) | Overwrite per date partition |
| `processed/graph_edges/` | Spark Job 2 | Spark Job 2 (incremental read) | Append new `run_date` daily |
| `processed/ml_features/` | Spark Job 3 | Spark Job 4 | Overwrite per date partition |
| `processed/wallet_clusters/` | Spark Job 4 | FastAPI (fallback) | Overwrite per date partition |
| `models/kmeans/` | Spark Job 4 (weekly) | Spark Job 4 (daily scoring) | Append new version weekly |
| `checkpoints/` | Crawler, Spark | Crawler, Spark | Read-write |
| `archive/` | Lifecycle policy | N/A | Immutable |

**Data retention policy:**

| Zone | Retention | Mechanism |
|---|---|---|
| `raw/` | 12 months hot | GCS Object Lifecycle: move to Nearline after 12 months, delete after 36 months |
| `processed/graph_edges/` | 30 days of run versions | Airflow cleanup task |
| `processed/ml_features/` | 30 days | Airflow cleanup task |
| `processed/snapshots/` | Indefinite | Small size (~100 rows/day) |
| `models/kmeans/` | 90 days of model versions | Airflow cleanup task |

---

## 5. Cassandra Data Model

### Cluster Configuration

| Config | Value |
|---|---|
| Keyspace | `eth_analytics` |
| Replication strategy | `SimpleStrategy` (course project) / `NetworkTopologyStrategy` (production) |
| Replication factor | 3 |
| Consistency level (writes) | `LOCAL_QUORUM` |
| Consistency level (reads) | `ONE` (eventual consistency acceptable for analytics) |

### Table: `wallet_transactions`

Stores every transaction denormalized by wallet. Each raw transaction produces **2 rows**: one for the sender (`role = 'sender'`) and one for the receiver (`role = 'receiver'`).

```cql
CREATE TABLE eth_analytics.wallet_transactions (
    wallet_address  TEXT,
    tx_timestamp    TIMESTAMP,
    tx_hash         TEXT,
    role            TEXT,              -- 'sender' or 'receiver'
    counterparty    TEXT,              -- the other wallet in the transaction
    value_eth       DECIMAL,
    gas             BIGINT,
    gas_price       BIGINT,
    block_number    BIGINT,
    PRIMARY KEY ((wallet_address), tx_timestamp, tx_hash)
) WITH CLUSTERING ORDER BY (tx_timestamp DESC, tx_hash ASC)
  AND default_time_to_live = 31536000   -- 365 days TTL
  AND compaction = {'class': 'TimeWindowCompactionStrategy',
                    'compaction_window_unit': 'DAYS',
                    'compaction_window_size': 7};
```

**Design rationale:**

| Decision | Reason |
|---|---|
| `wallet_address` as partition key | All transactions for a wallet co-located on the same node → single-partition reads |
| `tx_timestamp DESC` clustering | Most recent transactions returned first — matches UI pagination pattern |
| `tx_hash` in clustering key | Guarantees uniqueness within a partition (multiple tx in same second) |
| Denormalized (2 rows per tx) | Avoids cross-partition queries; Cassandra anti-pattern to query by non-partition-key |
| TTL 365 days | Auto-expire old data; raw history beyond 1 year available in GCS archive |
| `TimeWindowCompactionStrategy` | Optimal for time-series write-once data, reduces compaction overhead |

**Query patterns:**

```cql
-- Get recent transactions for a wallet (paginated)
SELECT * FROM wallet_transactions
WHERE wallet_address = '0xabc123...'
  AND tx_timestamp >= '2025-01-01'
  AND tx_timestamp <= '2025-04-01'
LIMIT 50;

-- Cursor-based pagination (next page)
SELECT * FROM wallet_transactions
WHERE wallet_address = '0xabc123...'
  AND tx_timestamp < '2025-03-15T10:30:00Z'
LIMIT 50;
```

**Estimated data volume:**

| Metric | Value |
|---|---|
| Transactions per day | ~100,000 |
| Rows per day (2x denormalized) | ~200,000 |
| Avg row size | ~200 bytes |
| Daily storage growth | ~40 MB |
| Annual storage (before compaction) | ~15 GB |
| With replication factor 3 | ~45 GB total |

---

## 6. PostgreSQL Data Model

### Table: `wallet_daily_snapshot`

One row per wallet per day. Populated by Spark Job 1 every morning. No separate `wallet` master table — wallet addresses are stored directly to avoid FK management overhead at ingestion time.

| Column | Type | Description |
|---|---|---|
| `id` | `SERIAL` PK | Auto-increment primary key |
| `wallet_address` | `VARCHAR(42)` | ETH wallet address (hex, checksummed) |
| `snapshot_date` | `DATE` | The day this snapshot covers |
| `rank` | `INT` | 1–100, ordered by `total_txns` descending |
| `total_volume` | `NUMERIC(38,18)` | `sent_eth + recv_eth` (exact precision) |
| `total_txns` | `BIGINT` | `sent_count + recv_count` |
| `sent_eth` | `NUMERIC(38,18)` | Total ETH sent on this date |
| `recv_eth` | `NUMERIC(38,18)` | Total ETH received on this date |
| `sent_count` | `BIGINT` | Number of outbound transactions |
| `recv_count` | `BIGINT` | Number of inbound transactions |
| `created_at` | `TIMESTAMP` | Row creation time |

**Indexes:**
- `UNIQUE (wallet_address, snapshot_date)` — prevents duplicates on Spark re-runs
- `INDEX (snapshot_date, rank)` — primary query pattern: get top 100 for a given date

---

### Table: `wallet_graph_edge`

One row per directed wallet pair, covering the 180-day rolling window. Updated incrementally by Spark Job 2 daily (full table replace via staging table swap).

| Column | Type | Description |
|---|---|---|
| `id` | `SERIAL` PK | Auto-increment primary key |
| `from_wallet` | `VARCHAR(42)` | Sender wallet |
| `to_wallet` | `VARCHAR(42)` | Receiver wallet |
| `total_volume` | `NUMERIC(38,18)` | Total ETH transferred between this pair |
| `tx_count` | `BIGINT` | Number of transactions between this pair |
| `period_start` | `DATE` | Start of the aggregation window |
| `period_end` | `DATE` | End of the aggregation window (= yesterday) |
| `updated_at` | `TIMESTAMP` | Last time Spark refreshed this row |

**Indexes:**
- `UNIQUE (from_wallet, to_wallet)` — one row per directed pair
- `INDEX (from_wallet)` — edges where wallet is sender
- `INDEX (to_wallet)` — edges where wallet is receiver

**Write strategy for `wallet_graph_edge`:**

Spark writes the full edge set to a staging table (`wallet_graph_edge_staging`), then Airflow executes an atomic swap:

```sql
BEGIN;
ALTER TABLE wallet_graph_edge RENAME TO wallet_graph_edge_old;
ALTER TABLE wallet_graph_edge_staging RENAME TO wallet_graph_edge;
DROP TABLE wallet_graph_edge_old;
COMMIT;
```

This ensures the API always reads a complete, consistent edge set — no partial writes visible.

---

### Table: `wallet_cluster`

One row per wallet per day. Populated by Spark Job 4. Contains cluster assignment and anomaly score.

| Column | Type | Description |
|---|---|---|
| `id` | `SERIAL` PK | Auto-increment primary key |
| `wallet_address` | `VARCHAR(42)` | ETH wallet address |
| `snapshot_date` | `DATE` | The date of this classification |
| `cluster_id` | `INT` | Cluster assignment (0 to k-1) |
| `cluster_label` | `VARCHAR(50)` | Human-readable label (e.g., 'exchange', 'bot', 'individual') |
| `anomaly_score` | `NUMERIC(5,4)` | Normalized distance to centroid [0.0000–1.0000] |
| `is_anomaly` | `BOOLEAN` | `true` if `anomaly_score > 0.85` |
| `feature_vector` | `JSONB` | All 16 features stored for debugging and UI display |
| `model_version` | `DATE` | Which trained model produced this result |
| `created_at` | `TIMESTAMP` | Row creation time |

**Indexes:**
- `UNIQUE (wallet_address, snapshot_date)` — prevents duplicates on re-runs
- `INDEX (snapshot_date, cluster_id)` — all wallets in a cluster for a given date
- `INDEX (snapshot_date, is_anomaly)` — anomalous wallets for a given date
- `INDEX (wallet_address)` — cluster history for a specific wallet

---

### Entity Relationships

```
wallet_daily_snapshot   — standalone, wallet_address as plain column
wallet_graph_edge       — standalone, from_wallet / to_wallet as plain columns
wallet_cluster          — standalone, wallet_address as plain column

Join pattern (API-level, not FK-enforced):
  wallet_daily_snapshot.wallet_address = wallet_cluster.wallet_address
  AND wallet_daily_snapshot.snapshot_date = wallet_cluster.snapshot_date
```

---

## 7. Data Flow

### End-to-end timeline

```
ETH Block (~12s)
    │
    ▼
Python Crawler polls RPC
    │  parse + validate schema + publish
    ▼
Kafka topic (eth-txns)
    ├── Consumer Group: gcs-writer
    │   └── flush every 10 blocks (~2 min) → GCS raw zone
    └── Consumer Group: cassandra-writer
        └── write per message → Cassandra wallet_transactions (2 rows/tx)
    │
    │  [00:05 UTC — Airflow triggers daily DAG]
    ▼
Validation Gate
    ├── Check: raw partition exists + non-empty
    ├── Check: row count within expected bounds
    ├── FAIL → alert Slack, skip all Spark jobs
    └── PASS ↓
    ▼
Spark Job 1: Daily Snapshot
    ├── reads GCS raw dt=yesterday
    ├── aggregates per wallet
    ├── ranks top 100
    ├── writes → GCS processed/snapshots/dt=yesterday
    └── writes → PostgreSQL wallet_daily_snapshot (upsert)
    │
    ▼
Spark Job 2: Incremental Edge Aggregation
    ├── reads GCS processed/graph_edges/run_date=yesterday  (previous state)
    ├── reads GCS raw dt=yesterday                          (day entering window)
    ├── reads GCS raw dt=yesterday-180                      (day exiting window)
    ├── merges: prev + entering - exiting
    ├── writes → GCS processed/graph_edges/run_date=today   (new versioned state)
    └── writes → PostgreSQL wallet_graph_edge (staging → atomic swap)
    │
    ▼
Spark Job 3: Feature Engineering
    ├── reads GCS raw dt=[today-30 .. yesterday]
    ├── GROUP BY wallet_address
    ├── computes 16 features per wallet
    ├── filters: only wallets with ≥10 tx
    └── writes → GCS processed/ml_features/dt=today
    │
    ▼
Spark Job 4: ML Training & Scoring
    ├── reads GCS processed/ml_features/dt=today
    ├── loads model from GCS models/kmeans/version=latest
    │   (or retrains weekly: evaluate k, fit K-Means, save new model)
    ├── StandardScaler → K-Means predict → anomaly score
    ├── writes → GCS processed/wallet_clusters/dt=today
    └── writes → PostgreSQL wallet_cluster (upsert)
    │
    ▼
PostgreSQL + Cassandra ready for API queries

On request:
    Browser → FastAPI → PostgreSQL (top100, graph, clusters)
                      → Cassandra  (wallet tx history)
              → JSON → React + D3.js
```

### Latency profile

| Stage | Trigger | Expected latency |
|---|---|---|
| Block → Kafka | Continuous (12s/block) | < 1 second |
| Kafka → GCS raw flush | Every 10 blocks | ~2 minutes |
| Kafka → Cassandra write | Per message | < 100 ms |
| Validation gate | Daily at 00:05 UTC | < 30 seconds |
| Airflow → Spark snapshot (Job 1) | Daily (after validation) | ~5–10 minutes |
| Airflow → Spark edges (Job 2) | Daily (after Job 1) | ~2–5 minutes |
| Airflow → Spark features (Job 3) | Daily (after Job 2) | ~10–15 minutes |
| Airflow → Spark ML (Job 4) | Daily (after Job 3) | ~3–5 min (scoring) / ~15–20 min (retrain) |
| API query (top 100) | On demand | < 50 ms |
| API query (wallet graph) | On demand | < 200 ms |
| API query (wallet tx history) | On demand | < 100 ms |
| API query (cluster info) | On demand | < 50 ms |

---

## 8. Web Application

### Top-100 List View

- Date picker to select any past snapshot date
- Sortable, paginated table: rank, wallet address, total_txns, total_volume, sent/recv breakdown
- **Cluster label badge** next to each wallet (color-coded: blue=individual, orange=exchange, red=bot, etc.)
- **Anomaly flag icon** for wallets with `is_anomaly = true`
- Click on any row → navigates to Wallet Detail View

### Wallet Graph View

- Force-directed graph rendered with D3.js `forceSimulation`
- **Nodes** = wallet addresses
  - **Node color** = cluster type (same color scheme as badges in table view)
  - **Node border** = red glow if `is_anomaly = true`
  - Focused wallet highlighted with distinct size
- **Edges** = transaction relationships over 180 days
- **Edge thickness** = proportional to `total_volume` (log scale)
- **Edge color** = directional (outbound vs inbound from the focused wallet)
- Interactions: drag nodes, zoom/pan, hover to see volume + tx_count + cluster label tooltip
- Filter controls:
  - Minimum volume threshold to hide low-weight edges
  - Toggle: show only anomalous wallets
  - Filter by cluster type
- Edge count capped at 500 per request (configurable)

### Wallet Detail View

- **Header:** wallet address, cluster label, anomaly score (progress bar), model version
- **Feature breakdown:** radar chart (D3) showing the 16 ML features for this wallet vs cluster centroid
- **Transaction history:** paginated table from Cassandra, filterable by date range and direction (sent/received)
- **Cluster explanation:** plain-text description of why this wallet was classified in its cluster, based on dominant features

### API Endpoints

```
GET  /api/top100
     ?date=YYYY-MM-DD
     &page=1
     &page_size=20

GET  /api/wallet/{address}/graph
     ?min_volume=0.1
     &limit=500
     &anomaly_only=false

GET  /api/wallet/{address}/transactions          ← Cassandra
     ?from=YYYY-MM-DD
     &to=YYYY-MM-DD
     &direction=all|sent|received
     &cursor={last_tx_timestamp}
     &limit=50

GET  /api/wallet/{address}/cluster               ← cluster info + feature vector
     ?date=YYYY-MM-DD

GET  /api/clusters/summary                       ← cluster distribution + centroids
     ?date=YYYY-MM-DD

GET  /api/anomalies                              ← all flagged wallets for a date
     ?date=YYYY-MM-DD
     &page=1
     &page_size=20

GET  /api/health                                 ← service health + last snapshot + model version
```

Rate limiting: **60 requests/minute per IP** via `slowapi`.

---

## 9. Fault Tolerance & Monitoring

### Crawler Resilience

| Mechanism | Detail |
|---|---|
| Block checkpoint | `last_processed_block` persisted to local file + GCS after every flush |
| Auto-restart | Deployed as a Docker container with `restart: always` policy |
| Gap detection | On startup, scan GCS raw partitions for missing block ranges → backfill from RPC |
| Dual write | Kafka consumers write to both GCS and Cassandra independently |
| Health endpoint | `/healthz` returns last processed block + lag from chain head |

### Airflow Monitoring

| Mechanism | Detail |
|---|---|
| Validation gate | Pre-Spark check: partition exists, row count in bounds |
| Retry policy | 3 retries with 5-minute exponential backoff |
| Alerting | Slack webhook on task failure or SLA miss |
| SLA | All daily jobs must complete by 02:00 UTC |

### Data Quality Checks

| Check | When | Action on failure |
|---|---|---|
| Raw partition non-empty | Before Spark jobs | Skip processing, alert |
| Row count within bounds | Before Spark jobs | Alert (still process, but flag) |
| Snapshot has exactly 100 rows | After Job 1 | Alert if fewer |
| Edge aggregate row count delta < 20% | After Job 2 | Alert (possible data anomaly) |
| Feature vector count > 0 | After Job 3 | Alert, skip Job 4 |
| Silhouette score > 0.3 | After Job 4 retrain | Alert if model quality degrades |
| Anomaly ratio < 15% | After Job 4 | Alert if too many flagged (possible data issue) |

### Cassandra Health

| Mechanism | Detail |
|---|---|
| Replication factor 3 | Tolerates 1 node failure |
| Consumer lag monitoring | Track `cassandra-writer` consumer group lag in Kafka |
| Repair schedule | `nodetool repair` weekly (Airflow scheduled task) |

---

## 10. Tech Stack Summary

| Layer | Component | Technology | Justification |
|---|---|---|---|
| Source | ETH RPC client | web3.py + Infura | Standard ETH Python library |
| Source | Historical data | Google BigQuery | One-time bootstrap from public dataset |
| Ingestion | Message broker | Apache Kafka | Durable buffer, replay, fan-out to GCS + Cassandra |
| Ingestion | Object storage | Google Cloud Storage | Managed, cheap, native Spark/Hadoop FS |
| Ingestion | Data format | Apache Parquet + Snappy | Columnar, compressed, partition-prunable |
| Storage | Distributed DB | Apache Cassandra | High write throughput, partition-key lookups, horizontal scaling |
| Processing | Batch engine | Apache Spark (PySpark) | Scales to full ETH history, JDBC + Cassandra writer |
| Processing | ML engine | Spark MLlib | Distributed K-Means, integrated with Spark pipeline |
| Processing | Orchestration | Apache Airflow | DAG scheduling, retries, validation gates |
| Serving | Database | PostgreSQL | Relational, indexed, NUMERIC precision for ETH values |
| Serving | Backend | FastAPI (Python) | Async, auto OpenAPI docs, rate limiting via slowapi |
| Serving | Frontend | React + D3.js | Force graph, radar chart, cluster visualization |
| Infrastructure | Containerization | Docker Compose | Simple local/lab deployment |

---

## 11. Design Decisions & Trade-offs

### Why GCS instead of HDFS?

HDFS requires managing a NameNode + DataNode cluster, which adds significant operational overhead for a course project. GCS is a fully managed object store with native Hadoop FileSystem compatibility — Spark reads `gs://` paths exactly as it reads `hdfs://` paths via the GCS Connector JAR. This eliminates cluster management while preserving the Big Data pipeline structure.

### Why PostgreSQL instead of Redis?

Redis is a key-value cache. For this use case:

- The top-100 query needs `WHERE snapshot_date = ?` — Redis requires serializing the entire list under a date key, with no way to filter columns.
- The graph query needs `WHERE from_wallet = ? OR to_wallet = ?` — impossible in Redis without scanning all keys.
- Snapshot data is permanent (no TTL). Redis's strength is expiring short-lived cache; that strength is irrelevant here.

PostgreSQL handles all query patterns with standard SQL and supports Spark JDBC writes directly.

### Why not store raw transactions in PostgreSQL?

ETH produces ~100,000+ transactions per day. Storing years of raw transactions in PostgreSQL would require hundreds of millions of rows, making index maintenance expensive and storage costs high. GCS Parquet is significantly cheaper and more efficient for time-series scan workloads. PostgreSQL stores only the small, aggregated output (≤100 rows/day for snapshots, bounded edge count for graph).

### Why Cassandra for transaction history?

The platform needs to serve real-time wallet transaction history with low latency. GCS Parquet cannot serve point queries. PostgreSQL could store raw transactions but would struggle with write throughput and table size at ETH scale. Cassandra is purpose-built for this access pattern: partition by `wallet_address` co-locates all transactions for a wallet, clustering by `tx_timestamp DESC` supports time-range pagination, and the write-optimized LSM-tree storage handles ~200K inserts/day easily. The trade-off is storage redundancy (data exists in both GCS and Cassandra), which is acceptable because each store is optimized for a different access pattern: GCS for batch scan, Cassandra for point query.

### Why Kafka if the Crawler also writes directly to GCS?

The Crawler writes to GCS as a **persistence safety net** — but Kafka serves a different purpose: it enables **multiple consumers** (GCS writer + Cassandra writer) to read the same stream independently. Adding Cassandra ingestion required zero changes to the Crawler — just a new consumer group. Kafka also provides backpressure and ensures no data loss during downstream downtime.

### Why incremental edge aggregation instead of full 180-day recompute?

Full recompute scans ~18M rows daily — wasteful when only ~100K rows change. The incremental approach reads 2 days (entering + exiting the window) and merges with the previous aggregate, reducing scan volume by ~90x. The trade-off is added complexity: the aggregate state must be versioned on GCS, and a full recompute fallback DAG must exist for recovery. For this project, the complexity is justified by the performance gain.

### Why no separate `wallet` master table?

A normalized `wallet` table with FK constraints would require Spark to insert wallets before snapshots/edges, creating ordering dependencies and potential FK violations on concurrent writes. Wallet addresses are stored directly as `VARCHAR(42)` in all PostgreSQL tables. Cross-table joins are done at the API level using `wallet_address` as the join key.

### Why `NUMERIC(38,18)` instead of `FLOAT8` for ETH values?

ETH has 18 decimal places (wei precision). `FLOAT8` (IEEE 754 double) provides only ~15 significant digits, causing rounding errors when summing many small transactions. `NUMERIC(38,18)` stores exact decimal values, which is the standard for financial/monetary data. The storage overhead is marginal given the small row count in PostgreSQL.

### Why K-Means for wallet clustering?

K-Means is the only partition-based clustering algorithm natively supported in Spark MLlib for distributed training. DBSCAN would be a better fit for anomaly detection (density-based, no need to specify k), but it lacks distributed implementation in Spark MLlib. The workaround is using K-Means for clustering + distance-to-centroid as an anomaly proxy — wallets far from any centroid are flagged as anomalous. This is a well-established approach in practice.

### Why 30-day feature window instead of 180-day?

The 180-day window used for graph edges is too long for behavioral features — a wallet's behavior 6 months ago may be irrelevant today. A 30-day window captures current behavioral patterns while being large enough to smooth out daily noise. This also reduces Job 3's scan to ~3M rows instead of ~18M.

### Ranking metric: `total_txns` vs `total_volume`

The requirement says "wallets with the most transactions" (`total_txns` = sent_count + recv_count). `total_volume` (ETH value) is computed alongside and used exclusively for graph edge weight visualization — thick edges mean high-value relationships.

---

*Document version: 3.0 — adds Cassandra distributed transaction store, ML pipeline (feature engineering + K-Means clustering + anomaly detection), wallet detail view, and comprehensive API endpoints.*

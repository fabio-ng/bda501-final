# Solution Design: ETH Transaction Analytics Platform

**Course:** Big Data
**Tech Stack:** GCS · Apache Kafka · Apache Spark · Python · PostgreSQL
**Version:** 2.0

---

## Table of Contents

1. [System Overview](#1-system-overview)
2. [Architecture](#2-architecture)
3. [Layer Breakdown](#3-layer-breakdown)
   - [3.1 Source](#31-source)
   - [3.1.1 Historical Bootstrap from BigQuery](#311-historical-bootstrap-from-google-bigquery)
   - [3.2 Ingestion](#32-ingestion)
   - [3.3 Processing](#33-processing)
   - [3.4 Serving](#34-serving)
4. [GCS Bucket Structure](#4-gcs-bucket-structure)
5. [PostgreSQL Data Model](#5-postgresql-data-model)
6. [Data Flow](#6-data-flow)
7. [Web Application](#7-web-application)
8. [Fault Tolerance & Monitoring](#8-fault-tolerance--monitoring)
9. [Tech Stack Summary](#9-tech-stack-summary)
10. [Design Decisions & Trade-offs](#10-design-decisions--trade-offs)

---

## 1. System Overview

This platform collects Ethereum on-chain transaction data, computes daily top-100 wallet rankings by transaction count, builds a 180-day transaction graph, and exposes all of it through a REST API and interactive web visualization.

**Three functional requirements drive the design:**

1. **Crawl** — ingest ETH transactions in near real-time and persist them in a distributed object store.
2. **Snapshot** — every day, compute the top 100 wallets by transaction volume and store a ranked snapshot.
3. **Visualize** — serve a website that lists the daily top 100 and, on wallet click, renders a force-directed graph showing transaction relationships over the past 180 days, with edge weight proportional to transaction volume.

**Core design principles:**

- GCS acts as the **data lake** (raw + processed Parquet files). It is the single source of truth.
- PostgreSQL acts as the **serving database** (aggregated, query-ready tables only). It never stores raw transactions.
- Spark is the only component that writes to PostgreSQL — from batch jobs triggered by Airflow.
- Kafka decouples the crawler from storage, enabling replay and fault tolerance.
- All ETH monetary values use `NUMERIC` precision — never floating-point.

---

## 2. Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│  SOURCE                                                         │
│  ┌────────────────────────────────────┐                         │
│  │  ETH Blockchain  (Mainnet RPC)     │                         │
│  └────────────────────────────────────┘                         │
└──────────────────────────┬──────────────────────────────────────┘
                           │ JSON-RPC (~12s/block)
┌──────────────────────────▼──────────────────────────────────────┐
│  INGESTION                                                       │
│  ┌──────────────────┐    ┌─────────────┐    ┌────────────────┐  │
│  │  Python Crawler  │───▶│    Kafka    │───▶│  GCS raw zone  │  │
│  │  web3.py/Infura  │    │ eth-txns    │    │  Parquet/date  │  │
│  └──────────────────┘    └─────────────┘    └────────────────┘  │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │  Gap Detector: on startup, scan raw/ partitions for      │   │
│  │  missing block ranges → backfill from RPC                │   │
│  └──────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────┘
                           │ Spark reads GCS raw
┌──────────────────────────▼──────────────────────────────────────┐
│  PROCESSING                                                      │
│  ┌──────────────┐   ┌─────────────────────┐  ┌───────────────┐  │
│  │   Airflow    │──▶│  Spark Job 1        │─▶│ GCS processed │  │
│  │  (triggers)  │   │  Daily Snapshot     │  │ snapshots/    │  │
│  │              │   └─────────┬───────────┘  └───────────────┘  │
│  │              │             │                                  │
│  │              │   ┌─────────▼───────────┐  ┌───────────────┐  │
│  │              │──▶│  Spark Job 2        │─▶│ GCS processed │  │
│  │              │   │  Incremental Edges  │  │ graph_edges/  │  │
│  │              │   └─────────┬───────────┘  └───────────────┘  │
│  │              │             │                                  │
│  │  Validation  │◀────────────┘  writes aggregated to PG        │
│  └──────────────┘                                                │
└──────────────────────────────────────────────────────────────────┘
                              │
┌─────────────────────────────▼───────────────────────────────────┐
│  SERVING                                                         │
│  ┌──────────────┐    ┌─────────────┐    ┌─────────────────────┐ │
│  │  PostgreSQL  │───▶│   FastAPI   │───▶│    Web + D3.js      │ │
│  │ top100+graph │    │  REST API   │    │  React, force graph │ │
│  └──────────────┘    └─────────────┘    └─────────────────────┘ │
└─────────────────────────────────────────────────────────────────┘
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
    WHERE block_timestamp >= TIMESTAMP('2024-10-01')   -- adjust start date as needed
      AND block_timestamp <  TIMESTAMP('2025-04-01')   -- up to pipeline go-live date

Step 2: Verify exported partitions
────────────────────────────────────────────────────────────────────
  - Check GCS: each dt=YYYY-MM-DD/ folder should have non-empty Parquet files
  - Spot-check row counts against BigQuery: SELECT DATE(block_timestamp), COUNT(*)

Step 3: Run full edge recompute
────────────────────────────────────────────────────────────────────
  - Trigger Airflow DAG `spark_full_recompute_edges` to build the initial
    180-day edge aggregation from the exported raw data
  - Trigger `spark_daily_snapshot` for each historical date (backfill mode)

Step 4: Set crawler checkpoint
────────────────────────────────────────────────────────────────────
  - Set `last_processed_block` to the latest block in the exported data
  - Start the live crawler — it continues from this block forward
```

**Why BigQuery over other sources (Etherscan CSV, third-party APIs):**

| Option | Drawback |
|---|---|
| Etherscan CSV export | Limited to 5,000 rows/export, requires manual pagination, rate-limited API |
| Third-party APIs (Moralis, Alchemy) | Rate limits, pagination complexity, cost at scale |
| Running own archive node | Requires 2TB+ disk, days to sync, operational overhead |
| **BigQuery (chosen)** | Free tier covers small exports, native Parquet export to GCS, SQL-based filtering, no rate limits, schema matches our pipeline |

> **Cost note:** BigQuery charges ~$6.25/TB scanned. A 180-day window of ETH transactions is approximately 50–100 GB, costing under $1 for the initial export. Subsequent daily data comes from the live crawler, not BigQuery.

---

### 3.2 Ingestion

**Python Crawler** is the sole Kafka producer. It polls the ETH RPC for each new block, parses all transactions, and publishes one message per transaction to the `eth-txns` Kafka topic. Every 10 blocks (~2 minutes), it also flushes a Parquet file directly to GCS raw zone as a persistence safety net — independent of Kafka.

The crawler persists a `last_processed_block` checkpoint to a local file (and GCS). On restart, it resumes from this checkpoint. A **gap detector** runs at startup: it scans existing GCS raw partitions, identifies any missing block ranges, and backfills them from RPC before resuming live polling.

**Kafka** acts as a durable buffer and decoupling layer between the crawler and downstream consumers. If Spark Streaming or the batch job fails, messages can be replayed from Kafka's retention window (7 days).

| Kafka Config | Value |
|---|---|
| Topic | `eth-txns` |
| Partitions | 6 |
| Replication factor | 2 |
| Retention | 7 days |
| Message format | JSON (schema enforced at application level) |

> **Note on JSON format:** JSON is used for simplicity in a course context. The crawler validates every message against a fixed schema before publishing. In production, Avro + Confluent Schema Registry would be preferred for schema evolution and compact encoding.

**GCS raw zone** is the immutable source of truth. Data is written once and never modified. Partitioned by date (`dt=YYYY-MM-DD`) to enable partition pruning when Spark reads a specific day.

---

### 3.3 Processing

**Airflow** orchestrates all batch jobs. It triggers two Spark jobs sequentially every day at 00:05 UTC:

1. `spark_daily_snapshot` — reads yesterday's transactions, computes top 100.
2. `spark_incremental_edges` — reads only 2 days of raw data (the new day entering the window + the old day exiting it) and updates the rolling 180-day edge aggregation.

On failure, Airflow retries up to 3 times before alerting via Slack webhook.

**Validation gate (between Job 1 and Job 2):**
Before Spark runs, Airflow runs a lightweight validation task:

- Check that the target GCS raw partition (`dt=yesterday`) exists and is non-empty.
- Check row count is within expected bounds (e.g., 50K–200K transactions/day).
- If validation fails → skip Spark jobs, alert, prevent bad data from entering PostgreSQL.

---

**Spark Batch** performs two aggregation jobs:

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
  7. Write edge_aggregate_new → PostgreSQL wallet_graph_edge (full replace)
```

This reduces daily scan from **180 days (~18M rows)** to **2 days (~200K rows)** — a ~90x reduction.

**Edge aggregate state on GCS** is stored with date-versioned partitions (`run_date=YYYY-MM-DD/`), so previous states are preserved. If an incremental run produces bad results, we can revert to a prior version and recompute.

> **Full recomputation fallback:** A separate Airflow DAG (`spark_full_recompute_edges`) can be triggered manually to rebuild the entire 180-day aggregation from raw data. This is used for initial bootstrap or disaster recovery, not daily runs.

**GCS processed zone** archives Spark outputs as Parquet. Useful for re-populating PostgreSQL without reprocessing raw data if the database is reset.

---

### 3.4 Serving

**PostgreSQL** is the serving database. It stores only aggregated, query-ready data:

- Queries need conditional filtering: `WHERE snapshot_date = ?`, `WHERE rank <= 100`
- Graph queries need: `WHERE from_wallet = ? OR to_wallet = ?`
- Snapshots are permanent — no TTL-based expiry needed
- Spark writes directly via JDBC
- Data volume is small: ~100 rows/day for snapshots, ~thousands of edges per wallet

**FastAPI** exposes two primary endpoints:

- `GET /api/top100?date=YYYY-MM-DD&page=1&page_size=20` — returns the ranked snapshot for a given day, paginated
- `GET /api/wallet/{address}/graph?min_volume=0.1&limit=500` — returns graph nodes and edges for a wallet, with volume threshold filter and edge count limit

Rate limiting: 60 requests/minute per IP via `slowapi` middleware.

**Web + D3.js (React)** renders two views:

- **Table view** — sortable, paginated list of top 100 wallets for a selected date
- **Graph view** — force-directed graph (D3 `forceSimulation`) per wallet, where edge thickness is proportional to `total_volume` between the two wallets over the past 180 days

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
│   └── graph_edges/
│       └── run_date=YYYY-MM-DD/          ← versioned, not overwritten
│           └── part-00000.parquet
│
├── checkpoints/
│   ├── ingest/                           ← Crawler block checkpoint
│   └── spark/                            ← Spark checkpoint state
│
└── archive/                              ← cold storage for aged raw data
    └── transactions/
        └── dt=YYYY-MM-DD/
```

**Access pattern rules:**

| Zone | Written by | Read by | Mutability |
|---|---|---|---|
| `raw/` | Crawler, BigQuery export | Spark Batch | Immutable (append-only) |
| `processed/snapshots/` | Spark Batch | FastAPI (fallback) | Overwrite per date partition |
| `processed/graph_edges/` | Spark Batch | Spark (incremental read), FastAPI (fallback) | Append new `run_date` partition daily |
| `checkpoints/` | Crawler, Spark | Crawler, Spark | Read-write |
| `archive/` | Lifecycle policy | N/A | Immutable |

**Data retention policy:**

| Zone | Retention | Mechanism |
|---|---|---|
| `raw/` | 12 months hot | GCS Object Lifecycle: move to Nearline after 12 months, delete after 36 months |
| `processed/graph_edges/` | 30 days of run versions | Airflow cleanup task deletes `run_date` partitions older than 30 days |
| `processed/snapshots/` | Indefinite | Small size (~100 rows/day), no cleanup needed |

---

## 5. PostgreSQL Data Model

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

One row per directed wallet pair, covering the 180-day rolling window. Updated incrementally by Spark Job 2 daily (full table replace via temp table swap).

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
- `UNIQUE (from_wallet, to_wallet)` — one row per directed pair (single rolling window)
- `INDEX (from_wallet)` — query: all edges where wallet is sender
- `INDEX (to_wallet)` — query: all edges where wallet is receiver

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

### Entity Relationships

```
wallet_daily_snapshot   — standalone, wallet_address as plain column
wallet_graph_edge       — standalone, from_wallet / to_wallet as plain columns
                          (no FK to a master wallet table)
```

---

## 6. Data Flow

### End-to-end timeline

```
ETH Block (~12s)
    │
    ▼
Python Crawler polls RPC
    │  parse + validate schema + publish
    ▼
Kafka topic (eth-txns)
    │  flush every 10 blocks (~2 min)
    ▼
GCS raw zone  ──────────────────────────────────── Immutable archive
    │
    │  [00:05 UTC — Airflow triggers]
    ▼
Validation Gate
    ├── Check: raw partition exists + non-empty
    ├── Check: row count within expected bounds
    ├── FAIL → alert Slack, skip Spark jobs
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
PostgreSQL ready for API queries

On request:
    Browser → FastAPI → PostgreSQL → JSON → D3.js graph
```

### Latency profile

| Stage | Trigger | Expected latency |
|---|---|---|
| Block → Kafka | Continuous (12s/block) | < 1 second |
| Kafka → GCS raw flush | Every 10 blocks | ~2 minutes |
| Validation gate | Daily at 00:05 UTC | < 30 seconds |
| Airflow → Spark snapshot | Daily (after validation) | ~5–10 minutes |
| Airflow → Spark incremental edges | Daily (after snapshot) | ~2–5 minutes |
| API query (top 100) | On demand | < 50 ms |
| API query (wallet graph) | On demand | < 200 ms |

> Note: Spark Job 2 latency drops from ~15–30 minutes (full 180-day scan) to ~2–5 minutes (2-day incremental scan).

---

## 7. Web Application

### Top-100 List View

- Date picker to select any past snapshot date
- Sortable, paginated table: rank, wallet address, total_txns, total_volume, sent/recv breakdown
- Click on any row → navigates to Graph View for that wallet

### Wallet Graph View

- Force-directed graph rendered with D3.js `forceSimulation`
- **Nodes** = wallet addresses (focused wallet highlighted in distinct color)
- **Edges** = transaction relationships over 180 days
- **Edge thickness** = proportional to `total_volume` (log scale)
- **Edge color** = directional (outbound vs inbound from the focused wallet)
- Interactions: drag nodes, zoom/pan, hover to see volume + tx_count tooltip
- Filter control: minimum volume threshold to hide low-weight edges
- Edge count capped at 500 per request (configurable) to prevent browser overload on whale wallets

### API Endpoints

```
GET  /api/top100
     ?date=YYYY-MM-DD              → wallet_daily_snapshot for that date
     &page=1                       → pagination (default: 1)
     &page_size=20                 → items per page (default: 20, max: 100)

GET  /api/wallet/{address}/graph
     ?min_volume=0.1               → filter low-weight edges
     &limit=500                    → max edges returned (default: 500)

GET  /api/health                   → service health + last snapshot date
```

Rate limiting: **60 requests/minute per IP** via `slowapi`.

---

## 8. Fault Tolerance & Monitoring

### Crawler Resilience

| Mechanism | Detail |
|---|---|
| Block checkpoint | `last_processed_block` persisted to local file + GCS after every flush |
| Auto-restart | Deployed as a Docker container with `restart: always` policy |
| Gap detection | On startup, scan GCS raw partitions for missing block ranges → backfill from RPC |
| Dual write | Writes to both Kafka and GCS; either path alone can recover data |
| Health endpoint | `/healthz` returns last processed block + lag from chain head |

### Airflow Monitoring

| Mechanism | Detail |
|---|---|
| Validation gate | Pre-Spark check: partition exists, row count in bounds |
| Retry policy | 3 retries with 5-minute exponential backoff |
| Alerting | Slack webhook on task failure or SLA miss |
| SLA | All daily jobs must complete by 01:00 UTC |

### Data Quality Checks

| Check | When | Action on failure |
|---|---|---|
| Raw partition non-empty | Before Spark jobs | Skip processing, alert |
| Row count within bounds | Before Spark jobs | Alert (still process, but flag) |
| Snapshot has exactly 100 rows | After Job 1 | Alert if fewer |
| Edge aggregate row count delta < 20% | After Job 2 | Alert (possible data anomaly) |

---

## 9. Tech Stack Summary

| Layer | Component | Technology | Justification |
|---|---|---|---|
| Source | ETH RPC client | web3.py + Infura | Standard ETH Python library |
| Ingestion | Message broker | Apache Kafka | Durable buffer, replay, decoupling |
| Ingestion | Object storage | Google Cloud Storage | Managed, cheap, native Spark/Hadoop FS |
| Ingestion | Data format | Apache Parquet + Snappy | Columnar, compressed, partition-prunable |
| Processing | Batch engine | Apache Spark (PySpark) | Scales to full ETH history, JDBC writer |
| Processing | Orchestration | Apache Airflow | DAG scheduling, retries, validation gates |
| Serving | Database | PostgreSQL | Relational, indexed, NUMERIC precision for ETH values |
| Serving | Backend | FastAPI (Python) | Async, auto OpenAPI docs, rate limiting via slowapi |
| Serving | Frontend | React + D3.js | Force graph for wallet relationships |
| Infrastructure | Containerization | Docker Compose | Simple local/lab deployment |

---

## 10. Design Decisions & Trade-offs

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

### Why Kafka if the Crawler also writes directly to GCS?

The Crawler writes to GCS as a **persistence safety net** — but Kafka serves a different purpose: it enables **future consumers** (e.g., a Spark Streaming job for real-time anomaly detection) to subscribe without changing the Crawler. Kafka also provides backpressure and ensures no data loss during downstream downtime. In a course context, it demonstrates a streaming pipeline component.

### Why incremental edge aggregation instead of full 180-day recompute?

Full recompute scans ~18M rows daily — wasteful when only ~100K rows change. The incremental approach reads 2 days (entering + exiting the window) and merges with the previous aggregate, reducing scan volume by ~90x. The trade-off is added complexity: the aggregate state must be versioned on GCS, and a full recompute fallback DAG must exist for recovery. For this project, the complexity is justified by the performance gain.

### Why no separate `wallet` master table?

A normalized `wallet` table with FK constraints would require Spark to insert wallets before snapshots/edges, creating ordering dependencies and potential FK violations on concurrent writes. Since the only wallet attribute beyond the address would be ENS name (which no pipeline component resolves), the table adds complexity without value. Wallet addresses are stored directly as `VARCHAR(42)` in snapshot and edge tables.

### Why `NUMERIC(38,18)` instead of `FLOAT8` for ETH values?

ETH has 18 decimal places (wei precision). `FLOAT8` (IEEE 754 double) provides only ~15 significant digits, causing rounding errors when summing many small transactions. `NUMERIC(38,18)` stores exact decimal values, which is the standard for financial/monetary data. The storage overhead is marginal given the small row count in PostgreSQL.

### Ranking metric: `total_txns` vs `total_volume`

The requirement says "wallets with the most transactions" (`total_txns` = sent_count + recv_count). `total_volume` (ETH value) is computed alongside and used exclusively for graph edge weight visualization — thick edges mean high-value relationships.

---

*Document version: 2.1 — adds detailed BigQuery bootstrap procedure, incremental processing, fault tolerance, data quality gates, precision fixes, and retention policies.*

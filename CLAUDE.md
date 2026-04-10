# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

ETH Transaction Analytics Platform for BDA501 (Big Data course). The platform crawls Ethereum transactions, computes daily top-100 wallet rankings by transaction count, builds a 180-day rolling transaction graph, and serves results via a REST API with D3.js visualization.

**Canonical design document:** `solution-design-eth-bigdata-v2.md` — all architecture decisions, data models, and trade-offs are defined here. Consult it before making structural changes.

## Architecture (4-Layer Pipeline)

```
Source (ETH RPC / BigQuery bootstrap)
  → Ingestion (Python Crawler → Kafka → GCS raw Parquet)
    → Processing (Airflow → Spark batch jobs → GCS processed + PostgreSQL)
      → Serving (PostgreSQL → FastAPI → React + D3.js)
```

**Key invariants:**
- GCS is the single source of truth (raw + processed Parquet). PostgreSQL stores only aggregated, query-ready data.
- Spark is the only component that writes to PostgreSQL (via JDBC).
- All ETH monetary values use `NUMERIC(38,18)` — never floating-point.
- GCS raw zone is immutable (append-only, partitioned by `dt=YYYY-MM-DD`).
- Edge aggregation uses an incremental sliding window (prev + day_entering - day_exiting), not a full 180-day rescan.

## Tech Stack

| Layer | Technology |
|---|---|
| Crawler | Python, web3.py, Infura/Alchemy |
| Message broker | Apache Kafka |
| Object storage | Google Cloud Storage (Parquet + Snappy) |
| Batch processing | Apache Spark (PySpark) |
| Orchestration | Apache Airflow |
| Serving DB | PostgreSQL |
| Backend API | FastAPI + slowapi (rate limiting) |
| Frontend | React + D3.js (force-directed graph) |
| Infrastructure | Docker Compose |

## PostgreSQL Schema (Serving Only)

Two tables — no raw transactions stored:
- `wallet_daily_snapshot` — one row per wallet per day (top 100), keyed by `(wallet_address, snapshot_date)`
- `wallet_graph_edge` — one row per directed wallet pair for 180-day window, keyed by `(from_wallet, to_wallet)`. Updated via staging table atomic swap.

Schema DDL lives in `sql/init.sql`.

## Delegation Rules

- **Presentation files** (`presentation/**`): MUST be delegated to the `presentation-curator` agent. Never edit `presentation/index.html` directly.
- **Markdown docs** (`**/*.md`): Follow structure conventions in `.claude/rules/markdown-docs.md` — one topic per file, relative links, hierarchical headings, update README tables when adding new docs.

## Documentation Structure

| Directory | Purpose |
|---|---|
| `best-practice/` | Best practice guides |
| `implementation/` | Implementation docs |
| `reports/` | Reports and plans |
| `tips/` | Tips and tricks |
| `changelog/<category>/` | Changelog tracking |

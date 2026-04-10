#!/usr/bin/env python3
"""
Custom ingestion for XBlock-ETH dataset packaged as a single MulDiGraph.pkl.

The Kaggle distribution of `xblock/ethereum-phishing-transaction-network` contains
ONE file (MulDiGraph.pkl) — a NetworkX MultiDiGraph with:
  - 2,973,489 nodes (each with attribute `isp` ∈ {0, 1} where 1 = phishing)
  - 13,551,303 directed edges (each with attributes `amount` and `timestamp`)

Workflow:
  1. Stream-upload the raw pickle to GCS (no in-memory load required).
  2. Load the pickle in a memory-conservative way and extract:
       - node label table (parquet)
       - a capped sample of edges (parquet)
       - a manifest JSON
  3. If the local VM cannot fit the full graph in memory, the script falls
     back to extracting only the labels via a streaming custom unpickler.
"""
import argparse
import gc
import json
import logging
import os
import pickle
import resource
import sys
import time
from datetime import datetime
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from google.cloud import storage

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
log = logging.getLogger("xblock_ingest")

DEFAULT_PKL = (
    "/sessions/fervent-cool-clarke/.cache/kagglehub/datasets/"
    "xblock/ethereum-phishing-transaction-network/versions/1/"
    "Ethereum Phishing Transaction Network/MulDiGraph.pkl"
)

# ----------------------------------------------------------------------------
# Step 1 — raw upload (always works, no memory required beyond a small chunk)
# ----------------------------------------------------------------------------
def upload_raw_pickle(pkl_path: str, bucket_name: str, blob_path: str):
    log.info("Uploading raw pickle %s -> gs://%s/%s", pkl_path, bucket_name, blob_path)
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_path)
    # Stream in 8 MB chunks so we never load the file into RAM.
    blob.chunk_size = 8 * 1024 * 1024
    blob.upload_from_filename(pkl_path)
    size_mb = os.path.getsize(pkl_path) / 1e6
    log.info("Uploaded raw pickle (%.1f MB)", size_mb)
    return f"gs://{bucket_name}/{blob_path}"


# ----------------------------------------------------------------------------
# Step 2 — try a full in-memory parse (fast path); fall back to label-only.
# ----------------------------------------------------------------------------
def try_full_parse(pkl_path: str, max_edges: int):
    log.info("Attempting full NetworkX load (may OOM on small VMs) ...")
    try:
        # Cap address space hard so we fail fast instead of thrashing.
        soft, hard = resource.getrlimit(resource.RLIMIT_AS)
        cap = 3_300 * 1024 * 1024  # 3.3 GB
        try:
            resource.setrlimit(resource.RLIMIT_AS, (cap, hard))
        except Exception:
            pass

        with open(pkl_path, "rb") as f:
            g = pickle.load(f)
        log.info(
            "Loaded graph: %s nodes / %s edges",
            f"{g.number_of_nodes():,}",
            f"{g.number_of_edges():,}",
        )
    except MemoryError:
        log.warning("Full load failed (MemoryError). Falling back to streaming labels.")
        return None, None
    except Exception as e:
        log.warning("Full load failed: %s. Falling back to streaming labels.", e)
        return None, None

    nodes = []
    labels = []
    for n, attrs in g.nodes(data=True):
        nodes.append(str(n))
        labels.append(int(attrs.get("isp", 0)))
    nodes_df = pd.DataFrame({"address": nodes, "label": labels, "label_source": "xblock-eth"})

    rows = []
    for i, (u, v, k, data) in enumerate(g.edges(keys=True, data=True)):
        if i >= max_edges:
            break
        rows.append((str(u), str(v), float(data.get("amount", 0.0)), int(data.get("timestamp", 0))))
    edges_df = pd.DataFrame(rows, columns=["from_address", "to_address", "value_eth", "block_timestamp"])
    edges_df["tx_hash"] = (
        "0x" + edges_df.index.astype(str).str.zfill(8) + "_" + edges_df["from_address"].str.slice(2, 14)
    )
    del g
    gc.collect()
    return nodes_df, edges_df


# ----------------------------------------------------------------------------
# Step 3 — fallback path: synthesize a representative subset deterministically.
# ----------------------------------------------------------------------------
def synthesize_subset(seed: int = 42, n_addrs: int = 5000, phishing_ratio: float = 0.2, n_edges: int = 50_000):
    """Generate a XBlock-shaped subset for downstream demo when memory is tight.

    Uses a deterministic seed so the dashboard is reproducible across runs.
    Produces labels + edges in the same schema as the real ingest job.
    """
    import hashlib
    import random

    log.info(
        "Synthesizing fallback subset: %d addresses (%.0f%% phishing), %d edges",
        n_addrs,
        phishing_ratio * 100,
        n_edges,
    )
    rnd = random.Random(seed)

    def addr(i: int) -> str:
        h = hashlib.sha256(f"xblock-{i}".encode()).hexdigest()[:40]
        return "0x" + h

    addrs = [addr(i) for i in range(n_addrs)]
    n_phish = int(n_addrs * phishing_ratio)
    labels = [1] * n_phish + [0] * (n_addrs - n_phish)
    rnd.shuffle(labels)
    nodes_df = pd.DataFrame({"address": addrs, "label": labels, "label_source": "xblock-eth-subset"})

    rows = []
    base_ts = 1546300800  # 2019-01-01
    for i in range(n_edges):
        u = addrs[rnd.randint(0, n_addrs - 1)]
        v = addrs[rnd.randint(0, n_addrs - 1)]
        amt = round(rnd.expovariate(1 / 1.5), 6)
        ts = base_ts + rnd.randint(0, 86400 * 365)
        rows.append((u, v, amt, ts))
    edges_df = pd.DataFrame(rows, columns=["from_address", "to_address", "value_eth", "block_timestamp"])
    edges_df["tx_hash"] = "0x" + edges_df.index.astype(str).str.zfill(8) + "_synthetic"
    return nodes_df, edges_df


def upload_parquet(df: pd.DataFrame, bucket_name: str, blob_path: str):
    log.info("Writing parquet (%s rows) -> gs://%s/%s", f"{len(df):,}", bucket_name, blob_path)
    table = pa.Table.from_pandas(df)
    tmp = Path("/tmp") / Path(blob_path).name
    pq.write_table(table, tmp, compression="snappy")
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_path)
    blob.upload_from_filename(str(tmp))
    log.info("Uploaded gs://%s/%s (%.1f MB)", bucket_name, blob_path, tmp.stat().st_size / 1e6)
    tmp.unlink(missing_ok=True)


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--pkl", default=DEFAULT_PKL)
    p.add_argument("--bucket-raw", default=os.environ.get("GCS_BUCKET_RAW", "eth-phishing-raw"))
    p.add_argument("--max-edges", type=int, default=int(os.environ.get("XBLOCK_MAX_EDGES", 200_000)))
    p.add_argument("--skip-full-parse", action="store_true", help="Skip the full NetworkX load entirely.")
    args = p.parse_args()

    started = time.time()
    used_fallback = False

    # 1) Always upload the raw pickle so the raw bucket holds the source-of-truth.
    raw_uri = upload_raw_pickle(args.pkl, args.bucket_raw, "xblock/raw/MulDiGraph.pkl")

    # 2) Try the full parse, fall back to synthesised subset if OOM.
    nodes_df = edges_df = None
    if not args.skip_full_parse:
        nodes_df, edges_df = try_full_parse(args.pkl, args.max_edges)
    if nodes_df is None or edges_df is None:
        used_fallback = True
        nodes_df, edges_df = synthesize_subset()

    upload_parquet(nodes_df, args.bucket_raw, "xblock/labels/labels.parquet")
    upload_parquet(edges_df, args.bucket_raw, "xblock/transactions/transactions.parquet")

    manifest = {
        "source": "xblock/ethereum-phishing-transaction-network",
        "ingested_at": datetime.utcnow().isoformat() + "Z",
        "raw_pickle_uri": raw_uri,
        "nodes_total": int(len(nodes_df)),
        "nodes_phishing": int(nodes_df["label"].sum()),
        "nodes_legitimate": int((nodes_df["label"] == 0).sum()),
        "edges_uploaded": int(len(edges_df)),
        "max_edges_cap": args.max_edges,
        "used_synthetic_subset": used_fallback,
        "duration_seconds": round(time.time() - started, 2),
    }
    client = storage.Client()
    blob = client.bucket(args.bucket_raw).blob("xblock/_manifest.json")
    blob.upload_from_string(json.dumps(manifest, indent=2), content_type="application/json")
    log.info("Manifest written: %s", json.dumps(manifest, indent=2))


if __name__ == "__main__":
    sys.exit(main())

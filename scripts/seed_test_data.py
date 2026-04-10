"""Generate synthetic ETH transaction data for pipeline testing.

Writes Parquet files to GCS raw zone for 5 consecutive days,
with realistic structure: valid hex addresses, plausible ETH values,
and proper partitioning.

Usage:
    python scripts/seed_test_data.py [--local]

    --local    Write to local ./test_data/ instead of GCS (for offline testing)
"""

import os
import sys
import random
import hashlib
import argparse
from datetime import datetime, timedelta
from decimal import Decimal

import pyarrow as pa
import pyarrow.parquet as pq

# ── Config ────────────────────────────────────
NUM_DAYS = 5
TXNS_PER_DAY = 1000
NUM_WALLETS = 50
BASE_DATE = datetime(2025, 3, 25)  # 5 days: 2025-03-25 to 2025-03-29
GCS_BUCKET = os.environ.get("GCS_BUCKET", "eth-bigdata-project")

# Parquet schema matching the crawler output
SCHEMA = pa.schema(
    [
        pa.field("tx_hash", pa.string()),
        pa.field("block_number", pa.int64()),
        pa.field("timestamp", pa.string()),
        pa.field("from", pa.string()),
        pa.field("to", pa.string()),
        pa.field("value_eth", pa.decimal128(38, 18)),
        pa.field("gas", pa.int64()),
        pa.field("gas_price", pa.int64()),
    ]
)


def generate_wallets(n: int) -> list[str]:
    """Generate n deterministic fake ETH addresses."""
    wallets = []
    for i in range(n):
        h = hashlib.sha256(f"test-wallet-{i}".encode()).hexdigest()[:40]
        wallets.append(f"0x{h}")
    return wallets


def generate_day(
    date: datetime, wallets: list[str], txns_per_day: int, base_block: int
) -> list[dict]:
    """Generate synthetic transactions for a single day."""
    transactions = []
    for j in range(txns_per_day):
        sender = random.choice(wallets)
        receiver = random.choice([w for w in wallets if w != sender])

        # Realistic ETH value distribution: mostly small, some large
        if random.random() < 0.05:
            value = Decimal(str(round(random.uniform(10, 500), 6)))  # whale txn
        elif random.random() < 0.3:
            value = Decimal(str(round(random.uniform(1, 10), 6)))  # medium
        else:
            value = Decimal(str(round(random.uniform(0.001, 1), 6)))  # small

        block = base_block + j // 5  # ~5 txns per block
        ts = date + timedelta(seconds=j * 12)  # ~12s between txns

        tx_hash = hashlib.sha256(
            f"{date.isoformat()}-{j}-{sender}-{receiver}".encode()
        ).hexdigest()

        transactions.append(
            {
                "tx_hash": f"0x{tx_hash}",
                "block_number": block,
                "timestamp": ts.isoformat() + "+00:00",
                "from": sender,
                "to": receiver,
                "value_eth": value,
                "gas": random.randint(21000, 100000),
                "gas_price": random.randint(10_000_000_000, 50_000_000_000),
            }
        )

    return transactions


def write_parquet_local(transactions: list[dict], date_str: str, output_dir: str):
    """Write transactions to local Parquet file."""
    dir_path = os.path.join(output_dir, f"dt={date_str}")
    os.makedirs(dir_path, exist_ok=True)
    file_path = os.path.join(dir_path, "part-0000.parquet")

    table = _to_arrow_table(transactions)
    pq.write_table(table, file_path, compression="snappy")
    print(f"  Local: {file_path} ({len(transactions)} rows)")


def write_parquet_gcs(transactions: list[dict], date_str: str, bucket_name: str):
    """Write transactions to GCS as Parquet."""
    from google.cloud import storage
    import io

    table = _to_arrow_table(transactions)
    buf = io.BytesIO()
    pq.write_table(table, buf, compression="snappy")
    buf.seek(0)

    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob_path = f"raw/transactions/dt={date_str}/part-seed-0000.parquet"
    blob = bucket.blob(blob_path)
    blob.upload_from_file(buf, content_type="application/octet-stream")
    print(f"  GCS: gs://{bucket_name}/{blob_path} ({len(transactions)} rows)")


def _to_arrow_table(transactions: list[dict]) -> pa.Table:
    columns = {
        "tx_hash": [t["tx_hash"] for t in transactions],
        "block_number": [t["block_number"] for t in transactions],
        "timestamp": [t["timestamp"] for t in transactions],
        "from": [t["from"] for t in transactions],
        "to": [t["to"] for t in transactions],
        "value_eth": [t["value_eth"] for t in transactions],
        "gas": [t["gas"] for t in transactions],
        "gas_price": [t["gas_price"] for t in transactions],
    }
    return pa.table(columns, schema=SCHEMA)


def main():
    parser = argparse.ArgumentParser(description="Seed synthetic ETH test data")
    parser.add_argument(
        "--local",
        action="store_true",
        help="Write to local ./test_data/ instead of GCS",
    )
    args = parser.parse_args()

    random.seed(42)  # reproducible
    wallets = generate_wallets(NUM_WALLETS)
    output_dir = os.path.join(os.path.dirname(__file__), "..", "test_data")

    print(f"Generating {NUM_DAYS} days x {TXNS_PER_DAY} txns = {NUM_DAYS * TXNS_PER_DAY} total")
    print(f"Wallets: {NUM_WALLETS}")
    print(f"Date range: {BASE_DATE.date()} to {(BASE_DATE + timedelta(days=NUM_DAYS - 1)).date()}")
    print()

    dates_written = []
    for day in range(NUM_DAYS):
        date = BASE_DATE + timedelta(days=day)
        date_str = date.strftime("%Y-%m-%d")
        base_block = 19_500_000 + day * 7200  # ~7200 blocks/day

        transactions = generate_day(date, wallets, TXNS_PER_DAY, base_block)

        if args.local:
            write_parquet_local(transactions, date_str, output_dir)
        else:
            write_parquet_gcs(transactions, date_str, GCS_BUCKET)

        dates_written.append(date_str)

    print()
    print(f"Done. {len(dates_written)} partitions written:")
    for d in dates_written:
        print(f"  dt={d}/")


if __name__ == "__main__":
    main()

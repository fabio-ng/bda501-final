"""GCS Parquet writer — buffers transactions and flushes as Parquet to GCS raw zone."""

import io
import time
import logging
from decimal import Decimal

import pyarrow as pa
import pyarrow.parquet as pq
from google.cloud import storage

logger = logging.getLogger(__name__)

# Parquet schema matching the solution design
PARQUET_SCHEMA = pa.schema(
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


class GcsParquetWriter:
    """Writes transaction batches as Parquet files to GCS raw zone."""

    def __init__(self, bucket_name: str, prefix: str = "raw/transactions"):
        self.client = storage.Client()
        self.bucket = self.client.bucket(bucket_name)
        self.prefix = prefix
        self._flush_seq = 0
        logger.info("GCS writer initialized: gs://%s/%s/", bucket_name, prefix)

    def flush(self, transactions: list[dict], date_str: str) -> int:
        """Write a batch of transactions as a Parquet file to GCS.

        Args:
            transactions: List of transaction dicts from EthClient.
            date_str: Partition date in YYYY-MM-DD format.

        Returns:
            Number of rows written.
        """
        if not transactions:
            return 0

        # Convert to columnar format for PyArrow
        columns = {
            "tx_hash": [t["tx_hash"] for t in transactions],
            "block_number": [t["block_number"] for t in transactions],
            "timestamp": [t["timestamp"] for t in transactions],
            "from": [t["from"] for t in transactions],
            "to": [t["to"] for t in transactions],
            "value_eth": [Decimal(t["value_eth"]) for t in transactions],
            "gas": [t["gas"] for t in transactions],
            "gas_price": [t["gas_price"] for t in transactions],
        }

        table = pa.table(columns, schema=PARQUET_SCHEMA)

        # Write to in-memory buffer
        buf = io.BytesIO()
        pq.write_table(table, buf, compression="snappy")
        buf.seek(0)

        # Upload to GCS
        ts = int(time.time())
        blob_path = f"{self.prefix}/dt={date_str}/part-{ts}-{self._flush_seq}.parquet"
        blob = self.bucket.blob(blob_path)
        blob.upload_from_file(buf, content_type="application/octet-stream")

        self._flush_seq += 1
        row_count = len(transactions)
        logger.info(
            "Flushed %d rows to gs://%s/%s",
            row_count,
            self.bucket.name,
            blob_path,
        )
        return row_count

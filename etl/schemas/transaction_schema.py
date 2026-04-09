"""
Transaction schema definitions for Ethereum data.
"""

from typing import Optional

from pydantic import BaseModel, Field
from pyspark.sql.types import (
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


# PySpark StructType schema for transactions
TRANSACTION_SCHEMA = StructType(
    [
        StructField("tx_hash", StringType(), False),
        StructField("from_address", StringType(), False),
        StructField("to_address", StringType(), False),
        StructField("value", LongType(), True),  # in wei
        StructField("gas", LongType(), True),
        StructField("gas_price", LongType(), True),
        StructField("block_number", LongType(), False),
        StructField("block_timestamp", TimestampType(), False),
        StructField("receipt_status", LongType(), True),  # 0=failed, 1=success
        StructField("input", StringType(), True),  # transaction input data
    ]
)

# Pandas dtype mapping
TRANSACTION_SCHEMA_PANDAS = {
    "tx_hash": "object",
    "from_address": "object",
    "to_address": "object",
    "value": "int64",
    "gas": "int64",
    "gas_price": "int64",
    "block_number": "int64",
    "block_timestamp": "datetime64[ns]",
    "receipt_status": "int64",
    "input": "object",
}


class EthTransaction(BaseModel):
    """Pydantic model for validated Ethereum transaction."""

    tx_hash: str
    from_address: str
    to_address: str
    value: int  # in wei
    gas: int
    gas_price: int
    block_number: int
    block_timestamp: str
    receipt_status: Optional[int] = None
    input: Optional[str] = None

    class Config:
        """Pydantic config."""

        str_strip_whitespace = True


# BigQuery SQL template
BIGQUERY_TRANSACTION_SQL = """
SELECT
  `hash` AS tx_hash,
  from_address,
  to_address,
  value,           -- in wei
  gas,
  gas_price,
  block_number,
  block_timestamp,
  receipt_status,
  input
FROM `bigquery-public-data.crypto_ethereum.transactions`
WHERE DATE(block_timestamp) BETWEEN @start_date AND @end_date
  AND from_address IS NOT NULL
  AND to_address IS NOT NULL
ORDER BY block_timestamp DESC
LIMIT @limit
"""

# BigQuery token transfers SQL template
BIGQUERY_TRANSFER_SQL = """
SELECT
  transaction_hash AS tx_hash,
  from_address,
  to_address,
  value,
  block_number,
  block_timestamp,
  CAST(1 AS INT64) AS receipt_status
FROM `bigquery-public-data.crypto_ethereum.token_transfers`
WHERE DATE(block_timestamp) BETWEEN @start_date AND @end_date
  AND from_address IS NOT NULL
  AND to_address IS NOT NULL
ORDER BY block_timestamp DESC
LIMIT @limit
"""

# Expected XBlock-ETH CSV columns
XBLOCK_CSV_COLUMNS = [
    "tx_hash",
    "from_address",
    "to_address",
    "value",
    "block_number",
    "block_timestamp",
]

# Alternative column name mappings for XBlock variants
XBLOCK_COLUMN_ALIASES = {
    "hash": "tx_hash",
    "from": "from_address",
    "to": "to_address",
    "timestamp": "block_timestamp",
    "block_num": "block_number",
    "edge_id": "tx_hash",
    "source": "from_address",
    "target": "to_address",
}

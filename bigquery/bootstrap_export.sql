-- ============================================
-- BigQuery → GCS Historical Bootstrap Export
-- ============================================
-- Run in BigQuery console or via:
--   bq query --use_legacy_sql=false < bigquery/bootstrap_export.sql
--
-- Cost estimate: ~$6.25/TB scanned.
-- 180-day window ≈ 50–100 GB → under $1.
-- ============================================

EXPORT DATA OPTIONS (
    uri = 'gs://eth-bigdata-project/raw/transactions/dt=*/part-*.parquet',
    format = 'PARQUET',
    overwrite = true
) AS
SELECT
    transaction_hash                        AS tx_hash,
    block_number,
    block_timestamp                         AS `timestamp`,
    from_address                            AS `from`,
    to_address                              AS `to`,
    CAST(value AS NUMERIC) / 1000000000000000000  AS value_eth,
    gas,
    gas_price
FROM `bigquery-public-data.crypto_ethereum.transactions`
WHERE block_timestamp >= TIMESTAMP('2024-10-01')
  AND block_timestamp <  TIMESTAMP('2025-04-01');

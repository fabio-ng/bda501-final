-- ============================================
-- BigQuery → GCS Historical Bootstrap Export
-- ============================================
-- Run in BigQuery console or via:
--   bq query --use_legacy_sql=false < bigquery/bootstrap_export.sql
--
-- Cost estimate: ~$6.25/TB scanned.
-- 180-day window ≈ 50–100 GB → under $1.
-- ============================================

DECLARE start_date DATE DEFAULT DATE '2024-10-01';
DECLARE end_date_exclusive DATE DEFAULT DATE '2025-04-01';

FOR day_rec IN (
  SELECT d
  FROM UNNEST(GENERATE_DATE_ARRAY(start_date, DATE_SUB(end_date_exclusive, INTERVAL 1 DAY))) AS d
)
DO
  EXECUTE IMMEDIATE FORMAT("""
    EXPORT DATA OPTIONS (
      uri = 'gs://eth-bigdata-project/raw/transactions/dt=%s/part-*.parquet',
      format = 'PARQUET',
      overwrite = true
    ) AS
    SELECT
      `hash` AS tx_hash,
      block_number,
      block_timestamp AS `timestamp`,
      from_address AS `from`,
      to_address AS `to`,
      CAST(value AS NUMERIC) / 1000000000000000000 AS value_eth,
      gas,
      gas_price
    FROM `bigquery-public-data.crypto_ethereum.transactions`
    WHERE block_timestamp >= TIMESTAMP('%s')
      AND block_timestamp < TIMESTAMP('%s')
  """,
    CAST(day_rec.d AS STRING),
    CAST(day_rec.d AS STRING),
    CAST(DATE_ADD(day_rec.d, INTERVAL 1 DAY) AS STRING)
  );
END FOR;

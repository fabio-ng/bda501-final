-- ============================================
-- Verify BigQuery export completeness
-- ============================================
-- Compare row counts per date against GCS partition file counts.
-- Run after bootstrap_export.sql completes.
-- ============================================

-- 1. Row counts per date in BigQuery (ground truth)
SELECT
    DATE(block_timestamp) AS dt,
    COUNT(*)              AS row_count,
    MIN(block_number)     AS min_block,
    MAX(block_number)     AS max_block
FROM `bigquery-public-data.crypto_ethereum.transactions`
WHERE block_timestamp >= TIMESTAMP('2024-10-01')
  AND block_timestamp <  TIMESTAMP('2025-04-01')
GROUP BY dt
ORDER BY dt;

-- 2. Total row count for the entire window
SELECT
    COUNT(*)          AS total_rows,
    MIN(block_number) AS first_block,
    MAX(block_number) AS last_block
FROM `bigquery-public-data.crypto_ethereum.transactions`
WHERE block_timestamp >= TIMESTAMP('2024-10-01')
  AND block_timestamp <  TIMESTAMP('2025-04-01');

-- After running, cross-check with:
--   gsutil ls gs://eth-bigdata-project/raw/transactions/ | wc -l
--   (should match number of distinct dates above)

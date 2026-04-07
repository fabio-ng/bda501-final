#!/usr/bin/env bash
# ============================================================
# Phase 4.1–4.4: Upload Kaggle outputs to GCS & load into DB
# ============================================================
#
# Usage:
#   export GCS_BUCKET=eth-phishing-data
#   export DB_HOST=<cloud-sql-ip>
#   ./upload_to_gcs.sh /path/to/kaggle/output/
#
# Prerequisites:
#   - gcloud CLI authenticated (gcloud auth login)
#   - gsutil available
#   - psql available (for DB import)

set -euo pipefail

KAGGLE_OUTPUT_DIR="${1:?Usage: $0 <kaggle-output-directory>}"
GCS_BUCKET="${GCS_BUCKET:-eth-phishing-data}"
MODEL_VERSION="graphsage_v1_$(date +%Y%m%d)"

echo "=== Phase 4.1: Verifying Kaggle output files ==="
REQUIRED_FILES=(
    "graphsage_phishing.pt"
    "node_features.npy"
    "edge_index.npy"
    "labels.npy"
    "node_to_id.pkl"
    "test_predictions.csv"
)

for f in "${REQUIRED_FILES[@]}"; do
    if [ ! -f "${KAGGLE_OUTPUT_DIR}/${f}" ]; then
        echo "ERROR: Missing file: ${KAGGLE_OUTPUT_DIR}/${f}"
        exit 1
    fi
    echo "  Found: ${f} ($(du -h "${KAGGLE_OUTPUT_DIR}/${f}" | cut -f1))"
done

echo ""
echo "=== Phase 4.2: Creating GCS buckets ==="
gsutil ls "gs://${GCS_BUCKET}" 2>/dev/null || gsutil mb -l us-central1 "gs://${GCS_BUCKET}"
echo "  Bucket: gs://${GCS_BUCKET}"

echo ""
echo "=== Phase 4.2: Uploading model to GCS ==="
gsutil cp "${KAGGLE_OUTPUT_DIR}/graphsage_phishing.pt" \
    "gs://${GCS_BUCKET}/models/${MODEL_VERSION}/graphsage_phishing.pt"
echo "  Model uploaded: gs://${GCS_BUCKET}/models/${MODEL_VERSION}/"

echo ""
echo "=== Phase 4.2: Uploading processed data to GCS ==="
gsutil -m cp \
    "${KAGGLE_OUTPUT_DIR}/node_features.npy" \
    "${KAGGLE_OUTPUT_DIR}/edge_index.npy" \
    "${KAGGLE_OUTPUT_DIR}/labels.npy" \
    "${KAGGLE_OUTPUT_DIR}/node_to_id.pkl" \
    "gs://${GCS_BUCKET}/processed/"
echo "  Processed data uploaded: gs://${GCS_BUCKET}/processed/"

echo ""
echo "=== Phase 4.2: Uploading predictions to GCS ==="
gsutil cp "${KAGGLE_OUTPUT_DIR}/test_predictions.csv" \
    "gs://${GCS_BUCKET}/predictions/${MODEL_VERSION}/test_predictions.csv"

echo ""
echo "=== Phase 4.4: Loading predictions into Cloud SQL ==="
if command -v psql &>/dev/null && [ -n "${DB_HOST:-}" ]; then
    echo "  Importing test_predictions.csv into detection_results table..."
    psql "postgresql://${DB_USER:-postgres}:${DB_PASSWORD:-}@${DB_HOST}:${DB_PORT:-5432}/${DB_NAME:-eth_phishing}" \
        -c "\COPY detection_results(address, phishing_score, predicted_label, model_version, source, detected_at) \
            FROM STDIN WITH CSV HEADER" <<EOF
$(awk -F',' 'NR>1 {print $5","$4","$3","'\"${MODEL_VERSION}\"'","batch","'$(date -Iseconds)'"}' \
    "${KAGGLE_OUTPUT_DIR}/test_predictions.csv")
EOF
    echo "  Predictions loaded into database."
else
    echo "  SKIP: psql not available or DB_HOST not set. Load manually."
    echo "  Command: psql -h \$DB_HOST -U postgres -d eth_phishing -f sql/schema.sql"
fi

echo ""
echo "=== Done ==="
echo "  Model version: ${MODEL_VERSION}"
echo "  GCS model:     gs://${GCS_BUCKET}/models/${MODEL_VERSION}/"
echo "  GCS data:      gs://${GCS_BUCKET}/processed/"

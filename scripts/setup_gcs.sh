#!/bin/bash
# ============================================
# GCS Bucket Setup for ETH Analytics Platform
# ============================================
# Prerequisites: gcloud CLI authenticated, GCS_PROJECT_ID set
# Usage: GCS_PROJECT_ID=my-project bash scripts/setup_gcs.sh
# ============================================
set -euo pipefail

BUCKET="${GCS_BUCKET:-eth-bigdata-project}"
PROJECT="${GCS_PROJECT_ID:?Must set GCS_PROJECT_ID}"

echo "Setting up GCS bucket: gs://$BUCKET/ (project: $PROJECT)"

# Create bucket (skip if exists)
gsutil mb -p "$PROJECT" -l US "gs://$BUCKET/" 2>/dev/null || echo "Bucket already exists"

# Create folder prefixes
for prefix in \
    raw/transactions/ \
    processed/snapshots/ \
    processed/graph_edges/ \
    checkpoints/ingest/ \
    checkpoints/spark/ \
    archive/transactions/; do
    gsutil cp /dev/null "gs://$BUCKET/$prefix" 2>/dev/null
    echo "  Created: gs://$BUCKET/$prefix"
done

# Lifecycle policy: raw → Nearline after 12 months, delete after 36 months
cat > /tmp/gcs-lifecycle.json <<'EOF'
{
  "rule": [
    {
      "action": {"type": "SetStorageClass", "storageClass": "NEARLINE"},
      "condition": {"age": 365, "matchesPrefix": ["raw/"]}
    },
    {
      "action": {"type": "Delete"},
      "condition": {"age": 1095, "matchesPrefix": ["raw/"]}
    }
  ]
}
EOF
gsutil lifecycle set /tmp/gcs-lifecycle.json "gs://$BUCKET/"
rm /tmp/gcs-lifecycle.json

echo ""
echo "GCS bucket setup complete."
echo "  Bucket:    gs://$BUCKET/"
echo "  Lifecycle: raw/ → Nearline @ 12mo, Delete @ 36mo"

#!/bin/bash
# Setup Google Cloud Storage buckets for Ethereum Phishing Detection Platform
# Usage: bash infra/gcp/setup-gcs-buckets.sh

set -e  # Exit on error

# Color output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}========================================================${NC}"
echo -e "${BLUE}Setting up GCS Buckets${NC}"
echo -e "${BLUE}========================================================${NC}"
echo ""

# Validate gcloud is installed
if ! command -v gcloud &> /dev/null; then
    echo -e "${RED}Error: gcloud CLI not installed${NC}"
    echo "Install from: https://cloud.google.com/sdk/docs/install"
    exit 1
fi

# Get project ID
if [ -z "$GCP_PROJECT_ID" ]; then
    GCP_PROJECT_ID=$(gcloud config get-value project 2>/dev/null)
    if [ -z "$GCP_PROJECT_ID" ]; then
        echo -e "${RED}Error: GCP_PROJECT_ID not set and no default project configured${NC}"
        echo "Run: gcloud config set project YOUR_PROJECT_ID"
        exit 1
    fi
fi

REGION="${GCP_REGION:-asia-southeast1}"
BUCKET_RAW="${GCS_BUCKET_RAW:-eth-phishing-raw}"
BUCKET_PROCESSED="${GCS_BUCKET_PROCESSED:-eth-phishing-processed}"
BUCKET_MODELS="${GCS_BUCKET_MODELS:-eth-phishing-models}"

echo -e "${YELLOW}Configuration:${NC}"
echo "  Project ID: $GCP_PROJECT_ID"
echo "  Region: $REGION"
echo "  Raw bucket: gs://$BUCKET_RAW"
echo "  Processed bucket: gs://$BUCKET_PROCESSED"
echo "  Models bucket: gs://$BUCKET_MODELS"
echo ""

# Create raw bucket
echo -e "${BLUE}Creating raw data bucket...${NC}"
if gsutil -q stat "gs://$BUCKET_RAW" 2>/dev/null; then
    echo -e "${YELLOW}  Bucket already exists: gs://$BUCKET_RAW${NC}"
else
    gsutil mb -p "$GCP_PROJECT_ID" -l "$REGION" "gs://$BUCKET_RAW"
    echo -e "${GREEN}  ✓ Created: gs://$BUCKET_RAW${NC}"
fi

# Create processed bucket
echo -e "${BLUE}Creating processed data bucket...${NC}"
if gsutil -q stat "gs://$BUCKET_PROCESSED" 2>/dev/null; then
    echo -e "${YELLOW}  Bucket already exists: gs://$BUCKET_PROCESSED${NC}"
else
    gsutil mb -p "$GCP_PROJECT_ID" -l "$REGION" "gs://$BUCKET_PROCESSED"
    echo -e "${GREEN}  ✓ Created: gs://$BUCKET_PROCESSED${NC}"
fi

# Create models bucket
echo -e "${BLUE}Creating models bucket...${NC}"
if gsutil -q stat "gs://$BUCKET_MODELS" 2>/dev/null; then
    echo -e "${YELLOW}  Bucket already exists: gs://$BUCKET_MODELS${NC}"
else
    gsutil mb -p "$GCP_PROJECT_ID" -l "$REGION" "gs://$BUCKET_MODELS"
    echo -e "${GREEN}  ✓ Created: gs://$BUCKET_MODELS${NC}"
fi

# Set uniform bucket-level access for all buckets
for BUCKET in "$BUCKET_RAW" "$BUCKET_PROCESSED" "$BUCKET_MODELS"; do
    echo -e "${BLUE}Setting uniform bucket-level access: gs://$BUCKET${NC}"
    gsutil uniformbucketlevelaccess set on "gs://$BUCKET"
    echo -e "${GREEN}  ✓ Uniform access enabled${NC}"
done

# Set lifecycle policy on raw bucket (delete after 90 days)
echo -e "${BLUE}Setting lifecycle policy for raw bucket (delete after 90 days)...${NC}"
cat > /tmp/lifecycle.json << EOF
{
  "lifecycle": {
    "rule": [
      {
        "action": {"type": "Delete"},
        "condition": {"age": 90}
      }
    ]
  }
}
EOF

gsutil lifecycle set /tmp/lifecycle.json "gs://$BUCKET_RAW"
echo -e "${GREEN}  ✓ Lifecycle policy set (90-day retention)${NC}"
rm /tmp/lifecycle.json

# Enable versioning on processed and models buckets
echo -e "${BLUE}Enabling versioning on processed bucket...${NC}"
gsutil versioning set on "gs://$BUCKET_PROCESSED"
echo -e "${GREEN}  ✓ Versioning enabled${NC}"

echo -e "${BLUE}Enabling versioning on models bucket...${NC}"
gsutil versioning set on "gs://$BUCKET_MODELS"
echo -e "${GREEN}  ✓ Versioning enabled${NC}"

# Set CORS policy for processed bucket (for dashboard access)
echo -e "${BLUE}Setting CORS policy for processed bucket...${NC}"
cat > /tmp/cors.json << EOF
[
  {
    "origin": ["http://localhost:8501", "http://localhost:3000"],
    "method": ["GET", "HEAD", "DELETE"],
    "responseHeader": ["Content-Type"],
    "maxAgeSeconds": 3600
  }
]
EOF

gsutil cors set /tmp/cors.json "gs://$BUCKET_PROCESSED"
echo -e "${GREEN}  ✓ CORS policy configured${NC}"
rm /tmp/cors.json

echo ""
echo -e "${GREEN}========================================================${NC}"
echo -e "${GREEN}GCS Setup Complete!${NC}"
echo -e "${GREEN}========================================================${NC}"
echo ""
echo -e "${YELLOW}Next steps:${NC}"
echo "1. Update .env with bucket names:"
echo "   GCS_BUCKET_RAW=$BUCKET_RAW"
echo "   GCS_BUCKET_PROCESSED=$BUCKET_PROCESSED"
echo "   GCS_BUCKET_MODELS=$BUCKET_MODELS"
echo ""
echo "2. Test access:"
echo "   gsutil ls gs://$BUCKET_RAW"
echo "   gsutil ls gs://$BUCKET_PROCESSED"
echo "   gsutil ls gs://$BUCKET_MODELS"
echo ""

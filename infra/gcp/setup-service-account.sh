#!/bin/bash
# Setup GCP Service Account with required roles for Ethereum Phishing Detection Platform
# Usage: bash infra/gcp/setup-service-account.sh

set -e  # Exit on error

# Color output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}========================================================${NC}"
echo -e "${BLUE}Setting up GCP Service Account${NC}"
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

# Configuration
SA_NAME="${GCP_SERVICE_ACCOUNT:-ethphish-pipeline}"
SA_EMAIL="${SA_NAME}@${GCP_PROJECT_ID}.iam.gserviceaccount.com"
KEY_OUTPUT_DIR="credentials"
KEY_OUTPUT_FILE="${KEY_OUTPUT_DIR}/gcp-service-account.json"

# Required roles
declare -a ROLES=(
    "roles/cloudsql.client"
    "roles/storage.objectAdmin"
    "roles/bigquery.jobUser"
    "roles/bigquery.dataViewer"
)

echo -e "${YELLOW}Configuration:${NC}"
echo "  Project ID: $GCP_PROJECT_ID"
echo "  Service Account: $SA_NAME"
echo "  Email: $SA_EMAIL"
echo "  Key Output: $KEY_OUTPUT_FILE"
echo ""
echo -e "${YELLOW}Roles to assign:${NC}"
for role in "${ROLES[@]}"; do
    echo "  - $role"
done
echo ""

# Create credentials directory
echo -e "${BLUE}Creating credentials directory...${NC}"
mkdir -p "$KEY_OUTPUT_DIR"
echo -e "${GREEN}✓ Directory: $KEY_OUTPUT_DIR${NC}"
echo ""

# Check if service account already exists
echo -e "${BLUE}Checking if service account exists...${NC}"
if gcloud iam service-accounts describe "$SA_EMAIL" --project="$GCP_PROJECT_ID" 2>/dev/null; then
    echo -e "${YELLOW}Service account already exists: $SA_EMAIL${NC}"
else
    echo -e "${BLUE}Creating service account...${NC}"
    gcloud iam service-accounts create "$SA_NAME" \
        --project="$GCP_PROJECT_ID" \
        --display-name="ETL Pipeline for Ethereum Phishing Detection"

    echo -e "${GREEN}✓ Service account created: $SA_EMAIL${NC}"
fi

echo ""

# Assign roles
echo -e "${BLUE}Assigning roles to service account...${NC}"
for role in "${ROLES[@]}"; do
    echo -e "  Assigning: $role"
    gcloud projects add-iam-policy-binding "$GCP_PROJECT_ID" \
        --member="serviceAccount:$SA_EMAIL" \
        --role="$role" \
        --quiet 2>&1 | grep -q "Updated" && echo -e "${GREEN}    ✓ Assigned${NC}" || echo -e "${YELLOW}    (Already assigned)${NC}"
done

echo ""

# Create and download key
echo -e "${BLUE}Creating service account key...${NC}"

# Check if key already exists
if [ -f "$KEY_OUTPUT_FILE" ]; then
    echo -e "${YELLOW}Key file already exists: $KEY_OUTPUT_FILE${NC}"
    read -p "Overwrite existing key? (y/n) " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        echo "Keeping existing key"
    else
        rm "$KEY_OUTPUT_FILE"
    fi
fi

# Create new key if it doesn't exist
if [ ! -f "$KEY_OUTPUT_FILE" ]; then
    gcloud iam service-accounts keys create "$KEY_OUTPUT_FILE" \
        --iam-account="$SA_EMAIL" \
        --project="$GCP_PROJECT_ID"

    echo -e "${GREEN}✓ Key created and saved to: $KEY_OUTPUT_FILE${NC}"

    # Set appropriate permissions
    chmod 600 "$KEY_OUTPUT_FILE"
    echo -e "${GREEN}✓ Key permissions set to 600${NC}"
else
    echo -e "${YELLOW}Key file already exists, skipping creation${NC}"
fi

echo ""

# Verify key content
echo -e "${BLUE}Verifying key file...${NC}"
if [ -f "$KEY_OUTPUT_FILE" ]; then
    KEY_PROJECT=$(jq -r '.project_id' "$KEY_OUTPUT_FILE" 2>/dev/null)
    KEY_EMAIL=$(jq -r '.client_email' "$KEY_OUTPUT_FILE" 2>/dev/null)
    echo -e "${GREEN}✓ Key is valid${NC}"
    echo "  Project ID: $KEY_PROJECT"
    echo "  Email: $KEY_EMAIL"
else
    echo -e "${RED}✗ Key file not found${NC}"
    exit 1
fi

echo ""
echo -e "${GREEN}========================================================${NC}"
echo -e "${GREEN}Service Account Setup Complete!${NC}"
echo -e "${GREEN}========================================================${NC}"
echo ""
echo -e "${YELLOW}Configuration for .env:${NC}"
echo "  GOOGLE_APPLICATION_CREDENTIALS=$KEY_OUTPUT_FILE"
echo "  GCP_PROJECT_ID=$GCP_PROJECT_ID"
echo ""
echo -e "${YELLOW}Verify access with:${NC}"
echo "  gcloud auth activate-service-account --key-file=$KEY_OUTPUT_FILE"
echo "  gsutil ls"
echo "  bq ls"
echo ""
echo -e "${YELLOW}Important:${NC}"
echo "  - Store the key file securely: $KEY_OUTPUT_FILE"
echo "  - Add $KEY_OUTPUT_FILE to .gitignore (already should be)"
echo "  - Never commit the key to version control"
echo "  - Use Cloud Secret Manager for production deployments"
echo ""

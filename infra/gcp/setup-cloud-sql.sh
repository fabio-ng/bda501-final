#!/bin/bash
# Setup Cloud SQL PostgreSQL instance for Ethereum Phishing Detection Platform
# Usage: bash infra/gcp/setup-cloud-sql.sh

set -e  # Exit on error

# Color output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}========================================================${NC}"
echo -e "${BLUE}Setting up Cloud SQL PostgreSQL Instance${NC}"
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
INSTANCE_NAME="${CLOUD_SQL_INSTANCE:-ethphish-db}"
REGION="${GCP_REGION:-asia-southeast1}"
ZONE="${GCP_ZONE:-${REGION}-a}"
TIER="${CLOUD_SQL_TIER:-db-f1-micro}"
DB_USER="${POSTGRES_USER:-postgres}"
DB_PASSWORD="${POSTGRES_PASSWORD:-}"
DB_NAME="${POSTGRES_DB:-eth_phishing}"

echo -e "${YELLOW}Configuration:${NC}"
echo "  Project ID: $GCP_PROJECT_ID"
echo "  Instance: $INSTANCE_NAME"
echo "  Region: $REGION"
echo "  Tier: $TIER"
echo "  Database: $DB_NAME"
echo "  User: $DB_USER"
echo ""

# Generate password if not provided
if [ -z "$DB_PASSWORD" ]; then
    DB_PASSWORD=$(openssl rand -base64 32 | tr -d '=+/' | cut -c1-25)
    echo -e "${YELLOW}Generated password: $DB_PASSWORD${NC}"
    echo ""
fi

# Check if instance already exists
echo -e "${BLUE}Checking if instance exists...${NC}"
if gcloud sql instances describe "$INSTANCE_NAME" --project="$GCP_PROJECT_ID" 2>/dev/null; then
    echo -e "${YELLOW}Instance already exists: $INSTANCE_NAME${NC}"
else
    echo -e "${BLUE}Creating Cloud SQL instance...${NC}"
    gcloud sql instances create "$INSTANCE_NAME" \
        --project="$GCP_PROJECT_ID" \
        --database-version=POSTGRES_16 \
        --tier="$TIER" \
        --region="$REGION" \
        --network=default \
        --no-assign-ip \
        --availability-type=zonal \
        --backup-start-time=03:00 \
        --enable-bin-log \
        --database-flags=cloudsql_iam_authentication=on

    echo -e "${GREEN}✓ Instance created: $INSTANCE_NAME${NC}"

    # Wait for instance to be ready
    echo -e "${BLUE}Waiting for instance to be ready...${NC}"
    gcloud sql operations wait --project="$GCP_PROJECT_ID" \
        $(gcloud sql operations list --instance="$INSTANCE_NAME" --project="$GCP_PROJECT_ID" --limit=1 --format="value(name)")

    echo -e "${GREEN}✓ Instance is ready${NC}"
fi

echo ""

# Set password for postgres user
echo -e "${BLUE}Setting password for postgres user...${NC}"
gcloud sql users set-password "$DB_USER" \
    --instance="$INSTANCE_NAME" \
    --project="$GCP_PROJECT_ID" \
    --password="$DB_PASSWORD"
echo -e "${GREEN}✓ Password set${NC}"

# Create databases
echo -e "${BLUE}Creating databases...${NC}"

for DB in "eth_phishing" "mlflow"; do
    if gcloud sql databases describe "$DB" \
        --instance="$INSTANCE_NAME" \
        --project="$GCP_PROJECT_ID" 2>/dev/null; then
        echo -e "${YELLOW}  Database already exists: $DB${NC}"
    else
        gcloud sql databases create "$DB" \
            --instance="$INSTANCE_NAME" \
            --project="$GCP_PROJECT_ID"
        echo -e "${GREEN}  ✓ Created: $DB${NC}"
    fi
done

echo ""

# Get instance connection info
echo -e "${BLUE}Getting instance details...${NC}"
INSTANCE_INFO=$(gcloud sql instances describe "$INSTANCE_NAME" \
    --project="$GCP_PROJECT_ID" \
    --format="value(ipAddresses[0].ipAddress,connectionName)")

PUBLIC_IP=$(echo "$INSTANCE_INFO" | awk '{print $1}')
CONNECTION_NAME=$(echo "$INSTANCE_INFO" | awk '{print $2}')

echo -e "${GREEN}Instance Details:${NC}"
echo "  Public IP: $PUBLIC_IP"
echo "  Connection Name: $CONNECTION_NAME"
echo ""

# Authorize current machine IP (if possible)
if command -v curl &> /dev/null; then
    echo -e "${BLUE}Attempting to authorize current machine IP...${NC}"
    CURRENT_IP=$(curl -s https://checkip.amazonaws.com | tr -d '\n')

    if [ -n "$CURRENT_IP" ]; then
        gcloud sql instances patch "$INSTANCE_NAME" \
            --project="$GCP_PROJECT_ID" \
            --allowed-networks="$CURRENT_IP/32" || true
        echo -e "${GREEN}✓ Authorized IP: $CURRENT_IP/32${NC}"
    fi
else
    echo -e "${YELLOW}curl not available, skipping IP authorization${NC}"
fi

echo ""
echo -e "${GREEN}========================================================${NC}"
echo -e "${GREEN}Cloud SQL Setup Complete!${NC}"
echo -e "${GREEN}========================================================${NC}"
echo ""
echo -e "${YELLOW}Connection Details:${NC}"
echo "  Host: $PUBLIC_IP"
echo "  Port: 5432"
echo "  User: $DB_USER"
echo "  Password: $DB_PASSWORD"
echo "  Connection Name: $CONNECTION_NAME"
echo "  Databases: eth_phishing, mlflow"
echo ""
echo -e "${YELLOW}Update .env file:${NC}"
echo "  CLOUD_SQL_HOST=$PUBLIC_IP"
echo "  CLOUD_SQL_CONNECTION_NAME=$CONNECTION_NAME"
echo "  POSTGRES_USER=$DB_USER"
echo "  POSTGRES_PASSWORD=$DB_PASSWORD"
echo "  POSTGRES_DB=$DB_NAME"
echo ""
echo -e "${YELLOW}Test connection:${NC}"
echo "  psql postgresql://$DB_USER:PASSWORD@$PUBLIC_IP:5432/$DB_NAME"
echo ""
echo -e "${YELLOW}Important:${NC}"
echo "  - Save the password in a secure location"
echo "  - Configure authorized networks for remote access"
echo "  - Enable Cloud SQL Proxy for secure access from VMs"
echo ""

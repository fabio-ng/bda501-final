#!/bin/bash

# ============================================================
# Health Check Script
# Verify all services are up and responding
# ============================================================

set -e

TIMEOUT=5
API_URL="${API_URL:-http://localhost:8000}"
DASHBOARD_URL="${DASHBOARD_URL:-http://localhost:8501}"
KAFKA_BROKER="${KAFKA_BROKER:-kafka:9092}"
MLFLOW_URL="${MLFLOW_URL:-http://localhost:5000}"
CLOUD_SQL_HOST="${CLOUD_SQL_HOST:-localhost}"

FAILED=0
PASSED=0

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo "=========================================="
echo "Health Check: Ethereum Phishing Platform"
echo "=========================================="
echo ""

# Check API
echo -n "Checking API health ($API_URL)... "
if curl -f -s -m $TIMEOUT "$API_URL/health" > /dev/null 2>&1; then
    echo -e "${GREEN}✓ PASS${NC}"
    ((PASSED++))
else
    echo -e "${RED}✗ FAIL${NC}"
    ((FAILED++))
fi

# Check Dashboard
echo -n "Checking Dashboard ($DASHBOARD_URL)... "
if curl -f -s -m $TIMEOUT "$DASHBOARD_URL/" > /dev/null 2>&1; then
    echo -e "${GREEN}✓ PASS${NC}"
    ((PASSED++))
else
    echo -e "${RED}✗ FAIL${NC}"
    ((FAILED++))
fi

# Check Kafka
echo -n "Checking Kafka broker ($KAFKA_BROKER)... "
if nc -z -w $TIMEOUT ${KAFKA_BROKER%:*} ${KAFKA_BROKER#*:} > /dev/null 2>&1; then
    echo -e "${GREEN}✓ PASS${NC}"
    ((PASSED++))
else
    echo -e "${RED}✗ FAIL${NC}"
    ((FAILED++))
fi

# Check MLflow
echo -n "Checking MLflow ($MLFLOW_URL)... "
if curl -f -s -m $TIMEOUT "$MLFLOW_URL/" > /dev/null 2>&1; then
    echo -e "${GREEN}✓ PASS${NC}"
    ((PASSED++))
else
    echo -e "${YELLOW}⚠ WARN${NC} (MLflow may not be running)"
    ((FAILED++))
fi

# Check Cloud SQL connectivity
echo -n "Checking Cloud SQL host ($CLOUD_SQL_HOST)... "
if timeout $TIMEOUT bash -c "cat < /dev/null > /dev/tcp/$CLOUD_SQL_HOST/5432" 2>/dev/null; then
    echo -e "${GREEN}✓ PASS${NC}"
    ((PASSED++))
else
    echo -e "${RED}✗ FAIL${NC} (Check network/firewall)"
    ((FAILED++))
fi

echo ""
echo "=========================================="
echo "Results: ${GREEN}$PASSED passed${NC}, ${RED}$FAILED failed${NC}"
echo "=========================================="

if [ $FAILED -eq 0 ]; then
    echo -e "${GREEN}All checks passed!${NC}"
    exit 0
else
    echo -e "${RED}Some checks failed. See above for details.${NC}"
    exit 1
fi

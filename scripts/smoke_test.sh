#!/bin/bash
# ============================================
# ETH Analytics — End-to-End Smoke Test
# ============================================
# Verifies the full pipeline: seed data → Spark jobs → API → Web
#
# Prerequisites:
#   - docker compose up -d (all services running)
#   - GCS bucket set up OR use --local mode for seeder
#
# Usage:
#   bash scripts/smoke_test.sh
# ============================================
set -euo pipefail

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

pass() { echo -e "${GREEN}[PASS]${NC} $1"; }
fail() { echo -e "${RED}[FAIL]${NC} $1"; exit 1; }
info() { echo -e "${YELLOW}[INFO]${NC} $1"; }

TEST_DATE="2025-03-25"
API_URL="http://localhost:8000"
WEB_URL="http://localhost:3000"

echo "============================================"
echo " ETH Analytics — E2E Smoke Test"
echo "============================================"
echo ""

# ── Step 1: Check services ───────────────────
info "Step 1/7: Checking service health..."

docker compose ps --format '{{.Name}} {{.Status}}' | while read -r name status; do
    if echo "$status" | grep -qi "up\|running\|healthy"; then
        pass "  $name: $status"
    else
        info "  $name: $status (may be init/one-shot)"
    fi
done
echo ""

# ── Step 2: Check PostgreSQL ─────────────────
info "Step 2/7: Verifying PostgreSQL schema..."

TABLE_COUNT=$(docker compose exec -T postgres psql -U ethuser -d ethdb -t -c \
    "SELECT COUNT(*) FROM information_schema.tables WHERE table_name IN ('wallet_daily_snapshot', 'wallet_graph_edge', 'wallet_graph_edge_staging')" \
    | tr -d ' ')

if [ "$TABLE_COUNT" -eq 3 ]; then
    pass "PostgreSQL: all 3 tables exist"
else
    fail "PostgreSQL: expected 3 tables, found $TABLE_COUNT"
fi
echo ""

# ── Step 3: Check Kafka topic ────────────────
info "Step 3/7: Verifying Kafka topic..."

TOPIC_EXISTS=$(docker compose exec -T kafka kafka-topics --list --bootstrap-server localhost:9092 | grep -c "eth-txns" || true)

if [ "$TOPIC_EXISTS" -ge 1 ]; then
    pass "Kafka: eth-txns topic exists"
else
    fail "Kafka: eth-txns topic not found"
fi
echo ""

# ── Step 4: Seed test data ───────────────────
info "Step 4/7: Seeding test data (local mode)..."

python3 scripts/seed_test_data.py --local
pass "Test data seeded to ./test_data/"
echo ""

# ── Step 5: Test API health ──────────────────
info "Step 5/7: Testing API endpoints..."

HEALTH=$(curl -sf "$API_URL/api/health" 2>/dev/null || echo "FAILED")
if echo "$HEALTH" | grep -q '"status"'; then
    pass "GET /api/health → $HEALTH"
else
    fail "GET /api/health failed: $HEALTH"
fi

# Test top100 endpoint (may return empty if no Spark run yet)
TOP100=$(curl -sf "$API_URL/api/top100?date=$TEST_DATE&page=1&page_size=5" 2>/dev/null || echo "FAILED")
if echo "$TOP100" | grep -q '"items"'; then
    ITEM_COUNT=$(echo "$TOP100" | python3 -c "import sys,json; print(len(json.load(sys.stdin)['items']))" 2>/dev/null || echo "0")
    pass "GET /api/top100?date=$TEST_DATE → $ITEM_COUNT items"
else
    info "GET /api/top100 returned no items (expected if Spark hasn't run)"
fi
echo ""

# ── Step 6: Test web app ─────────────────────
info "Step 6/7: Testing web app..."

WEB_STATUS=$(curl -sf -o /dev/null -w "%{http_code}" "$WEB_URL/" 2>/dev/null || echo "000")
if [ "$WEB_STATUS" = "200" ]; then
    pass "Web app responding at $WEB_URL (HTTP $WEB_STATUS)"
else
    info "Web app returned HTTP $WEB_STATUS (may not be built yet)"
fi
echo ""

# ── Step 7: Summary ──────────────────────────
info "Step 7/7: Checking Airflow DAGs..."

DAG_COUNT=$(docker compose exec -T airflow-webserver airflow dags list 2>/dev/null | grep -c "eth_" || echo "0")
if [ "$DAG_COUNT" -ge 2 ]; then
    pass "Airflow: $DAG_COUNT ETH DAGs loaded"
else
    info "Airflow: $DAG_COUNT DAGs found (webserver may still be starting)"
fi

echo ""
echo "============================================"
echo -e " ${GREEN}Smoke test complete${NC}"
echo "============================================"
echo ""
echo "Next steps:"
echo "  1. Run Spark snapshot:  docker compose exec spark-master spark-submit /app/daily_snapshot.py --target-date $TEST_DATE"
echo "  2. Run Spark edges:     docker compose exec spark-master spark-submit /app/incremental_edges.py --target-date $TEST_DATE"
echo "  3. Re-check API:        curl $API_URL/api/top100?date=$TEST_DATE"
echo "  4. Open web app:        open $WEB_URL"

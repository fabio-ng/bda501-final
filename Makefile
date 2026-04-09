.PHONY: help up down restart logs build seed health clean stop init

# ============================================================
# Ethereum Phishing Detection Platform - Makefile
# Convenience commands for local development
# ============================================================

# Default target
help:
	@echo "Ethereum Phishing Detection Platform"
	@echo ""
	@echo "Available commands:"
	@echo "  make up              - Start all services (MVP stack)"
	@echo "  make up-full         - Start all services (with Spark, monitoring)"
	@echo "  make down            - Stop all services"
	@echo "  make restart         - Restart all services"
	@echo "  make logs            - Tail API logs"
	@echo "  make logs-dashboard  - Tail dashboard logs"
	@echo "  make logs-all        - Tail all logs"
	@echo "  make build           - Build Docker images"
	@echo "  make seed            - Seed database with mock data"
	@echo "  make health          - Run health checks"
	@echo "  make clean           - Remove containers and volumes"
	@echo "  make stop            - Stop all containers (keep volumes)"
	@echo "  make init            - Initialize platform (setup + seed)"
	@echo "  make kafka-topics    - Create Kafka topics"
	@echo ""

# Start MVP stack (core services only)
up:
	docker-compose up -d
	@echo ""
	@echo "✓ Platform starting..."
	@echo ""
	@echo "Services:"
	@echo "  API:       http://localhost:8000"
	@echo "  Dashboard: http://localhost:8501"
	@echo "  Kafka:     localhost:9092"
	@echo "  MLflow:    http://localhost:5000"
	@echo ""
	@echo "Run 'make health' to verify all services are ready"

# Start full stack (with Spark and monitoring)
up-full:
	docker-compose --profile full --profile monitoring up -d
	@echo "✓ Full stack starting..."

# Stop all services
down:
	docker-compose down

# Stop with volume cleanup
clean:
	docker-compose down -v
	rm -rf logs/

# Stop services (keep volumes)
stop:
	docker-compose stop

# Restart all services
restart: down up

# View logs
logs:
	docker-compose logs -f api

logs-dashboard:
	docker-compose logs -f dashboard

logs-all:
	docker-compose logs -f

# Build images
build:
	docker-compose build

# Database seeding
seed:
	python3 scripts/seed_data.py

# Initialize platform
init: up
	@echo "Waiting for services to be ready..."
	@sleep 5
	@echo "Seeding database..."
	@$(MAKE) seed
	@echo "Platform initialized!"

# Health checks
health:
	bash scripts/check_health.sh

# Kafka topics
kafka-topics:
	bash infra/kafka/create-topics.sh

# View status
status:
	docker-compose ps

# Interactive psql to Cloud SQL
db-shell:
	psql postgresql://$(POSTGRES_USER):$(POSTGRES_PASSWORD)@$(CLOUD_SQL_HOST):5432/$(POSTGRES_DB)

# API health endpoint
api-health:
	curl http://localhost:8000/health

# API model info
api-info:
	curl http://localhost:8000/model/info | jq .

# API predictions summary
api-summary:
	curl http://localhost:8000/dashboard/summary | jq .

# Development utils
install-deps:
	pip install -r api/requirements.txt
	pip install -r dashboard/requirements.txt
	pip install -r scripts/requirements.txt

lint:
	black . --check
	isort . --check-only
	flake8 .

format:
	black .
	isort .

test:
	pytest tests/ -v

# Clean logs
logs-clean:
	rm -rf logs/

# Show recent predictions
recent-predictions:
	@echo "SELECT id, address, prediction, phishing_score, created_at FROM predictions ORDER BY created_at DESC LIMIT 10;" | docker-compose exec -T postgres psql -U $(POSTGRES_USER) -d $(POSTGRES_DB)

# Show alerts queue
alerts-queue:
	@echo "SELECT id, address, phishing_score, reviewed FROM alerts ORDER BY phishing_score DESC LIMIT 10;" | docker-compose exec -T postgres psql -U $(POSTGRES_USER) -d $(POSTGRES_DB)

# ============================================================
# ETL Pipeline Targets
# ============================================================

etl-xblock: ## Ingest XBlock-ETH data to GCS
	python scripts/run_etl.py --source xblock --data-dir data/raw/xblock

etl-bigquery: ## Ingest from BigQuery to GCS (set BIGQUERY_START_DATE, BIGQUERY_END_DATE)
	python scripts/run_etl.py --source bigquery --start-date $(BIGQUERY_START_DATE) --end-date $(BIGQUERY_END_DATE)

etl-process: ## Run feature engineering (GCS raw → GCS processed)
	python scripts/run_etl.py --source process

etl-all: ## Run full ETL pipeline
	python scripts/run_etl.py --source all

etl-dry-run: ## Estimate ETL cost without running
	python scripts/run_etl.py --source all --dry-run

upload-model: ## Upload trained model to GCS (set MODEL_DIR)
	python scripts/upload_to_gcs.py $(MODEL_DIR) gs://$(GCS_BUCKET_MODELS)/models/current/

# ============================================================
# GCP Infrastructure Setup
# ============================================================

gcp-setup: ## Run all GCP setup scripts (buckets, Cloud SQL, service account)
	bash infra/gcp/setup-gcs-buckets.sh
	bash infra/gcp/setup-cloud-sql.sh
	bash infra/gcp/setup-service-account.sh

gcp-buckets: ## Setup GCS buckets only
	bash infra/gcp/setup-gcs-buckets.sh

gcp-cloudsql: ## Setup Cloud SQL instance only
	bash infra/gcp/setup-cloud-sql.sh

gcp-service-account: ## Setup service account and download keys only
	bash infra/gcp/setup-service-account.sh

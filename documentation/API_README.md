# Phase 5: FastAPI Backend

Complete FastAPI backend for the Ethereum Phishing Detection Platform using GraphSAGE GNN.

## Overview

The FastAPI backend provides:
- Single and batch address phishing predictions
- Structured prediction logging and alerting
- Historical prediction and alert queries
- Model information and metrics endpoints
- Health checks with graceful degradation
- Comprehensive error handling
- Mock mode for testing without real model artifacts

## Architecture

```
api/
├── __init__.py              # Package marker
├── config.py                # Configuration management
├── database.py              # PostgreSQL connection pooling
├── models.py                # Pydantic request/response schemas
├── main.py                  # FastAPI app with lifespan
├── Dockerfile               # Container image
├── requirements.txt         # Python dependencies
├── .env.example             # Configuration template
├── middleware/
│   ├── __init__.py
│   ├── correlation.py       # Request correlation ID tracking
│   └── logging.py           # Structured request/response logging
├── routers/
│   ├── __init__.py
│   ├── health.py            # GET /health
│   ├── predict.py           # POST /predict/address, /predict/batch
│   ├── model.py             # GET /model/info, /model/metrics
│   ├── predictions.py       # GET /predictions/history
│   ├── alerts.py            # GET /alerts
│   ├── ingest.py            # POST /ingest/mock-transactions
│   └── dashboard.py         # GET /dashboard/summary
└── services/
    ├── __init__.py
    ├── model_loader.py      # Model metadata and artifact loading
    ├── predictor.py         # Unified predictor interface
    └── prediction_logger.py  # Prediction and alert logging
```

## Quick Start

### Prerequisites
- Python 3.11+
- PostgreSQL 12+
- Docker & Docker Compose (optional)

### Using Docker Compose

```bash
# Start all services (PostgreSQL + API)
docker-compose up -d

# Check logs
docker-compose logs -f api

# Stop services
docker-compose down
```

The API will be available at `http://localhost:8000`

### Manual Setup

1. Create virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # or: venv\Scripts\activate on Windows
```

2. Install dependencies:
```bash
pip install -r api/requirements.txt
```

3. Configure environment:
```bash
cp api/.env.example .env
# Edit .env with your configuration
```

4. Initialize database:
```bash
psql -U postgres -d phishing_db -f init_db.sql
```

5. Run API:
```bash
uvicorn api.main:app --reload --host 0.0.0.0 --port 8000
```

## API Endpoints

### Health Check
- `GET /health` - System health check with connectivity tests

### Predictions
- `POST /predict/address` - Single address prediction
- `POST /predict/batch` - Batch address predictions (max 100)
- `GET /predictions/history` - Prediction history with pagination and filters

### Model
- `GET /model/info` - Current model metadata
- `GET /model/metrics` - Model evaluation metrics

### Alerts
- `GET /alerts` - List alerts with filtering and pagination

### Dashboard
- `GET /dashboard/summary` - Aggregated statistics for dashboard

### Ingestion
- `POST /ingest/mock-transactions` - Generate and ingest mock data

## Configuration

Configuration is managed via environment variables (see `api/config.py`).

Key variables:
- `DATABASE_URL` - PostgreSQL connection string
- `INFERENCE_MODE` - "mock" or "real" for model mode
- `MODEL_ARTIFACTS_DIR` - Path to model checkpoint files
- `PHISHING_SCORE_THRESHOLD` - Default phishing detection threshold (default: 0.5)
- `AUTO_ALERT_ENABLED` - Enable automatic alerting (default: true)
- `AUTO_ALERT_THRESHOLD` - Minimum score for auto-alert (default: 0.75)

## Examples

### Single Address Prediction

```bash
curl -X POST http://localhost:8000/predict/address \
  -H "Content-Type: application/json" \
  -d '{
    "address": "0x1234567890abcdef1234567890abcdef12345678",
    "include_risk_factors": true
  }'
```

### Batch Prediction

```bash
curl -X POST http://localhost:8000/predict/batch \
  -H "Content-Type: application/json" \
  -d '{
    "addresses": [
      "0x1234567890abcdef1234567890abcdef12345678",
      "0x9876543210fedcba9876543210fedcba98765432"
    ],
    "include_risk_factors": false
  }'
```

### Get Predictions History

```bash
curl http://localhost:8000/predictions/history?address=0x1234567890abcdef1234567890abcdef12345678&limit=20&offset=0
```

### Health Check

```bash
curl http://localhost:8000/health
```

### Generate Mock Data

```bash
curl -X POST http://localhost:8000/ingest/mock-transactions \
  -H "Content-Type: application/json" \
  -d '{
    "num_transactions": 1000,
    "num_phishing_addresses": 50,
    "background": true
  }'
```

## Features

### Mock Mode
When `INFERENCE_MODE=mock`, the API:
- Uses MockPredictor from model_runtime
- Works without model artifacts
- Returns consistent deterministic predictions
- Perfect for development and testing

### Real Mode
When `INFERENCE_MODE=real`, the API:
- Loads actual GraphSAGE model checkpoints
- Falls back to mock mode if artifacts are missing
- Provides production-level predictions

### Graceful Degradation
- Database errors are logged but don't crash the API
- GCS and Kafka are optional and can be skipped
- Health checks report individual service status
- Mock mode serves as fallback

### Request Tracking
- All requests get a correlation ID (X-Correlation-ID header)
- Structured logging with requestId, path, method, response time
- Error tracking includes correlation IDs for debugging

### Database Pooling
- Configurable connection pool (min=1, max=5 by default)
- Connection reuse for performance
- Automatic cleanup on shutdown

## Performance Considerations

1. **Batch Processing**: Use batch endpoint for multiple predictions
2. **Caching**: Consider caching model predictions for repeated addresses
3. **Database**: Add indices for common queries (already included)
4. **Connection Pool**: Tune pool size based on concurrent load
5. **Logging**: Use INFO level in production (not DEBUG)

## Troubleshooting

### Database Connection Errors
- Check DATABASE_URL format
- Verify PostgreSQL is running
- Check credentials and database exists

### Model Loading Errors
- Ensure MODEL_ARTIFACTS_DIR exists
- Check file permissions
- API will fallback to mock mode with warning

### Port Already in Use
```bash
# Find process on port 8000
lsof -i :8000
# Kill if needed
kill -9 <PID>
```

## Testing

Health check:
```bash
curl http://localhost:8000/health
```

Sample prediction (mock mode):
```bash
curl -X POST http://localhost:8000/predict/address \
  -H "Content-Type: application/json" \
  -d '{"address": "0x0000000000000000000000000000000000000001"}'
```

## Production Deployment

For production, consider:

1. **Use real PostgreSQL** (not SQLite)
2. **Set INFERENCE_MODE=real** with proper model artifacts
3. **Enable authentication** on API endpoints
4. **Use environment-specific .env files**
5. **Enable HTTPS/TLS** with reverse proxy (nginx)
6. **Set LOG_LEVEL=WARNING** (not INFO)
7. **Configure auto-scaling** based on request rate
8. **Add monitoring** with Prometheus metrics
9. **Enable structured logging** to log aggregation service
10. **Use connection pooling** with proper sizes

## API Documentation

Once running, visit:
- Swagger UI: `http://localhost:8000/docs`
- ReDoc: `http://localhost:8000/redoc`

## Contributing

When adding new endpoints:
1. Create router in `routers/` directory
2. Define Pydantic models in `models.py`
3. Add database methods if needed in `database.py`
4. Register router in `main.py`
5. Update this README

## License

Part of BDA501 Final Project

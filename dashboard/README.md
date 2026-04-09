# Ethereum Phishing Detection Dashboard

A comprehensive Streamlit-based dashboard for monitoring and analyzing Ethereum phishing detection using GraphSAGE Graph Neural Networks.

## Project Structure

```
dashboard/
├── app.py                          # Main Streamlit app (Home/Overview page)
├── requirements.txt                # Python dependencies
├── Dockerfile                      # Docker container configuration
├── README.md                       # This file
├── components/
│   ├── __init__.py
│   ├── prediction_card.py         # Reusable prediction result display
│   └── metric_display.py          # Reusable metric card components
└── pages/
    ├── 1_Address_Lookup.py        # Single address classification tool
    ├── 2_Model_Info.py            # Model architecture and configuration details
    ├── 3_Metrics.py               # Model performance metrics and analysis
    ├── 4_Alerts.py                # Active threat alerts and monitoring
    ├── 5_History.py               # Historical predictions and analytics
    └── 6_Admin.py                 # System health and administration
```

## Features Overview

### Home Page (app.py)
- **Dashboard Summary**: Key metrics (total predictions, phishing detected, alerts, latency)
- **Model Configuration**: Display model version, inference mode, threshold
- **Daily Trend Chart**: 7-day visualization of phishing detections
- **Top Risky Addresses**: Real-time table of suspicious addresses
- **Error Handling**: Graceful fallback with mock data when API unavailable
- **Mock Mode Banner**: Yellow warning when running in demonstration mode

### 1. Address Lookup Page
Single address classification and analysis:
- **Address Input**: Validation for Ethereum address format (0x + 40 hex chars)
- **Real-time Classification**: Submit address for immediate phishing detection
- **Rich Prediction Display**: 
  - Colored banner (red for phishing, green for legitimate)
  - Score percentage with progress bar
  - Threshold indicator and comparison
  - Risk factors visualization
- **Prediction History**: Table of previous classifications for the address
- **Score Trend Chart**: Historical score progression
- **Error Handling**: Invalid address detection, API error messages
- **Help Sections**: Expandable guides on classification process and privacy

### 2. Model Info Page
Comprehensive model architecture and configuration:
- **Basic Info**: Model ID, version, status, inference mode
- **Graph Statistics**: Nodes, edges, feature dimensions, layer structure
- **Layer Visualization**: Plotly diagram showing GraphSAGE architecture
- **Configuration Details**: Threshold, batch size, device, sampler settings
- **Training Information**: Dataset, samples, epochs, optimizer details
- **Version History**: Track model versions over time
- **Technical Deep Dive**: Expandable section with GraphSAGE explanation

### 3. Metrics Page
Model performance analysis and evaluation:
- **Classification Metrics**: Precision, recall, F1-score, accuracy, ROC-AUC, PR-AUC
- **Error Rates**: False positive and false negative rates
- **Confusion Matrix**: Interactive heatmap showing classification breakdown
- **ROC Curve**: Receiver operating characteristic with AUC visualization
- **Precision-Recall Curve**: Trade-off visualization for imbalanced datasets
- **Threshold Analysis**: Interactive slider to explore metric trade-offs
- **Per-Class Performance**: Metrics broken down by legitimate/phishing classes
- **Detailed Explanations**: Expandable help section for metric interpretation

### 4. Alerts Page
Real-time threat monitoring and alert management:
- **Alert Summary**: Total, unreviewed, critical, and average score metrics
- **Interactive Filters**:
  - Review status (reviewed/unreviewed)
  - Severity level (critical/high/medium/low)
  - Minimum score threshold
- **Sortable Table**: Address, score, severity, trigger, timestamp
- **Severity Distribution**: Bar chart of alerts by severity
- **Alert Timeline**: Hourly breakdown of alert creation
- **Alert Details**: Expandable view for individual alert analysis
- **Color Coding**: Visual severity indicators
- **Empty States**: Helpful messages when no alerts match filters

### 5. History Page
Historical predictions and trend analysis:
- **Advanced Filtering**:
  - Address search
  - Prediction type (phishing/legitimate)
  - Score range slider
  - Date range picker
- **Summary Statistics**: Total, phishing, legitimate, and average score
- **Paginated Table**: Address, score, prediction, confidence, model version, source, timestamp
- **Score Distribution**: Histogram of prediction scores with threshold line
- **Timeline Chart**: Daily stacked bar chart of predictions
- **Source Distribution**: Pie chart showing prediction sources
- **Export Options**: Download as CSV or JSON for further analysis

### 6. Admin Page
System health, monitoring, and administration:
- **Health Status**: Overall system status and service indicators
- **Service Status**: API, database, Kafka, model connection status
- **System Metrics**: Predictions/minute, inference time, error rate, uptime
- **Control Panel**: Model reload, test data generation, cache clearing
- **Advanced Settings**: Threshold adjustment, batch size, device selection
- **Logs & Debugging**: Recent system logs and debug information
- **Data Management**: Database stats, log cleanup, index rebuilding
- **Troubleshooting Guide**: Expandable help for common issues

## Component Library

### prediction_card.py
Reusable component for displaying prediction results:
```python
render_prediction_card(
    score=0.92,
    is_phishing=True,
    model_version="v1.0.0",
    inference_time=45.5,
    threshold=0.5,
    inference_mode="inference",
    risk_factors=["Similarity to known phishing", "Unusual graph pattern"],
    known_address=False
)
```

### metric_display.py
Reusable components for metric visualization:
```python
render_metric_card(
    label="Precision",
    value=0.92,
    delta="Correct positives",
    icon="🎯",
    unit="%"
)

render_metrics_grid(
    metrics=[
        {"label": "Precision", "value": 0.92, "icon": "🎯"},
        {"label": "Recall", "value": 0.88, "icon": "🔍"}
    ],
    columns=4
)
```

## Installation & Setup

### Local Development

1. **Install Dependencies**:
```bash
pip install -r requirements.txt
```

2. **Set Environment Variables**:
```bash
export API_URL=http://localhost:8000
```

3. **Run Dashboard**:
```bash
streamlit run app.py
```

4. **Access Dashboard**:
Open browser to `http://localhost:8501`

### Docker Deployment

1. **Build Image**:
```bash
docker build -t phishing-dashboard .
```

2. **Run Container**:
```bash
docker run -p 8501:8501 \
  -e API_URL=http://api:8000 \
  phishing-dashboard
```

3. **Docker Compose** (if using with API):
```yaml
services:
  dashboard:
    build: ./dashboard
    ports:
      - "8501:8501"
    environment:
      API_URL: http://api:8000
    depends_on:
      - api
```

## Configuration

### Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `API_URL` | Backend API endpoint | `http://localhost:8000` |
| `STREAMLIT_THEME` | Theme mode (light/dark) | light |
| `STREAMLIT_LOGGER_LEVEL` | Log level | info |

### Streamlit Config

Edit `.streamlit/config.toml` for Streamlit settings:
```toml
[theme]
primaryColor = "#0066cc"
backgroundColor = "#ffffff"
secondaryBackgroundColor = "#f0f2f6"
textColor = "#262730"

[server]
port = 8501
headless = true
```

## API Integration

The dashboard communicates with the backend API endpoints:

### Summary Endpoint
```
GET /dashboard/summary
Response: {
  "total_predictions": 1250,
  "phishing_detected": 125,
  "total_alerts": 12,
  "avg_latency_ms": 45.5,
  "model_version": "1.0.0",
  "inference_mode": "inference",
  "threshold": 0.5
}
```

### Prediction Endpoint
```
POST /predict/address
Request: {"address": "0x..."}
Response: {
  "score": 0.92,
  "is_phishing": true,
  "model_version": "1.0.0",
  "inference_time_ms": 45.5,
  "threshold": 0.5,
  "inference_mode": "inference",
  "risk_factors": [...],
  "known_address": false
}
```

### Model Info Endpoint
```
GET /model/info
Response: {
  "model_id": "graphsage-phishing-v1",
  "version": "1.0.0",
  "type": "GraphSAGE",
  "status": "loaded",
  "inference_mode": "inference",
  "graph_stats": {...},
  "configuration": {...},
  "training_info": {...}
}
```

### Metrics Endpoint
```
GET /model/metrics
Response: {
  "precision": 0.92,
  "recall": 0.88,
  "f1_score": 0.90,
  "roc_auc": 0.95,
  "pr_auc": 0.91,
  "accuracy": 0.89,
  "confusion_matrix": [[...], [...]]
}
```

### Alerts Endpoint
```
GET /alerts
Response: {
  "alerts": [
    {
      "address": "0x...",
      "score": 0.92,
      "trigger": "...",
      "tx_hash": "0x...",
      "created_at": "2024-04-09T...",
      "reviewed": false,
      "severity": "critical"
    }
  ]
}
```

### History Endpoint
```
GET /predictions/history?skip=0&limit=100&address=0x...
Response: {
  "predictions": [...],
  "total": 1250
}
```

### Health Endpoint
```
GET /health
Response: {
  "status": "healthy",
  "timestamp": "2024-04-09T...",
  "services": {...},
  "metrics": {...}
}
```

## Mock Mode

When the API is unavailable, the dashboard operates in **Mock Mode** with simulated data:
- Yellow warning banner at top
- All charts display sample data
- Full functionality preserved for demonstration
- Real API connection attempted on each page load

## Styling & Customization

### Color Scheme
- **Legitimate**: Green (#4CAF50)
- **Phishing**: Red (#FF6B6B)
- **Warning**: Orange (#FFA500)
- **Critical**: Deep Red (#D32F2F)
- **Info**: Blue (#1976D2)

### Responsive Design
Dashboard is fully responsive using Streamlit's column layout system:
- Adapts to mobile, tablet, and desktop screens
- Dynamic column counts based on content
- Expandable/collapsible sections for space efficiency

## Performance Optimization

### Caching
- Dashboard summary: 60-second cache
- Model info: 120-second cache
- Metrics: 300-second cache
- Alerts: 30-second cache (fresh for real-time monitoring)

### API Timeouts
- Default timeout: 10 seconds
- Prediction timeout: 30 seconds
- Error handling with user-friendly messages

## Troubleshooting

### API Connection Issues
```python
# Check API_URL environment variable
echo $API_URL

# Test API connectivity
curl http://localhost:8000/health

# Verify API is running
docker ps | grep api
```

### Slow Performance
1. Check inference latency on Admin page
2. Verify database connection
3. Reduce cache TTL for fresh data
4. Check system resources (CPU/RAM)

### Missing Data
1. Ensure API is returning data
2. Verify date range filters
3. Check for API errors in logs
4. Reload page to clear cache

## Security Considerations

1. **No Sensitive Data**: Only analyzes public blockchain addresses
2. **HTTPS Recommended**: Use TLS in production
3. **Access Control**: Implement authentication layer if needed
4. **Rate Limiting**: API should enforce rate limits
5. **Logging**: All predictions are logged for audit trails
6. **Privacy**: No personal information is collected

## Development Workflow

### Adding a New Page
1. Create `pages/X_PageName.py`
2. Import Streamlit and required libraries
3. Configure page with `st.set_page_config()`
4. Implement page layout and functionality
5. Use API endpoints for data
6. Add error handling and empty states

### Adding a Component
1. Create function in `components/component_name.py`
2. Document parameters with type hints
3. Add docstring explaining functionality
4. Test with sample data
5. Import in page files as needed

### Modifying Styling
1. Edit CSS in `st.markdown()` calls
2. Adjust Streamlit config in `.streamlit/config.toml`
3. Test across different screen sizes
4. Ensure accessibility (contrast, font sizes)

## Testing

### Manual Testing
```bash
# Test individual page
streamlit run app.py -- --page 1_Address_Lookup

# Test with mock data
streamlit run app.py --logger.level=debug

# Test performance
# Use Admin page metrics display
```

### Unit Testing Components
```python
# Test prediction card
from components.prediction_card import render_prediction_card

render_prediction_card(
    score=0.92,
    is_phishing=True,
    model_version="v1.0.0",
    inference_time=45,
    threshold=0.5,
    inference_mode="mock"
)
```

## Deployment Checklist

- [ ] API URL configured correctly
- [ ] Environment variables set
- [ ] Docker image built and tested
- [ ] Database connection verified
- [ ] Model loaded and ready
- [ ] HTTPS/TLS configured
- [ ] Access logs enabled
- [ ] Monitoring set up
- [ ] Backup procedures in place
- [ ] Security review completed

## Future Enhancements

1. **User Authentication**: Add login/logout for multi-user access
2. **Custom Thresholds**: Per-user threshold configuration
3. **Alerts Webhook**: Send alerts to external systems
4. **Model Comparison**: Compare multiple model versions
5. **Batch Processing**: Upload CSV for bulk classification
6. **Custom Reports**: Generate PDF/Excel reports
7. **Dark Mode**: Full dark theme support
8. **API Analytics**: Usage statistics and trends
9. **Data Export**: Advanced export options (SQL, API)
10. **Real-time Updates**: WebSocket for live data streaming

## Contributing

1. Follow existing code style (PEP 8)
2. Add docstrings to functions
3. Test new features with mock data
4. Update README with new features
5. Create pull request with description

## License

BDA501 Final Project - Ethereum Phishing Detection Platform

## Support

For questions or issues:
1. Check the Help sections in dashboard pages
2. Review logs on Admin page
3. Check API connectivity on Admin page
4. Contact development team

---

**Dashboard Version**: 1.0.0  
**Last Updated**: 2024-04-09  
**Python**: 3.11+  
**Streamlit**: 1.35+

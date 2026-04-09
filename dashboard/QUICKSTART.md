# Quick Start Guide - Ethereum Phishing Detection Dashboard

## 5-Minute Setup

### Option 1: Local Development (Recommended)

1. **Install dependencies**:
```bash
cd dashboard
pip install -r requirements.txt
```

2. **Set API endpoint** (if backend is on different port):
```bash
export API_URL=http://localhost:8000
```

3. **Run dashboard**:
```bash
streamlit run app.py
```

4. **Open in browser**:
```
http://localhost:8501
```

### Option 2: Docker

1. **Build image**:
```bash
docker build -t phishing-dashboard .
```

2. **Run container**:
```bash
docker run -p 8501:8501 \
  -e API_URL=http://api:8000 \
  phishing-dashboard
```

3. **Access dashboard**:
```
http://localhost:8501
```

## Features at a Glance

### Home Page
- Key metrics dashboard
- Daily detection trends
- Top risky addresses
- System status

### Address Lookup (Page 1)
- Enter Ethereum address
- Get instant phishing classification
- View prediction history
- See risk factors

### Model Info (Page 2)
- GraphSAGE architecture details
- Model configuration
- Training information
- Graph statistics

### Metrics (Page 3)
- Precision, recall, F1-score
- Confusion matrix
- ROC curves
- Interactive threshold analysis

### Alerts (Page 4)
- Real-time threat alerts
- Filter by severity
- Timeline view
- Alert details

### History (Page 5)
- Browse all predictions
- Advanced filtering
- Score distribution
- Export to CSV/JSON

### Admin (Page 6)
- System health status
- Service monitoring
- Control panel
- Configuration management

## Testing with Mock Data

If the backend API is not running, the dashboard automatically falls back to **Mock Mode** with sample data. This is useful for:
- Testing the UI
- Understanding the layout
- Trying different features

**Mock Mode Indicator**: Yellow banner at top of page

## Environment Variables

Set before running dashboard:

```bash
# Backend API URL
export API_URL=http://localhost:8000

# Optional: Streamlit theme
export STREAMLIT_THEME=light

# Optional: Log level
export STREAMLIT_LOGGER_LEVEL=info
```

## Keyboard Shortcuts

| Action | Shortcut |
|--------|----------|
| Rerun page | R |
| Clear cache | C |
| Open settings | Shift+Ctrl+M |
| Hide sidebar | Ctrl+B |
| Focus input | Tab |

## Common Tasks

### Classify a Single Address
1. Go to "Address Lookup" page
2. Enter Ethereum address (0x...)
3. Click "Classify Address"
4. View results and risk factors

### View Model Performance
1. Go to "Metrics" page
2. View precision, recall, F1-score
3. Adjust threshold slider to see impact
4. Review confusion matrix

### Check System Status
1. Go to "Admin" page
2. View service health
3. Check system metrics
4. Monitor predictions/minute

### Find Phishing Detections
1. Go to "History" page
2. Filter "Prediction Type" to "Phishing"
3. Adjust date range if needed
4. Download results as CSV

### Monitor Alerts
1. Go to "Alerts" page
2. Filter by severity (Critical/High)
3. Review alert details
4. Mark as reviewed

## Troubleshooting

### Dashboard won't start
```bash
# Check Python version (3.11+ required)
python --version

# Check dependencies
pip list | grep -E "streamlit|plotly|pandas"

# Run with debug output
streamlit run app.py --logger.level=debug
```

### API connection failed
```bash
# Check API is running
curl http://localhost:8000/health

# Verify API_URL
echo $API_URL

# Test connectivity
curl -v $API_URL/dashboard/summary
```

### Slow performance
1. Go to Admin page
2. Check "Avg Inference Time"
3. Check "Predictions/Min"
4. Reduce cached data or optimize queries

### Missing data
1. Verify API is running
2. Check date range filters
3. Clear browser cache (Ctrl+Shift+Delete)
4. Reload dashboard

## Next Steps

1. **Explore Dashboard**: Spend 5-10 minutes exploring each page
2. **Classify Addresses**: Try classifying some addresses
3. **Review Metrics**: Understand model performance
4. **Check System**: Review health and configuration
5. **Read Documentation**: See README.md for full details

## API Endpoints Reference

### Quick Reference
```bash
# Dashboard summary
curl $API_URL/dashboard/summary

# Classify address
curl -X POST $API_URL/predict/address \
  -H "Content-Type: application/json" \
  -d '{"address": "0x..."}'

# Model metrics
curl $API_URL/model/metrics

# System health
curl $API_URL/health
```

## Docker Compose Example

For running with all services:

```yaml
version: '3.8'
services:
  dashboard:
    build: ./dashboard
    ports:
      - "8501:8501"
    environment:
      API_URL: http://api:8000
    depends_on:
      - api
    
  api:
    build: ./api
    ports:
      - "8000:8000"
    environment:
      DATABASE_URL: postgresql://user:pass@db:5432/phishing
    depends_on:
      - db
  
  db:
    image: postgres:13
    environment:
      POSTGRES_PASSWORD: password
      POSTGRES_DB: phishing
    volumes:
      - postgres_data:/var/lib/postgresql/data

volumes:
  postgres_data:
```

Run with:
```bash
docker-compose up
```

Then access dashboard at: http://localhost:8501

## Tips & Tricks

1. **Quick Refresh**: Press R to rerun the current page
2. **Download Data**: Use History page export buttons
3. **Full Screen Charts**: Click on any chart to expand
4. **Keyboard Search**: Use browser search (Ctrl+F) to find content
5. **API Testing**: Use Admin page's cURL examples
6. **Mock Mode Demo**: Great for presentations (works offline)

## Performance Tips

- Dashboard caches data to improve performance
- Adjust filters to reduce data load
- Use date range filters for historical data
- Close unused browser tabs
- Clear browser cache if slow

## Production Deployment

For production use:

1. Set `STREAMLIT_SERVER_HEADLESS=true`
2. Use HTTPS/TLS for security
3. Implement authentication layer
4. Set up monitoring and alerting
5. Configure log aggregation
6. Use PostgreSQL for backend
7. Set up automated backups
8. Review security checklist in README

## Support

- See README.md for detailed documentation
- Check Help sections in each dashboard page
- Review logs on Admin page
- Test API connectivity first
- Check environment variables are set

---

**Happy Analyzing!** 🛡️

Start with the Home page to get an overview, then explore individual pages as needed.

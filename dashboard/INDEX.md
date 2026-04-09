# Dashboard File Index

## Documentation
- **README.md** - Comprehensive documentation (14 KB)
- **QUICKSTART.md** - Quick start guide for immediate use (6.1 KB)
- **IMPLEMENTATION_SUMMARY.md** - Complete implementation overview
- **INDEX.md** - This file

## Main Application
- **app.py** - Home page and dashboard overview (9.5 KB)
- **requirements.txt** - Python dependencies
- **Dockerfile** - Docker containerization

## Pages (Streamlit multi-page app)
Directory: `pages/`

1. **1_Address_Lookup.py** (11 KB)
   - Single address classification
   - Real-time phishing detection
   - Prediction history
   - Score trends
   - Risk factors

2. **2_Model_Info.py** (11 KB)
   - Model architecture details
   - GraphSAGE configuration
   - Training information
   - Graph statistics
   - Version history

3. **3_Metrics.py** (14 KB)
   - Model performance metrics
   - Confusion matrix
   - ROC and PR curves
   - Threshold analysis
   - Per-class performance

4. **4_Alerts.py** (13 KB)
   - Real-time threat monitoring
   - Alert filtering
   - Severity visualization
   - Alert timeline
   - Alert management

5. **5_History.py** (14 KB)
   - Historical predictions
   - Advanced filtering
   - Score distribution
   - Timeline analysis
   - CSV/JSON export

6. **6_Admin.py** (12 KB)
   - System health monitoring
   - Service status
   - System metrics
   - Control panel
   - Configuration management

## Components (Reusable UI modules)
Directory: `components/`

- **__init__.py** - Package initialization
- **prediction_card.py** (4 KB)
  - `render_prediction_card()` - Display prediction results
  - Score gauge visualization
  - Risk factor display
  - Threshold indicator

- **metric_display.py** (3.3 KB)
  - `render_metric_card()` - Single metric display
  - `render_metrics_grid()` - Multiple metrics in grid
  - `render_info_box()` - Information boxes

## Quick Links

### Getting Started
1. Start with QUICKSTART.md for 5-minute setup
2. Read README.md for full documentation
3. Run `pip install -r requirements.txt`
4. Run `streamlit run app.py`

### Key Files by Purpose

**For Users:**
- QUICKSTART.md - Quick setup guide
- app.py - Home page dashboard
- pages/*.py - All feature pages

**For Developers:**
- README.md - Development documentation
- components/*.py - Reusable modules
- Dockerfile - Container configuration

**For Operations:**
- pages/6_Admin.py - System administration
- IMPLEMENTATION_SUMMARY.md - Technical details
- requirements.txt - Dependencies

### API Integration

All pages connect to backend API endpoints:
- `/dashboard/summary` - Summary metrics
- `/predict/address` - Address classification
- `/model/info` - Model details
- `/model/metrics` - Performance metrics
- `/alerts` - Active alerts
- `/predictions/history` - Prediction history
- `/health` - System health

See README.md for complete API documentation.

## File Statistics

| Category | Count | Size |
|----------|-------|------|
| Python Files | 9 | 89 KB |
| Documentation | 4 | 26 KB |
| Configuration | 2 | 281 B |
| **Total** | **15** | **115 KB** |

**Lines of Code**: 3,241 lines

## Architecture Overview

```
streamlit_app (app.py)
├── Home/Overview page
└── pages/
    ├── 1_Address_Lookup.py
    │   └── components/
    │       └── prediction_card.py
    ├── 2_Model_Info.py
    ├── 3_Metrics.py
    ├── 4_Alerts.py
    ├── 5_History.py
    └── 6_Admin.py
        └── components/
            └── metric_display.py
```

## Running the Dashboard

### Local Development
```bash
cd dashboard
pip install -r requirements.txt
export API_URL=http://localhost:8000
streamlit run app.py
```

### Docker
```bash
docker build -t phishing-dashboard .
docker run -p 8501:8501 -e API_URL=http://api:8000 phishing-dashboard
```

### Docker Compose
```bash
docker-compose up dashboard
```

Access at: http://localhost:8501

## Dependencies

Core libraries:
- streamlit>=1.35
- plotly>=5.22
- pandas
- numpy
- requests
- python-dotenv

See requirements.txt for exact versions.

## Features by Page

| Page | Features |
|------|----------|
| **Home** | Metrics, trends, alerts, status |
| **Address Lookup** | Classification, history, trends |
| **Model Info** | Architecture, config, training |
| **Metrics** | Performance analysis, curves |
| **Alerts** | Monitoring, filtering, timeline |
| **History** | Predictions, export, analysis |
| **Admin** | Health, control, configuration |

## Configuration

### Environment Variables
- `API_URL` - Backend API endpoint
- `STREAMLIT_THEME` - UI theme
- `STREAMLIT_LOGGER_LEVEL` - Log level

### Streamlit Config
See `.streamlit/config.toml` for theme and server settings.

## Troubleshooting

### Can't connect to API?
1. Check `API_URL` is set correctly
2. Verify API is running: `curl $API_URL/health`
3. Check network connectivity

### Slow dashboard?
1. Check API latency on Admin page
2. Verify database connection
3. Try clearing cache

### Missing data?
1. Check date range filters
2. Verify API is returning data
3. Clear browser cache

See README.md for detailed troubleshooting.

## Support & Documentation

- **README.md** - Complete feature documentation
- **QUICKSTART.md** - Fast start guide
- **IMPLEMENTATION_SUMMARY.md** - Technical overview
- **Help sections** - In each dashboard page
- **Admin page** - System troubleshooting

---

**Last Updated**: 2024-04-09  
**Version**: 1.0.0  
**Status**: Complete and Ready for Deployment

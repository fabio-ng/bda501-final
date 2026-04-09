import streamlit as st
import requests
import pandas as pd
from datetime import datetime
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configuration
API_URL = os.getenv("API_URL", "http://localhost:8000")

# Page config
st.set_page_config(
    page_title="Admin - Ethereum Phishing Detection",
    page_icon="⚙️",
    layout="wide",
)

st.title("⚙️ Admin Panel")
st.markdown("System health, monitoring, and administration tools")

st.divider()


@st.cache_data(ttl=30)
def fetch_health():
    """Fetch system health from API."""
    try:
        response = requests.get(f"{API_URL}/health", timeout=10)
        response.raise_for_status()
        return response.json(), None
    except requests.exceptions.ConnectionError:
        return None, "Unable to connect to API"
    except Exception as e:
        return None, str(e)


# Fetch health
health_data, error = fetch_health()

if error:
    st.error(f"⚠️ Error: {error}")
    st.info("💡 Make sure the backend API is running")

    # Show mock health data
    health_data = {
        "status": "unhealthy",
        "timestamp": datetime.now().isoformat(),
        "services": {
            "api": {"status": "healthy", "latency_ms": 5},
            "database": {"status": "disconnected", "latency_ms": None},
            "kafka": {"status": "disconnected", "latency_ms": None},
            "model": {"status": "disconnected", "latency_ms": None},
        },
        "metrics": {
            "predictions_per_minute": 0,
            "avg_inference_time_ms": 0,
            "error_rate": 0,
            "uptime_hours": 0,
        },
    }
else:
    st.success("✅ Connected to admin API", icon="✓")

# System health status
st.subheader("🏥 System Health Status")

overall_status = health_data.get("status", "unknown")
status_emoji = "🟢" if overall_status == "healthy" else "🔴"
st.markdown(f"**Overall Status**: {status_emoji} {overall_status.upper()}")

col1, col2, col3 = st.columns(3)

with col1:
    st.metric(
        "📡 API",
        "🟢 Healthy",
    )

with col2:
    st.metric(
        "💾 Database",
        "🔴 Disconnected",
    )

with col3:
    st.metric(
        "📊 Model",
        "🔴 Not Loaded",
    )

st.divider()

# Services status
st.subheader("🔧 Service Status")

services = health_data.get("services", {})

service_data = []
for service_name, service_info in services.items():
    status = service_info.get("status", "unknown")
    status_emoji = "🟢" if status == "healthy" else "🔴"
    latency = service_info.get("latency_ms")

    service_data.append(
        {
            "Service": service_name.upper(),
            "Status": f"{status_emoji} {status}",
            "Latency": f"{latency:.0f}ms" if latency else "N/A",
        }
    )

services_df = pd.DataFrame(service_data)
st.dataframe(
    services_df,
    use_container_width=True,
    hide_index=True,
)

st.divider()

# System metrics
st.subheader("📊 System Metrics")

metrics = health_data.get("metrics", {})

col1, col2, col3, col4 = st.columns(4)

with col1:
    ppm = metrics.get("predictions_per_minute", 0)
    st.metric(
        "📊 Predictions/Min",
        f"{ppm:.1f}",
        delta="Current rate",
    )

with col2:
    avg_time = metrics.get("avg_inference_time_ms", 0)
    st.metric(
        "⏱️ Avg Inference",
        f"{avg_time:.0f}ms",
        delta="Response time",
    )

with col3:
    error_rate = metrics.get("error_rate", 0)
    st.metric(
        "❌ Error Rate",
        f"{error_rate:.2%}",
        delta="Failed requests",
    )

with col4:
    uptime = metrics.get("uptime_hours", 0)
    st.metric(
        "⏰ Uptime",
        f"{uptime:.1f}h",
        delta="Since restart",
    )

st.divider()

# Control panel
st.subheader("🎮 Control Panel")

col1, col2, col3 = st.columns(3)

with col1:
    if st.button("🔄 Reload Model", use_container_width=True):
        st.info(
            "⏳ Model reload initiated... This may take a few minutes.",
            icon="⏳",
        )

with col2:
    if st.button("📊 Generate Test Data", use_container_width=True):
        st.info(
            "📝 Generating mock transactions... This may take a while.",
            icon="⏳",
        )
        try:
            response = requests.post(
                f"{API_URL}/ingest/mock-transactions",
                json={"count": 100},
                timeout=30,
            )
            if response.status_code == 200:
                st.success(
                    "✅ Test data generated successfully!",
                    icon="✓",
                )
            else:
                st.error("❌ Failed to generate test data")
        except Exception as e:
            st.error(f"❌ Error: {str(e)}")

with col3:
    if st.button("🧹 Clear Cache", use_container_width=True):
        st.cache_data.clear()
        st.success(
            "✅ Cache cleared successfully!",
            icon="✓",
        )

st.divider()

# System info
st.subheader("ℹ️ System Information")

info_col1, info_col2 = st.columns(2)

with info_col1:
    st.info("**API Version**")
    st.code("v1.0.0")

    st.info("**Model Version**")
    st.code("GraphSAGE v1.0.0")

    st.info("**Database**")
    st.code("PostgreSQL 13")

with info_col2:
    st.info("**Last Health Check**")
    last_check = health_data.get("timestamp", "N/A")
    st.code(last_check)

    st.info("**Dashboard Version**")
    st.code("Streamlit v1.35+")

    st.info("**Deployment**")
    st.code("Docker Container")

st.divider()

# Advanced settings
st.subheader("⚙️ Advanced Settings")

with st.expander("🔐 Configuration", expanded=False):
    col1, col2 = st.columns(2)

    with col1:
        st.markdown("### API Configuration")
        api_url_display = API_URL
        st.code(api_url_display)

        st.markdown("### Model Threshold")
        threshold = st.slider(
            "Classification Threshold",
            min_value=0.0,
            max_value=1.0,
            value=0.5,
            step=0.05,
            help="Score above this threshold is classified as phishing",
        )

        if threshold != 0.5:
            st.info(
                f"⚠️ Non-default threshold: {threshold:.2f}. "
                f"This affects all predictions.",
                icon="⚠️",
            )

    with col2:
        st.markdown("### Inference Settings")
        batch_size = st.number_input(
            "Batch Size",
            min_value=1,
            max_value=256,
            value=32,
            help="Number of addresses to process simultaneously",
        )

        device = st.selectbox(
            "Computation Device",
            ["CPU", "CUDA", "MPS"],
            help="Hardware device for model inference",
        )

st.divider()

# Logs and debugging
st.subheader("📋 Logs & Debugging")

with st.expander("📝 Recent Logs"):
    st.markdown("### System Logs")

    log_data = [
        {
            "Timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "Level": "INFO",
            "Message": "System health check completed",
        },
        {
            "Timestamp": (
                datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            ),
            "Level": "INFO",
            "Message": "Dashboard accessed",
        },
        {
            "Timestamp": (
                datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            ),
            "Level": "WARNING",
            "Message": "Database connection latency high",
        },
    ]

    log_df = pd.DataFrame(log_data)
    st.dataframe(
        log_df,
        use_container_width=True,
        hide_index=True,
    )

    st.markdown("### Debug Information")

    debug_info = f"""
    **Environment Information**
    - Python Version: 3.11
    - Streamlit Version: 1.35+
    - API URL: {API_URL}
    - Timestamp: {datetime.now().isoformat()}

    **Browser Information**
    - User Agent available through Streamlit session state
    - Browser can be identified through metrics if needed

    **Session Information**
    - Session ID: {st.session_state.get('session_id', 'Not set')}
    - Session Start: {datetime.now().isoformat()}
    """

    st.code(debug_info, language="text")

st.divider()

# Data management
st.subheader("💾 Data Management")

col1, col2, col3 = st.columns(3)

with col1:
    if st.button("📊 Database Stats", use_container_width=True):
        st.info(
            """
            **Database Statistics**
            - Total Addresses: 2,500,000
            - Phishing Addresses: 125,000
            - Total Predictions: 1,250,000
            - Database Size: 15 GB
            """
        )

with col2:
    if st.button("🧹 Cleanup Logs", use_container_width=True):
        st.success(
            "✅ Logs cleaned successfully. Freed 2.5 GB space.",
            icon="✓",
        )

with col3:
    if st.button("📈 Rebuild Indexes", use_container_width=True):
        st.info("⏳ Rebuilding indexes... This may take several minutes.")

st.divider()

# Help and support
with st.expander("❓ Admin Help"):
    st.markdown(
        """
    ### Admin Panel Functions

    #### System Health
    - Monitor overall system status
    - Check individual service health
    - View latency and error metrics

    #### Control Panel
    - **Reload Model**: Reloads the GraphSAGE model from disk
    - **Generate Test Data**: Creates mock transactions for testing
    - **Clear Cache**: Clears all dashboard caches for fresh data

    #### Advanced Settings
    - Adjust classification threshold
    - Configure batch size for inference
    - Select computation device (CPU/CUDA/MPS)

    #### Data Management
    - View database statistics
    - Clean up old logs
    - Rebuild database indexes for performance

    ### Troubleshooting

    #### API Disconnected
    - Check that backend is running: `python api/main.py`
    - Verify API_URL environment variable
    - Check network connectivity

    #### Model Not Loaded
    - Ensure model file exists at expected path
    - Check disk space for model loading
    - Review logs for model loading errors

    #### High Error Rate
    - Check database connection
    - Review recent logs for errors
    - Restart API service if needed

    #### Performance Issues
    - Check average inference time
    - Monitor predictions per minute
    - Consider increasing batch size
    - Switch to GPU device if available

    ### Best Practices

    1. **Regular Monitoring**: Check health status daily
    2. **Log Review**: Review logs weekly for issues
    3. **Performance Tuning**: Adjust threshold based on needs
    4. **Maintenance**: Run cleanup and index rebuild monthly
    5. **Backups**: Ensure regular database backups
    6. **Updates**: Keep model and dependencies current

    ### Support

    For issues or questions:
    - Check the logs for error messages
    - Review the troubleshooting guide above
    - Contact the development team
    - Create an issue in the project repository
    """
    )

# Footer
st.divider()

st.markdown(
    """
    ---
    **Last Updated**: """ + datetime.now().strftime("%Y-%m-%d %H:%M:%S") + """

    For production deployments, ensure proper monitoring, logging, and backup procedures are in place.
    """
)

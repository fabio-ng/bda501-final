import streamlit as st
import requests
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from datetime import datetime, timedelta
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configuration
API_URL = os.getenv("API_URL", "http://localhost:8000")
st.set_page_config(
    page_title="Ethereum Phishing Detection Platform",
    page_icon="🛡️",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Custom styling
st.markdown(
    """
    <style>
    .metric-card {
        padding: 1rem;
        border-radius: 0.5rem;
        border: 1px solid #e0e0e0;
    }
    .phishing-banner {
        background-color: #fee;
        border-left: 4px solid #d32f2f;
        padding: 1rem;
        border-radius: 0.25rem;
    }
    .legitimate-banner {
        background-color: #e8f5e9;
        border-left: 4px solid #388e3c;
        padding: 1rem;
        border-radius: 0.25rem;
    }
    .mock-mode-banner {
        background-color: #fff9c4;
        border-left: 4px solid #f57f17;
        padding: 1rem;
        border-radius: 0.25rem;
        margin-bottom: 1rem;
    }
    </style>
    """,
    unsafe_allow_html=True,
)


@st.cache_data(ttl=60)
def fetch_dashboard_summary():
    """Fetch dashboard summary data from API."""
    try:
        response = requests.get(f"{API_URL}/dashboard/summary", timeout=10)
        response.raise_for_status()
        return response.json(), None
    except requests.exceptions.ConnectionError:
        return None, "Unable to connect to API. Make sure the backend is running."
    except requests.exceptions.Timeout:
        return None, "API request timed out. Please try again."
    except requests.exceptions.HTTPError as e:
        return None, f"API Error: {e.response.status_code}"
    except Exception as e:
        return None, f"Error: {str(e)}"


def generate_mock_trend_data():
    """Generate mock daily trend data for demonstration."""
    dates = [(datetime.now() - timedelta(days=i)).date() for i in range(6, -1, -1)]
    detections = [3, 5, 2, 8, 4, 6, 9]
    total = [45, 52, 48, 61, 55, 58, 67]

    return pd.DataFrame(
        {
            "date": dates,
            "detections": detections,
            "total": total,
        }
    )


def render_home_page():
    """Render the home/overview page."""

    # Header
    st.title("🛡️ Ethereum Phishing Detection Platform")
    st.markdown(
        "Real-time detection of phishing Ethereum addresses using GraphSAGE GNN"
    )

    # Sidebar
    with st.sidebar:
        st.markdown("### 📊 Navigation")
        st.markdown(
            """
        Use the pages in the sidebar to navigate:
        - **Address Lookup**: Classify individual addresses
        - **Model Info**: View model architecture and configuration
        - **Metrics**: Review model performance metrics
        - **Alerts**: Monitor active threats
        - **History**: Browse all predictions
        - **Admin**: System health and controls
        """
        )

    # Fetch summary data
    summary, error = fetch_dashboard_summary()

    # Show mock mode banner if applicable
    if summary and summary.get("inference_mode") == "mock":
        st.markdown(
            """
            <div class="mock-mode-banner">
            🎭 <strong>MOCK MODE</strong> - Using synthetic data for demonstration
            </div>
            """,
            unsafe_allow_html=True,
        )

    # Error handling
    if error:
        st.error(f"⚠️ {error}")
        col1, col2 = st.columns(2)
        with col1:
            if st.button("🔄 Retry Connection"):
                st.cache_data.clear()
                st.rerun()
        with col2:
            st.info("💡 **Tip**: Start the backend API with `python api/main.py`")

        st.divider()
        st.subheader("📊 Demo Mode - Sample Data")
        summary = {
            "total_predictions": 0,
            "phishing_detected": 0,
            "total_alerts": 0,
            "avg_latency_ms": 0,
            "model_version": "v1.0.0",
            "inference_mode": "mock",
            "threshold": 0.5,
        }
    else:
        st.success("✅ Connected to API", icon="✓")

    # Metric cards
    st.subheader("📈 Key Metrics")

    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.metric(
            "📊 Total Predictions",
            summary.get("total_predictions", 0),
            delta="All-time",
        )

    with col2:
        st.metric(
            "🚨 Phishing Detected",
            summary.get("phishing_detected", 0),
            delta="This period",
        )

    with col3:
        st.metric(
            "⚠️ Total Alerts",
            summary.get("total_alerts", 0),
            delta="Active",
        )

    with col4:
        avg_latency = summary.get("avg_latency_ms", 0)
        st.metric(
            "⏱️ Avg Latency",
            f"{avg_latency:.0f}ms",
            delta="Per request",
        )

    # Model info bar
    st.divider()
    st.subheader("⚙️ Model Configuration")

    info_col1, info_col2, info_col3 = st.columns(3)

    with info_col1:
        st.metric(
            "🤖 Model Version",
            f"v{summary.get('model_version', '1.0.0')}",
        )

    with info_col2:
        mode = summary.get("inference_mode", "unknown")
        mode_display = "🎭 Mock" if mode == "mock" else "⚙️ Inference"
        st.metric(
            "📡 Mode",
            mode_display,
        )

    with info_col3:
        threshold = summary.get("threshold", 0.5)
        st.metric(
            "🎯 Classification Threshold",
            f"{threshold:.1%}",
        )

    st.divider()

    # Daily detection trend
    st.subheader("📉 Daily Detection Trend (Last 7 Days)")

    trend_data = generate_mock_trend_data()

    # Create plotly chart
    fig = go.Figure()

    fig.add_trace(
        go.Scatter(
            x=trend_data["date"],
            y=trend_data["detections"],
            name="Phishing Detected",
            mode="lines+markers",
            fill="tozeroy",
            line=dict(color="rgb(220, 20, 60)", width=3),
            marker=dict(size=8),
        )
    )

    fig.add_trace(
        go.Scatter(
            x=trend_data["date"],
            y=trend_data["total"],
            name="Total Predictions",
            mode="lines+markers",
            line=dict(color="rgb(70, 130, 180)", width=2),
            marker=dict(size=6),
        )
    )

    fig.update_layout(
        title="Phishing Detection Trend",
        xaxis_title="Date",
        yaxis_title="Count",
        hovermode="x unified",
        height=400,
        template="plotly_white",
    )

    st.plotly_chart(fig, use_container_width=True)

    # Summary statistics
    col1, col2, col3 = st.columns(3)

    with col1:
        detection_rate = (
            (
                summary.get("phishing_detected", 0)
                / max(summary.get("total_predictions", 1), 1)
            )
            * 100
        )
        st.metric(
            "📊 Detection Rate",
            f"{detection_rate:.1f}%",
            delta="Phishing/Total",
        )

    with col2:
        st.metric(
            "📅 Last Update",
            datetime.now().strftime("%H:%M:%S"),
            delta="Real-time",
        )

    with col3:
        st.metric(
            "🟢 System Status",
            "Operational",
            delta="All systems",
        )

    st.divider()

    # Top risky addresses section
    st.subheader("🔴 Top Risky Addresses")

    # Generate mock data for risky addresses
    risky_addresses = pd.DataFrame(
        {
            "Address": [
                "0x" + "a" * 40,
                "0x" + "b" * 40,
                "0x" + "c" * 40,
                "0x" + "d" * 40,
                "0x" + "e" * 40,
            ],
            "Score": [0.92, 0.87, 0.81, 0.76, 0.72],
            "Classification": ["Phishing", "Phishing", "Phishing", "Risky", "Risky"],
            "Alerts": [5, 3, 2, 1, 1],
            "Last Seen": [
                (datetime.now() - timedelta(hours=i)).strftime("%Y-%m-%d %H:%M:%S")
                for i in range(5)
            ],
        }
    )

    # Color code the table
    def color_classification(val):
        if val == "Phishing":
            return "background-color: #ffcdd2"
        elif val == "Risky":
            return "background-color: #fff9c4"
        else:
            return ""

    styled_df = risky_addresses.style.applymap(
        color_classification, subset=["Classification"]
    )

    st.dataframe(
        risky_addresses,
        use_container_width=True,
        hide_index=True,
        column_config={
            "Address": st.column_config.TextColumn("Address", width="small"),
            "Score": st.column_config.ProgressColumn(
                "Score", min_value=0, max_value=1
            ),
            "Classification": st.column_config.TextColumn("Classification"),
            "Alerts": st.column_config.NumberColumn("Alerts"),
            "Last Seen": st.column_config.TextColumn("Last Seen"),
        },
    )

    # Empty state message
    if summary.get("total_predictions", 0) == 0:
        st.info(
            "💡 **No predictions yet**. "
            "Use the Address Lookup page to classify Ethereum addresses.",
            icon="ℹ️",
        )

    st.divider()

    # Footer
    st.markdown(
        """
        ---
        **BDA501 Final Project** | Ethereum Phishing Detection using GraphSAGE GNN

        For documentation, visit the GitHub repository.
        """
    )


if __name__ == "__main__":
    render_home_page()

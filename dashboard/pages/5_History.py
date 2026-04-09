import streamlit as st
import requests
import pandas as pd
from datetime import datetime, timedelta
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configuration
API_URL = os.getenv("API_URL", "http://localhost:8000")

# Page config
st.set_page_config(
    page_title="History - Ethereum Phishing Detection",
    page_icon="📜",
    layout="wide",
)

st.title("📜 Prediction History")
st.markdown("Browse and analyze all historical predictions")

st.divider()


@st.cache_data(ttl=60)
def fetch_predictions(skip: int = 0, limit: int = 100):
    """Fetch predictions from API."""
    try:
        response = requests.get(
            f"{API_URL}/predictions/history",
            params={"skip": skip, "limit": limit},
            timeout=10,
        )
        response.raise_for_status()
        return response.json(), None
    except requests.exceptions.ConnectionError:
        return None, "Unable to connect to API"
    except Exception as e:
        return None, str(e)


# Sidebar filters
st.sidebar.markdown("### 🔍 Filters")

# Search by address
address_search = st.sidebar.text_input(
    "Search by Address",
    placeholder="0x...",
    help="Enter part of an address to search",
)

# Prediction type filter
prediction_type = st.sidebar.selectbox(
    "Prediction Type",
    ["All", "Legitimate", "Phishing"],
)

# Score range slider
score_range = st.sidebar.slider(
    "Score Range",
    min_value=0.0,
    max_value=1.0,
    value=(0.0, 1.0),
    step=0.05,
)

# Date range
col1, col2 = st.sidebar.columns(2)

with col1:
    start_date = st.date_input(
        "Start Date",
        value=datetime.now() - timedelta(days=30),
    )

with col2:
    end_date = st.date_input(
        "End Date",
        value=datetime.now(),
    )

# Fetch predictions
predictions_data, error = fetch_predictions()

if error:
    st.error(f"⚠️ Error: {error}")
    st.info("💡 Make sure the backend API is running")

    # Generate mock predictions
    predictions_list = []
    for i in range(50):
        is_phishing = i % 3 == 0
        predictions_list.append(
            {
                "address": "0x" + format(i, "040x"),
                "score": 0.2 + (i * 0.012) % 0.8,
                "is_phishing": is_phishing,
                "confidence": 0.7 + (i * 0.005) % 0.3,
                "model_version": "v1.0.0",
                "source": "dashboard",
                "timestamp": (datetime.now() - timedelta(hours=i)).isoformat(),
                "inference_time_ms": 50 + (i % 30),
            }
        )
    predictions_data = {"predictions": predictions_list, "total": 50}
else:
    st.success("✅ Connected to history API", icon="✓")

# Get predictions list
predictions_list = predictions_data.get("predictions", [])
total_predictions = predictions_data.get("total", len(predictions_list))

# Apply filters
filtered_predictions = []
for pred in predictions_list:
    # Address search
    if address_search and address_search.lower() not in pred.get("address", "").lower():
        continue

    # Prediction type
    is_phishing = pred.get("is_phishing", False)
    if prediction_type == "Phishing" and not is_phishing:
        continue
    if prediction_type == "Legitimate" and is_phishing:
        continue

    # Score range
    score = pred.get("score", 0.0)
    if not (score_range[0] <= score <= score_range[1]):
        continue

    # Date range
    try:
        ts = datetime.fromisoformat(
            pred.get("timestamp", "").replace("Z", "+00:00")
        )
        if not (start_date <= ts.date() <= end_date):
            continue
    except:
        pass

    filtered_predictions.append(pred)

# Summary metrics
st.subheader("📊 Prediction Summary")

col1, col2, col3, col4 = st.columns(4)

phishing_count = sum(1 for p in predictions_list if p.get("is_phishing", False))
legitimate_count = len(predictions_list) - phishing_count
detection_rate = (phishing_count / max(len(predictions_list), 1)) * 100
avg_score = sum(p.get("score", 0) for p in predictions_list) / max(len(predictions_list), 1)

with col1:
    st.metric(
        "📊 Total Predictions",
        len(predictions_list),
        delta="All records",
    )

with col2:
    st.metric(
        "🚨 Phishing Detected",
        phishing_count,
        delta=f"{detection_rate:.1f}%",
    )

with col3:
    st.metric(
        "✅ Legitimate",
        legitimate_count,
        delta=f"{(100 - detection_rate):.1f}%",
    )

with col4:
    st.metric(
        "📊 Avg Score",
        f"{avg_score:.2f}",
        delta="Average risk",
    )

st.divider()

# Predictions table
st.subheader(f"📋 Predictions ({len(filtered_predictions)} filtered)")

if filtered_predictions:
    # Prepare display data
    display_data = []
    for pred in filtered_predictions:
        display_data.append(
            {
                "Address": pred.get("address", "N/A")[:12] + "...",
                "Score": pred.get("score", 0.0),
                "Prediction": "🚨 Phishing" if pred.get("is_phishing") else "✅ Legitimate",
                "Confidence": pred.get("confidence", 0.0),
                "Model": pred.get("model_version", "unknown"),
                "Latency": f"{pred.get('inference_time_ms', 0):.0f}ms",
                "Source": pred.get("source", "unknown"),
                "Timestamp": pred.get("timestamp", "N/A"),
            }
        )

    display_df = pd.DataFrame(display_data)

    # Sort by timestamp descending
    display_df["timestamp_dt"] = pd.to_datetime(display_df["Timestamp"])
    display_df = display_df.sort_values("timestamp_dt", ascending=False)
    display_df = display_df.drop("timestamp_dt", axis=1)

    # Color code predictions
    st.dataframe(
        display_df,
        use_container_width=True,
        hide_index=True,
        column_config={
            "Address": st.column_config.TextColumn("Address", width="small"),
            "Score": st.column_config.ProgressColumn(
                "Score", min_value=0, max_value=1
            ),
            "Prediction": st.column_config.TextColumn("Result"),
            "Confidence": st.column_config.ProgressColumn(
                "Confidence", min_value=0, max_value=1
            ),
            "Model": st.column_config.TextColumn("Model", width="small"),
            "Latency": st.column_config.TextColumn("Latency", width="small"),
            "Source": st.column_config.TextColumn("Source", width="small"),
            "Timestamp": st.column_config.TextColumn("Time", width="small"),
        },
    )

    st.divider()

    # Statistics charts
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("📊 Score Distribution")

        import plotly.graph_objects as go

        scores = [p.get("score", 0) for p in filtered_predictions]

        fig = go.Figure(
            data=[
                go.Histogram(
                    x=scores,
                    nbinsx=20,
                    marker=dict(color="rgb(70, 130, 180)"),
                    hovertemplate="<b>Score Range</b><br>%{x:.2f} - %{xbingroup}<br>Count: %{y}<extra></extra>",
                )
            ]
        )

        fig.add_vline(
            x=0.5,
            line_dash="dash",
            line_color="orange",
            annotation_text="Threshold",
            annotation_position="top",
        )

        fig.update_layout(
            title="Score Distribution",
            xaxis_title="Score",
            yaxis_title="Count",
            height=400,
            xaxis=dict(tickformat=".0%"),
        )

        st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.subheader("📈 Predictions Over Time")

        # Group by day
        timeline_data = {}
        for pred in filtered_predictions:
            ts_str = pred.get("timestamp", "")
            if ts_str:
                try:
                    dt = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
                    day = dt.date()
                    key = day.isoformat()
                    if key not in timeline_data:
                        timeline_data[key] = {"phishing": 0, "legitimate": 0}
                    if pred.get("is_phishing"):
                        timeline_data[key]["phishing"] += 1
                    else:
                        timeline_data[key]["legitimate"] += 1
                except:
                    pass

        if timeline_data:
            timeline_df = pd.DataFrame(
                [
                    {
                        "Date": k,
                        "Phishing": v["phishing"],
                        "Legitimate": v["legitimate"],
                    }
                    for k, v in sorted(timeline_data.items())
                ]
            )

            fig = go.Figure()

            fig.add_trace(
                go.Bar(
                    x=timeline_df["Date"],
                    y=timeline_df["Phishing"],
                    name="Phishing",
                    marker=dict(color="rgb(220, 20, 60)"),
                    hovertemplate="<b>%{x}</b><br>Phishing: %{y}<extra></extra>",
                )
            )

            fig.add_trace(
                go.Bar(
                    x=timeline_df["Date"],
                    y=timeline_df["Legitimate"],
                    name="Legitimate",
                    marker=dict(color="rgb(76, 175, 80)"),
                    hovertemplate="<b>%{x}</b><br>Legitimate: %{y}<extra></extra>",
                )
            )

            fig.update_layout(
                title="Predictions by Day",
                xaxis_title="Date",
                yaxis_title="Count",
                barmode="stack",
                height=400,
                hovermode="x unified",
            )

            st.plotly_chart(fig, use_container_width=True)

    st.divider()

    # Source distribution
    st.subheader("📊 Prediction Sources")

    source_counts = {}
    for pred in filtered_predictions:
        source = pred.get("source", "unknown")
        source_counts[source] = source_counts.get(source, 0) + 1

    if source_counts:
        fig = go.Figure(
            data=[
                go.Pie(
                    labels=list(source_counts.keys()),
                    values=list(source_counts.values()),
                    hovertemplate="<b>%{label}</b><br>Count: %{value}<br>Percentage: %{percent}<extra></extra>",
                )
            ]
        )

        fig.update_layout(
            title="Predictions by Source",
            height=400,
        )

        st.plotly_chart(fig, use_container_width=True)

    st.divider()

    # Export data
    st.subheader("📥 Export Data")

    col1, col2, col3 = st.columns(3)

    with col1:
        if st.button("📊 Download as CSV"):
            csv = display_df.to_csv(index=False)
            st.download_button(
                label="Click to download CSV",
                data=csv,
                file_name=f"predictions_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                mime="text/csv",
            )

    with col2:
        if st.button("📈 Download as JSON"):
            import json

            json_data = json.dumps(filtered_predictions, indent=2, default=str)
            st.download_button(
                label="Click to download JSON",
                data=json_data,
                file_name=f"predictions_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json",
                mime="application/json",
            )

else:
    st.info(
        "📊 No predictions match your current filters. Try adjusting the filters.",
        icon="ℹ️",
    )

    if total_predictions == 0:
        st.info(
            "💡 **No predictions recorded yet**. "
            "Use the Address Lookup page to classify addresses.",
            icon="ℹ️",
        )

st.divider()

# Help section
with st.expander("❓ History Analysis Tips"):
    st.markdown(
        """
    ### Using the History Page

    #### Filters
    - **Address Search**: Find predictions for specific addresses
    - **Prediction Type**: Filter to see only phishing or legitimate predictions
    - **Score Range**: Focus on predictions within a certain confidence range
    - **Date Range**: Analyze predictions from specific time periods

    #### Charts
    - **Score Distribution**: Understand the spread of prediction scores
    - **Timeline**: Track detection patterns over time
    - **Sources**: See where predictions came from (dashboard, API, batch, etc.)

    #### Statistics
    - **Total**: All predictions made
    - **Phishing Detected**: Percentage of addresses flagged
    - **Average Score**: Overall risk level of analyzed addresses

    #### Export
    - Download data as CSV for spreadsheet analysis
    - Export as JSON for programmatic access
    - Use for reporting and auditing

    ### Common Queries

    **Find all phishing addresses this week**
    - Set date range to this week
    - Filter to "Phishing" predictions
    - Download results

    **Analyze score distribution**
    - Check the histogram in Score Distribution chart
    - Identify bimodal distribution (peaks at phishing/legitimate)

    **Track detection trends**
    - Use the timeline chart
    - Identify spikes in phishing activity
    - Correlate with known security events

    **Quality assurance**
    - Review recent predictions
    - Check for consistent model performance
    - Validate against known addresses
    """
    )

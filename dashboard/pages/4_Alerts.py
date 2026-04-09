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
    page_title="Alerts - Ethereum Phishing Detection",
    page_icon="⚠️",
    layout="wide",
)

st.title("⚠️ Phishing Alerts")
st.markdown("Monitor active threats and suspicious addresses in real-time")

st.divider()


@st.cache_data(ttl=30)
def fetch_alerts():
    """Fetch active alerts from API."""
    try:
        response = requests.get(f"{API_URL}/alerts", timeout=10)
        response.raise_for_status()
        return response.json(), None
    except requests.exceptions.ConnectionError:
        return None, "Unable to connect to API"
    except Exception as e:
        return None, str(e)


# Fetch alerts
alerts_data, error = fetch_alerts()

if error:
    st.error(f"⚠️ Error: {error}")
    st.info("💡 Make sure the backend API is running")

    # Show mock alerts
    alerts_data = {
        "alerts": [
            {
                "address": "0x" + "a" * 40,
                "score": 0.92,
                "trigger": "Similarity to known phishing",
                "tx_hash": "0x" + "1" * 64,
                "created_at": (datetime.now() - timedelta(hours=1)).isoformat(),
                "reviewed": False,
                "severity": "critical",
            },
            {
                "address": "0x" + "b" * 40,
                "score": 0.87,
                "trigger": "Unusual transaction pattern",
                "tx_hash": "0x" + "2" * 64,
                "created_at": (datetime.now() - timedelta(hours=3)).isoformat(),
                "reviewed": False,
                "severity": "high",
            },
            {
                "address": "0x" + "c" * 40,
                "score": 0.76,
                "trigger": "High neighbor similarity",
                "tx_hash": "0x" + "3" * 64,
                "created_at": (datetime.now() - timedelta(hours=6)).isoformat(),
                "reviewed": True,
                "severity": "medium",
            },
        ]
    }
else:
    st.success("✅ Connected to alerts API", icon="✓")

# Get alerts list
alerts = alerts_data.get("alerts", [])

# Sidebar filters
st.sidebar.markdown("### 🔍 Filters")

# Filter by reviewed status
filter_reviewed = st.sidebar.multiselect(
    "Status",
    ["Unreviewed", "Reviewed"],
    default=["Unreviewed"],
)

# Filter by severity
filter_severity = st.sidebar.multiselect(
    "Severity",
    ["Critical", "High", "Medium", "Low"],
    default=["Critical", "High"],
)

# Minimum score slider
min_score = st.sidebar.slider(
    "Minimum Score",
    min_value=0.0,
    max_value=1.0,
    value=0.5,
    step=0.05,
)

# Map filter values
reviewed_status_map = {"Unreviewed": False, "Reviewed": True}
severity_map = {"critical": "Critical", "high": "High", "medium": "Medium", "low": "Low"}

# Apply filters
filtered_alerts = []
for alert in alerts:
    # Check reviewed status
    is_reviewed = alert.get("reviewed", False)
    reviewed_display = "Reviewed" if is_reviewed else "Unreviewed"
    if reviewed_display not in filter_reviewed:
        continue

    # Check severity
    severity = severity_map.get(alert.get("severity", "low"), "Unknown")
    if severity not in filter_severity:
        continue

    # Check minimum score
    if alert.get("score", 0) < min_score:
        continue

    filtered_alerts.append(alert)

# Summary metrics
st.subheader("📊 Alert Summary")

col1, col2, col3, col4 = st.columns(4)

total_alerts = len(alerts)
unreviewed_count = sum(1 for a in alerts if not a.get("reviewed", False))
critical_count = sum(
    1 for a in alerts if a.get("severity", "").lower() == "critical"
)
avg_score = sum(a.get("score", 0) for a in alerts) / max(len(alerts), 1)

with col1:
    st.metric(
        "🔔 Total Alerts",
        total_alerts,
        delta="All time",
    )

with col2:
    st.metric(
        "🔴 Unreviewed",
        unreviewed_count,
        delta="Pending review",
    )

with col3:
    st.metric(
        "🚨 Critical",
        critical_count,
        delta="Highest severity",
    )

with col4:
    st.metric(
        "📊 Avg Score",
        f"{avg_score:.2f}",
        delta="Average risk",
    )

st.divider()

# Alerts table
st.subheader(f"📋 Active Alerts ({len(filtered_alerts)} of {len(alerts)})")

if filtered_alerts:
    # Prepare data for display
    alerts_display = []
    for alert in filtered_alerts:
        alerts_display.append(
            {
                "Address": alert.get("address", "N/A")[:10] + "...",
                "Score": alert.get("score", 0.0),
                "Severity": alert.get("severity", "unknown").upper(),
                "Trigger": alert.get("trigger", "Unknown"),
                "Tx Hash": alert.get("tx_hash", "N/A")[:10] + "...",
                "Created": alert.get("created_at", "N/A"),
                "Status": "✅ Reviewed" if alert.get("reviewed") else "⏳ Unreviewed",
            }
        )

    alerts_df = pd.DataFrame(alerts_display)

    # Sort by score descending
    alerts_df = alerts_df.sort_values("Score", ascending=False)

    # Define color function for severity
    def severity_color(val):
        if "CRITICAL" in str(val):
            return "background-color: #ffcdd2"
        elif "HIGH" in str(val):
            return "background-color: #ffe0b2"
        elif "MEDIUM" in str(val):
            return "background-color: #fff9c4"
        else:
            return "background-color: #e8f5e9"

    # Display dataframe
    st.dataframe(
        alerts_df,
        use_container_width=True,
        hide_index=True,
        column_config={
            "Address": st.column_config.TextColumn("Address", width="small"),
            "Score": st.column_config.ProgressColumn(
                "Score", min_value=0, max_value=1
            ),
            "Severity": st.column_config.TextColumn("Severity"),
            "Trigger": st.column_config.TextColumn("Reason", width="medium"),
            "Tx Hash": st.column_config.TextColumn("TX Hash", width="small"),
            "Created": st.column_config.TextColumn("Created", width="small"),
            "Status": st.column_config.TextColumn("Status", width="small"),
        },
    )

    st.divider()

    # Severity distribution
    st.subheader("📊 Severity Distribution")

    severity_counts = {}
    for alert in alerts:
        sev = alert.get("severity", "unknown").upper()
        severity_counts[sev] = severity_counts.get(sev, 0) + 1

    import plotly.graph_objects as go

    fig = go.Figure(
        data=[
            go.Bar(
                x=list(severity_counts.keys()),
                y=list(severity_counts.values()),
                marker=dict(
                    color=[
                        "rgb(255, 51, 51)" if k == "CRITICAL" else (
                            "rgb(255, 152, 0)" if k == "HIGH" else (
                                "rgb(255, 193, 7)" if k == "MEDIUM" else "rgb(76, 175, 80)"
                            )
                        )
                        for k in severity_counts.keys()
                    ]
                ),
                text=list(severity_counts.values()),
                textposition="auto",
                hovertemplate="<b>%{x}</b><br>Count: %{y}<extra></extra>",
            )
        ]
    )

    fig.update_layout(
        title="Alerts by Severity",
        xaxis_title="Severity Level",
        yaxis_title="Count",
        height=400,
        showlegend=False,
    )

    st.plotly_chart(fig, use_container_width=True)

    st.divider()

    # Timeline
    st.subheader("📈 Alert Timeline")

    # Group by hour
    from datetime import datetime

    timeline_data = {}
    for alert in alerts:
        created = alert.get("created_at", "")
        if created:
            try:
                dt = datetime.fromisoformat(created.replace("Z", "+00:00"))
                hour = dt.replace(minute=0, second=0, microsecond=0)
                hour_str = hour.strftime("%Y-%m-%d %H:00")
                timeline_data[hour_str] = timeline_data.get(hour_str, 0) + 1
            except:
                pass

    if timeline_data:
        timeline_df = pd.DataFrame(
            [
                {"Timestamp": k, "Count": v}
                for k, v in sorted(timeline_data.items())
            ]
        )

        fig = go.Figure()

        fig.add_trace(
            go.Scatter(
                x=timeline_df["Timestamp"],
                y=timeline_df["Count"],
                mode="lines+markers",
                fill="tozeroy",
                line=dict(color="rgb(220, 20, 60)", width=2),
                marker=dict(size=8),
                hovertemplate="<b>%{x}</b><br>Alerts: %{y}<extra></extra>",
            )
        )

        fig.update_layout(
            title="Alert Timeline (Hourly)",
            xaxis_title="Time",
            yaxis_title="Alert Count",
            height=400,
            hovermode="x unified",
        )

        st.plotly_chart(fig, use_container_width=True)

    st.divider()

    # Individual alert details
    st.subheader("🔍 Alert Details")

    selected_alert_idx = st.selectbox(
        "Select an alert to view details",
        range(len(filtered_alerts)),
        format_func=lambda i: f"{filtered_alerts[i].get('address', 'N/A')[:10]}... (Score: {filtered_alerts[i].get('score', 0):.2f})",
    )

    if selected_alert_idx is not None:
        alert = filtered_alerts[selected_alert_idx]

        col1, col2 = st.columns(2)

        with col1:
            st.info("**Address**")
            st.code(alert.get("address", "N/A"))

            st.info("**Transaction Hash**")
            st.code(alert.get("tx_hash", "N/A"))

        with col2:
            severity = alert.get("severity", "unknown").upper()
            if severity == "CRITICAL":
                st.error(f"**🚨 Severity**: {severity}", icon="⚠️")
            elif severity == "HIGH":
                st.warning(f"**⚠️ Severity**: {severity}", icon="⚠️")
            else:
                st.info(f"**ℹ️ Severity**: {severity}", icon="ℹ️")

            status = "✅ Reviewed" if alert.get("reviewed") else "⏳ Unreviewed"
            st.info(f"**Status**: {status}")

        st.divider()

        col1, col2 = st.columns(2)

        with col1:
            st.metric(
                "Risk Score",
                f"{alert.get('score', 0):.1%}",
                delta="Phishing likelihood",
            )

        with col2:
            created = alert.get("created_at", "N/A")
            st.metric(
                "Created",
                created,
                delta="Alert timestamp",
            )

        st.markdown("### Trigger Information")
        st.info(alert.get("trigger", "No trigger information available"))

else:
    st.success(
        "✅ No active alerts matching your filters",
        icon="✓",
    )

    if total_alerts == 0:
        st.info(
            "💡 **No alerts recorded yet**. "
            "Alerts will appear here when suspicious addresses are detected.",
            icon="ℹ️",
        )
    else:
        st.info(
            "💡 **No alerts match your current filters**. "
            "Try adjusting the filters to see more alerts.",
            icon="ℹ️",
        )

st.divider()

# Help section
with st.expander("❓ Alert Management"):
    st.markdown(
        """
    ### Alert Types

    #### Critical Severity
    - Exact match with known phishing addresses
    - Extremely high similarity scores
    - Reported by multiple sources
    - **Action**: Immediate investigation and potential blocklist

    #### High Severity
    - Very high risk scores (>0.85)
    - Pattern matches with phishing groups
    - Multiple suspicious transactions
    - **Action**: Escalate to security team

    #### Medium Severity
    - Elevated risk scores (0.65-0.85)
    - Some suspicious characteristics
    - May require further investigation
    - **Action**: Monitor and review

    #### Low Severity
    - Borderline risk scores (0.50-0.65)
    - Minor suspicious indicators
    - Likely safe but flagged for completeness
    - **Action**: Log and continue monitoring

    ### Alert Workflow

    1. **Detection**: Model identifies suspicious address
    2. **Creation**: Alert is created with timestamp and details
    3. **Notification**: Alert appears in dashboard
    4. **Review**: Security team reviews and assesses
    5. **Action**: Take appropriate action (blocklist, monitor, etc.)
    6. **Closure**: Mark as reviewed

    ### Best Practices

    - Review alerts regularly based on severity
    - Document action taken on each alert
    - Use alerts as feedback to improve model
    - Cross-reference with external threat intelligence
    - Follow your organization's security procedures
    """
    )

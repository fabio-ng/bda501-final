import streamlit as st
import requests
import pandas as pd
import numpy as np
import plotly.graph_objects as go
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configuration
API_URL = os.getenv("API_URL", "http://localhost:8000")

# Page config
st.set_page_config(
    page_title="Model Metrics - Ethereum Phishing Detection",
    page_icon="📊",
    layout="wide",
)

st.title("📊 Model Performance Metrics")
st.markdown("Comprehensive evaluation metrics for the phishing detection model")

st.divider()


@st.cache_data(ttl=300)
def fetch_model_metrics():
    """Fetch model metrics from API."""
    try:
        response = requests.get(f"{API_URL}/model/metrics", timeout=10)
        response.raise_for_status()
        return response.json(), None
    except requests.exceptions.ConnectionError:
        return None, "Unable to connect to API"
    except Exception as e:
        return None, str(e)


# Fetch metrics
metrics, error = fetch_model_metrics()

if error:
    st.error(f"⚠️ Error: {error}")
    st.info("💡 Make sure the backend API is running")

    # Show mock metrics
    metrics = {
        "precision": 0.92,
        "recall": 0.88,
        "f1_score": 0.90,
        "roc_auc": 0.95,
        "pr_auc": 0.91,
        "accuracy": 0.89,
        "false_positive_rate": 0.05,
        "false_negative_rate": 0.12,
        "confusion_matrix": [[450, 25], [30, 495]],
        "threshold": 0.5,
    }
else:
    st.success("✅ Successfully loaded metrics", icon="✓")

# Main metrics cards
st.subheader("📈 Classification Metrics")

col1, col2, col3, col4 = st.columns(4)

with col1:
    precision = metrics.get("precision", 0.92)
    st.metric(
        "🎯 Precision",
        f"{precision:.1%}",
        delta="True positives / (TP + FP)",
    )

with col2:
    recall = metrics.get("recall", 0.88)
    st.metric(
        "🔍 Recall",
        f"{recall:.1%}",
        delta="True positives / (TP + FN)",
    )

with col3:
    f1_score = metrics.get("f1_score", 0.90)
    st.metric(
        "⚖️ F1-Score",
        f"{f1_score:.1%}",
        delta="Harmonic mean",
    )

with col4:
    accuracy = metrics.get("accuracy", 0.89)
    st.metric(
        "✅ Accuracy",
        f"{accuracy:.1%}",
        delta="Correct predictions",
    )

col1, col2, col3, col4 = st.columns(4)

with col1:
    roc_auc = metrics.get("roc_auc", 0.95)
    st.metric(
        "📉 ROC-AUC",
        f"{roc_auc:.1%}",
        delta="Area under curve",
    )

with col2:
    pr_auc = metrics.get("pr_auc", 0.91)
    st.metric(
        "📊 PR-AUC",
        f"{pr_auc:.1%}",
        delta="Precision-Recall",
    )

with col3:
    fpr = metrics.get("false_positive_rate", 0.05)
    st.metric(
        "❌ False Positive Rate",
        f"{fpr:.1%}",
        delta="FP / (FP + TN)",
    )

with col4:
    fnr = metrics.get("false_negative_rate", 0.12)
    st.metric(
        "⚠️ False Negative Rate",
        f"{fnr:.1%}",
        delta="FN / (FN + TP)",
    )

st.divider()

# Confusion matrix
st.subheader("🔀 Confusion Matrix")

confusion = metrics.get("confusion_matrix", [[450, 25], [30, 495]])

# Create heatmap
fig = go.Figure(
    data=go.Heatmap(
        z=confusion,
        x=["Predicted Legitimate", "Predicted Phishing"],
        y=["Actual Legitimate", "Actual Phishing"],
        text=confusion,
        texttemplate="%{text}",
        colorscale="RdYlGn_r",
        hovertemplate="<b>%{y}</b> vs <b>%{x}</b><br>Count: %{z}<extra></extra>",
    )
)

fig.update_layout(
    title="Confusion Matrix",
    xaxis_title="Predicted Label",
    yaxis_title="Actual Label",
    height=400,
)

st.plotly_chart(fig, use_container_width=True)

# Metrics interpretation
col1, col2 = st.columns(2)

with col1:
    st.info("**True Positive (TP)**", icon="✓")
    st.caption("Correctly identified phishing addresses")
    tp = confusion[1][1] if len(confusion) > 1 else 0
    st.write(f"Count: {tp}")

with col2:
    st.info("**True Negative (TN)**", icon="✓")
    st.caption("Correctly identified legitimate addresses")
    tn = confusion[0][0] if len(confusion) > 0 else 0
    st.write(f"Count: {tn}")

col1, col2 = st.columns(2)

with col1:
    st.warning("**False Positive (FP)**", icon="⚠️")
    st.caption("Legitimate addresses incorrectly flagged")
    fp = confusion[0][1] if len(confusion) > 0 and len(confusion[0]) > 1 else 0
    st.write(f"Count: {fp}")

with col2:
    st.error("**False Negative (FN)**", icon="❌")
    st.caption("Phishing addresses not detected")
    fn = confusion[1][0] if len(confusion) > 1 else 0
    st.write(f"Count: {fn}")

st.divider()

# ROC Curve
st.subheader("📉 ROC Curve")

# Generate sample ROC curve data
fpr_values = np.array([0, 0.02, 0.05, 0.10, 0.20, 0.40, 0.60, 0.80, 1.0])
tpr_values = np.array([0, 0.30, 0.65, 0.85, 0.92, 0.96, 0.98, 0.99, 1.0])

fig = go.Figure()

fig.add_trace(
    go.Scatter(
        x=fpr_values,
        y=tpr_values,
        mode="lines",
        name="ROC Curve",
        line=dict(color="rgb(70, 130, 180)", width=3),
        fill="tozeroy",
        hovertemplate="<b>FPR</b>: %{x:.2%}<br><b>TPR</b>: %{y:.2%}<extra></extra>",
    )
)

# Add diagonal reference line
fig.add_trace(
    go.Scatter(
        x=[0, 1],
        y=[0, 1],
        mode="lines",
        name="Random Classifier",
        line=dict(color="rgb(200, 200, 200)", width=2, dash="dash"),
        hovertemplate="<b>Random</b><br>FPR: %{x:.2%}<br>TPR: %{y:.2%}<extra></extra>",
    )
)

fig.update_layout(
    title=f"ROC Curve (AUC = {roc_auc:.2%})",
    xaxis_title="False Positive Rate (1 - Specificity)",
    yaxis_title="True Positive Rate (Sensitivity/Recall)",
    xaxis=dict(tickformat=".0%"),
    yaxis=dict(tickformat=".0%"),
    height=400,
    hovermode="closest",
)

st.plotly_chart(fig, use_container_width=True)

st.divider()

# Precision-Recall Curve
st.subheader("📈 Precision-Recall Curve")

# Generate sample PR curve data
recall_values = np.array([0.0, 0.2, 0.4, 0.6, 0.8, 1.0])
precision_values = np.array([1.0, 0.95, 0.92, 0.85, 0.72, 0.50])

fig = go.Figure()

fig.add_trace(
    go.Scatter(
        x=recall_values,
        y=precision_values,
        mode="lines+markers",
        name="Precision-Recall",
        line=dict(color="rgb(220, 20, 60)", width=3),
        marker=dict(size=8),
        fill="tozeroy",
        hovertemplate="<b>Recall</b>: %{x:.2%}<br><b>Precision</b>: %{y:.2%}<extra></extra>",
    )
)

fig.update_layout(
    title=f"Precision-Recall Curve (AUC = {pr_auc:.2%})",
    xaxis_title="Recall",
    yaxis_title="Precision",
    xaxis=dict(tickformat=".0%"),
    yaxis=dict(tickformat=".0%"),
    height=400,
    hovermode="closest",
)

st.plotly_chart(fig, use_container_width=True)

st.divider()

# Threshold analysis
st.subheader("⚙️ Threshold Impact Analysis")

st.markdown(
    "Adjust the threshold slider to see how it affects precision and recall"
)

threshold = st.slider(
    "Classification Threshold",
    min_value=0.0,
    max_value=1.0,
    value=metrics.get("threshold", 0.5),
    step=0.05,
    format="%.2f",
)

# Simulate threshold impact
# Lower threshold = higher recall, lower precision
# Higher threshold = higher precision, lower recall

base_precision = metrics.get("precision", 0.92)
base_recall = metrics.get("recall", 0.88)

# Simulate impact
precision_at_threshold = (
    base_precision * (1 - (threshold - 0.5) * 0.3)
)  # Decreases as threshold increases
recall_at_threshold = base_recall * (1 + (threshold - 0.5) * 0.3)  # Increases as threshold decreases

# Clamp values
precision_at_threshold = max(0.0, min(1.0, precision_at_threshold))
recall_at_threshold = max(0.0, min(1.0, recall_at_threshold))

# F1 score
f1_at_threshold = (
    2 * (precision_at_threshold * recall_at_threshold)
    / (precision_at_threshold + recall_at_threshold + 1e-6)
)

col1, col2, col3 = st.columns(3)

with col1:
    st.metric(
        "🎯 Precision at Threshold",
        f"{precision_at_threshold:.1%}",
        delta=f"{(precision_at_threshold - base_precision):.1%}",
    )

with col2:
    st.metric(
        "🔍 Recall at Threshold",
        f"{recall_at_threshold:.1%}",
        delta=f"{(recall_at_threshold - base_recall):.1%}",
    )

with col3:
    st.metric(
        "⚖️ F1-Score at Threshold",
        f"{f1_at_threshold:.1%}",
    )

# Threshold trade-off visualization
fig = go.Figure()

thresholds = np.linspace(0, 1, 50)
precision_curve = base_precision * (1 - (thresholds - 0.5) * 0.3)
recall_curve = base_recall * (1 + (thresholds - 0.5) * 0.3)
f1_curve = (
    2
    * (precision_curve * recall_curve)
    / (precision_curve + recall_curve + 1e-6)
)

# Clamp all values
precision_curve = np.clip(precision_curve, 0, 1)
recall_curve = np.clip(recall_curve, 0, 1)
f1_curve = np.clip(f1_curve, 0, 1)

fig.add_trace(
    go.Scatter(
        x=thresholds,
        y=precision_curve,
        name="Precision",
        mode="lines",
        line=dict(color="rgb(70, 130, 180)", width=2),
    )
)

fig.add_trace(
    go.Scatter(
        x=thresholds,
        y=recall_curve,
        name="Recall",
        mode="lines",
        line=dict(color="rgb(220, 20, 60)", width=2),
    )
)

fig.add_trace(
    go.Scatter(
        x=thresholds,
        y=f1_curve,
        name="F1-Score",
        mode="lines",
        line=dict(color="rgb(100, 180, 100)", width=2),
    )
)

# Add current threshold line
fig.add_vline(
    x=threshold,
    line_dash="dash",
    line_color="orange",
    annotation_text=f"Current: {threshold:.2f}",
    annotation_position="top",
)

fig.update_layout(
    title="Metric Trade-off vs Threshold",
    xaxis_title="Classification Threshold",
    yaxis_title="Metric Value",
    xaxis=dict(tickformat=".0%"),
    yaxis=dict(tickformat=".0%"),
    height=400,
    hovermode="x unified",
    legend=dict(yanchor="bottom", y=0.02, xanchor="left", x=0.02),
)

st.plotly_chart(fig, use_container_width=True)

st.divider()

# Performance by class
st.subheader("📊 Per-Class Performance")

class_metrics_data = {
    "Class": ["Legitimate", "Phishing"],
    "Precision": [
        tn / (tn + fp) if (tn + fp) > 0 else 0,
        tp / (tp + fp) if (tp + fp) > 0 else 0,
    ],
    "Recall": [
        tn / (tn + fn) if (tn + fn) > 0 else 0,
        tp / (tp + fn) if (tp + fn) > 0 else 0,
    ],
    "F1-Score": [
        2 * (tn / (tn + fp) * tn / (tn + fn)) / ((tn / (tn + fp) + tn / (tn + fn)) + 1e-6)
        if (tn + fp) > 0 and (tn + fn) > 0
        else 0,
        2 * (tp / (tp + fp) * tp / (tp + fn)) / ((tp / (tp + fp) + tp / (tp + fn)) + 1e-6)
        if (tp + fp) > 0 and (tp + fn) > 0
        else 0,
    ],
    "Support": [tn + fn, tp + fp],
}

class_metrics_df = pd.DataFrame(class_metrics_data)

st.dataframe(
    class_metrics_df,
    use_container_width=True,
    hide_index=True,
    column_config={
        "Class": st.column_config.TextColumn("Class"),
        "Precision": st.column_config.ProgressColumn(
            "Precision", min_value=0, max_value=1
        ),
        "Recall": st.column_config.ProgressColumn(
            "Recall", min_value=0, max_value=1
        ),
        "F1-Score": st.column_config.ProgressColumn(
            "F1-Score", min_value=0, max_value=1
        ),
        "Support": st.column_config.NumberColumn("Support"),
    },
)

st.divider()

# Metrics summary
with st.expander("📖 Metrics Explanation"):
    st.markdown(
        """
    ### Understanding the Metrics

    #### Precision
    - **Definition**: Out of all predictions of phishing, how many were correct?
    - **Formula**: TP / (TP + FP)
    - **Interpretation**: Lower false positives when precision is high
    - **Use Case**: Important when false alarms are costly

    #### Recall (Sensitivity)
    - **Definition**: Out of all actual phishing cases, how many did we catch?
    - **Formula**: TP / (TP + FN)
    - **Interpretation**: Lower missed detections when recall is high
    - **Use Case**: Critical in security applications where missing threats is dangerous

    #### F1-Score
    - **Definition**: Harmonic mean of precision and recall
    - **Formula**: 2 * (Precision * Recall) / (Precision + Recall)
    - **Interpretation**: Balances both metrics, useful when you need a single score
    - **Range**: 0 to 1, where 1 is perfect

    #### Accuracy
    - **Definition**: Percentage of all predictions that were correct
    - **Formula**: (TP + TN) / (TP + TN + FP + FN)
    - **Caution**: Can be misleading with imbalanced datasets

    #### ROC-AUC
    - **Definition**: Area under the Receiver Operating Characteristic curve
    - **Range**: 0 to 1, where 0.5 is random and 1.0 is perfect
    - **Interpretation**: Threshold-independent measure of classification ability

    #### PR-AUC
    - **Definition**: Area under the Precision-Recall curve
    - **Useful for**: Imbalanced datasets where ROC-AUC might be misleading
    - **Interpretation**: Higher is better

    ### Trade-offs

    - **High Precision, Lower Recall**: Conservative predictions, fewer false alarms but miss some threats
    - **Lower Precision, High Recall**: Aggressive predictions, catch more threats but more false alarms
    - **Balanced**: Optimal F1-score provides good balance between the two

    ### Threshold Selection

    The classification threshold determines when a prediction is labeled as "phishing":
    - **Lower threshold** (e.g., 0.3): Higher recall, lower precision - catch more threats but more false positives
    - **Higher threshold** (e.g., 0.7): Higher precision, lower recall - fewer false alarms but miss some threats
    - **Default threshold** (0.5): Balanced approach

    ### Production Recommendations

    For a security application, consider:
    1. **Acceptable false positive rate**: How many legitimate addresses can we risk flagging?
    2. **Critical phishing detection**: How many real threats must we catch?
    3. **Operational load**: How many alerts can operations teams handle?

    The dashboard allows adjusting the threshold to find the optimal balance for your use case.
    """
    )

# Mock mode notice
if metrics.get("inference_mode") == "mock":
    st.warning(
        "⚠️ **MOCK MODE**: These are sample metrics for demonstration. "
        "Connect to a real model for actual performance data.",
        icon="🎭",
    )

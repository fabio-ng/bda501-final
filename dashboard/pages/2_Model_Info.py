import streamlit as st
import requests
import pandas as pd
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configuration
API_URL = os.getenv("API_URL", "http://localhost:8000")

# Page config
st.set_page_config(
    page_title="Model Info - Ethereum Phishing Detection",
    page_icon="🤖",
    layout="wide",
)

st.title("🤖 Model Information")
st.markdown("Detailed information about the GraphSAGE phishing detection model")

st.divider()


@st.cache_data(ttl=120)
def fetch_model_info():
    """Fetch model information from API."""
    try:
        response = requests.get(f"{API_URL}/model/info", timeout=10)
        response.raise_for_status()
        return response.json(), None
    except requests.exceptions.ConnectionError:
        return None, "Unable to connect to API"
    except Exception as e:
        return None, str(e)


# Fetch model info
model_info, error = fetch_model_info()

if error:
    st.error(f"⚠️ Error: {error}")
    st.info("💡 Make sure the backend API is running")

    # Show mock mode data
    st.subheader("📊 Sample Model Configuration (Mock Mode)")
    model_info = {
        "model_id": "graphsage-phishing-v1",
        "version": "1.0.0",
        "type": "GraphSAGE",
        "status": "loaded",
        "inference_mode": "mock",
        "loaded_at": "2024-04-09T10:30:00Z",
        "graph_stats": {
            "num_nodes": 2500000,
            "num_edges": 15000000,
            "feature_dim": 128,
        },
        "configuration": {
            "threshold": 0.5,
            "batch_size": 32,
            "device": "cpu",
            "num_sampler": 10,
            "num_layers": 2,
        },
        "training_info": {
            "dataset": "Ethereum Phishing Dataset v2.0",
            "train_samples": 50000,
            "test_samples": 10000,
            "epochs": 100,
            "optimizer": "Adam",
            "learning_rate": 0.001,
            "loss_function": "BCEWithLogitsLoss",
        },
    }

else:
    st.success("✅ Successfully connected to model API", icon="✓")

# Basic model info
st.subheader("📋 Model Overview")

col1, col2, col3 = st.columns(3)

with col1:
    st.metric(
        "🆔 Model ID",
        model_info.get("model_id", "N/A"),
    )

with col2:
    st.metric(
        "📌 Version",
        f"v{model_info.get('version', '1.0.0')}",
    )

with col3:
    status = model_info.get("status", "unknown")
    status_emoji = "🟢" if status == "loaded" else "🔴"
    st.metric(
        "🟢 Status",
        f"{status_emoji} {status}",
    )

col1, col2, col3 = st.columns(3)

with col1:
    st.metric(
        "🏗️ Architecture",
        model_info.get("type", "GraphSAGE"),
    )

with col2:
    mode = model_info.get("inference_mode", "inference")
    mode_emoji = "🎭" if mode == "mock" else "⚙️"
    st.metric(
        "📡 Inference Mode",
        f"{mode_emoji} {mode}",
    )

with col3:
    st.metric(
        "⏰ Loaded At",
        model_info.get("loaded_at", "N/A"),
    )

st.divider()

# Graph statistics
st.subheader("📊 Graph Statistics")

graph_stats = model_info.get("graph_stats", {})

col1, col2, col3, col4 = st.columns(4)

with col1:
    num_nodes = graph_stats.get("num_nodes", 0)
    st.metric(
        "🔵 Total Nodes",
        f"{num_nodes:,.0f}",
        delta="Ethereum addresses",
    )

with col2:
    num_edges = graph_stats.get("num_edges", 0)
    st.metric(
        "🔗 Total Edges",
        f"{num_edges:,.0f}",
        delta="Transactions",
    )

with col3:
    feature_dim = graph_stats.get("feature_dim", 0)
    st.metric(
        "🧮 Feature Dimension",
        f"{feature_dim}",
        delta="Embedding size",
    )

with col4:
    num_layers = model_info.get("configuration", {}).get("num_layers", 2)
    st.metric(
        "🏢 Graph Layers",
        f"{num_layers}",
        delta="Aggregation hops",
    )

# Visualize graph structure
st.markdown("### Graph Architecture Visualization")

import plotly.graph_objects as go

# Create a simple network visualization
fig = go.Figure()

# Add nodes (sample of graph structure)
layer_sizes = [256, 512, 256]  # Example layer sizes
colors = ["rgb(70, 130, 180)", "rgb(100, 150, 200)", "rgb(130, 180, 220)"]

node_x = []
node_y = []
node_color = []
node_text = []

for layer_idx, layer_size in enumerate(layer_sizes):
    for node_idx in range(min(layer_size, 20)):  # Show max 20 nodes per layer
        x = layer_idx
        y = node_idx / layer_size * 10
        node_x.append(x)
        node_y.append(y)
        node_color.append(colors[layer_idx])
        node_text.append(f"Layer {layer_idx + 1}")

fig.add_trace(
    go.Scatter(
        x=node_x,
        y=node_y,
        mode="markers",
        marker=dict(
            size=12,
            color=node_color,
            opacity=0.8,
            line=dict(width=2, color="white"),
        ),
        text=node_text,
        hovertemplate="<b>%{text}</b><br>Position: (%{x}, %{y})<extra></extra>",
        showlegend=False,
    )
)

fig.update_layout(
    title="GraphSAGE Layer Structure (Simplified)",
    xaxis=dict(
        tickvals=[0, 1, 2],
        ticktext=["Input", "Hidden", "Output"],
        showgrid=False,
        zeroline=False,
    ),
    yaxis=dict(showgrid=False, zeroline=False),
    height=400,
    hovermode="closest",
    plot_bgcolor="rgba(240,240,240,0.5)",
)

st.plotly_chart(fig, use_container_width=True)

st.divider()

# Configuration
st.subheader("⚙️ Model Configuration")

config = model_info.get("configuration", {})

col1, col2, col3, col4 = st.columns(4)

with col1:
    st.metric(
        "🎯 Threshold",
        f"{config.get('threshold', 0.5):.1%}",
        delta="Classification boundary",
    )

with col2:
    st.metric(
        "📦 Batch Size",
        config.get("batch_size", 32),
        delta="Samples per inference",
    )

with col3:
    st.metric(
        "💻 Device",
        config.get("device", "CPU"),
        delta="Computation device",
    )

with col4:
    st.metric(
        "🎯 Sampler Size",
        config.get("num_sampler", 10),
        delta="Neighbors per node",
    )

# Configuration details table
st.markdown("### Configuration Details")

config_data = []
for key, value in config.items():
    config_data.append({"Parameter": key, "Value": value})

if config_data:
    config_df = pd.DataFrame(config_data)
    st.dataframe(
        config_df,
        use_container_width=True,
        hide_index=True,
    )

st.divider()

# Training information
st.subheader("📚 Training Information")

train_info = model_info.get("training_info", {})

col1, col2, col3, col4 = st.columns(4)

with col1:
    st.metric(
        "📊 Training Samples",
        f"{train_info.get('train_samples', 0):,.0f}",
        delta="Addresses used",
    )

with col2:
    st.metric(
        "✅ Test Samples",
        f"{train_info.get('test_samples', 0):,.0f}",
        delta="Evaluation set",
    )

with col3:
    st.metric(
        "🔄 Epochs",
        train_info.get("epochs", 100),
        delta="Training iterations",
    )

with col4:
    st.metric(
        "🎯 Optimizer",
        train_info.get("optimizer", "Adam"),
        delta="Optimization method",
    )

# Training details
st.markdown("### Training Details")

col1, col2 = st.columns(2)

with col1:
    st.info("**Dataset**")
    st.write(f"📂 {train_info.get('dataset', 'N/A')}")

    st.info("**Loss Function**")
    st.write(f"📉 {train_info.get('loss_function', 'N/A')}")

with col2:
    st.info("**Learning Rate**")
    st.write(f"📈 {train_info.get('learning_rate', 0.001)}")

    st.info("**Hardware**")
    st.write(f"💻 {config.get('device', 'CPU')}")

st.divider()

# Version history (if available)
st.subheader("📜 Version History")

versions = model_info.get("version_history", [])

if versions:
    version_df = pd.DataFrame(versions)
    st.dataframe(
        version_df,
        use_container_width=True,
        hide_index=True,
    )
else:
    st.info(
        "📌 No version history available. This is the current model version.",
        icon="ℹ️",
    )

st.divider()

# Technical details (expander)
with st.expander("🔬 Technical Deep Dive"):
    st.markdown(
        f"""
    ### GraphSAGE Architecture

    **Model Type**: {model_info.get('type', 'GraphSAGE')}

    GraphSAGE (Graph Sample and Aggregate) is an inductive graph neural network framework
    that learns node embeddings by:

    1. **Sampling**: Randomly sampling a subset of neighboring nodes
    2. **Aggregation**: Aggregating feature information from neighbors
    3. **Concatenation**: Combining the aggregated information with the node's own features
    4. **Non-linear Transformation**: Applying a learned transformation function

    ### Key Parameters

    - **Number of Layers**: {config.get('num_layers', 2)} - Multiple layers enable learning of hierarchical features
    - **Sampler Size**: {config.get('num_sampler', 10)} - Number of neighbor samples per layer
    - **Batch Size**: {config.get('batch_size', 32)} - Trade-off between memory and speed
    - **Feature Dimension**: {graph_stats.get('feature_dim', 128)} - Dimension of learned embeddings

    ### Graph Structure

    - **Nodes**: {graph_stats.get('num_nodes', 0):,} Ethereum addresses
    - **Edges**: {graph_stats.get('num_edges', 0):,} Transaction relationships
    - **Density**: {(2 * graph_stats.get('num_edges', 0) / (graph_stats.get('num_nodes', 1) * (graph_stats.get('num_nodes', 1) - 1)) * 100) if graph_stats.get('num_nodes', 0) > 0 else 0:.6f}%

    ### Training Details

    - **Dataset**: {train_info.get('dataset', 'N/A')}
    - **Training Samples**: {train_info.get('train_samples', 0):,}
    - **Test Samples**: {train_info.get('test_samples', 0):,}
    - **Epochs**: {train_info.get('epochs', 100)}
    - **Optimizer**: {train_info.get('optimizer', 'Adam')}
    - **Learning Rate**: {train_info.get('learning_rate', 0.001)}
    - **Loss Function**: {train_info.get('loss_function', 'N/A')}

    ### Performance

    The model is evaluated on a held-out test set using metrics like:
    - Precision: True positives / (True positives + False positives)
    - Recall: True positives / (True positives + False negatives)
    - F1-Score: Harmonic mean of precision and recall
    - ROC-AUC: Area under the Receiver Operating Characteristic curve
    """
    )

# Mock mode notice
if model_info.get("inference_mode") == "mock":
    st.warning(
        "⚠️ **MOCK MODE**: This dashboard is showing sample data for demonstration. "
        "Connect to a real model API for actual predictions.",
        icon="🎭",
    )

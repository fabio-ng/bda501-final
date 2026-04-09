import streamlit as st
import requests
import pandas as pd
from datetime import datetime
import os
from dotenv import load_dotenv
import sys

# Load environment variables
load_dotenv()

# Configuration
API_URL = os.getenv("API_URL", "http://localhost:8000")

# Add parent directory to path for importing components
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from components.prediction_card import render_prediction_card

# Page config
st.set_page_config(
    page_title="Address Lookup - Ethereum Phishing Detection",
    page_icon="🔍",
    layout="wide",
)

st.title("🔍 Address Lookup & Classification")
st.markdown(
    "Enter an Ethereum address to check if it's associated with phishing activity"
)

st.divider()


def is_valid_ethereum_address(address: str) -> bool:
    """Validate Ethereum address format."""
    if not address.startswith("0x"):
        return False
    if len(address) != 42:
        return False
    try:
        int(address[2:], 16)
        return True
    except ValueError:
        return False


def fetch_prediction(address: str):
    """Fetch prediction from API."""
    try:
        response = requests.post(
            f"{API_URL}/predict/address",
            json={"address": address},
            timeout=30,
        )
        response.raise_for_status()
        return response.json(), None
    except requests.exceptions.ConnectionError:
        return None, "Unable to connect to API. Make sure the backend is running."
    except requests.exceptions.Timeout:
        return None, "Request timed out. The model might be busy. Please try again."
    except requests.exceptions.HTTPError as e:
        return None, f"API Error: {e.response.status_code} - {e.response.text}"
    except Exception as e:
        return None, f"Error: {str(e)}"


def fetch_address_history(address: str):
    """Fetch prediction history for address."""
    try:
        response = requests.get(
            f"{API_URL}/predictions/history",
            params={"address": address},
            timeout=10,
        )
        response.raise_for_status()
        return response.json(), None
    except Exception as e:
        return None, None


# Address input section
col1, col2 = st.columns([4, 1])

with col1:
    address_input = st.text_input(
        "Enter Ethereum Address",
        placeholder="0x742d35Cc6634C0532925a3b844Bc9e7595f42aD7",
        help="Enter a valid Ethereum address starting with 0x and 40 hex characters",
    )

with col2:
    classify_button = st.button(
        "🔍 Classify Address",
        type="primary",
        use_container_width=True,
    )

st.caption(
    "💡 **Hint**: Enter a 42-character address starting with '0x' followed by 40 hexadecimal characters"
)

st.divider()

# Process classification if button clicked
if classify_button:
    if not address_input.strip():
        st.error("❌ Please enter an address")
    elif not is_valid_ethereum_address(address_input.strip()):
        st.error(
            "❌ Invalid Ethereum address format. Must be 42 characters starting with 0x"
        )
    else:
        address = address_input.strip()

        # Show loading spinner
        with st.spinner("🔄 Analyzing address... This may take a moment"):
            prediction, error = fetch_prediction(address)

        if error:
            st.error(f"⚠️ {error}")
            st.info(
                "💡 **Troubleshooting**: Check that the API is running on "
                + f"{API_URL}",
                icon="ℹ️",
            )
        elif prediction:
            # Extract prediction data
            score = prediction.get("score", 0.0)
            is_phishing = prediction.get("is_phishing", False)
            model_version = prediction.get("model_version", "unknown")
            inference_time = prediction.get("inference_time_ms", 0.0)
            threshold = prediction.get("threshold", 0.5)
            inference_mode = prediction.get("inference_mode", "inference")
            risk_factors = prediction.get("risk_factors", [])
            known_address = prediction.get("known_address")

            # Render prediction card
            render_prediction_card(
                score=score,
                is_phishing=is_phishing,
                model_version=model_version,
                inference_time=inference_time,
                threshold=threshold,
                inference_mode=inference_mode,
                risk_factors=risk_factors,
                known_address=known_address,
            )

            st.divider()

            # Previous predictions for this address
            st.subheader("📋 Prediction History for This Address")

            history, _ = fetch_address_history(address)

            if history and history.get("predictions"):
                predictions = history.get("predictions", [])

                # Convert to DataFrame
                df = pd.DataFrame(
                    [
                        {
                            "Timestamp": p.get("timestamp"),
                            "Score": p.get("score", 0.0),
                            "Classification": (
                                "🚨 Phishing"
                                if p.get("is_phishing")
                                else "✅ Legitimate"
                            ),
                            "Model Version": p.get("model_version"),
                            "Inference Time": f"{p.get('inference_time_ms', 0):.0f}ms",
                        }
                        for p in predictions
                    ]
                )

                st.dataframe(
                    df,
                    use_container_width=True,
                    hide_index=True,
                    column_config={
                        "Timestamp": st.column_config.TextColumn("Timestamp"),
                        "Score": st.column_config.ProgressColumn(
                            "Score",
                            min_value=0,
                            max_value=1,
                        ),
                        "Classification": st.column_config.TextColumn("Result"),
                        "Model Version": st.column_config.TextColumn("Model"),
                        "Inference Time": st.column_config.TextColumn("Latency"),
                    },
                )

                # Score trend chart
                st.subheader("📈 Score Trend Over Time")

                import plotly.graph_objects as go

                scores = [p.get("score", 0.0) for p in predictions]
                timestamps = [p.get("timestamp", "") for p in predictions]

                fig = go.Figure()

                fig.add_trace(
                    go.Scatter(
                        x=timestamps,
                        y=scores,
                        mode="lines+markers",
                        fill="tozeroy",
                        line=dict(color="rgb(70, 130, 180)", width=2),
                        marker=dict(size=8),
                        hovertemplate="<b>%{x}</b><br>Score: %{y:.2%}<extra></extra>",
                    )
                )

                # Add threshold line
                fig.add_hline(
                    y=threshold,
                    line_dash="dash",
                    line_color="orange",
                    annotation_text="Threshold",
                    annotation_position="right",
                )

                fig.update_layout(
                    title="Prediction Score History",
                    xaxis_title="Timestamp",
                    yaxis_title="Score",
                    height=400,
                    template="plotly_white",
                    hovermode="x unified",
                    yaxis=dict(tickformat=".0%"),
                )

                st.plotly_chart(fig, use_container_width=True)

            else:
                st.info(
                    "📊 No previous predictions for this address",
                    icon="ℹ️",
                )

        else:
            st.error("❌ Unexpected error occurred")

st.divider()

# Help section
with st.expander("❓ How does the classification work?"):
    st.markdown(
        """
    ### Classification Process

    The Ethereum Phishing Detection Platform uses GraphSAGE, a Graph Neural Network
    architecture, to classify addresses as either legitimate or phishing:

    #### Key Features:
    - **Graph-based Analysis**: Analyzes transaction patterns and network relationships
    - **Feature Extraction**: Considers various address features (balance, transaction count, etc.)
    - **Neighbor Sampling**: Examines neighboring nodes in the Ethereum transaction graph
    - **Real-time Detection**: Processes addresses instantly for quick classification

    #### Score Interpretation:
    - **Score = 0%**: Definitely legitimate
    - **Score < Threshold (usually 50%)**: Classified as legitimate
    - **Score >= Threshold**: Classified as phishing
    - **Score = 100%**: Definitely phishing

    #### Risk Factors:
    If identified, risk factors appear above and indicate specific patterns that raised suspicion:
    - Unusual transaction patterns
    - High similarity to known phishing addresses
    - Suspicious graph connectivity
    - Other anomalies detected by the model

    ### Model Details:
    - **Architecture**: GraphSAGE with 2 layers
    - **Training Data**: Historical Ethereum transaction data
    - **Features**: 128-dimensional embeddings
    - **Threshold**: Dynamically adjustable (default: 50%)
    """
    )

with st.expander("🔒 Data Privacy & Security"):
    st.markdown(
        """
    ### Privacy Information

    - **Address Analysis Only**: We analyze public blockchain data only
    - **No Personal Data**: No personal or sensitive information is collected
    - **Stateless Processing**: Queries don't affect predictions for other addresses
    - **Transparent**: All model predictions include confidence scores
    - **Public Blockchain**: Data analyzed is from the public Ethereum blockchain

    ### Usage Guidelines:
    - Use results as part of a larger risk assessment strategy
    - Don't rely solely on this tool for critical decisions
    - Report false positives to help improve the model
    - Respect local regulations regarding blockchain analytics
    """
    )

st.divider()

# Example addresses
st.subheader("📝 Example Addresses to Try")

st.markdown(
    """
    Here are some example Ethereum addresses you can test:

    - **Legitimate Address**: `0x1234567890123456789012345678901234567890`
    - **High-Risk Address**: `0xabcdefabcdefabcdefabcdefabcdefabcdefabcd`

    Or enter any valid Ethereum address you want to analyze.
    """
)

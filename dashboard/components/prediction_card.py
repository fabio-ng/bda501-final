import streamlit as st
from typing import Optional


def render_prediction_card(
    score: float,
    is_phishing: bool,
    model_version: str,
    inference_time: float,
    threshold: float,
    inference_mode: str,
    risk_factors: Optional[list] = None,
    known_address: Optional[bool] = None,
) -> None:
    """
    Render a prediction result card with colored score indicator.

    Args:
        score: Prediction score (0-1)
        is_phishing: Whether address is classified as phishing
        model_version: Model version string
        inference_time: Inference time in milliseconds
        threshold: Classification threshold
        inference_mode: Mode of inference (e.g., "mock", "inference")
        risk_factors: List of risk factors (optional)
        known_address: Whether address is in known list (optional)
    """

    # Main prediction banner
    col1, col2 = st.columns([3, 1])

    with col1:
        if is_phishing:
            st.error("🚨 **PHISHING DETECTED**", icon="⚠️")
        else:
            st.success("✅ **LEGITIMATE ADDRESS**", icon="✓")

    # Score display with gauge
    col1, col2, col3 = st.columns(3)

    with col1:
        score_pct = score * 100
        st.metric(
            "Phishing Score",
            f"{score_pct:.1f}%",
            delta=f"Threshold: {threshold * 100:.0f}%",
        )

    with col2:
        threshold_pct = threshold * 100
        st.metric(
            "Threshold",
            f"{threshold_pct:.0f}%",
            delta="Classification boundary" if inference_mode == "inference" else None,
        )

    with col3:
        st.metric(
            "Inference Time",
            f"{inference_time:.0f}ms",
            delta="Response latency" if inference_mode == "inference" else None,
        )

    # Details section
    st.divider()

    detail_cols = st.columns(4)
    with detail_cols[0]:
        st.caption("**Model Version**")
        st.write(f"`{model_version}`")

    with detail_cols[1]:
        st.caption("**Inference Mode**")
        mode_display = "🎭 Mock" if inference_mode == "mock" else "⚙️ Live"
        st.write(mode_display)

    with detail_cols[2]:
        st.caption("**Known Address**")
        if known_address is not None:
            status = "Yes ✓" if known_address else "No"
            st.write(status)
        else:
            st.write("Unknown")

    with detail_cols[3]:
        st.caption("**Confidence**")
        confidence = max(score, 1 - score) * 100
        st.write(f"{confidence:.1f}%")

    # Risk factors
    if risk_factors and len(risk_factors) > 0:
        st.divider()
        st.subheader("🔍 Risk Factors")
        for factor in risk_factors:
            st.info(f"• {factor}", icon="⚠️")

    # Progress bar visualization
    st.divider()

    # Create a visual threshold indicator
    indicator_color = "🔴" if is_phishing else "🟢"
    st.caption(f"{indicator_color} Score Position Relative to Threshold")

    # Use plotly for better visualization
    import plotly.graph_objects as go

    fig = go.Figure()

    # Add threshold line
    fig.add_hline(
        y=threshold,
        line_dash="dash",
        line_color="orange",
        annotation_text="Classification Threshold",
        annotation_position="right",
    )

    # Add score point
    fig.add_scatter(
        x=[0],
        y=[score],
        mode="markers+text",
        marker=dict(
            size=20,
            color="red" if is_phishing else "green",
        ),
        text=[f"{score_pct:.1f}%"],
        textposition="top center",
        showlegend=False,
        hovertemplate="<b>Prediction Score</b><br>%{y:.2%}<extra></extra>",
    )

    # Styling
    fig.update_layout(
        xaxis=dict(visible=False),
        yaxis=dict(range=[0, 1], tickformat=".0%"),
        height=300,
        margin=dict(l=80, r=80, t=40, b=40),
        plot_bgcolor="rgba(240,240,240,0.5)",
        hovermode="y unified",
    )

    st.plotly_chart(fig, use_container_width=True)

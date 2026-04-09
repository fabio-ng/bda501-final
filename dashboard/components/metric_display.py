import streamlit as st
from typing import Optional, Union


def render_metric_card(
    label: str,
    value: Union[str, float, int],
    delta: Optional[str] = None,
    delta_color: str = "normal",
    icon: str = "📊",
    unit: str = "",
    fmt: Optional[str] = None,
) -> None:
    """
    Render a metric value with label and optional delta.

    Args:
        label: Metric label/title
        value: Metric value
        delta: Optional delta text to show change
        delta_color: Color of delta ("normal", "off", "inverse")
        icon: Icon to display
        unit: Unit suffix (e.g., "ms", "%")
        fmt: Format string for numeric values
    """

    # Format value if needed
    if fmt and isinstance(value, (int, float)):
        formatted_value = fmt.format(value)
    elif unit and isinstance(value, (int, float)):
        formatted_value = f"{value}{unit}"
    else:
        formatted_value = str(value)

    # Create container with metric
    metric_col = st.columns(1)[0]

    with metric_col:
        st.metric(
            label=f"{icon} {label}",
            value=formatted_value,
            delta=delta,
            delta_color=delta_color,
        )


def render_metrics_grid(
    metrics: list[dict],
    columns: int = 4,
) -> None:
    """
    Render multiple metrics in a grid layout.

    Args:
        metrics: List of metric dictionaries with keys:
                 - label: str
                 - value: Union[str, float, int]
                 - delta: Optional[str]
                 - icon: Optional[str]
                 - unit: Optional[str]
        columns: Number of columns in grid
    """

    cols = st.columns(columns)

    for idx, metric in enumerate(metrics):
        with cols[idx % columns]:
            label = metric.get("label", "Metric")
            value = metric.get("value", 0)
            delta = metric.get("delta")
            icon = metric.get("icon", "📊")
            unit = metric.get("unit", "")
            fmt = metric.get("fmt")

            # Format value if needed
            if fmt and isinstance(value, (int, float)):
                formatted_value = fmt.format(value)
            elif unit and isinstance(value, (int, float)):
                formatted_value = f"{value}{unit}"
            else:
                formatted_value = str(value)

            st.metric(
                label=f"{icon} {label}",
                value=formatted_value,
                delta=delta,
            )


def render_info_box(
    title: str,
    content: Union[str, dict],
    box_type: str = "info",
    icon: str = "ℹ️",
) -> None:
    """
    Render an information box with title and content.

    Args:
        title: Box title
        content: Content (string or dict with key-value pairs)
        box_type: Type of box ("info", "success", "warning", "error")
        icon: Icon to display
    """

    if box_type == "success":
        st.success(f"{icon} **{title}**", icon="✓")
    elif box_type == "warning":
        st.warning(f"{icon} **{title}**", icon="⚠️")
    elif box_type == "error":
        st.error(f"{icon} **{title}**", icon="❌")
    else:
        st.info(f"{icon} **{title}**", icon="ℹ️")

    if isinstance(content, dict):
        for key, value in content.items():
            st.caption(f"**{key}**: {value}")
    else:
        st.write(content)

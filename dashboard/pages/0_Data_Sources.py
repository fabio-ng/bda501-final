import streamlit as st
from google.cloud import storage, bigquery
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import os
from datetime import datetime
import json
from pathlib import Path

# Page configuration
st.set_page_config(
    page_title="Data Sources - Ethereum Phishing Detection",
    page_icon="📦",
    layout="wide",
)

st.title("📦 Data Sources & Pipeline")
st.markdown("Overview of data pipeline, GCS bucket status, and processed features")

st.divider()


# ============================================================================
# SECTION 1: Environment Configuration
# ============================================================================

GCP_PROJECT = os.getenv('GCP_PROJECT', 'eth-phishing-detection')
GCS_BUCKET_RAW = os.getenv('GCS_BUCKET_RAW', 'eth-phishing-raw')
GCS_BUCKET_PROCESSED = os.getenv('GCS_BUCKET_PROCESSED', 'eth-phishing-processed')
GCS_BUCKET_MODELS = os.getenv('GCS_BUCKET_MODELS', 'eth-phishing-models')

# ============================================================================
# SECTION 2: Initialize GCS Client
# ============================================================================

@st.cache_resource
def init_gcs_client():
    """Initialize GCS client"""
    try:
        client = storage.Client(project=GCP_PROJECT)
        # Test connection
        list(client.list_buckets(max_results=1))
        return client, None
    except Exception as e:
        return None, str(e)

@st.cache_resource
def init_bq_client():
    """Initialize BigQuery client"""
    try:
        client = bigquery.Client(project=GCP_PROJECT)
        return client, None
    except Exception as e:
        return None, str(e)

storage_client, gcs_error = init_gcs_client()
bq_client, bq_error = init_bq_client()

# ============================================================================
# SECTION 3: Data Pipeline Diagram
# ============================================================================

st.subheader("🔄 Data Pipeline Architecture")

pipeline_description = """
```
┌─────────────────────────────────────────────────────────────────┐
│                    DATA SOURCES & PIPELINE                      │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  XBlock-ETH (Kaggle)           BigQuery (Crypto_Ethereum)     │
│       ↓                                ↓                        │
│  CSV Files                    Large-scale Transactions         │
│  (node, edge, labels)         (30-day historical)              │
│       ↓                                ↓                        │
│  GCS Raw Bucket (eth-phishing-raw)                             │
│  ├─ xblock/transactions.parquet                                │
│  ├─ xblock/nodes.parquet                                       │
│  ├─ xblock/edges.parquet                                       │
│  └─ bigquery/transactions.parquet                              │
│       ↓                                                         │
│  Data Cleaning & Validation                                    │
│       ↓                                                         │
│  GCS Processed Bucket (eth-phishing-processed)                 │
│  ├─ features/node_features.npy       (normalized features)    │
│  ├─ features/edge_index.npy          (PyTorch format)         │
│  ├─ features/labels.npy              (phishing labels)        │
│  ├─ features/node_features.parquet   (readable format)        │
│  └─ features/metadata.json           (pipeline metadata)      │
│       ↓                                                         │
│  Model Training (GraphSAGE/GCN)                                │
│       ↓                                                         │
│  GCS Models Bucket (eth-phishing-models)                       │
│  ├─ graphsage_v1/model.pth           (trained weights)        │
│  ├─ graphsage_v1/config.json         (model config)           │
│  └─ graphsage_v1/scaler.pkl          (feature scaler)         │
│       ↓                                                         │
│  Dashboard & Inference API                                     │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```
"""

st.markdown(pipeline_description)

# ============================================================================
# SECTION 4: GCS Bucket Status
# ============================================================================

st.subheader("💾 GCS Bucket Status")

col1, col2, col3 = st.columns(3)

@st.cache_data(ttl=300)
def get_bucket_stats(bucket_name):
    """Get bucket statistics"""
    if storage_client is None:
        return None

    try:
        bucket = storage_client.bucket(bucket_name)
        blobs = list(bucket.list_blobs())

        total_size = sum(blob.size for blob in blobs)
        last_modified = max((blob.updated for blob in blobs), default=None)

        return {
            'objects': len(blobs),
            'size_mb': total_size / (1024**2),
            'size_gb': total_size / (1024**3),
            'last_modified': last_modified
        }
    except Exception as e:
        return None

# Raw bucket
with col1:
    stats = get_bucket_stats(GCS_BUCKET_RAW)
    if stats:
        st.metric(
            "📋 Raw Bucket",
            f"{stats['objects']} objects",
            f"{stats['size_gb']:.2f} GB"
        )
        if stats['last_modified']:
            st.caption(f"Updated: {stats['last_modified'].strftime('%Y-%m-%d %H:%M')}")
    else:
        st.warning("⚠ Not connected")
        with st.expander("Setup Instructions"):
            st.code(
                "export GCS_BUCKET_RAW=eth-phishing-raw\n"
                "export GOOGLE_APPLICATION_CREDENTIALS=/path/to/key.json\n"
                "gcloud auth application-default login",
                language="bash"
            )

# Processed bucket
with col2:
    stats = get_bucket_stats(GCS_BUCKET_PROCESSED)
    if stats:
        st.metric(
            "⚙️ Processed Bucket",
            f"{stats['objects']} objects",
            f"{stats['size_gb']:.2f} GB"
        )
        if stats['last_modified']:
            st.caption(f"Updated: {stats['last_modified'].strftime('%Y-%m-%d %H:%M')}")
    else:
        st.warning("⚠ Not connected")

# Models bucket
with col3:
    stats = get_bucket_stats(GCS_BUCKET_MODELS)
    if stats:
        st.metric(
            "🤖 Models Bucket",
            f"{stats['objects']} objects",
            f"{stats['size_gb']:.2f} GB"
        )
        if stats['last_modified']:
            st.caption(f"Updated: {stats['last_modified'].strftime('%Y-%m-%d %H:%M')}")
    else:
        st.info("ℹ No models trained yet")

# ============================================================================
# SECTION 5: Raw Data Statistics
# ============================================================================

st.subheader("📊 Raw Data Statistics")

@st.cache_data(ttl=300)
def get_xblock_stats():
    """Get XBlock data statistics"""
    if storage_client is None:
        return None

    try:
        bucket = storage_client.bucket(GCS_BUCKET_RAW)

        # Check if files exist
        nodes_blob = bucket.blob('xblock/nodes.parquet')
        edges_blob = bucket.blob('xblock/edges.parquet')

        stats = {}

        if nodes_blob.exists():
            # Estimate from file size (rough approximation)
            size_mb = nodes_blob.size / (1024**2)
            stats['nodes'] = f"~{int(size_mb * 100)} addresses (est.)"

        if edges_blob.exists():
            size_mb = edges_blob.size / (1024**2)
            stats['edges'] = f"~{int(size_mb * 50)} transactions (est.)"

        return stats if stats else None
    except Exception as e:
        return None

col_xblock, col_bq = st.columns(2)

with col_xblock:
    st.markdown("#### XBlock-ETH Data")
    stats = get_xblock_stats()
    if stats:
        st.success("✓ Data available in GCS")
        for key, val in stats.items():
            st.metric(key.capitalize(), val)
    else:
        st.info("Run: `python etl/jobs/ingest_xblock.py` to load XBlock data")

with col_bq:
    st.markdown("#### BigQuery Data")
    if bq_client is not None:
        st.success("✓ BigQuery connected")
        st.metric("Project", GCP_PROJECT)
        st.caption("Table: bigquery-public-data.crypto_ethereum.transactions")
    else:
        st.warning(f"⚠ BigQuery error: {bq_error}")

# ============================================================================
# SECTION 6: Processed Features Status
# ============================================================================

st.subheader("🎯 Processed Features Status")

@st.cache_data(ttl=300)
def get_features_metadata():
    """Load features metadata"""
    if storage_client is None:
        return None

    try:
        bucket = storage_client.bucket(GCS_BUCKET_PROCESSED)
        blob = bucket.blob('features/metadata.json')

        if blob.exists():
            data = blob.download_as_text()
            return json.loads(data)
    except Exception:
        pass

    return None

metadata = get_features_metadata()

if metadata:
    st.success("✓ Features computed and available")

    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Total Nodes", metadata.get('num_nodes', 'N/A'))
    with col2:
        st.metric("Feature Dimension", metadata.get('num_features', 'N/A'))
    with col3:
        st.metric("Graph Edges", metadata.get('num_edges', 'N/A'))

    # Label distribution
    label_dist = metadata.get('label_distribution', {})
    if label_dist:
        st.markdown("**Label Distribution:**")
        cols = st.columns(3)
        with cols[0]:
            st.metric("Phishing", label_dist.get('phishing', 0))
        with cols[1]:
            st.metric("Legitimate", label_dist.get('legitimate', 0))
        with cols[2]:
            st.metric("Unknown", label_dist.get('unknown', 0))

        # Pie chart
        labels_list = ['Phishing', 'Legitimate', 'Unknown']
        values_list = [
            label_dist.get('phishing', 0),
            label_dist.get('legitimate', 0),
            label_dist.get('unknown', 0)
        ]

        if sum(values_list) > 0:
            fig = go.Figure(data=[go.Pie(
                labels=labels_list,
                values=values_list,
                marker=dict(colors=['#e74c3c', '#2ecc71', '#95a5a6']),
                textposition='inside',
                textinfo='percent+label'
            )])
            fig.update_layout(height=400, showlegend=True)
            st.plotly_chart(fig, use_container_width=True)

    st.caption(f"Generated: {metadata.get('timestamp', 'unknown')}")

else:
    st.info("Features not yet computed")
    st.markdown("""
    **Next Steps:**
    1. Run notebook: `notebooks/01_data_exploration.ipynb`
    2. Run notebook: `notebooks/02_feature_engineering.ipynb`
    3. Or run ETL script: `python etl/jobs/process_features.py`
    """)

# ============================================================================
# SECTION 7: BigQuery Connection Test
# ============================================================================

st.subheader("🔗 BigQuery Connection Test")

col1, col2 = st.columns(2)

with col1:
    if st.button("Test BigQuery Connection", use_container_width=True):
        if bq_client is not None:
            try:
                # Run a simple dry run query
                query = """
                SELECT COUNT(*) as transaction_count,
                       MIN(block_timestamp) as earliest_block,
                       MAX(block_timestamp) as latest_block
                FROM `bigquery-public-data.crypto_ethereum.transactions`
                LIMIT 1
                """

                job_config = bigquery.QueryJobConfig(dry_run=True, use_query_cache=False)
                query_job = bq_client.query(query, job_config=job_config)

                bytes_scanned = query_job.total_bytes_processed
                cost_usd = (bytes_scanned / (10**9)) * 6.25

                st.success("✓ BigQuery connection successful")
                st.metric("Bytes to scan", f"{bytes_scanned / (10**9):.2f} GB")
                st.metric("Estimated cost", f"${cost_usd:.2f}")

            except Exception as e:
                st.error(f"Connection failed: {e}")
        else:
            st.error(f"BigQuery not initialized: {bq_error}")

with col2:
    if st.button("View BigQuery Quota", use_container_width=True):
        if bq_client is not None:
            try:
                project = bq_client.project
                st.info(f"Project: {project}")
                st.caption("Quota information available in BigQuery console")
            except Exception as e:
                st.error(f"Error: {e}")

# ============================================================================
# SECTION 8: Pipeline Run History
# ============================================================================

st.subheader("📝 Pipeline Run History")

@st.cache_data(ttl=300)
def get_job_history():
    """Get pipeline job history"""
    if storage_client is None:
        return None

    try:
        bucket = storage_client.bucket(GCS_BUCKET_RAW)
        blob = bucket.blob('_job_log.json')

        if blob.exists():
            data = blob.download_as_text()
            return json.loads(data)
    except Exception:
        pass

    return None

history = get_job_history()

if history and isinstance(history, list) and len(history) > 0:
    df_history = pd.DataFrame(history[-20:])  # Last 20 runs

    # Format timestamp column
    if 'timestamp' in df_history.columns:
        df_history['timestamp'] = pd.to_datetime(df_history['timestamp']).dt.strftime('%Y-%m-%d %H:%M:%S')

    st.dataframe(df_history, use_container_width=True, hide_index=True)
else:
    st.info("No pipeline runs recorded yet. Run the ETL pipeline to populate history.")

# ============================================================================
# SECTION 9: Quick Actions
# ============================================================================

st.subheader("⚡ Quick Actions")

col1, col2, col3 = st.columns(3)

with col1:
    if st.button("📥 Download Sample Data", use_container_width=True):
        sample_file = Path("../data/sample/transactions_sample.csv")
        if sample_file.exists():
            with open(sample_file, "rb") as f:
                st.download_button(
                    label="Download CSV",
                    data=f.read(),
                    file_name="transactions_sample.csv",
                    mime="text/csv"
                )
        else:
            st.warning("Sample data not found")

with col2:
    if st.button("📋 View ETL Logs", use_container_width=True):
        st.markdown("**Recent ETL Activity:**")
        st.caption("Logs would be displayed here if available")

with col3:
    if st.button("🔄 Refresh Statistics", use_container_width=True):
        st.cache_data.clear()
        st.rerun()

# ============================================================================
# SECTION 10: Setup Guide
# ============================================================================

st.divider()

with st.expander("🛠️ Setup Guide - Connect Real Data", expanded=False):
    st.markdown("""
    ### Prerequisites
    - GCP account with BigQuery and Cloud Storage access
    - Kaggle account (for XBlock-ETH dataset)
    - Service account key for authentication

    ### Step 1: Configure Environment Variables
    ```bash
    export GCP_PROJECT=your-gcp-project
    export GCS_BUCKET_RAW=eth-phishing-raw
    export GCS_BUCKET_PROCESSED=eth-phishing-processed
    export GCS_BUCKET_MODELS=eth-phishing-models
    export GOOGLE_APPLICATION_CREDENTIALS=/path/to/service-account-key.json
    export KAGGLE_USERNAME=your-kaggle-username
    export KAGGLE_KEY=your-kaggle-api-key
    ```

    ### Step 2: Create GCS Buckets
    ```bash
    gsutil mb gs://eth-phishing-raw
    gsutil mb gs://eth-phishing-processed
    gsutil mb gs://eth-phishing-models
    ```

    ### Step 3: Authenticate GCP
    ```bash
    gcloud auth application-default login
    gcloud config set project your-gcp-project
    ```

    ### Step 4: Ingest XBlock-ETH Dataset
    ```bash
    python etl/jobs/ingest_xblock.py \\
        --data-dir /path/to/xblock/data \\
        --bucket eth-phishing-raw
    ```

    ### Step 5: Ingest BigQuery Data
    ```bash
    python etl/jobs/ingest_bigquery.py \\
        --start-date 2024-01-01 \\
        --end-date 2024-01-31 \\
        --project your-gcp-project
    ```

    ### Step 6: Process Features
    ```bash
    python etl/jobs/process_features.py \\
        --input-bucket eth-phishing-raw \\
        --output-bucket eth-phishing-processed
    ```

    ### Step 7: Verify Setup
    Visit this page again and check:
    - ✓ GCS Bucket Status shows all buckets
    - ✓ Raw Data Statistics show XBlock and BigQuery data
    - ✓ Processed Features Status shows computed features
    - ✓ BigQuery Connection Test succeeds
    """)

# ============================================================================
# FOOTER
# ============================================================================

st.divider()

st.markdown("""
<div style='text-align: center; color: #888; font-size: 0.9em;'>
    <p>Ethereum Phishing Detection Platform v1.0</p>
    <p>Data pipeline last updated: 2024-04-09</p>
</div>
""", unsafe_allow_html=True)

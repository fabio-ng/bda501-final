"""
Phase 6.8: Airflow DAG for Periodic Model Retraining

Schedule: Weekly (every Sunday at 2:00 AM UTC)

Pipeline:
    1. Export new analyst-confirmed labels from Cloud SQL
    2. Upload updated labels to GCS
    3. Trigger Kaggle notebook re-run via Kaggle API
    4. Wait for Kaggle notebook completion
    5. Download trained model from Kaggle output
    6. Upload new model to GCS
    7. Evaluate new model vs. current model
    8. Promote new model if F1 improves
    9. Notify on completion/failure
"""

from datetime import datetime, timedelta
import os
import json
import logging

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import (
    LocalFilesystemToGCSOperator,
)

logger = logging.getLogger(__name__)

# ──────────────────────────────────────────────
# Configuration
# ──────────────────────────────────────────────

GCS_BUCKET = os.getenv("GCS_BUCKET", "eth-phishing-data")
KAGGLE_NOTEBOOK = "your-username/gnn-ethereum-phishing-detection"
DB_CONN_ID = "eth_phishing_db"
GCS_CONN_ID = "google_cloud_default"
MIN_F1_IMPROVEMENT = 0.005  # promote only if F1 improves by at least 0.5%


# ──────────────────────────────────────────────
# DAG definition
# ──────────────────────────────────────────────

default_args = {
    "owner": "ml-pipeline",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}

dag = DAG(
    dag_id="eth_phishing_retrain",
    default_args=default_args,
    description="Weekly GNN model retraining pipeline",
    schedule_interval="0 2 * * 0",  # Every Sunday at 2:00 AM UTC
    start_date=datetime(2026, 4, 1),
    catchup=False,
    tags=["ml", "ethereum", "phishing"],
)


# ──────────────────────────────────────────────
# Task functions
# ──────────────────────────────────────────────

def export_new_labels(**context):
    """
    Task 1: Export analyst-confirmed labels from Cloud SQL.

    Extracts reviewed alerts where analysts confirmed or rejected
    phishing predictions. These feed into the next training cycle.
    """
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    hook = PostgresHook(postgres_conn_id=DB_CONN_ID)
    conn = hook.get_conn()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT address, analyst_label AS label, 'analyst_feedback' AS source
        FROM phishing_alerts
        WHERE reviewed = TRUE
        AND analyst_label IS NOT NULL
    """)
    rows = cursor.fetchall()

    output_path = "/tmp/new_labels.csv"
    with open(output_path, "w") as f:
        f.write("address,label,source\n")
        for row in rows:
            f.write(f"{row[0]},{row[1]},{row[2]}\n")

    logger.info(f"Exported {len(rows)} analyst labels to {output_path}")
    context["ti"].xcom_push(key="new_labels_count", value=len(rows))
    context["ti"].xcom_push(key="labels_path", value=output_path)
    cursor.close()
    conn.close()


def trigger_kaggle_retrain(**context):
    """
    Task 3: Trigger Kaggle notebook re-run via API.

    Uses the Kaggle API to push new labels and start a new
    notebook execution with GPU enabled.
    """
    from kaggle.api.kaggle_api_extended import KaggleApi

    api = KaggleApi()
    api.authenticate()

    # Push the notebook for execution
    api.kernels_push(KAGGLE_NOTEBOOK)
    logger.info(f"Triggered Kaggle notebook: {KAGGLE_NOTEBOOK}")


def wait_for_kaggle(**context):
    """
    Task 4: Poll Kaggle API until notebook execution completes.
    """
    import time
    from kaggle.api.kaggle_api_extended import KaggleApi

    api = KaggleApi()
    api.authenticate()

    max_wait = 3600 * 4  # 4 hours max
    poll_interval = 120  # check every 2 minutes
    elapsed = 0

    while elapsed < max_wait:
        status = api.kernels_status(KAGGLE_NOTEBOOK)
        logger.info(f"Kaggle notebook status: {status}")

        if status == "complete":
            logger.info("Kaggle notebook completed successfully.")
            return
        elif status in ("error", "cancelAcknowledged"):
            raise Exception(f"Kaggle notebook failed with status: {status}")

        time.sleep(poll_interval)
        elapsed += poll_interval

    raise Exception("Kaggle notebook timed out after 4 hours.")


def download_kaggle_model(**context):
    """
    Task 5: Download trained model from Kaggle output.
    """
    from kaggle.api.kaggle_api_extended import KaggleApi

    api = KaggleApi()
    api.authenticate()

    output_dir = "/tmp/kaggle_model"
    os.makedirs(output_dir, exist_ok=True)

    api.kernels_output(KAGGLE_NOTEBOOK, path=output_dir)
    logger.info(f"Downloaded Kaggle output to {output_dir}")

    # Verify model file exists
    model_path = os.path.join(output_dir, "graphsage_phishing.pt")
    if not os.path.exists(model_path):
        raise FileNotFoundError(f"Model file not found: {model_path}")

    context["ti"].xcom_push(key="model_dir", value=output_dir)


def evaluate_and_decide(**context):
    """
    Task 7: Compare new model F1 vs. current active model.

    Returns the task ID to branch to:
    - 'promote_model' if new model is better
    - 'skip_promotion' if not
    """
    import torch
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    model_dir = context["ti"].xcom_pull(key="model_dir")
    model_path = os.path.join(model_dir, "graphsage_phishing.pt")

    # Load new model metrics
    checkpoint = torch.load(model_path, map_location="cpu", weights_only=False)
    new_f1 = checkpoint["metrics"]["test_f1"]

    # Get current active model F1
    hook = PostgresHook(postgres_conn_id=DB_CONN_ID)
    conn = hook.get_conn()
    cursor = conn.cursor()
    cursor.execute(
        "SELECT test_f1 FROM model_registry WHERE is_active = TRUE LIMIT 1"
    )
    row = cursor.fetchone()
    current_f1 = row[0] if row else 0.0
    cursor.close()
    conn.close()

    logger.info(f"Current F1: {current_f1:.4f}, New F1: {new_f1:.4f}")

    if new_f1 > current_f1 + MIN_F1_IMPROVEMENT:
        logger.info(f"New model is better by {new_f1 - current_f1:.4f}. Promoting.")
        context["ti"].xcom_push(key="new_f1", value=new_f1)
        return "promote_model"
    else:
        logger.info("New model is not significantly better. Skipping promotion.")
        return "skip_promotion"


def promote_model(**context):
    """
    Task 8: Promote new model as the active model.

    - Deactivate old model in registry
    - Register new model
    - Update GCS model path
    """
    import torch
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    model_dir = context["ti"].xcom_pull(key="model_dir")
    new_f1 = context["ti"].xcom_pull(key="new_f1")
    model_version = f"graphsage_v{datetime.now().strftime('%Y%m%d_%H%M')}"

    model_path = os.path.join(model_dir, "graphsage_phishing.pt")
    checkpoint = torch.load(model_path, map_location="cpu", weights_only=False)
    metrics = checkpoint["metrics"]

    # Update database
    hook = PostgresHook(postgres_conn_id=DB_CONN_ID)
    conn = hook.get_conn()
    cursor = conn.cursor()

    # Deactivate current model
    cursor.execute("UPDATE model_registry SET is_active = FALSE WHERE is_active = TRUE")

    # Register new model
    cursor.execute(
        """
        INSERT INTO model_registry (version, gcs_path, test_f1, test_auc_roc, test_auc_pr, is_active, trained_at)
        VALUES (%s, %s, %s, %s, %s, TRUE, NOW())
        """,
        (
            model_version,
            f"gs://{GCS_BUCKET}/models/{model_version}/graphsage_phishing.pt",
            metrics["test_f1"],
            metrics["test_auc_roc"],
            metrics["test_auc_pr"],
        ),
    )

    conn.commit()
    cursor.close()
    conn.close()

    logger.info(f"Promoted model {model_version} (F1={new_f1:.4f})")
    context["ti"].xcom_push(key="model_version", value=model_version)


def skip_promotion(**context):
    """Task: Log that promotion was skipped."""
    logger.info("Model promotion skipped — current model is still best.")


# ──────────────────────────────────────────────
# DAG tasks
# ──────────────────────────────────────────────

t1_export_labels = PythonOperator(
    task_id="export_new_labels",
    python_callable=export_new_labels,
    dag=dag,
)

t2_upload_labels = BashOperator(
    task_id="upload_labels_to_gcs",
    bash_command=f"gsutil cp /tmp/new_labels.csv gs://{GCS_BUCKET}/labels/feedback/",
    dag=dag,
)

t3_trigger_kaggle = PythonOperator(
    task_id="trigger_kaggle_retrain",
    python_callable=trigger_kaggle_retrain,
    dag=dag,
)

t4_wait_kaggle = PythonOperator(
    task_id="wait_for_kaggle",
    python_callable=wait_for_kaggle,
    dag=dag,
)

t5_download_model = PythonOperator(
    task_id="download_kaggle_model",
    python_callable=download_kaggle_model,
    dag=dag,
)

t6_upload_model = BashOperator(
    task_id="upload_model_to_gcs",
    bash_command=(
        "MODEL_VERSION=graphsage_v$(date +%Y%m%d_%H%M) && "
        f"gsutil -m cp /tmp/kaggle_model/* gs://{GCS_BUCKET}/models/$MODEL_VERSION/"
    ),
    dag=dag,
)

t7_evaluate = BranchPythonOperator(
    task_id="evaluate_and_decide",
    python_callable=evaluate_and_decide,
    dag=dag,
)

t8_promote = PythonOperator(
    task_id="promote_model",
    python_callable=promote_model,
    dag=dag,
)

t8_skip = PythonOperator(
    task_id="skip_promotion",
    python_callable=skip_promotion,
    dag=dag,
)

# ──────────────────────────────────────────────
# Task dependencies
# ──────────────────────────────────────────────
# export labels → upload → trigger kaggle → wait → download → upload model → evaluate → promote/skip

t1_export_labels >> t2_upload_labels >> t3_trigger_kaggle >> t4_wait_kaggle
t4_wait_kaggle >> t5_download_model >> t6_upload_model >> t7_evaluate
t7_evaluate >> [t8_promote, t8_skip]

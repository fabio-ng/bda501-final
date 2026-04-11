"""Airflow DAG — ETH Daily Pipeline.

Schedule: 00:05 UTC daily
Flow:    validate → snapshot → check_snapshot → edges → check_edges → cleanup

Triggers Spark Job 1 (daily snapshot) and Job 2 (incremental edges) with
pre-validation and post-execution data quality checks.
"""

import os
import logging
from datetime import datetime, timedelta

import psycopg2
import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from google.cloud import storage

logger = logging.getLogger(__name__)

# ── Config ────────────────────────────────────
GCS_BUCKET = os.environ.get("GCS_BUCKET", "eth-bigdata-project")
SLACK_WEBHOOK = os.environ.get("SLACK_WEBHOOK_URL", "")
SPARK_SUBMIT = "spark-submit --master spark://spark-master:7077"


# ── Callbacks ─────────────────────────────────
def slack_alert(context):
    """Post failure alert to Slack."""
    if not SLACK_WEBHOOK:
        logger.warning("SLACK_WEBHOOK_URL not set — skipping alert")
        return

    task = context.get("task_instance")
    dag_id = context.get("dag").dag_id
    exec_date = context.get("execution_date")
    exception = context.get("exception", "unknown")

    text = (
        f":red_circle: *Airflow Task Failed*\n"
        f"DAG: `{dag_id}`\n"
        f"Task: `{task.task_id}`\n"
        f"Execution: `{exec_date}`\n"
        f"Error: ```{exception}```"
    )
    try:
        requests.post(SLACK_WEBHOOK, json={"text": text}, timeout=10)
    except Exception as e:
        logger.error("Failed to send Slack alert: %s", e)


# ── Task functions ────────────────────────────
def validate_partition(**context):
    """Pre-Spark validation: check raw partition exists and has expected row count."""
    # AIRFLOW_HOME/plugins is on sys.path; import top-level module `validators`, not `plugins.validators`.
    from validators import validate_raw_partition

    target_date = context["ds"]
    result = validate_raw_partition(GCS_BUCKET, target_date)
    logger.info("Validation result: %s", result)

    if not result["passed"]:
        raise ValueError(f"Validation failed for {target_date}: {result['message']}")

    return result


def check_snapshot_quality(**context):
    """Post-Job 1: verify snapshot has exactly 100 rows."""
    target_date = context["ds"]
    conn = _pg_connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) FROM wallet_daily_snapshot WHERE snapshot_date = %s",
                (target_date,),
            )
            count = cur.fetchone()[0]
    finally:
        conn.close()

    if count != 100:
        msg = f"Snapshot for {target_date} has {count} rows (expected 100)"
        logger.warning(msg)
        # Alert but don't fail the DAG
        if SLACK_WEBHOOK:
            requests.post(
                SLACK_WEBHOOK,
                json={"text": f":warning: {msg}"},
                timeout=10,
            )
    else:
        logger.info("Snapshot quality check passed: %d rows for %s", count, target_date)


def check_edge_quality(**context):
    """Post-Job 2: verify edge row count delta < 20% from previous run."""
    conn = _pg_connect()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM wallet_graph_edge")
            current = cur.fetchone()[0]
    finally:
        conn.close()

    # Compare with previous count stored in XCom (if available)
    ti = context["task_instance"]
    prev_count = ti.xcom_pull(task_ids="check_edge_quality", key="edge_count") or current

    if prev_count > 0:
        delta_pct = abs(current - prev_count) / prev_count * 100
        if delta_pct > 20:
            msg = (
                f"Edge count changed by {delta_pct:.1f}%: "
                f"{prev_count} → {current} (threshold: 20%)"
            )
            logger.warning(msg)
            if SLACK_WEBHOOK:
                requests.post(
                    SLACK_WEBHOOK,
                    json={"text": f":warning: {msg}"},
                    timeout=10,
                )

    # Store current count for next run comparison
    ti.xcom_push(key="edge_count", value=current)
    logger.info("Edge quality check: %d edges", current)


def cleanup_old_partitions(**context):
    """Delete graph_edges run_date partitions older than 30 days."""
    client = storage.Client()
    bucket = client.bucket(GCS_BUCKET)
    prefix = "processed/graph_edges/"
    cutoff = datetime.strptime(context["ds"], "%Y-%m-%d").date() - timedelta(days=30)

    deleted = 0
    blobs = bucket.list_blobs(prefix=prefix, delimiter="/")
    # Iterate through prefixes (run_date=YYYY-MM-DD/)
    for page in blobs.pages:
        for blob_prefix in page.prefixes:
            # Extract date from run_date=YYYY-MM-DD/
            try:
                date_part = blob_prefix.split("run_date=")[1].rstrip("/")
                partition_date = datetime.strptime(date_part, "%Y-%m-%d").date()
                if partition_date < cutoff:
                    # Delete all blobs in this partition
                    old_blobs = list(bucket.list_blobs(prefix=blob_prefix))
                    for blob in old_blobs:
                        blob.delete()
                    deleted += 1
            except (IndexError, ValueError):
                continue

    logger.info("Cleaned up %d old edge partitions (before %s)", deleted, cutoff)


def _pg_connect():
    return psycopg2.connect(
        host=os.environ.get("POSTGRES_HOST", "postgres"),
        port=os.environ.get("POSTGRES_PORT", "5432"),
        dbname=os.environ.get("POSTGRES_DB", "ethdb"),
        user=os.environ.get("POSTGRES_USER", "ethuser"),
        password=os.environ.get("POSTGRES_PASSWORD", "ethpass"),
    )


# ── DAG Definition ────────────────────────────
default_args = {
    "owner": "eth-analytics",
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "on_failure_callback": slack_alert,
}

with DAG(
    dag_id="eth_daily_pipeline",
    default_args=default_args,
    description="Daily ETH top-100 snapshot + incremental edge aggregation",
    schedule_interval="5 0 * * *",  # 00:05 UTC
    start_date=datetime(2024, 10, 1),
    catchup=False,
    max_active_runs=1,
    sla_miss_callback=slack_alert,
    tags=["eth", "daily"],
) as dag:

    validate = PythonOperator(
        task_id="validate_raw_partition",
        python_callable=validate_partition,
        sla=timedelta(hours=1),
    )

    snapshot = BashOperator(
        task_id="spark_daily_snapshot",
        bash_command=f"{SPARK_SUBMIT} /app/daily_snapshot.py --target-date {{{{ ds }}}}",
        sla=timedelta(hours=1),
    )

    check_snapshot = PythonOperator(
        task_id="check_snapshot_quality",
        python_callable=check_snapshot_quality,
    )

    edges = BashOperator(
        task_id="spark_incremental_edges",
        bash_command=f"{SPARK_SUBMIT} /app/incremental_edges.py --target-date {{{{ ds }}}}",
        sla=timedelta(hours=1),
    )

    check_edges = PythonOperator(
        task_id="check_edge_quality",
        python_callable=check_edge_quality,
    )

    cleanup = PythonOperator(
        task_id="cleanup_old_partitions",
        python_callable=cleanup_old_partitions,
    )

    validate >> snapshot >> check_snapshot >> edges >> check_edges >> cleanup

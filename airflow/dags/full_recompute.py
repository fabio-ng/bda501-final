"""Airflow DAG — Full Edge Recompute (manual trigger only).

Used for:
  - Initial bootstrap after BigQuery export
  - Disaster recovery if incremental state is corrupted
  - Correctness validation against incremental results
"""

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.models.param import Param

SPARK_SUBMIT = "spark-submit --master spark://spark-master:7077"

default_args = {
    "owner": "eth-analytics",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}

with DAG(
    dag_id="eth_full_recompute",
    default_args=default_args,
    description="Full 180-day edge recompute from raw data (manual trigger)",
    schedule_interval=None,  # manual only
    start_date=datetime(2024, 10, 1),
    catchup=False,
    params={
        "end_date": Param(
            default=str(datetime.utcnow().date() - timedelta(days=1)),
            type="string",
            description="End date of the 180-day window (YYYY-MM-DD)",
        ),
    },
    tags=["eth", "recovery"],
) as dag:

    recompute = BashOperator(
        task_id="spark_full_recompute_edges",
        bash_command=(
            f"{SPARK_SUBMIT} /app/full_recompute_edges.py "
            "--end-date {{ params.end_date }}"
        ),
    )

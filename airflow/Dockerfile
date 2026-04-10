FROM apache/airflow:2.8.4-python3.11

USER root
RUN apt-get update && \
    apt-get install -y --no-install-recommends curl && \
    rm -rf /var/lib/apt/lists/*

USER airflow

ARG AIRFLOW_CONSTRAINTS_URL="https://raw.githubusercontent.com/apache/airflow/constraints-2.8.4/constraints-3.11.txt"

RUN pip install --no-cache-dir \
    --constraint "${AIRFLOW_CONSTRAINTS_URL}" \
    apache-airflow-providers-google \
    apache-airflow-providers-apache-spark \
    google-cloud-storage \
    psycopg2-binary \
    requests

COPY dags/ /opt/airflow/dags/
COPY plugins/ /opt/airflow/plugins/

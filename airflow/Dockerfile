FROM apache/airflow:2.8-python3.11

USER root
RUN apt-get update && \
    apt-get install -y --no-install-recommends curl && \
    rm -rf /var/lib/apt/lists/*

USER airflow

RUN pip install --no-cache-dir \
    apache-airflow-providers-google \
    apache-airflow-providers-apache-spark \
    google-cloud-storage \
    psycopg2-binary \
    requests

COPY dags/ /opt/airflow/dags/
COPY plugins/ /opt/airflow/plugins/

FROM bitnami/spark:3.5

USER root

# GCS connector + PostgreSQL JDBC driver
RUN curl -sL -o /opt/bitnami/spark/jars/gcs-connector-hadoop3-latest.jar \
        "https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop3-latest.jar" && \
    curl -sL -o /opt/bitnami/spark/jars/postgresql-42.7.1.jar \
        "https://jdbc.postgresql.org/download/postgresql-42.7.1.jar"

# Python dependencies
COPY requirements.txt /app/
RUN pip install --no-cache-dir -r /app/requirements.txt

# Application code
COPY . /app/

WORKDIR /app

USER 1001

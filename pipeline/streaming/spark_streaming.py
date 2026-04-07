"""
Phase 4.7–4.11: Spark Structured Streaming Pipeline

Consumes new transactions from Kafka, enriches with features,
runs GNN inference, and publishes alerts.

Usage:
    spark-submit \
        --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
        spark_streaming.py

Requires:
    - Kafka running with 'new_transactions' topic
    - Trained model available at MODEL_LOCAL_PATH
    - Graph data (node_features.npy, edge_index.npy, node_to_id.pkl)
"""

import json
import logging
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    LongType, IntegerType,
)

import sys
sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
from config import (
    KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC_NEW_TX, KAFKA_TOPIC_ALERTS,
    MODEL_LOCAL_PATH, PHISHING_THRESHOLD,
    DB_JDBC_URL, DB_USER, DB_PASSWORD,
    GCS_CHECKPOINTS_PATH,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ──────────────────────────────────────────────
# Transaction Schema (matches Kafka producer output)
# ──────────────────────────────────────────────

TX_SCHEMA = StructType([
    StructField("tx_hash", StringType(), True),
    StructField("from_address", StringType(), True),
    StructField("to_address", StringType(), True),
    StructField("value", DoubleType(), True),
    StructField("gas", LongType(), True),
    StructField("gas_price", LongType(), True),
    StructField("block_number", LongType(), True),
    StructField("timestamp", LongType(), True),
])


# ──────────────────────────────────────────────
# Inference module (loaded once per executor)
# ──────────────────────────────────────────────

# Lazy-loaded on each executor
_inference_service = None

def get_inference_service():
    """Singleton inference service per Spark executor."""
    global _inference_service
    if _inference_service is None:
        from inference.inference_service import PhishingInferenceService
        data_dir = os.getenv("GRAPH_DATA_DIR", "/tmp/graph_data")
        _inference_service = PhishingInferenceService(MODEL_LOCAL_PATH, data_dir)
    return _inference_service


# ──────────────────────────────────────────────
# Streaming Pipeline
# ──────────────────────────────────────────────

def create_spark_session() -> SparkSession:
    """Create Spark session with Kafka support."""
    return (
        SparkSession.builder
        .appName("EthPhishingStreamingInference")
        .config("spark.sql.streaming.checkpointLocation", GCS_CHECKPOINTS_PATH)
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .getOrCreate()
    )


def process_batch(batch_df, batch_id):
    """
    Process a micro-batch of new transactions.

    For each batch:
    1. Extract unique addresses (from + to)
    2. Score addresses using the GNN inference service
    3. Filter high-risk addresses (score > threshold)
    4. Write alerts to Kafka + PostgreSQL
    """
    if batch_df.isEmpty():
        return

    logger.info(f"Processing batch {batch_id}: {batch_df.count()} transactions")

    # Collect unique addresses from this batch
    from_addrs = batch_df.select("from_address").distinct().collect()
    to_addrs = batch_df.select("to_address").distinct().collect()
    all_addresses = list(set(
        [row.from_address for row in from_addrs if row.from_address] +
        [row.to_address for row in to_addrs if row.to_address]
    ))

    if not all_addresses:
        return

    # Score addresses
    service = get_inference_service()
    results = service.score_addresses(all_addresses)

    # Filter alerts (above threshold)
    alerts = [r for r in results if r["phishing_score"] >= PHISHING_THRESHOLD]

    if not alerts:
        logger.info(f"Batch {batch_id}: {len(results)} addresses scored, 0 alerts")
        return

    logger.info(
        f"Batch {batch_id}: {len(results)} scored, "
        f"{len(alerts)} ALERTS (score >= {PHISHING_THRESHOLD})"
    )

    spark = batch_df.sparkSession

    # Create alerts DataFrame
    alerts_data = [
        (
            a["address"],
            float(a["phishing_score"]),
            int(a["predicted_label"]),
        )
        for a in alerts
    ]
    alerts_df = spark.createDataFrame(
        alerts_data,
        schema=["address", "phishing_score", "predicted_label"],
    )

    # Write alerts to Kafka topic
    (
        alerts_df
        .select(
            F.col("address").alias("key"),
            F.to_json(F.struct("*")).alias("value"),
        )
        .write
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("topic", KAFKA_TOPIC_ALERTS)
        .save()
    )

    # Write alerts to PostgreSQL
    (
        alerts_df
        .withColumn("source", F.lit("streaming"))
        .withColumn("model_version", F.lit("graphsage_v1"))
        .withColumn("detected_at", F.current_timestamp())
        .write
        .format("jdbc")
        .option("url", DB_JDBC_URL)
        .option("dbtable", "detection_results")
        .option("user", DB_USER)
        .option("password", DB_PASSWORD)
        .option("driver", "org.postgresql.Driver")
        .mode("append")
        .save()
    )


def main():
    """Start the streaming pipeline."""
    spark = create_spark_session()
    logger.info("Spark session created. Starting streaming pipeline...")

    # Read from Kafka
    stream_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC_NEW_TX)
        .option("startingOffsets", "latest")
        .option("maxOffsetsPerTrigger", 10000)  # backpressure control
        .load()
    )

    # Parse JSON values
    parsed_df = (
        stream_df
        .select(F.from_json(
            F.col("value").cast("string"), TX_SCHEMA
        ).alias("tx"))
        .select("tx.*")
        .filter(F.col("from_address").isNotNull())
    )

    # Process each micro-batch
    query = (
        parsed_df.writeStream
        .foreachBatch(process_batch)
        .outputMode("update")
        .trigger(processingTime="10 seconds")
        .start()
    )

    logger.info("Streaming query started. Waiting for termination...")
    query.awaitTermination()


if __name__ == "__main__":
    main()

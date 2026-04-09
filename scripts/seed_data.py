#!/usr/bin/env python3
"""
Seed Database with Mock Data
Populates PostgreSQL with sample data for platform testing and development.
"""

import os
import sys
import json
import random
import logging
from datetime import datetime, timedelta
from typing import Optional

import psycopg2
from psycopg2.extras import execute_values

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class DatabaseSeeder:
    """Seed PostgreSQL database with mock data."""

    # Known phishing addresses
    PHISHING_ADDRESSES = [
        "0x0000000000000000000000000000000000000bad",
        "0xdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef",
        "0xcafebabecafebabecafebabecafebabecafebabe",
        "0x123456789abcdef0123456789abcdef012345678",
        "0x999999999999999999999999999999999999999a",
    ]

    # Known legitimate addresses
    LEGITIMATE_ADDRESSES = [
        "0x6b175474e89094c44da98b954eedeac495271d0f",  # USDC
        "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",  # USDT
        "0xdac17f958d2ee523a2206206994597c13d831ec7",  # Tether
        "0x2260fac5e5542a773aa44fbcff9faccf98bf5422",  # WBTC
        "0x7f39c581f595b53c5cb19bd0b3f8da6c935e2ca0",  # Lido stETH
        "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2",  # WETH
        "0x514910771af9ca656af840dff83e8264ecf986ca",  # LINK
        "0x6982508145454ce325ddbe47a25d4ec3d2311933",  # Pepe
        "0x1f9840a85d5af5bf1d1762f925bdaddc4201f984",  # UNI
        "0x95ad61b0a150d79219dcf64e1e6cc01f0b64c4ce",  # SHIB
        "0x2af64bd8ae3868b5e48309e6e522000aa7b91e3f",  # Other
        "0x1111111254fb6c44bac0bed2854e76f90643097d",  # 1inch
        "0xae461ca67b15dc8dc81ce7615e0320da1a9ab8d5",  # Gnosis
        "0x7a250d5630b4cf539739df2c5dacb4c659f2488d",  # Uniswap Router
        "0x68b3465833fb72B5a828cCEBEAB51CB9ab5D9C34",  # Uniswap V3 Router
    ]

    def __init__(self):
        """Initialize database connection."""
        self.conn = None
        self.cursor = None

    def connect(self) -> bool:
        """
        Connect to PostgreSQL database.

        Returns:
            bool: True if connection successful
        """
        try:
            host = os.getenv("CLOUD_SQL_HOST", "localhost")
            user = os.getenv("POSTGRES_USER", "postgres")
            password = os.getenv("POSTGRES_PASSWORD", "")
            database = os.getenv("POSTGRES_DB", "eth_phishing")
            port = os.getenv("CLOUD_SQL_PORT", 5432)

            self.conn = psycopg2.connect(
                host=host,
                user=user,
                password=password,
                database=database,
                port=port,
                connect_timeout=10,
            )
            self.cursor = self.conn.cursor()
            logger.info(f"Connected to PostgreSQL: {host}/{database}")
            return True
        except Exception as e:
            logger.error(f"Failed to connect to PostgreSQL: {e}")
            return False

    def close(self):
        """Close database connection."""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
            logger.info("Database connection closed")

    def seed_addresses(self) -> int:
        """
        Seed addresses table with sample data.

        Returns:
            int: Number of addresses inserted
        """
        logger.info("Seeding addresses table...")

        addresses_data = []
        now = datetime.utcnow()

        # Add phishing addresses
        for i, addr in enumerate(self.PHISHING_ADDRESSES):
            addresses_data.append(
                (
                    addr,
                    1,  # label=phishing
                    "seed-data",
                    now - timedelta(days=30),
                    now,
                    random.randint(10, 500),
                    random.choice([True, False]),
                    now,
                    now,
                )
            )

        # Add legitimate addresses
        for i, addr in enumerate(self.LEGITIMATE_ADDRESSES):
            addresses_data.append(
                (
                    addr,
                    0,  # label=legitimate
                    "seed-data",
                    now - timedelta(days=90),
                    now,
                    random.randint(100, 5000),
                    True,  # is_contract
                    now,
                    now,
                )
            )

        try:
            insert_sql = """
                INSERT INTO addresses
                (address, label, label_source, first_seen_at, last_seen_at,
                 total_tx_count, is_contract, created_at, updated_at)
                VALUES %s
                ON CONFLICT (address) DO NOTHING
            """
            execute_values(self.cursor, insert_sql, addresses_data, page_size=100)
            self.conn.commit()
            count = len(addresses_data)
            logger.info(f"Inserted {count} addresses")
            return count
        except Exception as e:
            logger.error(f"Failed to seed addresses: {e}")
            self.conn.rollback()
            return 0

    def seed_model_versions(self) -> int:
        """
        Seed model_versions table with mock model version.

        Returns:
            int: Number of models inserted
        """
        logger.info("Seeding model_versions table...")

        now = datetime.utcnow()
        model_data = [
            (
                "mock-0.0.0",
                "MockPredictor",
                "./model_artifacts/mock/model.pt",
                True,  # is_active
                0,
                0.7,
                "mock",
                0,
                0,
                now,
                now,
                None,
                now,
            )
        ]

        try:
            insert_sql = """
                INSERT INTO model_versions
                (version, model_type, model_path, is_active, feature_count,
                 threshold, training_dataset, graph_nodes, graph_edges,
                 trained_at, deployed_at, retired_at, created_at)
                VALUES %s
                ON CONFLICT (version) DO NOTHING
            """
            execute_values(self.cursor, insert_sql, model_data)
            self.conn.commit()
            logger.info("Inserted 1 model version")
            return 1
        except Exception as e:
            logger.error(f"Failed to seed model_versions: {e}")
            self.conn.rollback()
            return 0

    def seed_model_metrics(self) -> int:
        """
        Seed model_metrics table with mock metrics.

        Returns:
            int: Number of metrics inserted
        """
        logger.info("Seeding model_metrics table...")

        metrics = [
            ("test_precision", 0.912),
            ("test_recall", 0.867),
            ("test_f1", 0.889),
            ("test_roc_auc", 0.945),
            ("test_pr_auc", 0.823),
            ("test_accuracy", 0.987),
        ]

        metrics_data = []
        for metric_name, metric_value in metrics:
            metrics_data.append((1, metric_name, metric_value, "test", datetime.utcnow()))

        try:
            insert_sql = """
                INSERT INTO model_metrics
                (model_version_id, metric_name, metric_value, dataset_split, created_at)
                VALUES %s
                ON CONFLICT (model_version_id, metric_name, dataset_split) DO NOTHING
            """
            execute_values(self.cursor, insert_sql, metrics_data)
            self.conn.commit()
            logger.info(f"Inserted {len(metrics_data)} metrics")
            return len(metrics_data)
        except Exception as e:
            logger.error(f"Failed to seed model_metrics: {e}")
            self.conn.rollback()
            return 0

    def seed_predictions(self) -> int:
        """
        Seed predictions table with sample prediction history.

        Returns:
            int: Number of predictions inserted
        """
        logger.info("Seeding predictions table...")

        predictions_data = []
        all_addresses = self.PHISHING_ADDRESSES + self.LEGITIMATE_ADDRESSES
        now = datetime.utcnow()

        # Generate 200 sample predictions
        for i in range(200):
            addr = random.choice(all_addresses)
            is_phishing = addr in self.PHISHING_ADDRESSES

            # Phishing addresses get higher scores
            if is_phishing:
                score = random.uniform(0.75, 0.99)
            else:
                score = random.uniform(0.0, 0.35)

            prediction = "phishing" if score >= 0.7 else "legitimate"
            confidence = (
                "high" if score >= 0.8 or score <= 0.2 else "medium"
            )

            predictions_data.append(
                (
                    addr,
                    round(score, 4),
                    prediction,
                    confidence,
                    0.7,  # threshold_used
                    1,  # model_version_id
                    "mock",  # inference_mode
                    random.uniform(0.5, 2.0),  # inference_time_ms
                    random.choice(["api", "batch"]),  # source
                    addr in self.PHISHING_ADDRESSES + self.LEGITIMATE_ADDRESSES,
                    None,  # risk_factors
                    None,  # request_metadata
                    now - timedelta(hours=random.randint(0, 48)),
                )
            )

        try:
            insert_sql = """
                INSERT INTO predictions
                (address, phishing_score, prediction, confidence, threshold_used,
                 model_version_id, inference_mode, inference_time_ms, source,
                 is_known_address, risk_factors, request_metadata, created_at)
                VALUES %s
            """
            execute_values(self.cursor, insert_sql, predictions_data, page_size=100)
            self.conn.commit()
            logger.info(f"Inserted {len(predictions_data)} predictions")
            return len(predictions_data)
        except Exception as e:
            logger.error(f"Failed to seed predictions: {e}")
            self.conn.rollback()
            return 0

    def seed_alerts(self) -> int:
        """
        Seed alerts table with sample alerts.

        Returns:
            int: Number of alerts inserted
        """
        logger.info("Seeding alerts table...")

        alerts_data = []
        now = datetime.utcnow()

        # Generate alerts for phishing addresses
        for i, addr in enumerate(self.PHISHING_ADDRESSES[:5]):
            for j in range(2):  # 2 alerts per phishing address
                score = random.uniform(0.85, 0.99)
                alerts_data.append(
                    (
                        addr,
                        round(score, 4),
                        random.choice(["api", "streaming"]),
                        f"0x{''.join(random.choices('0123456789abcdef', k=64))}",  # fake tx hash
                        1,  # model_version_id
                        random.choice([True, False]),  # reviewed
                        None,  # analyst_label
                        None,  # reviewer_notes
                        now - timedelta(hours=random.randint(0, 24)),
                        None,  # reviewed_at
                    )
                )

        try:
            insert_sql = """
                INSERT INTO alerts
                (address, phishing_score, trigger_source, trigger_tx_hash,
                 model_version_id, reviewed, analyst_label, reviewer_notes,
                 created_at, reviewed_at)
                VALUES %s
            """
            execute_values(self.cursor, insert_sql, alerts_data)
            self.conn.commit()
            logger.info(f"Inserted {len(alerts_data)} alerts")
            return len(alerts_data)
        except Exception as e:
            logger.error(f"Failed to seed alerts: {e}")
            self.conn.rollback()
            return 0

    def run(self) -> bool:
        """
        Run all seeding operations.

        Returns:
            bool: True if all operations successful
        """
        if not self.connect():
            return False

        try:
            # Seed in order of dependencies
            addr_count = self.seed_addresses()
            model_count = self.seed_model_versions()
            metrics_count = self.seed_model_metrics()
            pred_count = self.seed_predictions()
            alert_count = self.seed_alerts()

            logger.info("Seeding complete!")
            logger.info(f"  - Addresses: {addr_count}")
            logger.info(f"  - Model versions: {model_count}")
            logger.info(f"  - Metrics: {metrics_count}")
            logger.info(f"  - Predictions: {pred_count}")
            logger.info(f"  - Alerts: {alert_count}")

            return True
        except Exception as e:
            logger.error(f"Seeding failed: {e}")
            return False
        finally:
            self.close()


def seed_gcs_stats() -> bool:
    """
    Write mock statistics to GCS.

    This creates a _stats.json file in the GCS processed bucket so that
    the dashboard Data Sources page can display sample statistics.

    Returns:
        bool: True if successful or if GCS not configured
    """
    logger.info("Attempting to seed GCS statistics...")

    try:
        from google.cloud import storage
    except ImportError:
        logger.warning("google-cloud-storage not installed, skipping GCS stats")
        return True

    try:
        # Check if GCS is configured
        bucket_name = os.getenv("GCS_BUCKET_PROCESSED")
        if not bucket_name:
            logger.info("GCS_BUCKET_PROCESSED not configured, skipping GCS stats")
            return True

        project_id = os.getenv("GCP_PROJECT_ID")
        if not project_id:
            logger.info("GCP_PROJECT_ID not configured, skipping GCS stats")
            return True

        # Create mock stats
        stats = {
            "timestamp": datetime.utcnow().isoformat(),
            "data_sources": {
                "xblock": {
                    "status": "available",
                    "files_count": 4,
                    "last_updated": (datetime.utcnow() - timedelta(days=1)).isoformat(),
                    "records": 123456,
                },
                "bigquery": {
                    "status": "available",
                    "last_updated": (datetime.utcnow() - timedelta(hours=6)).isoformat(),
                    "records": 5000000,
                    "date_range": "2024-01-01 to 2024-01-31",
                },
            },
            "features": {
                "computed_count": 12,
                "feature_names": [
                    "in_degree",
                    "out_degree",
                    "total_eth_received",
                    "total_eth_sent",
                    "avg_tx_value_in",
                    "avg_tx_value_out",
                    "max_tx_value",
                    "unique_in_neighbors",
                    "unique_out_neighbors",
                    "account_lifetime",
                    "failed_tx_ratio",
                    "avg_gas_used",
                ],
                "normalization": "standard_scaler",
            },
            "addresses": {
                "total": 128789,
                "labeled": {
                    "phishing": 12345,
                    "legitimate": 78901,
                    "unknown": 37543,
                },
            },
        }

        # Upload to GCS
        client = storage.Client(project=project_id)
        bucket = client.bucket(bucket_name)
        blob = bucket.blob("_stats.json")

        stats_json = json.dumps(stats, indent=2)
        blob.upload_from_string(
            stats_json,
            content_type="application/json",
        )

        logger.info(f"Seeded GCS stats to gs://{bucket_name}/_stats.json")
        return True

    except Exception as e:
        logger.warning(f"Failed to seed GCS stats: {e}")
        # This is optional, so don't fail the entire seeding process
        return True


def main():
    """Main entry point."""
    seeder = DatabaseSeeder()
    db_success = seeder.run()

    # Optional: seed GCS statistics
    gcs_success = seed_gcs_stats()

    sys.exit(0 if db_success else 1)


if __name__ == "__main__":
    main()

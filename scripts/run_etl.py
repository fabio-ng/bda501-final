#!/usr/bin/env python3
"""
ETL Pipeline Runner — Coordinates all data ingestion and processing steps.

Usage:
    python scripts/run_etl.py --help
    python scripts/run_etl.py --source all              # Run all jobs
    python scripts/run_etl.py --source xblock           # XBlock-ETH only
    python scripts/run_etl.py --source bigquery --start-date 2024-01-01 --end-date 2024-01-31
    python scripts/run_etl.py --source process          # Feature engineering only
    python scripts/run_etl.py --dry-run --source all    # Estimate cost, no execution
"""

import os
import sys
import json
import time
import logging
import argparse
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, Dict, Any

import yaml

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class ETLOrchestrator:
    """Orchestrates ETL pipeline execution."""

    def __init__(
        self,
        config_path: str = "configs/config.yaml",
        project_id: Optional[str] = None,
        bucket_raw: Optional[str] = None,
        bucket_processed: Optional[str] = None,
        data_dir: Optional[str] = None,
        credentials: Optional[str] = None,
    ):
        """
        Initialize ETL orchestrator.

        Args:
            config_path: Path to config.yaml
            project_id: GCP project ID (overrides config)
            bucket_raw: GCS raw bucket name (overrides config)
            bucket_processed: GCS processed bucket name (overrides config)
            data_dir: Local data directory (overrides config)
            credentials: Path to GCP credentials JSON (overrides config)
        """
        self.config_path = config_path
        self.config = self._load_config()
        self.project_id = project_id or self.config["gcp"]["project_id"]
        self.bucket_raw = bucket_raw or self.config["storage"]["gcs"]["buckets"]["raw"]
        self.bucket_processed = (
            bucket_processed or self.config["storage"]["gcs"]["buckets"]["processed"]
        )
        self.data_dir = data_dir or "data/raw"
        self.credentials = credentials or self.config["gcp"]["credentials_file"]

        # Expand environment variables
        self.project_id = os.path.expandvars(self.project_id)
        self.bucket_raw = os.path.expandvars(self.bucket_raw)
        self.bucket_processed = os.path.expandvars(self.bucket_processed)

        self.start_time = None
        self.end_time = None
        self.stats = {"jobs": {}, "total_rows": 0, "total_time_seconds": 0}

    def _load_config(self) -> Dict[str, Any]:
        """Load YAML configuration file."""
        try:
            with open(self.config_path, "r") as f:
                return yaml.safe_load(f)
        except FileNotFoundError:
            logger.error(f"Config file not found: {self.config_path}")
            raise
        except yaml.YAMLError as e:
            logger.error(f"Failed to parse config: {e}")
            raise

    def run_xblock_ingest(self, dry_run: bool = False) -> bool:
        """
        Run XBlock-ETH data ingestion.

        Args:
            dry_run: If True, estimate without executing

        Returns:
            bool: True if successful
        """
        logger.info("=" * 60)
        logger.info("Starting XBlock-ETH Ingestion")
        logger.info("=" * 60)

        xblock_config = self.config["data_sources"]["xblock_eth"]
        local_dir = xblock_config["local_data_dir"]
        gcs_prefix = self.config["etl"]["xblock_prefix"]

        try:
            # Check if local directory exists
            if not os.path.exists(local_dir):
                logger.warning(f"Local data directory not found: {local_dir}")
                logger.info("Skipping XBlock ingestion (files not available locally)")
                self.stats["jobs"]["xblock"] = {
                    "status": "skipped",
                    "reason": "local_files_not_found",
                    "files": 0,
                }
                return True

            # Count files
            csv_files = list(Path(local_dir).glob("*.csv")) + list(
                Path(local_dir).glob("*.txt")
            )
            logger.info(f"Found {len(csv_files)} files to upload")

            if dry_run:
                logger.info("[DRY RUN] Would upload to: gs://{}/{}/".format(
                    self.bucket_raw, gcs_prefix
                ))
                self.stats["jobs"]["xblock"] = {
                    "status": "dry_run",
                    "files_to_upload": len(csv_files),
                    "destination": f"gs://{self.bucket_raw}/{gcs_prefix}/",
                }
                return True

            # In a real scenario, this would call gsutil or google-cloud-storage
            logger.info(f"Uploading {len(csv_files)} files to gs://{self.bucket_raw}/{gcs_prefix}/")
            logger.info("[MOCK] Upload complete (in production, uses google-cloud-storage)")

            self.stats["jobs"]["xblock"] = {
                "status": "success",
                "files_uploaded": len(csv_files),
                "destination": f"gs://{self.bucket_raw}/{gcs_prefix}/",
            }
            return True

        except Exception as e:
            logger.error(f"XBlock ingestion failed: {e}")
            self.stats["jobs"]["xblock"] = {
                "status": "error",
                "error": str(e),
            }
            return False

    def run_bigquery_ingest(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        limit: Optional[int] = None,
        dry_run: bool = False,
    ) -> bool:
        """
        Run BigQuery data ingestion.

        Args:
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD)
            limit: Row limit
            dry_run: If True, estimate without executing

        Returns:
            bool: True if successful
        """
        logger.info("=" * 60)
        logger.info("Starting BigQuery Ingestion")
        logger.info("=" * 60)

        bq_config = self.config["data_sources"]["bigquery"]
        dataset = bq_config["dataset"]
        transactions_table = bq_config["transactions_table"]
        limit = limit or bq_config["default_limit"]
        gcs_prefix = self.config["etl"]["bigquery_prefix"]

        # Default date range
        if not end_date:
            end_date = datetime.now().strftime("%Y-%m-%d")
        if not start_date:
            lookback_days = bq_config["default_lookback_days"]
            start_dt = datetime.now() - timedelta(days=lookback_days)
            start_date = start_dt.strftime("%Y-%m-%d")

        logger.info(f"Date range: {start_date} to {end_date}")
        logger.info(f"Row limit: {limit}")

        try:
            query = f"""
            SELECT *
            FROM `{self.project_id}.{dataset}.{transactions_table}`
            WHERE block_timestamp BETWEEN CAST('{start_date}' AS DATE)
                AND CAST('{end_date}' AS DATE)
            LIMIT {limit}
            """

            if dry_run:
                logger.info("[DRY RUN] Query to execute:")
                logger.info(query)
                logger.info(f"[DRY RUN] Destination: gs://{self.bucket_raw}/{gcs_prefix}/")
                # Estimate cost (1 TB = $5)
                estimated_gb = 100  # Mock estimate
                estimated_cost = (estimated_gb / 1024) * 5
                logger.info(f"[DRY RUN] Estimated cost: ${estimated_cost:.2f}")

                self.stats["jobs"]["bigquery"] = {
                    "status": "dry_run",
                    "query_sample": query[:200] + "...",
                    "date_range": f"{start_date} to {end_date}",
                    "estimated_gb": estimated_gb,
                    "estimated_cost_usd": round(estimated_cost, 2),
                }
                return True

            logger.info("[MOCK] Executing BigQuery query")
            logger.info(f"[MOCK] Exporting to gs://{self.bucket_raw}/{gcs_prefix}/")
            logger.info("[MOCK] Export complete (in production, uses BigQuery API)")

            self.stats["jobs"]["bigquery"] = {
                "status": "success",
                "date_range": f"{start_date} to {end_date}",
                "row_limit": limit,
                "destination": f"gs://{self.bucket_raw}/{gcs_prefix}/",
            }
            return True

        except Exception as e:
            logger.error(f"BigQuery ingestion failed: {e}")
            self.stats["jobs"]["bigquery"] = {
                "status": "error",
                "error": str(e),
            }
            return False

    def run_feature_engineering(self, dry_run: bool = False) -> bool:
        """
        Run feature engineering (raw → processed).

        Args:
            dry_run: If True, estimate without executing

        Returns:
            bool: True if successful
        """
        logger.info("=" * 60)
        logger.info("Starting Feature Engineering")
        logger.info("=" * 60)

        fe_config = self.config["feature_engineering"]
        raw_prefix = self.config["etl"]["raw_prefix"]
        processed_prefix = self.config["etl"]["processed_prefix"]

        logger.info(f"Features: {', '.join(fe_config['feature_names'])}")
        logger.info(f"Normalization: {fe_config['normalization']}")

        try:
            if dry_run:
                logger.info(f"[DRY RUN] Reading from: gs://{self.bucket_raw}/{raw_prefix}/")
                logger.info(f"[DRY RUN] Writing to: gs://{self.bucket_processed}/{processed_prefix}/")
                logger.info(f"[DRY RUN] Computing {len(fe_config['feature_names'])} features")

                self.stats["jobs"]["feature_engineering"] = {
                    "status": "dry_run",
                    "feature_count": len(fe_config["feature_names"]),
                    "source": f"gs://{self.bucket_raw}/{raw_prefix}/",
                    "destination": f"gs://{self.bucket_processed}/{processed_prefix}/",
                }
                return True

            logger.info("[MOCK] Loading raw data")
            logger.info("[MOCK] Computing graph features")
            logger.info("[MOCK] Normalizing features")
            logger.info(f"[MOCK] Writing {len(fe_config['feature_names'])} features to processed bucket")
            logger.info("[MOCK] Feature engineering complete")

            self.stats["jobs"]["feature_engineering"] = {
                "status": "success",
                "feature_count": len(fe_config["feature_names"]),
                "source": f"gs://{self.bucket_raw}/{raw_prefix}/",
                "destination": f"gs://{self.bucket_processed}/{processed_prefix}/",
            }
            return True

        except Exception as e:
            logger.error(f"Feature engineering failed: {e}")
            self.stats["jobs"]["feature_engineering"] = {
                "status": "error",
                "error": str(e),
            }
            return False

    def run_all(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        limit: Optional[int] = None,
        dry_run: bool = False,
    ) -> bool:
        """
        Run full ETL pipeline.

        Args:
            start_date: For BigQuery ingestion
            end_date: For BigQuery ingestion
            limit: For BigQuery ingestion
            dry_run: If True, estimate without executing

        Returns:
            bool: True if all jobs succeeded
        """
        logger.info("\n")
        logger.info("=" * 60)
        logger.info("ETL PIPELINE START")
        logger.info("=" * 60)
        logger.info(f"Project: {self.project_id}")
        logger.info(f"Raw Bucket: {self.bucket_raw}")
        logger.info(f"Processed Bucket: {self.bucket_processed}")
        if dry_run:
            logger.info("Mode: DRY RUN (no data will be processed)")
        logger.info("=" * 60)
        logger.info("\n")

        self.start_time = time.time()

        # Run jobs in sequence
        xblock_ok = self.run_xblock_ingest(dry_run=dry_run)
        bigquery_ok = self.run_bigquery_ingest(
            start_date=start_date, end_date=end_date, limit=limit, dry_run=dry_run
        )
        fe_ok = self.run_feature_engineering(dry_run=dry_run)

        self.end_time = time.time()
        self.stats["total_time_seconds"] = round(self.end_time - self.start_time, 2)

        return xblock_ok and bigquery_ok and fe_ok

    def print_summary(self):
        """Print execution summary."""
        logger.info("\n")
        logger.info("=" * 60)
        logger.info("ETL PIPELINE SUMMARY")
        logger.info("=" * 60)

        for job_name, job_stats in self.stats["jobs"].items():
            status = job_stats.get("status", "unknown")
            logger.info(f"\n{job_name.upper()}: {status.upper()}")

            if status == "error":
                logger.error(f"  Error: {job_stats.get('error', 'Unknown error')}")
            elif status == "skipped":
                logger.info(f"  Reason: {job_stats.get('reason', 'Unknown')}")
            elif status == "dry_run":
                logger.info(f"  Files: {job_stats.get('files_to_upload', job_stats.get('feature_count', 'N/A'))}")
            else:
                for key, value in job_stats.items():
                    if key != "status":
                        logger.info(f"  {key}: {value}")

        logger.info(f"\nTotal time: {self.stats['total_time_seconds']} seconds")
        logger.info("=" * 60)
        logger.info("\n")


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="ETL Pipeline Runner",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )

    parser.add_argument(
        "--source",
        choices=["all", "xblock", "bigquery", "process"],
        default="all",
        help="Which ETL job(s) to run (default: all)",
    )
    parser.add_argument(
        "--start-date",
        type=str,
        help="Start date for BigQuery ingestion (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--end-date",
        type=str,
        help="End date for BigQuery ingestion (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        help="Row limit for BigQuery query",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Estimate costs and show what would run, without executing",
    )
    parser.add_argument(
        "--project",
        type=str,
        help="GCP project ID (overrides config)",
    )
    parser.add_argument(
        "--bucket-raw",
        type=str,
        help="GCS raw bucket name (overrides config)",
    )
    parser.add_argument(
        "--bucket-processed",
        type=str,
        help="GCS processed bucket name (overrides config)",
    )
    parser.add_argument(
        "--data-dir",
        type=str,
        help="Local data directory (overrides config)",
    )
    parser.add_argument(
        "--credentials",
        type=str,
        help="Path to GCP service account JSON (overrides config)",
    )
    parser.add_argument(
        "--config",
        type=str,
        default="configs/config.yaml",
        help="Path to config.yaml (default: configs/config.yaml)",
    )

    args = parser.parse_args()

    # Initialize orchestrator
    orchestrator = ETLOrchestrator(
        config_path=args.config,
        project_id=args.project,
        bucket_raw=args.bucket_raw,
        bucket_processed=args.bucket_processed,
        data_dir=args.data_dir,
        credentials=args.credentials,
    )

    # Run selected job(s)
    success = False
    if args.source == "all":
        success = orchestrator.run_all(
            start_date=args.start_date,
            end_date=args.end_date,
            limit=args.limit,
            dry_run=args.dry_run,
        )
    elif args.source == "xblock":
        success = orchestrator.run_xblock_ingest(dry_run=args.dry_run)
    elif args.source == "bigquery":
        success = orchestrator.run_bigquery_ingest(
            start_date=args.start_date,
            end_date=args.end_date,
            limit=args.limit,
            dry_run=args.dry_run,
        )
    elif args.source == "process":
        success = orchestrator.run_feature_engineering(dry_run=args.dry_run)

    # Print summary
    orchestrator.print_summary()

    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()

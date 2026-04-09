"""
Pipeline orchestrator: runs all ETL jobs in sequence.
"""

import argparse
import json
import logging
import time
from datetime import datetime
from typing import Dict, Optional

import yaml

from etl.jobs.export_predictions import ExportPredictionsJob
from etl.jobs.ingest_bigquery import IngestBigQueryJob
from etl.jobs.ingest_xblock import IngestXBlockJob
from etl.jobs.process_features import ProcessFeaturesJob

logger = logging.getLogger(__name__)


class ETLPipeline:
    """
    Orchestrates complete ETL pipeline:
    1. IngestXBlockJob (mandatory)
    2. IngestBigQueryJob (optional)
    3. ProcessFeaturesJob
    4. ExportPredictionsJob (optional)
    """

    def __init__(self, config_path: Optional[str] = None):
        """
        Initialize pipeline.

        Args:
            config_path: Path to config YAML file
        """
        self.config = self._load_config(config_path)
        self.job_logs = []

        logger.info(f"Initialized ETLPipeline with config: {self.config}")

    def run(
        self,
        dry_run: bool = False,
        skip_bigquery: bool = False,
        skip_predictions: bool = False,
    ) -> Dict[str, any]:
        """
        Run the complete pipeline.

        Args:
            dry_run: If True, only plan the jobs without executing
            skip_bigquery: Skip BigQuery ingestion
            skip_predictions: Skip predictions export

        Returns:
            Dictionary with pipeline execution results
        """
        pipeline_start = time.time()

        results = {
            "pipeline_start": datetime.now().isoformat(),
            "dry_run": dry_run,
            "jobs": [],
        }

        try:
            # Step 1: Ingest XBlock data (mandatory)
            logger.info("=" * 80)
            logger.info("STEP 1: Ingest XBlock data")
            logger.info("=" * 80)

            if not dry_run:
                try:
                    xblock_job = IngestXBlockJob(
                        gcs_bucket_raw=self.config["gcs_bucket_raw"],
                        local_data_dir=self.config.get("xblock_data_dir"),
                        credentials_path=self.config.get("credentials_path"),
                    )
                    xblock_result = xblock_job.run()
                    results["jobs"].append({"job": "ingest_xblock", **xblock_result})
                    logger.info(f"XBlock ingestion result: {xblock_result}")
                except Exception as e:
                    logger.error(f"XBlock ingestion failed: {e}")
                    results["jobs"].append({
                        "job": "ingest_xblock",
                        "status": "failed",
                        "error": str(e),
                    })
                    raise
            else:
                logger.info("[DRY RUN] Would ingest XBlock data")
                results["jobs"].append({"job": "ingest_xblock", "status": "skipped (dry_run)"})

            # Step 2: Ingest BigQuery data (optional)
            if not skip_bigquery and self.config.get("bigquery_enabled"):
                logger.info("=" * 80)
                logger.info("STEP 2: Ingest BigQuery data")
                logger.info("=" * 80)

                if not dry_run:
                    try:
                        bq_job = IngestBigQueryJob(
                            project_id=self.config["gcp_project"],
                            gcs_bucket_raw=self.config["gcs_bucket_raw"],
                            credentials_path=self.config.get("credentials_path"),
                        )
                        bq_result = bq_job.run(
                            start_date=self.config["bigquery_start_date"],
                            end_date=self.config["bigquery_end_date"],
                            limit=self.config.get("bigquery_limit"),
                            include_transfers=self.config.get("bigquery_include_transfers", False),
                        )
                        results["jobs"].append({"job": "ingest_bigquery", **bq_result})
                        logger.info(f"BigQuery ingestion result: {bq_result}")
                    except Exception as e:
                        logger.error(f"BigQuery ingestion failed: {e}")
                        results["jobs"].append({
                            "job": "ingest_bigquery",
                            "status": "failed",
                            "error": str(e),
                        })
                        # Don't re-raise - pipeline can continue with XBlock data only
                else:
                    logger.info("[DRY RUN] Would ingest BigQuery data")
                    results["jobs"].append({"job": "ingest_bigquery", "status": "skipped (dry_run)"})
            else:
                logger.info("Skipping BigQuery ingestion (disabled or --skip-bigquery)")
                results["jobs"].append({"job": "ingest_bigquery", "status": "skipped"})

            # Step 3: Process features (mandatory)
            logger.info("=" * 80)
            logger.info("STEP 3: Process features")
            logger.info("=" * 80)

            if not dry_run:
                try:
                    features_job = ProcessFeaturesJob(
                        gcs_bucket_raw=self.config["gcs_bucket_raw"],
                        gcs_bucket_processed=self.config["gcs_bucket_processed"],
                        credentials_path=self.config.get("credentials_path"),
                    )
                    features_result = features_job.run(
                        source=self.config.get("feature_source", "xblock"),
                        use_spark=self.config.get("use_spark", False),
                    )
                    results["jobs"].append({"job": "process_features", **features_result})
                    logger.info(f"Feature processing result: {features_result}")
                except Exception as e:
                    logger.error(f"Feature processing failed: {e}")
                    results["jobs"].append({
                        "job": "process_features",
                        "status": "failed",
                        "error": str(e),
                    })
                    raise
            else:
                logger.info("[DRY RUN] Would process features")
                results["jobs"].append({"job": "process_features", "status": "skipped (dry_run)"})

            # Step 4: Export predictions (optional)
            if not skip_predictions and self.config.get("predictions_enabled"):
                logger.info("=" * 80)
                logger.info("STEP 4: Export predictions")
                logger.info("=" * 80)

                if not dry_run:
                    try:
                        pred_job = ExportPredictionsJob(
                            gcs_bucket_processed=self.config["gcs_bucket_processed"],
                            credentials_path=self.config.get("credentials_path"),
                        )
                        pred_result = pred_job.run(
                            model_path=self.config.get("model_path"),
                            predictor=self.config.get("predictor_type", "mock"),
                        )
                        results["jobs"].append({"job": "export_predictions", **pred_result})
                        logger.info(f"Predictions export result: {pred_result}")
                    except Exception as e:
                        logger.error(f"Predictions export failed: {e}")
                        results["jobs"].append({
                            "job": "export_predictions",
                            "status": "failed",
                            "error": str(e),
                        })
                        # Don't re-raise - pipeline can succeed without predictions
                else:
                    logger.info("[DRY RUN] Would export predictions")
                    results["jobs"].append({"job": "export_predictions", "status": "skipped (dry_run)"})
            else:
                logger.info("Skipping predictions export (disabled or --skip-predictions)")
                results["jobs"].append({"job": "export_predictions", "status": "skipped"})

            # Pipeline completed
            pipeline_duration = time.time() - pipeline_start
            results["pipeline_end"] = datetime.now().isoformat()
            results["pipeline_duration_seconds"] = pipeline_duration
            results["status"] = "success"

            logger.info("=" * 80)
            logger.info(f"PIPELINE COMPLETED SUCCESSFULLY in {pipeline_duration:.2f}s")
            logger.info("=" * 80)

            return results

        except Exception as e:
            logger.error(f"Pipeline failed: {e}")
            results["status"] = "failed"
            results["error"] = str(e)
            pipeline_duration = time.time() - pipeline_start
            results["pipeline_duration_seconds"] = pipeline_duration
            return results

    def _load_config(self, config_path: Optional[str]) -> Dict:
        """
        Load configuration from YAML file or environment.

        Args:
            config_path: Path to config YAML

        Returns:
            Configuration dictionary
        """
        config = {
            # GCS buckets
            "gcs_bucket_raw": "eth-phishing-raw",
            "gcs_bucket_processed": "eth-phishing-processed",

            # GCP
            "gcp_project": "eth-phishing-project",
            "credentials_path": None,

            # XBlock
            "xblock_data_dir": "./xblock_data",

            # BigQuery
            "bigquery_enabled": False,
            "bigquery_start_date": "2020-01-01",
            "bigquery_end_date": "2020-01-31",
            "bigquery_limit": None,
            "bigquery_include_transfers": False,

            # Features
            "feature_source": "xblock",
            "use_spark": False,

            # Predictions
            "predictions_enabled": False,
            "model_path": None,
            "predictor_type": "mock",
        }

        # Load from YAML if provided
        if config_path:
            try:
                with open(config_path) as f:
                    yaml_config = yaml.safe_load(f)
                config.update(yaml_config)
                logger.info(f"Loaded config from {config_path}")
            except Exception as e:
                logger.warning(f"Failed to load config from {config_path}: {e}")

        # Override with environment variables
        import os
        env_overrides = {
            "gcs_bucket_raw": os.environ.get("GCS_BUCKET_RAW"),
            "gcs_bucket_processed": os.environ.get("GCS_BUCKET_PROCESSED"),
            "gcp_project": os.environ.get("GCP_PROJECT"),
            "credentials_path": os.environ.get("GOOGLE_APPLICATION_CREDENTIALS"),
            "xblock_data_dir": os.environ.get("XBLOCK_DATA_DIR"),
        }

        config.update({k: v for k, v in env_overrides.items() if v})

        logger.info(f"Final config: {config}")
        return config


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    parser = argparse.ArgumentParser(
        description="Run complete ETL pipeline"
    )
    parser.add_argument(
        "--config",
        default="configs/config.yaml",
        help="Path to config YAML (default: configs/config.yaml)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Plan pipeline without executing",
    )
    parser.add_argument(
        "--skip-bigquery",
        action="store_true",
        help="Skip BigQuery ingestion",
    )
    parser.add_argument(
        "--skip-predictions",
        action="store_true",
        help="Skip predictions export",
    )

    args = parser.parse_args()

    pipeline = ETLPipeline(config_path=args.config)
    result = pipeline.run(
        dry_run=args.dry_run,
        skip_bigquery=args.skip_bigquery,
        skip_predictions=args.skip_predictions,
    )

    logger.info(f"Pipeline result: {json.dumps(result, indent=2)}")

"""
BigQuery ingestion job: fetch Ethereum transactions and export to GCS.
"""

import argparse
import json
import logging
import time
from datetime import datetime
from typing import Dict, Optional

from etl.schemas.transaction_schema import (
    BIGQUERY_TRANSFER_SQL,
    BIGQUERY_TRANSACTION_SQL,
)
from etl.utils.bq_utils import BQClient
from etl.utils.gcs_utils import GCSClient

logger = logging.getLogger(__name__)


class IngestBigQueryJob:
    """
    Fetches Ethereum transactions from BigQuery public dataset
    and writes them as Parquet to GCS raw bucket.

    Source: bigquery-public-data.crypto_ethereum.transactions
    Target: gs://{GCS_BUCKET_RAW}/transactions/bigquery/year=YYYY/month=MM/
    """

    def __init__(
        self,
        project_id: str,
        gcs_bucket_raw: str,
        credentials_path: Optional[str] = None,
    ):
        """
        Initialize BigQuery ingestion job.

        Args:
            project_id: GCP project ID
            gcs_bucket_raw: GCS bucket name for raw data
            credentials_path: Path to service account JSON
        """
        self.project_id = project_id
        self.gcs_bucket_raw = gcs_bucket_raw
        self.credentials_path = credentials_path

        self.bq_client = BQClient(project_id, credentials_path)
        self.gcs_client = GCSClient(gcs_bucket_raw, credentials_path)

        logger.info(
            f"Initialized IngestBigQueryJob for project {project_id}, bucket {gcs_bucket_raw}"
        )

    def run(
        self,
        start_date: str,
        end_date: str,
        limit: Optional[int] = None,
        export_to_gcs: bool = True,
        include_transfers: bool = False,
    ) -> Dict[str, any]:
        """
        Main entry point for BigQuery ingestion job.

        Args:
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD)
            limit: Optional limit on number of rows
            export_to_gcs: Whether to export to GCS (default True)
            include_transfers: Whether to also fetch token transfers (default False)

        Returns:
            Dictionary with job statistics
        """
        job_start_time = time.time()
        stats = {
            "start_date": start_date,
            "end_date": end_date,
            "rows_exported": 0,
            "gcs_paths": [],
            "duration_seconds": 0,
            "bytes_billed": 0,
            "timestamp": datetime.now().isoformat(),
        }

        try:
            logger.info(f"Starting BigQuery ingestion: {start_date} to {end_date}")

            # Estimate query cost
            transaction_sql = self._build_transaction_sql(start_date, end_date, limit)
            cost_bytes = self.bq_client.estimate_query_cost(transaction_sql)
            stats["bytes_billed"] = cost_bytes
            logger.info(f"Estimated cost: ${cost_bytes / (1024**4) * 6.25:.2f}")

            # Export transactions
            if export_to_gcs:
                # Use GCS export for large queries
                year_month = start_date[:7].replace("-", "/")
                gcs_uri = (
                    f"gs://{self.gcs_bucket_raw}/transactions/bigquery/year={year_month}/*.parquet"
                )

                logger.info(f"Exporting to GCS: {gcs_uri}")
                self.bq_client.query_to_gcs(
                    transaction_sql,
                    gcs_uri,
                    format="PARQUET",
                    timeout=600,
                )
                stats["gcs_paths"].append(gcs_uri)

                # Count rows exported (approximate)
                result_df = self.bq_client.query_to_dataframe(
                    f"SELECT COUNT(*) as cnt FROM ({transaction_sql})", timeout=600
                )
                stats["rows_exported"] = int(result_df["cnt"].iloc[0])
            else:
                # Small query - load to memory
                result_df = self.bq_client.query_to_dataframe(
                    transaction_sql, timeout=600
                )
                stats["rows_exported"] = len(result_df)

                # Upload to GCS
                year_month = start_date[:7].replace("-", "/")
                gcs_path = f"transactions/bigquery/year={year_month}/transactions.parquet"
                self.gcs_client.upload_dataframe_as_parquet(result_df, gcs_path)
                stats["gcs_paths"].append(f"gs://{self.gcs_bucket_raw}/{gcs_path}")

            # Optional: fetch token transfers
            if include_transfers:
                logger.info("Fetching token transfers...")
                transfer_sql = self._build_transfer_sql(start_date, end_date, limit)
                year_month = start_date[:7].replace("-", "/")
                gcs_uri = f"gs://{self.gcs_bucket_raw}/transfers/bigquery/year={year_month}/*.parquet"

                self.bq_client.query_to_gcs(
                    transfer_sql,
                    gcs_uri,
                    format="PARQUET",
                    timeout=600,
                )
                stats["gcs_paths"].append(gcs_uri)
                logger.info(f"Token transfers exported to {gcs_uri}")

            # Register job in log
            self._log_job(stats)

            job_duration = time.time() - job_start_time
            stats["duration_seconds"] = job_duration

            logger.info(
                f"BigQuery ingestion completed: {stats['rows_exported']} rows in {job_duration:.2f}s"
            )
            return stats

        except Exception as e:
            logger.error(f"BigQuery ingestion failed: {e}")
            stats["error"] = str(e)
            self._log_job(stats)
            raise

    def _build_transaction_sql(
        self,
        start_date: str,
        end_date: str,
        limit: Optional[int] = None,
    ) -> str:
        """
        Build transaction query SQL.

        Args:
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD)
            limit: Optional row limit

        Returns:
            SQL query string
        """
        sql = BIGQUERY_TRANSACTION_SQL

        if limit:
            sql = sql.replace("LIMIT @limit", f"LIMIT {limit}")
        else:
            sql = sql.replace("LIMIT @limit", "")

        # Note: In production, use parameterized queries with job_config.query_parameters
        sql = sql.replace("@start_date", f"'{start_date}'")
        sql = sql.replace("@end_date", f"'{end_date}'")

        logger.debug(f"Built transaction SQL: {sql[:200]}...")
        return sql

    def _build_transfer_sql(
        self,
        start_date: str,
        end_date: str,
        limit: Optional[int] = None,
    ) -> str:
        """
        Build token transfer query SQL.

        Args:
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD)
            limit: Optional row limit

        Returns:
            SQL query string
        """
        sql = BIGQUERY_TRANSFER_SQL

        if limit:
            sql = sql.replace("LIMIT @limit", f"LIMIT {limit}")
        else:
            sql = sql.replace("LIMIT @limit", "")

        sql = sql.replace("@start_date", f"'{start_date}'")
        sql = sql.replace("@end_date", f"'{end_date}'")

        logger.debug(f"Built transfer SQL: {sql[:200]}...")
        return sql

    def _log_job(self, stats: Dict) -> None:
        """
        Register job execution in GCS log file.

        Args:
            stats: Job statistics dictionary
        """
        try:
            # Try to read existing log
            log_path = "etl_job_log.json"
            if self.gcs_client.blob_exists(log_path):
                existing_log = self.gcs_client.download_file(log_path, "/tmp/log.json")
                with open("/tmp/log.json") as f:
                    jobs = json.load(f)
            else:
                jobs = []

            # Append new job
            jobs.append(stats)

            # Write back to GCS
            with open("/tmp/log.json", "w") as f:
                json.dump(jobs, f, indent=2)

            self.gcs_client.upload_file("/tmp/log.json", log_path)
            logger.info(f"Logged job to {log_path}")
        except Exception as e:
            logger.warning(f"Failed to log job: {e}")


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    parser = argparse.ArgumentParser(
        description="Ingest Ethereum transactions from BigQuery"
    )
    parser.add_argument(
        "--start-date",
        required=True,
        help="Start date (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--end-date",
        required=True,
        help="End date (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Row limit (optional)",
    )
    parser.add_argument(
        "--project",
        required=True,
        help="GCP project ID",
    )
    parser.add_argument(
        "--bucket",
        required=True,
        help="GCS bucket name for raw data",
    )
    parser.add_argument(
        "--credentials",
        default=None,
        help="Path to service account JSON",
    )
    parser.add_argument(
        "--include-transfers",
        action="store_true",
        help="Also fetch token transfers",
    )

    args = parser.parse_args()

    job = IngestBigQueryJob(
        project_id=args.project,
        gcs_bucket_raw=args.bucket,
        credentials_path=args.credentials,
    )

    result = job.run(
        start_date=args.start_date,
        end_date=args.end_date,
        limit=args.limit,
        include_transfers=args.include_transfers,
    )

    logger.info(f"Job result: {json.dumps(result, indent=2)}")

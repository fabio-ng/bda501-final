"""
Google BigQuery utilities for ETL pipeline.
"""

import logging
import os
from typing import Optional

import pandas as pd
from google.cloud import bigquery
from google.cloud.exceptions import GoogleCloudError

logger = logging.getLogger(__name__)


class BQClient:
    """Wrapper around Google Cloud BigQuery client."""

    def __init__(
        self,
        project_id: str,
        credentials_path: Optional[str] = None,
    ):
        """
        Initialize BigQuery client.

        Args:
            project_id: GCP project ID
            credentials_path: Path to service account JSON
                             (defaults to GOOGLE_APPLICATION_CREDENTIALS env var)
        """
        self.project_id = project_id
        self.credentials_path = credentials_path or os.environ.get(
            "GOOGLE_APPLICATION_CREDENTIALS"
        )

        try:
            self.client = bigquery.Client(project=project_id)
            logger.info(f"Initialized BQClient for project: {project_id}")
        except GoogleCloudError as e:
            logger.error(f"Failed to initialize BQClient: {e}")
            raise

    def query_to_dataframe(
        self,
        sql: str,
        timeout: int = 300,
    ) -> pd.DataFrame:
        """
        Execute a BigQuery query and return results as pandas DataFrame.

        Args:
            sql: SQL query string
            timeout: Query timeout in seconds (default 300)

        Returns:
            Pandas DataFrame with query results
        """
        try:
            job_config = bigquery.QueryJobConfig()
            query_job = self.client.query(sql, job_config=job_config, timeout=timeout)
            df = query_job.to_dataframe()
            logger.info(f"Query returned {len(df)} rows")
            return df
        except GoogleCloudError as e:
            logger.error(f"BigQuery query failed: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error during query: {e}")
            raise

    def query_to_gcs(
        self,
        sql: str,
        gcs_uri: str,
        format: str = "PARQUET",
        timeout: int = 600,
    ) -> str:
        """
        Execute a BigQuery query and export results directly to GCS.

        Avoids loading entire result set into memory.

        Args:
            sql: SQL query string
            gcs_uri: Destination GCS URI (gs://bucket/path/*.ext)
            format: Export format ('PARQUET', 'CSV', 'JSON', 'AVRO')
            timeout: Query timeout in seconds (default 600)

        Returns:
            GCS URI where results were written
        """
        try:
            job_config = bigquery.QueryJobConfig()
            job_config.destination_format = getattr(
                bigquery.DestinationFormat, format
            )
            job_config.allow_large_results = True
            job_config.destination = gcs_uri

            query_job = self.client.query(sql, job_config=job_config, timeout=timeout)
            query_job.result()  # Wait for job to complete

            logger.info(f"Query results exported to {gcs_uri}")
            return gcs_uri
        except GoogleCloudError as e:
            logger.error(f"BigQuery export failed: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error during export: {e}")
            raise

    def get_table_schema(
        self,
        dataset: str,
        table: str,
    ) -> dict:
        """
        Get schema (field names and types) for a BigQuery table.

        Args:
            dataset: Dataset ID
            table: Table ID

        Returns:
            Dictionary mapping field names to types
        """
        try:
            table_id = f"{self.project_id}.{dataset}.{table}"
            bq_table = self.client.get_table(table_id)

            schema_dict = {
                field.name: field.field_type for field in bq_table.schema
            }
            logger.info(f"Retrieved schema for {table_id}: {len(schema_dict)} fields")
            return schema_dict
        except GoogleCloudError as e:
            logger.error(f"Failed to get table schema: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error retrieving schema: {e}")
            raise

    def estimate_query_cost(self, sql: str) -> int:
        """
        Estimate the number of bytes that will be scanned by a query (dry run).

        Args:
            sql: SQL query string

        Returns:
            Number of bytes that will be scanned
        """
        try:
            job_config = bigquery.QueryJobConfig(dry_run=True)
            query_job = self.client.query(sql, job_config=job_config)

            bytes_billed = query_job.total_bytes_billed or 0
            logger.info(
                f"Query dry run: {bytes_billed:,} bytes (${bytes_billed / (1024**4) * 6.25:.2f})"
            )
            return bytes_billed
        except GoogleCloudError as e:
            logger.error(f"Failed to estimate query cost: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error estimating cost: {e}")
            raise

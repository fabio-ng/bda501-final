"""
Database connection and query management using psycopg2.
"""
import time
import logging
from typing import Optional, List, Dict, Any
import psycopg2
from psycopg2 import pool, sql
from psycopg2.extras import RealDictCursor
from contextlib import contextmanager
from api.config import settings

logger = logging.getLogger(__name__)


class DatabasePool:
    """PostgreSQL connection pool manager."""

    _instance: Optional["DatabasePool"] = None
    _pool: Optional[pool.SimpleConnectionPool] = None

    def __init__(self):
        """Initialize the database pool."""
        if DatabasePool._pool is None:
            try:
                DatabasePool._pool = pool.SimpleConnectionPool(
                    minconn=1,
                    maxconn=settings.DATABASE_POOL_SIZE,
                    database=self._parse_database_url()["database"],
                    user=self._parse_database_url()["user"],
                    password=self._parse_database_url()["password"],
                    host=self._parse_database_url()["host"],
                    port=self._parse_database_url()["port"],
                )
                logger.info("Database connection pool initialized")
            except Exception as e:
                logger.warning(f"Failed to initialize database pool: {e}")
                DatabasePool._pool = None

    @staticmethod
    def _parse_database_url() -> Dict[str, Any]:
        """Parse PostgreSQL URL."""
        url = settings.DATABASE_URL
        # Format: postgresql://user:password@host:port/database
        if not url.startswith("postgresql://"):
            url = url.replace("postgres://", "postgresql://")

        url = url[len("postgresql://"):]
        user_pass, host_db = url.split("@")
        user, password = user_pass.split(":")
        host_port, database = host_db.split("/")
        host, port = host_port.split(":")

        return {
            "user": user,
            "password": password,
            "host": host,
            "port": int(port),
            "database": database,
        }

    @contextmanager
    def get_connection(self):
        """Get a connection from the pool."""
        if DatabasePool._pool is None:
            raise RuntimeError("Database pool not initialized")

        conn = None
        try:
            conn = DatabasePool._pool.getconn()
            yield conn
        except Exception as e:
            logger.error(f"Database connection error: {e}")
            if conn is not None:
                try:
                    conn.rollback()
                except Exception:
                    pass
                try:
                    DatabasePool._pool.putconn(conn, close=True)
                finally:
                    conn = None
            raise
        finally:
            # Only return to the pool if we did NOT already hand it back in
            # the except branch — double-putconn triggers "trying to put
            # unkeyed connection" errors from psycopg2.
            if conn is not None:
                DatabasePool._pool.putconn(conn)

    @classmethod
    def get_instance(cls) -> "DatabasePool":
        """Get singleton instance."""
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    @staticmethod
    def close_all():
        """Close all connections in the pool."""
        if DatabasePool._pool:
            DatabasePool._pool.closeall()
            DatabasePool._pool = None
            logger.info("Database connection pool closed")


class Database:
    """Database query interface."""

    def __init__(self):
        """Initialize database interface."""
        self.pool = DatabasePool.get_instance()

    def health_check(self) -> tuple[bool, Optional[str], Optional[float]]:
        """
        Check database connectivity.
        Returns: (is_healthy, message, response_time_ms)
        """
        start = time.time()
        try:
            with self.pool.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT 1")
            elapsed = (time.time() - start) * 1000
            return True, "Connected", elapsed
        except Exception as e:
            return False, str(e), None

    def execute_query(self, query: str, params: tuple = None) -> None:
        """Execute a query without returning results."""
        try:
            with self.pool.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(query, params)
                conn.commit()
        except Exception as e:
            logger.error(f"Query execution error: {e}")
            raise

    def fetch_one(self, query: str, params: tuple = None) -> Optional[Dict[str, Any]]:
        """Fetch a single row."""
        try:
            with self.pool.get_connection() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    cur.execute(query, params)
                    return cur.fetchone()
        except Exception as e:
            logger.error(f"Query fetch_one error: {e}")
            raise

    def fetch_all(self, query: str, params: tuple = None) -> List[Dict[str, Any]]:
        """Fetch all rows."""
        try:
            with self.pool.get_connection() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    cur.execute(query, params)
                    return cur.fetchall()
        except Exception as e:
            logger.error(f"Query fetch_all error: {e}")
            raise

    def log_prediction(
        self,
        address: str,
        phishing_score: float,
        is_phishing: bool,
        model_version: str,
        inference_mode: str,
        threshold: float,
        inference_time_ms: float,
    ) -> str:
        """
        Log a prediction to the database.
        Returns: prediction_id
        """
        # The predictions table has NOT NULL constraints on `prediction` and
        # `confidence` (legacy columns). Derive them from the score/threshold.
        prediction_label = "phishing" if is_phishing else "legitimate"
        if phishing_score >= 0.85 or phishing_score <= 0.15:
            confidence_label = "high"
        elif phishing_score >= 0.65 or phishing_score <= 0.35:
            confidence_label = "medium"
        else:
            confidence_label = "low"

        query = """
        INSERT INTO predictions (
            address, phishing_score, prediction, confidence,
            threshold_used, is_phishing, model_version, threshold,
            inference_mode, inference_time_ms, source, created_at
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
        RETURNING id
        """
        try:
            with self.pool.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(query, (
                        address,
                        phishing_score,
                        prediction_label,
                        confidence_label,
                        threshold,
                        is_phishing,
                        model_version,
                        threshold,
                        inference_mode,
                        inference_time_ms,
                        "api",
                    ))
                    prediction_id = cur.fetchone()[0]
                conn.commit()
                return str(prediction_id)
        except Exception as e:
            logger.error(f"Error logging prediction: {e}")
            raise

    def create_alert(
        self,
        address: str,
        phishing_score: float,
        alert_type: str,
        severity: str,
        message: str,
    ) -> str:
        """
        Create a high-risk alert.
        Returns: alert_id
        """
        query = """
        INSERT INTO alerts (
            address, phishing_score, trigger_source, alert_type, severity,
            message, created_at
        )
        VALUES (%s, %s, %s, %s, %s, %s, NOW())
        RETURNING id
        """
        try:
            with self.pool.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(query, (
                        address,
                        phishing_score,
                        "api",
                        alert_type,
                        severity,
                        message,
                    ))
                    alert_id = cur.fetchone()[0]
                conn.commit()
                return str(alert_id)
        except Exception as e:
            logger.error(f"Error creating alert: {e}")
            raise

    def ensure_address(self, address: str) -> None:
        """Ensure address exists in the addresses table."""
        query = """
        INSERT INTO addresses (address, first_seen)
        VALUES (%s, NOW())
        ON CONFLICT (address) DO NOTHING
        """
        try:
            with self.pool.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(query, (address,))
                conn.commit()
        except Exception as e:
            logger.error(f"Error ensuring address: {e}")
            # Don't raise - this is non-critical

    def get_predictions_history(
        self,
        address: Optional[str] = None,
        limit: int = 20,
        offset: int = 0,
        min_score: Optional[float] = None,
        sort: str = "created_at",
    ) -> tuple[List[Dict[str, Any]], int]:
        """
        Get prediction history with pagination.
        Returns: (predictions, total_count)
        """
        where_clauses = []
        params = []

        if address:
            where_clauses.append("address = %s")
            params.append(address.lower())

        if min_score is not None:
            where_clauses.append("phishing_score >= %s")
            params.append(min_score)

        where_clause = " AND ".join(where_clauses) if where_clauses else "1=1"

        # Get total count
        count_query = f"SELECT COUNT(*) FROM predictions WHERE {where_clause}"
        try:
            total = self.fetch_one(count_query, tuple(params))[0]
        except:
            total = 0

        # Get paginated results
        query = f"""
        SELECT id, address, phishing_score, is_phishing, model_version,
               inference_mode, threshold, created_at
        FROM predictions
        WHERE {where_clause}
        ORDER BY {sort} DESC
        LIMIT %s OFFSET %s
        """
        params.extend([limit, offset])

        try:
            predictions = self.fetch_all(query, tuple(params))
            return predictions, total
        except Exception as e:
            logger.error(f"Error fetching predictions history: {e}")
            return [], 0

    def get_alerts(
        self,
        since: Optional[str] = None,
        min_score: Optional[float] = None,
        reviewed: Optional[bool] = None,
        limit: int = 20,
        offset: int = 0,
    ) -> tuple[List[Dict[str, Any]], int]:
        """
        Get alerts with filters and pagination.
        Returns: (alerts, total_count)
        """
        where_clauses = []
        params = []

        if since:
            where_clauses.append("created_at >= %s::timestamp")
            params.append(since)

        if min_score is not None:
            where_clauses.append("phishing_score >= %s")
            params.append(min_score)

        if reviewed is not None:
            where_clauses.append("reviewed = %s")
            params.append(reviewed)

        where_clause = " AND ".join(where_clauses) if where_clauses else "1=1"

        # Get total count
        count_query = f"SELECT COUNT(*) FROM alerts WHERE {where_clause}"
        try:
            total = self.fetch_one(count_query, tuple(params))[0]
        except:
            total = 0

        # Get paginated results
        query = f"""
        SELECT id, address, phishing_score, alert_type, severity, message,
               created_at, reviewed, reviewed_at, notes
        FROM alerts
        WHERE {where_clause}
        ORDER BY created_at DESC
        LIMIT %s OFFSET %s
        """
        params.extend([limit, offset])

        try:
            alerts = self.fetch_all(query, tuple(params))
            return alerts, total
        except Exception as e:
            logger.error(f"Error fetching alerts: {e}")
            return [], 0

    def get_dashboard_summary(self) -> Dict[str, Any]:
        """Get dashboard summary statistics."""
        try:
            total_preds = self.fetch_one(
                "SELECT COUNT(*) as count FROM predictions"
            )["count"]
            phishing_detected = self.fetch_one(
                "SELECT COUNT(*) as count FROM predictions WHERE is_phishing = true"
            )["count"]
            total_alerts = self.fetch_one(
                "SELECT COUNT(*) as count FROM alerts"
            )["count"]
            unreviewed_alerts = self.fetch_one(
                "SELECT COUNT(*) as count FROM alerts WHERE reviewed = false"
            )["count"]
            high_risk_alerts = self.fetch_one(
                "SELECT COUNT(*) as count FROM alerts WHERE severity = 'CRITICAL' OR severity = 'HIGH'"
            )["count"]
            avg_score = self.fetch_one(
                "SELECT AVG(phishing_score) as avg FROM predictions"
            )["avg"] or 0.0
            last_24h = self.fetch_one(
                "SELECT COUNT(*) as count FROM predictions WHERE created_at > NOW() - INTERVAL '24 hours'"
            )["count"]
            alerts_24h = self.fetch_one(
                "SELECT COUNT(*) as count FROM alerts WHERE created_at > NOW() - INTERVAL '24 hours'"
            )["count"]

            return {
                "total_predictions": total_preds,
                "total_phishing_detected": phishing_detected,
                "phishing_detection_rate": (phishing_detected / total_preds * 100) if total_preds > 0 else 0.0,
                "total_alerts": total_alerts,
                "unreviewed_alerts": unreviewed_alerts,
                "high_risk_alerts": high_risk_alerts,
                "avg_phishing_score": float(avg_score),
                "predictions_last_24h": last_24h,
                "alerts_last_24h": alerts_24h,
            }
        except Exception as e:
            logger.error(f"Error getting dashboard summary: {e}")
            return {}


def get_db() -> Database:
    """Get database instance."""
    return Database()

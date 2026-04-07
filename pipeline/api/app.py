"""
Phase 5.4: FastAPI Alert REST API

Endpoints:
    GET  /alerts          — List recent phishing alerts
    GET  /alerts/{address} — Get detection results for a specific address
    POST /alerts/score    — Score addresses on-demand
    POST /feedback        — Submit analyst feedback on alerts
    GET  /model/info      — Get current model metadata
    GET  /health          — Health check

Usage:
    uvicorn app:app --host 0.0.0.0 --port 8000
"""

import os
import logging
from datetime import datetime, timedelta
from typing import Optional

from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel
import psycopg2
from psycopg2.extras import RealDictCursor

import sys
sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
from config import DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD, API_PORT

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="Ethereum Phishing Detection API",
    description="REST API for GNN-based Ethereum phishing detection alerts",
    version="1.0.0",
)


# ──────────────────────────────────────────────
# Database connection
# ──────────────────────────────────────────────

def get_db():
    """Get a PostgreSQL database connection."""
    return psycopg2.connect(
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME,
        user=DB_USER, password=DB_PASSWORD,
    )


# ──────────────────────────────────────────────
# Request/Response models
# ──────────────────────────────────────────────

class ScoreRequest(BaseModel):
    addresses: list[str]

class ScoreResult(BaseModel):
    address: str
    phishing_score: float
    predicted_label: int
    known: bool

class FeedbackRequest(BaseModel):
    alert_id: int
    analyst_label: int  # 0 = legitimate, 1 = confirmed phishing

class AlertResponse(BaseModel):
    id: int
    address: str
    phishing_score: float
    predicted_label: int
    source: str
    detected_at: str
    model_version: Optional[str] = None


# ──────────────────────────────────────────────
# Endpoints
# ──────────────────────────────────────────────

@app.get("/health")
def health_check():
    """Health check endpoint."""
    try:
        conn = get_db()
        conn.close()
        return {"status": "healthy", "database": "connected"}
    except Exception as e:
        return {"status": "unhealthy", "database": str(e)}


@app.get("/alerts", response_model=list[AlertResponse])
def get_alerts(
    since: Optional[str] = Query(None, description="ISO timestamp, e.g. 2026-04-01T00:00:00"),
    limit: int = Query(50, ge=1, le=500),
    min_score: float = Query(0.0, ge=0.0, le=1.0),
):
    """
    List recent phishing detection alerts.

    Filters by timestamp and minimum phishing score.
    """
    conn = get_db()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            query = """
                SELECT id, address, phishing_score, predicted_label,
                       source, model_version, detected_at
                FROM detection_results
                WHERE phishing_score >= %s
            """
            params = [min_score]

            if since:
                query += " AND detected_at >= %s"
                params.append(since)

            query += " ORDER BY detected_at DESC LIMIT %s"
            params.append(limit)

            cur.execute(query, params)
            rows = cur.fetchall()

            return [
                AlertResponse(
                    id=r["id"],
                    address=r["address"],
                    phishing_score=r["phishing_score"],
                    predicted_label=r["predicted_label"],
                    source=r["source"],
                    model_version=r.get("model_version"),
                    detected_at=r["detected_at"].isoformat(),
                )
                for r in rows
            ]
    finally:
        conn.close()


@app.get("/alerts/{address}", response_model=list[AlertResponse])
def get_alerts_by_address(address: str):
    """Get all detection results for a specific Ethereum address."""
    conn = get_db()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT id, address, phishing_score, predicted_label,
                       source, model_version, detected_at
                FROM detection_results
                WHERE address = %s
                ORDER BY detected_at DESC
                """,
                (address.lower(),),
            )
            rows = cur.fetchall()
            if not rows:
                raise HTTPException(status_code=404, detail="Address not found")

            return [
                AlertResponse(
                    id=r["id"],
                    address=r["address"],
                    phishing_score=r["phishing_score"],
                    predicted_label=r["predicted_label"],
                    source=r["source"],
                    model_version=r.get("model_version"),
                    detected_at=r["detected_at"].isoformat(),
                )
                for r in rows
            ]
    finally:
        conn.close()


@app.post("/alerts/score", response_model=list[ScoreResult])
def score_addresses(request: ScoreRequest):
    """
    Score a list of Ethereum addresses for phishing probability.

    Uses the loaded GNN model for inference.
    """
    from inference.inference_service import PhishingInferenceService

    model_path = os.getenv("MODEL_LOCAL_PATH", "/tmp/graphsage_phishing.pt")
    data_dir = os.getenv("GRAPH_DATA_DIR", "/tmp/graph_data")

    service = PhishingInferenceService(model_path, data_dir)
    results = service.score_addresses(request.addresses)

    return [ScoreResult(**r) for r in results]


@app.post("/feedback")
def submit_feedback(feedback: FeedbackRequest):
    """
    Submit analyst feedback on a phishing alert.

    Used for active learning — confirmed labels are fed back
    into the next training cycle.
    """
    if feedback.analyst_label not in (0, 1):
        raise HTTPException(status_code=400, detail="analyst_label must be 0 or 1")

    conn = get_db()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE phishing_alerts
                SET reviewed = TRUE, analyst_label = %s
                WHERE id = %s
                RETURNING id
                """,
                (feedback.analyst_label, feedback.alert_id),
            )
            if cur.fetchone() is None:
                raise HTTPException(status_code=404, detail="Alert not found")

            # Also insert confirmed label into phishing_labels for retraining
            cur.execute(
                """
                SELECT address FROM phishing_alerts WHERE id = %s
                """,
                (feedback.alert_id,),
            )
            row = cur.fetchone()
            if row:
                cur.execute(
                    """
                    INSERT INTO phishing_labels (address, label, source)
                    VALUES (%s, %s, 'analyst_feedback')
                    ON CONFLICT (address) DO UPDATE SET label = EXCLUDED.label
                    """,
                    (row[0], feedback.analyst_label),
                )

            conn.commit()
            return {"status": "ok", "alert_id": feedback.alert_id}
    finally:
        conn.close()


@app.get("/model/info")
def model_info():
    """Get metadata about the currently loaded model."""
    conn = get_db()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                "SELECT * FROM model_registry WHERE is_active = TRUE LIMIT 1"
            )
            row = cur.fetchone()
            if row:
                return dict(row)
            return {"message": "No active model registered"}
    finally:
        conn.close()


@app.get("/stats")
def get_stats():
    """Get summary statistics for the dashboard."""
    conn = get_db()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            stats = {}

            # Total detections
            cur.execute("SELECT COUNT(*) as total FROM detection_results")
            stats["total_detections"] = cur.fetchone()["total"]

            # Phishing flagged
            cur.execute(
                "SELECT COUNT(*) as total FROM detection_results WHERE predicted_label = 1"
            )
            stats["total_phishing_flagged"] = cur.fetchone()["total"]

            # Last 24h alerts
            cur.execute(
                """
                SELECT COUNT(*) as total FROM detection_results
                WHERE detected_at >= NOW() - INTERVAL '24 hours'
                AND predicted_label = 1
                """
            )
            stats["alerts_last_24h"] = cur.fetchone()["total"]

            # Pending reviews
            cur.execute(
                "SELECT COUNT(*) as total FROM phishing_alerts WHERE reviewed = FALSE"
            )
            stats["pending_reviews"] = cur.fetchone()["total"]

            return stats
    finally:
        conn.close()


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=API_PORT)

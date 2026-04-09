"""
Data ingestion endpoints.
"""
import logging
import uuid
from datetime import datetime
from fastapi import APIRouter, Request, BackgroundTasks
from api.models import IngestMockRequest, IngestMockResponse
from api.database import get_db

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/ingest", tags=["ingestion"])

# In-memory tracking of ingestion tasks (for demo purposes)
_ingestion_tasks = {}


@router.post("/mock-transactions", response_model=IngestMockResponse)
async def ingest_mock_transactions(
    request: Request,
    ingest_request: IngestMockRequest,
    background_tasks: BackgroundTasks,
) -> IngestMockResponse:
    """
    Ingest mock transaction data into the system.
    Can be run as background task or synchronously.

    Request parameters:
    - num_transactions: Total number of mock transactions to generate (1-10000)
    - num_phishing_addresses: Number of phishing addresses to include
    - background: Run as background task (default: true)
    """
    task_id = str(uuid.uuid4())

    # Track task status
    _ingestion_tasks[task_id] = {
        "status": "queued",
        "created_at": datetime.utcnow(),
        "num_transactions": ingest_request.num_transactions,
        "num_phishing_addresses": ingest_request.num_phishing_addresses,
    }

    if ingest_request.background:
        # Run as background task
        background_tasks.add_task(
            _generate_and_ingest_mock_data,
            task_id,
            ingest_request.num_transactions,
            ingest_request.num_phishing_addresses,
        )
        status = "queued"
        message = "Mock data ingestion queued"
    else:
        # Run synchronously
        try:
            _generate_and_ingest_mock_data(
                task_id,
                ingest_request.num_transactions,
                ingest_request.num_phishing_addresses,
            )
            status = "completed"
            message = "Mock data ingestion completed"
        except Exception as e:
            logger.error(f"Mock data ingestion error: {e}")
            status = "failed"
            message = f"Mock data ingestion failed: {e}"

    return IngestMockResponse(
        task_id=task_id,
        status=status,
        message=message,
        num_transactions=ingest_request.num_transactions,
        num_phishing_addresses=ingest_request.num_phishing_addresses,
        created_at=datetime.utcnow(),
    )


def _generate_and_ingest_mock_data(
    task_id: str,
    num_transactions: int,
    num_phishing_addresses: int,
) -> None:
    """Generate and ingest mock transaction data."""
    import random

    logger.info(f"Starting mock data ingestion task {task_id}")
    _ingestion_tasks[task_id]["status"] = "processing"

    try:
        db = get_db()

        # Generate mock addresses
        all_addresses = [
            "0x" + "".join(random.choices("0123456789abcdef", k=40))
            for _ in range(num_phishing_addresses + 100)
        ]
        phishing_addresses = set(all_addresses[:num_phishing_addresses])

        # Insert mock predictions
        for i in range(num_transactions):
            address = random.choice(all_addresses)
            is_phishing = address in phishing_addresses
            phishing_score = random.uniform(0.7, 0.99) if is_phishing else random.uniform(0.0, 0.3)

            db.ensure_address(address)
            db.log_prediction(
                address=address,
                phishing_score=phishing_score,
                is_phishing=is_phishing,
                model_version="mock-1.0.0",
                inference_mode="mock",
                threshold=0.5,
                inference_time_ms=random.uniform(10, 100),
            )

            # Create alerts for high-score predictions
            if phishing_score >= 0.8:
                db.create_alert(
                    address=address,
                    phishing_score=phishing_score,
                    alert_type="THRESHOLD_EXCEEDED",
                    severity="CRITICAL" if phishing_score >= 0.9 else "HIGH",
                    message=f"Mock alert for address with score {phishing_score:.2%}",
                )

        logger.info(f"Mock data ingestion task {task_id} completed")
        _ingestion_tasks[task_id]["status"] = "completed"

    except Exception as e:
        logger.error(f"Mock data ingestion task {task_id} failed: {e}")
        _ingestion_tasks[task_id]["status"] = "failed"
        _ingestion_tasks[task_id]["error"] = str(e)

"""
Prediction endpoints.
"""
import logging
from datetime import datetime
from fastapi import APIRouter, Request, HTTPException
from api.models import (
    PredictAddressRequest,
    PredictAddressResponse,
    PredictBatchRequest,
    PredictBatchResponse,
    BatchPrediction,
    RiskFactor,
)
from api.services.predictor import get_predictor
from api.services.prediction_logger import get_prediction_logger
from api.config import settings

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/predict", tags=["predictions"])


@router.post("/address", response_model=PredictAddressResponse)
async def predict_address(
    request: Request,
    pred_request: PredictAddressRequest,
) -> PredictAddressResponse:
    """
    Predict phishing probability for a single Ethereum address.
    """
    correlation_id = getattr(request.state, "correlation_id", "unknown")
    address = pred_request.address

    try:
        # Get predictor and make prediction
        predictor = get_predictor()
        pred_result = predictor.predict(address)

        # Determine threshold to use
        threshold = (
            pred_request.threshold_override
            if pred_request.threshold_override is not None
            else settings.PHISHING_SCORE_THRESHOLD
        )

        # Determine if phishing based on threshold
        is_phishing = pred_result["phishing_score"] >= threshold

        # Log prediction to database
        logger_svc = get_prediction_logger()
        logger_svc.ensure_address(address)
        prediction_id = logger_svc.log_prediction(
            address=address,
            phishing_score=pred_result["phishing_score"],
            is_phishing=is_phishing,
            model_version=pred_result["model_version"],
            inference_mode=settings.INFERENCE_MODE,
            threshold=threshold,
            inference_time_ms=pred_result["inference_time_ms"],
        )

        # Create alert if score is high enough
        if logger_svc.should_create_alert(pred_result["phishing_score"]):
            severity = logger_svc.get_alert_severity(pred_result["phishing_score"])
            logger_svc.create_alert(
                address=address,
                phishing_score=pred_result["phishing_score"],
                alert_type="THRESHOLD_EXCEEDED",
                severity=severity,
                message=f"Address flagged with phishing score {pred_result['phishing_score']:.2%}",
            )

        # Build response. Different predictors return risk_factors either as
        # a list of dicts or a list of strings — normalize here.
        risk_factors = None
        if pred_request.include_risk_factors and pred_result.get("risk_factors"):
            risk_factors = []
            for rf in pred_result["risk_factors"]:
                if isinstance(rf, dict):
                    risk_factors.append(
                        RiskFactor(
                            name=rf.get("name", "unknown"),
                            value=float(rf.get("value", 0.0)),
                            description=rf.get("description"),
                        )
                    )
                else:
                    risk_factors.append(
                        RiskFactor(
                            name=str(rf),
                            value=float(pred_result["phishing_score"]),
                            description=None,
                        )
                    )

        return PredictAddressResponse(
            address=address,
            phishing_score=pred_result["phishing_score"],
            is_phishing=is_phishing,
            confidence=pred_result["confidence"],
            risk_factors=risk_factors,
            model_version=pred_result["model_version"],
            inference_time_ms=pred_result["inference_time_ms"],
            timestamp=datetime.utcnow(),
            correlation_id=correlation_id,
        )

    except ValueError as e:
        logger.error(f"Validation error: {e}")
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Prediction error for {address}: {e}")
        raise HTTPException(status_code=500, detail="Prediction failed")


@router.post("/batch", response_model=PredictBatchResponse)
async def predict_batch(
    request: Request,
    batch_request: PredictBatchRequest,
) -> PredictBatchResponse:
    """
    Predict phishing probability for multiple Ethereum addresses.
    Maximum 100 addresses per request.
    """
    correlation_id = getattr(request.state, "correlation_id", "unknown")
    addresses = batch_request.addresses

    if len(addresses) > settings.MAX_BATCH_SIZE:
        raise HTTPException(
            status_code=400,
            detail=f"Batch size exceeds maximum of {settings.MAX_BATCH_SIZE}",
        )

    try:
        predictor = get_predictor()
        logger_svc = get_prediction_logger()

        predictions = []
        total_inference_ms = 0.0

        for address in addresses:
            # Get prediction
            pred_result = predictor.predict(address)
            total_inference_ms += float(pred_result.get("inference_time_ms", 0.0))
            is_phishing = pred_result["phishing_score"] >= settings.PHISHING_SCORE_THRESHOLD

            # Log prediction
            logger_svc.ensure_address(address)
            logger_svc.log_prediction(
                address=address,
                phishing_score=pred_result["phishing_score"],
                is_phishing=is_phishing,
                model_version=pred_result["model_version"],
                inference_mode=settings.INFERENCE_MODE,
                threshold=settings.PHISHING_SCORE_THRESHOLD,
                inference_time_ms=pred_result["inference_time_ms"],
            )

            # Create alert if needed
            if logger_svc.should_create_alert(pred_result["phishing_score"]):
                severity = logger_svc.get_alert_severity(pred_result["phishing_score"])
                logger_svc.create_alert(
                    address=address,
                    phishing_score=pred_result["phishing_score"],
                    alert_type="THRESHOLD_EXCEEDED",
                    severity=severity,
                )

            # Build batch prediction — same dict/str normalization as /predict/address
            risk_factors = None
            if batch_request.include_risk_factors and pred_result.get("risk_factors"):
                risk_factors = []
                for rf in pred_result["risk_factors"]:
                    if isinstance(rf, dict):
                        risk_factors.append(
                            RiskFactor(
                                name=rf.get("name", "unknown"),
                                value=float(rf.get("value", 0.0)),
                                description=rf.get("description"),
                            )
                        )
                    else:
                        risk_factors.append(
                            RiskFactor(
                                name=str(rf),
                                value=float(pred_result["phishing_score"]),
                                description=None,
                            )
                        )

            predictions.append(
                BatchPrediction(
                    address=address,
                    phishing_score=pred_result["phishing_score"],
                    is_phishing=is_phishing,
                    confidence=pred_result["confidence"],
                    risk_factors=risk_factors,
                )
            )

        return PredictBatchResponse(
            predictions=predictions,
            batch_size=len(addresses),
            processing_time_ms=total_inference_ms,
            timestamp=datetime.utcnow(),
            correlation_id=correlation_id,
        )

    except Exception as e:
        logger.error(f"Batch prediction error: {e}")
        raise HTTPException(status_code=500, detail="Batch prediction failed")

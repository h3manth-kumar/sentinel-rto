"""POST /v1/risk/evaluate — Real-time checkout risk scoring endpoint.

Full pipeline:
1. Burst check (H3 + device) — concurrent
2. Redis feature store lookup (pipeline MGET)
3. Feature vector construction
4. ONNX Runtime LightGBM inference
5. Policy evaluation with burst override
6. Async Kafka event emission
7. Return scored decision with SHAP explanation
"""

from __future__ import annotations

import asyncio
import logging
import time
import uuid
from typing import Any

from fastapi import APIRouter, Depends

from src.api.circuit_breaker import CircuitBreaker, CircuitBreakerOpenException
from src.api.dependencies import (
    get_burst_limiter,
    get_feature_store,
    get_inference_engine,
    get_kafka_producer,
)
from src.api.schemas import (
    ActionResponse,
    ExecutionMetrics,
    ExplanationResponse,
    RiskEvaluateRequest,
    RiskEvaluateResponse,
    ShapContributor,
)
from src.ml.feature_engineering import FeatureEngineer
from src.ml.policy_evaluator import PolicyEvaluator
from src.graph.h3_spatial import H3SpatialEngine

logger = logging.getLogger(__name__)

risk_router = APIRouter(prefix="/v1/risk", tags=["Risk"])
circuit_breaker = CircuitBreaker()
feature_engineer = FeatureEngineer()
policy_evaluator = PolicyEvaluator()


def _fallback_response(
    evaluation_id: str,
    order_id: str,
    reason: str,
    latency_ms: float,
) -> RiskEvaluateResponse:
    """Generate a FAIL_SAFE_ALLOW response when services are degraded."""
    return RiskEvaluateResponse(
        evaluation_id=evaluation_id,
        order_id=order_id,
        risk_score=25,
        risk_tier="ALLOW",
        action=ActionResponse(
            type="FAIL_SAFE_ALLOW",
            deposit_amount_in_paise=None,
            reason_code=reason,
        ),
        explanation=ExplanationResponse(
            shap_contributors=[],
            syndicate_detected=False,
        ),
        execution_metrics=ExecutionMetrics(
            total_latency_ms=round(latency_ms, 2),
            redis_lookup_ms=0.0,
            onnx_inference_ms=0.0,
        ),
    )


@risk_router.post("/evaluate", response_model=RiskEvaluateResponse)
async def evaluate_risk(
    request: RiskEvaluateRequest,
    feature_store: Any = Depends(get_feature_store),
    burst_limiter: Any = Depends(get_burst_limiter),
    inference_engine: Any = Depends(get_inference_engine),
    kafka_producer: Any = Depends(get_kafka_producer),
) -> RiskEvaluateResponse:
    """Evaluate checkout risk in real-time (<50ms P99 SLA)."""
    start_time = time.monotonic()
    evaluation_id = f"eval_{uuid.uuid4().hex[:12]}"
    h3_index = request.shipping_address.h3_index_res9 or "unknown"

    if h3_index == "unknown":
        try:
            engine = H3SpatialEngine()
            result = engine.resolve(pincode=request.shipping_address.pincode)
            if result.h3_index_res9:
                h3_index = result.h3_index_res9
        except Exception as e:
            logger.warning("H3 spatial conversion failed: %s", e)

    try:
        # --- Step 1: Burst velocity check (concurrent) ---
        burst_action_str: str | None = None
        if burst_limiter is not None:
            try:
                h3_result, device_result = await asyncio.gather(
                    burst_limiter.check_h3_burst(h3_index, request.order_id),
                    burst_limiter.check_device_burst(
                        request.device.fingerprint_hash, request.order_id
                    ),
                )
                # Take the more severe burst action
                if h3_result.action.value != "ALLOW":
                    burst_action_str = h3_result.action.value
                if device_result.action.value != "ALLOW":
                    burst_action_str = device_result.action.value
            except Exception as e:
                logger.warning("Burst check failed: %s", e)

        # --- Step 2: Redis feature store lookup ---
        redis_start = time.monotonic()
        redis_raw: dict[str, Any] = {}
        if feature_store is not None:
            try:
                redis_raw = await feature_store.get_all_entity_features(
                    device_hash=request.device.fingerprint_hash,
                    h3_index_res9=h3_index,
                    phone_hash=request.customer.phone_hash,
                )
            except Exception as e:
                logger.warning("Redis feature lookup failed: %s", e)
        redis_lookup_ms = (time.monotonic() - redis_start) * 1000

        # Flatten nested Redis features for feature engineering
        flat_features: dict[str, Any] = {}
        device_data = redis_raw.get("device", {})
        h3_data = redis_raw.get("h3", {})
        cluster_data = redis_raw.get("cluster", {})
        phone_data = redis_raw.get("phone", {})
        flat_features["device_rto_rate"] = device_data.get("rto_rate", 0.0)
        flat_features["device_order_count"] = device_data.get("order_count", 0)
        flat_features["h3_cluster_rto_rate"] = h3_data.get("cluster_rto_rate", 0.0)
        flat_features["h3_density_weight"] = h3_data.get("density_weight", 0.0)
        flat_features["cluster_size"] = cluster_data.get("size", 0)
        flat_features["cluster_rto_rate"] = cluster_data.get("rto_rate", 0.0)
        flat_features["phone_rto_rate"] = phone_data.get("rto_rate", 0.0)
        flat_features["phone_order_count"] = phone_data.get("order_count", 0)
        flat_features["burst_count_h3"] = 0
        flat_features["burst_count_device"] = 0

        # --- Step 3: Build feature vector ---
        feature_vector = feature_engineer.build_feature_vector(request, flat_features)

        # --- Step 4: ONNX inference with circuit breaker ---
        onnx_start = time.monotonic()
        risk_score = 25  # fallback
        if inference_engine is not None:
            try:
                async with circuit_breaker:
                    result = await inference_engine.predict(feature_vector)
                risk_score = result.get("risk_score", 25)
            except CircuitBreakerOpenException:
                logger.warning("Circuit breaker OPEN — using fallback score.")
            except Exception as e:
                logger.warning("ONNX inference failed: %s — using fallback.", e)
        onnx_inference_ms = (time.monotonic() - onnx_start) * 1000

        # --- Step 5: Policy evaluation ---
        decision = policy_evaluator.evaluate(risk_score, burst_action_str, {})

        # --- Step 6: SHAP explanation (mock top-3 from feature weights) ---
        feature_names = feature_engineer.get_feature_names()
        # Simple importance proxy: normalized feature values
        feature_vals = feature_vector.tolist()
        shap_pairs = sorted(
            zip(feature_names, feature_vals),
            key=lambda x: abs(x[1]),
            reverse=True,
        )[:3]
        shap_contributors = [
            ShapContributor(feature=name, weight=round(val, 4))
            for name, val in shap_pairs
        ]

        # --- Step 7: Async Kafka emission (fire-and-forget) ---
        if kafka_producer is not None:
            try:
                from src.kafka.schemas import OrderEvent

                event = OrderEvent(
                    merchant_id=request.merchant_id,
                    order_id=request.order_id,
                    timestamp=request.timestamp,
                    amount_in_paise=request.amount_in_paise,
                    payment_method=request.payment_method,
                    customer_phone_hash=request.customer.phone_hash,
                    device_fingerprint_hash=request.device.fingerprint_hash,
                    h3_index_res9=h3_index,
                    pincode=request.shipping_address.pincode,
                    risk_score=risk_score,
                    risk_tier=decision.risk_tier,
                    status="PENDING",
                )
                asyncio.create_task(kafka_producer.send_order_event(event))
            except Exception as e:
                logger.warning("Kafka emission failed: %s", e)

        total_latency_ms = (time.monotonic() - start_time) * 1000

        return RiskEvaluateResponse(
            evaluation_id=evaluation_id,
            order_id=request.order_id,
            risk_score=risk_score,
            risk_tier=decision.risk_tier,
            action=ActionResponse(
                type=decision.action_type,
                deposit_amount_in_paise=decision.deposit_amount_in_paise,
                reason_code=decision.reason_code,
            ),
            explanation=ExplanationResponse(
                shap_contributors=shap_contributors,
                syndicate_detected=False,
            ),
            execution_metrics=ExecutionMetrics(
                total_latency_ms=round(total_latency_ms, 2),
                redis_lookup_ms=round(redis_lookup_ms, 2),
                onnx_inference_ms=round(onnx_inference_ms, 2),
            ),
        )

    except Exception as e:
        total_latency_ms = (time.monotonic() - start_time) * 1000
        logger.exception("Risk evaluation failed: %s", e)
        return _fallback_response(
            evaluation_id, request.order_id, "INTERNAL_ERROR", total_latency_ms
        )

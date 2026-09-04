"""Ekart Logistics 3PL Webhook Ingestion Adapter.

Normalizes Ekart fulfillment tracking events for automated Bayesian delivery feedback.
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

from src.integrations.logistics.delhivery_adapter import CourierOutcome, NormalizedDeliveryEvent

logger = logging.getLogger(__name__)


class EkartAdapter:
    """Ingestion adapter for Ekart Logistics Webhooks."""

    PROVIDER_NAME = "EKART"

    STATUS_MAP = {
        "DELIVERED": CourierOutcome.DELIVERED,
        "COMPLETED": CourierOutcome.DELIVERED,
        "RETURNED_TO_SELLER": CourierOutcome.RTO,
        "RTO": CourierOutcome.RTO,
        "RTO_DELIVERED": CourierOutcome.RTO,
        "IN_TRANSIT": CourierOutcome.IN_TRANSIT,
        "DISPATCHED": CourierOutcome.IN_TRANSIT,
        "OUT_FOR_DELIVERY": CourierOutcome.IN_TRANSIT,
        "CANCELLED": CourierOutcome.CANCELLED,
        "UNDELIVERED": CourierOutcome.UNDELIVERED_ATTEMPT,
        "DELIVERY_ATTEMPTED": CourierOutcome.UNDELIVERED_ATTEMPT,
    }

    @classmethod
    def parse_webhook_payload(cls, payload: dict[str, Any]) -> NormalizedDeliveryEvent:
        """Parse raw Ekart tracking webhook payload."""
        tracking_id = str(payload.get("tracking_id", payload.get("merchant_reference_id", "")))
        order_id = str(payload.get("merchant_order_id", payload.get("order_id", f"ord_{tracking_id}")))
        status = str(payload.get("status", "IN_TRANSIT")).upper()
        pincode = str(payload.get("destination_pincode", payload.get("pincode", "560100")))
        city = payload.get("destination_city", payload.get("city", "Bengaluru"))
        reason = payload.get("failure_reason", payload.get("ndr_comment"))

        outcome = cls.STATUS_MAP.get(status, CourierOutcome.IN_TRANSIT)
        if "RETURN" in status or "RTO" in status:
            outcome = CourierOutcome.RTO

        loss_inr = 110.0 if str(pincode).startswith("560") else (160.0 if str(pincode).startswith(("57", "60", "50", "40")) else 230.0)

        ts_raw = payload.get("timestamp", datetime.now(timezone.utc).isoformat())
        try:
            timestamp = datetime.fromisoformat(ts_raw.replace("Z", "+00:00"))
        except Exception:
            timestamp = datetime.now(timezone.utc)

        logger.info("Parsed Ekart Webhook: order=%s, tracking=%s, outcome=%s", order_id, tracking_id, outcome.value)

        return NormalizedDeliveryEvent(
            provider=cls.PROVIDER_NAME,
            waybill=tracking_id,
            order_id=order_id,
            outcome=outcome,
            raw_status=status,
            ndr_reason_code=reason,
            timestamp=timestamp,
            location_city=city,
            pincode=pincode,
            courier_loss_inr=loss_inr,
            metadata=payload,
        )

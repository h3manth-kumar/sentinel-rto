"""Xpressbees 3PL Webhook Ingestion Adapter.

Normalizes Xpressbees tracking status events (DEL, RTO-REC, UNDEL) for Bayesian ML feedback.
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

from src.integrations.logistics.delhivery_adapter import CourierOutcome, NormalizedDeliveryEvent

logger = logging.getLogger(__name__)


class XpressbeesAdapter:
    """Ingestion adapter for Xpressbees Logistics Webhooks."""

    PROVIDER_NAME = "XPRESSBEES"

    STATUS_MAP = {
        "DEL": CourierOutcome.DELIVERED,
        "DELIVERED": CourierOutcome.DELIVERED,
        "RTO": CourierOutcome.RTO,
        "RTO-REC": CourierOutcome.RTO,
        "RTO-INT": CourierOutcome.RTO,
        "RTO_RETURNED": CourierOutcome.RTO,
        "INTRANSIT": CourierOutcome.IN_TRANSIT,
        "OFD": CourierOutcome.IN_TRANSIT,
        "OUT_FOR_DELIVERY": CourierOutcome.IN_TRANSIT,
        "CAN": CourierOutcome.CANCELLED,
        "UNDEL": CourierOutcome.UNDELIVERED_ATTEMPT,
        "UNDELIVERED": CourierOutcome.UNDELIVERED_ATTEMPT,
    }

    @classmethod
    def parse_webhook_payload(cls, payload: dict[str, Any]) -> NormalizedDeliveryEvent:
        """Parse raw Xpressbees manifest webhook payload."""
        awb = str(payload.get("awb_number", payload.get("awb", "")))
        order_id = str(payload.get("order_number", payload.get("order_id", f"ord_{awb}")))
        status_code = str(payload.get("status_code", payload.get("status", "INTRANSIT"))).upper()
        pincode = str(payload.get("destination_pincode", payload.get("pincode", "560100")))
        city = payload.get("current_location", payload.get("city", "Bengaluru"))
        reason = payload.get("reason_code", payload.get("remarks"))

        outcome = cls.STATUS_MAP.get(status_code, CourierOutcome.IN_TRANSIT)
        if "RTO" in status_code:
            outcome = CourierOutcome.RTO

        loss_inr = 110.0 if str(pincode).startswith("560") else (160.0 if str(pincode).startswith(("57", "60", "50", "40")) else 230.0)

        ts_raw = payload.get("event_time", datetime.now(timezone.utc).isoformat())
        try:
            timestamp = datetime.fromisoformat(ts_raw.replace("Z", "+00:00"))
        except Exception:
            timestamp = datetime.now(timezone.utc)

        logger.info("Parsed Xpressbees Webhook: order=%s, awb=%s, outcome=%s", order_id, awb, outcome.value)

        return NormalizedDeliveryEvent(
            provider=cls.PROVIDER_NAME,
            waybill=awb,
            order_id=order_id,
            outcome=outcome,
            raw_status=status_code,
            ndr_reason_code=reason,
            timestamp=timestamp,
            location_city=city,
            pincode=pincode,
            courier_loss_inr=loss_inr,
            metadata=payload,
        )

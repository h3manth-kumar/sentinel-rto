"""Delhivery 3PL Webhook Ingestion Adapter.

Parses unified tracking push events and Non-Delivery Report (NDR) codes from Delhivery
and normalizes them for the Bayesian delivery feedback engine.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Optional

logger = logging.getLogger(__name__)


class CourierOutcome(str, Enum):
    DELIVERED = "DELIVERED"
    RTO = "RTO"
    IN_TRANSIT = "IN_TRANSIT"
    CANCELLED = "CANCELLED"
    UNDELIVERED_ATTEMPT = "UNDELIVERED_ATTEMPT"


@dataclass
class NormalizedDeliveryEvent:
    """Universal normalized delivery event for the Bayesian learning engine."""
    provider: str
    waybill: str
    order_id: str
    outcome: CourierOutcome
    raw_status: str
    ndr_reason_code: Optional[str]
    timestamp: datetime
    location_city: str
    pincode: str
    courier_loss_inr: float
    metadata: dict[str, Any]


class DelhiveryAdapter:
    """Ingestion adapter for Delhivery Unified Logistics Webhooks."""

    PROVIDER_NAME = "DELHIVERY"

    # Delhivery status mapping
    STATUS_MAP = {
        "Delivered": CourierOutcome.DELIVERED,
        "DL": CourierOutcome.DELIVERED,
        "RTO": CourierOutcome.RTO,
        "RT": CourierOutcome.RTO,
        "Returned": CourierOutcome.RTO,
        "In Transit": CourierOutcome.IN_TRANSIT,
        "IT": CourierOutcome.IN_TRANSIT,
        "Dispatched": CourierOutcome.IN_TRANSIT,
        "Cancelled": CourierOutcome.CANCELLED,
        "UD": CourierOutcome.UNDELIVERED_ATTEMPT,
        "Undelivered": CourierOutcome.UNDELIVERED_ATTEMPT,
    }

    @classmethod
    def parse_webhook_payload(cls, payload: dict[str, Any]) -> NormalizedDeliveryEvent:
        """Parse raw Delhivery JSON webhook into normalized delivery event."""
        # Handle single event or nested Shipment structure
        shipment = payload.get("Shipment", payload)
        raw_status = shipment.get("Status", {}).get("Status", shipment.get("status", "Unknown"))
        status_type = shipment.get("Status", {}).get("StatusType", raw_status)

        waybill = str(shipment.get("AWB", shipment.get("waybill", "")))
        order_id = str(shipment.get("ReferenceNo", shipment.get("order_id", f"ord_{waybill}")))
        pincode = str(shipment.get("Destination", shipment.get("pincode", "560100")))
        city = shipment.get("City", shipment.get("location", "Bengaluru"))

        # NDR specific codes (NDR01 = Buyer Refused, NDR02 = Incorrect Address)
        ndr_code = shipment.get("Status", {}).get("Instructions", shipment.get("ndr_code"))

        outcome = cls.STATUS_MAP.get(status_type, cls.STATUS_MAP.get(raw_status, CourierOutcome.IN_TRANSIT))
        if "RTO" in str(raw_status).upper() or "RETURN" in str(raw_status).upper():
            outcome = CourierOutcome.RTO

        # Calculate courier loss penalty if RTO
        loss_inr = 110.0 if str(pincode).startswith("560") else (160.0 if str(pincode).startswith(("57", "60", "50", "40")) else 230.0)

        ts_raw = shipment.get("Status", {}).get("StatusDateTime", datetime.now(timezone.utc).isoformat())
        try:
            timestamp = datetime.fromisoformat(ts_raw.replace("Z", "+00:00"))
        except Exception:
            timestamp = datetime.now(timezone.utc)

        logger.info("Parsed Delhivery Webhook: order=%s, waybill=%s, outcome=%s", order_id, waybill, outcome.value)

        return NormalizedDeliveryEvent(
            provider=cls.PROVIDER_NAME,
            waybill=waybill,
            order_id=order_id,
            outcome=outcome,
            raw_status=raw_status,
            ndr_reason_code=ndr_code,
            timestamp=timestamp,
            location_city=city,
            pincode=pincode,
            courier_loss_inr=loss_inr,
            metadata=payload,
        )

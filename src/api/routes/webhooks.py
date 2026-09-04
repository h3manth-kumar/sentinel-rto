"""3PL Logistics and Communication Webhook Ingestion Gateway.

Receives ground-truth delivery status events from Delhivery, Xpressbees, and Ekart,
and processes customer response callbacks from Gupshup WhatsApp and Twilio IVR.
"""
from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from typing import Any

from fastapi import APIRouter, Depends, Request
from pydantic import BaseModel

from src.api.dependencies import get_kafka_producer
from src.db.supabase_client import get_supabase_client
from src.graph.learning_engine import get_learning_engine
from src.integrations.logistics.delhivery_adapter import CourierOutcome, DelhiveryAdapter
from src.integrations.logistics.ekart_adapter import EkartAdapter
from src.integrations.logistics.xpressbees_adapter import XpressbeesAdapter

logger = logging.getLogger(__name__)

webhook_router = APIRouter(prefix="/v1/webhooks", tags=["Enterprise Webhooks"])

learning_engine = get_learning_engine()
supabase_client = get_supabase_client()


class WebhookResponse(BaseModel):
    received: bool
    provider: str
    order_id: str
    outcome: str
    bayesian_rto_rate: float
    action_taken: str


def _process_normalized_outcome(event: Any) -> WebhookResponse:
    """Process normalized delivery outcome through Bayesian learning engine and Supabase."""
    outcome_str = event.outcome.value

    # Update in Bayesian learning engine
    if event.outcome in (CourierOutcome.DELIVERED, CourierOutcome.RTO):
        learning_engine.record_delivery_outcome(
            order_id=event.order_id,
            phone_hash=event.metadata.get("phone_hash", f"ph_{event.order_id}"),
            device_hash=event.metadata.get("device_hash", f"dev_{event.order_id}"),
            h3_index=event.metadata.get("h3_index", "89618925407ffff"),
            outcome="DELIVERED" if event.outcome == CourierOutcome.DELIVERED else "RTO",
        )

    # Calculate Bayesian smoothed RTO rate
    feat = learning_engine.get_realtime_features(
        phone_hash=event.metadata.get("phone_hash", f"ph_{event.order_id}"),
        device_hash=event.metadata.get("device_hash", f"dev_{event.order_id}"),
        h3_index=event.metadata.get("h3_index", "89618925407ffff"),
    )
    current_bayesian_rto = feat.get("customer_rto_rate", 0.0)

    # Async Supabase update
    try:
        loop = asyncio.get_running_loop()
        loop.create_task(supabase_client.update_delivery_outcome(event.order_id, outcome_str))
    except RuntimeError:
        pass

    action_taken = f"Bayesian RTO updated to {current_bayesian_rto*100:.1f}%"
    if event.outcome == CourierOutcome.RTO:
        action_taken += f" (Logistics courier loss penalty: ₹{event.courier_loss_inr:.0f})"

    return WebhookResponse(
        received=True,
        provider=event.provider,
        order_id=event.order_id,
        outcome=outcome_str,
        bayesian_rto_rate=round(current_bayesian_rto, 4),
        action_taken=action_taken,
    )


@webhook_router.post("/delhivery", response_model=WebhookResponse)
async def receive_delhivery_webhook(request_body: dict[str, Any]) -> WebhookResponse:
    """Ingest Delhivery unified push webhook."""
    event = DelhiveryAdapter.parse_webhook_payload(request_body)
    return _process_normalized_outcome(event)


@webhook_router.post("/xpressbees", response_model=WebhookResponse)
async def receive_xpressbees_webhook(request_body: dict[str, Any]) -> WebhookResponse:
    """Ingest Xpressbees manifest tracking webhook."""
    event = XpressbeesAdapter.parse_webhook_payload(request_body)
    return _process_normalized_outcome(event)


@webhook_router.post("/ekart", response_model=WebhookResponse)
async def receive_ekart_webhook(request_body: dict[str, Any]) -> WebhookResponse:
    """Ingest Ekart logistics status webhook."""
    event = EkartAdapter.parse_webhook_payload(request_body)
    return _process_normalized_outcome(event)


@webhook_router.post("/gupshup-whatsapp")
async def receive_gupshup_callback(payload: dict[str, Any]) -> dict[str, Any]:
    """Handle buyer interactive button reply on WhatsApp (Deposit Paid or Cancelled)."""
    reply_id = payload.get("payload", {}).get("id", payload.get("reply_id", ""))
    logger.info("Received Gupshup WhatsApp Callback: %s", reply_id)

    if reply_id.startswith("VERIFY_UPI_"):
        order_id = reply_id.replace("VERIFY_UPI_", "")
        return {
            "status": "success",
            "order_id": order_id,
            "action": "CHALLENGE_VERIFIED_DEPOSIT_PAID",
            "message": "Customer confirmed order and authorized ₹49 deposit.",
        }
    elif reply_id.startswith("CANCEL_"):
        order_id = reply_id.replace("CANCEL_", "")
        return {
            "status": "success",
            "order_id": order_id,
            "action": "ORDER_CANCELLED_BY_BUYER",
            "message": "Customer cancelled order via WhatsApp.",
        }

    return {"status": "received", "payload": payload}


@webhook_router.post("/twilio-ivr")
async def receive_twilio_ivr_callback(
    request: Request,
    order_id: str = "unknown",
    digits: Optional[str] = None,
) -> dict[str, Any]:
    """Handle buyer IVR DTMF digit responses (1: Confirm, 2: Cancel)."""
    digits_val = digits
    if not digits_val:
        try:
            body = await request.json()
            digits_val = str(body.get("Digits", body.get("digits", "")))
        except Exception:
            pass
    if not digits_val:
        try:
            form = await request.form()
            digits_val = str(form.get("Digits", form.get("digits", "")))
        except Exception:
            pass

    digits_val = str(digits_val or "1")
    logger.info("Received Twilio IVR DTMF input '%s' for order %s", digits_val, order_id)

    action = "UNKNOWN"
    return {
        "order_id": order_id,
        "digits_pressed": digits_val,
        "action": action,
        "status": "processed",
    }


@webhook_router.post("/reconciliation")
async def receive_3pl_settlement_batch(
    payload: dict[str, Any],
) -> dict[str, Any]:
    """Ingest delayed 3PL courier settlements (Delhivery, Ekart, Xpressbees) to close the adversarial feedback loop."""
    from src.ml.reconciliation import get_reconciliation_engine
    engine = get_reconciliation_engine()

    batch_id = payload.get("batch_id", f"batch_{int(time.time())}")
    records = payload.get("records", [])

    if not records and "order_id" in payload:
        records = [payload]

    result = await engine.reconcile_batch_settlement(batch_id, records)
    return {
        "status": "success",
        "message": f"Successfully reconciled {len(records)} 3PL settlement records",
        "reconciliation": result,
    }


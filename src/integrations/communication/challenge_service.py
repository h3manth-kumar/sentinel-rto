"""Unified Buyer Challenge Orchestration Service.

Coordinates automated WhatsApp and IVR challenge flows for moderate-risk buyers
(Score 40–70) to verify buyer intent before dispatch.
"""
from __future__ import annotations

import logging
from typing import Any

from src.integrations.communication.gupshup_client import GupshupWhatsAppClient
from src.integrations.communication.twilio_client import TwilioIVRClient

logger = logging.getLogger(__name__)


class BuyerChallengeService:
    """Orchestrates multi-channel buyer verification challenges."""

    def __init__(self) -> None:
        self.gupshup = GupshupWhatsAppClient()
        self.twilio = TwilioIVRClient()

    async def initiate_challenge(
        self,
        order_id: str,
        customer_name: str,
        customer_phone: str,
        amount_paise: int,
        preferred_channel: str = "whatsapp",
    ) -> dict[str, Any]:
        """Dispatch intent verification challenge to the customer."""
        amount_inr = amount_paise / 100.0

        if preferred_channel == "ivr":
            result = await self.twilio.trigger_ivr_confirmation_call(
                customer_phone=customer_phone,
                customer_name=customer_name,
                order_id=order_id,
                amount_inr=amount_inr,
            )
        else:
            # Default: WhatsApp interactive message with UPI deposit button
            result = await self.gupshup.send_order_verification_challenge(
                customer_phone=customer_phone,
                customer_name=customer_name,
                order_id=order_id,
                amount_inr=amount_inr,
                deposit_inr=49.0,
            )

        logger.info("Initiated %s challenge for order %s: %s", preferred_channel, order_id, result.get("status"))
        return {
            "order_id": order_id,
            "channel": preferred_channel,
            "challenge_status": "DISPATCHED",
            "provider_result": result,
        }


challenge_service = BuyerChallengeService()


def get_challenge_service() -> BuyerChallengeService:
    return challenge_service

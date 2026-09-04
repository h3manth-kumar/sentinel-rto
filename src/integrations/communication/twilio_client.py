"""Twilio Programmable Voice & IVR Challenge Client.

Triggers automated IVR verification phone calls ("Press 1 to Confirm Order, Press 2 to Cancel")
for unverified high-value Cash on Delivery orders.
"""
from __future__ import annotations

import logging
from typing import Any, Optional
import httpx

logger = logging.getLogger(__name__)


class TwilioIVRClient:
    """Twilio Voice IVR verification client."""

    def __init__(
        self,
        account_sid: str = "mock_twilio_account_sid",
        auth_token: str = "mock_twilio_auth_token",
        from_phone: str = "+18005550199",
    ) -> None:
        self.account_sid = account_sid
        self.auth_token = auth_token
        self.from_phone = from_phone

    async def trigger_ivr_confirmation_call(
        self,
        customer_phone: str,
        customer_name: str,
        order_id: str,
        amount_inr: float,
    ) -> dict[str, Any]:
        """Trigger automated outbound IVR confirmation phone call."""
        clean_phone = customer_phone if customer_phone.startswith("+") else f"+91{customer_phone.replace('-', '').strip()}"

        twiml_script = f"""
        <Response>
            <Say voice="Polly.Aditi">Hello {customer_name}, this is an automated confirmation call for your Cash on Delivery order {order_id} of rupees {amount_inr:.0f}.</Say>
            <Gather numDigits="1" action="/api/webhooks/twilio-ivr?order_id={order_id}" method="POST">
                <Say voice="Polly.Aditi">Press 1 to confirm your order. Press 2 to cancel.</Say>
            </Gather>
        </Response>
        """

        logger.info("Triggered Twilio IVR verification call to %s for order %s", clean_phone, order_id)

        # Mock / Development mode
        if self.account_sid.startswith("mock") or not self.account_sid:
            return {
                "status": "queued",
                "call_sid": f"CA_{order_id}",
                "recipient": clean_phone,
                "provider": "TWILIO_MOCK",
                "twiml": twiml_script.strip(),
            }

        url = f"https://api.twilio.com/2010-04-01/Accounts/{self.account_sid}/Calls.json"
        try:
            async with httpx.AsyncClient(timeout=5.0) as client:
                res = await client.post(
                    url,
                    auth=(self.account_sid, self.auth_token),
                    data={
                        "To": clean_phone,
                        "From": self.from_phone,
                        "Twiml": twiml_script,
                    },
                )
                return res.json() if res.status_code in (200, 201) else {"status": "failed", "code": res.status_code}
        except Exception as e:
            logger.warning("Twilio IVR dispatch error: %s", e)
            return {"status": "queued", "fallback": True, "call_sid": f"CA_fallback_{order_id}"}

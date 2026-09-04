"""Supabase integration client for SENTINEL-RTO.

Asynchronously pushes orders, risk evaluations, tax invoices, and 3PL outcomes
to Supabase PostgreSQL tables in real-time.
"""
from __future__ import annotations

import asyncio
import logging
from typing import Any
import httpx

logger = logging.getLogger(__name__)

SUPABASE_URL = "https://dqoaljyrmkvvhdawxqjk.supabase.co"
SUPABASE_KEY = "sb_publishable_t82CLpnGAe0De_LJT1LiFg_dGTsdNxC"


class SentinelSupabaseClient:
    """Async client to persist transactions to Supabase PostgREST tables."""

    def __init__(self, url: str = SUPABASE_URL, key: str = SUPABASE_KEY) -> None:
        self.url = url.rstrip("/")
        self.key = key
        self.headers = {
            "apikey": self.key,
            "Authorization": f"Bearer {self.key}",
            "Content-Type": "application/json",
            "Prefer": "resolution=merge-duplicates,return=representation",
        }

    async def insert_or_update_order(self, order_data: dict[str, Any]) -> bool:
        """Upsert order into Supabase 'orders' table."""
        payload = {
            "order_id": order_data.get("order_id"),
            "customer_name": order_data.get("customer_name"),
            "customer_phone": order_data.get("customer_phone"),
            "amount_paise": order_data.get("amount_paise"),
            "payment_method": order_data.get("payment_method"),
            "payment_status": order_data.get("payment_status"),
            "risk_score": order_data.get("risk_score"),
            "risk_tier": order_data.get("risk_tier"),
            "action": order_data.get("action"),
            "raw_address": order_data.get("raw_address"),
            "pincode": order_data.get("pincode"),
            "area_name": order_data.get("area_name"),
            "h3_index": order_data.get("h3_index"),
            "what_action": order_data.get("what_action"),
            "why_reason": order_data.get("why_reason"),
            "plain_english_reason": order_data.get("plain_english_reason"),
            "reasons_list": order_data.get("reasons_list", []),
            "items": order_data.get("items", []),
            "shipping_logistics": order_data.get("shipping_logistics", {}),
            "invoice": order_data.get("invoice", {}),
            "latency_ms": order_data.get("latency_ms"),
            "created_at": order_data.get("timestamp"),
        }

        try:
            async with httpx.AsyncClient(timeout=6.0) as client:
                res = await client.post(
                    f"{self.url}/rest/v1/orders",
                    headers=self.headers,
                    json=payload,
                )
                if res.status_code in (200, 201, 204):
                    logger.info("Successfully synced order %s to Supabase", payload["order_id"])
                    return True
                else:
                    logger.warning("Supabase order sync failed (%s): %s", res.status_code, res.text)
                    return False
        except Exception as e:
            logger.warning("Supabase sync exception: %s", e)
            return False

    async def update_delivery_outcome(self, order_id: str, outcome: str) -> bool:
        """Update delivery outcome (DELIVERED / RTO) in Supabase."""
        try:
            async with httpx.AsyncClient(timeout=6.0) as client:
                res = await client.patch(
                    f"{self.url}/rest/v1/orders?order_id=eq.{order_id}",
                    headers=self.headers,
                    json={"delivery_outcome": outcome},
                )
                return res.status_code in (200, 204)
        except Exception as e:
            logger.warning("Supabase outcome update exception: %s", e)
            return False

    async def delete_all_orders(self) -> bool:
        """Delete all orders from Supabase PostgreSQL orders table."""
        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                res = await client.delete(
                    f"{self.url}/rest/v1/orders?order_id=neq.placeholder_nonexistent",
                    headers=self.headers,
                )
                return res.status_code in (200, 204)
        except Exception as e:
            logger.warning("Supabase delete all orders exception: %s", e)
            return False

    async def fetch_all_orders(self, limit: int = 200) -> list[dict[str, Any]]:
        """Fetch historical orders from Supabase PostgreSQL table."""
        try:
            async with httpx.AsyncClient(timeout=6.0) as client:
                res = await client.get(
                    f"{self.url}/rest/v1/orders?select=*&order=created_at.desc&limit={limit}",
                    headers=self.headers,
                )
                if res.status_code == 200:
                    data = res.json()
                    logger.debug("Fetched %d orders from Supabase", len(data))
                    return data
                else:
                    logger.warning("Failed to fetch orders from Supabase (%d): %s", res.status_code, res.text)
        except Exception as e:
            logger.warning("Supabase fetch orders exception: %s", e)
        return []


supabase_client = SentinelSupabaseClient()


def get_supabase_client() -> SentinelSupabaseClient:
    return supabase_client

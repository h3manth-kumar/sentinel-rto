"""Realistic e-commerce traffic simulator.

Generates random buyer orders with varying risk profiles, multi-item carts,
UPI/Card prepaid transactions, returning trusted buyers, and Flink streaming velocity bursts.
"""
from __future__ import annotations

import asyncio
import logging
import random
import time
import uuid
from datetime import datetime, timezone
from typing import Any

import httpx

logger = logging.getLogger(__name__)

FIRST_NAMES = [
    # Naruto
    "Naruto", "Sasuke", "Kakashi", "Itachi", "Jiraiya", "Hinata", "Minato", "Tsunade", "Gaara", "Shikamaru",
    # Black Clover
    "Asta", "Yami", "Noelle", "Yuno", "Julius", "Mereoleona", "Luck", "Finral", "Charlotte", "Fuegoleon",
    # Game of Thrones
    "Jon", "Daenerys", "Tyrion", "Arya", "Ned", "Cersei", "Jaime", "Sansa", "Bran", "Robb",
    # Attack on Titan
    "Eren", "Levi", "Mikasa", "Armin", "Erwin", "Hange", "Jean", "Sasha", "Reiner", "Zeke",
    # Wizarding World (Harry Potter)
    "Harry", "Hermione", "Albus", "Severus", "Ron", "Draco", "Sirius", "Remus", "Luna", "Minerva",
    # Breaking Bad
    "Walter", "Jesse", "Saul", "Gustavo", "Hank", "Mike", "Skyler", "Hector", "Lalo", "Tuco",
]

LAST_NAMES = [
    # Naruto
    "Uzumaki", "Uchiha", "Hatake", "Hyuga", "Namikaze", "Senju", "Nara", "Sabaku",
    # Black Clover
    "Clover", "Sukehiro", "Silva", "Grinberryall", "Novachrono", "Vermillion", "Voltia",
    # Game of Thrones
    "Snow", "Targaryen", "Lannister", "Stark", "Baratheon", "Tyrell", "Martell", "Greyjoy",
    # Attack on Titan
    "Yeager", "Ackerman", "Arlert", "Smith", "Zoe", "Kirstein", "Braus", "Braun",
    # Wizarding World
    "Potter", "Granger", "Dumbledore", "Snape", "Weasley", "Malfoy", "Black", "Lupin", "Lovegood",
    # Breaking Bad
    "White", "Pinkman", "Goodman", "Fring", "Schrader", "Ehrmantraut", "Salamanca",
]

BANGALORE_ADDRESSES = [
    {"pincode": "560103", "area": "Bellandur (Winterfell Enclave)", "address": "Winterfell Keep, Castle Black Road, Bellandur, Bengaluru"},
    {"pincode": "560103", "area": "Bellandur (Dragonstone Heights)", "address": "Dragonstone Citadel, Outer Ring Road, Bellandur, Bengaluru"},
    {"pincode": "560034", "area": "Koramangala (Black Bulls Hideout)", "address": "Black Bulls HQ, Clover Kingdom Lane, 5th Block Koramangala, Bengaluru"},
    {"pincode": "560034", "area": "Koramangala (Magic Knights Tower)", "address": "Golden Dawn Sanctuary, 6th Block Near Forum Mall, Koramangala, Bengaluru"},
    {"pincode": "560102", "area": "HSR Layout (Negra Arroyo Enclave)", "address": "308 Negra Arroyo Lane, Albuquerque Heights, HSR Sector 2, Bengaluru"},
    {"pincode": "560102", "area": "HSR Layout (Better Call Saul Suites)", "address": "Suite 3B, Wexler-McGill Legal Way, HSR Sector 1, Bengaluru"},
    {"pincode": "560066", "area": "Whitefield (Wall Maria Circle)", "address": "Shiganshina District, Wall Maria Circle, ITPL Main Road, Whitefield, Bengaluru"},
    {"pincode": "560066", "area": "Whitefield (Survey Corps HQ)", "address": "Scout Regiment Garrison, Hope Farm Junction, Whitefield, Bengaluru"},
    {"pincode": "560008", "area": "Indiranagar (Hidden Leaf Way)", "address": "Hokage Residence, Hidden Leaf Way, 100 Feet Road, Indiranagar, Bengaluru"},
    {"pincode": "560008", "area": "Indiranagar (Uchiha Clan Compound)", "address": "Sharingan Manor, 12th Main Road, HAL 2nd Stage, Indiranagar, Bengaluru"},
    {"pincode": "560068", "area": "Electronic City (Hogwarts Tower)", "address": "4 Privet Drive, Godric's Hollow Enclave, Electronic City Phase 1, Bengaluru"},
    {"pincode": "560068", "area": "Electronic City (Diagon Alley Hub)", "address": "Ollivanders Wand Shop, Diagon Alley Crossing, Electronic City, Bengaluru"},
    {"pincode": "560078", "area": "JP Nagar (Los Pollos Hermanos)", "address": "Los Pollos Hermanos Plaza, 24th Main, JP Nagar 5th Phase, Bengaluru"},
    {"pincode": "560041", "area": "Jayanagar (King's Landing Gate)", "address": "Red Keep Courtyard, 9th Main, Jayanagar 4th Block, Bengaluru"},
    {"pincode": "560076", "area": "BTM Layout (Paradis Island Way)", "address": "Wall Rose Sector, 12th Cross, BTM 2nd Stage, Bengaluru"},
]

PRODUCTS = [
    {"product_id": "prod_001", "name": "Wireless Earbuds Pro", "price_paise": 149900},
    {"product_id": "prod_002", "name": "Cotton Kurti Set (Pack of 3)", "price_paise": 89900},
    {"product_id": "prod_003", "name": "Smart Fitness Band", "price_paise": 249900},
    {"product_id": "prod_004", "name": "Kitchen Mixer Grinder", "price_paise": 329900},
]


class TrafficGenerator:
    """Generates realistic e-commerce traffic with dynamic Poisson arrivals and cross-order memory."""

    def __init__(self, api_base: str = "http://localhost:8000") -> None:
        self.api_base = api_base
        self.running = False
        self.stats = {
            "total_sent": 0,
            "allowed": 0,
            "challenged": 0,
            "blocked": 0,
            "prepaid": 0,
            "errors": 0,
            "avg_latency_ms": 0.0,
            "start_time": None,
        }
        self._device_pool: list[str] = []
        self._task: asyncio.Task | None = None
        self._burst_attacker_device = "burst_attacker_dev_99"

    async def start(self) -> None:
        if self.running:
            return
        self.running = True
        self.stats["start_time"] = datetime.now(timezone.utc).isoformat()
        self._device_pool = [f"dev_{uuid.uuid4().hex[:8]}" for _ in range(20)]
        self._task = asyncio.create_task(self._run_loop())
        logger.info("Realistic Traffic Generator started.")

    async def stop(self) -> None:
        self.running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        logger.info("Realistic Traffic Generator stopped. Stats: %s", self.stats)

    async def _run_loop(self) -> None:
        async with httpx.AsyncClient(timeout=15) as client:
            while self.running:
                try:
                    order = self._generate_order()
                    resp = await client.post(
                        f"{self.api_base}/api/orders", json=order
                    )
                    if resp.status_code == 200:
                        result = resp.json()
                        self._record_result(result)
                    else:
                        self.stats["errors"] += 1
                except Exception as e:
                    self.stats["errors"] += 1
                    logger.warning("Traffic generator error: %s", e)

                # Poisson arrival delay
                wait_sec = random.expovariate(1.0 / random.uniform(2.2, 4.5))
                wait_sec = max(1.0, min(wait_sec, 6.0))
                await asyncio.sleep(wait_sec)

    def _generate_order(self) -> dict[str, Any]:
        """Generate an order across the spectrum of human, prepaid, burst, and apartment fraud."""
        roll = random.random()

        if roll < 0.12:
            return self._generate_streaming_burst_order()
        elif roll < 0.22:
            return self._generate_prepaid_order()
        elif roll < 0.35:
            return self._generate_moderate_risk_order()
        elif roll < 0.50:
            return self._generate_returning_loyal_order()
        else:
            return self._generate_legitimate_order()

    def _generate_legitimate_order(self) -> dict[str, Any]:
        """Normal genuine buyer in Bangalore."""
        first = random.choice(FIRST_NAMES)
        last = random.choice(LAST_NAMES)
        name = f"{first} {last}"
        phone = f"9{random.randint(100000000, 999999999)}"
        loc = random.choice(BANGALORE_ADDRESSES)

        chosen_prods = random.sample(PRODUCTS, k=random.choice([1, 1, 2]))
        items = []
        total_paise = 0
        for p in chosen_prods:
            q = random.choice([1, 1, 2])
            items.append({"product_id": p["product_id"], "name": p["name"], "quantity": q, "price_paise": p["price_paise"]})
            total_paise += p["price_paise"] * q

        return {
            "merchant_id": "mer_shopeasy_001",
            "order_id": f"ord_{uuid.uuid4().hex[:10]}",
            "items": items,
            "amount_in_paise": total_paise,
            "payment_method": "COD",
            "customer": {
                "name": name,
                "phone": phone,
                "phone_hash": f"ph_{phone}",
                "email_domain": "gmail.com",
                "account_age_days": random.randint(45, 600),
            },
            "device": {
                "fingerprint_hash": random.choice(self._device_pool),
                "ip_address": f"49.207.{random.randint(1, 254)}.{random.randint(1, 254)}",
                "user_agent_raw": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
                "client_signals": {
                    "is_bot_keystrokes": False,
                    "form_fill_duration_ms": random.randint(14000, 24000),
                    "canvas_entropy_score": round(random.uniform(0.85, 0.98), 2),
                },
            },
            "shipping_address": {
                "raw_text": loc["address"],
                "pincode": loc["pincode"],
            },
        }

    def _generate_prepaid_order(self) -> dict[str, Any]:
        """Prepaid buyer using Online UPI / Card."""
        order = self._generate_legitimate_order()
        order["payment_method"] = random.choice(["UPI_PREPAID", "CARD_PREPAID"])
        return order

    def _generate_returning_loyal_order(self) -> dict[str, Any]:
        """Returning loyal customer (e.g. Priya Sharma) to demonstrate ML memory."""
        order = self._generate_legitimate_order()
        order["customer"]["name"] = "Priya Sharma"
        order["customer"]["phone"] = "9876543210"
        order["customer"]["phone_hash"] = "ph_9876543210"
        order["customer"]["account_age_days"] = 450
        order["shipping_address"]["raw_text"] = "Flat 402, Tower B, Green Glen Layout, Bellandur, Bengaluru"
        order["shipping_address"]["pincode"] = "560103"
        return order

    def _generate_moderate_risk_order(self) -> dict[str, Any]:
        """Moderate risk order requiring ₹49 deposit."""
        order = self._generate_legitimate_order()
        order["customer"]["account_age_days"] = random.randint(5, 18)
        order["device"]["client_signals"]["form_fill_duration_ms"] = random.randint(2400, 3600)
        order["device"]["client_signals"]["canvas_entropy_score"] = round(random.uniform(0.50, 0.65), 2)
        return order

    def _generate_streaming_burst_order(self) -> dict[str, Any]:
        """Bot burst attacker using same device in rapid succession (caught by Flink streaming window)."""
        order = self._generate_legitimate_order()
        order["customer"]["name"] = f"Bot User #{random.randint(100, 999)}"
        order["customer"]["account_age_days"] = 1
        order["device"]["fingerprint_hash"] = self._burst_attacker_device
        order["device"]["client_signals"]["form_fill_duration_ms"] = random.randint(400, 800)
        order["device"]["client_signals"]["is_bot_keystrokes"] = True
        order["device"]["client_signals"]["canvas_entropy_score"] = 0.12
        return order

    def _record_result(self, result: dict) -> None:
        self.stats["total_sent"] += 1
        action = result["action"]["type"]
        pay = result.get("payment_status", "")
        if pay == "PAID_ONLINE":
            self.stats["prepaid"] += 1
        elif action == "ALLOW":
            self.stats["allowed"] += 1
        elif action == "CHALLENGE_DEPOSIT":
            self.stats["challenged"] += 1
        else:
            self.stats["blocked"] += 1

        n = self.stats["total_sent"]
        lat = result["execution_metrics"]["total_latency_ms"]
        self.stats["avg_latency_ms"] = (
            self.stats["avg_latency_ms"] * (n - 1) + lat
        ) / n


_generator = TrafficGenerator()


async def get_generator() -> TrafficGenerator:
    return _generator

"""Atomic sliding-window burst rate limiter using Redis ZSET.

Implements the edge velocity defense from PRD Story 1.3:
Intercepts sub-second multi-tab burst orders from the same device/address
before streaming pipelines finish updating.

Redis Data Model (from SCHEMA.md):
    Key:     burst:h3:{h3_index_res9} or burst:device:{device_hash}
    Type:    ZSET
    Score:   Unix Timestamp (Milliseconds)
    Value:   "{order_id}"
"""
from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from enum import Enum
from typing import Any

import redis.asyncio as redis

logger = logging.getLogger(__name__)


class BurstAction(str, Enum):
    """Actions resulting from burst check."""
    ALLOW = "ALLOW"
    CHALLENGE = "CHALLENGE_DEPOSIT"
    FORCE_PREPAID = "FORCE_PREPAID"


@dataclass(frozen=True)
class BurstCheckResult:
    """Result of a burst velocity check."""
    action: BurstAction
    current_count: int
    window_seconds: int
    key: str


class BurstRateLimiter:
    """Atomic sliding-window burst rate limiter.

    Uses Redis ZSET with millisecond-precision timestamps to detect
    rapid-fire checkout attempts from the same entity (device or H3 cell).

    Thresholds (from PRD Story 1.3):
    - >= 3 attempts in 10 seconds -> FORCE_PREPAID
    - >= 2 attempts in 10 seconds -> CHALLENGE_DEPOSIT
    """

    WINDOW_SECONDS = 10
    CHALLENGE_THRESHOLD = 2
    FORCE_PREPAID_THRESHOLD = 3
    BURST_KEY_TTL = 30  # Auto-expire burst keys after 30s

    def __init__(self, redis_url: str) -> None:
        self.redis_url = redis_url
        self._client: redis.Redis | None = None

    async def connect(self) -> None:
        """Initialize Redis connection."""
        self._client = redis.from_url(
            self.redis_url,
            decode_responses=True,
        )
        await self._client.ping()
        logger.info("BurstRateLimiter connected to Redis.")

    async def disconnect(self) -> None:
        """Close Redis connection."""
        if self._client:
            await self._client.aclose()

    async def check_and_record(
        self,
        entity_type: str,
        entity_id: str,
        order_id: str,
    ) -> BurstCheckResult:
        """Atomically check burst velocity and record the new attempt.

        This operation is atomic via Redis pipeline:
        1. ZREMRANGEBYSCORE to prune entries outside the window
        2. ZADD to record the current attempt
        3. ZCARD to count attempts in the window
        4. EXPIRE to set TTL on the key

        Args:
            entity_type: 'h3' or 'device'
            entity_id: The H3 index or device hash
            order_id: Current order ID being evaluated

        Returns:
            BurstCheckResult with action and current count.
        """
        assert self._client is not None

        key = f"burst:{entity_type}:{entity_id}"
        now_ms = int(time.time() * 1000)
        window_start_ms = now_ms - (self.WINDOW_SECONDS * 1000)

        try:
            async with self._client.pipeline(transaction=True) as pipe:
                # 1. Remove entries outside the sliding window
                await pipe.zremrangebyscore(key, 0, window_start_ms)
                # 2. Add current attempt
                await pipe.zadd(key, {order_id: now_ms})
                # 3. Count entries in window
                await pipe.zcard(key)
                # 4. Set TTL for auto-cleanup
                await pipe.expire(key, self.BURST_KEY_TTL)

                results = await pipe.execute()

            current_count = results[2]  # ZCARD result

            # Determine action based on count thresholds
            if current_count >= self.FORCE_PREPAID_THRESHOLD:
                action = BurstAction.FORCE_PREPAID
                logger.warning(
                    "Burst FORCE_PREPAID: %s has %d attempts in %ds",
                    key, current_count, self.WINDOW_SECONDS,
                )
            elif current_count >= self.CHALLENGE_THRESHOLD:
                action = BurstAction.CHALLENGE
                logger.info(
                    "Burst CHALLENGE: %s has %d attempts in %ds",
                    key, current_count, self.WINDOW_SECONDS,
                )
            else:
                action = BurstAction.ALLOW

            return BurstCheckResult(
                action=action,
                current_count=current_count,
                window_seconds=self.WINDOW_SECONDS,
                key=key,
            )
        except redis.RedisError as e:
            logger.error("Burst limiter Redis error: %s", e)
            # Fail-open: allow on Redis failure (defense-in-depth)
            return BurstCheckResult(
                action=BurstAction.ALLOW,
                current_count=0,
                window_seconds=self.WINDOW_SECONDS,
                key=key,
            )

    async def check_h3_burst(
        self, h3_index_res9: str, order_id: str
    ) -> BurstCheckResult:
        """Check burst velocity for an H3 spatial cell."""
        return await self.check_and_record("h3", h3_index_res9, order_id)

    async def check_device_burst(
        self, device_hash: str, order_id: str
    ) -> BurstCheckResult:
        """Check burst velocity for a device fingerprint."""
        return await self.check_and_record("device", device_hash, order_id)

    async def get_burst_count(
        self, entity_type: str, entity_id: str
    ) -> int:
        """Get current burst count without recording a new attempt."""
        assert self._client is not None
        key = f"burst:{entity_type}:{entity_id}"
        now_ms = int(time.time() * 1000)
        window_start_ms = now_ms - (self.WINDOW_SECONDS * 1000)

        try:
            await self._client.zremrangebyscore(key, 0, window_start_ms)
            return await self._client.zcard(key)
        except redis.RedisError:
            return 0

    async def __aenter__(self) -> BurstRateLimiter:
        await self.connect()
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        await self.disconnect()

"""Redis feature store client for real-time risk feature lookups.

Provides O(1) feature retrieval for the online scoring path,
using pipelined MGET/HGETALL for sub-5ms latency at checkout.
"""
from __future__ import annotations

import logging
import time
from typing import Any, Optional

import redis.asyncio as redis

logger = logging.getLogger(__name__)


class RedisFeatureStore:
    """O(1) feature store client for online risk scoring.

    Fetches precomputed entity features from Redis using pipelined
    commands for minimal latency during checkout evaluation.
    """

    def __init__(self, redis_url: str, socket_timeout: float = 0.01) -> None:
        self.redis_url = redis_url
        self.socket_timeout = socket_timeout
        self._client: redis.Redis | None = None

    async def connect(self) -> None:
        """Initialize Redis connection pool."""
        self._client = redis.from_url(
            self.redis_url,
            decode_responses=True,
            socket_timeout=self.socket_timeout,
            socket_connect_timeout=self.socket_timeout,
            retry_on_timeout=False,
        )
        await self._client.ping()
        logger.info("RedisFeatureStore connected: %s", self.redis_url)

    async def disconnect(self) -> None:
        """Close Redis connection pool."""
        if self._client:
            await self._client.aclose()
            logger.info("RedisFeatureStore disconnected.")

    async def get_device_features(self, device_hash: str) -> dict[str, Any]:
        """Fetch precomputed device risk features.

        Returns:
            Dict with rto_rate, order_count, cluster_id or empty dict.
        """
        assert self._client is not None
        key = f"entity:device:{device_hash}"
        try:
            data = await self._client.hgetall(key)
            return dict(data) if data else {}
        except redis.RedisError as e:
            logger.error("Redis error fetching device features: %s", e)
            return {}

    async def get_phone_features(self, phone_hash: str) -> dict[str, Any]:
        """Fetch precomputed phone risk features.

        Returns:
            Dict with rto_rate, order_count, cluster_id or empty dict.
        """
        assert self._client is not None
        key = f"entity:phone:{phone_hash}"
        try:
            data = await self._client.hgetall(key)
            return dict(data) if data else {}
        except redis.RedisError as e:
            logger.error("Redis error fetching phone features: %s", e)
            return {}

    async def get_h3_features(self, h3_index_res9: str) -> dict[str, Any]:
        """Fetch precomputed H3 spatial risk features.

        Returns:
            Dict with cluster_rto_rate, density_weight or empty dict.
        """
        assert self._client is not None
        key = f"entity:h3:{h3_index_res9}"
        try:
            data = await self._client.hgetall(key)
            return dict(data) if data else {}
        except redis.RedisError as e:
            logger.error("Redis error fetching H3 features: %s", e)
            return {}

    async def get_cluster_features(self, cluster_id: str) -> dict[str, Any]:
        """Fetch syndicate cluster risk features."""
        assert self._client is not None
        key = f"cluster:{cluster_id}"
        try:
            data = await self._client.hgetall(key)
            return dict(data) if data else {}
        except redis.RedisError as e:
            logger.error("Redis error fetching cluster features: %s", e)
            return {}

    async def get_all_entity_features(
        self,
        device_hash: str,
        h3_index_res9: str,
        phone_hash: str,
    ) -> dict[str, Any]:
        """Pipeline-fetch all entity features in a single round-trip.

        Uses Redis pipeline (MGET pattern) to fetch device and H3
        features in parallel for sub-5ms total latency.
        """
        assert self._client is not None
        start = time.monotonic()

        device_key = f"entity:device:{device_hash}"
        h3_key = f"entity:h3:{h3_index_res9}"
        phone_key = f"entity:phone:{phone_hash}"

        try:
            async with self._client.pipeline(transaction=False) as pipe:
                await pipe.hgetall(device_key)
                await pipe.hgetall(h3_key)
                await pipe.hgetall(phone_key)
                results = await pipe.execute()

            device_features = dict(results[0]) if results[0] else {}
            h3_features = dict(results[1]) if results[1] else {}
            phone_features = dict(results[2]) if results[2] else {}

            # If device belongs to a cluster, fetch cluster features too
            cluster_features = {}
            cluster_id = device_features.get("cluster_id")
            if cluster_id:
                cluster_features = await self.get_cluster_features(cluster_id)

            elapsed_ms = (time.monotonic() - start) * 1000
            logger.debug(
                "Feature lookup completed in %.2fms (device=%s, h3=%s)",
                elapsed_ms, device_hash[:8], h3_index_res9,
            )

            return {
                "device": device_features,
                "h3": h3_features,
                "phone": phone_features,
                "cluster": cluster_features,
                "lookup_ms": round(elapsed_ms, 2),
            }
        except redis.RedisError as e:
            logger.error("Redis pipeline error: %s", e)
            return {"device": {}, "h3": {}, "phone": {}, "cluster": {}, "lookup_ms": -1}

    async def __aenter__(self) -> RedisFeatureStore:
        await self.connect()
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        await self.disconnect()

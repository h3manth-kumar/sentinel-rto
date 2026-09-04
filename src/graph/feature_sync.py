"""Redis feature store synchronization worker.

Sinks precomputed graph risk metrics (cluster RTO rates, entity features,
spatial density weights) from the graph engine into Redis for O(1) lookup
during online inference.
"""
from __future__ import annotations

import logging
from typing import Any

import redis.asyncio as redis

from src.graph.community_detector import SyndicateClusterResult

logger = logging.getLogger(__name__)

# Redis key TTL: 24 hours (matches SCHEMA.md specification)
FEATURE_TTL_SECONDS = 86400


class FeatureStoreSync:
    """Syncs precomputed graph features to Redis for real-time lookups.

    Redis Data Models (from SCHEMA.md):
    1. entity:device:{device_hash} -> HASH {rto_rate, order_count, cluster_id}
    2. entity:h3:{h3_index_res9} -> HASH {cluster_rto_rate, density_weight}
    3. cluster:{cluster_id} -> HASH {size, rto_rate, is_blacklisted}
    """

    def __init__(self, redis_url: str) -> None:
        self.redis_url = redis_url
        self._client: redis.Redis | None = None

    async def connect(self) -> None:
        """Establish Redis connection."""
        self._client = redis.from_url(
            self.redis_url,
            decode_responses=True,
        )
        await self._client.ping()
        logger.info("Feature store connected to Redis: %s", self.redis_url)

    async def disconnect(self) -> None:
        """Close Redis connection."""
        if self._client:
            await self._client.aclose()
            logger.info("Feature store disconnected from Redis.")

    async def sync_device_features(
        self,
        device_hash: str,
        rto_rate: float,
        order_count: int,
        cluster_id: str | None = None,
    ) -> None:
        """Sync device entity features to Redis."""
        assert self._client is not None
        key = f"entity:device:{device_hash}"
        mapping: dict[str, str] = {
            "rto_rate": str(round(rto_rate, 4)),
            "order_count": str(order_count),
        }
        if cluster_id:
            mapping["cluster_id"] = cluster_id

        async with self._client.pipeline(transaction=True) as pipe:
            await pipe.hset(key, mapping=mapping)
            await pipe.expire(key, FEATURE_TTL_SECONDS)
            await pipe.execute()

    async def sync_h3_features(
        self,
        h3_index_res9: str,
        cluster_rto_rate: float,
        density_weight: float,
    ) -> None:
        """Sync H3 spatial features to Redis."""
        assert self._client is not None
        key = f"entity:h3:{h3_index_res9}"
        mapping = {
            "cluster_rto_rate": str(round(cluster_rto_rate, 4)),
            "density_weight": str(round(density_weight, 4)),
        }

        async with self._client.pipeline(transaction=True) as pipe:
            await pipe.hset(key, mapping=mapping)
            await pipe.expire(key, FEATURE_TTL_SECONDS)
            await pipe.execute()

    async def sync_cluster_features(
        self, cluster: SyndicateClusterResult
    ) -> None:
        """Sync syndicate cluster features to Redis."""
        assert self._client is not None
        key = f"cluster:{cluster.cluster_id}"
        mapping = {
            "size": str(cluster.cluster_size),
            "rto_rate": str(cluster.composite_rto_rate),
            "is_blacklisted": str(cluster.is_suspicious).lower(),
            "root_entity_type": cluster.root_entity_type,
            "account_count": str(cluster.metrics.get("account_count", 0)),
        }

        async with self._client.pipeline(transaction=True) as pipe:
            await pipe.hset(key, mapping=mapping)
            await pipe.expire(key, FEATURE_TTL_SECONDS)
            await pipe.execute()

    async def batch_sync_clusters(
        self, clusters: list[SyndicateClusterResult]
    ) -> int:
        """Batch sync multiple cluster results to Redis.

        Returns:
            Number of clusters synced.
        """
        synced = 0
        for cluster in clusters:
            await self.sync_cluster_features(cluster)
            synced += 1

        logger.info("Batch synced %d cluster features to Redis.", synced)
        return synced

    async def get_device_features(self, device_hash: str) -> dict[str, Any] | None:
        """Fetch precomputed device features from Redis."""
        assert self._client is not None
        key = f"entity:device:{device_hash}"
        data = await self._client.hgetall(key)
        return dict(data) if data else None

    async def get_h3_features(self, h3_index_res9: str) -> dict[str, Any] | None:
        """Fetch precomputed H3 spatial features from Redis."""
        assert self._client is not None
        key = f"entity:h3:{h3_index_res9}"
        data = await self._client.hgetall(key)
        return dict(data) if data else None

    async def __aenter__(self) -> FeatureStoreSync:
        await self.connect()
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        await self.disconnect()

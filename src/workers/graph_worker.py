"""Background graph worker for syndicate detection.

Consumes events from Kafka, builds the entity graph, runs Louvain
community detection, and syncs features back to Redis.

Run: python -m src.workers.graph_worker
"""
from __future__ import annotations

import asyncio
import logging
import signal
from typing import Any

from src.config import get_settings
from src.graph.community_detector import CommunityDetector
from src.graph.entity_resolver import EntityResolver
from src.graph.feature_sync import FeatureStoreSync
from src.kafka.consumer import SentinelKafkaConsumer
from src.kafka.schemas import (
    CancellationEvent,
    KafkaTopics,
    OrderEvent,
    RTOEvent,
)

logger = logging.getLogger(__name__)

CLUSTER_RECOMPUTE_INTERVAL = 100  # Recompute communities every N events


class GraphWorker:
    """Orchestrates the offline graph intelligence pipeline."""

    def __init__(self) -> None:
        self.settings = get_settings()
        self.resolver = EntityResolver()
        self.feature_sync: FeatureStoreSync | None = None
        self.consumer: SentinelKafkaConsumer | None = None
        self._event_count = 0
        self._running = False

    async def start(self) -> None:
        """Initialize Kafka consumer and Redis sync."""
        self.consumer = SentinelKafkaConsumer(
            bootstrap_servers=self.settings.kafka_bootstrap_servers,
            group_id="sentinel-graph-workers",
            topics=[
                KafkaTopics.ORDERS_RAW,
                KafkaTopics.CANCELLATIONS,
                KafkaTopics.RTO_EVENTS,
            ],
        )
        self.feature_sync = FeatureStoreSync(redis_url=self.settings.redis_url)

        await self.consumer.start()
        await self.feature_sync.connect()
        self._running = True
        logger.info("Graph worker started. Consuming events...")

    async def stop(self) -> None:
        """Graceful shutdown."""
        self._running = False
        if self.consumer:
            await self.consumer.stop()
        if self.feature_sync:
            await self.feature_sync.disconnect()
        logger.info("Graph worker stopped.")

    async def run(self) -> None:
        """Main event processing loop."""
        await self.start()

        try:
            async for event in self.consumer.consume():
                if not self._running:
                    break
                await self._process_event(event)
        except asyncio.CancelledError:
            logger.info("Graph worker cancelled.")
        finally:
            await self.stop()

    async def _process_event(self, event: Any) -> None:
        """Route and process a single event."""
        if isinstance(event, OrderEvent):
            await self._handle_order(event)
        elif isinstance(event, CancellationEvent):
            await self._handle_cancellation(event)
        elif isinstance(event, RTOEvent):
            await self._handle_rto(event)

        self._event_count += 1

        # Periodically recompute communities
        if self._event_count % CLUSTER_RECOMPUTE_INTERVAL == 0:
            await self._recompute_communities()

    async def _handle_order(self, event: OrderEvent) -> None:
        """Ingest order event into entity graph."""
        self.resolver.ingest_order_event(
            order_id=event.order_id,
            customer_phone_hash=event.customer_phone_hash,
            device_hash=event.device_fingerprint_hash,
            h3_index_res9=event.h3_index_res9,
        )
        logger.debug("Ingested order %s into graph.", event.order_id)

    async def _handle_cancellation(self, event: CancellationEvent) -> None:
        """Handle cancellation — increase edge risk weight."""
        account_id = f"account:{event.customer_phone_hash}"
        device_id = f"device:{event.device_fingerprint_hash}"
        if self.resolver.graph.has_edge(account_id, device_id):
            edge = self.resolver.graph.edges[account_id, device_id]
            edge["weight"] = edge.get("weight", 1.0) + 0.5
        logger.debug("Cancellation penalty applied for %s.", event.order_id)

    async def _handle_rto(self, event: RTOEvent) -> None:
        """Handle RTO — heavy negative reinforcement on graph edges."""
        account_id = f"account:{event.customer_phone_hash}"
        device_id = f"device:{event.device_fingerprint_hash}"
        address_id = f"address:{event.h3_index_res9}"

        for target_id in [device_id, address_id]:
            if self.resolver.graph.has_edge(account_id, target_id):
                edge = self.resolver.graph.edges[account_id, target_id]
                edge["weight"] = edge.get("weight", 1.0) + 2.0
                edge["rto_count"] = edge.get("rto_count", 0) + 1

        # Update address node RTO stats
        if address_id in self.resolver.graph:
            node = self.resolver.graph.nodes[address_id]
            node["rto_deliveries"] = node.get("rto_deliveries", 0) + 1
            node["total_orders"] = node.get("total_orders", 0) + 1

        logger.debug("RTO penalty applied for %s.", event.order_id)

    async def _recompute_communities(self) -> None:
        """Run Louvain detection and sync results to Redis."""
        logger.info(
            "Recomputing communities (nodes=%d, edges=%d)...",
            self.resolver.node_count,
            self.resolver.edge_count,
        )
        detector = CommunityDetector(self.resolver.graph)
        clusters = detector.detect_communities()

        if self.feature_sync:
            await self.feature_sync.batch_sync_clusters(clusters)

            # Sync individual device/H3 features
            for cluster in clusters:
                for member in cluster.members:
                    node_data = self.resolver.graph.nodes[member]
                    entity_type = node_data.get("entity_type", "")

                    if entity_type == "DEVICE":
                        device_hash = node_data.get("device_hash", "")
                        total = node_data.get("total_orders", 1)
                        rto = node_data.get("rto_deliveries", 0)
                        await self.feature_sync.sync_device_features(
                            device_hash=device_hash,
                            rto_rate=rto / max(total, 1),
                            order_count=total,
                            cluster_id=cluster.cluster_id,
                        )
                    elif entity_type == "ADDRESS":
                        h3_index = node_data.get("h3_index", "")
                        await self.feature_sync.sync_h3_features(
                            h3_index_res9=h3_index,
                            cluster_rto_rate=cluster.composite_rto_rate,
                            density_weight=cluster.metrics.get("density", 1.0),
                        )

        logger.info(
            "Community detection complete: %d clusters, %d suspicious.",
            len(clusters),
            len(detector.get_suspicious_clusters()),
        )


async def main() -> None:
    """Entry point for the graph worker."""
    worker = GraphWorker()

    loop = asyncio.get_event_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, lambda: asyncio.create_task(worker.stop()))
        except NotImplementedError:
            pass  # Windows doesn't support add_signal_handler

    await worker.run()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    asyncio.run(main())

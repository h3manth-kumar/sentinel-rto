import json
import logging
from typing import AsyncGenerator, List, Optional, Union

from aiokafka import AIOKafkaConsumer

from src.kafka.schemas import CancellationEvent, KafkaTopics, OrderEvent, RTOEvent

logger = logging.getLogger(__name__)


class SentinelKafkaConsumer:
    """Async Kafka consumer for SENTINEL-RTO events."""

    def __init__(self, bootstrap_servers: str, group_id: str, topics: List[str]) -> None:
        self.bootstrap_servers = bootstrap_servers
        self.group_id = group_id
        self.topics = topics
        self.consumer: Optional[AIOKafkaConsumer] = None

    async def start(self) -> None:
        """Creates and starts the AIOKafkaConsumer."""
        self.consumer = AIOKafkaConsumer(
            *self.topics,
            bootstrap_servers=self.bootstrap_servers,
            group_id=self.group_id
        )
        await self.consumer.start()
        logger.info(f"Kafka consumer started for topics={self.topics} group_id={self.group_id}")

    async def stop(self) -> None:
        """Stops the consumer."""
        if self.consumer:
            await self.consumer.stop()
            logger.info("Kafka consumer stopped")

    def _deserialize(self, topic: str, raw: bytes) -> Union[OrderEvent, CancellationEvent, RTOEvent]:
        """Routes to correct Pydantic schema based on topic."""
        payload = json.loads(raw.decode('utf-8'))
        if topic == KafkaTopics.ORDERS_RAW:
            return OrderEvent(**payload)
        elif topic == KafkaTopics.CANCELLATIONS:
            return CancellationEvent(**payload)
        elif topic == KafkaTopics.RTO_EVENTS:
            return RTOEvent(**payload)
        else:
            raise ValueError(f"Unknown topic: {topic}")

    async def consume(self) -> AsyncGenerator[Union[OrderEvent, CancellationEvent, RTOEvent], None]:
        """Yields deserialized messages from Kafka topics."""
        if not self.consumer:
            raise RuntimeError("Consumer not started")
        
        async for msg in self.consumer:
            try:
                deserialized_msg = self._deserialize(msg.topic, msg.value)
                logger.debug(f"Consumed message from {msg.topic}: {deserialized_msg}")
                yield deserialized_msg
            except Exception as e:
                logger.error(f"Failed to deserialize message from {msg.topic}: {e}")

    async def __aenter__(self):
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.stop()

import logging
from typing import Any, Optional

try:
    from aiokafka import AIOKafkaProducer
except ImportError:
    AIOKafkaProducer = None

from src.kafka.schemas import CancellationEvent, KafkaTopics, OrderEvent, RTOEvent

logger = logging.getLogger(__name__)


class SentinelKafkaProducer:
    """Async Kafka producer for SENTINEL-RTO events."""

    def __init__(self, bootstrap_servers: str) -> None:
        self.bootstrap_servers = bootstrap_servers
        self.producer: Optional[Any] = None

    async def start(self) -> None:
        """Creates and starts the AIOKafkaProducer."""
        if AIOKafkaProducer is None:
            logger.info("aiokafka not installed; running in in-memory streaming fallback mode.")
            return
        try:
            self.producer = AIOKafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                value_serializer=self._serialize
            )
            await self.producer.start()
            logger.info(f"Kafka producer started with bootstrap_servers={self.bootstrap_servers}")
        except Exception as e:
            logger.warning("Kafka broker connection skipped (%s). Using fallback stream.", e)
            self.producer = None

    async def stop(self) -> None:
        """Stops the producer."""
        if self.producer:
            try:
                await self.producer.stop()
            except Exception:
                pass
            logger.info("Kafka producer stopped")

    def _serialize(self, value: Any) -> bytes:
        """Converts Pydantic model to JSON bytes."""
        return value.model_dump_json().encode('utf-8')

    async def send_order_event(self, event: OrderEvent) -> None:
        """Serializes and sends an OrderEvent to ORDERS_RAW topic."""
        if not self.producer:
            return
        try:
            await self.producer.send_and_wait(KafkaTopics.ORDERS_RAW, event)
        except Exception as e:
            logger.warning("Kafka send skipped: %s", e)

    async def send_cancellation_event(self, event: CancellationEvent) -> None:
        """Sends a CancellationEvent to CANCELLATIONS topic."""
        if not self.producer:
            return
        try:
            await self.producer.send_and_wait(KafkaTopics.CANCELLATIONS, event)
        except Exception as e:
            logger.warning("Kafka send skipped: %s", e)

    async def send_rto_event(self, event: RTOEvent) -> None:
        """Sends an RTOEvent to RTO_EVENTS topic."""
        if not self.producer:
            return
        try:
            await self.producer.send_and_wait(KafkaTopics.RTO_EVENTS, event)
        except Exception as e:
            logger.warning("Kafka send skipped: %s", e)

    async def __aenter__(self):
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.stop()

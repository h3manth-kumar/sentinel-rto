"""Kafka Dead Letter Queue (DLQ) & Schema Evolution Error Router.

Routes malformed Indian address payloads, JSON serialization errors, and schema drift
into the 'orders.dlq' topic with structured diagnostics and quarantine policies.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
import json
import logging
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

TOPIC_ORDERS_DLQ = "orders.dlq"


@dataclass
class DLQRecord:
    dlq_id: str
    original_topic: str
    error_type: str
    error_message: str
    raw_payload: str
    quarantine_timestamp: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    retry_count: int = 0
    resolved: bool = False


class DeadLetterQueueRouter:
    """Manages poisoned and malformed stream records."""

    def __init__(self) -> None:
        self.dlq_buffer: List[DLQRecord] = []
        self._dlq_counter = 0

    def route_to_dlq(
        self,
        original_topic: str,
        error: Exception,
        raw_payload: Any,
        error_type: str = "SCHEMA_VALIDATION_ERROR",
    ) -> DLQRecord:
        """Quarantine a poisoned message to the Dead Letter Queue."""
        self._dlq_counter += 1
        dlq_id = f"dlq_{self._dlq_counter:06d}_{int(datetime.now().timestamp())}"

        if isinstance(raw_payload, (dict, list)):
            payload_str = json.dumps(raw_payload)
        elif isinstance(raw_payload, bytes):
            payload_str = raw_payload.decode("utf-8", errors="replace")
        else:
            payload_str = str(raw_payload)

        record = DLQRecord(
            dlq_id=dlq_id,
            original_topic=original_topic,
            error_type=error_type,
            error_message=str(error),
            raw_payload=payload_str,
        )

        self.dlq_buffer.append(record)
        logger.warning(
            "POISON PILL ROUTED TO DLQ [%s]: topic=%s error=%s payload_snippet=%s",
            dlq_id, original_topic, str(error), payload_str[:80]
        )
        return record

    def get_dlq_metrics(self) -> Dict[str, Any]:
        """Return DLQ health metrics for Prometheus/OpenTelemetry."""
        return {
            "dlq_topic": TOPIC_ORDERS_DLQ,
            "total_quarantined_messages": len(self.dlq_buffer),
            "unresolved_count": sum(1 for r in self.dlq_buffer if not r.resolved),
            "recent_dlq_errors": [
                {
                    "dlq_id": r.dlq_id,
                    "topic": r.original_topic,
                    "error": r.error_message,
                    "type": r.error_type,
                    "time": r.quarantine_timestamp,
                }
                for r in self.dlq_buffer[-10:]
            ],
        }


dlq_router = DeadLetterQueueRouter()


def get_dlq_router() -> DeadLetterQueueRouter:
    return dlq_router

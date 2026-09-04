"""OpenTelemetry Distributed Tracing & APM Metrics Instrumentation.

Provides W3C Trace Context propagation across FastAPI gateways, Kafka streaming,
Redis lookups, and ONNX Runtime inference execution.
"""
from __future__ import annotations

import contextlib
import logging
import time
import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any, AsyncGenerator, Generator, Optional

logger = logging.getLogger(__name__)


@dataclass
class SpanEvent:
    name: str
    timestamp: str
    attributes: dict[str, Any] = field(default_factory=dict)


@dataclass
class TraceSpan:
    """Represents an OpenTelemetry-compatible span."""
    trace_id: str
    span_id: str
    parent_span_id: Optional[str]
    name: str
    start_time_ns: int
    end_time_ns: Optional[int] = None
    duration_ms: Optional[float] = None
    status_code: str = "OK"  # 'OK' or 'ERROR'
    attributes: dict[str, Any] = field(default_factory=dict)
    events: list[SpanEvent] = field(default_factory=list)

    def set_attribute(self, key: str, value: Any) -> None:
        self.attributes[key] = value

    def add_event(self, name: str, attributes: Optional[dict[str, Any]] = None) -> None:
        self.events.append(SpanEvent(
            name=name,
            timestamp=datetime.now(timezone.utc).isoformat(),
            attributes=attributes or {},
        ))

    def end(self, status_code: str = "OK") -> None:
        self.end_time_ns = time.time_ns()
        self.duration_ms = round((self.end_time_ns - self.start_time_ns) / 1_000_000.0, 3)
        self.status_code = status_code


class SentinelTelemetryTracer:
    """Enterprise Distributed Tracer managing hierarchical TraceContexts."""

    def __init__(self, service_name: str = "sentinel-rto-gateway") -> None:
        self.service_name = service_name
        self._completed_spans: list[TraceSpan] = []
        self._max_recorded_spans = 2000

    @staticmethod
    def generate_trace_id() -> str:
        """Generate 128-bit W3C hexadecimal Trace ID."""
        return uuid.uuid4().hex

    @staticmethod
    def generate_span_id() -> str:
        """Generate 64-bit W3C hexadecimal Span ID."""
        return uuid.uuid4().hex[:16]

    def create_root_span(self, name: str, attributes: Optional[dict[str, Any]] = None) -> TraceSpan:
        """Create a new root trace span for an incoming HTTP request."""
        trace_id = self.generate_trace_id()
        span_id = self.generate_span_id()
        attrs = {"service.name": self.service_name, **(attributes or {})}

        span = TraceSpan(
            trace_id=trace_id,
            span_id=span_id,
            parent_span_id=None,
            name=name,
            start_time_ns=time.time_ns(),
            attributes=attrs,
        )
        return span

    def create_child_span(
        self,
        parent_span: TraceSpan,
        name: str,
        attributes: Optional[dict[str, Any]] = None,
    ) -> TraceSpan:
        """Create a child span linked to the parent trace context."""
        span_id = self.generate_span_id()
        attrs = {"service.name": self.service_name, **(attributes or {})}

        span = TraceSpan(
            trace_id=parent_span.trace_id,
            span_id=span_id,
            parent_span_id=parent_span.span_id,
            name=name,
            start_time_ns=time.time_ns(),
            attributes=attrs,
        )
        return span

    def record_span(self, span: TraceSpan) -> None:
        """Record completed span in in-memory APM buffer."""
        self._completed_spans.append(span)
        if len(self._completed_spans) > self._max_recorded_spans:
            self._completed_spans.pop(0)

    @contextlib.contextmanager
    def start_span(
        self,
        name: str,
        parent: Optional[TraceSpan] = None,
        attributes: Optional[dict[str, Any]] = None,
    ) -> Generator[TraceSpan, None, None]:
        """Synchronous context manager for span lifecycle."""
        if parent is None:
            span = self.create_root_span(name, attributes)
        else:
            span = self.create_child_span(parent, name, attributes)

        try:
            yield span
            span.end(status_code="OK")
        except Exception as e:
            span.set_attribute("error.message", str(e))
            span.set_attribute("error.type", type(e).__name__)
            span.end(status_code="ERROR")
            raise
        finally:
            self.record_span(span)

    @contextlib.asynccontextmanager
    async def start_async_span(
        self,
        name: str,
        parent: Optional[TraceSpan] = None,
        attributes: Optional[dict[str, Any]] = None,
    ) -> AsyncGenerator[TraceSpan, None]:
        """Asynchronous context manager for span lifecycle."""
        if parent is None:
            span = self.create_root_span(name, attributes)
        else:
            span = self.create_child_span(parent, name, attributes)

        try:
            yield span
            span.end(status_code="OK")
        except Exception as e:
            span.set_attribute("error.message", str(e))
            span.set_attribute("error.type", type(e).__name__)
            span.end(status_code="ERROR")
            raise
        finally:
            self.record_span(span)

    def get_recent_spans(self, limit: int = 50) -> list[dict[str, Any]]:
        """Retrieve recent spans for observability inspection."""
        return [asdict(s) for s in self._completed_spans[-limit:]]


tracer = SentinelTelemetryTracer()


def get_tracer() -> SentinelTelemetryTracer:
    return tracer

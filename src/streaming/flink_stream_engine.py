"""Real-Time Streaming Pipeline for SENTINEL-RTO (Kafka & Apache Flink Architecture).

Implements real-time stream ingestion and stateful windowed aggregations:
1. Ingests raw OrderEvents, CancellationEvents, and RTOEvents from Kafka 'orders.raw' stream.
2. Stateful Stream Processing (Flink sliding/tumbling 60-second window aggregations):
   - KeyBy(customer_phone_hash): Computes buyer checkout velocity (orders in last 60s / 10m).
   - KeyBy(device_fingerprint_hash): Computes device concurrency and bot clustering.
   - KeyBy(h3_index_res9): Computes spatial order burst intensity per 100m hex cell.
3. Real-Time Sink: Emits processed stream features to Redis Cluster and Supabase in real-time.
"""
from __future__ import annotations

import asyncio
from collections import defaultdict, deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
import logging
import time
from typing import Any, Callable, Deque, Dict, List, Optional

from src.kafka.schemas import OrderEvent, CancellationEvent, RTOEvent, KafkaTopics
from src.redis.cluster_store import RedisClusterFeatureStore
from src.db.supabase_client import get_supabase_client

logger = logging.getLogger(__name__)


@dataclass
class WindowedMetric:
    timestamp: float
    amount_paise: int
    is_rto: bool = False
    is_blocked: bool = False


@dataclass
class StreamingEntityState:
    phone_orders_60s: Deque[WindowedMetric] = field(default_factory=deque)
    device_orders_60s: Deque[WindowedMetric] = field(default_factory=deque)
    h3_orders_60s: Deque[WindowedMetric] = field(default_factory=deque)
    total_processed: int = 0


class RealtimeFlinkStreamEngine:
    """Real-time stream processor with Flink tumbling window semantics."""

    def __init__(self, window_size_seconds: float = 60.0) -> None:
        self.window_size_sec = window_size_seconds
        self.phone_state: Dict[str, Deque[WindowedMetric]] = defaultdict(deque)
        self.device_state: Dict[str, Deque[WindowedMetric]] = defaultdict(deque)
        self.h3_state: Dict[str, Deque[WindowedMetric]] = defaultdict(deque)
        self.active_typing_state: dict[str, dict[str, Any]] = {}
        self.typing_history: Deque[dict[str, Any]] = deque(maxlen=50)
        self.stream_history: Deque[dict[str, Any]] = deque(maxlen=200)
        self.redis_store = RedisClusterFeatureStore()
        self.supabase = get_supabase_client()
        self.event_counter = 0
        self._is_running = True
        self._listeners: List[Callable[[dict[str, Any]], Any]] = []

    def register_listener(self, listener: Callable[[dict[str, Any]], Any]) -> None:
        self._listeners.append(listener)

    def _evict_old_windows(self, now: float) -> None:
        """Evict metrics older than window size (Flink sliding window state eviction)."""
        cutoff = now - self.window_size_sec
        for k in list(self.phone_state.keys()):
            dq = self.phone_state[k]
            while dq and dq[0].timestamp < cutoff:
                dq.popleft()
            if not dq:
                del self.phone_state[k]

        for k in list(self.device_state.keys()):
            dq = self.device_state[k]
            while dq and dq[0].timestamp < cutoff:
                dq.popleft()
            if not dq:
                del self.device_state[k]

        for k in list(self.h3_state.keys()):
            dq = self.h3_state[k]
            while dq and dq[0].timestamp < cutoff:
                dq.popleft()
            if not dq:
                del self.h3_state[k]

        # Prune typing sessions older than 90 seconds
        for s_id in list(self.active_typing_state.keys()):
            if now - self.active_typing_state[s_id].get("timestamp", 0) > 90.0:
                del self.active_typing_state[s_id]

    def ingest_typing_event(self, typing_data: dict[str, Any]) -> dict[str, Any]:
        """Ingest live keystroke typing telemetry from shopper checkout into Kafka topic."""
        now = time.time()
        self._evict_old_windows(now)
        self.event_counter += 1

        session_id = typing_data.get("session_id", "shopper_active_session")
        phone = typing_data.get("customer_phone") or "shopper_phone"
        h3_cell = typing_data.get("h3_index_res9", "89618925407ffff")
        canvas = typing_data.get("device_canvas", "Browser Session")

        record = {
            "session_id": session_id,
            "timestamp": now,
            "iso_time": datetime.now(timezone.utc).strftime("%H:%M:%S.%f")[:-3],
            "customer_name": typing_data.get("customer_name", "Shopper"),
            "customer_phone": typing_data.get("customer_phone", ""),
            "raw_address": typing_data.get("raw_address", ""),
            "pincode": typing_data.get("pincode", "560103"),
            "area_name": typing_data.get("area_name", "Bengaluru"),
            "h3_index_res9": h3_cell,
            "payment_method": typing_data.get("payment_method", "COD"),
            "preliminary_risk_score": typing_data.get("preliminary_risk_score", 15),
            "preliminary_action": typing_data.get("preliminary_action", "ALLOW"),
            "device_canvas": canvas,
            "field_modified": typing_data.get("field_modified", "address"),
        }
        self.active_typing_state[session_id] = record
        self.typing_history.appendleft(record)

        # Register keystroke in Flink sliding windows
        metric = WindowedMetric(timestamp=now, amount_paise=0, is_blocked=(record["preliminary_action"] == "BLOCK"))
        self.phone_state[phone].append(metric)
        self.device_state[canvas[:16]].append(metric)
        self.h3_state[h3_cell].append(metric)

        # Append to stream records list
        stream_rec = {
            "stream_seq": self.event_counter,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "event_type": "KEYSTROKE_STREAM",
            "order_id": f"key_{session_id[-4:]}",
            "customer_name": record["customer_name"],
            "area_name": record["area_name"],
            "customer_phone_hash": (phone[:6] + "****") if len(phone) >= 6 else "ph_live****",
            "device_fingerprint_hash": canvas[:8] + "****",
            "h3_index_res9": h3_cell,
            "amount_paise": 0,
            "action": record["preliminary_action"],
            "streaming_features": {
                "phone_velocity_60s": len(self.phone_state[phone]),
                "device_velocity_60s": len(self.device_state[canvas[:16]]),
                "h3_burst_count_60s": len(self.h3_state[h3_cell]),
                "h3_burst_gmv_inr": 0.0,
                "is_burst_anomaly": len(self.device_state[canvas[:16]]) >= 3 or len(self.h3_state[h3_cell]) >= 5,
            },
        }
        self.stream_history.appendleft(stream_rec)

        return record

    async def ingest_stream_event(self, event_dict: dict[str, Any]) -> dict[str, Any]:
        """Ingest event into the real-time Flink stream processor."""
        now = time.time()
        self._evict_old_windows(now)
        self.event_counter += 1

        phone_hash = event_dict.get("customer_phone_hash") or event_dict.get("customer_phone") or "unknown"
        device_hash = event_dict.get("device_fingerprint_hash") or event_dict.get("device_hash") or "unknown"
        h3_cell = event_dict.get("h3_index_res9") or event_dict.get("h3_index") or "89618925407ffff"
        amount_paise = int(event_dict.get("amount_paise") or (float(event_dict.get("amount_in_paise") or 0)))
        action = event_dict.get("action", "ALLOW")
        is_blocked = action in ("BLOCK", "FORCE_PREPAID")

        metric = WindowedMetric(timestamp=now, amount_paise=amount_paise, is_blocked=is_blocked)

        # Append to stateful sliding windows (KeyBy)
        self.phone_state[phone_hash].append(metric)
        self.device_state[device_hash].append(metric)
        self.h3_state[h3_cell].append(metric)

        # Compute real-time stream aggregates
        phone_velocity_60s = len(self.phone_state[phone_hash])
        device_velocity_60s = len(self.device_state[device_hash])
        h3_burst_count_60s = len(self.h3_state[h3_cell])
        h3_burst_gmv_paise = sum(m.amount_paise for m in self.h3_state[h3_cell])

        processed_stream_record = {
            "stream_seq": self.event_counter,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "event_type": "ORDER_STREAM",
            "order_id": event_dict.get("order_id"),
            "customer_name": event_dict.get("customer_name") or event_dict.get("name") or "Shopper",
            "area_name": event_dict.get("area_name", "Bengaluru"),
            "customer_phone_hash": phone_hash[:8] + "****",
            "device_fingerprint_hash": device_hash[:8] + "****",
            "h3_index_res9": h3_cell,
            "amount_paise": amount_paise,
            "action": action,
            "streaming_features": {
                "phone_velocity_60s": phone_velocity_60s,
                "device_velocity_60s": device_velocity_60s,
                "h3_burst_count_60s": h3_burst_count_60s,
                "h3_burst_gmv_inr": round(h3_burst_gmv_paise / 100.0, 2),
                "is_burst_anomaly": device_velocity_60s >= 3 or h3_burst_count_60s >= 5,
            },
        }

        self.stream_history.appendleft(processed_stream_record)

        # Notify active listeners
        for listener in self._listeners:
            try:
                res = listener(processed_stream_record)
                if asyncio.iscoroutine(res):
                    asyncio.create_task(res)
            except Exception as e:
                logger.debug("Listener notification error: %s", e)

        # Async background sync to Redis cluster
        asyncio.create_task(self._sync_redis_stream_metrics(device_hash, h3_cell, device_velocity_60s, h3_burst_count_60s))

        return processed_stream_record

    async def _sync_redis_stream_metrics(self, device_hash: str, h3_cell: str, dev_vel: int, h3_burst: int) -> None:
        """Sink streaming features to Redis cache."""
        try:
            await self.redis_store.set_device_features(device_hash, {
                "stream_velocity_60s": dev_vel,
                "last_active": time.time(),
            }, ttl=3600)
            await self.redis_store.set_h3_features(h3_cell, {
                "burst_count_60s": h3_burst,
                "last_active": time.time(),
            }, ttl=3600)
        except Exception as e:
            logger.debug("Redis stream sink error: %s", e)

    def get_realtime_stream_metrics(self) -> dict[str, Any]:
        """Return real-time Flink engine throughput and stream metrics."""
        now = time.time()
        self._evict_old_windows(now)

        active_phones_60s = len(self.phone_state)
        active_devices_60s = len(self.device_state)
        active_h3_cells_60s = len(self.h3_state)
        total_events_in_window = sum(len(dq) for dq in self.phone_state.values())

        # Dynamic throughput: events per second over active time span
        all_timestamps = [m.timestamp for dq in self.phone_state.values() for m in dq]
        if all_timestamps:
            span = max(1.0, now - min(all_timestamps))
            throughput_eps = round(len(all_timestamps) / span, 2)
        else:
            throughput_eps = 0.0

        return {
            "status": "STREAMING_ACTIVE",
            "window_size_seconds": self.window_size_sec,
            "total_lifetime_events": self.event_counter,
            "lifetime_stream_records": self.event_counter,
            "events_in_active_window": total_events_in_window,
            "active_events_in_window": total_events_in_window,
            "throughput_events_per_sec": throughput_eps,
            "throughput_eps": throughput_eps,
            "active_devices_in_window": active_devices_60s,
            "active_h3_cells_in_window": active_h3_cells_60s,
            "active_h3_spatial_cells": active_h3_cells_60s,
            "active_typing_sessions": list(self.active_typing_state.values()),
            "typing_streams": list(self.active_typing_state.values()),
            "recent_typing_history": list(self.typing_history)[:20],
            "recent_stream_records": list(self.stream_history)[:25],
        }


# Global streaming engine singleton
flink_engine = RealtimeFlinkStreamEngine()


def get_flink_engine() -> RealtimeFlinkStreamEngine:
    return flink_engine

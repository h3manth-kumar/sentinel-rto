"""Idempotent Transaction Processing Engine.

Prevents double-charging, duplicate order placement, and concurrency race conditions
during flash-sale spikes using Redis-backed atomic token states.
"""
from __future__ import annotations

import hashlib
import json
import logging
from enum import Enum
from typing import Any, Optional, Tuple

import redis.asyncio as aioredis

logger = logging.getLogger(__name__)

IDEMPOTENCY_KEY_PREFIX = "idempotency:"
DEFAULT_IDEMPOTENCY_TTL = 3600  # 1 Hour


class IdempotencyState(str, Enum):
    PROCESSING = "PROCESSING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"


class IdempotencyEngine:
    """Manages transactional idempotency across API endpoints."""

    def __init__(self, redis_client: Optional[aioredis.Redis] = None) -> None:
        self.client = redis_client
        self._local_cache: dict[str, dict[str, Any]] = {}

    @staticmethod
    def generate_fingerprint(
        idempotency_key: str,
        request_body: dict[str, Any] | str,
    ) -> str:
        """Generate deterministic SHA-256 fingerprint for the request."""
        body_str = json.dumps(request_body, sort_keys=True) if isinstance(request_body, dict) else str(request_body)
        raw_combined = f"{idempotency_key}::{body_str}"
        return hashlib.sha256(raw_combined.encode()).hexdigest()

    async def start_transaction(
        self,
        idempotency_key: str,
        request_fingerprint: str,
        ttl_seconds: int = DEFAULT_IDEMPOTENCY_TTL,
    ) -> Tuple[bool, Optional[dict[str, Any]]]:
        """Attempt to initiate an idempotent transaction.
        
        Returns:
            (is_new_transaction, cached_response_if_completed)
        """
        redis_key = f"{IDEMPOTENCY_KEY_PREFIX}{idempotency_key}"

        if self.client is None:
            # Local fallback
            if idempotency_key in self._local_cache:
                entry = self._local_cache[idempotency_key]
                if entry["fingerprint"] != request_fingerprint:
                    raise ValueError("Idempotency key reused with mismatched request payload")
                if entry["state"] == IdempotencyState.COMPLETED.value:
                    return False, entry.get("response")
                elif entry["state"] == IdempotencyState.PROCESSING.value:
                    return False, None  # Concurrent in-flight request
            self._local_cache[idempotency_key] = {
                "fingerprint": request_fingerprint,
                "state": IdempotencyState.PROCESSING.value,
                "response": None,
            }
            return True, None

        try:
            # Check existing record
            existing = await self.client.get(redis_key)
            if existing:
                data = json.loads(existing)
                if data.get("fingerprint") != request_fingerprint:
                    raise ValueError("Idempotency key reused with mismatched payload")
                if data.get("state") == IdempotencyState.COMPLETED.value:
                    logger.info("Idempotent hit for key: %s (returning cached response)", idempotency_key)
                    return False, data.get("response")
                elif data.get("state") == IdempotencyState.PROCESSING.value:
                    logger.warning("Concurrent in-flight transaction for idempotency key: %s", idempotency_key)
                    return False, None

            # Mark as PROCESSING atomically
            initial_data = {
                "fingerprint": request_fingerprint,
                "state": IdempotencyState.PROCESSING.value,
                "response": None,
            }
            set_ok = await self.client.set(
                redis_key,
                json.dumps(initial_data),
                nx=True,
                ex=ttl_seconds,
            )
            return bool(set_ok), None

        except ValueError:
            raise
        except Exception as e:
            logger.error("Redis idempotency error for key %s: %s", idempotency_key, e)
            return True, None

    async def complete_transaction(
        self,
        idempotency_key: str,
        response_payload: dict[str, Any],
        ttl_seconds: int = DEFAULT_IDEMPOTENCY_TTL,
    ) -> None:
        """Mark the transaction as COMPLETED and cache the response."""
        redis_key = f"{IDEMPOTENCY_KEY_PREFIX}{idempotency_key}"

        if self.client is None:
            if idempotency_key in self._local_cache:
                self._local_cache[idempotency_key]["state"] = IdempotencyState.COMPLETED.value
                self._local_cache[idempotency_key]["response"] = response_payload
            return

        try:
            existing = await self.client.get(redis_key)
            fingerprint = ""
            if existing:
                fingerprint = json.loads(existing).get("fingerprint", "")

            completed_data = {
                "fingerprint": fingerprint,
                "state": IdempotencyState.COMPLETED.value,
                "response": response_payload,
            }
            await self.client.set(
                redis_key,
                json.dumps(completed_data),
                ex=ttl_seconds,
            )
            logger.debug("Marked idempotency key %s as COMPLETED", idempotency_key)
        except Exception as e:
            logger.error("Failed to mark idempotency key %s as completed: %s", idempotency_key, e)

    async def fail_transaction(self, idempotency_key: str) -> None:
        """Release or delete the idempotency key if transaction failed so user can retry."""
        redis_key = f"{IDEMPOTENCY_KEY_PREFIX}{idempotency_key}"
        if self.client is None:
            self._local_cache.pop(idempotency_key, None)
            return
        try:
            await self.client.delete(redis_key)
            logger.debug("Released idempotency key %s after failure", idempotency_key)
        except Exception as e:
            logger.warning("Error clearing failed idempotency key %s: %s", idempotency_key, e)

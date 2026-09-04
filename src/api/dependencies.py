"""FastAPI dependency injection and lifecycle management."""

from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from typing import AsyncGenerator

from fastapi import FastAPI

from src.config import get_settings
from src.kafka.producer import SentinelKafkaProducer
from src.ml.inference import ONNXInferenceEngine
from src.redis.burst_limiter import BurstRateLimiter
from src.redis.feature_store import RedisFeatureStore

logger = logging.getLogger(__name__)


class Dependencies:
    """Singleton container for shared service instances."""

    feature_store: RedisFeatureStore | None = None
    burst_limiter: BurstRateLimiter | None = None
    inference_engine: ONNXInferenceEngine | None = None
    kafka_producer: SentinelKafkaProducer | None = None


deps = Dependencies()


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    """Application lifespan: start/stop all shared services."""
    settings = get_settings()

    # --- Redis Feature Store ---
    try:
        deps.feature_store = RedisFeatureStore(redis_url=settings.redis_url)
        await deps.feature_store.connect()
        logger.info("Redis feature store connected.")
    except Exception as e:
        logger.warning("Redis feature store unavailable: %s. Using fallbacks.", e)
        deps.feature_store = None

    # --- Redis Burst Limiter ---
    try:
        deps.burst_limiter = BurstRateLimiter(redis_url=settings.redis_url)
        await deps.burst_limiter.connect()
        logger.info("Burst rate limiter connected.")
    except Exception as e:
        logger.warning("Burst rate limiter unavailable: %s. Burst checks disabled.", e)
        deps.burst_limiter = None

    # --- ONNX Inference Engine ---
    try:
        deps.inference_engine = ONNXInferenceEngine(
            model_path=settings.onnx_model_path,
        )
        await deps.inference_engine.start()
        logger.info("ONNX inference engine started.")
    except Exception as e:
        logger.warning("ONNX inference engine failed to start: %s. Using fallback scores.", e)
        deps.inference_engine = None

    # --- Kafka Producer ---
    try:
        deps.kafka_producer = SentinelKafkaProducer(
            bootstrap_servers=settings.kafka_bootstrap_servers,
        )
        await deps.kafka_producer.start()
        logger.info("Kafka producer started.")
    except Exception as e:
        logger.warning("Kafka producer unavailable: %s. Event emission disabled.", e)
        deps.kafka_producer = None

    logger.info("SENTINEL-RTO startup complete.")
    yield

    # --- Shutdown ---
    if deps.kafka_producer:
        try:
            await deps.kafka_producer.stop()
        except Exception:
            pass
    if deps.burst_limiter:
        try:
            await deps.burst_limiter.disconnect()
        except Exception:
            pass
    if deps.feature_store:
        try:
            await deps.feature_store.disconnect()
        except Exception:
            pass
    if deps.inference_engine:
        try:
            await deps.inference_engine.stop()
        except Exception:
            pass
    logger.info("SENTINEL-RTO shutdown complete.")


async def get_feature_store() -> RedisFeatureStore | None:
    """FastAPI dependency for Redis feature store."""
    yield deps.feature_store


async def get_burst_limiter() -> BurstRateLimiter | None:
    """FastAPI dependency for burst rate limiter."""
    yield deps.burst_limiter


async def get_inference_engine() -> ONNXInferenceEngine | None:
    """FastAPI dependency for ONNX inference engine."""
    yield deps.inference_engine


async def get_kafka_producer() -> SentinelKafkaProducer | None:
    """FastAPI dependency for Kafka producer."""
    yield deps.kafka_producer

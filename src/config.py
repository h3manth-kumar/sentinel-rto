"""Centralized application configuration using Pydantic BaseSettings."""

from functools import lru_cache
from typing import Any
from pydantic import field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # PostgreSQL
    database_url: str = (
        "postgresql+asyncpg://sentinel:sentinel_pass@localhost:5432/sentinel_db"
    )
    database_url_sync: str = (
        "postgresql://sentinel:sentinel_pass@localhost:5432/sentinel_db"
    )

    # Redis
    redis_url: str = "redis://localhost:6379/0"

    # Kafka
    kafka_bootstrap_servers: str = "localhost:9092"
    kafka_consumer_group: str = "sentinel-consumers"

    # Application
    app_env: str = "development"
    log_level: str = "INFO"
    api_port: int = 8000

    # ML Model
    onnx_model_path: str = "./models/sentinel_lgbm.onnx"

    # Security
    hmac_secret_key: str = "change-me-in-production"

    @field_validator("api_port", mode="before")
    @classmethod
    def validate_api_port(cls, v: Any) -> int:
        if v is None or v == "":
            return 8000
        try:
            return int(v)
        except (ValueError, TypeError):
            return 8000

    @field_validator("database_url", "database_url_sync", "redis_url", "kafka_bootstrap_servers", mode="before")
    @classmethod
    def validate_non_empty_strings(cls, v: Any, info) -> Any:
        if v == "" or v is None:
            defaults = {
                "database_url": "postgresql+asyncpg://sentinel:sentinel_pass@localhost:5432/sentinel_db",
                "database_url_sync": "postgresql://sentinel:sentinel_pass@localhost:5432/sentinel_db",
                "redis_url": "redis://localhost:6379/0",
                "kafka_bootstrap_servers": "localhost:9092",
            }
            return defaults.get(info.field_name, v)
        return v


@lru_cache
def get_settings() -> Settings:
    """Cached settings instance."""
    try:
        return Settings()
    except Exception:
        return Settings(
            database_url="postgresql+asyncpg://sentinel:sentinel_pass@localhost:5432/sentinel_db",
            database_url_sync="postgresql://sentinel:sentinel_pass@localhost:5432/sentinel_db",
            redis_url="redis://localhost:6379/0",
            kafka_bootstrap_servers="localhost:9092",
            api_port=8000,
        )


settings = get_settings()

"""Centralized application configuration using Pydantic BaseSettings."""

from functools import lru_cache
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
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


@lru_cache
def get_settings() -> Settings:
    """Cached settings instance."""
    return Settings()


settings = get_settings()

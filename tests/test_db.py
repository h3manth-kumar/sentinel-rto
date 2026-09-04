"""Database migration and model smoke tests."""

from __future__ import annotations

import importlib

import pytest


class TestDatabaseModels:
    """Verify database models and migrations are importable."""

    def test_models_import(self) -> None:
        """All ORM models should import without errors."""
        module = importlib.import_module("src.db.models")
        assert hasattr(module, "Base")
        assert hasattr(module, "Merchant")
        assert hasattr(module, "AddressH3")
        assert hasattr(module, "Device")
        assert hasattr(module, "SyndicateCluster")
        assert hasattr(module, "Transaction")
        assert hasattr(module, "RiskEvaluation")

    def test_session_import(self) -> None:
        """Database session factory should import without errors."""
        module = importlib.import_module("src.db.session")
        assert hasattr(module, "get_db_session")

    def test_config_import(self) -> None:
        """Application config should import without errors."""
        module = importlib.import_module("src.config")
        assert hasattr(module, "Settings")
        assert hasattr(module, "get_settings")


class TestKafkaModules:
    """Verify Kafka modules are importable."""

    def test_schemas_import(self) -> None:
        """Kafka message schemas should import without errors."""
        module = importlib.import_module("src.kafka.schemas")
        assert hasattr(module, "OrderEvent")
        assert hasattr(module, "CancellationEvent")
        assert hasattr(module, "RTOEvent")
        assert hasattr(module, "KafkaTopics")

    def test_producer_import(self) -> None:
        """Kafka producer should import without errors."""
        module = importlib.import_module("src.kafka.producer")
        assert hasattr(module, "SentinelKafkaProducer")

    def test_consumer_import(self) -> None:
        """Kafka consumer should import without errors."""
        module = importlib.import_module("src.kafka.consumer")
        assert hasattr(module, "SentinelKafkaConsumer")

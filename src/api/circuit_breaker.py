import time
from enum import Enum
import uuid
from typing import Any
from src.api.schemas import RiskEvaluateResponse, ActionResponse, ExplanationResponse, ExecutionMetrics

class CircuitState(Enum):
    CLOSED = "CLOSED"
    OPEN = "OPEN"
    HALF_OPEN = "HALF_OPEN"

class CircuitBreakerOpenException(Exception):
    pass

class CircuitBreaker:
    def __init__(self, failure_threshold: int = 5, cooldown_seconds: int = 30):
        self.failure_threshold = failure_threshold
        self.cooldown_seconds = cooldown_seconds
        self.state = CircuitState.CLOSED
        self.failure_count = 0
        self.last_failure_time = 0.0

    async def __aenter__(self):
        now = time.time()
        if self.state == CircuitState.OPEN:
            if now - self.last_failure_time >= self.cooldown_seconds:
                self.state = CircuitState.HALF_OPEN
            else:
                raise CircuitBreakerOpenException("Circuit breaker is OPEN")
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if exc_type is not None:
            if not issubclass(exc_type, CircuitBreakerOpenException):
                self.failure_count += 1
                self.last_failure_time = time.time()
                if self.failure_count >= self.failure_threshold:
                    self.state = CircuitState.OPEN
            return False  # Don't suppress exception, let caller handle fallback
        else:
            if self.state == CircuitState.HALF_OPEN:
                self.state = CircuitState.CLOSED
                self.failure_count = 0
        return False

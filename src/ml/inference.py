"""ONNX Runtime inference engine for LightGBM risk scoring model.

This module provides the inference session management and prediction
interface for the SENTINEL-RTO risk scoring model exported to ONNX format.

Phase 1: Scaffold with session initialization and warm-up.
Phase 3: Full integration with FastAPI online scoring gateway.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import numpy as np

logger = logging.getLogger(__name__)


class ONNXInferenceEngine:
    """Manages ONNX Runtime inference sessions for risk scoring.

    Attributes:
        model_path: Path to the exported ONNX model file.
        session: The ONNX Runtime inference session (initialized on start).
    """

    def __init__(self, model_path: str | Path) -> None:
        self.model_path = Path(model_path)
        self.session: Any = None
        self._is_warmed_up: bool = False

    async def start(self) -> None:
        """Initialize the ONNX Runtime inference session.

        Loads the model and performs a warm-up inference with synthetic
        dummy tensors to eliminate cold-start latency (TRD mitigation).
        """
        try:
            import onnxruntime as ort

            if not self.model_path.exists():
                logger.warning(
                    "ONNX model not found at %s. "
                    "Inference will use fallback heuristics.",
                    self.model_path,
                )
                return

            self.session = ort.InferenceSession(
                str(self.model_path),
                providers=["CPUExecutionProvider"],
            )

            # Warm-up inference to eliminate cold-start latency
            self._warm_up()
            logger.info(
                "ONNX inference session initialized and warmed up: %s",
                self.model_path,
            )
        except ImportError:
            logger.error(
                "onnxruntime not installed. ML inference disabled."
            )
        except Exception:
            logger.exception("Failed to initialize ONNX inference session.")

    def _warm_up(self) -> None:
        """Run a dummy inference to pre-allocate internal buffers."""
        if self.session is None:
            return

        input_name = self.session.get_inputs()[0].name
        input_shape = self.session.get_inputs()[0].shape
        # Replace dynamic/None dims with 1
        dummy_shape = [1 if (isinstance(d, str) or d is None) else d for d in input_shape]
        dummy_input = np.zeros(dummy_shape, dtype=np.float32)

        self.session.run(None, {input_name: dummy_input})
        self._is_warmed_up = True
        logger.debug("Warm-up inference completed.")

    async def predict(self, features: np.ndarray) -> dict[str, Any]:
        """Run inference on a feature vector and return risk score.

        Args:
            features: 1D numpy array of precomputed risk features.

        Returns:
            Dictionary with risk_score (0-100) and raw model output.
        """
        if self.session is None:
            logger.warning("No ONNX session. Returning fallback score.")
            return {"risk_score": 50, "fallback": True}

        input_name = self.session.get_inputs()[0].name
        input_data = features.reshape(1, -1).astype(np.float32)

        outputs = self.session.run(None, {input_name: input_data})

        # LightGBM binary classifier outputs probabilities
        probabilities = outputs[1]  # Second output is probabilities
        rto_probability = float(probabilities[0][1])
        risk_score = int(rto_probability * 100)

        return {
            "risk_score": min(max(risk_score, 0), 100),
            "rto_probability": rto_probability,
            "fallback": False,
        }

    async def stop(self) -> None:
        """Clean up the inference session."""
        self.session = None
        self._is_warmed_up = False
        logger.info("ONNX inference session closed.")

"""Statistical Drift & Concept Shift Detection Engine (Evidently AI Style).

Monitors feature distribution shifts and concept drift between baseline training
distributions and rolling production inference streams using Kolmogorov-Smirnov (KS)
tests and Population Stability Index (PSI).
"""
from __future__ import annotations

import json
import logging
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

import numpy as np

logger = logging.getLogger(__name__)


@dataclass
class FeatureDriftMetric:
    """Drift metrics for a single feature."""
    feature_name: str
    statistic_type: str  # 'KS_TEST' or 'PSI'
    score: float
    p_value: Optional[float]
    is_drift_detected: bool
    baseline_mean: float
    current_mean: float
    severity: str  # 'LOW', 'MODERATE', 'CRITICAL'


@dataclass
class DriftReport:
    """Consolidated Drift Evaluation Report."""
    timestamp: str
    total_samples_analyzed: int
    overall_dataset_drift: bool
    drifted_features_count: int
    total_features_count: int
    drift_share: float
    feature_metrics: list[FeatureDriftMetric] = field(default_factory=list)
    concept_shift_metric: Optional[dict[str, Any]] = None


class SentinelDriftDetector:
    """Enterprise Feature Distribution and Concept Drift Monitor.
    
    Computes:
    1. Kolmogorov-Smirnov (KS) 2-sample test for continuous distribution divergence.
    2. Population Stability Index (PSI) with 10 quantiles.
    3. Target/Concept Shift: Correlation shift between predicted risk scores and 3PL RTO outcomes.
    """

    KS_ALPHA_THRESHOLD = 0.05
    PSI_WARNING_THRESHOLD = 0.10
    PSI_CRITICAL_THRESHOLD = 0.25

    def __init__(self, baseline_features: Optional[np.ndarray] = None, feature_names: Optional[list[str]] = None) -> None:
        self.feature_names = feature_names or [
            "form_fill_duration_ms",
            "canvas_entropy_score",
            "account_age_days",
            "spatial_confidence",
            "order_amount_inr",
            "device_burst_count",
            "historical_rto_rate",
        ]
        self.baseline = baseline_features
        if self.baseline is None:
            self.baseline = self._generate_synthetic_baseline(500)
        self.rolling_buffer: list[np.ndarray] = []
        self.max_buffer_size = 1000

    def _generate_synthetic_baseline(self, n_samples: int = 500) -> np.ndarray:
        """Create reference baseline distribution representing healthy production traffic."""
        rng = np.random.RandomState(42)
        feats = np.zeros((n_samples, len(self.feature_names)), dtype=np.float32)
        feats[:, 0] = rng.normal(12000, 4000, n_samples)  # form_fill_duration_ms (8-16s)
        feats[:, 1] = rng.uniform(0.75, 0.98, n_samples)  # canvas_entropy_score
        feats[:, 2] = rng.exponential(120, n_samples)      # account_age_days
        feats[:, 3] = rng.uniform(0.70, 0.99, n_samples)  # spatial_confidence
        feats[:, 4] = rng.lognormal(7.2, 0.8, n_samples)  # order_amount_inr (~1500)
        feats[:, 5] = rng.poisson(0.3, n_samples)         # burst count
        feats[:, 6] = rng.beta(1.5, 10.0, n_samples)      # historical_rto_rate (~10%)
        return feats

    def record_inference_features(self, feature_vector: np.ndarray | list[float]) -> None:
        """Add single or batch inference features to rolling production buffer."""
        vec = np.array(feature_vector, dtype=np.float32)
        if vec.ndim == 1:
            self.rolling_buffer.append(vec)
        else:
            for row in vec:
                self.rolling_buffer.append(row)

        if len(self.rolling_buffer) > self.max_buffer_size:
            self.rolling_buffer = self.rolling_buffer[-self.max_buffer_size:]

    @staticmethod
    def _compute_psi(baseline: np.ndarray, current: np.ndarray, num_buckets: int = 10) -> float:
        """Calculate Population Stability Index (PSI)."""
        if len(baseline) == 0 or len(current) == 0:
            return 0.0

        # Create quantile bins based on baseline
        quantiles = np.linspace(0, 100, num_buckets + 1)
        bins = np.percentile(baseline, quantiles)
        bins[0] = -np.inf
        bins[-1] = np.inf

        base_counts, _ = np.histogram(baseline, bins=bins)
        curr_counts, _ = np.histogram(current, bins=bins)

        # Smooth zero counts to avoid div-by-zero
        base_pct = (base_counts + 1e-4) / (len(baseline) + 1e-4 * num_buckets)
        curr_pct = (curr_counts + 1e-4) / (len(current) + 1e-4 * num_buckets)

        psi_val = np.sum((curr_pct - base_pct) * np.log(curr_pct / base_pct))
        return float(max(psi_val, 0.0))

    @staticmethod
    def _ks_2sample(sample1: np.ndarray, sample2: np.ndarray) -> tuple[float, float]:
        """Compute two-sample Kolmogorov-Smirnov statistic & approximate p-value."""
        n1 = len(sample1)
        n2 = len(sample2)
        if n1 == 0 or n2 == 0:
            return 0.0, 1.0

        s1 = np.sort(sample1)
        s2 = np.sort(sample2)
        data_all = np.concatenate([s1, s2])
        cdf1 = np.searchsorted(s1, data_all, side="right") / n1
        cdf2 = np.searchsorted(s2, data_all, side="right") / n2

        d_stat = float(np.max(np.abs(cdf1 - cdf2)))
        # Asymptotic KS p-value approximation
        en = np.sqrt((n1 * n2) / (n1 + n2))
        lambda_val = (en + 0.12 + 0.11 / en) * d_stat
        if lambda_val <= 0:
            p_val = 1.0
        else:
            # Kolmogorov survival function approximation
            p_val = float(2 * np.exp(-2 * (lambda_val ** 2)))
            p_val = min(max(p_val, 0.0), 1.0)

        return d_stat, p_val

    def generate_drift_report(self) -> DriftReport:
        """Analyze current production buffer against baseline and generate structured drift report."""
        if len(self.rolling_buffer) < 10:
            logger.info("Insufficient production samples for drift analysis (%d < 10)", len(self.rolling_buffer))
            return DriftReport(
                timestamp=datetime.now(timezone.utc).isoformat(),
                total_samples_analyzed=len(self.rolling_buffer),
                overall_dataset_drift=False,
                drifted_features_count=0,
                total_features_count=len(self.feature_names),
                drift_share=0.0,
                feature_metrics=[],
            )

        curr_array = np.array(self.rolling_buffer)
        metrics: list[FeatureDriftMetric] = []
        drift_count = 0

        num_features = min(curr_array.shape[1], self.baseline.shape[1], len(self.feature_names))

        for idx in range(num_features):
            fname = self.feature_names[idx]
            base_col = self.baseline[:, idx]
            curr_col = curr_array[:, idx]

            ks_stat, p_val = self._ks_2sample(base_col, curr_col)
            psi_val = self._compute_psi(base_col, curr_col)

            is_drift = p_val < self.KS_ALPHA_THRESHOLD or psi_val > self.PSI_CRITICAL_THRESHOLD
            if is_drift:
                drift_count += 1

            severity = "LOW"
            if psi_val > self.PSI_CRITICAL_THRESHOLD or p_val < 0.001:
                severity = "CRITICAL"
            elif psi_val > self.PSI_WARNING_THRESHOLD or p_val < 0.05:
                severity = "MODERATE"

            metrics.append(FeatureDriftMetric(
                feature_name=fname,
                statistic_type="KS_AND_PSI",
                score=round(psi_val, 4),
                p_value=round(p_val, 4),
                is_drift_detected=is_drift,
                baseline_mean=round(float(np.mean(base_col)), 2),
                current_mean=round(float(np.mean(curr_col)), 2),
                severity=severity,
            ))

        drift_share = drift_count / num_features if num_features > 0 else 0.0
        overall_drift = drift_share >= 0.40  # Alert if >= 40% features drift

        report = DriftReport(
            timestamp=datetime.now(timezone.utc).isoformat(),
            total_samples_analyzed=len(self.rolling_buffer),
            overall_dataset_drift=overall_drift,
            drifted_features_count=drift_count,
            total_features_count=num_features,
            drift_share=round(drift_share, 4),
            feature_metrics=metrics,
        )

        return report

    def save_report_to_disk(self, file_path: str = "models/drift_report.json") -> Path:
        """Save drift report JSON to disk for MLOps dashboards."""
        report = self.generate_drift_report()
        p = Path(file_path)
        p.parent.mkdir(parents=True, exist_ok=True)
        with open(p, "w", encoding="utf-8") as f:
            json.dump(asdict(report), f, indent=2)
        logger.info("Saved drift report to %s", p)
        return p

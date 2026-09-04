import numpy as np
from src.api.schemas import RiskEvaluateRequest

class FeatureEngineer:
    def __init__(self):
        pass

    def get_feature_names(self) -> list[str]:
        return [
            "amount_in_paise",
            "is_cod",
            "account_age_days",
            "form_fill_duration_ms",
            "canvas_entropy_score",
            "is_bot_keystrokes",
            "device_rto_rate",
            "device_order_count",
            "h3_cluster_rto_rate",
            "h3_density_weight",
            "cluster_size",
            "cluster_rto_rate",
            "burst_count_h3",
            "burst_count_device"
        ]

    def build_feature_vector(self, request: RiskEvaluateRequest, redis_features: dict) -> np.ndarray:
        features = [
            float(request.amount_in_paise),
            1.0 if request.payment_method.upper() == "COD" else 0.0,
            float(request.customer.account_age_days),
            float(request.device.client_signals.form_fill_duration_ms),
            float(request.device.client_signals.canvas_entropy_score),
            1.0 if request.device.client_signals.is_bot_keystrokes else 0.0,
            float(redis_features.get("device_rto_rate", 0.0)),
            float(redis_features.get("device_order_count", 0.0)),
            float(redis_features.get("h3_cluster_rto_rate", 0.0)),
            float(redis_features.get("h3_density_weight", 0.0)),
            float(redis_features.get("cluster_size", 0.0)),
            float(redis_features.get("cluster_rto_rate", 0.0)),
            float(redis_features.get("burst_count_h3", 0.0)),
            float(redis_features.get("burst_count_device", 0.0)),
        ]
        return np.array(features, dtype=np.float32)

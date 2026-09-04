"""LightGBM model training and ONNX export pipeline.

Trains a binary classifier to predict RTO probability from
transaction features, evaluates on held-out test set, and
exports to ONNX format for sub-10ms inference.
"""
from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

import lightgbm as lgb
import numpy as np
import pandas as pd
from sklearn.metrics import (
    classification_report,
    precision_score,
    recall_score,
    roc_auc_score,
)
from sklearn.model_selection import train_test_split

logger = logging.getLogger(__name__)

# Feature columns (must match FeatureEngineer.FEATURE_NAMES)
FEATURE_COLUMNS = [
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
    "burst_count_device",
]

TARGET_COLUMN = "is_rto"


class ModelTrainer:
    """Trains LightGBM RTO prediction model and exports to ONNX."""

    def __init__(self, data_path: str = "data/synthetic_transactions.csv") -> None:
        self.data_path = Path(data_path)
        self.model: lgb.Booster | None = None
        self.metrics: dict[str, Any] = {}

    def load_data(self) -> tuple[pd.DataFrame, pd.DataFrame]:
        """Load and split data into train/test sets."""
        df = pd.read_csv(self.data_path)
        logger.info("Loaded %d records from %s", len(df), self.data_path)

        # Add derived features
        df["is_cod"] = (df["payment_method"] == "COD").astype(int)

        # Train/test split (80/20, stratified)
        train_df, test_df = train_test_split(
            df, test_size=0.2, random_state=42, stratify=df[TARGET_COLUMN]
        )
        logger.info("Train: %d, Test: %d", len(train_df), len(test_df))
        return train_df, test_df

    def train(self, train_df: pd.DataFrame) -> lgb.Booster:
        """Train LightGBM binary classifier."""
        X_train = train_df[FEATURE_COLUMNS].values
        y_train = train_df[TARGET_COLUMN].values

        train_data = lgb.Dataset(X_train, label=y_train, feature_name=FEATURE_COLUMNS)

        params = {
            "objective": "binary",
            "metric": ["binary_logloss", "auc"],
            "boosting_type": "gbdt",
            "num_leaves": 63,
            "learning_rate": 0.05,
            "feature_fraction": 0.8,
            "bagging_fraction": 0.8,
            "bagging_freq": 5,
            "min_child_samples": 20,
            "scale_pos_weight": (y_train == 0).sum() / max((y_train == 1).sum(), 1),
            "verbose": -1,
            "seed": 42,
        }

        self.model = lgb.train(
            params,
            train_data,
            num_boost_round=500,
        )
        logger.info("Model training completed: %d trees", self.model.num_trees())
        return self.model

    def evaluate(self, test_df: pd.DataFrame) -> dict[str, Any]:
        """Evaluate model on held-out test set."""
        assert self.model is not None, "Model not trained"

        X_test = test_df[FEATURE_COLUMNS].values
        y_test = test_df[TARGET_COLUMN].values

        y_proba = self.model.predict(X_test)
        y_pred = (y_proba >= 0.5).astype(int)

        precision = precision_score(y_test, y_pred)
        recall = recall_score(y_test, y_pred)
        roc_auc = roc_auc_score(y_test, y_proba)

        self.metrics = {
            "precision": round(precision, 4),
            "recall": round(recall, 4),
            "roc_auc": round(roc_auc, 4),
            "test_size": len(y_test),
            "positive_rate": round(y_test.mean(), 4),
        }

        logger.info(
            "Test metrics — Precision: %.4f, Recall: %.4f, ROC-AUC: %.4f",
            precision, recall, roc_auc,
        )
        print("\nClassification Report:")
        print(classification_report(y_test, y_pred, target_names=["Legit", "RTO"]))

        return self.metrics

    def export_onnx(self, output_dir: str = "models") -> Path:
        """Export trained model to ONNX format."""
        assert self.model is not None, "Model not trained"

        try:
            from onnxmltools import convert_lightgbm
            from onnxmltools.convert.common.data_types import FloatTensorType
        except ImportError:
            logger.error(
                "onnxmltools not installed. Install with: pip install onnxmltools"
            )
            # Fallback: save as native LightGBM format
            output_path = Path(output_dir)
            output_path.mkdir(parents=True, exist_ok=True)
            model_path = output_path / "sentinel_lgbm.txt"
            self.model.save_model(str(model_path))
            logger.info("Model saved as native LightGBM: %s", model_path)
            return model_path

        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)

        initial_type = [("input", FloatTensorType([None, len(FEATURE_COLUMNS)]))]
        onnx_model = convert_lightgbm(
            self.model, initial_types=initial_type, target_opset=15
        )

        model_path = output_path / "sentinel_lgbm.onnx"
        with open(model_path, "wb") as f:
            f.write(onnx_model.SerializeToString())

        logger.info("ONNX model exported to %s", model_path)
        return model_path

    def save_metrics(self, output_dir: str = "models") -> Path:
        """Save evaluation metrics to JSON."""
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)
        metrics_path = output_path / "benchmark_report.json"
        with open(metrics_path, "w") as f:
            json.dump(self.metrics, f, indent=2)
        logger.info("Metrics saved to %s", metrics_path)
        return metrics_path

    def save_feature_importance(self, output_dir: str = "models") -> Path:
        """Save feature importance rankings."""
        assert self.model is not None
        importance = self.model.feature_importance(importance_type="gain")
        feature_imp = sorted(
            zip(FEATURE_COLUMNS, importance),
            key=lambda x: x[1],
            reverse=True,
        )
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)
        imp_path = output_path / "feature_importance.json"
        with open(imp_path, "w") as f:
            json.dump(
                [{"feature": f, "importance": round(float(i), 4)} for f, i in feature_imp],
                f, indent=2,
            )
        logger.info("Feature importance saved to %s", imp_path)
        return imp_path


def run_full_pipeline() -> None:
    """Run the complete training pipeline."""
    trainer = ModelTrainer()
    train_df, test_df = trainer.load_data()
    trainer.train(train_df)
    trainer.evaluate(test_df)
    trainer.export_onnx()
    trainer.save_metrics()
    trainer.save_feature_importance()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run_full_pipeline()

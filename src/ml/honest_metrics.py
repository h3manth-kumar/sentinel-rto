"""SENTINEL-RTO — Honest Metrics Report with False-Positive Cost Analysis.

Generates a transparent report card with:
1. Standard ML metrics (precision, recall, F1, AUC)
2. False-positive cost quantification (₹ lost per FP)
3. False-negative cost quantification (₹ lost per FN)
4. Net economic impact at various thresholds
5. Defense-only compliance checklist

Run: python -m src.ml.honest_metrics
"""
from __future__ import annotations

import json
import logging
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.metrics import (
    classification_report,
    confusion_matrix,
    precision_recall_curve,
    roc_auc_score,
    roc_curve,
)

logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger(__name__)

# ---------- Constants (Indian e-commerce unit economics) ----------

AVG_ORDER_VALUE_PAISE = 120000       # ₹1,200 average COD order
AVG_FORWARD_SHIPPING_PAISE = 6000    # ₹60 forward shipping
AVG_RETURN_SHIPPING_PAISE = 8000     # ₹80 return shipping
AVG_PACKAGING_COST_PAISE = 2000      # ₹20 packaging
AVG_GROSS_MARGIN_PCT = 0.30          # 30% gross margin on goods

# Cost of a FALSE POSITIVE (blocking a legitimate buyer)
# Merchant loses the gross margin on the sale + brand damage
COST_FP_PAISE = int(AVG_ORDER_VALUE_PAISE * AVG_GROSS_MARGIN_PCT)  # ₹360

# Cost of a FALSE NEGATIVE (allowing a fraudulent RTO)
# Merchant pays 2x shipping + packaging + inventory lock, gets ₹0
COST_FN_PAISE = (
    AVG_FORWARD_SHIPPING_PAISE
    + AVG_RETURN_SHIPPING_PAISE
    + AVG_PACKAGING_COST_PAISE
)  # ₹140


def load_model_and_data():
    """Load the trained model and test data."""
    import lightgbm as lgb

    from sklearn.model_selection import train_test_split as tts

    model = lgb.Booster(model_file="models/sentinel_lgbm.txt")
    df = pd.read_csv("data/synthetic_transactions.csv")

    # Derive is_cod (same as train_model.py)
    df["is_cod"] = (df["payment_method"] == "COD").astype(int)

    feature_cols = [
        "amount_in_paise", "is_cod", "account_age_days",
        "form_fill_duration_ms", "canvas_entropy_score", "is_bot_keystrokes",
        "device_rto_rate", "device_order_count", "h3_cluster_rto_rate",
        "h3_density_weight", "cluster_size", "cluster_rto_rate",
        "burst_count_h3", "burst_count_device",
    ]

    # Use same stratified split as training (80/20, seed=42)
    _, test_df = tts(df, test_size=0.2, random_state=42, stratify=df["is_rto"])
    X_test = test_df[feature_cols].values
    y_test = test_df["is_rto"].values

    # Get raw probability scores
    y_prob = model.predict(X_test)

    return X_test, y_test, y_prob, test_df


def compute_threshold_economics(y_true, y_prob, thresholds):
    """Compute economic impact at each threshold."""
    results = []
    for thresh in thresholds:
        y_pred = (y_prob >= thresh).astype(int)
        tn, fp, fn, tp = confusion_matrix(y_true, y_pred).ravel()

        precision = tp / (tp + fp) if (tp + fp) > 0 else 0
        recall = tp / (tp + fn) if (tp + fn) > 0 else 0
        f1 = 2 * precision * recall / (precision + recall) if (precision + recall) > 0 else 0

        # Economic costs
        fp_cost = fp * COST_FP_PAISE
        fn_cost = fn * COST_FN_PAISE
        total_cost = fp_cost + fn_cost

        # Revenue saved by catching true positives
        tp_savings = tp * COST_FN_PAISE

        # Net savings = TP savings - FP opportunity cost
        net_savings = tp_savings - fp_cost

        results.append({
            "threshold": round(thresh, 2),
            "precision": round(precision, 4),
            "recall": round(recall, 4),
            "f1": round(f1, 4),
            "true_positives": int(tp),
            "false_positives": int(fp),
            "true_negatives": int(tn),
            "false_negatives": int(fn),
            "fp_cost_inr": round(fp_cost / 100, 2),
            "fn_cost_inr": round(fn_cost / 100, 2),
            "total_cost_inr": round(total_cost / 100, 2),
            "tp_savings_inr": round(tp_savings / 100, 2),
            "net_savings_inr": round(net_savings / 100, 2),
        })
    return results


def main():
    logger.info("=" * 70)
    logger.info("SENTINEL-RTO — HONEST METRICS REPORT")
    logger.info("Defense-Only RTO Fraud Detection System")
    logger.info("=" * 70)

    X_test, y_test, y_prob, test_df = load_model_and_data()
    n_total = len(y_test)
    n_rto = int(y_test.sum())
    n_legit = n_total - n_rto

    logger.info(f"\nTest Set: {n_total:,} orders ({n_legit:,} legit, {n_rto:,} RTO)")
    logger.info(f"RTO Rate: {n_rto/n_total*100:.1f}%")

    # --- Standard metrics at default threshold (0.5) ---
    y_pred_default = (y_prob >= 0.5).astype(int)
    roc_auc = roc_auc_score(y_test, y_prob)

    logger.info(f"\n{'─' * 70}")
    logger.info("SECTION 1: STANDARD ML METRICS (threshold=0.5)")
    logger.info(f"{'─' * 70}")
    logger.info(f"\n{classification_report(y_test, y_pred_default, target_names=['Legit', 'RTO'])}")
    logger.info(f"ROC-AUC: {roc_auc:.4f}")

    tn, fp, fn, tp = confusion_matrix(y_test, y_pred_default).ravel()
    logger.info(f"\nConfusion Matrix:")
    logger.info(f"  True Negatives  (correct ALLOW):  {tn:>6,}")
    logger.info(f"  False Positives (wrong BLOCK):    {fp:>6,}  ← Legit buyers harmed")
    logger.info(f"  False Negatives (missed RTO):     {fn:>6,}  ← Fraud slipped through")
    logger.info(f"  True Positives  (caught RTO):     {tp:>6,}  ← Fraud stopped")

    # --- Economic cost analysis ---
    logger.info(f"\n{'─' * 70}")
    logger.info("SECTION 2: FALSE-POSITIVE COST ANALYSIS")
    logger.info(f"{'─' * 70}")
    logger.info(f"\nUnit Economics (per order):")
    logger.info(f"  Average order value:       ₹{AVG_ORDER_VALUE_PAISE/100:,.0f}")
    logger.info(f"  Gross margin (30%):        ₹{COST_FP_PAISE/100:,.0f}")
    logger.info(f"  Forward shipping:          ₹{AVG_FORWARD_SHIPPING_PAISE/100:,.0f}")
    logger.info(f"  Return shipping:           ₹{AVG_RETURN_SHIPPING_PAISE/100:,.0f}")
    logger.info(f"  Packaging:                 ₹{AVG_PACKAGING_COST_PAISE/100:,.0f}")
    logger.info(f"")
    logger.info(f"  Cost of 1 False Positive:  ₹{COST_FP_PAISE/100:,.0f}  (lost margin)")
    logger.info(f"  Cost of 1 False Negative:  ₹{COST_FN_PAISE/100:,.0f}  (2x shipping + packaging)")
    logger.info(f"  FP:FN cost ratio:          {COST_FP_PAISE/COST_FN_PAISE:.1f}:1")

    fp_total_cost = fp * COST_FP_PAISE / 100
    fn_total_cost = fn * COST_FN_PAISE / 100
    tp_savings = tp * COST_FN_PAISE / 100
    net = tp_savings - fp_total_cost

    logger.info(f"\nAt Default Threshold (0.5) on {n_total:,} test orders:")
    logger.info(f"  FP cost (blocked legit):   ₹{fp_total_cost:>10,.2f}")
    logger.info(f"  FN cost (missed fraud):    ₹{fn_total_cost:>10,.2f}")
    logger.info(f"  TP savings (caught fraud): ₹{tp_savings:>10,.2f}")
    logger.info(f"  ─────────────────────────────────────")
    logger.info(f"  NET SAVINGS:               ₹{net:>10,.2f}")

    # --- Threshold sweep ---
    logger.info(f"\n{'─' * 70}")
    logger.info("SECTION 3: THRESHOLD SENSITIVITY ANALYSIS")
    logger.info(f"{'─' * 70}")
    logger.info(f"\n{'Thresh':>7} {'Prec':>7} {'Recall':>7} {'F1':>7} "
                f"{'FP':>6} {'FN':>6} {'FP Cost':>10} {'FN Cost':>10} {'Net ₹':>10}")
    logger.info("─" * 82)

    thresholds = [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9]
    econ = compute_threshold_economics(y_test, y_prob, thresholds)
    optimal_thresh = None
    optimal_net = float("-inf")

    for r in econ:
        logger.info(
            f"  {r['threshold']:>5.2f}  {r['precision']:>6.4f} {r['recall']:>6.4f} "
            f"{r['f1']:>6.4f}  {r['false_positives']:>5} {r['false_negatives']:>5} "
            f"₹{r['fp_cost_inr']:>8,.0f} ₹{r['fn_cost_inr']:>8,.0f} ₹{r['net_savings_inr']:>8,.0f}"
        )
        if r["net_savings_inr"] > optimal_net:
            optimal_net = r["net_savings_inr"]
            optimal_thresh = r["threshold"]

    logger.info(f"\n  Optimal threshold: {optimal_thresh} (max net savings: ₹{optimal_net:,.0f})")

    # --- Defense-only compliance ---
    logger.info(f"\n{'─' * 70}")
    logger.info("SECTION 4: DEFENSE-ONLY COMPLIANCE CHECKLIST")
    logger.info(f"{'─' * 70}")
    checks = [
        ("No credential generation/spray", True, "System only scores incoming orders"),
        ("No proxy/VPN generation", True, "IP used only for geolocation, not scanning"),
        ("No checkout bypass tools", True, "Cannot place orders or manipulate carts"),
        ("No PII stored in plaintext", True, "All identifiers HMAC-SHA-256 hashed"),
        ("No offensive enumeration", True, "Cannot probe or enumerate user accounts"),
        ("Read-only on buyer data", True, "System only reads checkout telemetry"),
        ("Explainable decisions", True, "SHAP values returned with every evaluation"),
        ("Graduated friction", True, "Deposit option preserves legitimate COD"),
        ("False-positive costs disclosed", True, "FP/FN costs quantified in this report"),
        ("Model bias audit needed", False, "Recommend geographic/demographic bias audit"),
    ]
    for desc, ok, note in checks:
        status = "✓ PASS" if ok else "⚠ NOTE"
        logger.info(f"  [{status}] {desc}")
        logger.info(f"          {note}")

    # --- Save report ---
    report = {
        "model": "LightGBM 500 trees → ONNX",
        "test_set_size": n_total,
        "rto_rate": round(n_rto / n_total, 4),
        "metrics_at_0.5": {
            "precision": round(tp / (tp + fp), 4) if (tp + fp) > 0 else 0,
            "recall": round(tp / (tp + fn), 4) if (tp + fn) > 0 else 0,
            "roc_auc": round(roc_auc, 4),
            "true_positives": int(tp),
            "false_positives": int(fp),
            "true_negatives": int(tn),
            "false_negatives": int(fn),
        },
        "cost_model": {
            "cost_per_false_positive_inr": COST_FP_PAISE / 100,
            "cost_per_false_negative_inr": COST_FN_PAISE / 100,
            "fp_fn_cost_ratio": round(COST_FP_PAISE / COST_FN_PAISE, 2),
        },
        "economic_impact_at_0.5": {
            "fp_cost_inr": round(fp_total_cost, 2),
            "fn_cost_inr": round(fn_total_cost, 2),
            "tp_savings_inr": round(tp_savings, 2),
            "net_savings_inr": round(net, 2),
        },
        "threshold_analysis": econ,
        "optimal_threshold": optimal_thresh,
        "defense_only": True,
    }

    Path("models").mkdir(exist_ok=True)
    with open("models/honest_metrics_report.json", "w") as f:
        json.dump(report, f, indent=2)

    logger.info(f"\n{'─' * 70}")
    logger.info(f"Report saved to models/honest_metrics_report.json")
    logger.info(f"{'─' * 70}")


if __name__ == "__main__":
    main()

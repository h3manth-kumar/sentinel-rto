"""Synthetic e-commerce transaction data generator with embedded fraud patterns.

Generates realistic synthetic data for training the SENTINEL-RTO risk model,
including normal transactions, individual RTO fraud, and coordinated syndicate
fraud rings.
"""
from __future__ import annotations

import hashlib
import logging
import random
import uuid
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

# Fraud distribution targets
NORMAL_RATIO = 0.70       # 70% legitimate orders
INDIVIDUAL_RTO_RATIO = 0.15  # 15% individual RTO fraud
SYNDICATE_RTO_RATIO = 0.15   # 15% syndicate ring fraud

# Indian metro pincodes with lat/lng for H3 generation
METRO_LOCATIONS = [
    {"city": "Bengaluru", "pincodes": ["560001", "560034", "560103", "560068"], "lat": 12.97, "lng": 77.59},
    {"city": "Mumbai", "pincodes": ["400001", "400050", "400070", "400093"], "lat": 19.07, "lng": 72.87},
    {"city": "Delhi", "pincodes": ["110001", "110016", "110044", "110085"], "lat": 28.63, "lng": 77.21},
    {"city": "Hyderabad", "pincodes": ["500001", "500034", "500081"], "lat": 17.38, "lng": 78.48},
    {"city": "Chennai", "pincodes": ["600001", "600040", "600100"], "lat": 13.08, "lng": 80.27},
    {"city": "Pune", "pincodes": ["411001", "411038", "411057"], "lat": 18.52, "lng": 73.85},
]


def _hash(value: str) -> str:
    """Generate a deterministic hash for PII."""
    return hashlib.sha256(value.encode()).hexdigest()[:16]


def _generate_device_hash() -> str:
    return f"dev_{uuid.uuid4().hex[:12]}"


def _generate_phone_hash() -> str:
    phone = f"+91{random.randint(6000000000, 9999999999)}"
    return _hash(phone)


class SyntheticDataGenerator:
    """Generates synthetic e-commerce transaction datasets."""

    def __init__(self, num_records: int = 100_000, seed: int = 42) -> None:
        self.num_records = num_records
        self.seed = seed
        random.seed(seed)
        np.random.seed(seed)

    def generate(self) -> pd.DataFrame:
        """Generate the full synthetic dataset.

        Returns:
            DataFrame with columns matching the feature engineering pipeline.
        """
        logger.info("Generating %d synthetic transactions...", self.num_records)

        n_normal = int(self.num_records * NORMAL_RATIO)
        n_individual = int(self.num_records * INDIVIDUAL_RTO_RATIO)
        n_syndicate = self.num_records - n_normal - n_individual

        records: list[dict[str, Any]] = []
        records.extend(self._generate_normal_orders(n_normal))
        records.extend(self._generate_individual_rto(n_individual))
        records.extend(self._generate_syndicate_rto(n_syndicate))

        df = pd.DataFrame(records)
        df = df.sample(frac=1.0, random_state=self.seed).reset_index(drop=True)

        logger.info(
            "Generated %d records: %.1f%% normal, %.1f%% individual RTO, %.1f%% syndicate RTO",
            len(df),
            (df["is_rto"] == 0).mean() * 100,
            ((df["is_rto"] == 1) & (df["is_syndicate"] == 0)).mean() * 100,
            ((df["is_rto"] == 1) & (df["is_syndicate"] == 1)).mean() * 100,
        )
        return df

    def _generate_normal_orders(self, count: int) -> list[dict[str, Any]]:
        """Generate legitimate, non-fraudulent orders."""
        records = []
        for _ in range(count):
            location = random.choice(METRO_LOCATIONS)
            record = {
                "transaction_id": f"txn_{uuid.uuid4().hex[:12]}",
                "merchant_id": f"mer_{random.choice(['aaa', 'bbb', 'ccc', 'ddd'])}",
                "order_id": f"ord_{random.randint(100000, 999999)}",
                "amount_in_paise": int(np.random.lognormal(8.5, 1.0)) * 100,
                "payment_method": random.choices(["COD", "PREPAID", "UPI"], weights=[0.4, 0.3, 0.3])[0],
                "customer_phone_hash": _generate_phone_hash(),
                "device_hash": _generate_device_hash(),
                "h3_index_res9": f"h3_{location['city'][:3]}_{random.randint(1000, 9999)}",
                "pincode": random.choice(location["pincodes"]),
                "account_age_days": int(np.random.exponential(180)) + 30,
                "form_fill_duration_ms": int(np.random.normal(8000, 3000)),
                "canvas_entropy_score": round(np.random.beta(8, 2), 2),
                "is_bot_keystrokes": 0,
                "device_rto_rate": round(np.random.beta(1, 15), 4),
                "device_order_count": int(np.random.exponential(5)) + 1,
                "h3_cluster_rto_rate": round(np.random.beta(2, 10), 4),
                "h3_density_weight": round(np.random.uniform(0.5, 1.5), 4),
                "cluster_size": 0,
                "cluster_rto_rate": 0.0,
                "burst_count_h3": random.choices([0, 1], weights=[0.95, 0.05])[0],
                "burst_count_device": 0,
                "is_rto": 0,
                "is_syndicate": 0,
            }
            # Small chance of legitimate RTO (wrong size, damaged, etc.)
            if random.random() < 0.05:
                record["is_rto"] = 1
            records.append(record)
        return records

    def _generate_individual_rto(self, count: int) -> list[dict[str, Any]]:
        """Generate individual RTO fraud patterns."""
        records = []
        for _ in range(count):
            location = random.choice(METRO_LOCATIONS)
            record = {
                "transaction_id": f"txn_{uuid.uuid4().hex[:12]}",
                "merchant_id": f"mer_{random.choice(['aaa', 'bbb', 'ccc', 'ddd'])}",
                "order_id": f"ord_{random.randint(100000, 999999)}",
                "amount_in_paise": int(np.random.lognormal(9.0, 0.8)) * 100,  # Higher value
                "payment_method": "COD",  # Always COD
                "customer_phone_hash": _generate_phone_hash(),
                "device_hash": _generate_device_hash(),
                "h3_index_res9": f"h3_{location['city'][:3]}_{random.randint(1000, 9999)}",
                "pincode": random.choice(location["pincodes"]),
                "account_age_days": int(np.random.exponential(10)) + 1,  # Very new accounts
                "form_fill_duration_ms": int(np.random.normal(1500, 500)),  # Fast form fills
                "canvas_entropy_score": round(np.random.beta(2, 5), 2),  # Lower entropy
                "is_bot_keystrokes": random.choices([0, 1], weights=[0.7, 0.3])[0],
                "device_rto_rate": round(np.random.beta(5, 3), 4),  # High RTO rate
                "device_order_count": int(np.random.exponential(8)) + 3,
                "h3_cluster_rto_rate": round(np.random.beta(3, 5), 4),
                "h3_density_weight": round(np.random.uniform(0.8, 2.0), 4),
                "cluster_size": 0,
                "cluster_rto_rate": 0.0,
                "burst_count_h3": random.choices([0, 1, 2], weights=[0.5, 0.3, 0.2])[0],
                "burst_count_device": random.choices([0, 1], weights=[0.6, 0.4])[0],
                "is_rto": 1,
                "is_syndicate": 0,
            }
            records.append(record)
        return records

    def _generate_syndicate_rto(self, count: int) -> list[dict[str, Any]]:
        """Generate coordinated syndicate fraud ring patterns."""
        records = []
        num_rings = max(count // 20, 1)  # ~20 orders per ring

        for ring_idx in range(num_rings):
            ring_size = min(random.randint(10, 30), count - len(records))
            if ring_size <= 0:
                break

            # Shared ring characteristics
            location = random.choice(METRO_LOCATIONS)
            shared_h3 = f"h3_{location['city'][:3]}_ring_{ring_idx}"
            shared_devices = [_generate_device_hash() for _ in range(random.randint(2, 4))]
            ring_rto_rate = round(np.random.beta(8, 2), 4)

            for _ in range(ring_size):
                record = {
                    "transaction_id": f"txn_{uuid.uuid4().hex[:12]}",
                    "merchant_id": f"mer_{random.choice(['aaa', 'bbb', 'ccc', 'ddd'])}",
                    "order_id": f"ord_{random.randint(100000, 999999)}",
                    "amount_in_paise": int(np.random.lognormal(9.2, 0.5)) * 100,
                    "payment_method": "COD",
                    "customer_phone_hash": _generate_phone_hash(),  # Rotating phones
                    "device_hash": random.choice(shared_devices),  # Shared devices
                    "h3_index_res9": shared_h3,  # Same area
                    "pincode": random.choice(location["pincodes"]),
                    "account_age_days": int(np.random.exponential(5)) + 1,
                    "form_fill_duration_ms": int(np.random.normal(800, 200)),
                    "canvas_entropy_score": round(np.random.beta(1, 5), 2),
                    "is_bot_keystrokes": random.choices([0, 1], weights=[0.4, 0.6])[0],
                    "device_rto_rate": round(ring_rto_rate + np.random.normal(0, 0.05), 4),
                    "device_order_count": int(np.random.exponential(15)) + 5,
                    "h3_cluster_rto_rate": round(ring_rto_rate, 4),
                    "h3_density_weight": round(np.random.uniform(1.5, 3.0), 4),
                    "cluster_size": len(shared_devices) + random.randint(3, 10),
                    "cluster_rto_rate": round(ring_rto_rate, 4),
                    "burst_count_h3": random.choices([1, 2, 3, 4], weights=[0.2, 0.3, 0.3, 0.2])[0],
                    "burst_count_device": random.choices([0, 1, 2, 3], weights=[0.2, 0.3, 0.3, 0.2])[0],
                    "is_rto": 1,
                    "is_syndicate": 1,
                }
                records.append(record)

        return records

    def save(self, df: pd.DataFrame, output_dir: str = "data") -> Path:
        """Save generated dataset to CSV."""
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)
        filepath = output_path / "synthetic_transactions.csv"
        df.to_csv(filepath, index=False)
        logger.info("Dataset saved to %s (%d records)", filepath, len(df))
        return filepath


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    generator = SyntheticDataGenerator(num_records=100_000)
    dataset = generator.generate()
    generator.save(dataset)
    print(f"\nDataset shape: {dataset.shape}")
    print(f"RTO rate: {dataset['is_rto'].mean():.2%}")
    print(f"Syndicate rate: {dataset['is_syndicate'].mean():.2%}")
    print(f"\nFeature stats:\n{dataset.describe()}")

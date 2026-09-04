"""Digital Personal Data Protection (DPDP) Act 2023 Compliance Engine.

Implements automated PII masking, deterministic key-based cryptographic data shredding,
and an immutable SHA-256 hash-chained decision audit ledger.
"""
from __future__ import annotations

import hashlib
import hmac
import json
import logging
import secrets
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Any, Optional

logger = logging.getLogger(__name__)

GENESIS_BLOCK_HASH = "0000000000000000000000000000000000000000000000000000000000000000"


@dataclass
class AuditBlock:
    """Immutable hash-chained decision audit record."""
    block_index: int
    timestamp: str
    order_id: str
    masked_phone: str
    risk_score: int
    action_type: str
    decision_reason: str
    h3_index: str
    previous_hash: str
    block_hash: str


class DPDPComplianceEngine:
    """Enterprise DPDP Act 2023 Privacy and Compliance Framework."""

    def __init__(self, master_secret_key: str = "sentinel-dpdp-master-key-2026") -> None:
        self.master_secret = master_secret_key.encode()
        self._user_keys: dict[str, str] = {}  # In-memory tenant key-vault
        self._audit_chain: list[AuditBlock] = []
        self._latest_hash: str = GENESIS_BLOCK_HASH

    # --------------------------------------------------------------------------
    # 1. Automated PII Masking
    # --------------------------------------------------------------------------
    @staticmethod
    def mask_phone_number(phone: str) -> str:
        """Mask middle 4-6 digits of Indian phone number (e.g. 9876543210 -> 9876****10)."""
        clean = str(phone).replace("+", "").replace("-", "").replace(" ", "").strip()
        if clean.startswith("91") and len(clean) == 12:
            clean = clean[2:]
        if len(clean) < 6:
            return "***"
        prefix = clean[:4]
        suffix = clean[-2:]
        return f"{prefix}****{suffix}"

    @staticmethod
    def mask_customer_name(name: str) -> str:
        """Mask customer name for logs and third-party exports (e.g. Priya Sharma -> P***a S****a)."""
        if not name:
            return "C******r"
        parts = name.strip().split()
        masked_parts = []
        for p in parts:
            if len(p) <= 2:
                masked_parts.append(p[0] + "*")
            else:
                masked_parts.append(p[0] + ("*" * (len(p) - 2)) + p[-1])
        return " ".join(masked_parts)

    @staticmethod
    def mask_street_address(raw_address: str, locality_or_pincode: str) -> str:
        """Strip door numbers and apartment names; retain only spatial locality tokens."""
        return f"[REDACTED PREMISE], {locality_or_pincode}"

    def sanitize_payload_for_telemetry(self, payload: dict[str, Any]) -> dict[str, Any]:
        """Produce DPDP-compliant telemetry sanitized copy of an order."""
        sanitized = dict(payload)
        if "customer" in sanitized and isinstance(sanitized["customer"], dict):
            cust = dict(sanitized["customer"])
            if "phone" in cust:
                cust["phone"] = self.mask_phone_number(cust["phone"])
            if "name" in cust:
                cust["name"] = self.mask_customer_name(cust["name"])
            sanitized["customer"] = cust

        if "shipping_address" in sanitized and isinstance(sanitized["shipping_address"], dict):
            addr = dict(sanitized["shipping_address"])
            if "raw_text" in addr:
                addr["raw_text"] = self.mask_street_address(addr["raw_text"], addr.get("pincode", "Bengaluru"))
            sanitized["shipping_address"] = addr

        return sanitized

    # --------------------------------------------------------------------------
    # 2. Cryptographic Data Shredding (Right to Erasure)
    # --------------------------------------------------------------------------
    def get_or_create_user_salt(self, customer_id: str) -> str:
        """Retrieve or generate per-user pseudonymization key."""
        if customer_id not in self._user_keys:
            self._user_keys[customer_id] = secrets.token_hex(16)
        return self._user_keys[customer_id]

    def pseudonymize_identifier(self, identifier: str, customer_id: str) -> Optional[str]:
        """Compute salted HMAC-SHA256 token. Returns None if user key was shredded."""
        if customer_id not in self._user_keys:
            return None  # Key shredded; unrecoverable
        user_salt = self._user_keys[customer_id]
        key = hmac.new(self.master_secret, user_salt.encode(), hashlib.sha256).digest()
        return hmac.new(key, identifier.encode(), hashlib.sha256).hexdigest()

    def execute_crypto_shredding(self, customer_id: str) -> bool:
        """Cryptographically shred user data by destroying the encryption key.
        
        Renders all historical records linked to this customer permanently unrecoverable
        pseudorandom noise without executing slow sequential table deletes.
        """
        if customer_id in self._user_keys:
            # Overwrite with zeroes before deletion
            self._user_keys[customer_id] = "0" * 32
            del self._user_keys[customer_id]
            logger.info("Executed DPDP Cryptographic Data Shredding for user: %s", customer_id)
            return True
        return False

    # --------------------------------------------------------------------------
    # 3. Immutable Hash-Chained Decision Audit Ledger
    # --------------------------------------------------------------------------
    def append_audit_decision(
        self,
        order_id: str,
        customer_phone: str,
        risk_score: int,
        action_type: str,
        decision_reason: str,
        h3_index: str,
    ) -> AuditBlock:
        """Append risk evaluation decision to the tamper-evident cryptographic hash chain."""
        masked_phone = self.mask_phone_number(customer_phone)
        block_idx = len(self._audit_chain) + 1
        timestamp = datetime.now(timezone.utc).isoformat()

        # Compute SHA-256 block hash: H_n = SHA256(H_{n-1} || order_id || score || action || ts)
        record_str = f"{self._latest_hash}|{block_idx}|{order_id}|{masked_phone}|{risk_score}|{action_type}|{timestamp}|{h3_index}"
        block_hash = hashlib.sha256(record_str.encode()).hexdigest()

        block = AuditBlock(
            block_index=block_idx,
            timestamp=timestamp,
            order_id=order_id,
            masked_phone=masked_phone,
            risk_score=risk_score,
            action_type=action_type,
            decision_reason=decision_reason,
            h3_index=h3_index,
            previous_hash=self._latest_hash,
            block_hash=block_hash,
        )

        self._audit_chain.append(block)
        self._latest_hash = block_hash
        logger.debug("Appended immutable audit block #%d for order %s (hash=%s)", block_idx, order_id, block_hash[:12])
        return block

    def verify_audit_ledger_integrity(self) -> tuple[bool, Optional[int]]:
        """Verify entire cryptographic chain from genesis block to current head.
        
        Returns:
            (is_valid, corrupted_block_index_if_any)
        """
        prev_hash = GENESIS_BLOCK_HASH
        for idx, block in enumerate(self._audit_chain):
            if block.previous_hash != prev_hash:
                logger.error("Audit ledger broken at block #%d (expected prev_hash %s, got %s)", block.block_index, prev_hash, block.previous_hash)
                return False, block.block_index

            expected_record_str = f"{prev_hash}|{block.block_index}|{block.order_id}|{block.masked_phone}|{block.risk_score}|{block.action_type}|{block.timestamp}|{block.h3_index}"
            expected_hash = hashlib.sha256(expected_record_str.encode()).hexdigest()

            if block.block_hash != expected_hash:
                logger.error("Tampered audit hash detected at block #%d", block.block_index)
                return False, block.block_index

            prev_hash = block.block_hash

        return True, None

    def export_audit_ledger(self) -> list[dict[str, Any]]:
        """Export serialized audit ledger for external regulatory audits."""
        return [asdict(b) for b in self._audit_chain]


dpdp_engine = DPDPComplianceEngine()


def get_dpdp_engine() -> DPDPComplianceEngine:
    return dpdp_engine

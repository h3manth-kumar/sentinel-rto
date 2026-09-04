"""Database models."""
from datetime import datetime
from typing import Any, Optional

from sqlalchemy import BigInteger, Boolean, ForeignKey, Index, Integer, Numeric, String, func
from sqlalchemy.dialects.postgresql import JSONB, TIMESTAMP
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase):
    pass


class Merchant(Base):
    __tablename__ = "merchants"

    merchant_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    api_key_hash: Mapped[str] = mapped_column(String(128), nullable=False)
    deposit_step_up_enabled: Mapped[bool] = mapped_column(Boolean, default=True)
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), default=func.now()
    )


class AddressH3(Base):
    __tablename__ = "addresses_h3"

    h3_index_res9: Mapped[str] = mapped_column(String(15), primary_key=True)
    h3_index_res8: Mapped[str] = mapped_column(String(15), nullable=False)
    pincode: Mapped[str] = mapped_column(String(10), nullable=False)
    total_orders: Mapped[int] = mapped_column(Integer, default=0)
    rto_deliveries: Mapped[int] = mapped_column(Integer, default=0)
    last_updated: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), default=func.now()
    )

    __table_args__ = (Index("idx_addresses_h3_res8", "h3_index_res8"),)


class Device(Base):
    __tablename__ = "devices"

    device_hash: Mapped[str] = mapped_column(String(64), primary_key=True)
    canvas_hash: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    first_seen: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), default=func.now()
    )
    last_seen: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), default=func.now()
    )
    is_proxy: Mapped[bool] = mapped_column(Boolean, default=False)
    associated_accounts_count: Mapped[int] = mapped_column(Integer, default=1)


class SyndicateCluster(Base):
    __tablename__ = "syndicate_clusters"

    cluster_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    root_entity_type: Mapped[str] = mapped_column(String(32), nullable=False)
    cluster_size: Mapped[int] = mapped_column(Integer, nullable=False)
    composite_rto_rate: Mapped[float] = mapped_column(Numeric(5, 4), nullable=False)
    is_blacklisted: Mapped[bool] = mapped_column(Boolean, default=False)
    discovered_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), default=func.now()
    )


class Transaction(Base):
    __tablename__ = "transactions"

    transaction_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    merchant_id: Mapped[str] = mapped_column(
        String(64), ForeignKey("merchants.merchant_id"), nullable=False
    )
    order_id: Mapped[str] = mapped_column(String(128), nullable=False)
    amount_in_paise: Mapped[int] = mapped_column(BigInteger, nullable=False)
    payment_method: Mapped[str] = mapped_column(String(32), nullable=False)
    customer_phone_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    device_hash: Mapped[str] = mapped_column(
        String(64), ForeignKey("devices.device_hash"), nullable=False
    )
    h3_index_res9: Mapped[str] = mapped_column(
        String(15), ForeignKey("addresses_h3.h3_index_res9"), nullable=False
    )
    status: Mapped[str] = mapped_column(String(32), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), default=func.now()
    )

    __table_args__ = (
        Index("idx_transactions_phone", "customer_phone_hash"),
        Index("idx_transactions_created_at", "created_at"),
    )


class RiskEvaluation(Base):
    __tablename__ = "risk_evaluations"

    evaluation_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    transaction_id: Mapped[str] = mapped_column(
        String(64), ForeignKey("transactions.transaction_id"), nullable=False
    )
    risk_score: Mapped[int] = mapped_column(Integer, nullable=False)
    risk_tier: Mapped[str] = mapped_column(String(32), nullable=False)
    decision_action: Mapped[str] = mapped_column(String(64), nullable=False)
    shap_attribution: Mapped[Any] = mapped_column(JSONB, nullable=False)
    total_latency_ms: Mapped[float] = mapped_column(Numeric(6, 2), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), default=func.now()
    )

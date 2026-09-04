"""initial schema

Revision ID: 001_initial_schema
Revises: 
Create Date: 2026-08-28 23:29:06.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = '001_initial_schema'
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # 1. merchants
    op.create_table('merchants',
        sa.Column('merchant_id', sa.String(length=64), nullable=False),
        sa.Column('name', sa.String(length=255), nullable=False),
        sa.Column('api_key_hash', sa.String(length=128), nullable=False),
        sa.Column('deposit_step_up_enabled', sa.Boolean(), server_default='true', nullable=True),
        sa.Column('created_at', postgresql.TIMESTAMP(timezone=True), server_default=sa.text('CURRENT_TIMESTAMP'), nullable=True),
        sa.PrimaryKeyConstraint('merchant_id')
    )

    # 2. addresses_h3
    op.create_table('addresses_h3',
        sa.Column('h3_index_res9', sa.String(length=15), nullable=False),
        sa.Column('h3_index_res8', sa.String(length=15), nullable=False),
        sa.Column('pincode', sa.String(length=10), nullable=False),
        sa.Column('total_orders', sa.Integer(), server_default='0', nullable=True),
        sa.Column('rto_deliveries', sa.Integer(), server_default='0', nullable=True),
        sa.Column('last_updated', postgresql.TIMESTAMP(timezone=True), server_default=sa.text('CURRENT_TIMESTAMP'), nullable=True),
        sa.PrimaryKeyConstraint('h3_index_res9')
    )
    op.create_index('idx_addresses_h3_res8', 'addresses_h3', ['h3_index_res8'], unique=False)

    # 3. devices
    op.create_table('devices',
        sa.Column('device_hash', sa.String(length=64), nullable=False),
        sa.Column('canvas_hash', sa.String(length=64), nullable=True),
        sa.Column('first_seen', postgresql.TIMESTAMP(timezone=True), server_default=sa.text('CURRENT_TIMESTAMP'), nullable=True),
        sa.Column('last_seen', postgresql.TIMESTAMP(timezone=True), server_default=sa.text('CURRENT_TIMESTAMP'), nullable=True),
        sa.Column('is_proxy', sa.Boolean(), server_default='false', nullable=True),
        sa.Column('associated_accounts_count', sa.Integer(), server_default='1', nullable=True),
        sa.PrimaryKeyConstraint('device_hash')
    )

    # 4. syndicate_clusters
    op.create_table('syndicate_clusters',
        sa.Column('cluster_id', sa.String(length=64), nullable=False),
        sa.Column('root_entity_type', sa.String(length=32), nullable=False),
        sa.Column('cluster_size', sa.Integer(), nullable=False),
        sa.Column('composite_rto_rate', sa.Numeric(precision=5, scale=4), nullable=False),
        sa.Column('is_blacklisted', sa.Boolean(), server_default='false', nullable=True),
        sa.Column('discovered_at', postgresql.TIMESTAMP(timezone=True), server_default=sa.text('CURRENT_TIMESTAMP'), nullable=True),
        sa.PrimaryKeyConstraint('cluster_id')
    )

    # 5. transactions
    op.create_table('transactions',
        sa.Column('transaction_id', sa.String(length=64), nullable=False),
        sa.Column('merchant_id', sa.String(length=64), nullable=False),
        sa.Column('order_id', sa.String(length=128), nullable=False),
        sa.Column('amount_in_paise', sa.BigInteger(), nullable=False),
        sa.Column('payment_method', sa.String(length=32), nullable=False),
        sa.Column('customer_phone_hash', sa.String(length=64), nullable=False),
        sa.Column('device_hash', sa.String(length=64), nullable=False),
        sa.Column('h3_index_res9', sa.String(length=15), nullable=False),
        sa.Column('status', sa.String(length=32), nullable=False),
        sa.Column('created_at', postgresql.TIMESTAMP(timezone=True), server_default=sa.text('CURRENT_TIMESTAMP'), nullable=True),
        sa.ForeignKeyConstraint(['device_hash'], ['devices.device_hash'], ),
        sa.ForeignKeyConstraint(['h3_index_res9'], ['addresses_h3.h3_index_res9'], ),
        sa.ForeignKeyConstraint(['merchant_id'], ['merchants.merchant_id'], ),
        sa.PrimaryKeyConstraint('transaction_id')
    )
    op.create_index('idx_transactions_phone', 'transactions', ['customer_phone_hash'], unique=False)
    op.create_index('idx_transactions_created_at', 'transactions', ['created_at'], unique=False)

    # 6. risk_evaluations
    op.create_table('risk_evaluations',
        sa.Column('evaluation_id', sa.String(length=64), nullable=False),
        sa.Column('transaction_id', sa.String(length=64), nullable=False),
        sa.Column('risk_score', sa.Integer(), nullable=False),
        sa.Column('risk_tier', sa.String(length=32), nullable=False),
        sa.Column('decision_action', sa.String(length=64), nullable=False),
        sa.Column('shap_attribution', postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column('total_latency_ms', sa.Numeric(precision=6, scale=2), nullable=False),
        sa.Column('created_at', postgresql.TIMESTAMP(timezone=True), server_default=sa.text('CURRENT_TIMESTAMP'), nullable=True),
        sa.ForeignKeyConstraint(['transaction_id'], ['transactions.transaction_id'], ),
        sa.PrimaryKeyConstraint('evaluation_id')
    )


def downgrade() -> None:
    op.drop_table('risk_evaluations')
    
    op.drop_index('idx_transactions_created_at', table_name='transactions')
    op.drop_index('idx_transactions_phone', table_name='transactions')
    op.drop_table('transactions')
    
    op.drop_table('syndicate_clusters')
    
    op.drop_table('devices')
    
    op.drop_index('idx_addresses_h3_res8', table_name='addresses_h3')
    op.drop_table('addresses_h3')
    
    op.drop_table('merchants')

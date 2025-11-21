"""create_fund_manager_table

Revision ID: a2d38996d493
Revises: b2c3d4e5f6g7
Create Date: 2025-09-26 15:06:03.829409

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'a2d38996d493'
down_revision: Union[str, None] = 'c3d4e5f6g7h8'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Create fund_manager table in public schema
    op.create_table('fund_manager',
        sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
        sa.Column('fund_manager_name', sa.String(length=255), nullable=False),
        sa.Column('contact_title', sa.String(length=100), nullable=True),
        sa.Column('contact_first_name', sa.String(length=100), nullable=False),
        sa.Column('contact_last_name', sa.String(length=100), nullable=False),
        sa.Column('contact_email', sa.String(length=255), nullable=False),
        sa.Column('contact_number', sa.String(length=50), nullable=True),
        sa.Column('status', sa.String(length=20), nullable=False, server_default='active'),
        sa.Column('created_at', sa.DateTime(), nullable=True, server_default=sa.text('CURRENT_TIMESTAMP')),
        sa.Column('updated_at', sa.DateTime(), nullable=True, server_default=sa.text('CURRENT_TIMESTAMP')),
        sa.PrimaryKeyConstraint('id'),
        sa.CheckConstraint("status IN ('active', 'inactive')", name='chk_fund_manager_status'),
        schema='public'
    )
    
    # Create indexes for better query performance
    op.create_index(op.f('ix_public_fund_manager_name'), 'fund_manager', ['fund_manager_name'], unique=True, schema='public')
    op.create_index(op.f('ix_public_fund_manager_email'), 'fund_manager', ['contact_email'], unique=True, schema='public')
    op.create_index(op.f('ix_public_fund_manager_status'), 'fund_manager', ['status'], unique=False, schema='public')


def downgrade() -> None:
    # Drop the fund_manager table and its indexes
    op.drop_index(op.f('ix_public_fund_manager_status'), table_name='fund_manager', schema='public')
    op.drop_index(op.f('ix_public_fund_manager_email'), table_name='fund_manager', schema='public')
    op.drop_index(op.f('ix_public_fund_manager_name'), table_name='fund_manager', schema='public')
    op.drop_table('fund_manager', schema='public')

"""Add calendar table to public schema

Revision ID: c3d4e5f6g7h8
Revises: b2c3d4e5f6g7
Create Date: 2025-01-27 12:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'c3d4e5f6g7h8'
down_revision: Union[str, None] = 'b2c3d4e5f6g7'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Create calendars table in public schema
    op.create_table('calendars',
        sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
        sa.Column('fund_id', sa.Integer(), nullable=False),
        sa.Column('frequency', sa.String(length=50), nullable=False),
        sa.Column('delay', sa.Integer(), nullable=False),
        sa.Column('documents', sa.JSON(), nullable=False),
        sa.Column('created_by', sa.String(length=255), nullable=False),
        sa.Column('updated_by', sa.String(length=255), nullable=True),
        sa.Column('created_at', sa.DateTime(), nullable=True),
        sa.Column('updated_at', sa.DateTime(), nullable=True),
        sa.Column('is_active', sa.Boolean(), nullable=True),
        sa.PrimaryKeyConstraint('id'),
        sa.ForeignKeyConstraint(['fund_id'], ['public.funds.id'], ondelete='CASCADE'),
        sa.CheckConstraint("frequency IN ('daily', 'weekly', 'monthly', 'quarterly', 'annually')", name='chk_frequency'),
        sa.CheckConstraint("delay >= 0", name='chk_delay_positive'),
        schema='public'
    )
    
    # Create indexes for better query performance
    op.create_index(op.f('ix_public_calendars_fund_id'), 'calendars', ['fund_id'], unique=False, schema='public')
    op.create_index(op.f('ix_public_calendars_frequency'), 'calendars', ['frequency'], unique=False, schema='public')
    op.create_index(op.f('ix_public_calendars_is_active'), 'calendars', ['is_active'], unique=False, schema='public')


def downgrade() -> None:
    # Drop the calendars table and its indexes
    op.drop_index(op.f('ix_public_calendars_is_active'), table_name='calendars', schema='public')
    op.drop_index(op.f('ix_public_calendars_frequency'), table_name='calendars', schema='public')
    op.drop_index(op.f('ix_public_calendars_fund_id'), table_name='calendars', schema='public')
    op.drop_table('calendars', schema='public')

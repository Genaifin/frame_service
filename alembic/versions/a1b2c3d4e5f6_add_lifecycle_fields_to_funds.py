"""Add lifecycle fields to funds table

Revision ID: a1b2c3d4e5f6
Revises: 777483469cff
Create Date: 2025-01-27 10:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'a1b2c3d4e5f6'
down_revision: Union[str, None] = 'a2adcc88aefd'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Add lifecycle fields to funds table
    op.add_column('funds', sa.Column('stage', sa.String(length=50), nullable=True), schema='public')
    op.add_column('funds', sa.Column('inception_date', sa.Date(), nullable=True), schema='public')
    op.add_column('funds', sa.Column('investment_start_date', sa.Date(), nullable=True), schema='public')
    op.add_column('funds', sa.Column('commitment_subscription', sa.Numeric(precision=18, scale=2), nullable=True), schema='public')
    
    # Add index for stage field for better query performance
    op.create_index(op.f('ix_public_funds_stage'), 'funds', ['stage'], unique=False, schema='public')


def downgrade() -> None:
    # Remove the added lifecycle fields
    op.drop_index(op.f('ix_public_funds_stage'), table_name='funds', schema='public')
    op.drop_column('funds', 'commitment_subscription', schema='public')
    op.drop_column('funds', 'investment_start_date', schema='public')
    op.drop_column('funds', 'inception_date', schema='public')
    op.drop_column('funds', 'stage', schema='public')

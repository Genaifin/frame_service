"""add_new_distribution_fields

Revision ID: d11d6897ab9a
Revises: 3fcb284c0bb5
Create Date: 2025-11-07 14:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = 'd11d6897ab9a'
down_revision: Union[str, None] = '3fcb284c0bb5'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Add new distribution fields to distributions table
    op.add_column('distributions', sa.Column('Carry', sa.Numeric(precision=18, scale=2), nullable=True))
    op.add_column('distributions', sa.Column('Clawback', sa.Numeric(precision=18, scale=2), nullable=True))
    op.add_column('distributions', sa.Column('RealizedGainCash', sa.Numeric(precision=18, scale=2), nullable=True))
    op.add_column('distributions', sa.Column('RealizedGainStock', sa.Numeric(precision=18, scale=2), nullable=True))
    op.add_column('distributions', sa.Column('RealizedLossCash', sa.Numeric(precision=18, scale=2), nullable=True))
    op.add_column('distributions', sa.Column('RealizedLossStock', sa.Numeric(precision=18, scale=2), nullable=True))
    op.add_column('distributions', sa.Column('ReturnOfCapitalManagementFees', sa.Numeric(precision=18, scale=2), nullable=True))
    op.add_column('distributions', sa.Column('ReturnOfCapitalPartnershipExpenses', sa.Numeric(precision=18, scale=2), nullable=True))
    op.add_column('distributions', sa.Column('ReturnOfCapitalStock', sa.Numeric(precision=18, scale=2), nullable=True))
    op.add_column('distributions', sa.Column('TemporaryReturnOfCapitalManagementFees', sa.Numeric(precision=18, scale=2), nullable=True))
    op.add_column('distributions', sa.Column('SubsequentCloseInterest', sa.Numeric(precision=18, scale=2), nullable=True))
    op.add_column('distributions', sa.Column('Other', sa.Numeric(precision=18, scale=2), nullable=True))


def downgrade() -> None:
    # Remove new distribution fields from distributions table
    op.drop_column('distributions', 'Other')
    op.drop_column('distributions', 'SubsequentCloseInterest')
    op.drop_column('distributions', 'TemporaryReturnOfCapitalManagementFees')
    op.drop_column('distributions', 'ReturnOfCapitalStock')
    op.drop_column('distributions', 'ReturnOfCapitalPartnershipExpenses')
    op.drop_column('distributions', 'ReturnOfCapitalManagementFees')
    op.drop_column('distributions', 'RealizedLossStock')
    op.drop_column('distributions', 'RealizedLossCash')
    op.drop_column('distributions', 'RealizedGainStock')
    op.drop_column('distributions', 'RealizedGainCash')
    op.drop_column('distributions', 'Clawback')
    op.drop_column('distributions', 'Carry')


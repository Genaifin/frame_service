"""Add threshold range fields to validation and ratio masters

Revision ID: f1g2h3i4j5k6
Revises: b1f0d004108f
Create Date: 2025-01-15 10:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'f1g2h3i4j5k6'
down_revision: Union[str, None] = 'b1f0d004108f'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Add threshold range fields to tbl_validation_master
    op.add_column('tbl_validation_master',
                  sa.Column('vcthreshold_abs_range', sa.String(length=20), nullable=True),
                  schema='validus')
    
    op.add_column('tbl_validation_master',
                  sa.Column('intthresholdmin', sa.Numeric(precision=30, scale=6), nullable=True),
                  schema='validus')
    
    op.add_column('tbl_validation_master',
                  sa.Column('intthresholdmax', sa.Numeric(precision=30, scale=6), nullable=True),
                  schema='validus')
    
    # Add threshold range fields to tbl_ratio_master
    op.add_column('tbl_ratio_master',
                  sa.Column('vcthreshold_abs_range', sa.String(length=20), nullable=True),
                  schema='validus')
    
    op.add_column('tbl_ratio_master',
                  sa.Column('intthresholdmin', sa.Numeric(precision=30, scale=6), nullable=True),
                  schema='validus')
    
    op.add_column('tbl_ratio_master',
                  sa.Column('intthresholdmax', sa.Numeric(precision=30, scale=6), nullable=True),
                  schema='validus')


def downgrade() -> None:
    # Remove threshold range fields from tbl_ratio_master
    op.drop_column('tbl_ratio_master', 'intthresholdmax', schema='validus')
    op.drop_column('tbl_ratio_master', 'intthresholdmin', schema='validus')
    op.drop_column('tbl_ratio_master', 'vcthreshold_abs_range', schema='validus')
    
    # Remove threshold range fields from tbl_validation_master
    op.drop_column('tbl_validation_master', 'intthresholdmax', schema='validus')
    op.drop_column('tbl_validation_master', 'intthresholdmin', schema='validus')
    op.drop_column('tbl_validation_master', 'vcthreshold_abs_range', schema='validus')


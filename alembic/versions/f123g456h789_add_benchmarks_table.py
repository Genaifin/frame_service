"""add_benchmarks_table

Revision ID: f123g456h789
Revises: b2c3d4e5f6g7
Create Date: 2025-09-22 15:30:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'f123g456h789'
down_revision: Union[str, None] = 'b2c3d4e5f6g7'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Create benchmarks table
    op.create_table('benchmarks',
        sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
        sa.Column('benchmark', sa.String(length=100), nullable=False),
        sa.Column('date', sa.Date(), nullable=False),
        sa.Column('value', sa.Numeric(precision=15, scale=4), nullable=False),
        sa.Column('extra_data', sa.JSON(), nullable=True),
        sa.Column('created_at', sa.DateTime(), nullable=False),
        sa.Column('updated_at', sa.DateTime(), nullable=False),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('benchmark', 'date', name='uq_benchmark_date'),
        schema='public'
    )
    
    # Create indexes for benchmarks table
    op.create_index(op.f('ix_public_benchmarks_benchmark'), 'benchmarks', ['benchmark'], unique=False, schema='public')
    op.create_index(op.f('ix_public_benchmarks_date'), 'benchmarks', ['date'], unique=False, schema='public')


def downgrade() -> None:
    # Drop benchmarks table
    op.drop_table('benchmarks', schema='public')

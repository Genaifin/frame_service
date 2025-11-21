"""Add fund_investors mapping table

Revision ID: 6bcb71ea4cfa
Revises: 9f402e8f0c1e
Create Date: 2025-10-06 20:41:56.869271

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '6bcb71ea4cfa'
down_revision: Union[str, None] = '9f402e8f0c1e'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Create fund_investors mapping table
    op.create_table('fund_investors',
        sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
        sa.Column('fund_id', sa.Integer(), nullable=False),
        sa.Column('investor_id', sa.Integer(), nullable=False),
        sa.Column('investment_amount', sa.Numeric(precision=15, scale=2), nullable=True),
        sa.Column('investment_date', sa.Date(), nullable=True),
        sa.Column('investment_type', sa.String(length=50), nullable=True),
        sa.Column('units_held', sa.Numeric(precision=15, scale=6), nullable=True),
        sa.Column('unit_price', sa.Numeric(precision=10, scale=4), nullable=True),
        sa.Column('is_active', sa.Boolean(), nullable=False),
        sa.Column('notes', sa.Text(), nullable=True),
        sa.Column('fund_investor_metadata', sa.JSON(), nullable=True),
        sa.Column('created_at', sa.DateTime(), nullable=False),
        sa.Column('updated_at', sa.DateTime(), nullable=False),
        sa.ForeignKeyConstraint(['fund_id'], ['funds.id'], ondelete='CASCADE'),
        sa.ForeignKeyConstraint(['investor_id'], ['investors.id'], ondelete='CASCADE'),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('fund_id', 'investor_id', name='unique_fund_investor'),
        schema='public'
    )
    
    # Create indexes
    op.create_index(op.f('ix_public_fund_investors_fund_id'), 'fund_investors', ['fund_id'], unique=False, schema='public')
    op.create_index(op.f('ix_public_fund_investors_investor_id'), 'fund_investors', ['investor_id'], unique=False, schema='public')
    op.create_index(op.f('ix_public_fund_investors_is_active'), 'fund_investors', ['is_active'], unique=False, schema='public')


def downgrade() -> None:
    # Drop indexes first
    op.drop_index(op.f('ix_public_fund_investors_is_active'), table_name='fund_investors', schema='public')
    op.drop_index(op.f('ix_public_fund_investors_investor_id'), table_name='fund_investors', schema='public')
    op.drop_index(op.f('ix_public_fund_investors_fund_id'), table_name='fund_investors', schema='public')
    
    # Drop table
    op.drop_table('fund_investors', schema='public')

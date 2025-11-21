"""add_portfolio_dividend_columns

Revision ID: 309b718beec2
Revises: 62dd538bfae8
Create Date: 2025-09-17 17:02:34.489113

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '309b718beec2'
down_revision: Union[str, None] = 'e34c1d90804b'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Add extra_data column to portfolio_valuation table
    op.add_column('portfolio_valuation',
                  sa.Column('extra_data', sa.Text(), nullable=True),
                  schema='nexbridge')
    
    # Add end_book_mv column to portfolio_valuation table
    op.add_column('portfolio_valuation',
                  sa.Column('end_book_mv', sa.Numeric(precision=18, scale=2), nullable=True),
                  schema='nexbridge')
    
    # Add extra_data column to dividend table  
    op.add_column('dividend',
                  sa.Column('extra_data', sa.Text(), nullable=True),
                  schema='nexbridge')


def downgrade() -> None:
    # Remove extra_data column from dividend table
    op.drop_column('dividend', 'extra_data', schema='nexbridge')
    
    # Remove end_book_mv column from portfolio_valuation table
    op.drop_column('portfolio_valuation', 'end_book_mv', schema='nexbridge')
    
    # Remove extra_data column from portfolio_valuation table
    op.drop_column('portfolio_valuation', 'extra_data', schema='nexbridge')

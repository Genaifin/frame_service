"""add_extra_data_column_to_trial_balance

Revision ID: 62dd538bfae8
Revises: 005_nav_trial
Create Date: 2025-09-11 13:29:56.906739

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '62dd538bfae8'
down_revision: Union[str, None] = '309b718beec2'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Add extra_data column to trial_balance table
    op.add_column('trial_balance', 
                  sa.Column('extra_data', sa.Text(), nullable=True),
                  schema='nexbridge')


def downgrade() -> None:
    # Remove extra_data column from trial_balance table
    op.drop_column('trial_balance', 'extra_data', schema='nexbridge')

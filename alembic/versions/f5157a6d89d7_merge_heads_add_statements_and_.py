"""merge heads add_statements_and_validation_ratio_tables

Revision ID: f5157a6d89d7
Revises: 1ad0d4c4e4c6, c13c0c759c9c
Create Date: 2025-10-29 17:57:49.607478

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'f5157a6d89d7'
down_revision: Union[str, None] = ('1ad0d4c4e4c6', 'c13c0c759c9c')
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass

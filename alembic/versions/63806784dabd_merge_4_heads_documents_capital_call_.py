"""Merge 4 heads: documents, capital_call, distribution, calendar

Revision ID: 63806784dabd
Revises: 38832ba5acac, 3f3b2add3bb, d11d6897ab9a, e5f6g7h8i9j0
Create Date: 2025-11-13 23:30:31.422789

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '63806784dabd'
down_revision: Union[str, None] = ('38832ba5acac', '3f3b2add3bb', 'd11d6897ab9a', 'e5f6g7h8i9j0')
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass


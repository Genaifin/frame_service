"""empty message

Revision ID: a08e3a78e92f
Revises: a2d38996d493, d4e5f6g7h8i9
Create Date: 2025-09-30 13:03:21.681463

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'a08e3a78e92f'
down_revision: Union[str, None] = ('a2d38996d493', 'd4e5f6g7h8i9')
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass

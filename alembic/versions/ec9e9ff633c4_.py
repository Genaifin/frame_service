"""empty message

Revision ID: ec9e9ff633c4
Revises: a08e3a78e92f, f123g456h789
Create Date: 2025-09-30 13:06:04.044444

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'ec9e9ff633c4'
down_revision: Union[str, None] = ('a08e3a78e92f', 'f123g456h789')
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass

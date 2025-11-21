"""Make documents and created_by optional in calendar table

Revision ID: e5f6g7h8i9j0
Revises: d4e5f6g7h8i9
Create Date: 2025-11-06 10:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'e5f6g7h8i9j0'
down_revision: Union[str, None] = 'd4e5f6g7h8i9'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Make documents and created_by nullable in calendars table
    op.alter_column('calendars', 'documents',
               existing_type=sa.JSON(),
               nullable=True,
               schema='public')
    
    op.alter_column('calendars', 'created_by',
               existing_type=sa.String(length=255),
               nullable=True,
               schema='public')


def downgrade() -> None:
    # Revert documents and created_by to non-nullable
    # Note: This may fail if there are NULL values in the database
    op.alter_column('calendars', 'created_by',
               existing_type=sa.String(length=255),
               nullable=False,
               schema='public')
    
    op.alter_column('calendars', 'documents',
               existing_type=sa.JSON(),
               nullable=False,
               schema='public')


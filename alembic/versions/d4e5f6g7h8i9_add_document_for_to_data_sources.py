"""Add document_for JSON field to data_sources table

Revision ID: d4e5f6g7h8i9
Revises: c3d4e5f6g7h8
Create Date: 2025-01-27 13:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'd4e5f6g7h8i9'
down_revision: Union[str, None] = 'c3d4e5f6g7h8'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Add document_for JSON column to data_sources table
    op.add_column('data_sources', 
        sa.Column('document_for', sa.JSON(), nullable=True), 
        schema='public'
    )


def downgrade() -> None:
    # Remove document_for column from data_sources table
    op.drop_column('data_sources', 'document_for', schema='public')

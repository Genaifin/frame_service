"""add_job_title_to_users

Revision ID: 397a724c4bcb
Revises: aedf529d3408
Create Date: 2025-10-24 18:18:23.713991

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '397a724c4bcb'
down_revision: Union[str, None] = 'aedf529d3408'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Add job_title column to users table
    op.add_column('users', sa.Column('job_title', sa.String(length=100), nullable=True), schema='public')


def downgrade() -> None:
    # Remove job_title column from users table
    op.drop_column('users', 'job_title', schema='public')

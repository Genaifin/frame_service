"""Remove UserClient table and references

Revision ID: 777483469cff
Revises: 62dd538bfae8
Create Date: 2025-09-18 13:12:02.802535

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '777483469cff'
down_revision: Union[str, None] = '62dd538bfae8'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Drop UserClient table if it exists
    op.drop_table('user_client', schema='public')


def downgrade() -> None:
    # Recreate UserClient table (if needed for rollback)
    op.create_table('user_client',
        sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
        sa.Column('user_id', sa.Integer(), nullable=False),
        sa.Column('client_id', sa.Integer(), nullable=False),
        sa.Column('is_active', sa.Boolean(), nullable=True),
        sa.Column('created_at', sa.DateTime(), nullable=True),
        sa.Column('updated_at', sa.DateTime(), nullable=True),
        sa.ForeignKeyConstraint(['client_id'], ['public.clients.id'], ondelete='CASCADE'),
        sa.ForeignKeyConstraint(['user_id'], ['public.users.id'], ondelete='CASCADE'),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('user_id', 'client_id', name='uq_user_client'),
        schema='public'
    )

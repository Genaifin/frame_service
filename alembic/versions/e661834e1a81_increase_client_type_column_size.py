"""increase_client_type_column_size

Revision ID: e661834e1a81
Revises: f1g2h3i4j5k6
Create Date: 2025-11-19 11:54:06.861325

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'e661834e1a81'
down_revision: Union[str, None] = 'f1g2h3i4j5k6'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Alter the type column in clients table from VARCHAR(50) to VARCHAR(200)
    op.alter_column('clients', 'type',
                    existing_type=sa.String(50),
                    type_=sa.String(200),
                    existing_nullable=True,
                    schema='public')


def downgrade() -> None:
    # Revert the type column back to VARCHAR(50)
    op.alter_column('clients', 'type',
                    existing_type=sa.String(200),
                    type_=sa.String(50),
                    existing_nullable=True,
                    schema='public')

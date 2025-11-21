"""add_status_to_investors

Revision ID: de9cdd326eae
Revises: 6c3d74ab41bb
Create Date: 2025-10-15 16:48:32.143309

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'de9cdd326eae'
down_revision: Union[str, None] = '6c3d74ab41bb'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Add sector column to investors table (JSON array of sectors)
    op.add_column('investors',
        sa.Column('sector', sa.Text(), nullable=True),
        schema='public'
    )
    
    # Add status column to investors table
    op.add_column('investors',
        sa.Column('status', sa.String(50), nullable=True),
        schema='public'
    )
    
    # Set default value for existing records
    op.execute("UPDATE public.investors SET status = 'invested' WHERE status IS NULL")


def downgrade() -> None:
    # Remove status and sector columns
    op.drop_column('investors', 'status', schema='public')
    op.drop_column('investors', 'sector', schema='public')

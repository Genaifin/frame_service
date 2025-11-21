"""add_isactive_to_data_model_master

Revision ID: 0add6b4c0970
Revises: da8abfe94d4
Create Date: 2025-11-04 14:20:48.659306

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '0add6b4c0970'
down_revision: Union[str, None] = 'da8abfe94d4'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Check if column already exists
    from sqlalchemy import inspect
    bind = op.get_bind()
    inspector = inspect(bind)
    columns = [col['name'] for col in inspector.get_columns('tbl_data_model_master', schema='validus')]
    
    if 'isactive' not in columns:
        # Add isactive column with Boolean type and default value
        op.add_column(
            'tbl_data_model_master',
            sa.Column('isactive', sa.Boolean(), server_default=sa.text('TRUE'), nullable=False),
            schema='validus'
        )


def downgrade() -> None:
    # Remove isactive column
    op.drop_column('tbl_data_model_master', 'isactive', schema='validus')

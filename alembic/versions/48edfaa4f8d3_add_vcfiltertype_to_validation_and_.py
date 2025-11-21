"""add vcfiltertype to validation and ratio details

Revision ID: 48edfaa4f8d3
Revises: 41041cfb988f
Create Date: 2025-10-28 23:45:49.934084

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '48edfaa4f8d3'
down_revision: Union[str, None] = '41041cfb988f'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Check if columns already exist before adding them
    from sqlalchemy import inspect
    bind = op.get_bind()
    inspector = inspect(bind)
    
    # Helper function to check if a column exists
    def column_exists(table_name: str, column_name: str, schema: str = 'validus') -> bool:
        try:
            columns = inspector.get_columns(table_name, schema=schema)
            return any(col['name'] == column_name for col in columns)
        except Exception:
            return False
    
    # Add vcfiltertype column to tbl_validation_details if it doesn't exist
    if not column_exists('tbl_validation_details', 'vcfiltertype'):
        op.add_column(
            'tbl_validation_details',
            sa.Column('vcfiltertype', sa.String(1), nullable=True),
            schema='validus'
        )
    
    # Add vcfiltertype column to tbl_ratio_details if it doesn't exist
    if not column_exists('tbl_ratio_details', 'vcfiltertype'):
        op.add_column(
            'tbl_ratio_details',
            sa.Column('vcfiltertype', sa.String(1), nullable=True),
            schema='validus'
        )


def downgrade() -> None:
    # Drop vcfiltertype column from tbl_ratio_details
    op.drop_column('tbl_ratio_details', 'vcfiltertype', schema='validus')
    
    # Drop vcfiltertype column from tbl_validation_details
    op.drop_column('tbl_validation_details', 'vcfiltertype', schema='validus')

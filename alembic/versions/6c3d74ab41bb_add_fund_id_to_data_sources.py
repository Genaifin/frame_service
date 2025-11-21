"""add_fund_id_to_data_sources

Revision ID: 6c3d74ab41bb
Revises: 1c74e3a5bfd0
Create Date: 2025-10-15 12:00:26.445908

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '6c3d74ab41bb'
down_revision: Union[str, None] = '1c74e3a5bfd0'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Add fund_id column to data_sources table
    op.add_column('data_sources', 
        sa.Column('fund_id', sa.Integer(), nullable=True),
        schema='public'
    )
    
    # Add foreign key constraint
    op.create_foreign_key(
        'fk_data_sources_fund_id',
        'data_sources', 'funds',
        ['fund_id'], ['id'],
        source_schema='public',
        referent_schema='public',
        ondelete='CASCADE'
    )
    
    # Create index on fund_id for better query performance
    op.create_index('ix_data_sources_fund_id', 'data_sources', ['fund_id'], schema='public')
    
    # Note: After migration, you may need to update existing records to set fund_id
    # Example: UPDATE public.data_sources SET fund_id = 1 WHERE fund_id IS NULL;
    # Then alter column to NOT NULL if needed


def downgrade() -> None:
    # Remove index
    op.drop_index('ix_data_sources_fund_id', table_name='data_sources', schema='public')
    
    # Remove foreign key constraint
    op.drop_constraint('fk_data_sources_fund_id', 'data_sources', schema='public', type_='foreignkey')
    
    # Remove fund_id column
    op.drop_column('data_sources', 'fund_id', schema='public')

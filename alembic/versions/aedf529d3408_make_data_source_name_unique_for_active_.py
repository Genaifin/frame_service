"""make_data_source_name_unique_for_active_only

Revision ID: aedf529d3408
Revises: de9cdd326eae
Create Date: 2025-10-15 17:50:11.288492

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'aedf529d3408'
down_revision: Union[str, None] = 'de9cdd326eae'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Drop the existing unique index on name
    op.drop_index('ix_public_data_sources_name', table_name='data_sources', schema='public')
    
    # Create a new non-unique index on name (for lookups)
    op.create_index('ix_public_data_sources_name', 'data_sources', ['name'], unique=False, schema='public')
    
    # Create a partial unique index on (fund_id, name) that only applies to active sources
    # This allows: same name for different funds, and reusing names after soft delete
    op.execute("""
        CREATE UNIQUE INDEX ix_public_data_sources_fund_name_active 
        ON public.data_sources (fund_id, name) 
        WHERE is_active = true
    """)


def downgrade() -> None:
    # Drop the partial unique index
    op.drop_index('ix_public_data_sources_fund_name_active', table_name='data_sources', schema='public')
    
    # Drop the non-unique index
    op.drop_index('ix_public_data_sources_name', table_name='data_sources', schema='public')
    
    # Recreate the original unique index on name
    op.create_index('ix_public_data_sources_name', 'data_sources', ['name'], unique=True, schema='public')

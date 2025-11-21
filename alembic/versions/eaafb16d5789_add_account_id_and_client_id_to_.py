"""add_account_id_and_client_id_to_documents

Revision ID: eaafb16d5789
Revises: e661834e1a81
Create Date: 2025-11-20 18:55:19.853863

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'eaafb16d5789'
down_revision: Union[str, None] = 'e661834e1a81'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Add account_id column to documents table
    op.add_column(
        'documents',
        sa.Column('account_id', sa.Integer(), nullable=True),
        schema='public'
    )
    
    # Add client_id column to documents table
    op.add_column(
        'documents',
        sa.Column('client_id', sa.Integer(), nullable=True),
        schema='public'
    )
    
    # Create foreign key constraint for client_id
    op.create_foreign_key(
        'fk_documents_client_id',
        'documents', 'clients',
        ['client_id'], ['id'],
        ondelete='SET NULL',
        source_schema='public',
        referent_schema='public'
    )
    
    # Create indexes for the new columns (following the pattern of fund_id)
    op.create_index(
        'ix_documents_account_id',
        'documents',
        ['account_id'],
        unique=False,
        schema='public'
    )
    op.create_index(
        'ix_documents_client_id',
        'documents',
        ['client_id'],
        unique=False,
        schema='public'
    )


def downgrade() -> None:
    # Drop indexes first
    op.drop_index('ix_documents_client_id', table_name='documents', schema='public')
    op.drop_index('ix_documents_account_id', table_name='documents', schema='public')
    
    # Drop foreign key constraint
    op.drop_constraint('fk_documents_client_id', 'documents', type_='foreignkey', schema='public')
    
    # Drop columns
    op.drop_column('documents', 'client_id', schema='public')
    op.drop_column('documents', 'account_id', schema='public')

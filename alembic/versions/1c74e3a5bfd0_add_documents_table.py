"""add_documents_table

Revision ID: 1c74e3a5bfd0
Revises: c6a09e1609ac
Create Date: 2025-10-10 14:55:03.996435

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision: str = '1c74e3a5bfd0'
down_revision: Union[str, None] = 'c6a09e1609ac'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Create documents table
    op.create_table('documents',
    sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
    sa.Column('name', sa.String(length=255), nullable=False),
    sa.Column('type', sa.String(length=50), nullable=True),
    sa.Column('path', sa.String(length=500), nullable=False),
    sa.Column('size', sa.Integer(), nullable=True),
    sa.Column('status', sa.String(length=50), nullable=False),
    sa.Column('fund_id', sa.Integer(), nullable=True),
    sa.Column('upload_date', sa.DateTime(), nullable=False),
    sa.Column('replay', sa.Boolean(), nullable=False, server_default='false'),
    sa.Column('created_by', sa.String(length=100), nullable=True),
    sa.Column('metadata', sa.JSON(), nullable=True),
    sa.Column('is_active', sa.Boolean(), nullable=True),
    sa.Column('created_at', sa.DateTime(), nullable=False),
    sa.Column('updated_at', sa.DateTime(), nullable=False),
    sa.CheckConstraint("status IN ('pending', 'processing', 'completed', 'failed', 'validated', 'rejected')", name='chk_document_status'),
    sa.ForeignKeyConstraint(['fund_id'], ['public.funds.id'], ondelete='SET NULL'),
    sa.PrimaryKeyConstraint('id'),
    schema='public'
    )
    
    # Create indexes
    op.create_index(op.f('ix_public_documents_document_type'), 'documents', ['type'], unique=False, schema='public')
    op.create_index(op.f('ix_public_documents_name'), 'documents', ['name'], unique=False, schema='public')
    op.create_index(op.f('ix_public_documents_fund_id'), 'documents', ['fund_id'], unique=False, schema='public')
    op.create_index(op.f('ix_public_documents_is_active'), 'documents', ['is_active'], unique=False, schema='public')
    op.create_index(op.f('ix_public_documents_status'), 'documents', ['status'], unique=False, schema='public')


def downgrade() -> None:
    # Drop indexes
    op.drop_index(op.f('ix_public_documents_status'), table_name='documents', schema='public')
    op.drop_index(op.f('ix_public_documents_is_active'), table_name='documents', schema='public')
    op.drop_index(op.f('ix_public_documents_fund_id'), table_name='documents', schema='public')
    op.drop_index(op.f('ix_public_documents_name'), table_name='documents', schema='public')
    op.drop_index(op.f('ix_public_documents_document_type'), table_name='documents', schema='public')
    
    # Drop table
    op.drop_table('documents', schema='public')

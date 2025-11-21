"""consolidated_documents_table_updates

Revision ID: 38832ba5acac
Revises: 0add6b4c0970
Create Date: 2025-11-06 10:08:53.000000

This is a consolidated migration that combines the following migrations:
- 20251105163806_add_file_id_to_documents (replaced)
- 841fc4524f4d_update_file_id_to_required_with_default (replaced)
- 20251105170000_rename_file_id_to_doc_id (replaced)
- 605a31275cbb_merge_heads (replaced)

This migration should be used in place of the above migrations.
"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql
import uuid

# revision identifiers, used by Alembic.
revision = '38832ba5acac'
down_revision = '0add6b4c0970'  # This should be the revision before the first migration we're consolidating
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Add doc_id column with UUID type, not nullable, with default
    op.add_column(
        'documents',
        sa.Column(
            'doc_id',  # Using final column name directly
            postgresql.UUID(as_uuid=True),
            nullable=False,
            server_default=sa.text('gen_random_uuid()'),
            comment='Unique identifier for the document'
        ),
        schema='public'
    )
    
    # Create index on doc_id
    op.create_index(
        op.f('ix_public_documents_doc_id'),
        'documents',
        ['doc_id'],
        unique=True,
        schema='public'
    )


def downgrade() -> None:
    # Drop the index
    conn = op.get_bind()
    inspector = sa.inspect(conn)
    indexes = inspector.get_indexes('documents', schema='public')
    
    # Check if the index exists before trying to drop it
    if any(idx['name'] == 'ix_public_documents_doc_id' for idx in indexes):
        op.drop_index(
            'ix_public_documents_doc_id',
            table_name='documents',
            schema='public'
        )
    
    # Drop the column
    columns = [col['name'] for col in inspector.get_columns('documents', schema='public')]
    if 'doc_id' in columns:
        op.drop_column('documents', 'doc_id', schema='public')

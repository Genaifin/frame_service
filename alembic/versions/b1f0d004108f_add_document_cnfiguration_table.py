"""add_document_cnfiguration_table

Revision ID: b1f0d004108f
Revises: 565e6d536ea1
Create Date: 2025-01-27 00:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'b1f0d004108f'
down_revision: Union[str, None] = '565e6d536ea1'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Create document_cnfiguration table
    op.create_table('document_configuration',
    sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
    sa.Column('name', sa.String(length=255), nullable=False),
    sa.Column('description', sa.Text(), nullable=True),
    sa.Column('sla', sa.Integer(), nullable=True),
    sa.Column('fields', sa.JSON(), nullable=True),
    sa.PrimaryKeyConstraint('id'),
    schema='public'
    )
    
    # Create index on name
    op.create_index(op.f('ix_public_document_configuration_name'), 'document_configuration', ['name'], unique=False, schema='public')


def downgrade() -> None:
    # Drop index
    op.drop_index(op.f('ix_public_document_configuration_name'), table_name='document_configuration', schema='public')
    
    # Drop table
    op.drop_table('document_configuration', schema='public')


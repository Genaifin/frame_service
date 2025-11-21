"""Add data sources table to public schema

Revision ID: b2c3d4e5f6g7
Revises: a1b2c3d4e5f6
Create Date: 2025-01-27 11:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'b2c3d4e5f6g7'
down_revision: Union[str, None] = 'a1b2c3d4e5f6'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Create data_sources table in public schema
    op.create_table('data_sources',
        sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
        sa.Column('name', sa.String(length=255), nullable=False),
        sa.Column('source', sa.String(length=50), nullable=False),
        sa.Column('holiday_calendar', sa.String(length=20), nullable=False),
        sa.Column('source_details', sa.JSON(), nullable=True),
        sa.Column('additional_details', sa.Text(), nullable=True),
        sa.Column('is_active', sa.Boolean(), nullable=True),
        sa.Column('created_at', sa.DateTime(), nullable=True),
        sa.Column('updated_at', sa.DateTime(), nullable=True),
        sa.Column('created_by', sa.String(length=255), nullable=False),
        sa.Column('updated_by', sa.String(length=255), nullable=True),
        sa.PrimaryKeyConstraint('id'),
        sa.CheckConstraint("source IN ('Email', 'S3 Bucket', 'Portal', 'API', 'SFTP')", name='chk_source_type'),
        sa.CheckConstraint("holiday_calendar IN ('US', 'Europe')", name='chk_holiday_calendar'),
        schema='public'
    )
    
    # Create indexes for better query performance
    op.create_index(op.f('ix_public_data_sources_name'), 'data_sources', ['name'], unique=True, schema='public')
    op.create_index(op.f('ix_public_data_sources_source'), 'data_sources', ['source'], unique=False, schema='public')
    op.create_index(op.f('ix_public_data_sources_holiday_calendar'), 'data_sources', ['holiday_calendar'], unique=False, schema='public')
    op.create_index(op.f('ix_public_data_sources_is_active'), 'data_sources', ['is_active'], unique=False, schema='public')


def downgrade() -> None:
    # Drop the data_sources table and its indexes
    op.drop_index(op.f('ix_public_data_sources_is_active'), table_name='data_sources', schema='public')
    op.drop_index(op.f('ix_public_data_sources_holiday_calendar'), table_name='data_sources', schema='public')
    op.drop_index(op.f('ix_public_data_sources_source'), table_name='data_sources', schema='public')
    op.drop_index(op.f('ix_public_data_sources_name'), table_name='data_sources', schema='public')
    op.drop_table('data_sources', schema='public')

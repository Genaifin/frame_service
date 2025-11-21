"""add validation and ratio configuration tables

Revision ID: c13c0c759c9c
Revises: 48edfaa4f8d3
Create Date: 2025-01 comprehensive configuration tables for validations and ratios

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy import text


# revision identifiers, used by Alembic.
revision: str = 'c13c0c759c9c'
down_revision: Union[str, None] = '48edfaa4f8d3'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Check if tables already exist
    from sqlalchemy import inspect
    bind = op.get_bind()
    inspector = inspect(bind)
    existing_tables = inspector.get_table_names(schema='validus')
    
    # Create tbl_validation_configuration
    if 'tbl_validation_configuration' not in existing_tables:
        op.create_table(
            'tbl_validation_configuration',
            sa.Column('intvalidationconfigurationid', sa.Integer(), sa.Identity(start=1, increment=1), primary_key=True, nullable=False),
            sa.Column('intclientid', sa.Integer(), nullable=True),
            sa.Column('intfundid', sa.Integer(), nullable=True),
            sa.Column('intvalidationmasterid', sa.Integer(), nullable=True),
            sa.Column('isactive', sa.Boolean(), server_default=sa.text('FALSE'), nullable=False),
            sa.Column('vccondition', sa.String(100), nullable=True),
            sa.Column('intthreshold', sa.Numeric(12, 4), nullable=True),
            sa.Column('vcthresholdtype', sa.String(100), nullable=True),
            sa.Column('intprecision', sa.Numeric(12, 10), nullable=True),
            sa.Column('intcreatedby', sa.Integer(), nullable=True),
            sa.Column('dtcreatedat', sa.DateTime(timezone=False), server_default=sa.text('CURRENT_TIMESTAMP'), nullable=False),
            sa.Column('intupdatedby', sa.Integer(), nullable=True),
            sa.Column('dtupdatedat', sa.DateTime(timezone=False), nullable=True),
            schema='validus'
        )
        op.create_foreign_key(
            'tbl_validation_configuration_intvalidationmasterid_fkey',
            'tbl_validation_configuration', 'tbl_validation_master',
            ['intvalidationmasterid'], ['intvalidationmasterid'],
            source_schema='validus', referent_schema='validus'
        )
        op.create_index('ix_tbl_validation_configuration_intvalidationmasterid', 'tbl_validation_configuration', ['intvalidationmasterid'], schema='validus')
        op.create_index('ix_tbl_validation_configuration_intclientid', 'tbl_validation_configuration', ['intclientid'], schema='validus')
        op.create_index('ix_tbl_validation_configuration_intfundid', 'tbl_validation_configuration', ['intfundid'], schema='validus')
    
    # Create tbl_ratio_configuration
    if 'tbl_ratio_configuration' not in existing_tables:
        op.create_table(
            'tbl_ratio_configuration',
            sa.Column('intratioconfigurationid', sa.Integer(), sa.Identity(start=1, increment=1), primary_key=True, nullable=False),
            sa.Column('intclientid', sa.Integer(), nullable=True),
            sa.Column('intfundid', sa.Integer(), nullable=True),
            sa.Column('intratiomasterid', sa.Integer(), nullable=True),
            sa.Column('isactive', sa.Boolean(), server_default=sa.text('FALSE'), nullable=False),
            sa.Column('vccondition', sa.String(100), nullable=True),
            sa.Column('intthreshold', sa.Numeric(12, 4), nullable=True),
            sa.Column('vcthresholdtype', sa.String(100), nullable=True),
            sa.Column('intprecision', sa.Numeric(12, 10), nullable=True),
            sa.Column('intcreatedby', sa.Integer(), nullable=True),
            sa.Column('dtcreatedat', sa.DateTime(timezone=False), server_default=sa.text('CURRENT_TIMESTAMP'), nullable=False),
            sa.Column('intupdatedby', sa.Integer(), nullable=True),
            sa.Column('dtupdatedat', sa.DateTime(timezone=False), nullable=True),
            schema='validus'
        )
        op.create_foreign_key(
            'tbl_ratio_configuration_intratiomasterid_fkey',
            'tbl_ratio_configuration', 'tbl_ratio_master',
            ['intratiomasterid'], ['intratiomasterid'],
            source_schema='validus', referent_schema='validus'
        )
        op.create_index('ix_tbl_ratio_configuration_intratiomasterid', 'tbl_ratio_configuration', ['intratiomasterid'], schema='validus')
        op.create_index('ix_tbl_ratio_configuration_intclientid', 'tbl_ratio_configuration', ['intclientid'], schema='validus')
        op.create_index('ix_tbl_ratio_configuration_intfundid', 'tbl_ratio_configuration', ['intfundid'], schema='validus')


def downgrade() -> None:
    # Drop tables in reverse order
    op.drop_table('tbl_ratio_configuration', schema='validus')
    op.drop_table('tbl_validation_configuration', schema='validus')


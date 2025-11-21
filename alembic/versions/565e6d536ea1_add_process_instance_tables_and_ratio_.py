"""add_process_instance_tables_and_ratio_numerator_denominator

Revision ID: 565e6d536ea1
Revises: 63806784dabd
Create Date: 2025-11-13 23:36:45.572695

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy import text


# revision identifiers, used by Alembic.
revision: str = '565e6d536ea1'
down_revision: Union[str, None] = '63806784dabd'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Check if tables already exist
    from sqlalchemy import inspect
    bind = op.get_bind()
    inspector = inspect(bind)
    existing_tables = inspector.get_table_names(schema='validus')
    
    # Create tbl_process_instance
    if 'tbl_process_instance' not in existing_tables:
        op.create_table(
            'tbl_process_instance',
            sa.Column('intprocessinstanceid', sa.BigInteger(), sa.Identity(start=1, increment=1), primary_key=True, nullable=False),
            sa.Column('intclientid', sa.Integer(), nullable=True),
            sa.Column('intfundid', sa.Integer(), nullable=True),
            sa.Column('vccurrency', sa.String(10), nullable=True),
            sa.Column('vcvalidustype', sa.String(100), nullable=True),
            sa.Column('vcsourcetype', sa.String(50), nullable=True),
            sa.Column('vcsource_a', sa.String(250), nullable=True),
            sa.Column('vcsource_b', sa.String(250), nullable=True),
            sa.Column('dtdate_a', sa.Date(), nullable=True),
            sa.Column('dtdate_b', sa.Date(), nullable=True),
            sa.Column('dtprocesstime_start', sa.DateTime(timezone=False), nullable=True),
            sa.Column('dtprocesstime_end', sa.DateTime(timezone=False), nullable=True),
            sa.Column('vcprocessstats', sa.String(50), nullable=True),
            sa.Column('vcstatusdescription', sa.String(250), nullable=True),
            sa.Column('intuserid', sa.Integer(), nullable=True),
            schema='validus'
        )
        op.create_index('ix_tbl_process_instance_intclientid', 'tbl_process_instance', ['intclientid'], schema='validus')
        op.create_index('ix_tbl_process_instance_intfundid', 'tbl_process_instance', ['intfundid'], schema='validus')
    
    # Create tbl_data_load_instance
    if 'tbl_data_load_instance' not in existing_tables:
        op.create_table(
            'tbl_data_load_instance',
            sa.Column('intdataloadinstanceid', sa.Integer(), sa.Identity(start=1, increment=1), primary_key=True, nullable=False),
            sa.Column('intclientid', sa.Integer(), nullable=True),
            sa.Column('intfundid', sa.Integer(), nullable=True),
            sa.Column('vccurrency', sa.String(10), nullable=True),
            sa.Column('intdatamodelid', sa.Integer(), nullable=True),
            sa.Column('dtdataasof', sa.Date(), nullable=True),
            sa.Column('vcdatadate', sa.String(250), nullable=True),
            sa.Column('vcdatasourcetype', sa.String(100), nullable=True),
            sa.Column('vcdatasourcename', sa.String(100), nullable=True),
            sa.Column('vcloadtype', sa.String(100), server_default=sa.text("'Manual'"), nullable=True),
            sa.Column('vcloadstatus', sa.String(100), nullable=True),
            sa.Column('vcdataloaddescription', sa.String(500), nullable=True),
            sa.Column('intloadedby', sa.Integer(), nullable=True),
            sa.Column('dtloadedat', sa.DateTime(timezone=False), server_default=sa.text('CURRENT_TIMESTAMP'), nullable=True),
            schema='validus'
        )
        op.create_foreign_key(
            'fk_tbl_data_load_instance_intdatamodelid',
            'tbl_data_load_instance', 'tbl_data_model_master',
            ['intdatamodelid'], ['intdatamodelid'],
            source_schema='validus', referent_schema='validus'
        )
        op.create_index('ix_tbl_data_load_instance_intclientid', 'tbl_data_load_instance', ['intclientid'], schema='validus')
        op.create_index('ix_tbl_data_load_instance_intfundid', 'tbl_data_load_instance', ['intfundid'], schema='validus')
        op.create_index('ix_tbl_data_load_instance_intdatamodelid', 'tbl_data_load_instance', ['intdatamodelid'], schema='validus')
    
    # Create tbl_process_instance_details
    if 'tbl_process_instance_details' not in existing_tables:
        op.create_table(
            'tbl_process_instance_details',
            sa.Column('intprocessinstancedetailid', sa.BigInteger(), sa.Identity(start=1, increment=1), primary_key=True, nullable=False),
            sa.Column('intprocessinstanceid', sa.BigInteger(), nullable=True),
            sa.Column('intdataloadinstanceid', sa.BigInteger(), nullable=True),
            sa.Column('dtprocesstime', sa.DateTime(timezone=False), nullable=True),
            schema='validus'
        )
        op.create_foreign_key(
            'fk_tbl_process_instance_details_intprocessinstanceid',
            'tbl_process_instance_details', 'tbl_process_instance',
            ['intprocessinstanceid'], ['intprocessinstanceid'],
            source_schema='validus', referent_schema='validus'
        )
        op.create_index('ix_tbl_process_instance_details_intprocessinstanceid', 'tbl_process_instance_details', ['intprocessinstanceid'], schema='validus')
        op.create_index('ix_tbl_process_instance_details_intdataloadinstanceid', 'tbl_process_instance_details', ['intdataloadinstanceid'], schema='validus')
    
    # Add vcnumerator and vcdenominator columns to tbl_ratio_details if they don't exist
    columns = [col['name'] for col in inspector.get_columns('tbl_ratio_details', schema='validus')]
    
    if 'vcnumerator' not in columns:
        op.add_column(
            'tbl_ratio_details',
            sa.Column('vcnumerator', sa.Text(), nullable=True),
            schema='validus'
        )
    
    if 'vcdenominator' not in columns:
        op.add_column(
            'tbl_ratio_details',
            sa.Column('vcdenominator', sa.Text(), nullable=True),
            schema='validus'
        )


def downgrade() -> None:
    # Drop columns from tbl_ratio_details
    from sqlalchemy import inspect
    bind = op.get_bind()
    inspector = inspect(bind)
    columns = [col['name'] for col in inspector.get_columns('tbl_ratio_details', schema='validus')]
    
    if 'vcdenominator' in columns:
        op.drop_column('tbl_ratio_details', 'vcdenominator', schema='validus')
    
    if 'vcnumerator' in columns:
        op.drop_column('tbl_ratio_details', 'vcnumerator', schema='validus')
    
    # Drop tables in reverse order
    op.drop_table('tbl_process_instance_details', schema='validus')
    op.drop_table('tbl_process_instance', schema='validus')
    op.drop_table('tbl_data_load_instance', schema='validus')

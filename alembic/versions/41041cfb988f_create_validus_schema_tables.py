"""create_validus_schema_tables

Revision ID: 41041cfb988f
Revises: 397a724c4bcb
Create Date: 2025-10-28 13:11:04.800254

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy import text


# revision identifiers, used by Alembic.
revision: str = '41041cfb988f'
down_revision: Union[str, None] = '397a724c4bcb'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Create validus schema if it doesn't exist
    op.execute("CREATE SCHEMA IF NOT EXISTS validus")
    
    # Check if tables already exist
    from sqlalchemy import inspect
    bind = op.get_bind()
    inspector = inspect(bind)
    existing_tables = inspector.get_table_names(schema='validus')
    
    # Helper function to check if a constraint exists
    def constraint_exists(table_name: str, constraint_name: str, schema: str = 'validus') -> bool:
        try:
            constraints = inspector.get_foreign_keys(table_name, schema=schema)
            return any(fk['name'] == constraint_name for fk in constraints)
        except Exception:
            return False
    
    # Helper function to check if an index exists
    def index_exists(table_name: str, index_name: str, schema: str = 'validus') -> bool:
        try:
            indexes = inspector.get_indexes(table_name, schema=schema)
            return any(idx['name'] == index_name for idx in indexes)
        except Exception:
            return False
    
    # Create tbl_data_model_master
    if 'tbl_data_model_master' not in existing_tables:
        op.create_table(
        'tbl_data_model_master',
        sa.Column('intdatamodelid', sa.Integer(), sa.Identity(start=1, increment=1), primary_key=True, nullable=False),
        sa.Column('vcmodelname', sa.String(250), nullable=True),
        sa.Column('vcdescription', sa.String(500), nullable=True),
        sa.Column('vcmodelid', sa.String(100), nullable=True),
        sa.Column('vccategory', sa.String(100), nullable=True),
        sa.Column('vcsource', sa.String(100), nullable=True),
        sa.Column('vctablename', sa.String(250), nullable=True),
        sa.Column('intcreatedby', sa.Integer(), nullable=True),
        sa.Column('dtcreatedat', sa.DateTime(timezone=False), server_default=sa.text('CURRENT_TIMESTAMP'), nullable=False),
        sa.Column('intupdatedby', sa.Integer(), nullable=True),
        sa.Column('dtupdatedat', sa.DateTime(timezone=False), nullable=True),
        schema='validus'
    )
    
    # Create tbl_data_model_details
    if 'tbl_data_model_details' not in existing_tables:
        op.create_table(
        'tbl_data_model_details',
        sa.Column('intdatamodeldetailid', sa.Integer(), sa.Identity(start=1, increment=1), primary_key=True, nullable=False),
        sa.Column('intdatamodelid', sa.Integer(), nullable=False),
        sa.Column('vcfieldname', sa.String(250), nullable=True),
        sa.Column('vcfielddescription', sa.String(500), nullable=True),
        sa.Column('vcdatatype', sa.String(100), nullable=True),
        sa.Column('intlength', sa.Integer(), nullable=True),
        sa.Column('intprecision', sa.Integer(), nullable=True),
        sa.Column('intscale', sa.Integer(), nullable=True),
        sa.Column('vcdateformat', sa.String(100), nullable=True),
        sa.Column('vcdmcolumnname', sa.String(250), nullable=True),
        sa.Column('vcdefaultvalue', sa.String(255), nullable=True),
        sa.Column('ismandatory', sa.Boolean(), nullable=True),
        sa.Column('intdisplayorder', sa.Integer(), nullable=True),
        sa.Column('intcreatedby', sa.Integer(), nullable=True),
        sa.Column('dtcreatedat', sa.DateTime(timezone=False), server_default=sa.text('CURRENT_TIMESTAMP'), nullable=False),
        sa.Column('intupdatedby', sa.Integer(), nullable=True),
        sa.Column('dtupdatedat', sa.DateTime(timezone=False), nullable=True),
        schema='validus'
    )
    # Create foreign key constraint if it doesn't exist
    if not constraint_exists('tbl_data_model_details', 'fk_tbl_data_model_details_intdatamodelid'):
        op.create_foreign_key(
            'fk_tbl_data_model_details_intdatamodelid',
            'tbl_data_model_details', 'tbl_data_model_master',
            ['intdatamodelid'], ['intdatamodelid'],
            source_schema='validus', referent_schema='validus',
            ondelete='CASCADE'
        )
    # Create index if it doesn't exist
    if not index_exists('tbl_data_model_details', 'ix_tbl_data_model_details_intdatamodelid'):
        op.create_index('ix_tbl_data_model_details_intdatamodelid', 'tbl_data_model_details', ['intdatamodelid'], schema='validus')
    
    # Create tbl_subproduct_master
    if 'tbl_subproduct_master' not in existing_tables:
        op.create_table(
        'tbl_subproduct_master',
        sa.Column('intsubproductid', sa.Integer(), sa.Identity(start=1, increment=1), primary_key=True, nullable=False),
        sa.Column('vcsubproductname', sa.String(250), nullable=False),
        sa.Column('vcdescription', sa.String(500), nullable=True),
        sa.Column('isactive', sa.Boolean(), server_default=sa.text('TRUE'), nullable=False),
        sa.Column('intcreatedby', sa.Integer(), nullable=True),
        sa.Column('dtcreatedat', sa.DateTime(timezone=False), server_default=sa.text('CURRENT_TIMESTAMP'), nullable=False),
        sa.Column('intupdatedby', sa.Integer(), nullable=True),
        sa.Column('dtupdatedat', sa.DateTime(timezone=False), nullable=True),
        schema='validus'
    )
    
    # Create tbl_subproduct_details
    if 'tbl_subproduct_details' not in existing_tables:
        op.create_table(
        'tbl_subproduct_details',
        sa.Column('intsubproductdetailid', sa.Integer(), sa.Identity(start=1, increment=1), primary_key=True, nullable=False),
        sa.Column('intsubproductid', sa.Integer(), nullable=False),
        sa.Column('vcvalidustype', sa.String(250), nullable=True),
        sa.Column('vctype', sa.String(250), nullable=True),
        sa.Column('vcsubtype', sa.String(250), nullable=True),
        sa.Column('vcdescription', sa.String(500), nullable=True),
        sa.Column('isactive', sa.Boolean(), server_default=sa.text('TRUE'), nullable=False),
        sa.Column('intcreatedby', sa.Integer(), nullable=True),
        sa.Column('dtcreatedat', sa.DateTime(timezone=False), server_default=sa.text('CURRENT_TIMESTAMP'), nullable=False),
        sa.Column('intupdatedby', sa.Integer(), nullable=True),
        sa.Column('dtupdatedat', sa.DateTime(timezone=False), nullable=True),
        schema='validus'
    )
    # Create foreign key constraint if it doesn't exist
    if not constraint_exists('tbl_subproduct_details', 'fk_tbl_subproduct_details_intsubproductid'):
        op.create_foreign_key(
            'fk_tbl_subproduct_details_intsubproductid',
            'tbl_subproduct_details', 'tbl_subproduct_master',
            ['intsubproductid'], ['intsubproductid'],
            source_schema='validus', referent_schema='validus',
            ondelete='CASCADE'
        )
    # Create index if it doesn't exist
    if not index_exists('tbl_subproduct_details', 'ix_tbl_subproduct_details_intsubproductid'):
        op.create_index('ix_tbl_subproduct_details_intsubproductid', 'tbl_subproduct_details', ['intsubproductid'], schema='validus')
    
    # Create tbl_validation_master
    if 'tbl_validation_master' not in existing_tables:
        op.create_table(
        'tbl_validation_master',
        sa.Column('intvalidationmasterid', sa.Integer(), sa.Identity(start=1, increment=1), primary_key=True, nullable=False),
        sa.Column('intsubproductid', sa.Integer(), nullable=False),
        sa.Column('vcsourcetype', sa.String(250), nullable=True),
        sa.Column('vctype', sa.String(250), nullable=True),
        sa.Column('vcsubtype', sa.String(250), nullable=True),
        sa.Column('issubtype_subtotal', sa.Boolean(), nullable=True),
        sa.Column('vcvalidationname', sa.String(250), nullable=True),
        sa.Column('isvalidation_subtotal', sa.Boolean(), nullable=True),
        sa.Column('vcdescription', sa.String(500), nullable=True),
        sa.Column('intthreshold', sa.Numeric(12, 4), nullable=True),
        sa.Column('vcthresholdtype', sa.String(100), nullable=True),
        sa.Column('intprecision', sa.Numeric(12, 10), nullable=True),
        sa.Column('isactive', sa.Boolean(), server_default=sa.text('TRUE'), nullable=False),
        sa.Column('intcreatedby', sa.Integer(), nullable=True),
        sa.Column('dtcreatedat', sa.DateTime(timezone=False), server_default=sa.text('CURRENT_TIMESTAMP'), nullable=False),
        sa.Column('intupdatedby', sa.Integer(), nullable=True),
        sa.Column('dtupdatedat', sa.DateTime(timezone=False), nullable=True),
        schema='validus'
    )
    # Create foreign key constraint if it doesn't exist
    if not constraint_exists('tbl_validation_master', 'fk_tbl_validation_master_intsubproductid'):
        op.create_foreign_key(
            'fk_tbl_validation_master_intsubproductid',
            'tbl_validation_master', 'tbl_subproduct_master',
            ['intsubproductid'], ['intsubproductid'],
            source_schema='validus', referent_schema='validus',
            ondelete='CASCADE'
        )
    # Create index if it doesn't exist
    if not index_exists('tbl_validation_master', 'ix_tbl_validation_master_intsubproductid'):
        op.create_index('ix_tbl_validation_master_intsubproductid', 'tbl_validation_master', ['intsubproductid'], schema='validus')
    
    # Create tbl_validation_details
    if 'tbl_validation_details' not in existing_tables:
        op.create_table(
        'tbl_validation_details',
        sa.Column('intvalidationdetailid', sa.Integer(), sa.Identity(start=1, increment=1), primary_key=True, nullable=False),
        sa.Column('intvalidationmasterid', sa.Integer(), nullable=False),
        sa.Column('intdatamodelid', sa.Integer(), nullable=False),
        sa.Column('intgroup_attributeid', sa.Integer(), nullable=True),
        sa.Column('intassettypeid', sa.Integer(), nullable=True),
        sa.Column('intcalc_attributeid', sa.Integer(), nullable=True),
        sa.Column('vcaggregationtype', sa.String(20), nullable=True),
        sa.Column('vcfilter', sa.Text(), nullable=True),
        sa.Column('vcformula', sa.Text(), nullable=True),
        sa.Column('intcreatedby', sa.Integer(), nullable=True),
        sa.Column('dtcreatedat', sa.DateTime(timezone=False), server_default=sa.text('CURRENT_TIMESTAMP'), nullable=False),
        sa.Column('intupdatedby', sa.Integer(), nullable=True),
        sa.Column('dtupdatedat', sa.DateTime(timezone=False), nullable=True),
        schema='validus'
    )
    # Create foreign key constraints if they don't exist
    if not constraint_exists('tbl_validation_details', 'fk_tbl_validation_details_intvalidationmasterid'):
        op.create_foreign_key(
            'fk_tbl_validation_details_intvalidationmasterid',
            'tbl_validation_details', 'tbl_validation_master',
            ['intvalidationmasterid'], ['intvalidationmasterid'],
            source_schema='validus', referent_schema='validus',
            ondelete='CASCADE'
        )
    if not constraint_exists('tbl_validation_details', 'fk_tbl_validation_details_intdatamodelid'):
        op.create_foreign_key(
            'fk_tbl_validation_details_intdatamodelid',
            'tbl_validation_details', 'tbl_data_model_master',
            ['intdatamodelid'], ['intdatamodelid'],
            source_schema='validus', referent_schema='validus'
        )
    if not constraint_exists('tbl_validation_details', 'fk_tbl_validation_details_intgroup_attributeid'):
        op.create_foreign_key(
            'fk_tbl_validation_details_intgroup_attributeid',
            'tbl_validation_details', 'tbl_data_model_details',
            ['intgroup_attributeid'], ['intdatamodeldetailid'],
            source_schema='validus', referent_schema='validus'
        )
    if not constraint_exists('tbl_validation_details', 'fk_tbl_validation_details_intassettypeid'):
        op.create_foreign_key(
            'fk_tbl_validation_details_intassettypeid',
            'tbl_validation_details', 'tbl_data_model_details',
            ['intassettypeid'], ['intdatamodeldetailid'],
            source_schema='validus', referent_schema='validus'
        )
    if not constraint_exists('tbl_validation_details', 'fk_tbl_validation_details_intcalc_attributeid'):
        op.create_foreign_key(
            'fk_tbl_validation_details_intcalc_attributeid',
            'tbl_validation_details', 'tbl_data_model_details',
            ['intcalc_attributeid'], ['intdatamodeldetailid'],
            source_schema='validus', referent_schema='validus'
        )
    # Create indexes if they don't exist
    if not index_exists('tbl_validation_details', 'ix_tbl_validation_details_intvalidationmasterid'):
        op.create_index('ix_tbl_validation_details_intvalidationmasterid', 'tbl_validation_details', ['intvalidationmasterid'], schema='validus')
    if not index_exists('tbl_validation_details', 'ix_tbl_validation_details_intdatamodelid'):
        op.create_index('ix_tbl_validation_details_intdatamodelid', 'tbl_validation_details', ['intdatamodelid'], schema='validus')
    if not index_exists('tbl_validation_details', 'ix_tbl_validation_details_intgroup_attributeid'):
        op.create_index('ix_tbl_validation_details_intgroup_attributeid', 'tbl_validation_details', ['intgroup_attributeid'], schema='validus')
    if not index_exists('tbl_validation_details', 'ix_tbl_validation_details_intassettypeid'):
        op.create_index('ix_tbl_validation_details_intassettypeid', 'tbl_validation_details', ['intassettypeid'], schema='validus')
    if not index_exists('tbl_validation_details', 'ix_tbl_validation_details_intcalc_attributeid'):
        op.create_index('ix_tbl_validation_details_intcalc_attributeid', 'tbl_validation_details', ['intcalc_attributeid'], schema='validus')
    
    # Create tbl_ratio_master
    if 'tbl_ratio_master' not in existing_tables:
        op.create_table(
        'tbl_ratio_master',
        sa.Column('intratiomasterid', sa.Integer(), sa.Identity(start=1, increment=1), primary_key=True, nullable=False),
        sa.Column('intsubproductid', sa.Integer(), nullable=False),
        sa.Column('vcsourcetype', sa.String(250), nullable=True),
        sa.Column('vctype', sa.String(250), nullable=True),
        sa.Column('vcrationame', sa.String(250), nullable=True),
        sa.Column('isratio_subtotal', sa.Boolean(), nullable=True),
        sa.Column('vcdescription', sa.String(500), nullable=True),
        sa.Column('intthreshold', sa.Numeric(12, 4), nullable=True),
        sa.Column('vcthresholdtype', sa.String(100), nullable=True),
        sa.Column('intprecision', sa.Numeric(12, 10), nullable=True),
        sa.Column('isactive', sa.Boolean(), server_default=sa.text('TRUE'), nullable=False),
        sa.Column('intcreatedby', sa.Integer(), nullable=True),
        sa.Column('dtcreatedat', sa.DateTime(timezone=False), server_default=sa.text('CURRENT_TIMESTAMP'), nullable=False),
        sa.Column('intupdatedby', sa.Integer(), nullable=True),
        sa.Column('dtupdatedat', sa.DateTime(timezone=False), nullable=True),
        schema='validus'
    )
    # Create foreign key constraint if it doesn't exist
    if not constraint_exists('tbl_ratio_master', 'fk_tbl_ratio_master_intsubproductid'):
        op.create_foreign_key(
            'fk_tbl_ratio_master_intsubproductid',
            'tbl_ratio_master', 'tbl_subproduct_master',
            ['intsubproductid'], ['intsubproductid'],
            source_schema='validus', referent_schema='validus',
            ondelete='CASCADE'
        )
    # Create index if it doesn't exist
    if not index_exists('tbl_ratio_master', 'ix_tbl_ratio_master_intsubproductid'):
        op.create_index('ix_tbl_ratio_master_intsubproductid', 'tbl_ratio_master', ['intsubproductid'], schema='validus')
    
    # Create tbl_ratio_details
    if 'tbl_ratio_details' not in existing_tables:
        op.create_table(
        'tbl_ratio_details',
        sa.Column('intratiodetailid', sa.Integer(), sa.Identity(start=1, increment=1), primary_key=True, nullable=False),
        sa.Column('intratiomasterid', sa.Integer(), nullable=False),
        sa.Column('intdatamodelid', sa.Integer(), nullable=False),
        sa.Column('vcfilter', sa.Text(), nullable=True),
        sa.Column('vcformula', sa.Text(), nullable=True),
        sa.Column('intcreatedby', sa.Integer(), nullable=True),
        sa.Column('dtcreatedat', sa.DateTime(timezone=False), server_default=sa.text('CURRENT_TIMESTAMP'), nullable=False),
        sa.Column('intupdatedby', sa.Integer(), nullable=True),
        sa.Column('dtupdatedat', sa.DateTime(timezone=False), nullable=True),
        schema='validus'
    )
    # Create foreign key constraints if they don't exist
    if not constraint_exists('tbl_ratio_details', 'fk_tbl_ratio_details_intratiomasterid'):
        op.create_foreign_key(
            'fk_tbl_ratio_details_intratiomasterid',
            'tbl_ratio_details', 'tbl_ratio_master',
            ['intratiomasterid'], ['intratiomasterid'],
            source_schema='validus', referent_schema='validus',
            ondelete='CASCADE'
        )
    if not constraint_exists('tbl_ratio_details', 'fk_tbl_ratio_details_intdatamodelid'):
        op.create_foreign_key(
            'fk_tbl_ratio_details_intdatamodelid',
            'tbl_ratio_details', 'tbl_data_model_master',
            ['intdatamodelid'], ['intdatamodelid'],
            source_schema='validus', referent_schema='validus'
        )
    # Create indexes if they don't exist
    if not index_exists('tbl_ratio_details', 'ix_tbl_ratio_details_intratiomasterid'):
        op.create_index('ix_tbl_ratio_details_intratiomasterid', 'tbl_ratio_details', ['intratiomasterid'], schema='validus')
    if not index_exists('tbl_ratio_details', 'ix_tbl_ratio_details_intdatamodelid'):
        op.create_index('ix_tbl_ratio_details_intdatamodelid', 'tbl_ratio_details', ['intdatamodelid'], schema='validus')


def downgrade() -> None:
    # Drop tables in reverse order (to respect foreign key constraints)
    op.drop_table('tbl_ratio_details', schema='validus')
    op.drop_table('tbl_ratio_master', schema='validus')
    op.drop_table('tbl_validation_details', schema='validus')
    op.drop_table('tbl_validation_master', schema='validus')
    op.drop_table('tbl_subproduct_details', schema='validus')
    op.drop_table('tbl_subproduct_master', schema='validus')
    op.drop_table('tbl_data_model_details', schema='validus')
    op.drop_table('tbl_data_model_master', schema='validus')
    
    # Optionally drop the schema (commented out to preserve data)
    # op.execute("DROP SCHEMA IF EXISTS validus CASCADE")
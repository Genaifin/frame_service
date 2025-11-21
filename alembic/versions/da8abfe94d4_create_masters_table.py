"""Create masters table and seed initial data

Revision ID: 20241030_123600
Revises: 20241030_123500
Create Date: 2024-10-30 12:36:00.000000

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql
from sqlalchemy.sql import table, column, text

# revision identifiers, used by Alembic.
revision = 'da8abfe94d4'
down_revision = '30cb044ae8d'
branch_labels = None
depends_on = None

def upgrade():
    # Create masters table
    op.create_table(
        'masters',
        sa.Column('id', sa.Integer(), nullable=False, primary_key=True),
        sa.Column('name', sa.String(100), nullable=False),
        sa.Column('code', sa.String(50), nullable=False, unique=True, index=True),
        sa.Column('description', sa.Text, nullable=True),
        sa.Column('is_active', sa.Boolean, default=True, index=True),
        sa.Column('created_at', sa.DateTime, default=sa.func.now()),
        sa.Column('updated_at', sa.DateTime, default=sa.func.now(), onupdate=sa.func.now()),
        schema=op.get_context().opts.get('schema', 'public')
    )
    
    # Create foreign key for master_id in role_or_client_based_module_level_permissions
    op.create_foreign_key(
        'fk_role_or_client_based_module_level_permissions_master_id',
        'role_or_client_based_module_level_permissions', 'masters',
        ['master_id'], ['id'],
        ondelete='CASCADE',
        source_schema=op.get_context().opts.get('schema', 'public'),
        referent_schema=op.get_context().opts.get('schema', 'public')
    )
    
    # Get the current timestamp in a way that works with bulk_insert
    current_timestamp = text('CURRENT_TIMESTAMP')
    
    # Insert master records one by one to handle the timestamps
    op.execute(
        """
        INSERT INTO masters (name, code, description, is_active, created_at, updated_at)
        VALUES 
            ('Client Master', 'CLIENT_MASTER', 'Client management', true, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
            ('Fund Master', 'FUND_MASTER', 'Fund management', true, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
            ('Fund Manager Master', 'FUND_MANAGER_MASTER', 'Fund manager management', true, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
            ('Account Master', 'ACCOUNT_MASTER', 'Account management', true, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
            ('Process Configuration', 'PROCESS_CONFIGURATION', 'Process configuration', true, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
        """
    )

def downgrade():
    # Drop the foreign key first
    op.drop_constraint(
        'fk_role_or_client_based_module_level_permissions_master_id',
        'role_or_client_based_module_level_permissions',
        type_='foreignkey'
    )
    
    # Drop the masters table
    op.drop_table('masters', schema=op.get_context().opts.get('schema', 'public'))

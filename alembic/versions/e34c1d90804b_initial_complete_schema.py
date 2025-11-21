"""initial_complete_schema

Revision ID: e34c1d90804b
Revises: 
Create Date: 2025-09-11 17:35:12.661482

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'e34c1d90804b'
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Create public schema first (if it doesn't exist)
    op.execute("CREATE SCHEMA IF NOT EXISTS public")
    
    # Create nexbridge schema
    op.execute("CREATE SCHEMA IF NOT EXISTS nexbridge")
    
    # Import sqlalchemy for table creation
    import sqlalchemy as sa
    
    # Create roles table
    op.create_table('roles',
        sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
        sa.Column('role_name', sa.String(length=50), nullable=False),
        sa.Column('role_code', sa.String(length=50), nullable=False),
        sa.Column('description', sa.Text(), nullable=True),
        sa.Column('is_active', sa.Boolean(), nullable=True),
        sa.Column('created_at', sa.DateTime(), nullable=True),
        sa.Column('updated_at', sa.DateTime(), nullable=True),
        sa.PrimaryKeyConstraint('id'),
        schema='public'
    )
    op.create_index(op.f('ix_public_roles_is_active'), 'roles', ['is_active'], unique=False, schema='public')
    op.create_index(op.f('ix_public_roles_role_code'), 'roles', ['role_code'], unique=True, schema='public')
    op.create_index(op.f('ix_public_roles_role_name'), 'roles', ['role_name'], unique=True, schema='public')

    # Create modules table
    op.create_table('modules',
        sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
        sa.Column('module_name', sa.String(length=50), nullable=False),
        sa.Column('module_code', sa.String(length=50), nullable=False),
        sa.Column('description', sa.Text(), nullable=True),
        sa.Column('is_active', sa.Boolean(), nullable=True),
        sa.Column('created_at', sa.DateTime(), nullable=True),
        sa.Column('updated_at', sa.DateTime(), nullable=True),
        sa.PrimaryKeyConstraint('id'),
        schema='public'
    )
    op.create_index(op.f('ix_public_modules_is_active'), 'modules', ['is_active'], unique=False, schema='public')
    op.create_index(op.f('ix_public_modules_module_code'), 'modules', ['module_code'], unique=True, schema='public')
    op.create_index(op.f('ix_public_modules_module_name'), 'modules', ['module_name'], unique=True, schema='public')

    # Create permissions table
    op.create_table('permissions',
        sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
        sa.Column('permission_name', sa.String(length=50), nullable=False),
        sa.Column('permission_code', sa.String(length=50), nullable=False),
        sa.Column('description', sa.Text(), nullable=True),
        sa.Column('is_active', sa.Boolean(), nullable=True),
        sa.Column('created_at', sa.DateTime(), nullable=True),
        sa.Column('updated_at', sa.DateTime(), nullable=True),
        sa.PrimaryKeyConstraint('id'),
        schema='public'
    )
    op.create_index(op.f('ix_public_permissions_is_active'), 'permissions', ['is_active'], unique=False, schema='public')
    op.create_index(op.f('ix_public_permissions_permission_code'), 'permissions', ['permission_code'], unique=True, schema='public')
    op.create_index(op.f('ix_public_permissions_permission_name'), 'permissions', ['permission_name'], unique=True, schema='public')

    # Create clients table
    op.create_table('clients',
        sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
        sa.Column('name', sa.String(length=100), nullable=False),
        sa.Column('code', sa.String(length=50), nullable=False),
        sa.Column('description', sa.Text(), nullable=True),
        sa.Column('type', sa.String(length=50), nullable=True),
        sa.Column('contact_title', sa.String(length=20), nullable=True),
        sa.Column('contact_first_name', sa.String(length=100), nullable=True),
        sa.Column('contact_last_name', sa.String(length=100), nullable=True),
        sa.Column('contact_email', sa.String(length=100), nullable=True),
        sa.Column('contact_number', sa.String(length=30), nullable=True),
        sa.Column('admin_title', sa.String(length=20), nullable=True),
        sa.Column('admin_first_name', sa.String(length=100), nullable=True),
        sa.Column('admin_last_name', sa.String(length=100), nullable=True),
        sa.Column('admin_email', sa.String(length=100), nullable=True),
        sa.Column('admin_job_title', sa.String(length=100), nullable=True),
        sa.Column('is_active', sa.Boolean(), nullable=True),
        sa.Column('created_at', sa.DateTime(), nullable=True),
        sa.Column('updated_at', sa.DateTime(), nullable=True),
        sa.PrimaryKeyConstraint('id'),
        schema='public'
    )
    op.create_index(op.f('ix_public_clients_is_active'), 'clients', ['is_active'], unique=False, schema='public')
    op.create_index(op.f('ix_public_clients_code'), 'clients', ['code'], unique=True, schema='public')
    op.create_index(op.f('ix_public_clients_name'), 'clients', ['name'], unique=True, schema='public')
    op.create_index(op.f('ix_public_clients_type'), 'clients', ['type'], unique=False, schema='public')
    op.create_index(op.f('ix_public_clients_contact_email'), 'clients', ['contact_email'], unique=False, schema='public')
    op.create_index(op.f('ix_public_clients_admin_email'), 'clients', ['admin_email'], unique=False, schema='public')

    # Create funds table
    op.create_table('funds',
        sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
        sa.Column('name', sa.String(length=150), nullable=False),
        sa.Column('code', sa.String(length=80), nullable=False),
        sa.Column('description', sa.Text(), nullable=True),
        sa.Column('type', sa.String(length=50), nullable=True),
        sa.Column('fund_manager', sa.Text(), nullable=True),
        sa.Column('base_currency', sa.String(length=10), nullable=True),
        sa.Column('fund_admin', sa.JSON(), nullable=True),
        sa.Column('shadow', sa.JSON(), nullable=True),
        sa.Column('contact_person', sa.String(length=100), nullable=True),
        sa.Column('contact_email', sa.String(length=100), nullable=True),
        sa.Column('contact_number', sa.String(length=30), nullable=True),
        sa.Column('sector', sa.String(length=100), nullable=True),
        sa.Column('geography', sa.String(length=100), nullable=True),
        sa.Column('strategy', sa.JSON(), nullable=True),
        sa.Column('market_cap', sa.String(length=50), nullable=True),
        sa.Column('benchmark', sa.JSON(), nullable=True),
        sa.Column('metadata', sa.JSON(), nullable=True),
        sa.Column('is_active', sa.Boolean(), nullable=True),
        sa.Column('created_at', sa.DateTime(), nullable=True),
        sa.Column('updated_at', sa.DateTime(), nullable=True),
        sa.PrimaryKeyConstraint('id'),
        schema='public'
    )
    op.create_index(op.f('ix_public_funds_fund_name'), 'funds', ['name'], unique=True, schema='public')
    op.create_index(op.f('ix_public_funds_fund_code'), 'funds', ['code'], unique=True, schema='public')
    op.create_index(op.f('ix_public_funds_is_active'), 'funds', ['is_active'], unique=False, schema='public')
    op.create_index(op.f('ix_public_funds_type'), 'funds', ['type'], unique=False, schema='public')
    op.create_index(op.f('ix_public_funds_sector'), 'funds', ['sector'], unique=False, schema='public')
    op.create_index(op.f('ix_public_funds_geography'), 'funds', ['geography'], unique=False, schema='public')
    op.create_index(op.f('ix_public_funds_contact_email'), 'funds', ['contact_email'], unique=False, schema='public')

    # Create client_funds association table
    op.create_table('client_funds',
        sa.Column('client_id', sa.Integer(), nullable=False),
        sa.Column('fund_id', sa.Integer(), nullable=False),
        sa.Column('created_at', sa.DateTime(), nullable=True),
        sa.Column('updated_at', sa.DateTime(), nullable=True),
        sa.ForeignKeyConstraint(['client_id'], ['public.clients.id'], ondelete='CASCADE'),
        sa.ForeignKeyConstraint(['fund_id'], ['public.funds.id'], ondelete='CASCADE'),
        sa.PrimaryKeyConstraint('client_id', 'fund_id'),
        schema='public'
    )

    # Create users table
    op.create_table('users',
        sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
        sa.Column('username', sa.String(length=50), nullable=False),
        sa.Column('email', sa.String(length=100), nullable=True),
        sa.Column('display_name', sa.String(length=100), nullable=False),
        sa.Column('password_hash', sa.String(length=255), nullable=False),
        sa.Column('role_id', sa.Integer(), nullable=False),
        sa.Column('client_id', sa.Integer(), nullable=True),
        sa.Column('is_active', sa.Boolean(), nullable=True),
        sa.Column('created_at', sa.DateTime(), nullable=True),
        sa.Column('updated_at', sa.DateTime(), nullable=True),
        sa.ForeignKeyConstraint(['role_id'], ['public.roles.id'], ),
        sa.ForeignKeyConstraint(['client_id'], ['public.clients.id'], ondelete='SET NULL'),
        sa.PrimaryKeyConstraint('id'),
        schema='public'
    )
    op.create_index(op.f('ix_public_users_email'), 'users', ['email'], unique=True, schema='public')
    op.create_index(op.f('ix_public_users_is_active'), 'users', ['is_active'], unique=False, schema='public')
    op.create_index(op.f('ix_public_users_role_id'), 'users', ['role_id'], unique=False, schema='public')
    op.create_index(op.f('ix_public_users_client_id'), 'users', ['client_id'], unique=False, schema='public')
    op.create_index(op.f('ix_public_users_username'), 'users', ['username'], unique=True, schema='public')

    # Create user_client table
    op.create_table('user_client',
        sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
        sa.Column('user_id', sa.Integer(), nullable=False),
        sa.Column('client_id', sa.Integer(), nullable=False),
        sa.Column('is_active', sa.Boolean(), nullable=True),
        sa.Column('created_at', sa.DateTime(), nullable=True),
        sa.Column('updated_at', sa.DateTime(), nullable=True),
        sa.ForeignKeyConstraint(['client_id'], ['public.clients.id'], ondelete='CASCADE'),
        sa.ForeignKeyConstraint(['user_id'], ['public.users.id'], ondelete='CASCADE'),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('user_id', 'client_id', name='uq_user_client'),
        schema='public'
    )
    op.create_index(op.f('ix_public_user_client_user_id'), 'user_client', ['user_id'], unique=False, schema='public')
    op.create_index(op.f('ix_public_user_client_client_id'), 'user_client', ['client_id'], unique=False, schema='public')
    op.create_index(op.f('ix_public_user_client_is_active'), 'user_client', ['is_active'], unique=False, schema='public')

    # Create role_module_permissions table
    op.create_table('role_module_permissions',
        sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
        sa.Column('role_id', sa.Integer(), nullable=False),
        sa.Column('module_id', sa.Integer(), nullable=False),
        sa.Column('permission_id', sa.Integer(), nullable=False),
        sa.Column('is_active', sa.Boolean(), nullable=True),
        sa.Column('created_at', sa.DateTime(), nullable=True),
        sa.Column('updated_at', sa.DateTime(), nullable=True),
        sa.ForeignKeyConstraint(['module_id'], ['public.modules.id'], ondelete='CASCADE'),
        sa.ForeignKeyConstraint(['permission_id'], ['public.permissions.id'], ondelete='CASCADE'),
        sa.ForeignKeyConstraint(['role_id'], ['public.roles.id'], ondelete='CASCADE'),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('role_id', 'module_id', 'permission_id', name='uq_role_module_permission'),
        schema='public'
    )
    op.create_index(op.f('ix_public_role_module_permissions_is_active'), 'role_module_permissions', ['is_active'], unique=False, schema='public')
    op.create_index(op.f('ix_public_role_module_permissions_module_id'), 'role_module_permissions', ['module_id'], unique=False, schema='public')
    op.create_index(op.f('ix_public_role_module_permissions_permission_id'), 'role_module_permissions', ['permission_id'], unique=False, schema='public')
    op.create_index(op.f('ix_public_role_module_permissions_role_id'), 'role_module_permissions', ['role_id'], unique=False, schema='public')

    # NEXBRIDGE SCHEMA TABLES
    
    # Create KPI library table
    op.create_table('kpi_library',
        sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
        sa.Column('kpi_code', sa.String(length=100), nullable=False),
        sa.Column('kpi_name', sa.String(length=200), nullable=False),
        sa.Column('kpi_type', sa.String(length=50), nullable=False),
        sa.Column('category', sa.String(length=100), nullable=True),
        sa.Column('description', sa.Text(), nullable=True),
        sa.Column('source_type', sa.String(length=50), nullable=False),
        sa.Column('precision_type', sa.String(length=50), nullable=False),
        sa.Column('numerator_field', sa.String(length=200), nullable=True),
        sa.Column('denominator_field', sa.String(length=200), nullable=True),
        sa.Column('numerator_description', sa.Text(), nullable=True),
        sa.Column('denominator_description', sa.Text(), nullable=True),
        sa.Column('is_active', sa.Boolean(), nullable=True),
        sa.Column('created_at', sa.DateTime(), nullable=True),
        sa.Column('updated_at', sa.DateTime(), nullable=True),
        sa.Column('created_by', sa.String(length=100), nullable=True),
        sa.Column('updated_by', sa.String(length=100), nullable=True),
        sa.CheckConstraint("kpi_type IN ('NAV_VALIDATION', 'RATIO_VALIDATION')", name='chk_kpi_type'),
        sa.CheckConstraint("kpi_type != 'RATIO_VALIDATION' OR (numerator_field IS NOT NULL AND denominator_field IS NOT NULL)", name='chk_ratio_fields_required'),
        sa.CheckConstraint("precision_type IN ('PERCENTAGE', 'ABSOLUTE')", name='chk_precision_type'),
        sa.CheckConstraint("source_type IN ('SINGLE_SOURCE', 'DUAL_SOURCE')", name='chk_source_type'),
        sa.UniqueConstraint('kpi_code', 'kpi_name', 'kpi_type', 'category', 'source_type', name='uq_kpi_combination'),
        sa.PrimaryKeyConstraint('id'),
        schema='nexbridge'
    )
    op.create_index(op.f('ix_nexbridge_kpi_library_category'), 'kpi_library', ['category'], unique=False, schema='nexbridge')
    op.create_index(op.f('ix_nexbridge_kpi_library_is_active'), 'kpi_library', ['is_active'], unique=False, schema='nexbridge')
    op.create_index(op.f('ix_nexbridge_kpi_library_kpi_code'), 'kpi_library', ['kpi_code'], unique=True, schema='nexbridge')
    op.create_index(op.f('ix_nexbridge_kpi_library_kpi_type'), 'kpi_library', ['kpi_type'], unique=False, schema='nexbridge')

    # Create source table
    op.create_table('source',
        sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
        sa.Column('name', sa.String(length=255), nullable=False),
        sa.Column('created_at', sa.DateTime(), nullable=True),
        sa.Column('created_by', sa.String(length=255), nullable=False),
        sa.PrimaryKeyConstraint('id'),
        schema='nexbridge'
    )
    op.create_index(op.f('ix_nexbridge_source_name'), 'source', ['name'], unique=True, schema='nexbridge')
    
    # Create KPI thresholds table
    op.create_table('kpi_thresholds',
        sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
        sa.Column('kpi_id', sa.Integer(), nullable=False),
        sa.Column('fund_id', sa.String(length=100), nullable=True),
        sa.Column('threshold_value', sa.Numeric(precision=18, scale=6), nullable=False),
        sa.Column('is_active', sa.Boolean(), nullable=True),
        sa.Column('created_at', sa.DateTime(), nullable=True),
        sa.Column('updated_at', sa.DateTime(), nullable=True),
        sa.Column('created_by', sa.String(length=100), nullable=True),
        sa.Column('updated_by', sa.String(length=100), nullable=True),
        sa.ForeignKeyConstraint(['kpi_id'], ['nexbridge.kpi_library.id'], ondelete='CASCADE'),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('kpi_id', 'fund_id', name='uq_kpi_fund_threshold'),
        schema='nexbridge'
    )
    op.create_index(op.f('ix_nexbridge_kpi_thresholds_fund_id'), 'kpi_thresholds', ['fund_id'], unique=False, schema='nexbridge')
    op.create_index(op.f('ix_nexbridge_kpi_thresholds_is_active'), 'kpi_thresholds', ['is_active'], unique=False, schema='nexbridge')
    op.create_index(op.f('ix_nexbridge_kpi_thresholds_kpi_id'), 'kpi_thresholds', ['kpi_id'], unique=False, schema='nexbridge')

    # Create nav_pack table
    op.create_table('nav_pack',
        sa.Column('navpack_id', sa.Integer(), autoincrement=True, nullable=False),
        sa.Column('fund_id', sa.Integer(), nullable=False),
        sa.Column('source_id', sa.Integer(), nullable=False),
        sa.Column('file_date', sa.Date(), nullable=False),
        sa.ForeignKeyConstraint(['source_id'], ['nexbridge.source.id'], ),
        sa.PrimaryKeyConstraint('navpack_id'),
        sa.UniqueConstraint('fund_id', 'source_id', 'file_date', name='uq_fund_source_date'),
        schema='nexbridge'
    )
    op.create_index(op.f('ix_nexbridge_nav_pack_fund_id'), 'nav_pack', ['fund_id'], unique=False, schema='nexbridge')
    op.create_index(op.f('ix_nexbridge_nav_pack_source_id'), 'nav_pack', ['source_id'], unique=False, schema='nexbridge')
    op.create_index(op.f('ix_nexbridge_nav_pack_file_date'), 'nav_pack', ['file_date'], unique=False, schema='nexbridge')
    
    # Create navpack_version table
    op.create_table('navpack_version',
        sa.Column('navpack_version_id', sa.Integer(), autoincrement=True, nullable=False),
        sa.Column('navpack_id', sa.Integer(), nullable=False),
        sa.Column('version', sa.Integer(), nullable=False),
        sa.Column('file_name', sa.String(length=255), nullable=False),
        sa.Column('uploaded_by', sa.String(length=255), nullable=False),
        sa.Column('uploaded_on', sa.DateTime(), nullable=True),
        sa.Column('override_on', sa.DateTime(), nullable=True),
        sa.Column('override_by', sa.String(length=255), nullable=True),
        sa.Column('base_version', sa.Integer(), nullable=True),
        sa.CheckConstraint('version > 0', name='chk_positive_version'),
        sa.ForeignKeyConstraint(['base_version'], ['nexbridge.navpack_version.navpack_version_id'], ),
        sa.ForeignKeyConstraint(['navpack_id'], ['nexbridge.nav_pack.navpack_id'], ),
        sa.PrimaryKeyConstraint('navpack_version_id'),
        sa.UniqueConstraint('navpack_id', 'version', name='uq_navpack_version'),
        schema='nexbridge'
    )
    op.create_index(op.f('ix_nexbridge_navpack_version_navpack_id'), 'navpack_version', ['navpack_id'], unique=False, schema='nexbridge')
    op.create_index(op.f('ix_nexbridge_navpack_version_version'), 'navpack_version', ['version'], unique=False, schema='nexbridge')
    
    # Create trial_balance table
    op.create_table('trial_balance',
        sa.Column('row_id', sa.Integer(), autoincrement=True, nullable=False),
        sa.Column('type', sa.String(length=255), nullable=False),
        sa.Column('category', sa.String(length=255), nullable=True),
        sa.Column('accounting_head', sa.String(length=255), nullable=True),
        sa.Column('financial_account', sa.String(length=255), nullable=False),
        sa.Column('ending_balance', sa.Numeric(precision=15, scale=2), nullable=True),
        sa.Column('navpack_version_id', sa.Integer(), nullable=False),
        sa.ForeignKeyConstraint(['navpack_version_id'], ['nexbridge.navpack_version.navpack_version_id'], ),
        sa.PrimaryKeyConstraint('row_id'),
        schema='nexbridge'
    )
    op.create_index(op.f('ix_nexbridge_trial_balance_type'), 'trial_balance', ['type'], unique=False, schema='nexbridge')
    op.create_index(op.f('ix_nexbridge_trial_balance_category'), 'trial_balance', ['category'], unique=False, schema='nexbridge')
    op.create_index(op.f('ix_nexbridge_trial_balance_accounting_head'), 'trial_balance', ['accounting_head'], unique=False, schema='nexbridge')
    op.create_index(op.f('ix_nexbridge_trial_balance_financial_account'), 'trial_balance', ['financial_account'], unique=False, schema='nexbridge')
    op.create_index(op.f('ix_nexbridge_trial_balance_navpack_version_id'), 'trial_balance', ['navpack_version_id'], unique=False, schema='nexbridge')
    
    # Create portfolio_valuation table
    op.create_table('portfolio_valuation',
        sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
        sa.Column('inv_type', sa.String(length=255), nullable=False),
        sa.Column('inv_id', sa.String(length=255), nullable=False),
        sa.Column('end_qty', sa.Numeric(precision=18, scale=6), nullable=False),
        sa.Column('end_local_market_price', sa.Numeric(precision=18, scale=6), nullable=True),
        sa.Column('end_local_mv', sa.Numeric(precision=18, scale=2), nullable=True),
        sa.Column('navpack_version_id', sa.Integer(), nullable=False),
        sa.ForeignKeyConstraint(['navpack_version_id'], ['nexbridge.navpack_version.navpack_version_id'], ),
        sa.PrimaryKeyConstraint('id'),
        schema='nexbridge'
    )
    op.create_index(op.f('ix_nexbridge_portfolio_valuation_inv_type'), 'portfolio_valuation', ['inv_type'], unique=False, schema='nexbridge')
    op.create_index(op.f('ix_nexbridge_portfolio_valuation_inv_id'), 'portfolio_valuation', ['inv_id'], unique=False, schema='nexbridge')
    op.create_index(op.f('ix_nexbridge_portfolio_valuation_navpack_version_id'), 'portfolio_valuation', ['navpack_version_id'], unique=False, schema='nexbridge')
    
    # Create dividend table
    op.create_table('dividend',
        sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
        sa.Column('security_id', sa.String(length=255), nullable=False),
        sa.Column('security_name', sa.String(length=255), nullable=False),
        sa.Column('amount', sa.Numeric(precision=18, scale=2), nullable=False),
        sa.Column('navpack_version_id', sa.Integer(), nullable=False),
        sa.ForeignKeyConstraint(['navpack_version_id'], ['nexbridge.navpack_version.navpack_version_id'], ),
        sa.PrimaryKeyConstraint('id'),
        schema='nexbridge'
    )
    op.create_index(op.f('ix_nexbridge_dividend_security_id'), 'dividend', ['security_id'], unique=False, schema='nexbridge')
    op.create_index(op.f('ix_nexbridge_dividend_security_name'), 'dividend', ['security_name'], unique=False, schema='nexbridge')
    op.create_index(op.f('ix_nexbridge_dividend_navpack_version_id'), 'dividend', ['navpack_version_id'], unique=False, schema='nexbridge')


def downgrade() -> None:
    # Drop tables in reverse order (dependencies first)
    
    # Drop nexbridge tables first (due to foreign key dependencies)
    op.drop_table('dividend', schema='nexbridge')
    op.drop_table('portfolio_valuation', schema='nexbridge')
    op.drop_table('trial_balance', schema='nexbridge')
    op.drop_table('navpack_version', schema='nexbridge')
    op.drop_table('nav_pack', schema='nexbridge')
    op.drop_table('kpi_thresholds', schema='nexbridge')
    op.drop_table('source', schema='nexbridge')
    op.drop_table('kpi_library', schema='nexbridge')
    
    # Drop public schema tables
    op.drop_table('role_module_permissions', schema='public')
    op.drop_table('user_client', schema='public')
    op.drop_table('users', schema='public')
    op.drop_table('client_funds', schema='public')
    op.drop_table('funds', schema='public')
    op.drop_table('clients', schema='public')
    op.drop_table('permissions', schema='public')
    op.drop_table('modules', schema='public')
    op.drop_table('roles', schema='public')

"""add_module_hierarchy_support

Revision ID: c6a09e1609ac
Revises: 6bcb71ea4cfa
Create Date: 2025-10-07 15:51:25.314844

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'c6a09e1609ac'
down_revision: Union[str, None] = '6bcb71ea4cfa'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Add hierarchy columns to modules table
    op.add_column('modules', sa.Column('parent_id', sa.Integer(), nullable=True), schema='public')
    op.add_column('modules', sa.Column('level', sa.Integer(), nullable=True), schema='public')  # Make nullable first
    op.add_column('modules', sa.Column('sort_order', sa.Integer(), nullable=True), schema='public')  # Make nullable first
    
    # Update existing modules to have level 0 (root modules) and sort_order 1
    op.execute("UPDATE public.modules SET level = 0, sort_order = 1 WHERE level IS NULL")
    
    # Now make the columns NOT NULL
    op.alter_column('modules', 'level', nullable=False, schema='public')
    op.alter_column('modules', 'sort_order', nullable=False, schema='public')
    
    # Add foreign key constraint for parent_id
    op.create_foreign_key(
        'fk_modules_parent_id',
        'modules', 'modules',
        ['parent_id'], ['id'],
        source_schema='public',
        referent_schema='public',
        ondelete='CASCADE'
    )
    
    # Add indexes for performance
    op.create_index('ix_public_modules_parent_id', 'modules', ['parent_id'], schema='public')
    op.create_index('ix_public_modules_level', 'modules', ['level'], schema='public')
    op.create_index('ix_public_modules_sort_order', 'modules', ['sort_order'], schema='public')
    
    # Insert the hierarchical module structure
    # First, insert main modules (they should already exist, but let's ensure they have correct level)
    op.execute("""
        INSERT INTO public.modules (module_name, module_code, description, level, sort_order, is_active, created_at, updated_at)
        VALUES 
        ('Frame', 'frame', 'Frame module for document processing', 0, 1, true, NOW(), NOW()),
        ('NAV Validus', 'nav_validus', 'NAV Validus module for NAV processing', 0, 2, true, NOW(), NOW())
        ON CONFLICT (module_name) DO UPDATE SET level = 0, sort_order = EXCLUDED.sort_order
    """)
    
    # Clean up duplicate Validus modules and reassign IDs properly
    op.execute("""
        -- First, update any existing 'Validus' to 'NAV Validus' if 'NAV Validus' doesn't exist
        UPDATE public.modules 
        SET module_name = 'NAV Validus', 
            module_code = 'nav_validus',
            description = 'NAV Validus module for NAV processing'
        WHERE module_name = 'Validus' 
        AND module_code = 'validus'
        AND NOT EXISTS (
            SELECT 1 FROM public.modules 
            WHERE module_name = 'NAV Validus' 
            AND module_code = 'nav_validus'
        );
    """)
    
    op.execute("""
        -- If both exist, we need to handle this carefully to avoid ID gaps
        -- First, update any role_module_permissions that reference the old 'Validus' module
        UPDATE public.role_module_permissions 
        SET module_id = (
            SELECT id FROM public.modules 
            WHERE module_name = 'NAV Validus' 
            AND module_code = 'nav_validus'
        )
        WHERE module_id = (
            SELECT id FROM public.modules 
            WHERE module_name = 'Validus' 
            AND module_code = 'validus'
        )
        AND EXISTS (
            SELECT 1 FROM public.modules 
            WHERE module_name = 'NAV Validus' 
            AND module_code = 'nav_validus'
        );
    """)
    
    op.execute("""
        -- Now delete the old 'Validus' module if 'NAV Validus' already exists
        DELETE FROM public.modules 
        WHERE module_name = 'Validus' 
        AND module_code = 'validus'
        AND EXISTS (
            SELECT 1 FROM public.modules 
            WHERE module_name = 'NAV Validus' 
            AND module_code = 'nav_validus'
        );
    """)
    
    # Reassign IDs to eliminate gaps and maintain proper sequence
    op.execute("""
        -- Create a temporary sequence to reassign IDs properly
        CREATE TEMPORARY TABLE module_id_mapping AS
        SELECT 
            id as old_id,
            ROW_NUMBER() OVER (ORDER BY sort_order, module_name) as new_id
        FROM public.modules 
        WHERE level = 0
        ORDER BY sort_order, module_name;
    """)
    
    op.execute("""
        -- Update role_module_permissions to use new IDs
        UPDATE public.role_module_permissions 
        SET module_id = mapping.new_id
        FROM module_id_mapping mapping
        WHERE role_module_permissions.module_id = mapping.old_id;
    """)
    
    op.execute("""
        -- Update parent_id references in modules table
        UPDATE public.modules 
        SET parent_id = mapping.new_id
        FROM module_id_mapping mapping
        WHERE modules.parent_id = mapping.old_id;
    """)
    
    op.execute("""
        -- Reassign module IDs to eliminate gaps
        UPDATE public.modules 
        SET id = mapping.new_id
        FROM module_id_mapping mapping
        WHERE modules.id = mapping.old_id;
    """)
    
    op.execute("""
        -- Reset the sequence to continue from the highest ID
        SELECT setval('public.modules_id_seq', (SELECT MAX(id) FROM public.modules));
    """)
    
    op.execute("""
        -- Clean up temporary table
        DROP TABLE module_id_mapping;
    """)
    
    # Insert submodules (level 1)
    op.execute("""
        INSERT INTO public.modules (module_name, module_code, description, parent_id, level, sort_order, is_active, created_at, updated_at)
        SELECT 
            submodule_name,
            LOWER(REPLACE(submodule_name, ' ', '_')),
            submodule_name || ' submodule',
            m.id,
            1,
            submodule_order,
            true,
            NOW(),
            NOW()
        FROM (VALUES 
            ('Dashboard', 'frame', 1),
            ('File Manager', 'frame', 2),
            ('Single Fund', 'nav_validus', 1),
            ('Multi Fund', 'nav_validus', 2)
        ) AS submodules(submodule_name, parent_code, submodule_order)
        JOIN public.modules m ON m.module_code = submodules.parent_code
        ON CONFLICT (module_name) DO UPDATE SET 
            parent_id = EXCLUDED.parent_id,
            level = 1,
            sort_order = EXCLUDED.sort_order
    """)
    
    # Insert sub-submodules (level 2)
    op.execute("""
        INSERT INTO public.modules (module_name, module_code, description, parent_id, level, sort_order, is_active, created_at, updated_at)
        SELECT 
            subsubmodule_name,
            LOWER(REPLACE(subsubmodule_name, ' ', '_')),
            subsubmodule_name || ' sub-submodule',
            sm.id,
            2,
            subsubmodule_order,
            true,
            NOW(),
            NOW()
        FROM (VALUES 
            ('Statuswise Dashboard', 'Dashboard', 1),
            ('Completeness Dashboard', 'Dashboard', 2),
            ('File Info', 'File Manager', 1),
            ('NAV Validations', 'Single Fund', 1),
            ('Ratio Validations', 'Single Fund', 2),
            ('Validations', 'Multi Fund', 1)
        ) AS subsubmodules(subsubmodule_name, parent_name, subsubmodule_order)
        JOIN public.modules sm ON sm.module_name = subsubmodules.parent_name AND sm.level = 1
        ON CONFLICT (module_name) DO UPDATE SET 
            parent_id = EXCLUDED.parent_id,
            level = 2,
            sort_order = EXCLUDED.sort_order
    """)
    
    # Fix foreign key schema references for fund_investors table
    op.execute("""
        -- Drop existing foreign key constraints
        ALTER TABLE public.fund_investors DROP CONSTRAINT IF EXISTS fund_investors_fund_id_fkey;
        ALTER TABLE public.fund_investors DROP CONSTRAINT IF EXISTS fund_investors_investor_id_fkey;
        
        -- Add new foreign key constraints with proper schema references
        ALTER TABLE public.fund_investors 
        ADD CONSTRAINT fund_investors_fund_id_fkey 
        FOREIGN KEY (fund_id) REFERENCES public.funds(id) ON DELETE CASCADE;
        
        ALTER TABLE public.fund_investors 
        ADD CONSTRAINT fund_investors_investor_id_fkey 
        FOREIGN KEY (investor_id) REFERENCES public.investors(id) ON DELETE CASCADE;
    """)


def downgrade() -> None:
    # Drop indexes
    op.drop_index('ix_public_modules_sort_order', 'modules', schema='public')
    op.drop_index('ix_public_modules_level', 'modules', schema='public')
    op.drop_index('ix_public_modules_parent_id', 'modules', schema='public')
    
    # Drop foreign key constraint
    op.drop_constraint('fk_modules_parent_id', 'modules', schema='public')
    
    # Drop hierarchy columns
    op.drop_column('modules', 'sort_order', schema='public')
    op.drop_column('modules', 'level', schema='public')
    op.drop_column('modules', 'parent_id', schema='public')
    
    # Revert foreign key schema references for fund_investors table
    op.execute("""
        -- Drop the new foreign key constraints
        ALTER TABLE public.fund_investors DROP CONSTRAINT IF EXISTS fund_investors_fund_id_fkey;
        ALTER TABLE public.fund_investors DROP CONSTRAINT IF EXISTS fund_investors_investor_id_fkey;
        
        -- Recreate the old foreign key constraints without schema
        ALTER TABLE public.fund_investors 
        ADD CONSTRAINT fund_investors_fund_id_fkey 
        FOREIGN KEY (fund_id) REFERENCES funds(id) ON DELETE CASCADE;
        
        ALTER TABLE public.fund_investors 
        ADD CONSTRAINT fund_investors_investor_id_fkey 
        FOREIGN KEY (investor_id) REFERENCES investors(id) ON DELETE CASCADE;
    """)

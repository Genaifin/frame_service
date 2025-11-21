"""Rename role_module_permissions and add client/master support

Revision ID: 20241030_123500
Revises: 
Create Date: 2024-10-30 12:35:00.000000

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = '30cb044ae8d'
down_revision = 'f5157a6d89d7' 
branch_labels = None
depends_on = None

def upgrade():
    # 1. Rename the table
    op.rename_table('role_module_permissions', 'role_or_client_based_module_level_permissions')
    
    # 2. First, make role_id and module_id nullable
    op.alter_column('role_or_client_based_module_level_permissions', 'role_id',
                   existing_type=sa.INTEGER(),
                   nullable=True,
                   existing_server_default=sa.text("1"))
    
    op.alter_column('role_or_client_based_module_level_permissions', 'module_id',
                   existing_type=sa.INTEGER(),
                   nullable=True)
    
    # 3. Add new columns as nullable
    op.add_column('role_or_client_based_module_level_permissions', 
                 sa.Column('client_id', sa.Integer(), nullable=True))
    op.add_column('role_or_client_based_module_level_permissions', 
                 sa.Column('master_id', sa.Integer(), nullable=True))
    op.add_column('role_or_client_based_module_level_permissions',
                 sa.Column('client_has_permission', sa.Boolean(), nullable=True))
    
    # 3. Create indexes
    op.create_index(op.f('ix_role_or_client_based_module_level_permissions_client_id'), 
                   'role_or_client_based_module_level_permissions', ['client_id'], 
                   unique=False)
    op.create_index(op.f('ix_role_or_client_based_module_level_permissions_master_id'), 
                   'role_or_client_based_module_level_permissions', ['master_id'], 
                   unique=False)
    
    # 4. Add foreign key constraints
    op.create_foreign_key(
        'fk_role_or_client_based_module_level_permissions_client_id',
        'role_or_client_based_module_level_permissions', 'clients',
        ['client_id'], ['id'],
        ondelete='CASCADE'
    )
    
    # 5. Make permission_id nullable for client-based permissions
    op.alter_column('role_or_client_based_module_level_permissions', 'permission_id',
                   existing_type=sa.INTEGER(),
                   nullable=True,
                   existing_comment=None,
               )
    
    # 6. First add a more lenient constraint
    op.create_check_constraint(
        'chk_role_or_client_lenient',
        'role_or_client_based_module_level_permissions',
        """
        (
            (role_id IS NOT NULL AND client_id IS NULL) OR 
            (role_id IS NULL AND client_id IS NOT NULL)
        )
        """
    )
    
    # 6. Update existing data to meet the new constraints
    conn = op.get_bind()
    from sqlalchemy import text
    conn.execute(text("""
        UPDATE role_or_client_based_module_level_permissions
        SET client_has_permission = FALSE
        WHERE client_id IS NOT NULL
    """))
    
    # 7. Now add the stricter constraints
    # First drop the lenient constraint if it exists
    try:
        op.drop_constraint('chk_role_or_client_lenient', 'role_or_client_based_module_level_permissions', type_='check')
    except Exception as e:
        print(f"Warning: Could not drop lenient constraint: {str(e)}")
    
    # Constraint for role vs client
    op.create_check_constraint(
        'chk_role_or_client',
        'role_or_client_based_module_level_permissions',
        """
        (
            (role_id IS NOT NULL AND client_id IS NULL AND client_has_permission IS NULL) OR 
            (role_id IS NULL AND client_id IS NOT NULL AND permission_id IS NULL)
        )
        """
    )
    
    # Constraint for module vs master
    op.create_check_constraint(
        'chk_module_or_master',
        'role_or_client_based_module_level_permissions',
        """
        (module_id IS NOT NULL AND master_id IS NULL) OR 
        (module_id IS NULL AND master_id IS NOT NULL)
        """
    )
    
    # Constraint for client_has_permission
    op.create_check_constraint(
        'chk_client_permission',
        'role_or_client_based_module_level_permissions',
        """
        (client_id IS NULL AND client_has_permission IS NULL) OR 
        (client_id IS NOT NULL AND client_has_permission IS NOT NULL)
        """
    )

def downgrade():
    # Get the connection to execute raw SQL
    conn = op.get_bind()
    
    # Import text from sqlalchemy
    from sqlalchemy import text
    
    # Check if the table exists first
    table_exists = conn.execute(text("""
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_schema = 'public'
            AND table_name = 'role_or_client_based_module_level_permissions'
        )
    """)).scalar()
    
    if not table_exists:
        print("Table 'role_or_client_based_module_level_permissions' doesn't exist. Skipping downgrade.")
        return
    
    # List of constraints to drop - try both old and new naming patterns
    constraint_pairs = [
        ('chk_role_or_client', 'check'),
        ('fk_role_or_client_based_module_level_permissions_client_id', 'foreignkey'),
        ('role_module_permissions_module_id_fkey', 'foreignkey'),
        ('role_or_client_based_module_level_permissions_module_id_fkey', 'foreignkey'),
        ('role_module_permissions_permission_id_fkey', 'foreignkey'),
        ('role_or_client_based_module_level_permissions_permission_id_fkey', 'foreignkey'),
        ('role_module_permissions_pkey', 'primary'),
        ('role_or_client_based_module_level_permissions_pkey', 'primary'),
        ('role_module_permissions_role_id_fkey', 'foreignkey'),
        ('role_or_client_based_module_level_permissions_role_id_fkey', 'foreignkey'),
        ('uq_role_module_permission', 'unique'),
        ('uq_role_or_client_based_module_level_permission', 'unique')
    ]
    
    # Drop each constraint if it exists
    for constraint_name, constraint_type in constraint_pairs:
        try:
            op.drop_constraint(
                constraint_name,
                'role_or_client_based_module_level_permissions',
                type_=constraint_type
            )
            print(f"Dropped constraint: {constraint_name}")
        except Exception as e:
            # Only print error if it's not a "does not exist" error
            if 'does not exist' not in str(e):
                print(f"Could not drop constraint {constraint_name}: {str(e)}")
            conn.execute(text('ROLLBACK'))
    
    # Drop indexes if they exist
    for index in ['ix_role_or_client_based_module_level_permissions_master_id', 
                 'ix_role_or_client_based_module_level_permissions_client_id']:
        try:
            op.drop_index(op.f(index), table_name='role_or_client_based_module_level_permissions')
        except Exception:
            conn.execute(text('ROLLBACK'))
    
    # Drop columns if they exist
    for column in ['master_id', 'client_id', 'client_has_permission']:
        try:
            op.drop_column('role_or_client_based_module_level_permissions', column)
        except Exception:
            conn.execute(text('ROLLBACK'))
            
    # Make columns NOT NULL again
    try:
        op.alter_column('role_or_client_based_module_level_permissions', 'module_id',
                       existing_type=sa.INTEGER(),
                       nullable=False)
        op.alter_column('role_or_client_based_module_level_permissions', 'permission_id',
                       existing_type=sa.INTEGER(),
                       nullable=False)
    except Exception as e:
        print(f"Error setting columns to NOT NULL: {str(e)}")
        conn.execute(text('ROLLBACK'))
        
    # Drop the constraints we added, but only if they exist
    for constraint in ['chk_role_or_client', 'chk_module_or_master', 'chk_client_permission']:
        try:
            # Check if constraint exists before trying to drop it
            constraint_exists = conn.execute(
                text("""
                    SELECT 1 FROM information_schema.table_constraints 
                    WHERE table_name = 'role_or_client_based_module_level_permissions' 
                    AND constraint_name = :constraint_name
                    AND constraint_type = 'CHECK'
                """).bindparams(constraint_name=constraint)
            ).scalar()
            
            if constraint_exists:
                op.drop_constraint(
                    constraint, 
                    'role_or_client_based_module_level_permissions', 
                    type_='check'
                )
        except Exception as e:
            print(f"Warning: Could not drop constraint {constraint}: {str(e)}")
            conn.execute(text('ROLLBACK'))
            
    # Rename the table back
    try:
        op.rename_table('role_or_client_based_module_level_permissions', 'role_module_permissions')
    except Exception as e:
        print(f"Error renaming table: {str(e)}")
        raise
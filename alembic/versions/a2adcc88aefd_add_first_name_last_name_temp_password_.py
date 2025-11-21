"""add_first_name_last_name_temp_password_to_users

Revision ID: a2adcc88aefd
Revises: 777483469cff
Create Date: 2025-09-22 17:18:08.769295

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'a2adcc88aefd'
down_revision: Union[str, None] = '777483469cff'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Add first_name column (nullable initially for data population)
    op.add_column('users', sa.Column('first_name', sa.String(length=50), nullable=True), schema='public')
    
    # Add last_name column (nullable initially for data population)
    op.add_column('users', sa.Column('last_name', sa.String(length=50), nullable=True), schema='public')
    
    # Add temp_password column
    op.add_column('users', sa.Column('temp_password', sa.Boolean(), nullable=False, server_default='false'), schema='public')
    
    # Update existing users with first_name and last_name based on display_name
    from sqlalchemy import text
    connection = op.get_bind()
    
    # Get all users and populate first_name and last_name
    result = connection.execute(text("SELECT id, username, display_name FROM public.users WHERE first_name IS NULL OR first_name = ''"))
    
    for user in result:
        user_id, username, display_name = user
        
        if display_name and display_name.strip():
            # For names like "Roshi D." or "K.B" - use entire display_name as first_name
            name_parts = display_name.strip().split()
            
            if len(name_parts) >= 2:
                # Check if it looks like an initial (single letter followed by period)
                last_part = name_parts[-1]
                if len(last_part) <= 2 and last_part.endswith('.'):
                    # Names like "Roshi D." - use entire display_name as first_name
                    first_name = display_name.strip()
                    last_name = "User"
                else:
                    # Normal case: "John Doe" or "John Michael Doe"
                    first_name = name_parts[0]
                    last_name = ' '.join(name_parts[1:])
            elif len(name_parts) == 1:
                # Single name: use as first_name
                first_name = name_parts[0]
                last_name = "User"
            else:
                # Empty name_parts: use username
                first_name = username.capitalize() if username else "User"
                last_name = "User"
        else:
            # No display_name: use username
            first_name = username.capitalize() if username else "User"
            last_name = "User"
        
        # Update the user
        connection.execute(
            text("UPDATE public.users SET first_name = :first_name, last_name = :last_name, temp_password = false WHERE id = :user_id"),
            {"first_name": first_name, "last_name": last_name, "user_id": user_id}
        )
    
    # Now make the columns non-nullable
    op.alter_column('users', 'first_name', nullable=False, schema='public')
    op.alter_column('users', 'last_name', nullable=False, schema='public')


def downgrade() -> None:
    # Remove the columns in reverse order
    op.drop_column('users', 'temp_password', schema='public')
    op.drop_column('users', 'last_name', schema='public') 
    op.drop_column('users', 'first_name', schema='public')

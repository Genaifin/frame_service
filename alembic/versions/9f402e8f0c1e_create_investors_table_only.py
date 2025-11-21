"""Create investors table only

Revision ID: 9f402e8f0c1e
Revises: ec9e9ff633c4
Create Date: 2025-10-06 20:29:49.184682

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '9f402e8f0c1e'
down_revision: Union[str, None] = 'ec9e9ff633c4'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Create investors table only
    op.create_table('investors',
        sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
        sa.Column('investor_name', sa.String(length=200), nullable=False),
        sa.Column('account_name', sa.String(length=200), nullable=False),
        sa.Column('account_number', sa.String(length=100), nullable=False),
        sa.Column('contact_title', sa.String(length=20), nullable=True),
        sa.Column('contact_first_name', sa.String(length=100), nullable=True),
        sa.Column('contact_last_name', sa.String(length=100), nullable=True),
        sa.Column('contact_email', sa.String(length=150), nullable=True),
        sa.Column('contact_number', sa.String(length=30), nullable=True),
        sa.Column('address_line1', sa.String(length=200), nullable=True),
        sa.Column('address_line2', sa.String(length=200), nullable=True),
        sa.Column('city', sa.String(length=100), nullable=True),
        sa.Column('state', sa.String(length=100), nullable=True),
        sa.Column('postal_code', sa.String(length=20), nullable=True),
        sa.Column('country', sa.String(length=100), nullable=True),
        sa.Column('investor_type', sa.String(length=50), nullable=True),
        sa.Column('tax_id', sa.String(length=50), nullable=True),
        sa.Column('kyc_status', sa.String(length=20), nullable=True),
        sa.Column('risk_profile', sa.String(length=20), nullable=True),
        sa.Column('is_active', sa.Boolean(), nullable=False),
        sa.Column('notes', sa.Text(), nullable=True),
        sa.Column('investor_metadata', sa.JSON(), nullable=True),
        sa.Column('created_at', sa.DateTime(), nullable=False),
        sa.Column('updated_at', sa.DateTime(), nullable=False),
        sa.PrimaryKeyConstraint('id'),
        schema='public'
    )
    
    # Create indexes
    op.create_index(op.f('ix_public_investors_account_number'), 'investors', ['account_number'], unique=True, schema='public')
    op.create_index(op.f('ix_public_investors_contact_email'), 'investors', ['contact_email'], unique=False, schema='public')
    op.create_index(op.f('ix_public_investors_investor_name'), 'investors', ['investor_name'], unique=False, schema='public')
    op.create_index(op.f('ix_public_investors_is_active'), 'investors', ['is_active'], unique=False, schema='public')


def downgrade() -> None:
    # Drop indexes first
    op.drop_index(op.f('ix_public_investors_is_active'), table_name='investors', schema='public')
    op.drop_index(op.f('ix_public_investors_investor_name'), table_name='investors', schema='public')
    op.drop_index(op.f('ix_public_investors_contact_email'), table_name='investors', schema='public')
    op.drop_index(op.f('ix_public_investors_account_number'), table_name='investors', schema='public')
    
    # Drop table
    op.drop_table('investors', schema='public')

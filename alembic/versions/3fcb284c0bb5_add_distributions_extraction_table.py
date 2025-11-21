"""add_distributions_extraction_table

Revision ID: 3fcb284c0bb5
Revises: 0340bff983b0
Create Date: 2025-10-27 16:07:35.756108

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = '3fcb284c0bb5'
down_revision: Union[str, None] = '0340bff983b0'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Create distributions table
    op.create_table('distributions',
        sa.Column('id', sa.Integer(), nullable=False, autoincrement=True),
        sa.Column('doc_id', sa.Integer(), nullable=False),
        sa.Column('schema_id', sa.Integer(), nullable=True),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=True),
        sa.Column('updated_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=True),
        
        # Core identification fields
        sa.Column('Investor', sa.Text(), nullable=True),
        sa.Column('Account', sa.Text(), nullable=True),
        sa.Column('InvestorRefID', sa.Text(), nullable=True),
        sa.Column('AccountRefID', sa.Text(), nullable=True),
        sa.Column('Security', sa.Text(), nullable=True),
        
        # Transaction details
        sa.Column('TransactionDate', sa.Date(), nullable=True),
        sa.Column('Currency', sa.String(length=10), nullable=True),
        
        # Financial fields (decimal precision for monetary values)
        sa.Column('Distribution', sa.Numeric(precision=18, scale=2), nullable=True),
        sa.Column('DeemeedCapitalCall', sa.Numeric(precision=18, scale=2), nullable=True),  # Note: keeping original typo from schema
        sa.Column('IncomeDistribution', sa.Numeric(precision=18, scale=2), nullable=True),
        sa.Column('IncomeReinvested', sa.Numeric(precision=18, scale=2), nullable=True),
        sa.Column('RecallableSell', sa.Numeric(precision=18, scale=2), nullable=True),
        sa.Column('ReturnOfCapital', sa.Numeric(precision=18, scale=2), nullable=True),
        sa.Column('DistributionOutsideCommitment', sa.Numeric(precision=18, scale=2), nullable=True),
        sa.Column('CapitalCall', sa.Numeric(precision=18, scale=2), nullable=True),
        sa.Column('CapitalCallOutsideCommitment', sa.Numeric(precision=18, scale=2), nullable=True),
        sa.Column('NetCashFlowQC', sa.Numeric(precision=18, scale=2), nullable=True),
        sa.Column('TransferIn', sa.Numeric(precision=18, scale=2), nullable=True),
        sa.Column('TransferOut', sa.Numeric(precision=18, scale=2), nullable=True),
        
        # Security transaction details
        sa.Column('Quantity', sa.Numeric(precision=18, scale=4), nullable=True),
        sa.Column('Price', sa.Numeric(precision=18, scale=2), nullable=True),
        
        # Capital commitment tracking
        sa.Column('CommittedCapital', sa.Numeric(precision=18, scale=2), nullable=True),
        sa.Column('RemainingCommittedCapital', sa.Numeric(precision=18, scale=2), nullable=True),
        sa.Column('ContributionsToDate', sa.Numeric(precision=18, scale=2), nullable=True),
        sa.Column('DistributionsToDate', sa.Numeric(precision=18, scale=2), nullable=True),
        sa.Column('ReturnOfCapitalToDate', sa.Numeric(precision=18, scale=2), nullable=True),
        
        # Confidence scores and verbatim text (JSON fields)
        sa.Column('ConfidenceScore', sa.Text(), nullable=True),
        sa.Column('VerbatimText', sa.Text(), nullable=True),
        
        # Common extraction metadata
        sa.Column('additional_columns', sa.Text(), nullable=True),
        sa.Column('document_name', sa.String(length=2000), nullable=True),
        sa.Column('extraction', sa.String(length=255), nullable=False),
        sa.Column('file_hex_hash', sa.String(length=255), nullable=True),
        sa.Column('total', sa.Integer(), nullable=True),
        sa.Column('found', sa.Integer(), nullable=True),
        sa.Column('missing', sa.Integer(), nullable=True),
        
        sa.PrimaryKeyConstraint('id'),
        sa.ForeignKeyConstraint(['doc_id'], ['documents.id'], ),
    )
    
    # Create indexes for commonly queried fields
    op.create_index('ix_distributions_doc_id', 'distributions', ['doc_id'], unique=False)
    op.create_index('ix_distributions_investor', 'distributions', ['Investor'], unique=False)
    op.create_index('ix_distributions_account', 'distributions', ['Account'], unique=False)
    op.create_index('ix_distributions_security', 'distributions', ['Security'], unique=False)
    op.create_index('ix_distributions_transaction_date', 'distributions', ['TransactionDate'], unique=False)
    op.create_index('ix_distributions_extraction', 'distributions', ['extraction'], unique=False)
    op.create_index('ix_distributions_currency', 'distributions', ['Currency'], unique=False)
    
    # Create composite indexes for common query patterns
    op.create_index('ix_distributions_investor_account', 'distributions', ['Investor', 'Account'], unique=False)
    op.create_index('ix_distributions_security_date', 'distributions', ['Security', 'TransactionDate'], unique=False)


def downgrade() -> None:
    # Drop indexes first
    op.drop_index('ix_distributions_security_date', table_name='distributions')
    op.drop_index('ix_distributions_investor_account', table_name='distributions')
    op.drop_index('ix_distributions_currency', table_name='distributions')
    op.drop_index('ix_distributions_extraction', table_name='distributions')
    op.drop_index('ix_distributions_transaction_date', table_name='distributions')
    op.drop_index('ix_distributions_security', table_name='distributions')
    op.drop_index('ix_distributions_account', table_name='distributions')
    op.drop_index('ix_distributions_investor', table_name='distributions')
    op.drop_index('ix_distributions_doc_id', table_name='distributions')
    
    # Drop the table
    op.drop_table('distributions')

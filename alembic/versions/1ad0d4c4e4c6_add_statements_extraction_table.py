"""add_statements_extraction_table

Revision ID: 1ad0d4c4e4c6
Revises: 3fcb284c0bb5
Create Date: 2025-10-27 16:11:31.223377

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = '1ad0d4c4e4c6'
down_revision: Union[str, None] = '3fcb284c0bb5'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Create statements table
    op.create_table('statements',
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
        
        # Period details
        sa.Column('PeriodBeginningDT', sa.Date(), nullable=True),
        sa.Column('PeriodEndingDT', sa.Date(), nullable=True),
        sa.Column('Currency', sa.String(length=10), nullable=True),
        
        # Capital and contribution fields (decimal precision for monetary values)
        sa.Column('NetOpeningCapital', sa.Numeric(precision=18, scale=2), nullable=True),
        sa.Column('Contributions', sa.Numeric(precision=18, scale=2), nullable=True),
        sa.Column('ContributionOutsideCommitment', sa.Numeric(precision=18, scale=2), nullable=True),
        sa.Column('Withdrawals', sa.Numeric(precision=18, scale=2), nullable=True),
        sa.Column('ReturnOfCapital', sa.Numeric(precision=18, scale=2), nullable=True),
        sa.Column('NetCapitalActivity', sa.Numeric(precision=18, scale=2), nullable=True),
        
        # Transfer fields
        sa.Column('TransfersIn', sa.Numeric(precision=18, scale=2), nullable=True),
        sa.Column('TransfersOut', sa.Numeric(precision=18, scale=2), nullable=True),
        sa.Column('NetTransfers', sa.Numeric(precision=18, scale=2), nullable=True),
        
        # Income and performance fields
        sa.Column('IncomeDistribution', sa.Numeric(precision=18, scale=2), nullable=True),
        sa.Column('RealizedGainLoss', sa.Numeric(precision=18, scale=2), nullable=True),
        sa.Column('UnrealizedGainLoss', sa.Numeric(precision=18, scale=2), nullable=True),
        sa.Column('NetGainLoss', sa.Numeric(precision=18, scale=2), nullable=True),
        sa.Column('InvestmentIncome', sa.Numeric(precision=18, scale=2), nullable=True),
        sa.Column('OtherIncomeLoss', sa.Numeric(precision=18, scale=2), nullable=True),
        
        # Fee and expense fields
        sa.Column('ManagementFee', sa.Numeric(precision=18, scale=2), nullable=True),
        sa.Column('OtherExpenses', sa.Numeric(precision=18, scale=2), nullable=True),
        sa.Column('CarriedInterest', sa.Numeric(precision=18, scale=2), nullable=True),
        sa.Column('OtherAdjustments', sa.Numeric(precision=18, scale=2), nullable=True),
        
        # Closing capital and security details
        sa.Column('NetClosingCapital', sa.Numeric(precision=18, scale=2), nullable=True),
        sa.Column('Quantity', sa.Numeric(precision=18, scale=4), nullable=True),
        sa.Column('Price', sa.Numeric(precision=18, scale=2), nullable=True),
        
        # Performance metrics
        sa.Column('MTDPerformance', sa.Numeric(precision=18, scale=2), nullable=True),
        sa.Column('QTDPerformance', sa.Numeric(precision=18, scale=2), nullable=True),
        sa.Column('YTDPerformance', sa.Numeric(precision=18, scale=2), nullable=True),
        sa.Column('IRR', sa.Numeric(precision=18, scale=2), nullable=True),
        
        # Commitment tracking
        sa.Column('Commitment', sa.Numeric(precision=18, scale=2), nullable=True),
        sa.Column('ContributionsToDate', sa.Numeric(precision=18, scale=2), nullable=True),
        sa.Column('UnfundedCommitment', sa.Numeric(precision=18, scale=2), nullable=True),
        sa.Column('UnfundedCommitmentAdj', sa.Numeric(precision=18, scale=2), nullable=True),
        sa.Column('DistributionsSubjectToRecall', sa.Numeric(precision=18, scale=2), nullable=True),
        sa.Column('TotalDistributions', sa.Numeric(precision=18, scale=2), nullable=True),
        
        # Boolean fields
        sa.Column('RecallableDistribution', sa.Boolean(), nullable=True),
        sa.Column('CommitmentOnlyStatement', sa.Boolean(), nullable=True),
        
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
    op.create_index('ix_statements_doc_id', 'statements', ['doc_id'], unique=False)
    op.create_index('ix_statements_investor', 'statements', ['Investor'], unique=False)
    op.create_index('ix_statements_account', 'statements', ['Account'], unique=False)
    op.create_index('ix_statements_security', 'statements', ['Security'], unique=False)
    op.create_index('ix_statements_period_beginning', 'statements', ['PeriodBeginningDT'], unique=False)
    op.create_index('ix_statements_period_ending', 'statements', ['PeriodEndingDT'], unique=False)
    op.create_index('ix_statements_extraction', 'statements', ['extraction'], unique=False)
    op.create_index('ix_statements_currency', 'statements', ['Currency'], unique=False)
    
    # Create composite indexes for common query patterns
    op.create_index('ix_statements_investor_account', 'statements', ['Investor', 'Account'], unique=False)
    op.create_index('ix_statements_security_period', 'statements', ['Security', 'PeriodBeginningDT'], unique=False)
    op.create_index('ix_statements_period_range', 'statements', ['PeriodBeginningDT', 'PeriodEndingDT'], unique=False)


def downgrade() -> None:
    # Drop indexes first
    op.drop_index('ix_statements_period_range', table_name='statements')
    op.drop_index('ix_statements_security_period', table_name='statements')
    op.drop_index('ix_statements_investor_account', table_name='statements')
    op.drop_index('ix_statements_currency', table_name='statements')
    op.drop_index('ix_statements_extraction', table_name='statements')
    op.drop_index('ix_statements_period_ending', table_name='statements')
    op.drop_index('ix_statements_period_beginning', table_name='statements')
    op.drop_index('ix_statements_security', table_name='statements')
    op.drop_index('ix_statements_account', table_name='statements')
    op.drop_index('ix_statements_investor', table_name='statements')
    op.drop_index('ix_statements_doc_id', table_name='statements')
    
    # Drop the table
    op.drop_table('statements')

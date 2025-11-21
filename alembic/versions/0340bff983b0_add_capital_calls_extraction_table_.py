"""add_capital_calls_extraction_table_complete

Revision ID: 0340bff983b0
Revises: 422db2e8190e
Create Date: 2025-10-27 15:25:04.244515

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = '0340bff983b0'
down_revision: Union[str, None] = '397a724c4bcb'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Try to create the table, but skip if it already exists
    conn = op.get_bind()
    from sqlalchemy import text
    from sqlalchemy.exc import ProgrammingError
    
    # Check if the table already exists
    table_exists = False
    try:
        result = conn.execute(text("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'public'
                AND table_name = 'capital_calls'
            )
        """))
        # Handle both SQLAlchemy 1.4 and 2.0 result objects
        if hasattr(result, 'scalar'):
            table_exists = bool(result.scalar())
        else:
            row = result.fetchone()
            table_exists = bool(row[0]) if row else False
    except Exception as e:
        print(f"Could not check if table exists: {e}")
        # If check fails, we'll rely on the try-except around create_table
    
    if table_exists:
        print("Table 'capital_calls' already exists. Skipping table and index creation.")
        # If table exists, indexes likely exist too - skip to avoid transaction issues
        return
    
    # Create capital_calls table - wrap in try-except as backup
    try:
        op.create_table('capital_calls',
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
        sa.Column('DeemedCapitalCall', sa.Numeric(precision=18, scale=2), nullable=True),
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
        op.create_index('ix_capital_calls_doc_id', 'capital_calls', ['doc_id'], unique=False)
        op.create_index('ix_capital_calls_investor', 'capital_calls', ['Investor'], unique=False)
        op.create_index('ix_capital_calls_account', 'capital_calls', ['Account'], unique=False)
        op.create_index('ix_capital_calls_security', 'capital_calls', ['Security'], unique=False)
        op.create_index('ix_capital_calls_transaction_date', 'capital_calls', ['TransactionDate'], unique=False)
        op.create_index('ix_capital_calls_extraction', 'capital_calls', ['extraction'], unique=False)
        op.create_index('ix_capital_calls_currency', 'capital_calls', ['Currency'], unique=False)
        
        # Create composite indexes for common query patterns
        op.create_index('ix_capital_calls_investor_account', 'capital_calls', ['Investor', 'Account'], unique=False)
        op.create_index('ix_capital_calls_security_date', 'capital_calls', ['Security', 'TransactionDate'], unique=False)
    except ProgrammingError as e:
        error_str = str(e.orig) if hasattr(e, 'orig') else str(e)
        if 'already exists' in error_str.lower() or 'duplicatetable' in error_str.lower() or 'duplicate table' in error_str.lower():
            print("Table 'capital_calls' already exists. Skipping table and index creation.")
            # If table exists, indexes likely exist too - skip to avoid transaction issues
        else:
            raise


def downgrade() -> None:
    # Drop indexes first
    op.drop_index('ix_capital_calls_security_date', table_name='capital_calls')
    op.drop_index('ix_capital_calls_investor_account', table_name='capital_calls')
    op.drop_index('ix_capital_calls_currency', table_name='capital_calls')
    op.drop_index('ix_capital_calls_extraction', table_name='capital_calls')
    op.drop_index('ix_capital_calls_transaction_date', table_name='capital_calls')
    op.drop_index('ix_capital_calls_security', table_name='capital_calls')
    op.drop_index('ix_capital_calls_account', table_name='capital_calls')
    op.drop_index('ix_capital_calls_investor', table_name='capital_calls')
    op.drop_index('ix_capital_calls_doc_id', table_name='capital_calls')
    
    # Drop the table
    op.drop_table('capital_calls')

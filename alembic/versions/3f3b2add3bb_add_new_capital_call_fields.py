"""add_new_capital_call_fields

Revision ID: 3f3b2add3bb
Revises: 0340bff983b0
Create Date: 2025-01-16 12:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = '3f3b2add3bb'
down_revision: Union[str, None] = '0340bff983b0'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Add new financial fields to capital_calls table
    op.add_column('capital_calls', sa.Column('DeemedGPContribution', sa.Numeric(precision=18, scale=2), nullable=True))
    op.add_column('capital_calls', sa.Column('Investments', sa.Numeric(precision=18, scale=2), nullable=True))
    op.add_column('capital_calls', sa.Column('ManagementFeeInsideCommitment', sa.Numeric(precision=18, scale=2), nullable=True))
    op.add_column('capital_calls', sa.Column('ManagementFeeOutsideCommitment', sa.Numeric(precision=18, scale=2), nullable=True))
    op.add_column('capital_calls', sa.Column('PartnershipExpenses', sa.Numeric(precision=18, scale=2), nullable=True))
    op.add_column('capital_calls', sa.Column('PartnershipExpensesAccountingAdminIT', sa.Numeric(precision=18, scale=2), nullable=True))
    op.add_column('capital_calls', sa.Column('PartnershipExpensesAuditTax', sa.Numeric(precision=18, scale=2), nullable=True))
    op.add_column('capital_calls', sa.Column('PartnershipExpensesBankFees', sa.Numeric(precision=18, scale=2), nullable=True))
    op.add_column('capital_calls', sa.Column('PartnershipExpensesCustodyFees', sa.Numeric(precision=18, scale=2), nullable=True))
    op.add_column('capital_calls', sa.Column('PartnershipExpensesDueDiligence', sa.Numeric(precision=18, scale=2), nullable=True))
    op.add_column('capital_calls', sa.Column('PartnershipExpensesLegal', sa.Numeric(precision=18, scale=2), nullable=True))
    op.add_column('capital_calls', sa.Column('PartnershipExpensesOrganizationCosts', sa.Numeric(precision=18, scale=2), nullable=True))
    op.add_column('capital_calls', sa.Column('PartnershipExpensesTravelEntertainment', sa.Numeric(precision=18, scale=2), nullable=True))
    op.add_column('capital_calls', sa.Column('PartnershipExpensesOther', sa.Numeric(precision=18, scale=2), nullable=True))
    op.add_column('capital_calls', sa.Column('PlacementAgentFees', sa.Numeric(precision=18, scale=2), nullable=True))
    op.add_column('capital_calls', sa.Column('SubsequentCloseInterest', sa.Numeric(precision=18, scale=2), nullable=True))
    op.add_column('capital_calls', sa.Column('WorkingCapital', sa.Numeric(precision=18, scale=2), nullable=True))


def downgrade() -> None:
    # Remove the new columns
    op.drop_column('capital_calls', 'WorkingCapital')
    op.drop_column('capital_calls', 'SubsequentCloseInterest')
    op.drop_column('capital_calls', 'PlacementAgentFees')
    op.drop_column('capital_calls', 'PartnershipExpensesOther')
    op.drop_column('capital_calls', 'PartnershipExpensesTravelEntertainment')
    op.drop_column('capital_calls', 'PartnershipExpensesOrganizationCosts')
    op.drop_column('capital_calls', 'PartnershipExpensesLegal')
    op.drop_column('capital_calls', 'PartnershipExpensesDueDiligence')
    op.drop_column('capital_calls', 'PartnershipExpensesCustodyFees')
    op.drop_column('capital_calls', 'PartnershipExpensesBankFees')
    op.drop_column('capital_calls', 'PartnershipExpensesAuditTax')
    op.drop_column('capital_calls', 'PartnershipExpensesAccountingAdminIT')
    op.drop_column('capital_calls', 'PartnershipExpenses')
    op.drop_column('capital_calls', 'ManagementFeeOutsideCommitment')
    op.drop_column('capital_calls', 'ManagementFeeInsideCommitment')
    op.drop_column('capital_calls', 'Investments')
    op.drop_column('capital_calls', 'DeemedGPContribution')


"""
KPI Validations Package
Contains separate modules for each KPI validation type
"""

# Import all validation functions for easy access
from .pricing_validations import (
    unchanged_price_securities_validation,
    major_price_changes_validation,
    major_fx_changes_validation,
    null_missing_price_validation,
)

from .position_validations import (
    major_position_changes_validation,
    large_trades_validation,
    missing_fx_mv_data_validation
)

from .trading_ie_validations import (
    major_dividends_validation,
    material_swap_financing_validation,
    material_interest_accrual_validation
)

from .market_value_validations import (
    major_mv_change_validation
)

from .expense_validations import (
    legal_fees_validation,
    admin_fees_validation,
    other_admin_expenses_validation,
    accounting_expense_validation,
    interest_expense_validation,
    management_fees_validation
)

from .fee_validations import (
    fee_validations
)

__all__ = [
    # Pricing validations
    'unchanged_price_securities_validation',
    'major_price_changes_validation',
    'major_fx_changes_validation',
    'null_missing_price_validation',
    
    # Position validations
    'major_position_changes_validation',
    'large_trades_validation',
    'missing_fx_mv_data_validation',
    
    # Trading I&E validations
    'major_dividends_validation',
    'material_swap_financing_validation',
    'material_interest_accrual_validation',
    
    # Market value validations
    'major_mv_change_validation',
    
    # Expense validations
    'legal_fees_validation',
    'admin_fees_validation',
    'other_admin_expenses_validation',
    'accounting_expense_validation',
    'interest_expense_validation',
    'management_fees_validation',
    
    # Fee validations
    'fee_validations'
]

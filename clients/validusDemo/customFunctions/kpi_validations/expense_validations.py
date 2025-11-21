"""
Dynamic Expense validation KPI modules
Implements dynamic expense validation rules based on KPI configurations from database
Handles: Legal Fees, Admin Fees, Interest Expense, Accounting Expenses, etc.
"""

from typing import List, Dict, Optional, Any
from clients.validusDemo.customFunctions.validation_utils import (
    greater_than_threshold_check, price_change_percentage_check,
    create_detailed_validation_result
)
from clients.validusDemo.customFunctions.financial_metrics import calculate_financial_metrics, get_expense_patterns
from validations import VALIDATION_STATUS


def expense_validations(trial_balance_a: List[Dict], trial_balance_b: List[Dict], 
                       expense_kpis: List[Dict], fund_id: Optional[int] = None,
                       is_dual_source: bool = False) -> List[Any]:
    """
    Main dynamic expense validation function that processes all expense KPIs
    Routes to specific validation functions based on KPI configuration and numerator_field
    
    Args:
        trial_balance_a: Trial balance data for first period
        trial_balance_b: Trial balance data for second period
        expense_kpis: List of expense category KPIs from database
        fund_id: Fund identifier for threshold lookup
        is_dual_source: True for dual source comparison, False for single source
        
    Returns:
        List of VALIDATION_STATUS objects
    """
    validations = []
    
    # For single-source, we need both datasets; for dual-source, we can work with one
    if not is_dual_source and (not trial_balance_a or not trial_balance_b):
        return validations
    elif is_dual_source and not trial_balance_a and not trial_balance_b:
        return validations  # Need at least one dataset
    
    # For dual source, add Total Expense validation first
    if is_dual_source:
        total_expense_validation = _create_total_expense_validation(
            trial_balance_a, trial_balance_b, expense_kpis, fund_id
        )
        if total_expense_validation:
            validations.append(total_expense_validation)
    
    else:
        # Process each expense KPI dynamically
        for kpi in expense_kpis:
            try:
                # Get KPI configuration
                numerator_field = kpi.get('numerator_field', '')
                precision_type = kpi.get('precision_type', 'PERCENTAGE')
                kpi_id = kpi.get('id')
                
                # Get threshold from database service
                from server.APIServerUtils.db_validation_service import db_validation_service
                threshold = db_validation_service.get_kpi_threshold(kpi_id, fund_id)
                
                # Use default threshold if none configured (especially for dual source)
                if threshold is None:
                    continue
                
                if not numerator_field:
                    continue
                # Route to specific validation based on numerator field using trial balance data with breakdown
                validation = _execute_dynamic_expense_validation(
                    trial_balance_a, trial_balance_b, kpi, threshold, is_dual_source
                )
                if validation:
                    validations.append(validation)
                    
            except Exception as e:
                # Handle individual KPI errors gracefully
                kpi_name = kpi.get('kpi_name', 'Unknown')
                error_validation = _create_error_validation(f'Error processing KPI {kpi_name}: {str(e)}', 'Expenses')
                validations.append(error_validation)
        
    return validations


def _convert_precision_type_to_int(precision_type: str) -> int:
    """
    Convert precision type string to integer for frontend consumption
    0 = PERCENTAGE, 1 = ABSOLUTE
    """
    return 0 if precision_type == 'PERCENTAGE' else 1

def _execute_dynamic_expense_validation(trial_balance_a: List[Dict], trial_balance_b: List[Dict], 
                                      kpi: Dict, threshold: float, is_dual_source: bool = False) -> Any:
    """
    Execute dynamic expense validation with proper breakdown structure using financial metrics
    Uses the existing financial metrics calculation and extracts breakdown from extra_data
    
    Args:
        trial_balance_a: Trial balance data for first period
        trial_balance_b: Trial balance data for second period  
        kpi: KPI configuration dictionary
        threshold: Validation threshold
        
    Returns:
        VALIDATION_STATUS object or None if validation cannot be performed
    """
    numerator_field = kpi.get('numerator_field', '')
    kpi_name = kpi.get('kpi_name', 'Unknown Expense')
    
    # Use existing financial metrics instead of manual filtering
    from clients.validusDemo.customFunctions.financial_metrics import calculate_financial_metrics
    
    # Calculate metrics for both periods/sources using existing financial metrics
    # Handle cases where one dataset might be empty (especially for dual-source)
    metrics_a = calculate_financial_metrics(trial_balance_a or [])
    metrics_b = calculate_financial_metrics(trial_balance_b or [])
    
    # Get the specific expense metric values
    value_a = metrics_a.get(numerator_field, 0.0)
    value_b = metrics_b.get(numerator_field, 0.0)
    
    # For dual-source, we compare values between sources; for single-source, we look at changes over time
    if is_dual_source:
        # Cross-source comparison: check if expense amounts match between sources
        difference = abs(value_a - value_b)
        comparison_type = "Cross-source discrepancy"
    else:
        # Period comparison: check if expense amounts changed significantly over time
        difference = abs(value_b - value_a)
        comparison_type = "Period-over-period change"
    
    # Extract children breakdown from extra_data by finding matching expense records
    children_data_a = {}  # period A breakdown by transaction description
    children_data_b = {}  # period B breakdown by transaction description
    
    # Get the search pattern from the financial metrics module to ensure consistency
    from clients.validusDemo.customFunctions.financial_metrics import get_expense_patterns
    expense_patterns = get_expense_patterns()
    
    search_pattern = expense_patterns.get(numerator_field, kpi_name)
    
    # Process period A trial balance (if available)
    for record in (trial_balance_a or []):
        # Handle both ORM objects and dictionaries
        if hasattr(record, 'type'):  # ORM object
            record_type = record.type
            financial_account = record.financial_account
            ending_balance = float(record.ending_balance or 0)
            extra_data = record.extra_data
        else:  # Dictionary
            record_type = record.get('Type')
            financial_account = record.get('Financial Account', '')
            ending_balance = float(record.get('Ending Balance', 0) or 0)
            extra_data = record.get('extra_data')
        
        # Check if this trial balance record is related to this expense type
        if (record_type == 'Expense' and 
            search_pattern.lower() in financial_account.lower()):
            
            # Extract breakdown from extra_data
            if extra_data:
                try:
                    import json
                    extra_data_json = json.loads(extra_data)
                    
                    # Check if there's general_ledger data
                    if 'general_ledger' in extra_data_json:
                        for transaction in extra_data_json['general_ledger']:
                            tran_desc = transaction.get('tran_description', '')
                            local_amount = float(transaction.get('local_amount', 0))
                            if tran_desc:
                                children_data_a[tran_desc] = {
                                    'local_amount': local_amount,
                                    'gl_account': financial_account
                                }
                except (json.JSONDecodeError, TypeError):
                    continue
    
    # Process period B trial balance (if available)
    for record in (trial_balance_b or []):
        # Handle both ORM objects and dictionaries
        if hasattr(record, 'type'):  # ORM object
            record_type = record.type
            financial_account = record.financial_account
            ending_balance = float(record.ending_balance or 0)
            extra_data = record.extra_data
        else:  # Dictionary
            record_type = record.get('Type')
            financial_account = record.get('Financial Account', '')
            ending_balance = float(record.get('Ending Balance', 0) or 0)
            extra_data = record.get('extra_data')
        
        # Check if this trial balance record is related to this expense type
        if (record_type == 'Expense' and 
            search_pattern.lower() in financial_account.lower()):
            
            # Extract breakdown from extra_data
            if extra_data:
                try:
                    import json
                    extra_data_json = json.loads(extra_data)
                    
                    # Check if there's general_ledger data
                    if 'general_ledger' in extra_data_json:
                        for transaction in extra_data_json['general_ledger']:
                            tran_desc = transaction.get('tran_description', '')
                            local_amount = float(transaction.get('local_amount', 0))
                            if tran_desc:
                                children_data_b[tran_desc] = {
                                    'local_amount': local_amount,
                                    'gl_account': financial_account
                                }
                except (json.JSONDecodeError, TypeError):
                    continue
    
    # Calculate change based on precision_type from KPI library
    failed_items = []
    passed_items = []
    precision_type = kpi.get('precision_type', 'PERCENTAGE')
    
    if precision_type == 'PERCENTAGE':
        if is_dual_source:
            # For dual-source: Calculate percentage difference between sources
            if value_a != 0:
                percentage_difference = abs(value_b - value_a) / value_a * 100
            elif value_b != 0:
                percentage_difference = 100  # One source has value, other doesn't
            else:
                percentage_difference = 0  # Both sources have zero value
            comparison_value = percentage_difference
        else:
            # For single-source: Calculate percentage change over time
            if value_a != 0:
                percentage_change = abs((value_b - value_a) / value_a) * 100
            else:
                # Handle case where value_a is 0
                percentage_change = 100 if value_b != 0 else 0
            comparison_value = percentage_change
        
        # Create children array in the expected format
        children = []
        all_transaction_names = set(children_data_a.keys()) | set(children_data_b.keys())
        
        for tran_desc in all_transaction_names:
            child_data_a = children_data_a.get(tran_desc, {})
            child_data_b = children_data_b.get(tran_desc, {})
            
            child_value_a = float(child_data_a.get('local_amount', 0))
            child_value_b = float(child_data_b.get('local_amount', 0))
            
            # Calculate change for this child
            if child_value_a != 0:
                child_change = abs((child_value_b - child_value_a) / child_value_a) * 100
            else:
                child_change = 100 if child_value_b != 0 else 0
            
            child_is_exception = child_change > threshold
            
            children.append({
                'transaction_description': tran_desc,  # Frontend expects this field name
                'source_a_value': child_value_a,       # Frontend expects this field name  
                'source_b_value': child_value_b,       # Frontend expects this field name
                'gl_account': child_data_a.get('gl_account') or child_data_b.get('gl_account', search_pattern),
                'type': 'expense_detail',
                'is_exception': int(child_is_exception),     # Convert boolean to int for JSON serialization
                'change': child_change                  # Include change value for frontend
            })
        
        # Calculate tooltip (opposite precision type)
        if precision_type == 'PERCENTAGE':
            tooltip_value = value_b - value_a
            if tooltip_value >= 0:
                tooltip_format = f"${tooltip_value:,.3f}"
            else:
                tooltip_format = f"-${abs(tooltip_value):,.3f}"
        else:  # ABSOLUTE
            if value_a != 0:
                tooltip_percentage = ((value_b - value_a) / abs(value_a)) * 100
                tooltip_format = f"{tooltip_percentage:.3f}%"
            else:
                tooltip_format = "100.000%" if value_b != 0 else "0.000%"
        
        # Create parent item in the exact expected format
        precision_type_int = _convert_precision_type_to_int(precision_type)
        is_parent_exception = bool(comparison_value > threshold)
        parent_item = {
            'security': kpi_name,
            'subType': kpi_name,
            'subType2': '',
            'tooltipInfo': tooltip_format,
            'precision_type': precision_type_int,
            'validation_precision_type': precision_type_int,
            'isEditable': False,
            'isRemarkOnlyEditable': False,
            'extra_data_children': children,  # Frontend expects this field name
            # Add fields that frontend uses to calculate parent row values
            'value_a': value_a,
            'value_b': value_b,
            'change': comparison_value,  # Frontend expects 'change' field
            'change_value': comparison_value,  # Keep for compatibility
            'tooltip_change': tooltip_format,  # Pre-calculated tooltip
            'is_failed': int(is_parent_exception),  # Convert boolean to int for JSON serialization
            'threshold_exceeded': int(is_parent_exception),  # Convert boolean to int for JSON serialization
            'isParentException': int(is_parent_exception)  # Convert boolean to int for JSON serialization  # New property for parent exception status
        }
        
        if comparison_value > threshold:
            failed_items.append(parent_item)
        else:
            passed_items.append(parent_item)
    else:
        # ABSOLUTE precision type
        if is_dual_source:
            # For dual-source: Calculate absolute difference between sources
            absolute_difference = abs(value_b - value_a)
            comparison_value = absolute_difference
        else:
            # For single-source: Calculate absolute change over time
            absolute_change = abs(value_b - value_a)
            comparison_value = absolute_change
        
        # Create children array in the expected format for absolute
        children = []
        all_transaction_names = set(children_data_a.keys()) | set(children_data_b.keys())
        
        for tran_desc in all_transaction_names:
            child_data_a = children_data_a.get(tran_desc, {})
            child_data_b = children_data_b.get(tran_desc, {})
            
            child_value_a = float(child_data_a.get('local_amount', 0))
            child_value_b = float(child_data_b.get('local_amount', 0))
            
            # Calculate absolute change for this child
            child_absolute_change = abs(child_value_b - child_value_a)
            child_is_exception = child_absolute_change > threshold
            
            children.append({
                'transaction_description': tran_desc,  # Frontend expects this field name
                'source_a_value': child_value_a,       # Frontend expects this field name  
                'source_b_value': child_value_b,       # Frontend expects this field name
                'gl_account': child_data_a.get('gl_account') or child_data_b.get('gl_account', search_pattern),
                'type': 'expense_detail',
                'is_exception': int(child_is_exception),     # Convert boolean to int for JSON serialization
                'change': child_absolute_change         # Include change value for frontend
            })
        
        # Calculate tooltip (opposite precision type)
        if precision_type == 'PERCENTAGE':
            tooltip_value = value_b - value_a
            if tooltip_value >= 0:
                tooltip_format = f"${tooltip_value:,.3f}"
            else:
                tooltip_format = f"-${abs(tooltip_value):,.3f}"
        else:  # ABSOLUTE
            if value_a != 0:
                tooltip_percentage = ((value_b - value_a) / abs(value_a)) * 100
                tooltip_format = f"{tooltip_percentage:.3f}%"
            else:
                tooltip_format = "100.000%" if value_b != 0 else "0.000%"
        
        # Create parent item in the exact expected format
        precision_type_int = _convert_precision_type_to_int(precision_type)
        is_parent_exception = bool(comparison_value > threshold)
        parent_item = {
            'security': kpi_name,
            'subType': kpi_name,
            'subType2': '',
            'tooltipInfo': tooltip_format,
            'precision_type': precision_type_int,
            'validation_precision_type': precision_type_int,
            'isEditable': False,
            'isRemarkOnlyEditable': False,
            'extra_data_children': children,  # Frontend expects this field name
            # Add fields that frontend uses to calculate parent row values
            'value_a': value_a,
            'value_b': value_b,
            'change': comparison_value,  # Frontend expects 'change' field
            'change_value': comparison_value,  # Keep for compatibility
            'tooltip_change': tooltip_format,  # Pre-calculated tooltip
            'is_failed': int(is_parent_exception),  # Convert boolean to int for JSON serialization
            'threshold_exceeded': int(is_parent_exception),  # Convert boolean to int for JSON serialization
            'isParentException': int(is_parent_exception)  # Convert boolean to int for JSON serialization  # New property for parent exception status
        }
        
        if comparison_value > threshold:
            failed_items.append(parent_item)
        else:
            passed_items.append(parent_item)
    
    # Create validation result with children breakdown
    validation_data = create_detailed_validation_result(
        validation_type='Non-Trading',
        subtype='Expenses',
        subtype2=kpi_name,
        failed_items=failed_items,
        passed_items=passed_items,
        threshold=threshold,
        kpi_info=kpi
    )
    
    # Create and return VALIDATION_STATUS object
    validation = (VALIDATION_STATUS()
                  .setProductName('validus')
                  .setType('Non-Trading')
                  .setSubType('Expenses')
                  .setSubType2(kpi_name)
                  .setMessage(validation_data['message'])
                  .setData(validation_data['data']))
    
    return validation


def _create_total_expense_validation(trial_balance_a: List[Dict], trial_balance_b: List[Dict], 
                                   expense_kpis: List[Dict], fund_id: Optional[int] = None) -> Any:
    """
    Create Total Expense validation for dual source comparison
    
    This function calculates the total of all expenses for each source and creates a parent
    validation item with individual expense types as children.
    
    Args:
        trial_balance_a: Trial balance data for first source
        trial_balance_b: Trial balance data for second source
        expense_kpis: List of expense category KPIs from database
        fund_id: Fund identifier for threshold lookup
        
    Returns:
        VALIDATION_STATUS object for Total Expense validation
    """
    try:
        # Calculate financial metrics for both sources
        metrics_a = calculate_financial_metrics(trial_balance_a or [])
        metrics_b = calculate_financial_metrics(trial_balance_b or [])
        
        # Get expense metrics only for active KPIs
        active_expense_metrics = []
        for kpi in expense_kpis:
            numerator_field = kpi.get('numerator_field', '')
            if numerator_field and numerator_field in [
                'legal_fees', 'admin_fees', 'other_admin_expenses', 'interest_expense', 
                'accounting_expenses', 'allocation_fee', 'audit_expense', 'bank_fees',
                'borrow_fee_estimate', 'borrow_fee_expense', 'distribution_fee_expense',
                'fs_prep_fees', 'fund_expense', 'stockloan_fees', 'tax_preparation_fees',
                'management_fees', 'performance_fees', 'non_trading_expenses'
            ]:
                active_expense_metrics.append(numerator_field)
        
        # If no active expense KPIs, use default set
        if not active_expense_metrics:
            active_expense_metrics = [
                'legal_fees', 'admin_fees', 'other_admin_expenses', 'interest_expense', 
                'accounting_expenses', 'allocation_fee', 'audit_expense', 'bank_fees',
                'borrow_fee_estimate', 'borrow_fee_expense', 'distribution_fee_expense',
                'fs_prep_fees', 'fund_expense', 'stockloan_fees', 'tax_preparation_fees',
                'management_fees', 'performance_fees', 'non_trading_expenses'
            ]
        
        # Calculate total expenses for each source
        total_expense_a = sum(metrics_a.get(metric, 0.0) for metric in active_expense_metrics)
        total_expense_b = sum(metrics_b.get(metric, 0.0) for metric in active_expense_metrics)
        
        # Get threshold for Total Expense (use a default threshold if not configured)
        from server.APIServerUtils.db_validation_service import db_validation_service
        total_expense_threshold = 0  # Default 5% threshold for total expense comparison
        
        # Try to get threshold from database if there's a Total Expense KPI
        for kpi in expense_kpis:
            if kpi.get('kpi_name', '').lower() == 'total expense':
                kpi_id = kpi.get('id')
                if kpi_id:
                    threshold = db_validation_service.get_kpi_threshold(kpi_id, fund_id)
                    if threshold is not None:
                        total_expense_threshold = threshold
                break
        
        # Calculate percentage difference between sources
        if total_expense_a != 0:
            percentage_difference = abs(total_expense_b - total_expense_a) / total_expense_a * 100
        elif total_expense_b != 0:
            percentage_difference = 100  # One source has value, other doesn't
        else:
            percentage_difference = 0  # Both sources have zero value
        
        # Create children array with individual expense types (only active KPIs)
        children = []
        expense_patterns = get_expense_patterns()
        
        for metric in active_expense_metrics:
            value_a = metrics_a.get(metric, 0.0)
            value_b = metrics_b.get(metric, 0.0)
            
            # Calculate change for this expense type
            if value_a != 0:
                child_change = abs((value_b - value_a) / value_a) * 100
            else:
                child_change = 100 if value_b != 0 else 0
            
            # Get display name for the expense type
            expense_display_name = metric.replace('_', ' ').title()
            
            children.append({
                'transaction_description': expense_display_name,
                'source_a_value': value_a,
                'source_b_value': value_b,
                'gl_account': expense_patterns.get(metric, expense_display_name),
                'type': 'expense_detail',
                'is_exception': int(child_change > total_expense_threshold),  # Convert boolean to int for JSON serialization
                'change': child_change
            })
        
        # Calculate tooltip (absolute difference)
        tooltip_value = total_expense_b - total_expense_a
        if tooltip_value >= 0:
            tooltip_format = f"${tooltip_value:,.3f}"
        else:
            tooltip_format = f"-${abs(tooltip_value):,.3f}"
        
        # Determine if Total Expense is an exception
        is_parent_exception = percentage_difference > total_expense_threshold
        
        # Create parent item
        parent_item = {
            'security': 'Total Expense',
            'subType': 'Total Expense',
            'subType2': '',
            'tooltipInfo': tooltip_format,
            'precision_type': 0,  # PERCENTAGE
            'validation_precision_type': 0,
            'isEditable': False,
            'isRemarkOnlyEditable': False,
            'extra_data_children': children,
            'value_a': total_expense_a,
            'value_b': total_expense_b,
            'change': percentage_difference,
            'change_value': percentage_difference,
            'tooltip_change': tooltip_format,
            'is_failed': int(is_parent_exception),  # Convert boolean to int for JSON serialization
            'threshold_exceeded': int(is_parent_exception),  # Convert boolean to int for JSON serialization
            'isParentException': int(is_parent_exception)  # Convert boolean to int for JSON serialization
        }
        
        # Create validation result
        failed_items = [parent_item] if is_parent_exception else []
        passed_items = [parent_item] if not is_parent_exception else []
        
        validation_data = create_detailed_validation_result(
            validation_type='Non-Trading',
            subtype='Expenses',
            subtype2='Total Expense',
            failed_items=failed_items,
            passed_items=passed_items,
            threshold=total_expense_threshold,
            kpi_info={'kpi_name': 'Total Expense', 'precision_type': 'PERCENTAGE'}
        )
        
        # Create and return VALIDATION_STATUS object
        validation = (VALIDATION_STATUS()
                      .setProductName('validus')
                      .setType('Non-Trading')
                      .setSubType('Expenses')
                      .setSubType2('Total Expense')
                      .setMessage(validation_data['message'])
                      .setData(validation_data['data']))
        
        return validation
        
    except Exception as e:
        # Return error validation if something goes wrong
        return _create_error_validation(f'Error creating Total Expense validation: {str(e)}', 'Expenses')


def _create_error_validation(message: str, category: str) -> Any:
    """
    Create error validation status object
    
    Args:
        message: Error message
        category: Validation category
        
    Returns:
        VALIDATION_STATUS object
    """
    validation = (VALIDATION_STATUS()
                  .setProductName('validus')
                  .setType('System')
                  .setSubType('Error')
                  .setSubType2(category)
                  .setMessage(message)
                  .setData({}))
    
    return validation



def expenses_validation(trial_balance_a, trial_balance_b, expense_type, threshold, kpi_info=None):
    """
    Generic expenses validation
    
    Rule: Filter the Trial Balance for the type 'expense'. For all the expenses, other than 
    management fees, check the column 'Ending Balance'. If the diff between the 2 periods 
    exceeds the threshold set, then flag it as an exception. Show this exception for each 
    expense other than management fees for which threshold is set on NAV validation 
    configurable UI.
    
    Args:
        trial_balance_a: Trial balance data for first period
        trial_balance_b: Trial balance data for second period
        expense_type: Type of expense to validate (e.g., 'Legal Fees', 'Admin Fees')
        threshold: Percentage threshold for expense changes
        kpi_info: KPI configuration information
    
    Returns:
        VALIDATION_STATUS: Validation result object
    """
    # Filter for expense type entries in both periods
    filtered_tb_a = [
        item for item in trial_balance_a
        if item.get('Type', '').lower() == 'expense' and
           expense_type.lower() in item.get('Financial Account', '').lower()
    ]
    
    filtered_tb_b = [
        item for item in trial_balance_b
        if item.get('Type', '').lower() == 'expense' and
           expense_type.lower() in item.get('Financial Account', '').lower()
    ]
    
    # Check for changes exceeding threshold
    failed_items, passed_items, total_items = price_change_percentage_check(
        filtered_tb_a, filtered_tb_b, 'Ending Balance', threshold / 100.0, 'Financial Account'
    )
    
    # Create validation result
    validation_data = create_detailed_validation_result(
        validation_type='NON-TRADING',
        subtype='Expenses',
        subtype2=expense_type,
        failed_items=failed_items,
        passed_items=passed_items,
        threshold=threshold,
        kpi_info=kpi_info
    )
    
    # Create and return VALIDATION_STATUS object
    validation = (VALIDATION_STATUS()
                  .setProductName('validus')
                  .setType('Non-Trading')
                  .setSubType('Expenses')
                  .setSubType2(expense_type)
                  .setMessage(validation_data['message'])
                  .setData(validation_data['data']))
    
    return validation


def legal_fees_validation(trial_balance_a, trial_balance_b, threshold, kpi_info=None):
    """
    Legal Fees validation
    
    Rule: Check Legal Fees expense changes between periods
    
    Args:
        trial_balance_a: Trial balance data for first period
        trial_balance_b: Trial balance data for second period
        threshold: Percentage threshold for legal fees changes
        kpi_info: KPI configuration information
    
    Returns:
        VALIDATION_STATUS: Validation result object
    """
    return expenses_validation(
        trial_balance_a, trial_balance_b, 'Legal Expense', threshold, kpi_info
    )


def admin_fees_validation(trial_balance_a, trial_balance_b, threshold, kpi_info=None):
    """
    Admin Fees validation
    
    Rule: Check Admin Fees expense changes between periods
    
    Args:
        trial_balance_a: Trial balance data for first period
        trial_balance_b: Trial balance data for second period
        threshold: Percentage threshold for admin fees changes
        kpi_info: KPI configuration information
    
    Returns:
        VALIDATION_STATUS: Validation result object
    """
    return expenses_validation(
        trial_balance_a, trial_balance_b, 'Admin Fees', threshold, kpi_info
    )


def other_admin_expenses_validation(trial_balance_a, trial_balance_b, threshold, kpi_info=None):
    """
    Other Admin Expenses validation
    
    Rule: Check Other Admin Expenses changes between periods
    
    Args:
        trial_balance_a: Trial balance data for first period
        trial_balance_b: Trial balance data for second period
        threshold: Percentage threshold for other admin expenses changes
        kpi_info: KPI configuration information
    
    Returns:
        VALIDATION_STATUS: Validation result object
    """
    return expenses_validation(
        trial_balance_a, trial_balance_b, 'Other Admin Expenses', threshold, kpi_info
    )


def accounting_expense_validation(trial_balance_a, trial_balance_b, threshold, kpi_info=None):
    """
    Accounting Expense validation
    
    Rule: Check Accounting Expense changes between periods
    
    Args:
        trial_balance_a: Trial balance data for first period
        trial_balance_b: Trial balance data for second period
        threshold: Percentage threshold for accounting expense changes
        kpi_info: KPI configuration information
    
    Returns:
        VALIDATION_STATUS: Validation result object
    """
    return expenses_validation(
        trial_balance_a, trial_balance_b, 'Accounting Expense', threshold, kpi_info
    )


def interest_expense_validation(trial_balance_a, trial_balance_b, threshold, kpi_info=None):
    """
    Interest Expense validation
    
    Rule: Check Interest Expense changes between periods
    
    Args:
        trial_balance_a: Trial balance data for first period
        trial_balance_b: Trial balance data for second period
        threshold: Percentage threshold for interest expense changes
        kpi_info: KPI configuration information
    
    Returns:
        VALIDATION_STATUS: Validation result object
    """
    return expenses_validation(
        trial_balance_a, trial_balance_b, 'Interest Expense', threshold, kpi_info
    )


def management_fees_validation(trial_balance_a, trial_balance_b, threshold, kpi_info=None):
    """
    Management Fees validation
    
    Rule: Filter the Trial Balance for the type 'expense'. For the expense management fees, check 
    the column 'Ending Balance'. If the diff between the 2 sources exceeds the 
    threshold set, then flag it as an exception.
    
    Args:
        trial_balance_a: Trial balance data for first period
        trial_balance_b: Trial balance data for second period
        threshold: Percentage threshold for management fees changes
        kpi_info: KPI configuration information
    
    Returns:
        VALIDATION_STATUS: Validation result object
    """
    # Create validation result using the generic expenses validation
    validation = expenses_validation(
        trial_balance_a, trial_balance_b, 'Management Fees', threshold, kpi_info
    )
    
    # Update the subtype to 'Fees' for management fees
    validation.setSubType('Fees')
    
    return validation

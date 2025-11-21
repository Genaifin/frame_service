"""
Dynamic Fee validation KPI modules
Implements dynamic fee validation rules based on KPI configurations from database
Handles: Management Fees, Performance Fees, Administration Fees, etc.
"""

from typing import List, Dict, Optional, Any
from clients.validusDemo.customFunctions.validation_utils import (
    greater_than_threshold_check, price_change_percentage_check,
    create_detailed_validation_result
)
from clients.validusDemo.customFunctions.financial_metrics import calculate_financial_metrics
from validations import VALIDATION_STATUS


def fee_validations(trial_balance_a: List[Dict], trial_balance_b: List[Dict], 
                   fee_kpis: List[Dict], fund_id: Optional[int] = None,
                   is_dual_source: bool = False) -> List[Any]:
    """
    Main dynamic fee validation function that processes all fee KPIs
    Routes to specific validation functions based on KPI configuration and numerator_field
    
    Args:
        trial_balance_a: Trial balance data for first period
        trial_balance_b: Trial balance data for second period
        fee_kpis: List of fee category KPIs from database
        fund_id: Fund identifier for threshold lookup
        is_dual_source: True for cross-source comparison, False for period comparison
        
    Returns:
        List of VALIDATION_STATUS objects
    """
    validations = []
    
    # For single-source, we need both datasets; for dual-source, we can work with one
    if not is_dual_source and (not trial_balance_a or not trial_balance_b):
        return validations
    elif is_dual_source and not trial_balance_a and not trial_balance_b:
        return validations  # Need at least one dataset
    
    
    # Process each fee KPI dynamically
    for kpi in fee_kpis:
        try:
            # Get KPI configuration
            numerator_field = kpi.get('numerator_field', '')
            precision_type = kpi.get('precision_type', 'PERCENTAGE')
            kpi_id = kpi.get('id')
            
            # Get threshold from database service
            from server.APIServerUtils.db_validation_service import db_validation_service
            threshold = db_validation_service.get_kpi_threshold(kpi_id, fund_id)
            
            if threshold is None or not numerator_field:
                continue
                
            # Route to specific validation based on numerator field using trial balance data with breakdown
            validation = _execute_dynamic_fee_validation(
                trial_balance_a, trial_balance_b, kpi, threshold, is_dual_source
            )
            if validation:
                validations.append(validation)
                
        except Exception as e:
            # Handle individual KPI errors gracefully
            kpi_name = kpi.get('kpi_name', 'Unknown')
            error_validation = _create_error_validation(f'Error processing KPI {kpi_name}: {str(e)}', 'Fees')
            validations.append(error_validation)
    
    return validations


def _execute_dynamic_fee_validation(trial_balance_a: List[Dict], trial_balance_b: List[Dict], 
                                  kpi: Dict, threshold: float, is_dual_source: bool = False) -> Any:
    """
    Execute dynamic fee validation with proper breakdown structure using validation utilities
    Uses the existing validation utilities for consistent dual source handling
    
    Args:
        trial_balance_a: Trial balance data for first period
        trial_balance_b: Trial balance data for second period  
        kpi: KPI configuration dictionary
        threshold: Validation threshold
        is_dual_source: True for cross-source comparison, False for period comparison
        
    Returns:
        VALIDATION_STATUS object or None if validation cannot be performed
    """
    numerator_field = kpi.get('numerator_field', '')
    kpi_name = kpi.get('kpi_name', 'Unknown Fee')
    
    # Get the search pattern from the financial metrics module to ensure consistency
    from clients.validusDemo.customFunctions.financial_metrics import get_fee_patterns
    fee_patterns = get_fee_patterns()
    search_pattern = fee_patterns.get(numerator_field, kpi_name)
    
    # Filter trial balance data for this specific fee type
    filtered_tb_a = []
    filtered_tb_b = []

    # Process trial balance A
    for record in (trial_balance_a or []):
        # Handle both ORM objects and dictionaries
        if hasattr(record, 'type'):  # ORM object
            record_type = record.type
            financial_account = record.financial_account
            ending_balance = float(record.ending_balance or 0)
        else:  # Dictionary
            record_type = record.get('Type')
            financial_account = record.get('Financial Account', '')
            ending_balance = float(record.get('Ending Balance', 0) or 0)
        
        # Check if this trial balance record is related to this fee type
        if (record_type == 'Expense' and 
            search_pattern.lower() in financial_account.lower()):
            
            # Convert to dictionary format for validation utilities
            if hasattr(record, 'type'):  # ORM object
                filtered_tb_a.append({
                    'Type': record.type,
                    'Financial Account': record.financial_account,
                    'Ending Balance': record.ending_balance,
                    'extra_data': record.extra_data
                })
            else:  # Already a dictionary
                filtered_tb_a.append(record)
    
    # Process trial balance B
    for record in (trial_balance_b or []):
        # Handle both ORM objects and dictionaries
        if hasattr(record, 'type'):  # ORM object
            record_type = record.type
            financial_account = record.financial_account
            ending_balance = float(record.ending_balance or 0)
        else:  # Dictionary
            record_type = record.get('Type')
            financial_account = record.get('Financial Account', '')
            ending_balance = float(record.get('Ending Balance', 0) or 0)
        
        # Check if this trial balance record is related to this fee type
        if (record_type == 'Expense' and 
            search_pattern.lower() in financial_account.lower()):
            
            # Convert to dictionary format for validation utilities
            if hasattr(record, 'type'):  # ORM object
                filtered_tb_b.append({
                    'Type': record.type,
                    'Financial Account': record.financial_account,
                    'Ending Balance': record.ending_balance,
                    'extra_data': record.extra_data
                })
            else:  # Already a dictionary
                filtered_tb_b.append(record)

    # Use validation utilities for consistent dual source handling
    precision_type = kpi.get('precision_type', 'PERCENTAGE')
    
    if precision_type == 'PERCENTAGE':
        # Use percentage-based validation utility
        failed_items, passed_items, total_items = price_change_percentage_check(
            filtered_tb_a, filtered_tb_b, 'Ending Balance', threshold / 100.0, 'Financial Account'
        )
    else:
        # For ABSOLUTE precision type, we need to implement absolute difference check
        # For now, use percentage check but this could be enhanced with a dedicated absolute check utility
        failed_items, passed_items, total_items = price_change_percentage_check(
            filtered_tb_a, filtered_tb_b, 'Ending Balance', threshold / 100.0, 'Financial Account'
        )
    
    # Create validation result
    validation_data = create_detailed_validation_result(
        validation_type='Non-Trading',
        subtype='Fees',
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
                  .setSubType('Fees')
                  .setSubType2(kpi_name)
                  .setMessage(validation_data['message'])
                  .setData(validation_data['data']))
    
    return validation


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
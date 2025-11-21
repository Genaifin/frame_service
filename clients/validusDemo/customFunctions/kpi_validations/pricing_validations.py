"""
Dynamic Pricing validation KPI modules
Implements dynamic pricing validation rules based on KPI configurations from database
Handles: Major Price Change, Major FX Change, Default Pricing Validations (Unchanged Prices, Missing Prices)
"""

from typing import List, Dict, Optional, Any
from clients.validusDemo.customFunctions.validation_utils import (
    null_missing_check, unchanged_value_check, price_change_percentage_check, price_change_percentage_check_with_composite_id,
    missing_price_zero_mv_check, missing_price_null_check, create_detailed_validation_result, create_default_validation_result
)
from clients.validusDemo.customFunctions.financial_metrics import get_metric_value
from validations import VALIDATION_STATUS


def pricing_validations(portfolio_a: List[Dict], portfolio_b: List[Dict], 
                       pricing_kpis: List[Dict], fund_id: Optional[int] = None,
                       is_dual_source: bool = False) -> List[Any]:
    """
    Main dynamic pricing validation function that processes all pricing KPIs
    Routes to specific validation functions based on KPI configuration
    
    Args:
        portfolio_a: Portfolio data for first source/period  
        portfolio_b: Portfolio data for second source/period
        pricing_kpis: List of pricing category KPIs from database
        fund_id: Fund identifier for threshold lookup
        is_dual_source: True for cross-source comparison, False for period comparison
        
    Returns:
        List of VALIDATION_STATUS objects
    """
    validations = []
    
    if not portfolio_a or not portfolio_b:
        return validations
    
    
    # Process each pricing KPI dynamically
    for kpi in pricing_kpis:
        try:
            # Get KPI configuration
            kpi_code = kpi.get('kpi_code', '').lower()
            numerator_field = kpi.get('numerator_field', '')
            precision_type = kpi.get('precision_type', 'PERCENTAGE')
            threshold = kpi.get('threshold')
            
            if threshold is None:
                continue
                
            # Route to specific validation based on numerator field only
            if numerator_field == 'current_price':
                # Price change/discrepancy validation - filter out CASH and CASHF
                validation = _execute_dynamic_price_validation(
                    portfolio_a, portfolio_b, kpi, threshold, 
                    filter_func=lambda item: item.get('Inv Type', '').upper() not in ['CASH', 'CASHF'],
                    is_dual_source=is_dual_source
                )
                validations.append(validation)
                
            elif numerator_field == 'fx_rate':
                # FX rate change/discrepancy validation - only CASHF investment types
                validation = _execute_dynamic_price_validation(
                    portfolio_a, portfolio_b, kpi, threshold,
                    filter_func=lambda item: item.get('Inv Type', '').upper() == 'CASHF',
                    is_dual_source=is_dual_source
                )
                validations.append(validation)
                
        except Exception as e:
            # Handle individual KPI errors gracefully
            error_validation = _create_error_validation(f'Error processing KPI {kpi_code}: {str(e)}', 'Pricing')
            validations.append(error_validation)
    
    # Add default pricing validations (unchanged prices, missing prices)
    default_validations = _execute_default_pricing_validations(portfolio_a, portfolio_b, is_dual_source)
    validations.extend(default_validations)
    
    return validations


def _execute_dynamic_price_validation(portfolio_a: List[Dict], portfolio_b: List[Dict], 
                                    kpi: Dict, threshold: float, 
                                    filter_func=None, is_dual_source: bool = False) -> Any:
    """
    Execute dynamic price validation based on KPI configuration
    
    Args:
        portfolio_a: Portfolio data for first period
        portfolio_b: Portfolio data for second period  
        kpi: KPI configuration dictionary
        threshold: Validation threshold
        filter_func: Optional function to filter portfolio data
        
    Returns:
        VALIDATION_STATUS object
    """
    # Apply filtering if provided
    if filter_func:
        filtered_portfolio_a = [item for item in portfolio_a if filter_func(item)]
        filtered_portfolio_b = [item for item in portfolio_b if filter_func(item)]
    else:
        filtered_portfolio_a = portfolio_a
        filtered_portfolio_b = portfolio_b
    
    # Determine price field based on numerator_field
    price_field = 'End Local Market Price'  # Default field for current_price
    
    # Convert threshold to decimal if needed (5% = 0.05)
    
    # Execute validation based on precision type
    precision_type = kpi.get('precision_type', 'PERCENTAGE')
    
    if precision_type == 'PERCENTAGE':
        # Use percentage-based validation
        failed_items, passed_items, total_items = price_change_percentage_check_with_composite_id(
            filtered_portfolio_a, filtered_portfolio_b, price_field, threshold
        )
    else:
        # For ABSOLUTE precision type, use absolute difference check
        # This would need a new validation utility function for absolute checks
        failed_items, passed_items, total_items = price_change_percentage_check_with_composite_id(
            filtered_portfolio_a, filtered_portfolio_b, price_field, threshold
        )
    
    # Add special properties for Tesla security in major pricing changes
    for item in failed_items + passed_items:
        security_name = item.get('description', '').lower()
        if 'tesla inc' in security_name:
            item['isCorpAction'] = True
            item['corpActionInfo'] = "A Corporate action has been recorded due to reverse stock split 2:1 on 16th Feb 2024"
    
    # Get validation hierarchy from KPI
    level1, level2, subtype2 = _get_validation_hierarchy_from_kpi(kpi)
    
    # Create validation result
    validation_data = create_detailed_validation_result(
        validation_type=level1,
        subtype=level2,
        subtype2=subtype2,
        failed_items=failed_items,
        passed_items=passed_items,
        threshold=threshold,
        kpi_info=kpi
    )
    
    # Create and return VALIDATION_STATUS object
    validation = (VALIDATION_STATUS()
                  .setProductName('validus')
                  .setType(level1)
                  .setSubType(level2)
                  .setSubType2(subtype2)
                  .setMessage(validation_data['message'])
                  .setData(validation_data['data']))
    
    return validation


def _execute_default_pricing_validations(portfolio_a: List[Dict], portfolio_b: List[Dict], 
                                        is_dual_source: bool = False) -> List[Any]:
    """
    Execute default pricing validations (unchanged prices, missing prices)
    These are executed regardless of KPI configuration
    
    Args:
        portfolio_a: Portfolio data for first source/period
        portfolio_b: Portfolio data for second source/period
        is_dual_source: True for cross-source comparison, False for period comparison
        
    Returns:
        List of VALIDATION_STATUS objects for default validations
    """
    validations = []
    
    # 1. Unchanged price validation - SKIP for dual-source mode
    if not is_dual_source:
        # Only run unchanged price validation for period-over-period comparison
        try:
            unchanged_validation = unchanged_price_securities_validation(portfolio_a, portfolio_b)
            validations.append(unchanged_validation)
        except Exception as e:
            error_validation = _create_error_validation(f'Error in unchanged price validation: {str(e)}', 'Pricing')
            validations.append(error_validation)
    else:
        pass
    # 2. Missing price validation (applies to both single-source and dual-source)
    try:
        missing_validation = null_missing_price_validation(portfolio_b, portfolio_a)
        validations.append(missing_validation)
    except Exception as e:
        error_validation = _create_error_validation(f'Error in missing price validation: {str(e)}', 'Pricing')
        validations.append(error_validation)
    
    return validations


def _get_validation_hierarchy_from_kpi(kpi: Dict) -> tuple:
    """
    Get validation hierarchy from KPI configuration using KPI name
    
    Args:
        kpi: KPI configuration dictionary
        
    Returns:
        Tuple of (level1, level2, subtype2)
    """
    # Use KPI name directly from database - no hardcoded keywords
    kpi_name = kpi.get('kpi_name', 'Unknown Price Validation')
    
    return 'PnL', 'Pricing', kpi_name


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


def unchanged_price_securities_validation(portfolio_a, portfolio_b, kpi_info=None):
    """
    Unchanged price Securities validation
    
    Rule: From the input files "Portfolio Valuation By Instrum" report compare the column "End 
    Local Market Price" for each security. If the difference in the price is equal to zero, then 
    flag the exception. This is because the threshold is set as equal to 0%
    Excludes CASH and CASHF investment types from validation.
    
    Args:
        portfolio_a: Portfolio data for first period
        portfolio_b: Portfolio data for second period
        kpi_info: KPI configuration information
    
    Returns:
        VALIDATION_STATUS: Validation result object
    """
    # Filter out CASH & CASHF investment types
    filtered_portfolio_a = [
        item for item in portfolio_a 
        if item.get('Inv Type', '').upper() not in ['CASH', 'CASHF']
    ]
    filtered_portfolio_b = [
        item for item in portfolio_b 
        if item.get('Inv Type', '').upper() not in ['CASH', 'CASHF']
    ]
    
    # Check for unchanged prices between two periods
    failed_items, passed_items, total_items = unchanged_value_check(
        filtered_portfolio_a, filtered_portfolio_b, 'End Local Market Price'
    )
    
    # Create validation result
    validation_data = create_detailed_validation_result(
        validation_type='PnL',
        subtype='Pricing',
        subtype2='Unchanged Price',
        failed_items=failed_items,
        passed_items=passed_items,
        threshold=0,  # Display "-" instead of 0% for unchanged prices
        kpi_info=kpi_info
    )

    # Create and return VALIDATION_STATUS object
    validation = (VALIDATION_STATUS()
                  .setProductName('validus')
                  .setType('PnL')
                  .setSubType('Pricing')
                  .setSubType2('Unchanged Price')
                  .setMessage(validation_data['message'])
                  .setData(validation_data['data']))
    
    return validation


def major_price_changes_validation(portfolio_a, portfolio_b, threshold, kpi_info=None):
    """
    Major price changes validation
    
    Rule: From Portfolio Valuation table compare the column "End Local Market Price" 
    for the 'Inv Type' other than CASH & CASHF between 2 different periods. 
    If the difference in the price exceeds the threshold set, then flag it as an exception.
    
    Args:
        portfolio_a: Portfolio data for first period
        portfolio_b: Portfolio data for second period
        threshold: Percentage threshold for major price changes
        kpi_info: KPI configuration information
    
    Returns:
        VALIDATION_STATUS: Validation result object
    """
    # Filter out CASH & CASHF investment types
    filtered_portfolio_a = [
        item for item in portfolio_a 
        if item.get('Inv Type', '').upper() not in ['CASH', 'CASHF']
    ]
    filtered_portfolio_b = [
        item for item in portfolio_b 
        if item.get('Inv Type', '').upper() not in ['CASH', 'CASHF']
    ]
       
    
    # Check for major price changes using composite identifier matching
    failed_items, passed_items, total_items = price_change_percentage_check_with_composite_id(
        filtered_portfolio_a, filtered_portfolio_b, 'End Local Market Price', threshold
    )
    
    # Add special properties for Tesla security in major pricing changes
    for item in failed_items + passed_items:
        security_name = item.get('description', '').lower()
        if 'tesla' in security_name:
            item['isCorpAction'] = True
            item['corpActionInfo'] = "A Corporate action has been recorded due to reverse stock split 2:1 on 16th Feb 2024"
    
    # Create validation result
    validation_data = create_detailed_validation_result(
        validation_type='PnL',
        subtype='Pricing',
        subtype2='Major Price Change',
        failed_items=failed_items,
        passed_items=passed_items,
        threshold=threshold,
        kpi_info=kpi_info
    )
    
    # Create and return VALIDATION_STATUS object
    validation = (VALIDATION_STATUS()
                  .setProductName('validus')
                  .setType('PnL')
                  .setSubType('Pricing')
                  .setSubType2('Major Price Change')
                  .setMessage(validation_data['message'])
                  .setData(validation_data['data']))
    
    return validation


def major_fx_changes_validation(portfolio_a, portfolio_b, threshold, kpi_info=None):
    """
    Major FX changes validation
    
    Rule: From Portfolio Valuation table compare the column "End Local Market Price" 
    for the 'Inv Type' CASHF between 2 different periods. 
    If the difference in the price exceeds the threshold set, then flag it as an exception.
    
    Args:
        portfolio_a: Portfolio data for first period
        portfolio_b: Portfolio data for second period
        threshold: Percentage threshold for major FX changes
        kpi_info: KPI configuration information
    
    Returns:
        VALIDATION_STATUS: Validation result object
    """
    # Filter to only CASHF investment types
    filtered_portfolio_a = [
        item for item in portfolio_a 
        if item.get('Inv Type', '').upper() == 'CASHF'
    ]
    filtered_portfolio_b = [
        item for item in portfolio_b 
        if item.get('Inv Type', '').upper() == 'CASHF'
    ]
    
    # Ensure threshold is in decimal format for comparison (5% = 0.05)
    # If threshold > 1, assume it's in percentage format and convert to decimal    
    
    # Check for major FX changes using composite identifier matching
    failed_items, passed_items, total_items = price_change_percentage_check_with_composite_id(
        filtered_portfolio_a, filtered_portfolio_b, 'End Local Market Price', threshold
    )
    
    # Create validation result
    validation_data = create_detailed_validation_result(
        validation_type='PnL',
        subtype='Pricing',
        subtype2='Major FX Change',
        failed_items=failed_items,
        passed_items=passed_items,
        threshold=threshold,
        kpi_info=kpi_info
    )
    
    # Create and return VALIDATION_STATUS object
    validation = (VALIDATION_STATUS()
                  .setProductName('validus')
                  .setType('PnL')
                  .setSubType('Pricing')
                  .setSubType2('Major FX Change')
                  .setMessage(validation_data['message'])
                  .setData(validation_data['data']))
    
    return validation


def null_missing_price_validation(portfolio_b, portfolio_a=None, kpi_info=None):
    """
    Missing Price validation based on new business rule
    
    Rule: Check the column "end_local_market_price". If it is NULL/None for any security 
    and the "end_qty" is not equal to zero, then flag the exception.
    Zero values are no longer considered exceptions - only NULL values.
    This needs to be checked for ending period only. For start period, system doesn't need 
    to check it because it will be already checked in the prior month.
    Excludes CASH and CASHF investment types from validation.
    
    Args:
        portfolio_b: Portfolio data to check (ending period only)
        portfolio_a: Portfolio data from previous period (for source A values)
        kpi_info: KPI configuration information (optional for default validations)
    
    Returns:
        VALIDATION_STATUS: Validation result object
    """
    # Handle case when no portfolio data is available - consider validation as passed
    if not portfolio_b or len(portfolio_b) == 0:
        # Create validation result for no data case - show as passed
        validation_data = create_default_validation_result(
            validation_type='PnL',
            subtype='Pricing',
            subtype2='Missing Price',
            failed_items=[],
            passed_items=[]
        )
        
        # Override message to 0 (passed) when no data is available
        validation_data['message'] = 0
        
        # Create and return VALIDATION_STATUS object
        validation = (VALIDATION_STATUS()
                      .setProductName('validus')
                      .setType('PnL')
                      .setSubType('Pricing')
                      .setSubType2('Missing Price')
                      .setMessage(validation_data['message'])
                      .setData(validation_data['data']))
        
        return validation
    
    # Filter out CASH & CASHF investment types
    filtered_portfolio_b = [
        item for item in portfolio_b 
        if item.get('Inv Type', '').upper() not in ['CASH', 'CASHF']
    ]
    filtered_portfolio_a = [
        item for item in portfolio_a 
        if item.get('Inv Type', '').upper() not in ['CASH', 'CASHF']
    ] if portfolio_a else None
    
    # Check for missing prices using the new NULL logic with source A data
    failed_items, passed_items, total_items = missing_price_null_check(
        filtered_portfolio_b, 'Inv Id', filtered_portfolio_a
    )
    
    # Create validation result
    validation_data = create_default_validation_result(
        validation_type='PnL',
        subtype='Pricing',
        subtype2='Missing Price',
        failed_items=failed_items,
        passed_items=passed_items
    )
    
    # Create and return VALIDATION_STATUS object
    validation = (VALIDATION_STATUS()
                  .setProductName('validus')
                  .setType('PnL')
                  .setSubType('Pricing')
                  .setSubType2('Missing Price')
                  .setMessage(validation_data['message'])
                  .setData(validation_data['data']))
    
    return validation
"""
Dynamic Trading I&E validation KPI modules
Implements dynamic trading income & expense validation rules based on KPI configurations from database
Handles: Major Dividends, Material Swap Financing, Material Interest Accruals
"""

from typing import List, Dict, Optional, Any
from clients.validusDemo.customFunctions.validation_utils import (
    greater_than_threshold_check, null_missing_check,
    create_detailed_validation_result, create_default_validation_result
)
from clients.validusDemo.customFunctions.financial_metrics import get_metric_value
from validations import VALIDATION_STATUS


def trading_ie_validations(trial_balance_a: List[Dict], trial_balance_b: List[Dict], 
                          trading_kpis: List[Dict], fund_id: Optional[int] = None,
                          is_dual_source: bool = False, dividend_a: List[Dict] = None, dividend_b: List[Dict] = None) -> List[Any]:
    """
    Main dynamic trading I&E validation function that processes all trading I&E KPIs
    Routes to specific validation functions based on KPI configuration
    
    Args:
        dividend_a: Dividend data for first period
        dividend_b: Dividend data for second period
        trading_kpis: List of trading I&E category KPIs from database
        fund_id: Fund identifier for threshold lookup
        
    Returns:
        List of VALIDATION_STATUS objects
    """
    validations = []
    
    # For dividend validations, we need at least one dataset
    # For swap financing and interest accruals, we use trial balance data
    # So we only skip if we have no data at all
    if not dividend_a and not dividend_b and not trial_balance_a and not trial_balance_b:
        return validations
    
    
    # Process each trading I&E KPI dynamically
    for kpi in trading_kpis:
        try:
            # Get KPI configuration
            kpi_code = kpi.get('kpi_code', '').lower()
            numerator_field = kpi.get('numerator_field', '')
            precision_type = kpi.get('precision_type', 'PERCENTAGE')
            
            # Get threshold from database service (like other validation functions)
            from server.APIServerUtils.db_validation_service import db_validation_service
            threshold = db_validation_service.get_kpi_threshold(kpi['id'], fund_id)
            
            if threshold is None:
                continue
                
            # Route to specific validation based on numerator field only
            if numerator_field == 'dividend_amount':
                # Dividend amount validation - only process if dividend data is available
                if dividend_a or dividend_b:
                    validation = _execute_dynamic_dividend_validation(
                        dividend_a, dividend_b, kpi, threshold, is_dual_source
                    )
                    validations.append(validation)
                
            elif numerator_field == 'swap_financing':
                # Swap financing validation - uses trial balance data
                validation = _execute_dynamic_trading_ie_validation(
                    trial_balance_a, trial_balance_b, kpi, threshold, kpi.get('kpi_name', 'Material Swap Financing'), is_dual_source
                )
                validations.append(validation)
                
            elif numerator_field == 'interest_accruals':
                # Interest accruals validation - uses trial balance data
                validation = _execute_dynamic_trading_ie_validation(
                    trial_balance_a, trial_balance_b, kpi, threshold, kpi.get('kpi_name', 'Material Interest Accruals'), is_dual_source
                )
                validations.append(validation)
                
        except Exception as e:
            # Handle individual KPI errors gracefully
            error_validation = _create_error_validation(f'Error processing KPI {kpi_code}: {str(e)}', 'Trading I&E')
            validations.append(error_validation)
    
    return validations


def _execute_dynamic_dividend_validation(dividend_a: List[Dict], dividend_b: List[Dict], 
                                       kpi: Dict, threshold: float, is_dual_source: bool = False) -> Any:
    """
    Execute dynamic dividend validation based on KPI configuration
    
    For DUAL_SOURCE: Compare security-level dividends
    For SINGLE_SOURCE: Compare total dividend values only
    
    Args:
        dividend_a: Dividend data for first source/period
        dividend_b: Dividend data for second source/period  
        kpi: KPI configuration dictionary
        threshold: Validation threshold
        is_dual_source: True for cross-source comparison, False for period comparison
        
    Returns:
        VALIDATION_STATUS object
    """
    
    if is_dual_source:
        # For dual source: Compare by individual securities
        validation_data = _compare_dividends_by_security(dividend_a, dividend_b, kpi, threshold)
    else:
        # For single source: Compare by total values only
        validation_data = _compare_dividends_total(dividend_a, dividend_b, kpi, threshold)
    
    # Create and return VALIDATION_STATUS object
    validation = (VALIDATION_STATUS()
                  .setProductName('validus')
                  .setType('PnL')
                  .setSubType('Trading I&E')
                  .setSubType2(kpi.get('kpi_name', 'Major Dividends'))
                  .setMessage(validation_data['message'])
                  .setData(validation_data['data']))
    
    return validation


def _execute_dynamic_trading_ie_validation(trial_balance_a: List[Dict], trial_balance_b: List[Dict], 
                                         kpi: Dict, threshold: float, subtype2: str, is_dual_source: bool = False) -> Any:
    """
    Execute dynamic trading I&E validation for swap financing and interest accruals
    
    Args:
        trial_balance_a: Trial balance data for first period
        trial_balance_b: Trial balance data for second period
        kpi: KPI configuration dictionary
        threshold: Validation threshold
        subtype2: Specific validation subtype
        
    Returns:
        VALIDATION_STATUS object
    """
    
    # Route to specific validation based on subtype2
    if subtype2 == 'Material Swap Financing':
        return material_swap_financing_validation(trial_balance_a, trial_balance_b, threshold, kpi, is_dual_source)
    elif subtype2 == 'Material Interest Accruals':
        return material_interest_accrual_validation(trial_balance_a, trial_balance_b, threshold, kpi, is_dual_source)
    else:
        # Fallback for unknown subtypes
        validation_data = create_detailed_validation_result(
            validation_type='PnL',
            subtype='Trading I&E',
            subtype2=subtype2,
            failed_items=[],
            passed_items=[],
            threshold=threshold,
            kpi_info=kpi
        )
        
        validation = (VALIDATION_STATUS()
                      .setProductName('validus')
                      .setType('PnL')
                      .setSubType('Trading I&E')
                      .setSubType2(subtype2)
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


def major_dividends_validation(dividend_data, threshold=None, kpi_info=None):
    """
    Major Dividends validation
    
    Rule: Check Dividend report from Accounting system. If the difference in the total dividend 
    between 2 periods exceeds the threshold, then flag it as an exception
    
    Args:
        dividend_data: Dividend data to check
        threshold: Threshold for major dividend changes (optional)
        kpi_info: KPI configuration information
    
    Returns:
        VALIDATION_STATUS: Validation result object
    """
    # Handle case when no dividend data is available - consider validation as passed
    if not dividend_data or len(dividend_data) == 0:
        # Create validation result for no data case - show as passed
        validation_data = create_detailed_validation_result(
            validation_type='PnL',
            subtype='Trading I&E',
            subtype2='Major Dividends',
            failed_items=[],
            passed_items=[],
            threshold=threshold,
            kpi_info=kpi_info
        )
        
        # Override message to 0 (passed) when no data is available
        validation_data['message'] = 0
        
        # Create and return VALIDATION_STATUS object
        validation = (VALIDATION_STATUS()
                      .setProductName('validus')
                      .setType('PnL')
                      .setSubType('Trading I&E')
                      .setSubType2('Major Dividends')
                      .setMessage(validation_data['message'])
                      .setData(validation_data['data']))
        
        return validation
    
    # For now, we'll check for null/missing dividend amounts as a basic validation
    # In a full implementation, this would compare dividend totals between periods
    failed_items, passed_items, total_items = null_missing_check(
        dividend_data, 'Amount', 'Security id'
    )
    
    # Create validation result
    validation_data = create_detailed_validation_result(
        validation_type='PnL',
        subtype='Trading I&E',
        subtype2='Major Dividends',
        failed_items=failed_items,
        passed_items=passed_items,
        threshold=threshold,
        kpi_info=kpi_info
    )
    
    # Create and return VALIDATION_STATUS object
    validation = (VALIDATION_STATUS()
                  .setProductName('validus')
                  .setType('PnL')
                  .setSubType('Trading I&E')
                  .setSubType2('Major Dividends')
                  .setMessage(validation_data['message'])
                  .setData(validation_data['data']))
    
    return validation


def material_swap_financing_validation(trial_balance_data_a, trial_balance_data_b, threshold=None, kpi_info=None, is_dual_source=False):
    """
    Material Swap Financing validation
    
    Rule: Check for material swap financing entries using financial metrics
    
    Args:
        trial_balance_data_a: Trial balance data for period A
        trial_balance_data_b: Trial balance data for period B
        threshold: Threshold for material swap financing changes (percentage)
        kpi_info: KPI configuration information
    
    Returns:
        VALIDATION_STATUS: Validation result object
    """
    # Use financial metrics to get swap financing values
    total_swap_a = get_metric_value(trial_balance_data_a, 'swap_financing')
    total_swap_b = get_metric_value(trial_balance_data_b, 'swap_financing')
    
    
    failed_items = []
    passed_items = []
    
    if threshold is not None and kpi_info and kpi_info.get('precision_type') == 'PERCENTAGE':
        # For dual-source, we compare values between sources; for single-source, we look at changes over time
        if is_dual_source:
            # Cross-source comparison: check if swap financing amounts match between sources
            if total_swap_a != 0:
                percentage_change = abs((total_swap_b - total_swap_a) / abs(total_swap_a)) * 100
                signed_percentage_change = ((total_swap_b - total_swap_a) / abs(total_swap_a)) * 100
            else:
                percentage_change = 100 if total_swap_b != 0 else 0
                signed_percentage_change = 100 if total_swap_b > 0 else (-100 if total_swap_b < 0 else 0)
            comparison_type = "Cross-source discrepancy"
        else:
            # Period comparison: check if swap financing amounts changed significantly over time
            if total_swap_a != 0:
                percentage_change = abs((total_swap_b - total_swap_a) / abs(total_swap_a)) * 100
                signed_percentage_change = ((total_swap_b - total_swap_a) / abs(total_swap_a)) * 100
            else:
                percentage_change = 100 if total_swap_b != 0 else 0
                signed_percentage_change = 100 if total_swap_b > 0 else (-100 if total_swap_b < 0 else 0)
            comparison_type = "Period-over-period change"
            
        # Calculate absolute change for tooltip
        absolute_change = total_swap_b - total_swap_a
        tooltip_format = f"${absolute_change:,.3f}" if absolute_change >= 0 else f"-${abs(absolute_change):,.3f}"
        
        validation_item = {
            'identifier': 'Total Swap Financing',
            'field': 'Ending Balance',
            'value_a': total_swap_a,
            'value_b': total_swap_b,
            'change': signed_percentage_change,  # Frontend expects this field
            'change_value': percentage_change,
            'percentage_change': signed_percentage_change,
            'precision_type': 'PERCENTAGE',
            'threshold': threshold,
            'comparison': 'percentage_change',
            'display_change': f"{signed_percentage_change:.3f}%",
            'tooltip_change': tooltip_format,  # Opposite precision type for tooltip
            'is_failed': bool(percentage_change > threshold),
            'threshold_exceeded': bool(percentage_change > threshold)
        }
        
        if percentage_change > threshold:
            failed_items = [validation_item]
        else:
            passed_items = [validation_item]
    else:
        # Basic validation - check for presence of swap financing values
        failed_items = []
        if total_swap_a != 0 or total_swap_b != 0:
            passed_items = [{
                'identifier': 'Total Swap Financing',
                'field': 'Ending Balance',
                'value_a': total_swap_a,
                'value_b': total_swap_b,
                'change': 0,
                'precision_type': 'PERCENTAGE'
            }]
        else:
            passed_items = []
    
    total_items = len(passed_items) + len(failed_items)
    
    # Create validation result
    validation_data = create_detailed_validation_result(
        validation_type='PnL',
        subtype='Trading I&E',
        subtype2='Material Swap Financing',
        failed_items=failed_items,
        passed_items=passed_items,
        threshold=threshold,
        kpi_info=kpi_info
    )
    
    # Create and return VALIDATION_STATUS object
    validation = (VALIDATION_STATUS()
                  .setProductName('validus')
                  .setType('PnL')
                  .setSubType('Trading I&E')
                  .setSubType2('Material Swap Financing')
                  .setMessage(validation_data['message'])
                  .setData(validation_data['data']))
    
    return validation


def material_interest_accrual_validation(trial_balance_data_a, trial_balance_data_b, threshold=None, kpi_info=None, is_dual_source=False):
    """
    Material Interest Accrual validation
    
    Rule: Check for material interest accrual entries using financial metrics
    
    Args:
        trial_balance_data_a: Trial balance data for period A
        trial_balance_data_b: Trial balance data for period B
        threshold: Threshold for material interest accrual changes (percentage)
        kpi_info: KPI configuration information
    
    Returns:
        VALIDATION_STATUS: Validation result object
    """
    # Use financial metrics to get interest accrual values
    total_interest_a = get_metric_value(trial_balance_data_a, 'interest_accruals')
    total_interest_b = get_metric_value(trial_balance_data_b, 'interest_accruals')
    
    
    failed_items = []
    passed_items = []
    
    if threshold is not None and kpi_info and kpi_info.get('precision_type') == 'PERCENTAGE':
        # For dual-source, we compare values between sources; for single-source, we look at changes over time
        if is_dual_source:
            # Cross-source comparison: check if interest accrual amounts match between sources
            if total_interest_a != 0:
                percentage_change = abs((total_interest_b - total_interest_a) / abs(total_interest_a)) * 100
                signed_percentage_change = ((total_interest_b - total_interest_a) / abs(total_interest_a)) * 100
            else:
                percentage_change = 100 if total_interest_b != 0 else 0
                signed_percentage_change = 100 if total_interest_b > 0 else (-100 if total_interest_b < 0 else 0)
            comparison_type = "Cross-source discrepancy"
        else:
            # Period comparison: check if interest accrual amounts changed significantly over time
            if total_interest_a != 0:
                percentage_change = abs((total_interest_b - total_interest_a) / abs(total_interest_a)) * 100
                signed_percentage_change = ((total_interest_b - total_interest_a) / abs(total_interest_a)) * 100
            else:
                percentage_change = 100 if total_interest_b != 0 else 0
                signed_percentage_change = 100 if total_interest_b > 0 else (-100 if total_interest_b < 0 else 0)
            comparison_type = "Period-over-period change"
            
        # Calculate absolute change for tooltip
        absolute_change = total_interest_b - total_interest_a
        tooltip_format = f"${absolute_change:,.3f}" if absolute_change >= 0 else f"-${abs(absolute_change):,.3f}"
        
        validation_item = {
            'identifier': 'Total Interest Accruals',
            'field': 'Ending Balance',
            'value_a': total_interest_a,
            'value_b': total_interest_b,
            'change': signed_percentage_change,  # Frontend expects this field
            'change_value': percentage_change,
            'percentage_change': signed_percentage_change,
            'precision_type': 'PERCENTAGE',
            'threshold': threshold,
            'comparison': 'percentage_change',
            'display_change': f"{signed_percentage_change:.3f}%",
            'tooltip_change': tooltip_format,  # Opposite precision type for tooltip
            'is_failed': bool(percentage_change > threshold),
            'threshold_exceeded': bool(percentage_change > threshold)
        }
        
        if percentage_change > threshold:
            failed_items = [validation_item]
        else:
            passed_items = [validation_item]
    else:
        # Basic validation - check for presence of interest accrual values
        failed_items = []
        if total_interest_a != 0 or total_interest_b != 0:
            passed_items = [{
                'identifier': 'Total Interest Accruals',
                'field': 'Ending Balance',
                'value_a': total_interest_a,
                'value_b': total_interest_b,
                'change': 0,
                'precision_type': 'PERCENTAGE'
            }]
        else:
            passed_items = []
    
    total_items = len(passed_items) + len(failed_items)
    
    # Create validation result
    validation_data = create_detailed_validation_result(
        validation_type='PnL',
        subtype='Trading I&E',
        subtype2='Material Interest Accrual',
        failed_items=failed_items,
        passed_items=passed_items,
        threshold=threshold,
        kpi_info=kpi_info
    )
    
    # Create and return VALIDATION_STATUS object
    validation = (VALIDATION_STATUS()
                  .setProductName('validus')
                  .setType('PnL')
                  .setSubType('Trading I&E')
                  .setSubType2('Material Interest Accrual')
                  .setMessage(validation_data['message'])
                  .setData(validation_data['data']))
    
    return validation


def _create_dividend_composite_identifier(div_item):
    """
    Create composite identifier for dividend items using security ID and security name
    Similar to pricing validations composite identifier logic
    
    Args:
        div_item: Dividend item dictionary
        
    Returns:
        Tuple of (security_id, security_name) for composite matching
    """
    security_id = div_item.get('Security Id', div_item.get('security_id', div_item.get('Security id', '')))
    security_name = div_item.get('Security name', div_item.get('security_name', div_item.get('Security Name', '')))
    
    # Clean and normalize the values
    security_id = str(security_id).strip() if security_id else ''
    security_name = str(security_name).strip() if security_name else ''
    
    return (security_id, security_name)


def _compare_dividends_by_security(dividend_a: List[Dict], dividend_b: List[Dict], 
                                 kpi: Dict, threshold: float) -> Dict:
    """
    Compare dividends at security level between sources using composite identifier (security_id + security_name)
    Shows ALL securities from both sources - matched and unmatched (unmatched are marked as PASSED)
    
    Args:
        dividend_a: Dividend data from source A
        dividend_b: Dividend data from source B
        kpi: KPI configuration
        threshold: Validation threshold
        
    Returns:
        Dictionary with validation data structure
    """
    failed_items = []
    passed_items = []
    
    if not dividend_a and not dividend_b:
        # No dividend data from either source
        return create_detailed_validation_result(
            validation_type='PnL',
            subtype='Trading I&E',
            subtype2=kpi.get('kpi_name', 'Major Dividends'),
            failed_items=[],
            passed_items=[],
            threshold=threshold,
            kpi_info=kpi
        )
    
    # Create lookups for both sources using composite identifiers
    dividends_a_lookup = {}
    if dividend_a:
        for div in dividend_a:
            composite_key = _create_dividend_composite_identifier(div)
            dividends_a_lookup[composite_key] = div
    
    dividends_b_lookup = {}
    if dividend_b:
        for div in dividend_b:
            composite_key = _create_dividend_composite_identifier(div)
            dividends_b_lookup[composite_key] = div
    
    # Get all unique composite keys from both sources
    all_composite_keys = set(dividends_a_lookup.keys()) | set(dividends_b_lookup.keys())
    
    # Process all securities from both sources
    for composite_key in all_composite_keys:
        security_id, security_name = composite_key
        div_a = dividends_a_lookup.get(composite_key)
        div_b = dividends_b_lookup.get(composite_key)
        
        # Get values from both periods (use 0 if not present)
        try:
            amount_a = 0.0
            amount_b = 0.0
            
            if div_a:
                amount_a_raw = div_a.get('Amount', div_a.get('amount', 0))
                if amount_a_raw is not None and amount_a_raw != '' and str(amount_a_raw).lower() not in ['nan', 'null', 'none']:
                    if not (isinstance(amount_a_raw, float) and amount_a_raw != amount_a_raw):  # Not NaN
                        amount_a = float(amount_a_raw)
            
            if div_b:
                amount_b_raw = div_b.get('Amount', div_b.get('amount', 0))
                if amount_b_raw is not None and amount_b_raw != '' and str(amount_b_raw).lower() not in ['nan', 'null', 'none']:
                    if not (isinstance(amount_b_raw, float) and amount_b_raw != amount_b_raw):  # Not NaN
                        amount_b = float(amount_b_raw)
            
            # Use security name for display, fallback to security_id
            display_identifier = security_name if security_name else security_id
            
            # Calculate percentage change for threshold comparison
            if amount_a != 0:
                percentage_change = abs((amount_b - amount_a) / abs(amount_a)) * 100
                signed_percentage_change = ((amount_b - amount_a) / abs(amount_a)) * 100
            else:
                # If amount_a is 0, use large percentage if amount_b is not 0
                percentage_change = 999999.99 if amount_b != 0 else 0
                signed_percentage_change = 100 if amount_b > 0 else (-100 if amount_b < 0 else 0)
            
            # Determine if validation failed
            # Only fail if both sources have the security AND difference exceeds threshold
            is_failed = bool(div_a and div_b and percentage_change > threshold)
            
            # Calculate tooltip with absolute change
            absolute_change = amount_b - amount_a
            tooltip_format = f"${absolute_change:,.3f}" if absolute_change >= 0 else f"-${abs(absolute_change):,.3f}"
            
            # Create validation item
            validation_item = {
                'identifier': display_identifier,
                'inv_id': security_id,
                'description': security_name,
                'asset_type': '-',  # Set asset type for dividends
                'field': 'Amount',
                'value_a': amount_a if div_a else '-',
                'value_b': amount_b if div_b else '-',  # Show '-' if not found in source B
                'change': signed_percentage_change if div_a and div_b else 0,  # Only show change if both sources have data
                'change_value': percentage_change if div_a and div_b else 0,
                'percentage_change': signed_percentage_change if div_a and div_b else 0,
                'precision_type': kpi.get('precision_type', 'PERCENTAGE'),
                'threshold': threshold,
                'is_failed': is_failed,
                'threshold_exceeded': is_failed,
                'display_change': f"{signed_percentage_change:.3f}%" if div_a and div_b else "0.000%",
                'tooltip_change': tooltip_format,
                'comparison': 'greater_than',
                'issue': 'major_dividend_change',
                'raw_data_a': div_a,
                'raw_data_b': div_b
            }
            
            # Categorize as failed or passed
            if is_failed:
                failed_items.append(validation_item)
            else:
                passed_items.append(validation_item)
                
        except (ValueError, TypeError) as e:
            # Skip items with invalid numeric values
            continue
    
    return create_detailed_validation_result(
        validation_type='PnL',
        subtype='Trading I&E',
        subtype2=kpi.get('kpi_name', 'Major Dividends'),
        failed_items=failed_items,
        passed_items=passed_items,
        threshold=threshold,
        kpi_info=kpi
    )


def _compare_dividends_total(dividend_a: List[Dict], dividend_b: List[Dict], 
                           kpi: Dict, threshold: float) -> Dict:
    """
    Compare total dividend amounts between periods (SINGLE_SOURCE mode)
    Now includes children showing individual dividend sources
    
    Args:
        dividend_a: Dividend data from period A
        dividend_b: Dividend data from period B
        kpi: KPI configuration
        threshold: Validation threshold
        
    Returns:
        Dictionary with validation data structure including children
    """
    # Calculate total dividends for each period
    total_a = 0.0
    total_b = 0.0
    
    # Track individual dividend sources for children
    dividend_sources_a = {}
    dividend_sources_b = {}
    
    if dividend_a:
        for div in dividend_a:
            try:
                amount = float(div.get('Amount', div.get('amount', 0)))
                total_a += amount
                
                # Track by security for children
                security_id = div.get('Security ID', div.get('security_id', ''))
                security_name = div.get('Security Name', div.get('security_name', ''))
                composite_key = (security_id, security_name)
                
                if composite_key not in dividend_sources_a:
                    dividend_sources_a[composite_key] = 0.0
                dividend_sources_a[composite_key] += amount
                
            except (ValueError, TypeError):
                continue
                
    if dividend_b:
        for div in dividend_b:
            try:
                amount = float(div.get('Amount', div.get('amount', 0)))
                total_b += amount
                
                # Track by security for children
                security_id = div.get('Security ID', div.get('security_id', ''))
                security_name = div.get('Security Name', div.get('security_name', ''))
                composite_key = (security_id, security_name)
                
                if composite_key not in dividend_sources_b:
                    dividend_sources_b[composite_key] = 0.0
                dividend_sources_b[composite_key] += amount
                
            except (ValueError, TypeError):
                continue
    
    # Calculate absolute and percentage changes
    absolute_change = abs(total_b - total_a)
    
    # Calculate percentage change for threshold comparison and display
    if total_a != 0:
        unsigned_percentage_change = abs((total_b - total_a) / abs(total_a)) * 100
        signed_percentage_change = ((total_b - total_a) / abs(total_a)) * 100
    else:
        unsigned_percentage_change = 100 if total_b != 0 else 0
        signed_percentage_change = 100 if total_b > 0 else (-100 if total_b < 0 else 0)
    
    # Get precision type from KPI
    precision_type = kpi.get('precision_type', 'PERCENTAGE')
    
    # Format threshold based on precision type
    if precision_type == 'PERCENTAGE':
        formatted_threshold = f"{threshold:.2f}%"
        is_major_change = unsigned_percentage_change > threshold
        change_value = unsigned_percentage_change
    else:  # ABSOLUTE
        formatted_threshold = f"${threshold:,.2f}"
        is_major_change = absolute_change > threshold
        change_value = absolute_change
    
    # Create validation result
    failed_items = []
    passed_items = []
    
    # Create main validation item (Major Dividend - parent)
    validation_item = {
        'identifier': 'Major Dividends',
        'inv_id': 'MAJOR_DIVIDENDS',
        'description': 'Major Dividends',
        'asset_type': '-',
        'field': 'Summary',
        'value_a': total_a,
        'value_b': total_b,
        'change': signed_percentage_change,
        'change_value': change_value,
        'absolute_change': absolute_change,
        'percentage_change': signed_percentage_change,
        'threshold': formatted_threshold,
        'precision_type': precision_type,
        'is_failed': is_major_change,
        'threshold_exceeded': is_major_change,
        'display_change': f"{signed_percentage_change:.3f}%",
        'tooltip_change': f"${absolute_change:,.3f}",
        'issue': 'major_dividend_summary',
        'raw_data_a': {'total': total_a},
        'raw_data_b': {'total': total_b},
        'extra_data_children': []  # Will be populated below
    }
    
    # Create Total Dividend as child
    total_dividend_child = {
        'identifier': 'Total Dividends',
        'inv_id': 'TOTAL_DIVIDENDS',
        'description': 'Total Dividends',
        'asset_type': '-',
        'field': 'Total Amount',
        'value_a': total_a,
        'value_b': total_b,
        'change': signed_percentage_change,
        'change_value': change_value,
        'absolute_change': absolute_change,
        'percentage_change': signed_percentage_change,
        'threshold': formatted_threshold,
        'precision_type': precision_type,
        'is_failed': is_major_change,
        'threshold_exceeded': is_major_change,
        'display_change': f"{signed_percentage_change:.3f}%",
        'tooltip_change': f"${absolute_change:,.3f}",
        'issue': 'total_dividend_change',
        'raw_data_a': {'total': total_a},
        'raw_data_b': {'total': total_b},
        'extra_data_children': [],  # Will contain individual securities
        'is_child': True
    }
    
    # Create grandchildren for individual dividend sources
    all_security_keys = set(dividend_sources_a.keys()) | set(dividend_sources_b.keys())
    
    for security_key in all_security_keys:
        security_id, security_name = security_key
        
        # Get amounts from both periods
        amount_a = dividend_sources_a.get(security_key, 0.0)
        amount_b = dividend_sources_b.get(security_key, 0.0)
        
        # Use security name for display, fallback to security_id
        display_name = security_name if security_name else security_id
        
        # Create grandchild item - grandchildren don't have change/threshold comparisons
        grandchild_item = {
            'identifier': display_name,
            'inv_id': security_id,
            'description': security_name,
            'asset_type': '-',
            'field': 'Dividend Amount',
            'value_a': amount_a if amount_a > 0 else '-',
            'value_b': amount_b if amount_b > 0 else '-',
            'change': '-',  # No change comparison for grandchildren
            'change_value': '-',
            'absolute_change': '-',
            'percentage_change': '-',
            'threshold': '-',  # No threshold for grandchildren
            'precision_type': precision_type,
            'is_failed': False,  # Grandchildren are always passed
            'threshold_exceeded': False,
            'display_change': '-',
            'tooltip_change': '-',
            'issue': 'dividend_source',
            'raw_data_a': {'amount': amount_a},
            'raw_data_b': {'amount': amount_b},
            'is_child': True,  # Mark as child for frontend
            'is_grandchild': True  # Mark as grandchild for frontend
        }
        
        total_dividend_child['extra_data_children'].append(grandchild_item)
    
    # Add Total Dividend child to main validation item
    validation_item['extra_data_children'].append(total_dividend_child)
    
    if is_major_change:
        failed_items.append(validation_item)
    else:
        passed_items.append(validation_item)
    
    return create_detailed_validation_result(
        validation_type='PnL',
        subtype='Trading I&E',
        subtype2=kpi.get('kpi_name', 'Major Dividends'),
        failed_items=failed_items,
        passed_items=passed_items,
        threshold=threshold,
        kpi_info=kpi
    )

"""
Dynamic Market Value validation KPI modules
Implements dynamic market value validation rules based on KPI configurations from database
Handles: Major MV Change and other market value validations
"""

from typing import List, Dict, Optional, Any
from clients.validusDemo.customFunctions.validation_utils import (
    greater_than_threshold_check, price_change_percentage_check, price_change_percentage_check_with_composite_id,
    create_detailed_validation_result, _create_composite_identifier
)
from clients.validusDemo.customFunctions.financial_metrics import get_metric_value
from validations import VALIDATION_STATUS


def market_value_validations(trial_balance_a: List[Dict], trial_balance_b: List[Dict],
                           portfolio_a: List[Dict], portfolio_b: List[Dict], 
                           mv_kpis: List[Dict], fund_id: Optional[int] = None,
                           is_dual_source: bool = False) -> List[Any]:
    """
    Main dynamic market value validation function that processes all market value KPIs
    Routes to specific validation functions based on KPI configuration
    
    Args:
        trial_balance_a: Trial balance data for first period
        trial_balance_b: Trial balance data for second period
        portfolio_a: Portfolio data for first period  
        portfolio_b: Portfolio data for second period
        mv_kpis: List of market value category KPIs from database
        fund_id: Fund identifier for threshold lookup
        
    Returns:
        List of VALIDATION_STATUS objects
    """
    validations = []
    
    if not portfolio_a or not portfolio_b:
        return validations
    
    
    # Process each market value KPI dynamically
    for kpi in mv_kpis:
        try:
            # Get KPI configuration
            kpi_code = kpi.get('kpi_code', '').lower()
            numerator_field = kpi.get('numerator_field', '')
            precision_type = kpi.get('precision_type', 'PERCENTAGE')
            threshold = kpi.get('threshold')
            
            if threshold is None:
                continue
                
            # Route to specific validation based on numerator field only
            if numerator_field == 'market_value':
                # Market value change validation
                validation = _execute_dynamic_mv_validation(
                    portfolio_a, portfolio_b, kpi, threshold, is_dual_source
                )
                validations.append(validation)
                
        except Exception as e:
            # Handle individual KPI errors gracefully
            error_validation = _create_error_validation(f'Error processing KPI {kpi_code}: {str(e)}', 'Market Value')
            validations.append(error_validation)
    
    return validations


def _execute_dynamic_mv_validation(portfolio_a: List[Dict], portfolio_b: List[Dict], 
                                 kpi: Dict, threshold: float, is_dual_source: bool = False) -> Any:
    """
    Execute dynamic market value validation based on KPI configuration
    
    Args:
        portfolio_a: Portfolio data for first period
        portfolio_b: Portfolio data for second period  
        kpi: KPI configuration dictionary
        threshold: Validation threshold
        
    Returns:
        VALIDATION_STATUS object
    """
    # Use End Book MV field for market value validation
    mv_field = 'End Book MV'
    
    # Execute validation based on precision type
    precision_type = kpi.get('precision_type', 'PERCENTAGE')
    
    if precision_type == 'PERCENTAGE':
        # Convert threshold to decimal if needed (5% = 0.05)
        
        # Use percentage-based validation
        failed_items, passed_items, total_items = price_change_percentage_check_with_composite_id(
            portfolio_a, portfolio_b, mv_field, threshold
        )
    else:
        # For ABSOLUTE precision type, use absolute difference check
        failed_items = []
        passed_items = []
        
        # Create lookup dictionary for portfolio_a using composite identifiers
        portfolio_a_lookup = {}
        for item in portfolio_a:
            composite_key = _create_composite_identifier(item, 'Inv Id')
            portfolio_a_lookup[composite_key] = item
        
        matched_items = 0
        skipped_items = 0
        
        for item_b in portfolio_b:
            composite_key = _create_composite_identifier(item_b, 'Inv Id')
            inv_id, description = composite_key
            
            if composite_key in portfolio_a_lookup:
                item_a = portfolio_a_lookup[composite_key]
                matched_items += 1
                
                # Get market values
                mv_a = item_a.get(mv_field, 0.0) or 0.0
                mv_b = item_b.get(mv_field, 0.0) or 0.0
                
                # Calculate absolute difference
                abs_diff = abs(mv_b - mv_a)
                
                # Use description for display, fallback to inv_id
                display_identifier = description if description else inv_id
                
                # Extract Asset Type from item data
                from clients.validusDemo.customFunctions.validation_utils import extract_asset_type
                asset_type = extract_asset_type(item_b)
                
                # Calculate percentage change for display
                if mv_a != 0:
                    percentage_change = ((mv_b - mv_a) / abs(mv_a)) * 100
                else:
                    percentage_change = 100 if mv_b > 0 else (-100 if mv_b < 0 else 0)
                
                # Determine if validation failed (ensure JSON-serializable boolean)
                is_failed = bool(abs_diff > threshold)
                
            # Calculate display and tooltip based on precision type with consistent 3 decimal places
            if precision_type == 'ABSOLUTE':
                # Main display shows absolute change, tooltip shows percentage
                display_change = f"${abs_diff:,.3f}"
                tooltip_format = f"{percentage_change:.3f}%"
                change_value = abs_diff  # For absolute precision, use absolute change
            else:  # PERCENTAGE
                # Main display shows percentage, tooltip shows absolute change
                display_change = f"{percentage_change:.3f}%"
                tooltip_format = f"${abs_diff:,.3f}" if abs_diff >= 0 else f"-${abs(abs_diff):,.3f}"
                change_value = percentage_change  # For percentage precision, use percentage change
                
                # Create comprehensive validation item
                validation_item = {
                    'identifier': display_identifier,
                    'inv_id': inv_id,
                    'description': description,
                    'asset_type': asset_type,
                    'field': mv_field,
                    'value_a': mv_a,
                    'value_b': mv_b,
                    'change': change_value,  # Dynamic based on precision type
                    'absolute_change': abs_diff,
                    'percentage_change': percentage_change,
                    'threshold': threshold,
                    'precision_type': precision_type,  # Use KPI precision type dynamically
                    'is_failed': is_failed,
                    'threshold_exceeded': is_failed,
                    'display_change': display_change,  # Dynamic based on precision type
                    'tooltip_change': tooltip_format,  # Opposite precision type for tooltip
                    'issue': 'major_mv_change',
                    'raw_data_a': item_a,
                    'raw_data_b': item_b
                }
                
                if is_failed:
                    failed_items.append(validation_item)
                else:
                    passed_items.append(validation_item)
    
    # Rearrange the failed_items as well as the passed_items so that asset_type cash comes after cashf at bottom
    def _get_sort_key(item):
        asset_type = (item.get('asset_type') or '').lower()
        if asset_type == 'cash':
            return 2  # CASH comes last (highest priority for sorting to bottom)
        elif asset_type == 'cashf':
            return 1  # CASHF comes second to last
        else:
            return 0  # All other asset types come first
    
    failed_items = sorted(failed_items, key=_get_sort_key)
    passed_items = sorted(passed_items, key=_get_sort_key)
    
    # Create validation result with sorted items
    validation_data = create_detailed_validation_result(
        validation_type='PnL',
        subtype='Market Value',
        subtype2=kpi.get('kpi_name', 'Major MV Change'),
        failed_items=failed_items,
        passed_items=passed_items,
        threshold=threshold,
        kpi_info=kpi
    )
    
    # Create and return VALIDATION_STATUS object
    validation = (VALIDATION_STATUS()
                  .setProductName('validus')
                  .setType('PnL')
                  .setSubType('Market Value')
                  .setSubType2(kpi.get('kpi_name', 'Major MV Change'))
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


def major_mv_change_validation(portfolio_a, portfolio_b, threshold, kpi_info=None):
    """
    Major MV change validation
    
    Rule: From the input files "Portfolio Valuation By Instrum" report compare the column "End 
    Book MV" for each security between 2 different periods. If the difference in the End Book 
    MV exceeds the threshold set, then flag it as an exception
    
    Args:
        portfolio_a: Portfolio data for first period
        portfolio_b: Portfolio data for second period
        threshold: Threshold for major market value changes
        kpi_info: KPI configuration information
    
    Returns:
        VALIDATION_STATUS: Validation result object
    """
    # Check for major market value changes using End Book MV field with composite identifier matching
    # Use KPI info to determine precision type, fallback to threshold value check for backwards compatibility
    precision_type = kpi_info.get('precision_type', 'ABSOLUTE') if kpi_info else 'ABSOLUTE'
    
    if precision_type == 'ABSOLUTE' or (isinstance(threshold, (int, float)) and threshold >= 1000):
        # Absolute threshold in currency units
        failed_items = []
        passed_items = []
        
        # Create lookup dictionary for portfolio_a using composite identifiers
        portfolio_a_lookup = {}
        for item in portfolio_a:
            composite_key = _create_composite_identifier(item, 'Inv Id')
            portfolio_a_lookup[composite_key] = item
        
        matched_items = 0
        skipped_items = 0
        
        for item_b in portfolio_b:
            composite_key = _create_composite_identifier(item_b, 'Inv Id')
            inv_id, description = composite_key
            
            # Skip if no matching composite identifier in period A
            item_a = portfolio_a_lookup.get(composite_key)
            if item_a is None:
                skipped_items += 1
                continue
                
            matched_items += 1
            
            try:
                mv_a = float(item_a.get('End Book MV', 0))
                mv_b = float(item_b.get('End Book MV', 0))
                mv_change = abs(mv_b - mv_a)
                
                # Calculate signed percentage change using formula (B-A)/|A|
                if mv_a != 0:
                    percentage_change = ((mv_b - mv_a) / abs(mv_a)) * 100
                else:
                    percentage_change = 100 if mv_b > 0 else (-100 if mv_b < 0 else 0)
                
                # Use description for display, fallback to inv_id
                display_identifier = description if description else inv_id
                
                if mv_change > threshold:
                    failed_items.append({
                        'identifier': display_identifier,
                        'inv_id': inv_id,
                        'description': description,
                        'field': 'End Book MV',
                        'value_a': mv_a,
                        'value_b': mv_b,
                        'absolute_change': mv_change,
                        'percentage_change': percentage_change,
                        'threshold': threshold,
                        'issue': 'major_mv_change',
                        'raw_data_a': item_a,
                        'raw_data_b': item_b
                    })
                else:
                    passed_items.append({
                        'identifier': display_identifier,
                        'inv_id': inv_id,
                        'description': description,
                        'field': 'End Book MV',
                        'value_a': mv_a,
                        'value_b': mv_b,
                        'absolute_change': mv_change,
                        'percentage_change': percentage_change,
                        'threshold': threshold,
                        'issue': 'major_mv_change',
                        'raw_data_a': item_a,
                        'raw_data_b': item_b
                    })
            except (ValueError, TypeError):
                # Skip items with invalid numeric values
                continue
        
    else:
        # Percentage threshold - use composite identifier version
        failed_items, passed_items, total_items = price_change_percentage_check_with_composite_id(
            portfolio_a, portfolio_b, 'End Book MV', threshold, 'Inv Id'
        )
    
    # Rearrange the failed_items as well as the passed_items so that asset_type cash comes after cashf at bottom
    def _get_sort_key(item):
        asset_type = (item.get('asset_type') or '').lower()
        if asset_type == 'cash':
            return 2  # CASH comes last (highest priority for sorting to bottom)
        elif asset_type == 'cashf':
            return 1  # CASHF comes second to last
        else:
            return 0  # All other asset types come first
    
    failed_items = sorted(failed_items, key=_get_sort_key)
    passed_items = sorted(passed_items, key=_get_sort_key)
    
    # Create validation result with sorted items
    validation_data = create_detailed_validation_result(
        validation_type='PnL',
        subtype='Market Value',
        subtype2='Major MV Change',
        failed_items=failed_items,
        passed_items=passed_items,
        threshold=threshold,
        kpi_info=kpi_info
    )
    
    # Create and return VALIDATION_STATUS object
    validation = (VALIDATION_STATUS()
                  .setProductName('validus')
                  .setType('PnL')
                  .setSubType('Market Value')
                  .setSubType2('Major MV Change')
                  .setMessage(validation_data['message'])
                  .setData(validation_data['data']))
    
    return validation

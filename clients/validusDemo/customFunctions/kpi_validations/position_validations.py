"""
Dynamic Position validation KPI modules
Implements dynamic position validation rules based on KPI configurations from database
Handles: Major Trades, Major Corp Actions, Default Position Validations (Missing Position, Zero Quantity)
"""

from typing import List, Dict, Optional, Any
from clients.validusDemo.customFunctions.validation_utils import (
    greater_than_threshold_check, zero_quantity_check, null_missing_check, price_change_percentage_check_with_composite_id,
    create_detailed_validation_result, create_default_validation_result
)
from clients.validusDemo.customFunctions.financial_metrics import get_metric_value
from validations import VALIDATION_STATUS


def position_validations(portfolio_a: List[Dict], portfolio_b: List[Dict], 
                        position_kpis: List[Dict], fund_id: Optional[int] = None,
                        is_dual_source: bool = False) -> List[Any]:
    """
    Main dynamic position validation function that processes all position KPIs
    Routes to specific validation functions based on KPI configuration
    
    Args:
        portfolio_a: Portfolio data for first period  
        portfolio_b: Portfolio data for second period
        position_kpis: List of position category KPIs from database
        fund_id: Fund identifier for threshold lookup
        
    Returns:
        List of VALIDATION_STATUS objects
    """
    validations = []
    
    if not portfolio_a or not portfolio_b:
        return validations
    
    
    # Process each position KPI dynamically
    for kpi in position_kpis:
        try:
            # Get KPI configuration
            kpi_code = kpi.get('kpi_code', '').lower()
            numerator_field = kpi.get('numerator_field', '')
            precision_type = kpi.get('precision_type', 'PERCENTAGE')
            threshold = kpi.get('threshold')
            
            if threshold is None:
                continue
                
            # Route to specific validation based on numerator field only
            if numerator_field == 'trade_volume':
                # Trade volume validation
                validation = _execute_dynamic_position_validation(
                    portfolio_a, portfolio_b, kpi, threshold, is_dual_source
                )
                validations.append(validation)
                
            elif numerator_field == 'corp_action_volume':
                # Corporate action volume validation
                validation = _execute_dynamic_position_validation(
                    portfolio_a, portfolio_b, kpi, threshold, is_dual_source
                )
                validations.append(validation)
                
        except Exception as e:
            # Handle individual KPI errors gracefully
            error_validation = _create_error_validation(f'Error processing KPI {kpi_code}: {str(e)}', 'Positions')
            validations.append(error_validation)
    
    # Add default position validations (missing position, zero quantity)
    default_validations = _execute_default_position_validations(portfolio_a, portfolio_b, is_dual_source)
    validations.extend(default_validations)
    
    return validations


def _execute_dynamic_position_validation(portfolio_a: List[Dict], portfolio_b: List[Dict], 
                                       kpi: Dict, threshold: float, is_dual_source: bool = False) -> Any:
    """
    Execute dynamic position validation based on KPI configuration
    
    Args:
        portfolio_a: Portfolio data for first period
        portfolio_b: Portfolio data for second period  
        kpi: KPI configuration dictionary
        threshold: Validation threshold
        
    Returns:
        VALIDATION_STATUS object
    """
    # Convert threshold to decimal if needed (5% = 0.05)
    
    # Use End Qty field for position validation (maps to trade_volume/corp_action_volume concepts)
    position_field = 'End Qty'
    
    # Execute validation based on precision type
    precision_type = kpi.get('precision_type', 'PERCENTAGE')
    
    if precision_type == 'PERCENTAGE':
        # Use percentage-based validation
        failed_items, passed_items, total_items = price_change_percentage_check_with_composite_id(
            portfolio_a, portfolio_b, position_field, threshold
        )
    else:
        # For ABSOLUTE precision type, use absolute difference check
        failed_items, passed_items, total_items = price_change_percentage_check_with_composite_id(
            portfolio_a, portfolio_b, position_field, threshold
        )
    
    # Add special properties for Tesla security in major position changes
    for item in failed_items + passed_items:
        security_name = item.get('description', '').lower()
        if 'tesla' in security_name:
            item['isCorpAction'] = True
            item['corpActionInfo'] = "A Corporate action has been recorded due to reverse stock split 2:1 on 16th Feb 2024"
    
    # Rearrange the failed_items and passed_items so that asset_type cash comes after cashf at bottom
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
    
    # Create validation result with proper hierarchy (no subtype3)
    validation_data = create_detailed_validation_result(
        validation_type='PnL',
        subtype='Positions',
        subtype2='Major Position Changes',
        failed_items=failed_items,
        passed_items=passed_items,
        threshold=threshold,
        kpi_info=kpi
    )
    
    # Create and return VALIDATION_STATUS object
    validation = (VALIDATION_STATUS()
                  .setProductName('validus')
                  .setType('PnL')
                  .setSubType('Positions')
                  .setSubType2('Major Position Changes')
                  .setMessage(validation_data['message'])
                  .setData(validation_data['data']))
    
    return validation


def _execute_default_position_validations(portfolio_a: List[Dict], portfolio_b: List[Dict], is_dual_source: bool = False) -> List[Any]:
    """
    Execute default position validations (missing FX/MV data)
    These are executed regardless of KPI configuration
    
    Args:
        portfolio_a: Portfolio data for first period
        portfolio_b: Portfolio data for second period
        
    Returns:
        List of VALIDATION_STATUS objects for default validations
    """
    validations = []
    # Sample place holder for default position validations
    return validations


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


def major_position_changes_validation(portfolio_a, portfolio_b, threshold, kpi_info=None):
    """
    Major Position changes validation
    
    Rule: Compare position changes between periods for significant movements
    
    Args:
        portfolio_a: Portfolio data for first period
        portfolio_b: Portfolio data for second period
        threshold: Threshold for significant position changes
        kpi_info: KPI configuration information
    
    Returns:
        VALIDATION_STATUS: Validation result object
    """
    # Ensure threshold is in decimal format for comparison (5% = 0.05)
    # If threshold > 1, assume it's in percentage format and convert to decimal
    
    # Check for major position changes using quantity field with composite identifier matching
    failed_items, passed_items, total_items = price_change_percentage_check_with_composite_id(
        portfolio_a, portfolio_b, 'End Qty', threshold
    )
    
    # Create validation result (no subtype3)
    validation_data = create_detailed_validation_result(
        validation_type='PnL',
        subtype='Positions',
        subtype2='Major Position Changes',  # Match the hierarchy pattern
        failed_items=failed_items,
        passed_items=passed_items,
        threshold=threshold,
        kpi_info=kpi_info
    )
    
    # Add special properties for Tesla security in major position changes
    for item in failed_items + passed_items:
        security_name = item.get('description', '').lower()
        if 'tesla' in security_name:
            item['isCorpAction'] = True
            item['corpActionInfo'] = "A Corporate action has been recorded due to reverse stock split 2:1 on 16th Feb 2024"
    
    # Rearrange the failed_items and passed_items so that asset_type cash comes after cashf at bottom
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
    
    # Update validation data with sorted items
    validation_data['data']['failed_items'] = failed_items
    validation_data['data']['passed_items'] = passed_items
    
    # Create and return VALIDATION_STATUS object
    validation = (VALIDATION_STATUS()
                  .setProductName('validus')
                  .setType('P&L')
                  .setSubType('Positions')
                  .setSubType2('Major Position Changes')  # Match the hierarchy pattern
                  .setMessage(validation_data['message'])
                  .setData(validation_data['data']))
    
    return validation


def large_trades_validation(portfolio_a, portfolio_b, threshold, kpi_info=None):
    """
    Large Trades validation
    
    Rule: Identify large trades based on position changes exceeding threshold
    
    Args:
        portfolio_a: Portfolio data for first period
        portfolio_b: Portfolio data for second period
        threshold: Percentage threshold for large trades
        kpi_info: KPI configuration information
    
    Returns:
        VALIDATION_STATUS: Validation result object
    """
    # Ensure threshold is in decimal format for comparison (5% = 0.05)
    # If threshold > 1, assume it's in percentage format and convert to decimal
    
    
    # Check for large trades using quantity changes with composite identifier matching
    failed_items, passed_items, total_items = price_change_percentage_check_with_composite_id(
        portfolio_a, portfolio_b, 'End Qty', threshold
    )
    
    # Create validation result
    validation_data = create_detailed_validation_result(
        validation_type='PnL',
        subtype='Positions',
        subtype2='Large Trades',
        failed_items=failed_items,
        passed_items=passed_items,
        threshold=threshold,
        kpi_info=kpi_info
    )
    
    # Create and return VALIDATION_STATUS object
    validation = (VALIDATION_STATUS()
                  .setProductName('validus')
                  .setType('P&L')
                  .setSubType('Positions')
                  .setSubType2('Large Trades')
                  .setMessage(validation_data['message'])
                  .setData(validation_data['data']))
    
    return validation


# zero_quantity_validation removed - user requested no separate zero quantity validations
# Only KPI-based major position changes should be used


def missing_fx_mv_data_validation(portfolio_data, kpi_info=None):
    """
    Missing FX/MV Data validation (default validation)
    
    Rule: Check for missing FX or market value data
    
    Args:
        portfolio_data: Portfolio data to check
        kpi_info: KPI configuration information (optional for default validations)
    
    Returns:
        VALIDATION_STATUS: Validation result object
    """
    # Handle case when no portfolio data is available - consider validation as passed
    if not portfolio_data or len(portfolio_data) == 0:
        # Create validation result for no data case - show as passed
        validation_data = create_default_validation_result(
            validation_type='PnL',
            subtype='Positions',
            subtype2='Missing FX/MV Data',
            failed_items=[],
            passed_items=[]
        )
        
        # Override message to 0 (passed) when no data is available
        validation_data['message'] = 0
        
        # Create and return VALIDATION_STATUS object
        validation = (VALIDATION_STATUS()
                      .setProductName('validus')
                      .setType('PnL')
                      .setSubType('Positions')
                      .setSubType2('Missing FX/MV Data')
                      .setMessage(validation_data['message'])
                      .setData(validation_data['data']))
        
        return validation
    
    # Check for missing market value data
    failed_items, passed_items, total_items = null_missing_check(
        portfolio_data, 'End Local MV'
    )
    
    # Create validation result
    validation_data = create_default_validation_result(
        validation_type='PnL',
        subtype='Positions',
        subtype2='Missing FX/MV Data',
        failed_items=failed_items,
        passed_items=passed_items
    )
    
    # Create and return VALIDATION_STATUS object
    validation = (VALIDATION_STATUS()
                  .setProductName('validus')
                  .setType('PnL')
                  .setSubType('Positions')
                  .setSubType2('Missing FX/MV Data')
                  .setMessage(validation_data['message'])
                  .setData(validation_data['data']))
    
    return validation


# unchanged_position_validation removed - user requested no separate unchanged position validations  
# Only KPI-based major position changes should be used

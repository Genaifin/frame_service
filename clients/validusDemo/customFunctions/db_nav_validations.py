"""
Database-driven NAV validation functions
Modular functions for price changes, position changes, and market value validations
"""

from server.APIServerUtils.db_validation_service import db_validation_service
from validations import VALIDATION_STATUS
from clients.validusDemo.customFunctions.validation_utils import (
    greater_than_threshold_check, less_than_threshold_check, null_missing_check,
    zero_quantity_check, unchanged_value_check, price_change_percentage_check,
    missing_price_zero_mv_check, missing_price_null_check, create_detailed_validation_result, create_default_validation_result
)
from clients.validusDemo.customFunctions.kpi_validations import (
    unchanged_price_securities_validation, major_price_changes_validation,
    major_fx_changes_validation, null_missing_price_validation,
    large_trades_validation
)
from typing import List, Dict, Any, Optional
import pandas as pd
from .financial_metrics import calculate_financial_metrics, get_metric_value


def nav_validations(fund_name: str, source_a: str, source_b: str, date_a: str, date_b: str, 
                   nav_kpis: List[Dict], fund_id: Optional[int] = None,
                   trial_balance_a: List[Dict] = None, trial_balance_b: List[Dict] = None,
                   portfolio_a: List[Dict] = None, portfolio_b: List[Dict] = None,
                   dividend_a: List[Dict] = None, dividend_b: List[Dict] = None,
                   is_dual_source: bool = False) -> List[Any]:
    """
    Dynamic NAV validation system - reads KPI configurations from database and routes to category-based functions
    OPTIMIZED: Accept pre-fetched data to avoid redundant database calls
    Supports both SINGLE_SOURCE and DUAL_SOURCE validation modes
    """
    validations = []
        
    # OPTIMIZATION: Use pre-fetched data if provided, otherwise fetch from database
    if trial_balance_a is None:
        trial_balance_a = db_validation_service.get_trial_balance_data(fund_name, source_a, date_a)
    if portfolio_a is None:
        portfolio_a = db_validation_service.get_portfolio_valuation_data(fund_name, source_a, date_a)
    if dividend_a is None:
        dividend_a = db_validation_service.get_dividend_data(fund_name, source_a, date_a)
    
    if trial_balance_b is None:
        if date_b != date_a or source_b != source_a:
            trial_balance_b = db_validation_service.get_trial_balance_data(fund_name, source_b, date_b)
        else:
            trial_balance_b = trial_balance_a
            
    if portfolio_b is None:
        if date_b != date_a or source_b != source_a:
            portfolio_b = db_validation_service.get_portfolio_valuation_data(fund_name, source_b, date_b)
        else:
            portfolio_b = portfolio_a
            
    if dividend_b is None:
        if date_b != date_a or source_b != source_a:
            dividend_b = db_validation_service.get_dividend_data(fund_name, source_b, date_b)
        else:
            dividend_b = dividend_a
    
    if not trial_balance_a and not portfolio_a:
        return [_create_error_validation('No database data found', 'Data Availability')]
    
    # DYNAMIC ROUTING: Group NAV KPIs by category and route to appropriate validation modules
    kpi_categories = {}
    for kpi in nav_kpis:
        category = kpi.get('category', '-')
        if category not in kpi_categories:
            kpi_categories[category] = []
        kpi_categories[category].append(kpi)
    
    # Route to category-specific validation functions
    for category, category_kpis in kpi_categories.items():
        try:
            if category == 'Pricing':
                # Import and call pricing validations
                from clients.validusDemo.customFunctions.kpi_validations.pricing_validations import pricing_validations
                validations.extend(pricing_validations(portfolio_a, portfolio_b, category_kpis, fund_id, is_dual_source))
                
            elif category == 'Positions':
                # Import and call position validations
                from clients.validusDemo.customFunctions.kpi_validations.position_validations import position_validations
                validations.extend(position_validations(portfolio_a, portfolio_b, category_kpis, fund_id, is_dual_source))
                
            elif category == 'Market Value':
                # Import and call market value validations
                from clients.validusDemo.customFunctions.kpi_validations.market_value_validations import market_value_validations
                validations.extend(market_value_validations(trial_balance_a, trial_balance_b, portfolio_a, portfolio_b, category_kpis, fund_id, is_dual_source))
                
            elif category == 'Trading I&E':
                # Import and call trading I&E validations
                from clients.validusDemo.customFunctions.kpi_validations.trading_ie_validations import trading_ie_validations
                # For swap financing and interest accruals, we need trial balance data
                # For dividends, we need dividend data - pass both datasets
                validations.extend(trading_ie_validations(trial_balance_a, trial_balance_b, category_kpis, fund_id, is_dual_source, dividend_a, dividend_b))
                
            elif category == 'Expenses':
                # Import and call expense validations
                from clients.validusDemo.customFunctions.kpi_validations.expense_validations import expense_validations
                validations.extend(expense_validations(trial_balance_a, trial_balance_b, category_kpis, fund_id, is_dual_source))
                
            elif category == 'Fees':
                # Import and call dynamic fee validations
                from clients.validusDemo.customFunctions.kpi_validations.fee_validations import fee_validations
                validations.extend(fee_validations(trial_balance_a, trial_balance_b, category_kpis, fund_id, is_dual_source))
                    
        except Exception as e:
            # Handle import/execution errors gracefully
            error_validation = _create_error_validation(f'Error in {category} validations: {str(e)}', category)
            validations.append(error_validation)
    
    return validations


# DEPRECATED: Old hardcoded validation functions removed - now using dynamic KPI-based system


def _perform_cascading_price_validations(portfolio_a: List[Dict], portfolio_b: List[Dict], 
                                       price_kpis: List[Dict], fund_id: Optional[int] = None) -> List[Any]:
    """
    DEPRECATED: Cascading price validations function - no longer used
    This function is deprecated in favor of direct validations without cascading
    Priority: Missing/Null Price -> Unchanged Price -> Major Price Change
    Only includes KPIs that have threshold values in KPI Threshold table
    """
    validations = []
    
    # Create lookup for portfolio_a
    portfolio_a_lookup = {item.get('Inv Id'): item for item in portfolio_a}
    
    # Track which entities have been categorized to avoid duplicates
    categorized_entities = set()
    
    # 1. FIRST PRIORITY: Missing Price Check (New Logic: end_local_market_price is NULL AND end_qty != 0)
    # Only check ending period (portfolio_b) as start period was checked in prior month
    missing_failed, missing_passed, _ = missing_price_null_check(portfolio_b, 'Inv Id', portfolio_a)
    
    # Track categorized entities for missing prices
    for item in missing_failed:
        categorized_entities.add(item['identifier'])
    
    # Create Missing Price validation
    if missing_failed or missing_passed:
        validation_data = {
            'count': len(missing_failed),
            'total_checked': len(missing_failed) + len(missing_passed),
            'passed_count': len(missing_passed),
            'validation_source': 'default',
            'failed_items': missing_failed,
            'passed_items': missing_passed
        }
        validation = (VALIDATION_STATUS()
                     .setProductName('validus')
                     .setType('PnL')
                     .setSubType('Pricing')
                     .setSubType2('Missing Price')
                     .setMessage(1 if len(missing_failed) > 0 else 0)
                     .setData(validation_data))
        validations.append(validation)
    
    # 3. THIRD PRIORITY: Unchanged Price Check
    unchanged_failed = []
    unchanged_passed = []
    
    for item_b in portfolio_b:
        identifier = item_b.get('Inv Id', '-')
        
        # Skip if already categorized
        if identifier in categorized_entities:
            continue
            
        item_a = portfolio_a_lookup.get(identifier)
        if item_a:
            try:
                price_a = float(item_a.get('End Local Market Price', 0))
                price_b = float(item_b.get('End Local Market Price', 0))
                
                if price_a == price_b:
                    unchanged_failed.append({
                        'identifier': identifier,
                        'field': 'End Local Market Price',
                        'value_a': price_a,
                        'value_b': price_b,
                        'issue': 'unchanged_value',
                        'raw_data_a': item_a,
                        'raw_data_b': item_b
                    })
                    categorized_entities.add(identifier)
                else:
                    unchanged_passed.append({
                        'identifier': identifier,
                        'field': 'End Local Market Price',
                        'value_a': price_a,
                        'value_b': price_b,
                        'issue': 'unchanged_value',
                        'raw_data_a': item_a,
                        'raw_data_b': item_b
                    })
            except (ValueError, TypeError):
                continue
    
    # Create Unchanged Price validation
    if unchanged_failed or unchanged_passed:
        validation_data = {
            'count': len(unchanged_failed),
            'total_checked': len(unchanged_failed) + len(unchanged_passed),
            'passed_count': len(unchanged_passed),
            'validation_source': 'default',
            'failed_items': unchanged_failed,
            'passed_items': unchanged_passed
        }
        validation = (VALIDATION_STATUS()
                     .setProductName('validus')
                     .setType('PnL')
                     .setSubType('Pricing')
                     .setSubType2('Unchanged Price')
                     .setMessage(1 if len(unchanged_failed) > 0 else 0)
                     .setData(validation_data))
        validations.append(validation)
    
    # 4. KPI-DRIVEN VALIDATIONS: Only for remaining uncategorized entities with thresholds
    # Get pricing-related KPIs (Pricing category - title case)
    service = db_validation_service if hasattr(db_validation_service, 'get_active_kpis') else db_validation_service.DatabaseValidationService()
    all_pricing_kpis = service.get_active_kpis(category='Pricing')
    
    for kpi in all_pricing_kpis:
        threshold = service.get_kpi_threshold(kpi['id'], fund_id)
        if threshold is None:
            continue
            
        kpi_code = kpi.get('kpi_code', '').lower()
        kpi_name = kpi.get('kpi_name', '')
        
        # Map KPI codes to proper hierarchy
        level1, level2, subtype2 = _get_validation_hierarchy(kpi)
        
        if kpi_code == 'major_price_change':
            # Major Price Change validation - check all non-CASH/CASHF entities
            major_failed = []
            major_passed = []
            precision_type = kpi.get('precision_type', 'PERCENTAGE')  # Default to percentage
            
            for item_b in portfolio_b:
                identifier = item_b.get('Inv Id', '-')
                
                # Filter to only non-CASH/CASHF investment types (as per business rule)
                inv_type = item_b.get('Inv Type', '').upper()
                if inv_type in ['CASH', 'CASHF']:
                    continue
                
                # For portfolio validations, use Description from extra_data as display identifier
                display_identifier = identifier
                if 'extra_data' in item_b and item_b['extra_data']:
                    try:
                        import json
                        extra_data = json.loads(item_b['extra_data'])
                        if 'description' in extra_data:
                            display_identifier = extra_data['description']
                    except (json.JSONDecodeError, TypeError):
                        pass
                    
                item_a = portfolio_a_lookup.get(identifier)
                if item_a:
                    try:
                        price_a = float(item_a.get('End Local Market Price', 0))
                        price_b = float(item_b.get('End Local Market Price', 0))
                        
                        # Calculate change based on precision_type
                        if precision_type == 'ABSOLUTE':
                            # Calculate absolute change
                            change_value = abs(price_b - price_a)
                            is_threshold_exceeded = change_value > threshold
                        else:  # PERCENTAGE
                        # Calculate percentage change
                            if price_a != 0:
                                change_value = abs((price_b - price_a) / price_a) * 100
                            else:
                                change_value = 999999.99 if price_b != 0 else 0  # Use large number instead of inf
                            is_threshold_exceeded = change_value > threshold
                        
                        if is_threshold_exceeded:
                            major_failed.append({
                                'identifier': display_identifier,
                                'inv_id': identifier,  # Keep original for tracking
                                'field': 'End Local Market Price',
                                'value_a': price_a,
                                'value_b': price_b,
                                'change_value': change_value,
                                'precision_type': precision_type,
                                'threshold': threshold,
                                'issue': 'major_price_change',
                                'raw_data_a': item_a,
                                'raw_data_b': item_b
                            })
                        else:
                            major_passed.append({
                                'identifier': display_identifier,
                                'inv_id': identifier,  # Keep original for tracking
                                'field': 'End Local Market Price',
                                'value_a': price_a,
                                'value_b': price_b,
                                'change_value': change_value,
                                'precision_type': precision_type,
                                'threshold': threshold,
                                'issue': 'major_price_change',
                                'raw_data_a': item_a,
                                'raw_data_b': item_b
                            })
                    except (ValueError, TypeError):
                        continue
            
            # Create Major Price Change validation
            if major_failed or major_passed:
                validation_data = {
                    'count': len(major_failed),
                    'total_checked': len(major_failed) + len(major_passed),
                    'passed_count': len(major_passed),
                    'threshold': threshold,
                    'precision_type': precision_type,
                    'kpi_code': kpi.get('kpi_code', ''),
                    'kpi_name': kpi.get('kpi_name', ''),
                    'kpi_id': kpi.get('id', ''),
                    'kpi_description': kpi.get('description', ''),
                    'failed_items': major_failed,
                    'passed_items': major_passed
                }
                validation = (VALIDATION_STATUS()
                             .setProductName('validus')
                             .setType(level1)
                             .setSubType(level2)
                             .setSubType2(subtype2)
                             .setMessage(1 if len(major_failed) > 0 else 0)
                             .setData(validation_data))
                validations.append(validation)
    return validations


def _get_validation_hierarchy(kpi: Dict) -> tuple:
    """
    Map KPI to proper validation hierarchy (Level1, Level2, SubType2)
    Based on actual KPI categories from database - NAV validations only
    """
    kpi_code = kpi.get('kpi_code', '').lower()
    category = kpi.get('category', '')  # Keep original case from database
    
    # Pricing KPIs -> PnL/Pricing
    if category == 'Pricing':
        if 'price' in kpi_code:
            # return from DB
            return 'PnL', kpi.get('category', ''), kpi.get('kpi_name', 'Major Price Change')

        elif 'fx' in kpi_code:
            return 'PnL', kpi.get('category', ''), kpi.get('kpi_name', 'Major FX Change')
    
    # Positions KPIs -> P&L/Positions/Major Position Changes with sub-categories  
    elif category == 'Positions':
        if 'trade' in kpi_code:
            return 'P&L', 'Positions', 'Major Position Changes'
        elif 'corp' in kpi_code or 'corporate' in kpi_code or 'action' in kpi_code:
            return 'P&L', 'Positions', 'Major Position Changes'
        else:
            # Default for other position KPIs
            return 'P&L', 'Positions', 'Major Position Changes'
    
    # Market Value KPIs -> PnL/Market Value
    elif category == 'Market Value':
        if 'mv' in kpi_code:
            return 'PnL', kpi.get('category', ''), kpi.get('kpi_name', 'Major MV Change')
    
    # Expenses KPIs -> Non-Trading/Expenses (ALL Expenses category KPIs)
    elif category == 'Expenses':
        # All expense KPIs should go to Non-Trading/Expenses regardless of their specific type
        return 'Non-Trading', kpi.get('category', ''), kpi.get('kpi_name', 'Unknown Expense')
    
    # Fees KPIs -> Non-Trading/Fees (Management Fees Change only)
    elif category == 'Fees':
        if 'management' in kpi_code:
            return 'Non-Trading', kpi.get('category', ''), kpi.get('kpi_name', 'Management Fees')
    
    # Trading I&E KPIs -> PnL/Trading I&E (Dividend validations)
    elif 'dividend' in kpi_code:
        return 'PnL', 'Trading I&E', 'Major Dividends'
    
    # Default mapping for unknown categories
    return 'PnL', 'Other', kpi.get('kpi_name', '-')


def _perform_default_price_validations(portfolio_a: List[Dict], portfolio_b: List[Dict]) -> List[Any]:
    """
    Perform default price validations that are always checked (not in KPI library)
    """
    validations = []
    
    # 1. Missing Price Check (New Logic: end_local_market_price is NULL AND end_qty != 0)
    # Only check ending period (portfolio_b) as start period was checked in prior month
    failed_items_b, passed_items_b, _ = missing_price_null_check(portfolio_b, 'Inv Id', portfolio_a)
        
    if failed_items_b or passed_items_b:
        validation_data = {
            'count': len(failed_items_b),
            'total_checked': len(failed_items_b) + len(passed_items_b),
            'passed_count': len(passed_items_b),
            'validation_source': 'default',
            'kpi_description': 'For the current period for the securities where quantity is not equal to zero, but the local market price is NULL, then it will be shown as an exception',
            'failed_items': failed_items_b,
            'passed_items': passed_items_b
        }
        validation = (VALIDATION_STATUS()
                     .setProductName('validus')
                     .setType('PnL')
                     .setSubType('Pricing')
                     .setSubType2('Missing Price')
                     .setMessage(1 if len(failed_items_b) > 0 else 0)
                     .setData(validation_data))
        validations.append(validation)
    
    # 2. Unchanged Price Check (Stale Price Detection)
    failed_items_unchanged, passed_items_unchanged, _ = unchanged_value_check(
        portfolio_a, portfolio_b, 'End Local Market Price'
    )
    
    if failed_items_unchanged or passed_items_unchanged:
        validation_data = {
            'count': len(failed_items_unchanged),
            'total_checked': len(failed_items_unchanged) + len(passed_items_unchanged),
            'passed_count': len(passed_items_unchanged),
            'validation_source': 'default',
            'kpi_description': 'For each security local market price is compared between 2 periods & if it will exceed the threshold of 0%, then it will be shown as an exception',
            'failed_items': failed_items_unchanged,
            'passed_items': passed_items_unchanged
        }
        validation = (VALIDATION_STATUS()
                     .setProductName('validus')
                     .setType('PnL')
                     .setSubType('Pricing')
                     .setSubType2('Unchanged Price')
                     .setMessage(1 if len(failed_items_unchanged) > 0 else 0)
                     .setData(validation_data))
        validations.append(validation)
    


    
    return validations


def _perform_kpi_price_validations(portfolio_a: List[Dict], portfolio_b: List[Dict], 
                                  price_kpis: List[Dict], fund_id: Optional[int] = None) -> List[Any]:
    """
    Perform KPI-based price validations (major price changes, FX changes)
    Uses the KPI validation modules for consistency
    """
    validations = []
    
    if not price_kpis:
        return validations
    
    service = db_validation_service
    
    for kpi in price_kpis:
        threshold = service.get_kpi_threshold(kpi['id'], fund_id)
        if threshold is None:
            continue
            
        kpi_code = kpi.get('kpi_code', '').lower()
        
        if kpi_code == 'major_price_change':
            # Import the validation function from KPI modules
            from clients.validusDemo.customFunctions.kpi_validations.pricing_validations import major_price_changes_validation
            validation = major_price_changes_validation(portfolio_a, portfolio_b, threshold, kpi)
            validations.append(validation)
            
        elif kpi_code == 'major_fx_change':
            # Import the validation function from KPI modules
            from clients.validusDemo.customFunctions.kpi_validations.pricing_validations import major_fx_changes_validation
            validation = major_fx_changes_validation(portfolio_a, portfolio_b, threshold, kpi)
            validations.append(validation)
    
    return validations


def _perform_kpi_price_validations_optimized(portfolio_a: List[Dict], portfolio_b: List[Dict], 
                                           price_kpis: List[Dict]) -> List[Any]:
    """
    OPTIMIZED: Perform KPI-based price validations using pre-fetched thresholds
    Eliminates redundant database calls for threshold lookup
    """
    validations = []
    
    if not price_kpis:
        return validations
    
    for kpi in price_kpis:
        threshold = kpi.get('threshold')  # Already fetched and included
        if threshold is None:
            continue
            
        kpi_code = kpi.get('kpi_code', '').lower()
        
        if kpi_code == 'major_price_change':
            # Import the validation function from KPI modules
            from clients.validusDemo.customFunctions.kpi_validations.pricing_validations import major_price_changes_validation
            validation = major_price_changes_validation(portfolio_a, portfolio_b, threshold, kpi)
            validations.append(validation)
            
        elif kpi_code == 'major_fx_change':
            # Import the validation function from KPI modules
            from clients.validusDemo.customFunctions.kpi_validations.pricing_validations import major_fx_changes_validation
            validation = major_fx_changes_validation(portfolio_a, portfolio_b, threshold, kpi)
            validations.append(validation)
    
    return validations


def _legacy_position_change_validations(portfolio_a: List[Dict], portfolio_b: List[Dict], 
                              nav_kpis: List[Dict], fund_id: Optional[int] = None) -> List[Any]:
    """
    DEPRECATED: Legacy position change validations - renamed to avoid conflicts with KPI-based system
    Calculate position change validations - direct validations without cascading
    Includes: Missing Position, Zero Quantity, Large Trades
    """
    validations = []
    
    if not portfolio_a or not portfolio_b:
        return validations
    
    # Get relevant KPIs
    position_kpis = [kpi for kpi in nav_kpis if 'position' in kpi.get('kpi_name', '').lower() or 'trade' in kpi.get('kpi_name', '').lower()]
    
    # Use direct validation logic for positions (no cascading)
    validations.extend(_perform_default_position_validations(portfolio_a, portfolio_b))
    
    # Add KPI-based validations for major position changes
    validations.extend(_perform_kpi_position_validations(portfolio_a, portfolio_b, position_kpis, fund_id))
    
    return validations


def _perform_kpi_position_validations(portfolio_a: List[Dict], portfolio_b: List[Dict], 
                                     position_kpis: List[Dict], fund_id: Optional[int] = None) -> List[Any]:
    """
    Perform KPI-based position validations (major position changes, large trades)
    Uses the KPI validation modules for consistency
    """
    validations = []
    
    if not position_kpis:
        return validations
    
    service = db_validation_service
    
    for kpi in position_kpis:
        threshold = service.get_kpi_threshold(kpi['id'], fund_id)
        if threshold is None:
            continue
            
        kpi_code = kpi.get('kpi_code', '').lower()
        
        if kpi_code == 'major_trades':
            # Import the validation function from KPI modules
            from clients.validusDemo.customFunctions.kpi_validations.position_validations import major_position_changes_validation
            validation = major_position_changes_validation(portfolio_a, portfolio_b, threshold, kpi)
            validations.append(validation)
    
    return validations


def _perform_cascading_position_validations(portfolio_a: List[Dict], portfolio_b: List[Dict], 
                                          position_kpis: List[Dict], fund_id: Optional[int] = None) -> List[Any]:
    """
    DEPRECATED: Cascading position validations function - no longer used
    This function is deprecated in favor of direct validations without cascading
    Priority: Missing Position -> Zero Quantity -> Large Trades
    """
    validations = []
    
    # Create lookup for portfolio_a
    portfolio_a_lookup = {item.get('Inv Id'): item for item in portfolio_a}
    
    # Track which entities have been categorized
    categorized_entities = set()
    
    # 1. FIRST PRIORITY: Missing Position Data
    missing_failed = []
    missing_passed = []
    
    for item_b in portfolio_b:
        identifier = item_b.get('Inv Id', '-')
        qty_b = item_b.get('End Qty')
        
        # Check if position data is missing
        is_missing = (
            qty_b is None or 
            qty_b == '' or 
            str(qty_b).lower() in ['nan', 'null', 'none'] or
            (isinstance(qty_b, float) and qty_b != qty_b)  # NaN check
        )
        
        if is_missing:
            missing_failed.append({
                'identifier': identifier,
                'field': 'End Qty',
                'value': qty_b,
                'issue': 'missing_position',
                'raw_data': item_b
            })
            categorized_entities.add(identifier)
        else:
            missing_passed.append({
                'identifier': identifier,
                'field': 'End Qty', 
                'value': qty_b,
                'issue': 'missing_position',
                'raw_data': item_b
            })
    
    # Create Missing Position validation (if any missing found)
    if missing_failed:
        validation_data = {
            'count': len(missing_failed),
            'total_checked': len(missing_failed) + len(missing_passed),
            'passed_count': len(missing_passed),
            'validation_source': 'default',
            'failed_items': missing_failed,
            'passed_items': missing_passed
        }
        validation = (VALIDATION_STATUS()
                     .setProductName('validus')
                     .setType('PnL')
                     .setSubType('Positions')
                     .setSubType2('Missing Position')
                     .setMessage(1)
                     .setData(validation_data))
        validations.append(validation)
    
    # 2. SECOND PRIORITY: Zero Quantity Check
    zero_failed = []
    zero_passed = []
    
    for item_b in portfolio_b:
        identifier = item_b.get('Inv Id', '-')
        
        # Skip if already categorized
        if identifier in categorized_entities:
            continue
            
        try:
            qty_b = float(item_b.get('End Qty', 0))
            
            if qty_b == 0.0:
                zero_failed.append({
                    'identifier': identifier,
                    'field': 'End Qty',
                    'value': qty_b,
                    'issue': 'zero_quantity',
                    'raw_data': item_b
                })
                categorized_entities.add(identifier)
            else:
                zero_passed.append({
                    'identifier': identifier,
                    'field': 'End Qty',
                    'value': qty_b,
                    'issue': 'zero_quantity',
                    'raw_data': item_b
                })
        except (ValueError, TypeError):
            continue
    
    # Create Zero Quantity validation (if any zero quantities found)
    if zero_failed:
        validation_data = {
            'count': len(zero_failed),
            'total_checked': len(zero_failed) + len(zero_passed),
            'passed_count': len(zero_passed),
            'validation_source': 'default',
            'failed_items': zero_failed,
            'passed_items': zero_passed
        }
        validation = (VALIDATION_STATUS()
                     .setProductName('validus')
                     .setType('PnL')
                     .setSubType('Positions')
                     .setSubType2('Zero Quantity')
                     .setMessage(1)
                     .setData(validation_data))
        validations.append(validation)
    
    # 3. KPI-DRIVEN VALIDATIONS: Only for remaining uncategorized entities
    for kpi in position_kpis:
        threshold = db_validation_service.get_kpi_threshold(kpi['id'], fund_id)
        if threshold is None:
            continue
            
        kpi_name = kpi['kpi_name'].lower()
        
    # 3. KPI-DRIVEN VALIDATIONS: Only for remaining uncategorized entities with thresholds
    # Get position-related KPIs (Positions category - title case)
    service = db_validation_service if hasattr(db_validation_service, 'get_active_kpis') else db_validation_service.DatabaseValidationService()
    trading_kpis = service.get_active_kpis(category='Positions')
    
    for kpi in trading_kpis:
        threshold = service.get_kpi_threshold(kpi['id'], fund_id)
        if threshold is None:
            continue
            
        kpi_code = kpi.get('kpi_code', '').lower()
        
        if kpi_code == 'major_trades':
            # Map KPI to proper hierarchy
            level1, level2, subtype2 = _get_validation_hierarchy(kpi)
            
            # Major Trades validation - only for uncategorized entities
            trade_failed = []
            trade_passed = []
            
            for item_b in portfolio_b:
                identifier = item_b.get('Inv Id', '-')
                
                # Skip if already categorized
                if identifier in categorized_entities:
                    continue
                    
                item_a = portfolio_a_lookup.get(identifier)
                if item_a:
                    try:
                        qty_a = float(item_a.get('End Qty', 0))
                        qty_b = float(item_b.get('End Qty', 0))
                        
                        # Calculate percentage change in position
                        if qty_a != 0:
                            percentage_change = abs((qty_b - qty_a) / qty_a)
                        else:
                            percentage_change = float('inf') if qty_b != 0 else 0
                        
                        if percentage_change > (threshold / 100.0):
                            trade_failed.append({
                                'identifier': identifier,
                                'field': 'End Qty',
                                'value_a': qty_a,
                                'value_b': qty_b,
                                'percentage_change': percentage_change * 100,
                                'threshold': threshold,
                                'issue': 'major_trade',
                                'raw_data_a': item_a,
                                'raw_data_b': item_b
                            })
                        else:
                            trade_passed.append({
                                'identifier': identifier,
                                'field': 'End Qty',
                                'value_a': qty_a,
                                'value_b': qty_b,
                                'percentage_change': percentage_change * 100,
                                'threshold': threshold,
                                'issue': 'major_trade',
                                'raw_data_a': item_a,
                                'raw_data_b': item_b
                            })
                    except (ValueError, TypeError):
                        continue
            
            # Create Major Trades validation
            if trade_failed or trade_passed:
                validation_data = {
                    'count': len(trade_failed),
                    'total_checked': len(trade_failed) + len(trade_passed),
                    'passed_count': len(trade_passed),
                    'threshold': threshold,
                    'kpi_code': kpi.get('kpi_code', ''),
                    'kpi_name': kpi.get('kpi_name', ''),
                    'kpi_id': kpi.get('id', ''),
                    'kpi_description': kpi.get('description', ''),
                    'precision_type': kpi.get('precision_type', 'PERCENTAGE'),
                    'subType3': 'By Trade',  # 4th level hierarchy
                    'failed_items': trade_failed,
                    'passed_items': trade_passed
                }
                validation = (VALIDATION_STATUS()
                             .setProductName('validus')
                             .setType(level1)
                             .setSubType(level2)
                             .setSubType2(subtype2)
                             .setMessage(1 if len(trade_failed) > 0 else 0)
                             .setData(validation_data))
                validations.append(validation)
    
    return validations


def _legacy_expense_validations(trial_balance_a: List[Dict], trial_balance_b: List[Dict], 
                       fund_id: Optional[int] = None, nav_kpis: List[Dict] = None) -> List[Any]:
    """
    DEPRECATED: Legacy expense validations - renamed to avoid conflicts with KPI-based system
    Calculate expense validations based on EXPENSES KPIs with thresholds using metrics-based approach
    Level 1: Non-Trading, Level 2: Expenses
    OPTIMIZED: Use pre-fetched KPI data and calculate metrics once
    """
    validations = []
    
    # OPTIMIZATION: Use pre-fetched KPIs if provided, otherwise fetch
    if nav_kpis:
        expense_kpis = [kpi for kpi in nav_kpis if kpi.get('category', '').lower() == 'expenses' and 'threshold' in kpi]
    else:
        # Fallback to original approach
        service = db_validation_service if hasattr(db_validation_service, 'get_active_kpis') else db_validation_service.DatabaseValidationService()
        expense_kpis = service.get_active_kpis(category='Expenses')
    
    # OPTIMIZATION: Calculate financial metrics once for both periods
    metrics_a = calculate_financial_metrics(trial_balance_a)
    metrics_b = calculate_financial_metrics(trial_balance_b)
    
    for kpi in expense_kpis:
        # OPTIMIZATION: Use pre-fetched threshold
        threshold = kpi.get('threshold') if 'threshold' in kpi else db_validation_service.get_kpi_threshold(kpi['id'], fund_id)
        if threshold is None:
            continue
            
        kpi_code = kpi.get('kpi_code', '').lower()
        level1, level2, subtype2 = _get_validation_hierarchy(kpi)
        
        # Get the metric field name from the KPI definition
        metric_field = kpi.get('numerator_field', '')
        if not metric_field:
            continue
            
        # Get metric values from financial metrics
        value_a = metrics_a.get(metric_field, 0.0)
        value_b = metrics_b.get(metric_field, 0.0)
        
        # Calculate change based on precision_type from KPI library
        failed_items = []
        passed_items = []
        precision_type = kpi.get('precision_type', 'PERCENTAGE')  # Default to percentage
        
        if value_a is not None and value_b is not None:
            if precision_type == 'ABSOLUTE':
                # Calculate absolute change
                change_value = abs(value_b - value_a)
                # Calculate signed percentage change for display using formula (B-A)/|A|
                if value_a != 0:
                    signed_percentage_change = ((value_b - value_a) / abs(value_a)) * 100
                else:
                    signed_percentage_change = 100 if value_b > 0 else (-100 if value_b < 0 else 0)
                is_threshold_exceeded = change_value > threshold
            else:  # PERCENTAGE
                # Calculate percentage change using formula (B-A)/|A|
                if value_a != 0:
                    change_value = abs((value_b - value_a) / abs(value_a)) * 100
                    signed_percentage_change = ((value_b - value_a) / abs(value_a)) * 100
                else:
                    change_value = float('inf') if value_b != 0 else 0
                    signed_percentage_change = 100 if value_b > 0 else (-100 if value_b < 0 else 0)
                is_threshold_exceeded = change_value > threshold
            
            # Get extra_data from trial balance for this expense type
            extra_data_children = []
            
            # Map metric fields to financial account keywords
            expense_account_mapping = {
                'legal_fees': 'legal',
                'admin_fees': 'administration',  # Changed to match "Fund Administration Fees"
                'other_admin_expenses': 'other admin',
                'interest_expense': 'interest expense',
                'accounting_expenses': 'accounting',
                'allocation_fee': 'allocation',
                'audit_expense': 'audit',
                'bank_fees': 'bank fees',
                'borrow_fee_estimate': 'borrowfeeestimate',
                'borrow_fee_expense': 'borrowfeeexpense',
                'distribution_fee_expense': 'distributionfeeexpense',
                'fs_prep_fees': 'fsprepfees',
                'fund_expense': 'fund expense',
                'stockloan_fees': 'stockloan',
                'tax_preparation_fees': 'tax preparation'
            }
            
            # Get the account keyword for this metric
            account_keyword = expense_account_mapping.get(metric_field, metric_field)
            
            # Collect breakdown data from both periods
            children_data_a = {}  # period A breakdown by transaction description
            children_data_b = {}  # period B breakdown by transaction description
            
            # Process period A trial balance
            for record in trial_balance_a:
                # Handle both ORM objects and dictionaries
                if hasattr(record, 'type'):  # ORM object
                    record_type = record.type
                    financial_account = record.financial_account
                    extra_data = record.extra_data
                else:  # Dictionary
                    record_type = record.get('Type')
                    financial_account = record.get('Financial Account', '')
                    extra_data = record.get('extra_data')
                
                # Check if this trial balance record is related to this expense type
                if (record_type == 'Expense' and 
                    extra_data and
                    account_keyword.lower() in financial_account.lower()):
                    
                    try:
                        import json
                        extra_data_json = json.loads(extra_data)
                        
                        # Check if there's general_ledger data
                        if 'general_ledger' in extra_data_json:
                            for transaction in extra_data_json['general_ledger']:
                                tran_desc = transaction.get('tran_description', '')
                                local_amount = transaction.get('local_amount', 0)
                                if tran_desc:
                                    children_data_a[tran_desc] = {
                                        'local_amount': local_amount,
                                        'gl_account': financial_account
                                    }
                    except (json.JSONDecodeError, TypeError):
                        continue
            
            # Process period B trial balance
            for record in trial_balance_b:
                # Handle both ORM objects and dictionaries
                if hasattr(record, 'type'):  # ORM object
                    record_type = record.type
                    financial_account = record.financial_account
                    extra_data = record.extra_data
                else:  # Dictionary
                    record_type = record.get('Type')
                    financial_account = record.get('Financial Account', '')
                    extra_data = record.get('extra_data')
                
                # Check if this trial balance record is related to this expense type
                if (record_type == 'Expense' and 
                    extra_data and
                    account_keyword.lower() in financial_account.lower()):
                    
                    try:
                        import json
                        extra_data_json = json.loads(extra_data)
                        
                        # Check if there's general_ledger data
                        if 'general_ledger' in extra_data_json:
                            for transaction in extra_data_json['general_ledger']:
                                tran_desc = transaction.get('tran_description', '')
                                local_amount = transaction.get('local_amount', 0)
                                if tran_desc:
                                    children_data_b[tran_desc] = {
                                        'local_amount': local_amount,
                                        'gl_account': financial_account
                                    }
                    except (json.JSONDecodeError, TypeError):
                        continue
            
            # Combine data from both periods to create children
            all_transaction_keys = set(children_data_a.keys()) | set(children_data_b.keys())
            
            for tran_desc in all_transaction_keys:
                data_a = children_data_a.get(tran_desc, {})
                data_b = children_data_b.get(tran_desc, {})
                
                source_a_value = data_a.get('local_amount', 0) if data_a else 0
                source_b_value = data_b.get('local_amount', 0) if data_b else 0
                gl_account = data_a.get('gl_account') or data_b.get('gl_account', financial_account)
                
                extra_data_children.append({
                    'transaction_description': tran_desc,
                    'source_a_value': source_a_value,
                    'source_b_value': source_b_value,
                    'gl_account': gl_account,
                    'type': 'expense_detail'
                })
            
            item_data = {
                'identifier': metric_field,
                'field': kpi.get('kpi_name', metric_field),
                'value_a': value_a,
                'value_b': value_b,
                'change_value': change_value,
                'percentage_change': signed_percentage_change,
                'precision_type': precision_type,
                'threshold': threshold,
                'issue': 'expense_change',
                'extra_data_children': extra_data_children  # Add children data for expenses
            }
            
            
            if is_threshold_exceeded:
                failed_items.append(item_data)
            else:
                passed_items.append(item_data)
        
        # Create validation
        validation_data = {
            'count': len(failed_items),
            'total_checked': len(failed_items) + len(passed_items),
            'passed_count': len(passed_items),
            'threshold': threshold,
            'precision_type': precision_type,
            'kpi_code': kpi.get('kpi_code', ''),
            'kpi_name': kpi.get('kpi_name', ''),
            'kpi_id': kpi.get('id', ''),
            'kpi_description': kpi.get('description', ''),
            'failed_items': failed_items,
            'passed_items': passed_items
        }
        validation = (VALIDATION_STATUS()
                     .setProductName('validus')
                     .setType(level1)
                     .setSubType(level2)
                     .setSubType2(subtype2)
                     .setMessage(1 if len(failed_items) > 0 else 0)
                     .setData(validation_data))
        validations.append(validation)
    
    return validations


def _legacy_fee_validations(trial_balance_a: List[Dict], trial_balance_b: List[Dict], 
                   fund_id: Optional[int] = None, nav_kpis: List[Dict] = None,
                   is_dual_source: bool = False) -> List[Any]:
    """
    DEPRECATED: Legacy fee validations - renamed to avoid conflicts with KPI-based system
    Calculate fee validations based on FEES KPIs with thresholds
    Level 1: Non-Trading, Level 2: Fees
    OPTIMIZED: Use pre-fetched KPI data
    """
    validations = []
    
    
    # OPTIMIZATION: Use pre-fetched KPIs if provided, otherwise fetch
    if nav_kpis:
        fee_kpis = [kpi for kpi in nav_kpis if kpi.get('category', '').lower() == 'fees']
    else:
        # Fallback to original approach
        service = db_validation_service if hasattr(db_validation_service, 'get_active_kpis') else db_validation_service.DatabaseValidationService()
        fee_kpis = service.get_active_kpis(category='Fees')
    
    for kpi in fee_kpis:
        try:
            # Get KPI configuration
            kpi_id = kpi.get('id')
            kpi_name = kpi.get('kpi_name', 'Unknown Fee')
            numerator_field = kpi.get('numerator_field', '')
            
            # Get threshold from database service
            threshold = db_validation_service.get_kpi_threshold(kpi_id, fund_id)
            if threshold is None or not numerator_field:
                continue
                
        except Exception as e:
            print(f"Error processing fee KPI {kpi.get('id', '-')}: {str(e)}")
            continue
            
        # Use financial metrics for consistent fee calculation
        from clients.validusDemo.customFunctions.financial_metrics import calculate_financial_metrics
        
        # Calculate metrics for both periods/sources using existing financial metrics
        # Handle cases where one dataset might be empty (especially for dual-source)
        metrics_a = calculate_financial_metrics(trial_balance_a or [])
        metrics_b = calculate_financial_metrics(trial_balance_b or [])
        
        # Get the specific fee metric values
        value_a = metrics_a.get(numerator_field, 0.0)
        value_b = metrics_b.get(numerator_field, 0.0)
        
        # For dual-source, we compare values between sources; for single-source, we look at changes over time
        if is_dual_source:
            # Cross-source comparison: check if fee amounts match between sources
            comparison_type = "Cross-source discrepancy"
        else:
            # Period comparison: check if fee amounts changed significantly over time
            comparison_type = "Period-over-period change"
        
        # Calculate change based on precision_type from KPI library
        failed_items = []
        passed_items = []
        precision_type = kpi.get('precision_type', 'PERCENTAGE')
        
        if value_a is not None and value_b is not None:
            if precision_type == 'ABSOLUTE':
                if is_dual_source:
                    # For dual-source: Calculate absolute difference between sources
                    change_value = abs(value_b - value_a)
                else:
                    # For single-source: Calculate absolute change over time
                    change_value = abs(value_b - value_a)
                is_threshold_exceeded = change_value > threshold
            else:  # PERCENTAGE
                if is_dual_source:
                    # For dual-source: Calculate percentage difference between sources
                    if value_a != 0:
                        change_value = abs(value_b - value_a) / value_a * 100
                    elif value_b != 0:
                        change_value = 100  # One source has value, other doesn't
                    else:
                        change_value = 0  # Both sources have zero value
                else:
                    # For single-source: Calculate percentage change over time
                    if value_a != 0:
                        change_value = abs((value_b - value_a) / value_a) * 100
                    else:
                        change_value = 100 if value_b != 0 else 0
                is_threshold_exceeded = change_value > threshold
            
        # Calculate tooltip (opposite precision type) with consistent 3 decimal places
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
            
            # Create a simple fee validation item (similar to expense validations)
            # Use the same validation format as expenses for consistency
            fee_item = {
                'security': kpi_name,
                'subType': kpi_name,
                'subType2': '',
                'tooltipInfo': tooltip_format,
                'precision_type': precision_type,
                'validation_precision_type': precision_type,
                'isEditable': False,
                'isRemarkOnlyEditable': False,
                'extra_data_children': [],  # Simplified for now
                'value_a': value_a,
                'value_b': value_b,
                'change': change_value,  # Frontend expects 'change' field
                'change_value': change_value,  # Keep for compatibility
                'tooltip_change': tooltip_format,  # Pre-calculated tooltip
                'is_failed': bool(is_threshold_exceeded),
                'threshold_exceeded': bool(is_threshold_exceeded)
            }

            if is_threshold_exceeded:
                failed_items.append(fee_item)
            else:
                passed_items.append(fee_item)
        
        # Create validation result using the same format as expense validations
        from clients.validusDemo.customFunctions.validation_utils import create_detailed_validation_result
        
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
        
        validations.append(validation)
    
    return validations


def _get_trial_balance_value(trial_balance_data: List[Dict], accounting_head: str) -> Optional[float]:
    """
    Get ending balance value for a specific accounting head from trial balance data
    """
    for item in trial_balance_data:
        if item.get('Accounting Head') == accounting_head:
            try:
                return float(item.get('Ending Balance', 0))
            except (ValueError, TypeError):
                return None
    return None


def _legacy_market_value_validations(trial_balance_a: List[Dict], trial_balance_b: List[Dict],
                           portfolio_a: List[Dict], portfolio_b: List[Dict],
                           nav_kpis: List[Dict], fund_id: Optional[int] = None) -> List[Any]:
    """
    DEPRECATED: Legacy market value validations - renamed to avoid conflicts with KPI-based system
    Calculate market value validations based on MARKET VALUE KPIs with thresholds
    Level 1: PnL, Level 2: Market Value
    """
    validations = []
    
    if not portfolio_a or not portfolio_b:
        return validations
    
    # Get service instance and market value KPIs
    service = db_validation_service if hasattr(db_validation_service, 'get_active_kpis') else db_validation_service.DatabaseValidationService()
    mv_kpis = service.get_active_kpis(category='Market Value')
    
    for kpi in mv_kpis:
        threshold = service.get_kpi_threshold(kpi['id'], fund_id)
        if threshold is None:
            continue
            
        kpi_code = kpi.get('kpi_code', '').lower()
        level1, level2, subtype2 = _get_validation_hierarchy(kpi)
        
        if kpi_code == 'major_mv_change':
            # Calculate major MV changes
            mv_failed = []
            mv_passed = []
            precision_type = kpi.get('precision_type', 'PERCENTAGE')  # Default to percentage
            
            # Create lookup for portfolio_a using composite identifiers
            from .validation_utils import _create_composite_identifier
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
                
                # Use description for display, fallback to inv_id
                display_identifier = description if description else inv_id
                
                try:
                    mv_a = float(item_a.get('End Book MV', 0))
                    mv_b = float(item_b.get('End Book MV', 0))
                    
                    # Calculate change based on precision type
                    if precision_type == 'ABSOLUTE':
                        change_value = abs(mv_b - mv_a)
                        # Calculate signed percentage change for display using formula (B-A)/|A|
                        if mv_a != 0:
                            signed_percentage_change = ((mv_b - mv_a) / abs(mv_a)) * 100
                        else:
                            signed_percentage_change = 100 if mv_b > 0 else (-100 if mv_b < 0 else 0)
                    else:  # PERCENTAGE
                        if mv_a != 0:
                            change_value = abs((mv_b - mv_a) / abs(mv_a)) * 100
                            # Calculate signed percentage change using formula (B-A)/|A|
                            signed_percentage_change = ((mv_b - mv_a) / abs(mv_a)) * 100
                        else:
                            change_value = float('inf') if mv_b != 0 else 0
                            signed_percentage_change = 100 if mv_b > 0 else (-100 if mv_b < 0 else 0)
                    
                    # For absolute threshold, compare absolute change; for percentage threshold, compare percentage change
                    if change_value > threshold:
                        mv_failed.append({
                            'identifier': display_identifier,  # Use description for display
                            'inv_id': inv_id,  # Keep original ID for tracking
                            'description': description,
                            'field': 'End Book MV',
                            'value_a': mv_a,
                            'value_b': mv_b,
                            'change_value': change_value,
                            'percentage_change': signed_percentage_change,
                            'threshold': threshold,
                            'precision_type': precision_type,
                            'issue': 'major_mv_change',
                            'raw_data_a': item_a,
                            'raw_data_b': item_b
                        })
                    else:
                        mv_passed.append({
                            'identifier': display_identifier,  # Use description for display
                            'inv_id': inv_id,  # Keep original ID for tracking
                            'description': description,
                            'field': 'End Book MV',
                            'value_a': mv_a,
                            'value_b': mv_b,
                            'change_value': change_value,
                            'percentage_change': signed_percentage_change,
                            'threshold': threshold,
                            'precision_type': precision_type,
                            'issue': 'major_mv_change',
                            'raw_data_a': item_a,
                            'raw_data_b': item_b
                        })
                except (ValueError, TypeError):
                    # Skip items with invalid numeric values
                    continue
            
            
            # Create Major MV Change validation
            if mv_failed or mv_passed:
                validation_data = {
                    'count': len(mv_failed),
                    'total_checked': len(mv_failed) + len(mv_passed),
                    'passed_count': len(mv_passed),
                    'threshold': threshold,
                    'precision_type': precision_type,
                    'kpi_code': kpi.get('kpi_code', ''),
                    'kpi_name': kpi.get('kpi_name', ''),
                    'kpi_id': kpi.get('id', ''),
                    'kpi_description': kpi.get('description', ''),
                    'failed_items': mv_failed,
                    'passed_items': mv_passed
                }
                validation = (VALIDATION_STATUS()
                             .setProductName('validus')
                             .setType(level1)
                             .setSubType(level2)
                             .setSubType2(subtype2)
                             .setMessage(1 if len(mv_failed) > 0 else 0)
                             .setData(validation_data))
                validations.append(validation)
    
    return validations


def _calculate_stale_prices(merged_df: pd.DataFrame) -> int:
    """Calculate number of stale prices"""
    if merged_df.empty:
        return 0
    
    # Check for identical prices between periods
    stale_mask = (
        (merged_df['End Local Market Price_a'] == merged_df['End Local Market Price_b']) &
        (merged_df['End Qty_a'] == merged_df['End Qty_b']) &
        (merged_df['End Local Market Price_a'].notna()) &
        (merged_df['End Local Market Price_b'].notna())
    )
    
    return int(stale_mask.sum())


def _calculate_missing_prices(merged_df: pd.DataFrame) -> int:
    """Calculate number of missing prices"""
    if merged_df.empty:
        return 0
    
    # Check for missing prices
    missing_mask = (
        (merged_df['End Local Market Price_a'].isna()) |
        (merged_df['End Local Market Price_b'].isna())
    )
    
    return int(missing_mask.sum())


def _calculate_major_price_changes(merged_df: pd.DataFrame, threshold: float) -> int:
    """Calculate number of major price changes"""
    if merged_df.empty:
        return 0
    
    # Calculate price change percentage
    valid_mask = (
        (merged_df['End Local Market Price_a'] > 0) &
        (merged_df['End Local Market Price_b'] > 0) &
        (merged_df['End Local Market Price_a'].notna()) &
        (merged_df['End Local Market Price_b'].notna())
    )
    
    price_change = abs(
        (merged_df['End Local Market Price_b'] - merged_df['End Local Market Price_a']) /
        merged_df['End Local Market Price_a']
    )
    
    major_change_mask = valid_mask & (price_change > threshold)
    
    return int(major_change_mask.sum())


def _calculate_large_trades(merged_df: pd.DataFrame, threshold: float) -> int:
    """Calculate number of large trades"""
    if merged_df.empty:
        return 0
    
    # Calculate quantity change
    qty_change = abs(merged_df['End Qty_b'].fillna(0) - merged_df['End Qty_a'].fillna(0))
    
    # Check for large trades based on threshold
    base_qty = merged_df[['End Qty_a', 'End Qty_b']].abs().max(axis=1)
    
    large_trade_mask = (base_qty > 0) & ((qty_change / base_qty) > threshold)
    
    return int(large_trade_mask.sum())


def _calculate_position_changes(merged_df: pd.DataFrame, threshold: float) -> int:
    """Calculate number of significant position changes"""
    if merged_df.empty:
        return 0
    
    # Calculate market value change
    mv_change = abs(merged_df['End Book MV_b'].fillna(0) - merged_df['End Book MV_a'].fillna(0))
    
    # Check for position changes based on threshold
    base_mv = merged_df[['End Book MV_a', 'End Book MV_b']].abs().max(axis=1)
    
    position_change_mask = (base_mv > 0) & ((mv_change / base_mv) > threshold)
    
    return int(position_change_mask.sum())


def _calculate_mv_changes(portfolio_a: List[Dict], portfolio_b: List[Dict], threshold: float) -> int:
    """Calculate number of major market value changes"""
    if not portfolio_a or not portfolio_b:
        return 0
    
    df_a = pd.DataFrame(portfolio_a)
    df_b = pd.DataFrame(portfolio_b)
    
    total_mv_a = df_a['End Book MV'].sum()
    total_mv_b = df_b['End Book MV'].sum()
    
    if total_mv_a == 0:
        return 0 if total_mv_b == 0 else 1
    
    mv_change = abs((total_mv_b - total_mv_a) / total_mv_a)
    
    return 1 if mv_change > threshold else 0


def _calculate_nav_change(trial_balance_a: List[Dict], trial_balance_b: List[Dict], threshold: float) -> int:
    """Calculate NAV change validation"""
    if not trial_balance_a or not trial_balance_b:
        return 0
    
    df_a = pd.DataFrame(trial_balance_a)
    df_b = pd.DataFrame(trial_balance_b)
    
    # Calculate NAV from trial balance (sum of ending balances)
    nav_a = df_a['ending_balance'].sum()
    nav_b = df_b['ending_balance'].sum()
    
    if nav_a == 0:
        return 0 if nav_b == 0 else 1
    
    nav_change = abs((nav_b - nav_a) / nav_a)
    
    return 1 if nav_change > threshold else 0


def _create_validation(validation_type: str, sub_type: str, sub_type2: str, 
                      count: int, threshold: float, kpi: Dict) -> Any:
    """Create a validation status object"""
    message = 1 if count > 0 else 0
    
    return (VALIDATION_STATUS()
            .setProductName('validus')
            .setType(validation_type)
            .setSubType(sub_type)
            .setSubType2(sub_type2)
            .setMessage(message)
            .setData({
                'count': count,
                'threshold': threshold,
                'kpi_code': kpi.get('kpi_code'),
                'kpi_name': kpi.get('kpi_name'),
                'kpi_id': kpi.get('id'),
                'kpi_description': kpi.get('description', '')
            }))


def fx_change_validations(portfolio_a: List[Dict], portfolio_b: List[Dict], 
                         nav_kpis: List[Dict], fund_id: Optional[int] = None) -> List[Any]:
    """
    Major FX Change validation: Compare 'End Local Market Price' for CASHF positions
    between two periods and flag if price difference exceeds threshold
    """
    validations = []
    
    # Get FX change KPIs
    fx_kpis = [kpi for kpi in nav_kpis if 'fx' in kpi.get('kpi_code', '').lower() and kpi.get('category', '') == 'Pricing']
    
    if not portfolio_a or not portfolio_b or not fx_kpis:
        return validations
    
    
    for kpi in fx_kpis:
        threshold = db_validation_service.get_kpi_threshold(kpi['id'], fund_id)
        if threshold is None:
            continue
        
        try:
            # Convert to DataFrames for easier processing
            portfolio_df_a = pd.DataFrame(portfolio_a)
            portfolio_df_b = pd.DataFrame(portfolio_b)
            
            # Filter for CASHF (cash/FX) positions only
            cashf_positions_a = portfolio_df_a[portfolio_df_a['Inv Type'] == 'CASHF'].copy()
            cashf_positions_b = portfolio_df_b[portfolio_df_b['Inv Type'] == 'CASHF'].copy()
            
            if cashf_positions_a.empty or cashf_positions_b.empty:
                continue
            
            
            # Compare End Local Market Price for matching positions
            failed_items = []
            passed_items = []
            total_fx_exceptions = 0
            
            for _, pos_a in cashf_positions_a.iterrows():
                inv_id = pos_a['Inv Id']
                price_a = pos_a.get('End Local Market Price', 0)
                
                # Find matching position in period B
                matching_pos_b = cashf_positions_b[cashf_positions_b['Inv Id'] == inv_id]
                
                if not matching_pos_b.empty:
                    price_b = matching_pos_b.iloc[0].get('End Local Market Price', 0)
                    
                    # Calculate percentage change
                    if price_a != 0:
                        price_change_pct = abs((price_b - price_a) / price_a) * 100
                    else:
                        price_change_pct = 100 if price_b != 0 else 0
                    
                    validation_item = {
                        'identifier': inv_id,
                        'field': 'fx_rate_change',
                        'price_a': price_a,
                        'price_b': price_b,
                        'change_percent': price_change_pct,
                        'threshold_percent': threshold,
                        'raw_data_a': pos_a.to_dict(),
                        'raw_data_b': matching_pos_b.iloc[0].to_dict()
                    }
                    
                    # Check if change exceeds threshold
                    if price_change_pct > threshold:
                        failed_items.append(validation_item)
                        total_fx_exceptions += 1
                    else:
                        passed_items.append(validation_item)
            
            # Create validation result based on hierarchy mapping
            level1, level2, subtype2 = _get_validation_hierarchy(kpi)
            
            validation = (VALIDATION_STATUS()
                         .setProductName('validus')
                         .setType(level1)
                         .setSubType(level2)
                         .setSubType2(subtype2)
                         .setMessage(total_fx_exceptions)
                         .setData({
                             'count': total_fx_exceptions,
                             'failed_items': failed_items,
                             'passed_items': passed_items,
                             'threshold': threshold,
                             'kpi_info': kpi
                         }))
            
            validations.append(validation)
            
        except Exception as e:
            continue
    
    return validations


def dividend_change_validations(dividend_a: List[Dict], dividend_b: List[Dict], 
                               nav_kpis: List[Dict], fund_id: Optional[int] = None) -> List[Any]:
    """
    Major Dividends validation: Check dividend report from accounting system.
    If the difference in total dividend between 2 periods exceeds threshold, flag as exception
    """
    validations = []
    
    # Get dividend KPIs
    dividend_kpis = [kpi for kpi in nav_kpis if 'dividend' in kpi.get('kpi_code', '').lower()]
    
    if not dividend_kpis:
        return validations
    
    
    for kpi in dividend_kpis:
        threshold = db_validation_service.get_kpi_threshold(kpi['id'], fund_id)
        if threshold is None:
            continue
        
        # Get precision_type from KPI library
        precision_type = kpi.get('precision_type', 'PERCENTAGE')
        
        try:
            # Calculate total dividends for each period
            total_dividends_a = sum(item.get('Amount', 0) for item in dividend_a) if dividend_a else 0
            total_dividends_b = sum(item.get('Amount', 0) for item in dividend_b) if dividend_b else 0
            
            
            # Calculate absolute and percentage change
            absolute_change = abs(total_dividends_b - total_dividends_a)
            
            # Calculate unsigned percentage change for threshold comparison using formula (B-A)/|A|
            if total_dividends_a != 0:
                unsigned_percentage_change = abs((total_dividends_b - total_dividends_a) / abs(total_dividends_a)) * 100
                # Calculate signed percentage change for display using formula (B-A)/|A|
                signed_percentage_change = ((total_dividends_b - total_dividends_a) / abs(total_dividends_a)) * 100
            else:
                unsigned_percentage_change = 100 if total_dividends_b != 0 else 0
                signed_percentage_change = 100 if total_dividends_b > 0 else (-100 if total_dividends_b < 0 else 0)
            
            # Check if change exceeds threshold based on precision_type
            if precision_type == 'ABSOLUTE':
                is_major_change = absolute_change > threshold
            else:  # PERCENTAGE
                is_major_change = unsigned_percentage_change > threshold
            
            exception_count = 1 if is_major_change else 0
            
            # Prepare detailed validation data
            failed_items = []
            passed_items = []
            
            # Calculate change_value based on precision_type for frontend display
            if precision_type == 'ABSOLUTE':
                change_value = absolute_change
            else:  # PERCENTAGE
                change_value = unsigned_percentage_change
            
            if is_major_change:
                failed_items.append({
                    'identifier': 'Total Dividends',
                    'field': 'dividend_amount',
                    'value_a': total_dividends_a,
                    'value_b': total_dividends_b,
                    'change_value': change_value,
                    'absolute_change': absolute_change,
                    'percentage_change': signed_percentage_change,
                    'threshold': threshold,
                    'precision_type': precision_type
                })
            else:
                passed_items.append({
                    'identifier': 'Total Dividends',
                    'field': 'dividend_amount',
                    'value_a': total_dividends_a,
                    'value_b': total_dividends_b,
                    'change_value': change_value,
                    'absolute_change': absolute_change,
                    'percentage_change': signed_percentage_change,
                    'threshold': threshold,
                    'precision_type': precision_type
                })
            
            # Create validation result based on hierarchy mapping
            level1, level2, subtype2 = _get_validation_hierarchy(kpi)
            
            validation = (VALIDATION_STATUS()
                         .setProductName('validus')
                         .setType(level1)
                         .setSubType(level2)
                         .setSubType2(subtype2)
                         .setMessage(exception_count)
                         .setData({
                             'count': exception_count,
                             'failed_items': failed_items,
                             'passed_items': passed_items,
                             'threshold': threshold,
                             'precision_type': precision_type,
                             'kpi_code': kpi.get('kpi_code', ''),
                             'kpi_name': kpi.get('kpi_name', ''),
                             'kpi_id': kpi.get('id', ''),
                             'kpi_description': kpi.get('description', '')
                         }))
            
            validations.append(validation)
                
        except Exception as e:
            continue
    
    return validations


def _create_error_validation(error_message: str, sub_type2: str) -> Any:
    """Create an error validation status"""
    return (VALIDATION_STATUS()
            .setProductName('validus')
            .setType('PnL')
            .setSubType('Error')
            .setSubType2(sub_type2)
            .setMessage(-1)
            .setData({'error': error_message}))


# Enhanced position validation functions with detailed tracking

def _perform_default_position_validations(portfolio_a: List[Dict], portfolio_b: List[Dict]) -> List[Any]:
    """
    Perform default position validations that don't require KPI configuration
    
    Note: User requested to remove zero quantity and missing position as separate validations.
    Only KPI-based major position changes should be included.
    """
    validations = []
    
    # No default position validations - only KPI-based validations should run
    return validations


def _calculate_large_trades_detailed(portfolio_a: List[Dict], portfolio_b: List[Dict], threshold: float) -> tuple:
    """Calculate large trades with detailed tracking using composite identifiers"""
    failed_items = []  # Large trades
    passed_items = []  # Normal trades
    
    # Create lookup for portfolio A using composite identifiers
    from .validation_utils import _create_composite_identifier
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
            continue  # Skip unmatched positions
            
        matched_items += 1
        
        try:
            mv_a = float(item_a.get('End Book MV', 0))
            mv_b = float(item_b.get('End Book MV', 0))
            
            trade_value = abs(mv_b - mv_a)
            
            # Use description for display, fallback to inv_id
            display_identifier = description if description else inv_id
            
            validation_item = {
                'identifier': display_identifier,
                'inv_id': inv_id,
                'description': description,
                'field': 'trade_value',
                'mv_a': mv_a,
                'mv_b': mv_b,
                'trade_value': trade_value,
                'threshold': threshold,
                'raw_data_a': item_a,
                'raw_data_b': item_b
            }
            
            if trade_value > threshold:
                failed_items.append(validation_item)
            else:
                passed_items.append(validation_item)
                
        except (ValueError, TypeError):
            # Skip items with invalid numeric values
            continue
    
    
    return failed_items, passed_items, len(failed_items) + len(passed_items)


def _calculate_position_changes_detailed(portfolio_a: List[Dict], portfolio_b: List[Dict], threshold: float) -> tuple:
    """Calculate position changes with detailed tracking using composite identifiers"""
    failed_items = []  # Large changes
    passed_items = []  # Normal changes
    
    # Create lookup for portfolio A using composite identifiers
    from .validation_utils import _create_composite_identifier
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
            continue  # Skip unmatched positions
            
        matched_items += 1
        
        try:
            qty_a = float(item_a.get('End Qty', 0))
            qty_b = float(item_b.get('End Qty', 0))
            
            if qty_a == 0:
                continue  # Skip division by zero
            
            change_percent = abs((qty_b - qty_a) / qty_a) * 100
            
            # Use description for display, fallback to inv_id
            display_identifier = description if description else inv_id
            
            validation_item = {
                'identifier': display_identifier,
                'inv_id': inv_id,
                'description': description,
                'field': 'position_change',
                'qty_a': qty_a,
                'qty_b': qty_b,
                'change_percent': change_percent,
                'threshold_percent': threshold,
                'raw_data_a': item_a,
                'raw_data_b': item_b
            }
            
            if change_percent > threshold:
                failed_items.append(validation_item)
            else:
                passed_items.append(validation_item)
                
        except (ValueError, TypeError):
            # Skip items with invalid numeric values
            continue
    
    
    return failed_items, passed_items, len(failed_items) + len(passed_items)

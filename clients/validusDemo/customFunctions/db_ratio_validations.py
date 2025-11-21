
"""
Database-driven ratio validation functions
Modular functions for financial ratios, liquidity ratios, and concentration ratios
"""

from server.APIServerUtils.db_validation_service import db_validation_service
from validations import VALIDATION_STATUS
from typing import List, Dict, Any, Optional
import pandas as pd


def ratio_validations(fund_name: str, source_a: str, source_b: str, date_a: str, date_b: str,
                     ratio_kpis: List[Dict], fund_id: Optional[int] = None,
                     trial_balance_a: List[Dict] = None, trial_balance_b: List[Dict] = None,
                     portfolio_a: List[Dict] = None, portfolio_b: List[Dict] = None,
                     dividend_a: List[Dict] = None, dividend_b: List[Dict] = None,
                     is_dual_source: bool = False) -> List[Any]:
    """
    Main function to calculate all ratio validations using comprehensive database data
    Ingests data from all relevant tables and calculates comprehensive metrics
    Supports both SINGLE_SOURCE and DUAL_SOURCE validation modes
    """
    validations = []
        
    # ========================================
    # COMPREHENSIVE DATA INGESTION (OPTIMIZED)
    # ========================================
    
    # Use pre-fetched data if provided (OPTIMIZATION), otherwise fetch from database
    if trial_balance_a is None:
        trial_balance_a = db_validation_service.get_trial_balance_data(fund_name, source_a, date_a)
    if portfolio_a is None:
        portfolio_valuation_a = db_validation_service.get_portfolio_valuation_data(fund_name, source_a, date_a)
    else:
        portfolio_valuation_a = portfolio_a
        
    if dividend_a is None:
        dividend_data_a = db_validation_service.get_dividend_data(fund_name, source_a, date_a)
    else:
        dividend_data_a = dividend_a
    
    # Get all database tables for period B
    if trial_balance_b is None:
        if date_b != date_a or source_b != source_a:
            trial_balance_b = db_validation_service.get_trial_balance_data(fund_name, source_b, date_b)
        else:
            trial_balance_b = trial_balance_a
            
    if portfolio_b is None:
        if date_b != date_a or source_b != source_a:
            portfolio_valuation_b = db_validation_service.get_portfolio_valuation_data(fund_name, source_b, date_b)
        else:
            portfolio_valuation_b = portfolio_valuation_a
    else:
        portfolio_valuation_b = portfolio_b
        
    if dividend_b is None:
        if date_b != date_a or source_b != source_a:
            dividend_data_b = db_validation_service.get_dividend_data(fund_name, source_b, date_b)
        else:
            dividend_data_b = dividend_data_a
    else:
        dividend_data_b = dividend_b
    
    if not trial_balance_a:
        return [_create_error_validation('No trial balance data found', 'Data Availability')]
    
    # ========================================
    # CONVERT TO DATAFRAMES
    # ========================================
    
    # Period A DataFrames
    tb_df_a = pd.DataFrame(trial_balance_a) if trial_balance_a else pd.DataFrame()
    portfolio_df_a = pd.DataFrame(portfolio_valuation_a) if portfolio_valuation_a else pd.DataFrame()
    dividend_df_a = pd.DataFrame(dividend_data_a) if dividend_data_a else pd.DataFrame()
    
    # Period B DataFrames  
    tb_df_b = pd.DataFrame(trial_balance_b) if trial_balance_b else pd.DataFrame()
    portfolio_df_b = pd.DataFrame(portfolio_valuation_b) if portfolio_valuation_b else pd.DataFrame()
    dividend_df_b = pd.DataFrame(dividend_data_b) if dividend_data_b else pd.DataFrame()
    
    
    # ========================================
    # COMPREHENSIVE METRICS CALCULATION (using centralized function)
    # ========================================
    
    # Use centralized comprehensive metrics calculator
    from .financial_metrics import calculate_comprehensive_metrics
    
    metrics_a = calculate_comprehensive_metrics(trial_balance_a, portfolio_valuation_a, dividend_data_a, source_a, date_a)
    metrics_b = calculate_comprehensive_metrics(trial_balance_b, portfolio_valuation_b, dividend_data_b, source_b, date_b)
    
    
    # ========================================
    # RATIO VALIDATIONS WITH CALCULATED METRICS
    # ========================================
    
    # Pass pre-calculated metrics to validation functions
    validations.extend(financial_ratio_validations(metrics_a, metrics_b, ratio_kpis, fund_id, is_dual_source))
    validations.extend(liquidity_ratio_validations(metrics_a, metrics_b, ratio_kpis, fund_id, is_dual_source))
    validations.extend(concentration_ratio_validations(metrics_a, metrics_b, ratio_kpis, fund_id, is_dual_source))
    validations.extend(sentiment_ratio_validations(metrics_a, metrics_b, ratio_kpis, fund_id, is_dual_source))
    
    return validations


def financial_ratio_validations(metrics_a: Dict[str, float], metrics_b: Dict[str, float],
                               ratio_kpis: List[Dict], fund_id: Optional[int] = None, 
                               is_dual_source: bool = False) -> List[Any]:
    """
    Calculate financial ratio validations using pre-calculated comprehensive metrics:
    1. Debt-to-Equity Ratio = Total Liabilities / Total Equity
    2. Gross Leverage Ratio = Total Assets / NAV
    3. Expense Ratio = Total Non-trading expenses / Total Assets
    4. Management Fee Ratio = Management Fees / NAV
    5. Performance Fee Ratio = Performance Fees / NAV
    """
    validations = []
    
    
    # Get relevant KPIs
    financial_kpis = [kpi for kpi in ratio_kpis if 'financial' in kpi.get('category', '').lower()]
    
    if not metrics_a or not metrics_b:
        return validations
    
    for kpi in financial_kpis:
        # OPTIMIZATION: Use pre-fetched threshold
        threshold = kpi.get('threshold') if 'threshold' in kpi else db_validation_service.get_kpi_threshold(kpi['id'], fund_id)
        if threshold is None:
            continue
            
        # Get numerator and denominator fields from database
        numerator_field = kpi.get('numerator_field')
        denominator_field = kpi.get('denominator_field')
        
        if not numerator_field or not denominator_field:
            continue
        
        # Get metric values using database-driven field names
        numerator_a = metrics_a.get(numerator_field, 0)
        denominator_a = metrics_a.get(denominator_field, 1)  # Avoid division by zero
        numerator_b = metrics_b.get(numerator_field, 0)
        denominator_b = metrics_b.get(denominator_field, 1)  # Avoid division by zero
        
        # Create readable display names for numerator and denominator
        numerator_display = _get_metric_display_name(numerator_field)
        denominator_display = _get_metric_display_name(denominator_field)
        
        # Calculate ratio validation using database-driven fields
        validation = _calculate_ratio_validation(
            kpi.get('category', 'Financial'), 
            kpi.get('kpi_name', 'Unknown Ratio'),
            numerator_a, denominator_a,
            numerator_b, denominator_b,
            threshold, kpi,
            numerator_display, denominator_display,
            is_dual_source
        )
        
        # Enhance validation data with metric information for flow visualization
        if hasattr(validation, 'data') and isinstance(validation.data, dict):
            validation.data.update({
                'sourceANumerator': numerator_a,
                'sourceBNumerator': numerator_b,
                'sourceADenominator': denominator_a,
                'sourceBDenominator': denominator_b,
                'numeratorDescription': numerator_display,
                'denominatorDescription': denominator_display
            })
        
        validations.append(validation)
    
    return validations


def liquidity_ratio_validations(metrics_a: Dict[str, float], metrics_b: Dict[str, float],
                               ratio_kpis: List[Dict], fund_id: Optional[int] = None,
                               is_dual_source: bool = False) -> List[Any]:
    """
    Calculate liquidity ratio validations using pre-calculated comprehensive metrics:
    1. Current Ratio = Current Assets / Current Liabilities  
    2. Liquidity Ratio = (Cash + Marketable Securities) / Current Liabilities
    3. Redemption Liquidity Ratio = Liquid Assets / Total NAV
    """
    validations = []
    
    
    # Get relevant KPIs
    liquidity_kpis = [kpi for kpi in ratio_kpis if 'liquidity' in kpi.get('category', '').lower()]
    
    if not metrics_a or not metrics_b:
        return validations
    
    for kpi in liquidity_kpis:
        # OPTIMIZATION: Use pre-fetched threshold
        threshold = kpi.get('threshold') if 'threshold' in kpi else db_validation_service.get_kpi_threshold(kpi['id'], fund_id)
        if threshold is None:
            continue
            
        # Get numerator and denominator fields from database
        numerator_field = kpi.get('numerator_field')
        denominator_field = kpi.get('denominator_field')
        
        if not numerator_field or not denominator_field:
            continue
        
        # Get metric values using database-driven field names
        numerator_a = metrics_a.get(numerator_field, 0)
        denominator_a = metrics_a.get(denominator_field, 1)  # Avoid division by zero
        numerator_b = metrics_b.get(numerator_field, 0)
        denominator_b = metrics_b.get(denominator_field, 1)  # Avoid division by zero
        
        # Create readable display names for numerator and denominator
        numerator_display = _get_metric_display_name(numerator_field)
        denominator_display = _get_metric_display_name(denominator_field)
        
        # Calculate ratio validation using database-driven fields
        validation = _calculate_ratio_validation(
            kpi.get('category', 'Liquidity'), 
            kpi.get('kpi_name', 'Unknown Ratio'),
            numerator_a, denominator_a,
            numerator_b, denominator_b,
            threshold, kpi,
            numerator_display, denominator_display,
            is_dual_source
        )
        
        # Enhance validation data with metric information for flow visualization
        if hasattr(validation, 'data') and isinstance(validation.data, dict):
            validation.data.update({
                'sourceANumerator': numerator_a,
                'sourceBNumerator': numerator_b,
                'sourceADenominator': denominator_a,
                'sourceBDenominator': denominator_b,
                'numeratorDescription': numerator_display,
                'denominatorDescription': denominator_display
            })
        
        validations.append(validation)
    
    return validations

def concentration_ratio_validations(metrics_a: Dict[str, float], metrics_b: Dict[str, float],
                                   ratio_kpis: List[Dict], fund_id: Optional[int] = None,
                                   is_dual_source: bool = False) -> List[Any]:
    """
    Calculate concentration ratio validations using pre-calculated comprehensive metrics
    """
    validations = []
    
    # Get relevant KPIs
    concentration_kpis = [kpi for kpi in ratio_kpis if 'concentration' in kpi.get('category', '').lower()]
    if not metrics_a or not metrics_b:
        return validations
    
    for kpi in concentration_kpis:
        # OPTIMIZATION: Use pre-fetched threshold
        threshold = kpi.get('threshold') if 'threshold' in kpi else db_validation_service.get_kpi_threshold(kpi['id'], fund_id)
        if threshold is None:
            continue
            
        kpi_name = kpi['kpi_name'].lower()

        # Get numerator and denominator fields from database
        numerator_field = kpi.get('numerator_field')
        denominator_field = kpi.get('denominator_field')

        # Create readable display names for numerator and denominator
        numerator_display = _get_metric_display_name(numerator_field)
        denominator_display = _get_metric_display_name(denominator_field)

        # Get metric values using database-driven field names
        numerator_a = metrics_a.get(numerator_field, 0)
        denominator_a = metrics_a.get(denominator_field, 1)  # Avoid division by zero
        numerator_b = metrics_b.get(numerator_field, 0)
        denominator_b = metrics_b.get(denominator_field, 1)  # Avoid division by zero
        if 'asset' in kpi_name or 'concentration' in kpi_name:
            validation = _calculate_ratio_validation(
            kpi.get('category', 'Concentration'), 
            kpi.get('kpi_name', 'Unknown Ratio'),
            numerator_a, denominator_a,
            numerator_b, denominator_b,
            threshold, kpi,
            numerator_display, denominator_display,
            is_dual_source
            )
            validations.append(validation)
    
    return validations


def sentiment_ratio_validations(metrics_a: Dict[str, float], metrics_b: Dict[str, float],
                               ratio_kpis: List[Dict], fund_id: Optional[int] = None,
                               is_dual_source: bool = False) -> List[Any]:
    """
    Calculate sentiment ratio validations using pre-calculated comprehensive metrics:
    1. Subscription Redemption Ratio = Subscription Inflows / Redemption Outflows
    2. Net Long Position Ratio = Net Long Positions / NAV
    3. Excess Return over Benchmark = Portfolio Return / Benchmark Return
    4. Gross Exposure Ratio = Gross Exposure / NAV
    5. Net Exposure Ratio = Net Exposure / NAV
    """
    validations = []
    
    # Filter for sentiment-related KPIs
    sentiment_kpis = [kpi for kpi in ratio_kpis if kpi.get('category') in ['Sentiment', 'Activity']]
    
    for kpi in sentiment_kpis:
        numerator_field = kpi.get('numerator_field')
        denominator_field = kpi.get('denominator_field')
        
        if not numerator_field or not denominator_field:
            continue
        
        # OPTIMIZATION: Use pre-fetched threshold
        threshold = kpi.get('threshold') if 'threshold' in kpi else db_validation_service.get_kpi_threshold(kpi['id'], fund_id)
        if threshold is None:
            continue
        
        # Convert field names to metric names (automatic conversion)
        def convert_to_metric_name(field_name):
            # Handle special cases
            if field_name == 'NAV':
                return 'nav'
            # Convert space-separated title case to snake_case
            return field_name.lower().replace(' ', '_')
        
        mapped_numerator = convert_to_metric_name(numerator_field)
        mapped_denominator = convert_to_metric_name(denominator_field)
        
        # Get values from metrics
        numerator_a = metrics_a.get(mapped_numerator, 0)
        denominator_a = metrics_a.get(mapped_denominator, 0)
        numerator_b = metrics_b.get(mapped_numerator, 0)
        denominator_b = metrics_b.get(mapped_denominator, 0)
        
        # Get display names for the fields
        numerator_display = _get_metric_display_name(numerator_field)
        denominator_display = _get_metric_display_name(denominator_field)
        
        if numerator_display == numerator_field:
            numerator_display = numerator_field.replace('_', ' ').title()
        if denominator_display == denominator_field:
            denominator_display = denominator_field.replace('_', ' ').title()
        
        # Special handling for excess return over benchmark (uses subtraction, not division)
        if kpi.get('kpi_code') == 'excess_return_over_benchmark':
            validation = _calculate_excess_return_validation(
                'Sentiment', kpi.get('kpi_name', 'Excess Return Over Benchmark'),
                numerator_a, denominator_a, numerator_b, denominator_b,
                threshold, kpi,
                numerator_display, denominator_display
            )
        else:
            # Create validation using the common ratio validation function
            validation = _calculate_ratio_validation(
                'Sentiment', kpi.get('kpi_name', 'Unknown KPI'),
                numerator_a, denominator_a, numerator_b, denominator_b,
                threshold, kpi,
                numerator_display, denominator_display,
                is_dual_source
            )
        validations.append(validation)
    
    return validations


def _get_metric_display_name(metric_field: str) -> str:
    """Convert metric field names to readable display names"""
    # Only keep special cases that can't be handled by automatic conversion
    special_cases = {
        'nav': 'NAV',
        'NAV': 'NAV', 
        'investments': 'MV of Investments',
        'concentrated_assets_value': 'Concentrated Assets',
        'largest_position_mv': 'Largest Position MV',
        'top_5_positions_mv': 'Top 5 Positions MV'
    }
    
    # Check special cases first
    if metric_field in special_cases:
        return special_cases[metric_field]
    
    # Automatic conversion for everything else
    return metric_field.replace('_', ' ').title()

# REMOVED: _calculate_comprehensive_metrics function - now centralized in financial_metrics.py
# This function has been moved to financial_metrics.py as calculate_comprehensive_metrics()
# to provide a single source of truth for all metric calculations across the system

# REMOVED: _calculate_financial_metrics_excel function - redundant with centralized financial_metrics.py
# This legacy function is no longer needed as all metrics are calculated centrally

def _calculate_ratio_validation(ratio_Type: str, ratio_sub_Type: str,
                              source_a_numerator: float, source_a_denominator: float,
                              source_b_numerator: float, source_b_denominator: float,
                              threshold: float, kpi: Dict,
                              numerator_description: str, denominator_description: str,
                              is_dual_source: bool = False) -> Any:
    """
    Calculate ratio validation with support for both single-source and dual-source modes
    For single-source: calculates period-over-period ratio changes
    For dual-source: calculates cross-source ratio differences
    """
    
    # Calculate ratios
    source_a_ratio = source_a_numerator / source_a_denominator if source_a_denominator != 0 else None
    source_b_ratio = source_b_numerator / source_b_denominator if source_b_denominator != 0 else None
    
    # Calculate change in ratio based on validation mode
    if source_a_ratio is None or source_b_ratio is None:
        change_in_ratio = None
    elif is_dual_source:
        # For dual-source: Calculate percentage difference between source ratios
        if source_a_ratio == 0:
            change_in_ratio = 100 if source_b_ratio != 0 else 0
        else:
            change_in_ratio = abs(source_b_ratio - source_a_ratio) / source_a_ratio * 100
    else:
        # For single-source: Calculate period-over-period ratio change
        if source_a_ratio == 0:
            change_in_ratio = 100 if source_b_ratio != 0 else 0
        else:
            change_in_ratio = ((source_b_ratio / source_a_ratio) - 1) * 100  # Convert to percentage
    
    # Determine if it's a major change (threshold is already in percentage)
    is_major = bool(abs(change_in_ratio) > threshold) if change_in_ratio is not None else False

    # Create validation data
    validation_data = {
        'ratioType': ratio_Type,
        'ratioSubType': ratio_sub_Type,
        'sourceA': source_a_ratio,
        'sourceB': source_b_ratio,
        'change': change_in_ratio,
        'isMajor': is_major,
        'sourceANumerator': source_a_numerator,
        'sourceBNumerator': source_b_numerator,
        'sourceADenominator': source_a_denominator,
        'sourceBDenominator': source_b_denominator,
        'numeratorDescription': numerator_description,
        'denominatorDescription': denominator_description,
        'threshold': threshold,
        'kpi_code': kpi.get('kpi_code'),
        'kpi_name': kpi.get('kpi_name'),
        'kpi_id': kpi.get('id'),
        'kpi_description': kpi.get('description', ''),  # Add missing KPI description for tooltips
        'description': kpi.get('description', '')       # Also add as 'description' for frontend compatibility
    }
    
    return (VALIDATION_STATUS()
            .setProductName('validus')
            .setType('Ratio')
            .setSubType(ratio_Type)
            .setSubType2(ratio_sub_Type)
            .setMessage(1 if is_major else 0)
            .setData(validation_data))


def _calculate_excess_return_validation(ratio_Type: str, ratio_sub_Type: str,
                                      fund_return_a: float, benchmark_return_a: float,
                                      fund_return_b: float, benchmark_return_b: float,
                                      threshold: float, kpi: Dict,
                                      fund_return_label: str, benchmark_return_label: str) -> Any:
    """
    Calculate excess return validation (Fund Return - Benchmark Return)
    This is different from traditional ratio validation as it uses subtraction
    """
    from validations import VALIDATION_STATUS
    
    # Calculate excess returns (Fund Return - Benchmark Return)
    excess_return_a = fund_return_a - benchmark_return_a
    excess_return_b = fund_return_b - benchmark_return_b
    
    # Calculate percentage change in excess return (similar to other ratios)
    if excess_return_a != 0:
        change_in_excess_return = ((excess_return_b - excess_return_a) / abs(excess_return_a)) * 100
    else:
        # If excess_return_a is 0, calculate based on the magnitude of excess_return_b
        change_in_excess_return = excess_return_b * 100 if excess_return_b != 0 else 0
    
    # Check if excess return exceeds threshold
    is_major = abs(change_in_excess_return) > threshold
    
    validation_data = {
        'kpi_name': kpi.get('kpi_name', 'Excess Return Over Benchmark'),
        'kpi_description': kpi.get('description', ''),
        'threshold': threshold,
        'precision_type': kpi.get('precision_type', 'PERCENTAGE'),
        'sourceA': excess_return_a,
        'sourceB': excess_return_b,
        'change': change_in_excess_return,
        'sourceANumerator': fund_return_a,
        'sourceBNumerator': fund_return_b,
        'sourceADenominator': benchmark_return_a,
        'sourceBDenominator': benchmark_return_b,
        'numeratorDescription': fund_return_label,
        'denominatorDescription': benchmark_return_label,
        'description': kpi.get('description', ''),
        'formula': f'= {fund_return_label} - {benchmark_return_label}'  # Custom formula for excess return
    }
    
    return (VALIDATION_STATUS()
            .setProductName('validus')
            .setType('Ratio')
            .setSubType(ratio_Type)
            .setSubType2(ratio_sub_Type)
            .setMessage(1 if is_major else 0)
            .setData(validation_data))


def _create_error_validation(error_message: str, sub_Type2: str) -> Any:
    """Create an error validation status"""
    return (VALIDATION_STATUS()
            .setProductName('validus')
            .setType('Ratio')
            .setSubType('Error')
            .setSubType2(sub_Type2)
            .setMessage(-1)
            .setData({'error': error_message}))

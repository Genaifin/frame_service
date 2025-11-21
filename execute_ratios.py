"""
Execute Ratios - Batch execution of all active ratios
Gets ratio configurations from database and executes them using formula_validator.py
"""

import sys
import os
from typing import Dict, Any, List, Optional
from datetime import datetime
import pandas as pd
import json
import numpy as np

# Add the project root to the path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from formula_validator import validateFormulaWithDatabase
from utils.formula_utils import extractFormulaFromDisplayName as _extractFormulaFromDisplayName
from server.APIServerUtils.db_validation_service import DatabaseValidationService
from database_models import get_database_manager
import re



def _addPeriodPrefixToFormula(formula: str, period_name: str) -> str:
    """
    Add period prefix to all column references in a formula
    
    Converts: {'Trial Balance'}.[Account Type]
    To: Period1.{'Trial Balance'}.[Account Type]
    
    Args:
        formula: Formula without period prefix (e.g., SUM(CASE WHEN {'Trial Balance'}.[Account Type] ...))
        period_name: Period name to add (e.g., 'Period1', 'Period2')
    
    Returns:
        Formula with period prefix added to all column references
    """
    if not formula:
        return formula
    
    # Pattern to match column references: {'Model Name'}.[Column Name]
    # This pattern matches: { followed by anything (non-greedy) } followed by .[ followed by anything (non-greedy) ]
    pattern = r"(\{'[^']+'\}\.\[[^\]]+\])"
    
    def replace_with_period(match):
        column_ref = match.group(1)
        return f"{period_name}.{column_ref}"
    
    # Replace all column references with period-prefixed versions
    result = re.sub(pattern, replace_with_period, formula)
    
    return result


def _substituteNumeratorDenominator(
    formula: str,
    numerator_formula: str,
    denominator_formula: str,
    source_mapping: Optional[Dict[str, str]] = None
) -> str:
    """
    Substitute Numerator and Denominator placeholders in the formula with actual formulas
    
    The formula may contain:
    - Period1.`Numerator` and Period2.`Numerator` -> replace with numerator_formula
    - Period1.`Denominator` and Period2.`Denominator` -> replace with denominator_formula
    - SourceA.`Numerator` and SourceB.`Numerator` -> replace with numerator_formula (for dual source)
    - SourceA.`Denominator` and SourceB.`Denominator` -> replace with denominator_formula (for dual source)
    
    Args:
        formula: The formula with placeholders (e.g., Period1.`Numerator` or SourceA.`Numerator`)
        numerator_formula: The actual numerator formula (without period prefix)
        denominator_formula: The actual denominator formula (without period prefix)
        source_mapping: Optional source mapping to determine which period to use for SourceA/SourceB
    
    Returns:
        Formula with placeholders substituted
    """
    if not formula:
        return formula
    
    # Extract actual formulas (remove Displayname prefix if present)
    numerator_base = _extractFormulaFromDisplayName(numerator_formula)[1]
    denominator_base = _extractFormulaFromDisplayName(denominator_formula)[1]
    
    if not numerator_base or not denominator_base:
        print(f"Warning: Could not extract numerator or denominator formula")
        return formula
    
    # Determine which period to use for SourceA and SourceB
    # For Case 1 (2 sources, 1 period): SourceA and SourceB both use Period1 (the single period)
    # For Case 2 (1 source, 2 periods): SourceA uses Period1, but formulas use Period1/Period2 directly
    # When source_mapping is provided and has SourceB, it's Case 1 (2 sources, 1 period)
    # In that case, both SourceA and SourceB use Period1 (same period, different sources)
    if source_mapping and 'SourceB' in source_mapping:
        # Case 1: 2 sources, 1 period - both use Period1
        period_for_source_a = 'Period1'
        period_for_source_b = 'Period1'
    else:
        # Case 2 or single source: SourceA uses Period1, SourceB uses Period2 (if needed)
        period_for_source_a = 'Period1'
        period_for_source_b = 'Period2'
    
    # Add period prefix to formulas for each period (add Period1/Period2 to each column reference inside)
    numerator_period1 = _addPeriodPrefixToFormula(numerator_base, 'Period1')
    numerator_period2 = _addPeriodPrefixToFormula(numerator_base, 'Period2')
    numerator_source_a = _addPeriodPrefixToFormula(numerator_base, period_for_source_a)
    numerator_source_b = _addPeriodPrefixToFormula(numerator_base, period_for_source_b)
    
    denominator_period1 = _addPeriodPrefixToFormula(denominator_base, 'Period1')
    denominator_period2 = _addPeriodPrefixToFormula(denominator_base, 'Period2')
    denominator_source_a = _addPeriodPrefixToFormula(denominator_base, period_for_source_a)
    denominator_source_b = _addPeriodPrefixToFormula(denominator_base, period_for_source_b)
    
    result = formula
    
    # Replace Period1.`Numerator` or Period1.Numerator with Period1-prefixed numerator formula
    result = re.sub(
        r'Period1\.`?Numerator`?',
        f'({numerator_period1})',
        result,
        flags=re.IGNORECASE
    )
    
    # Replace Period2.`Numerator` or Period2.Numerator with Period2-prefixed numerator formula
    result = re.sub(
        r'Period2\.`?Numerator`?',
        f'({numerator_period2})',
        result,
        flags=re.IGNORECASE
    )
    
    # Replace SourceA.`Numerator` or SourceA.Numerator with period-prefixed numerator formula
    # Try multiple patterns to handle different formats
    patterns_source_a_num = [
        r'SourceA\.`Numerator`',  # Exact match with backticks
        r'SourceA\.\s*`Numerator`',  # With optional whitespace
        r'SourceA\.\s*Numerator',  # Without backticks
        r'SourceA\.`?Numerator`?',  # Optional backticks
    ]
    for pattern in patterns_source_a_num:
        result = re.sub(pattern, f'({numerator_source_a})', result, flags=re.IGNORECASE)
    
    # Replace SourceB.`Numerator` or SourceB.Numerator with period-prefixed numerator formula
    # Try multiple patterns to handle different formats
    patterns_source_b_num = [
        r'SourceB\.`Numerator`',  # Exact match with backticks
        r'SourceB\.\s*`Numerator`',  # With optional whitespace
        r'SourceB\.\s*Numerator',  # Without backticks
        r'SourceB\.`?Numerator`?',  # Optional backticks
    ]
    for pattern in patterns_source_b_num:
        result = re.sub(pattern, f'({numerator_source_b})', result, flags=re.IGNORECASE)
    
    # Replace Period1.`Denominator` or Period1.Denominator with Period1-prefixed denominator formula
    result = re.sub(
        r'Period1\.`?Denominator`?',
        f'({denominator_period1})',
        result,
        flags=re.IGNORECASE
    )
    
    # Replace Period2.`Denominator` or Period2.Denominator with Period2-prefixed denominator formula
    result = re.sub(
        r'Period2\.`?Denominator`?',
        f'({denominator_period2})',
        result,
        flags=re.IGNORECASE
    )
    
    # Replace SourceA.`Denominator` or SourceA.Denominator with period-prefixed denominator formula
    # Try multiple patterns to handle different formats
    patterns_source_a_den = [
        r'SourceA\.`Denominator`',  # Exact match with backticks
        r'SourceA\.\s*`Denominator`',  # With optional whitespace
        r'SourceA\.\s*Denominator',  # Without backticks
        r'SourceA\.`?Denominator`?',  # Optional backticks
    ]
    for pattern in patterns_source_a_den:
        result = re.sub(pattern, f'({denominator_source_a})', result, flags=re.IGNORECASE)
    
    # Replace SourceB.`Denominator` or SourceB.Denominator with period-prefixed denominator formula
    # Try multiple patterns to handle different formats
    patterns_source_b_den = [
        r'SourceB\.`Denominator`',  # Exact match with backticks
        r'SourceB\.\s*`Denominator`',  # With optional whitespace
        r'SourceB\.\s*Denominator',  # Without backticks
        r'SourceB\.`?Denominator`?',  # Optional backticks
    ]
    for pattern in patterns_source_b_den:
        result = re.sub(pattern, f'({denominator_source_b})', result, flags=re.IGNORECASE)
    
    # Debug: Print if substitution occurred
    if result != formula:
        print(f"DEBUG - Substitution occurred. Original length: {len(formula)}, New length: {len(result)}")
        if 'SourceA.`Numerator' in formula or 'SourceB.`Numerator' in formula:
            print(f"DEBUG - SourceA/SourceB Numerator/Denominator references were in original formula")
            if 'SourceA.`Numerator' not in result and 'SourceB.`Numerator' not in result:
                print(f"DEBUG - SourceA/SourceB Numerator/Denominator references successfully replaced")
            else:
                print(f"DEBUG - WARNING: SourceA/SourceB Numerator/Denominator references still present in result!")
                print(f"DEBUG - Result snippet: {result[:300]}")
    
    return result


def _processRatioDetailWithSources(
    detail: Dict[str, Any],
    detail_idx: int,
    ratio_config: Dict[str, Any],
    client_id: int,
    fund_id: int,
    period_dates: Dict[str, str],
    source_mapping: Dict[str, str],
    include_full_data: bool,
    db_manager
) -> Dict[str, Any]:
    """
    Process a single ratio detail with source mapping support
    
    Note: Ratios produce single aggregated values per period/source, so alignment keys
    are not needed (unlike validations which compare individual records).
    
    Args:
        detail: Ratio detail dictionary
        detail_idx: Index of detail in list
        ratio_config: Ratio configuration dictionary
        client_id: Client ID
        fund_id: Fund ID
        period_dates: Dictionary mapping period names to dates
        source_mapping: Dictionary mapping source names to actual source values
        include_full_data: Whether to include full result DataFrames/Series
        db_manager: Database manager instance
    
    Returns:
        Dictionary containing detail result
    """
    detail_id = detail.get('intratiodetailid')
    formula = detail.get('vcformula')
    numerator_formula = detail.get('vcnumerator') or ''
    denominator_formula = detail.get('vcdenominator') or ''
    detail_filter = detail.get('vcfilter')
    detail_filter_type = detail.get('vcfiltertype', 'I')  # Default to 'I' (Include)
    
    # Get threshold from config (prefer config threshold over master threshold)
    threshold = ratio_config.get('config_threshold')
    if threshold is None:
        threshold = ratio_config.get('intthreshold')
    
    # Use detail filter if available, otherwise use config filter
    filter_condition = ratio_config.get('vccondition')
    final_filter = detail_filter if detail_filter else filter_condition
    
    # Substitute Numerator and Denominator placeholders if formula contains them
    original_formula = formula
    print(f"DEBUG - Formula: {formula}")
    print(f"DEBUG - Numerator Formula: {numerator_formula}")
    print(f"DEBUG - Denominator Formula: {denominator_formula}")
    print(f"DEBUG - Source Mapping: {source_mapping}")
    
    # For dual source, we'll calculate numerator/denominator values first, then substitute with actual values
    # For single source, we'll use the period-based substitution
    will_calculate_numerator_denominator = formula and (numerator_formula or denominator_formula) and ('Numerator' in formula or 'Denominator' in formula)
    
    if will_calculate_numerator_denominator:
        # We'll substitute after calculating numerator/denominator values for dual source
        # For now, keep the original formula
        pass
    else:
        print(f"\n  Detail {detail_idx + 1} (ID: {detail_id})")
        print(f"    Formula: {formula}")
    
    print(f"    Filter: {final_filter}")
    print(f"    Filter Type: {detail_filter_type} ({'Exclude' if detail_filter_type == 'E' else 'Include'})")
    
    if not formula:
        print(f"    Warning: No formula found for detail {detail_id}")
        return {
            'intratiodetailid': detail_id,
            'formula': original_formula,
            'filter': final_filter,
            'filter_type': detail_filter_type,
            'status': 'skipped',
            'error': 'No formula found'
        }
    
    try:
        # Calculate numerator and denominator values for each period/source if formulas are available
        numerator_values = {}  # {period_name: value} or {source_name: value} for dual source
        denominator_values = {}  # {period_name: value} or {source_name: value} for dual source
        
        # Check if this is a dual source scenario (Case 1: 2 sources, 1 period)
        is_dual_source = source_mapping and 'SourceB' in source_mapping and len(period_dates) == 1
        
        if numerator_formula and denominator_formula:
            # Extract base formulas (remove Displayname prefix)
            numerator_base = _extractFormulaFromDisplayName(numerator_formula)[1]
            denominator_base = _extractFormulaFromDisplayName(denominator_formula)[1]
            
            if numerator_base and denominator_base:
                if is_dual_source:
                    # For dual source (2 sources, 1 period), calculate per source
                    # Get the single period name
                    period_name = list(period_dates.keys())[0] if period_dates else None
                    
                    if period_name:
                        # Calculate for SourceA
                        # For dual source, Period1 should map to SourceA, and SourceA should map to actual source name
                        try:
                            source_a_mapping = {
                                period_name: 'SourceA',  # Map Period1 to SourceA
                                'SourceA': source_mapping['SourceA']  # Map SourceA to actual source name
                            }
                            numerator_period_formula = _addPeriodPrefixToFormula(numerator_base, period_name)
                            numerator_result = validateFormulaWithDatabase(
                                formula=numerator_period_formula,
                                client_id=client_id,
                                fund_id=fund_id,
                                period_dates={period_name: period_dates[period_name]},
                                threshold=None,
                                align_data=False,
                                align_key=None,
                                filter_condition=final_filter,
                                filter_type=detail_filter_type,
                                source_mapping=source_a_mapping
                            )
                            numerator_result_value = numerator_result.get('result')
                            if isinstance(numerator_result_value, (pd.Series, pd.DataFrame)):
                                if len(numerator_result_value) > 0:
                                    numerator_result_value = numerator_result_value.iloc[0] if isinstance(numerator_result_value, pd.Series) else numerator_result_value.iloc[0, 0]
                                else:
                                    numerator_result_value = None
                            numerator_values['SourceA'] = float(numerator_result_value) if numerator_result_value is not None and pd.notna(numerator_result_value) else None
                            
                            denominator_period_formula = _addPeriodPrefixToFormula(denominator_base, period_name)
                            denominator_result = validateFormulaWithDatabase(
                                formula=denominator_period_formula,
                                client_id=client_id,
                                fund_id=fund_id,
                                period_dates={period_name: period_dates[period_name]},
                                threshold=None,
                                align_data=False,
                                align_key=None,
                                filter_condition=final_filter,
                                filter_type=detail_filter_type,
                                source_mapping=source_a_mapping
                            )
                            denominator_result_value = denominator_result.get('result')
                            if isinstance(denominator_result_value, (pd.Series, pd.DataFrame)):
                                if len(denominator_result_value) > 0:
                                    denominator_result_value = denominator_result_value.iloc[0] if isinstance(denominator_result_value, pd.Series) else denominator_result_value.iloc[0, 0]
                                else:
                                    denominator_result_value = None
                            denominator_values['SourceA'] = float(denominator_result_value) if denominator_result_value is not None and pd.notna(denominator_result_value) else None
                        except Exception as e:
                            print(f"    Warning: Could not calculate numerator/denominator for SourceA: {e}")
                            numerator_values['SourceA'] = None
                            denominator_values['SourceA'] = None
                        
                        # Calculate for SourceB
                        # For dual source, Period1 should map to SourceB, and SourceB should map to actual source name
                        try:
                            source_b_mapping = {
                                period_name: 'SourceB',  # Map Period1 to SourceB
                                'SourceB': source_mapping['SourceB']  # Map SourceB to actual source name
                            }
                            numerator_period_formula = _addPeriodPrefixToFormula(numerator_base, period_name)
                            numerator_result = validateFormulaWithDatabase(
                                formula=numerator_period_formula,
                                client_id=client_id,
                                fund_id=fund_id,
                                period_dates={period_name: period_dates[period_name]},
                                threshold=None,
                                align_data=False,
                                align_key=None,
                                filter_condition=final_filter,
                                filter_type=detail_filter_type,
                                source_mapping=source_b_mapping
                            )
                            numerator_result_value = numerator_result.get('result')
                            if isinstance(numerator_result_value, (pd.Series, pd.DataFrame)):
                                if len(numerator_result_value) > 0:
                                    numerator_result_value = numerator_result_value.iloc[0] if isinstance(numerator_result_value, pd.Series) else numerator_result_value.iloc[0, 0]
                                else:
                                    numerator_result_value = None
                            numerator_values['SourceB'] = float(numerator_result_value) if numerator_result_value is not None and pd.notna(numerator_result_value) else None
                            
                            denominator_period_formula = _addPeriodPrefixToFormula(denominator_base, period_name)
                            denominator_result = validateFormulaWithDatabase(
                                formula=denominator_period_formula,
                                client_id=client_id,
                                fund_id=fund_id,
                                period_dates={period_name: period_dates[period_name]},
                                threshold=None,
                                align_data=False,
                                align_key=None,
                                filter_condition=final_filter,
                                filter_type=detail_filter_type,
                                source_mapping=source_b_mapping
                            )
                            denominator_result_value = denominator_result.get('result')
                            if isinstance(denominator_result_value, (pd.Series, pd.DataFrame)):
                                if len(denominator_result_value) > 0:
                                    denominator_result_value = denominator_result_value.iloc[0] if isinstance(denominator_result_value, pd.Series) else denominator_result_value.iloc[0, 0]
                                else:
                                    denominator_result_value = None
                            denominator_values['SourceB'] = float(denominator_result_value) if denominator_result_value is not None and pd.notna(denominator_result_value) else None
                        except Exception as e:
                            print(f"    Warning: Could not calculate numerator/denominator for SourceB: {e}")
                            numerator_values['SourceB'] = None
                            denominator_values['SourceB'] = None
                    
                    # For dual source, substitute SourceA/SourceB Numerator/Denominator with calculated values
                    # This ensures the main formula uses the correct source-specific values
                    if will_calculate_numerator_denominator:
                        # Substitute SourceA.`Numerator` with actual calculated value
                        source_a_num = numerator_values.get('SourceA')
                        source_a_den = denominator_values.get('SourceA')
                        source_b_num = numerator_values.get('SourceB')
                        source_b_den = denominator_values.get('SourceB')
                        
                        if source_a_num is not None:
                            formula = re.sub(r'SourceA\.`?Numerator`?', str(source_a_num), formula, flags=re.IGNORECASE)
                        if source_a_den is not None:
                            formula = re.sub(r'SourceA\.`?Denominator`?', str(source_a_den), formula, flags=re.IGNORECASE)
                        if source_b_num is not None:
                            formula = re.sub(r'SourceB\.`?Numerator`?', str(source_b_num), formula, flags=re.IGNORECASE)
                        if source_b_den is not None:
                            formula = re.sub(r'SourceB\.`?Denominator`?', str(source_b_den), formula, flags=re.IGNORECASE)
                        
                        print(f"\n  Detail {detail_idx + 1} (ID: {detail_id})")
                        print(f"    Original Formula: {original_formula}")
                        print(f"    Substituted Formula (with calculated values): {formula[:200]}..." if len(formula) > 200 else f"    Substituted Formula: {formula}")
                else:
                    # For single source or multi-period, calculate per period (existing logic)
                    for period_name in period_dates.keys():
                        try:
                            # Add period prefix to numerator formula
                            numerator_period_formula = _addPeriodPrefixToFormula(numerator_base, period_name)
                            # Execute numerator formula for this period
                            numerator_result = validateFormulaWithDatabase(
                                formula=numerator_period_formula,
                                client_id=client_id,
                                fund_id=fund_id,
                                period_dates={period_name: period_dates[period_name]},  # Single period
                                threshold=None,
                                align_data=False,
                                align_key=None,
                                filter_condition=final_filter,
                                filter_type=detail_filter_type,
                                source_mapping=source_mapping
                            )
                            # Extract scalar value
                            numerator_result_value = numerator_result.get('result')
                            if isinstance(numerator_result_value, (pd.Series, pd.DataFrame)):
                                if len(numerator_result_value) > 0:
                                    numerator_result_value = numerator_result_value.iloc[0] if isinstance(numerator_result_value, pd.Series) else numerator_result_value.iloc[0, 0]
                                else:
                                    numerator_result_value = None
                            numerator_values[period_name] = float(numerator_result_value) if numerator_result_value is not None and pd.notna(numerator_result_value) else None
                            
                            # Add period prefix to denominator formula
                            denominator_period_formula = _addPeriodPrefixToFormula(denominator_base, period_name)
                            # Execute denominator formula for this period
                            denominator_result = validateFormulaWithDatabase(
                                formula=denominator_period_formula,
                                client_id=client_id,
                                fund_id=fund_id,
                                period_dates={period_name: period_dates[period_name]},  # Single period
                                threshold=None,
                                align_data=False,
                                align_key=None,
                                filter_condition=final_filter,
                                filter_type=detail_filter_type,
                                source_mapping=source_mapping
                            )
                            # Extract scalar value
                            denominator_result_value = denominator_result.get('result')
                            if isinstance(denominator_result_value, (pd.Series, pd.DataFrame)):
                                if len(denominator_result_value) > 0:
                                    denominator_result_value = denominator_result_value.iloc[0] if isinstance(denominator_result_value, pd.Series) else denominator_result_value.iloc[0, 0]
                                else:
                                    denominator_result_value = None
                            denominator_values[period_name] = float(denominator_result_value) if denominator_result_value is not None and pd.notna(denominator_result_value) else None
                        except Exception as e:
                            print(f"    Warning: Could not calculate numerator/denominator for {period_name}: {e}")
                            numerator_values[period_name] = None
                            denominator_values[period_name] = None
                    
                    # For single source or multi-period, substitute Period1/Period2 Numerator/Denominator
                    if will_calculate_numerator_denominator:
                        formula = _substituteNumeratorDenominator(
                            formula=formula,
                            numerator_formula=numerator_formula,
                            denominator_formula=denominator_formula,
                            source_mapping=source_mapping
                        )
                        print(f"\n  Detail {detail_idx + 1} (ID: {detail_id})")
                        print(f"    Original Formula: {original_formula}")
                        print(f"    Substituted Formula: {formula[:200]}..." if len(formula) > 200 else f"    Substituted Formula: {formula}")
        
        # Execute the ratio calculation
        # Note: align_data=False and align_key=None because ratios are single aggregated values
        # For dual source, ensure Period1 maps to both SourceA and SourceB correctly
        # The formula substitution converts SourceA/SourceB to Period1, so we need proper mapping
        formula_source_mapping = source_mapping.copy() if source_mapping else {}
        if is_dual_source and period_dates:
            # For dual source, add Period1 mapping to ensure SourceA and SourceB references work correctly
            # The formula uses Period1 for both SourceA and SourceB after substitution
            period_name = list(period_dates.keys())[0] if period_dates else None
            if period_name:
                # The source_mapping should already have SourceA and SourceB mapped to actual source names
                # But we need to ensure that when Period1 is referenced in SourceA context, it uses SourceA's source
                # And when Period1 is referenced in SourceB context, it uses SourceB's source
                # Since the formula substitution creates Period1 references for both, we need a mapping that works
                # The formula validator should handle this by checking the source_mapping for Period1->SourceA/SourceB
                # But for now, we'll keep the original source_mapping which should work
                # Actually, the issue is that both SourceA and SourceB references get converted to Period1
                # So we need to ensure the source_mapping has Period1->SourceA and Period1->SourceB mappings
                # But that's not possible with a single mapping. The formula validator needs to handle this differently.
                # Let me check if we need to update the source_mapping structure...
                pass  # Keep original source_mapping for now
        
        result = validateFormulaWithDatabase(
            formula=formula,
            client_id=client_id,
            fund_id=fund_id,
            period_dates=period_dates,
            threshold=threshold,
            align_data=False,  # Ratios don't need alignment (single values per period)
            align_key=None,  # No alignment keys needed for ratios
            filter_condition=final_filter,
            filter_type=detail_filter_type,
            source_mapping=formula_source_mapping
        )
        
        # Extract results
        passed_count = result.get('passed_count', 0)
        failed_count = result.get('failed_count', 0)
        total_count = result.get('total_count', 0)
        
        detail_status = 'passed' if failed_count == 0 else 'failed'
        
        print(f"Result: {detail_status.upper()} - {passed_count}/{total_count} passed, {failed_count} failed")
        
        # Store summary information
        detail_result = {
            'intratiodetailid': detail_id,
            'formula': original_formula,  # Store original formula with placeholders
            'substituted_formula': formula,  # Store substituted formula that was actually executed
            'filter': final_filter,
            'filter_type': detail_filter_type,
            'status': detail_status,
            'passed_count': passed_count,
            'failed_count': failed_count,
            'total_count': total_count,
            'passed_items': result.get('passed_items', []),
            'failed_items': result.get('failed_items', []),
            'combined_df': result.get('combined_df'),  # Always include combined_df for CSV export
            'numerator_values': numerator_values,  # Store numerator values per period or per source
            'denominator_values': denominator_values,  # Store denominator values per period or per source
            'is_dual_source': is_dual_source,  # Flag to indicate dual source scenario
            'source_mapping': source_mapping  # Store source mapping for reference
        }
        
        # Optionally include full result data (can be large)
        if include_full_data:
            detail_result['result'] = result.get('result')
            detail_result['variables'] = result.get('variables', {})
        
        return detail_result
        
    except Exception as e:
        print(f"    ERROR: {str(e)}")
        import traceback
        traceback.print_exc()
        
        return {
            'intratiodetailid': detail_id,
            'formula': original_formula,
            'substituted_formula': formula,
            'filter': final_filter,
            'filter_type': detail_filter_type,
            'status': 'error',
            'error': str(e)
        }


def _processRatioDetail(
    detail: Dict[str, Any],
    detail_idx: int,
    ratio_config: Dict[str, Any],
    client_id: int,
    fund_id: int,
    period_dates: Dict[str, str],
    include_full_data: bool,
    db_manager
) -> Dict[str, Any]:
    """
    Process a single ratio detail
    
    Note: Ratios produce single aggregated values per period/source, so alignment keys
    are not needed (unlike validations which compare individual records).
    
    Args:
        detail: Ratio detail dictionary
        detail_idx: Index of detail in list
        ratio_config: Ratio configuration dictionary
        client_id: Client ID
        fund_id: Fund ID
        period_dates: Dictionary mapping period names to dates
        include_full_data: Whether to include full result DataFrames/Series
        db_manager: Database manager instance
    
    Returns:
        Dictionary containing detail result
    """
    detail_id = detail.get('intratiodetailid')
    formula = detail.get('vcformula')
    numerator_formula = detail.get('vcnumerator') or ''
    denominator_formula = detail.get('vcdenominator') or ''
    detail_filter = detail.get('vcfilter')
    detail_filter_type = detail.get('vcfiltertype', 'I')  # Default to 'I' (Include)
    
    # Get threshold from config (prefer config threshold over master threshold)
    threshold = ratio_config.get('config_threshold')
    if threshold is None:
        threshold = ratio_config.get('intthreshold')
    
    # Use detail filter if available, otherwise use config filter
    filter_condition = ratio_config.get('vccondition')
    final_filter = detail_filter if detail_filter else filter_condition
    
    # Substitute Numerator and Denominator placeholders if formula contains them
    original_formula = formula
    print(f"DEBUG - Formula: {formula}")
    print(f"DEBUG - Numerator Formula: {numerator_formula}")
    print(f"DEBUG - Denominator Formula: {denominator_formula}")
    if formula and (numerator_formula or denominator_formula):

        if 'Numerator' in formula or 'Denominator' in formula:
            formula = _substituteNumeratorDenominator(
                formula=formula,
                numerator_formula=numerator_formula,
                denominator_formula=denominator_formula,
                source_mapping=None  # No source_mapping for single source ratios
            )
            print(f"\n  Detail {detail_idx + 1} (ID: {detail_id})")
            print(f"    Original Formula: {original_formula}")
            print(f"    Substituted Formula: {formula[:200]}..." if len(formula) > 200 else f"    Substituted Formula: {formula}")
        else:
            print(f"\n  Detail {detail_idx + 1} (ID: {detail_id})")
            print(f"    Formula: {formula}")
    else:
        print(f"\n  Detail {detail_idx + 1} (ID: {detail_id})")
        print(f"    Formula: {formula}")
    
    print(f"    Filter: {final_filter}")
    print(f"    Filter Type: {detail_filter_type} ({'Exclude' if detail_filter_type == 'E' else 'Include'})")
    
    if not formula:
        print(f"    Warning: No formula found for detail {detail_id}")
        return {
            'intratiodetailid': detail_id,
            'formula': original_formula,
            'filter': final_filter,
            'filter_type': detail_filter_type,
            'status': 'skipped',
            'error': 'No formula found'
        }
    
    try:
        # Calculate numerator and denominator values for each period if formulas are available
        numerator_values = {}  # {period_name: value}
        denominator_values = {}  # {period_name: value}
        
        if numerator_formula and denominator_formula:
            # Extract base formulas (remove Displayname prefix)
            numerator_base = _extractFormulaFromDisplayName(numerator_formula)[1]
            denominator_base = _extractFormulaFromDisplayName(denominator_formula)[1]
            
            if numerator_base and denominator_base:
                # Calculate numerator and denominator for each period
                for period_name in period_dates.keys():
                    try:
                        # Add period prefix to numerator formula
                        numerator_period_formula = _addPeriodPrefixToFormula(numerator_base, period_name)
                        # Execute numerator formula for this period
                        numerator_result = validateFormulaWithDatabase(
                            formula=numerator_period_formula,
                            client_id=client_id,
                            fund_id=fund_id,
                            period_dates={period_name: period_dates[period_name]},  # Single period
                            threshold=None,
                            align_data=False,
                            align_key=None,
                            filter_condition=final_filter,
                            filter_type=detail_filter_type
                        )
                        # Extract scalar value
                        numerator_result_value = numerator_result.get('result')
                        if isinstance(numerator_result_value, (pd.Series, pd.DataFrame)):
                            if len(numerator_result_value) > 0:
                                numerator_result_value = numerator_result_value.iloc[0] if isinstance(numerator_result_value, pd.Series) else numerator_result_value.iloc[0, 0]
                            else:
                                numerator_result_value = None
                        numerator_values[period_name] = float(numerator_result_value) if numerator_result_value is not None and pd.notna(numerator_result_value) else None
                        
                        # Add period prefix to denominator formula
                        denominator_period_formula = _addPeriodPrefixToFormula(denominator_base, period_name)
                        # Execute denominator formula for this period
                        denominator_result = validateFormulaWithDatabase(
                            formula=denominator_period_formula,
                            client_id=client_id,
                            fund_id=fund_id,
                            period_dates={period_name: period_dates[period_name]},  # Single period
                            threshold=None,
                            align_data=False,
                            align_key=None,
                            filter_condition=final_filter,
                            filter_type=detail_filter_type
                        )
                        # Extract scalar value
                        denominator_result_value = denominator_result.get('result')
                        if isinstance(denominator_result_value, (pd.Series, pd.DataFrame)):
                            if len(denominator_result_value) > 0:
                                denominator_result_value = denominator_result_value.iloc[0] if isinstance(denominator_result_value, pd.Series) else denominator_result_value.iloc[0, 0]
                            else:
                                denominator_result_value = None
                        denominator_values[period_name] = float(denominator_result_value) if denominator_result_value is not None and pd.notna(denominator_result_value) else None
                    except Exception as e:
                        print(f"    Warning: Could not calculate numerator/denominator for {period_name}: {e}")
                        numerator_values[period_name] = None
                        denominator_values[period_name] = None
        
        # Execute the ratio calculation
        # Note: align_data=False and align_key=None because ratios are single aggregated values
        result = validateFormulaWithDatabase(
            formula=formula,
            client_id=client_id,
            fund_id=fund_id,
            period_dates=period_dates,
            threshold=threshold,
            align_data=False,  # Ratios don't need alignment (single values per period)
            align_key=None,  # No alignment keys needed for ratios
            filter_condition=final_filter,
            filter_type=detail_filter_type
        )
        
        # Extract results
        passed_count = result.get('passed_count', 0)
        failed_count = result.get('failed_count', 0)
        total_count = result.get('total_count', 0)
        
        detail_status = 'passed' if failed_count == 0 else 'failed'
        
        print(f"Result: {detail_status.upper()} - {passed_count}/{total_count} passed, {failed_count} failed")
        
        # Store summary information
        detail_result = {
            'intratiodetailid': detail_id,
            'formula': original_formula,  # Store original formula with placeholders
            'substituted_formula': formula,  # Store substituted formula that was actually executed
            'filter': final_filter,
            'filter_type': detail_filter_type,
            'status': detail_status,
            'passed_count': passed_count,
            'failed_count': failed_count,
            'total_count': total_count,
            'passed_items': result.get('passed_items', []),
            'failed_items': result.get('failed_items', []),
            'combined_df': result.get('combined_df'),  # Always include combined_df for CSV export
            'numerator_values': numerator_values,  # Store numerator values per period or per source
            'denominator_values': denominator_values,  # Store denominator values per period or per source
            'is_dual_source': False,  # Default to False for single source ratios (not using WithSources)
            'source_mapping': {}  # Default to empty for single source ratios (not using WithSources)
        }
        
        # Optionally include full result data (can be large)
        if include_full_data:
            detail_result['result'] = result.get('result')
            detail_result['variables'] = result.get('variables', {})
        
        return detail_result
        
    except Exception as e:
        print(f"    ERROR: {str(e)}")
        import traceback
        traceback.print_exc()
        
        return {
            'intratiodetailid': detail_id,
            'formula': original_formula,
            'substituted_formula': formula,
            'filter': final_filter,
            'filter_type': detail_filter_type,
            'status': 'error',
            'error': str(e)
        }


def _processRatioWithSources(
    ratio_config: Dict[str, Any],
    client_id: int,
    fund_id: int,
    period_dates: Dict[str, str],
    source_mapping: Dict[str, str],
    include_full_data: bool,
    db_manager
) -> Dict[str, Any]:
    """
    Process a single ratio configuration with source mapping support
    
    Note: Ratios produce single aggregated values per period/source, so alignment
    is not needed (unlike validations which compare individual records).
    
    Args:
        ratio_config: Ratio configuration dictionary
        client_id: Client ID
        fund_id: Fund ID
        period_dates: Dictionary mapping period names to dates
        source_mapping: Dictionary mapping source names to actual source values
        include_full_data: Whether to include full result DataFrames/Series
        db_manager: Database manager instance
    
    Returns:
        Dictionary containing ratio result
    """
    ratio_id = ratio_config.get('intratiomasterid')
    ratio_name = ratio_config.get('vcrationame', 'Unknown')
    ratio_type = ratio_config.get('vctype', 'Unknown')
    
    # Get threshold from config (prefer config threshold over master threshold)
    threshold = ratio_config.get('config_threshold')
    if threshold is None:
        threshold = ratio_config.get('intthreshold')
    
    # Get filter condition from config
    filter_condition = ratio_config.get('vccondition')
    
    print(f"\n{'='*80}")
    print(f"Processing Ratio: {ratio_name} (ID: {ratio_id}, Type: {ratio_type})")
    print(f"Threshold: {threshold}")
    print(f"Filter Condition: {filter_condition}")
    print(f"Source Mapping: {source_mapping}")
    print(f"{'='*80}")
    
    # Get ratio details (formulas and filters)
    details = ratio_config.get('details', [])
    
    if not details:
        print(f"  Warning: No details found for ratio {ratio_name}")
        return {
            'ratio_info': {
                'intratiomasterid': ratio_id,
                'vcrationame': ratio_name,
                'vctype': ratio_type,
                'threshold': threshold,
                'filter_condition': filter_condition
            },
            'detail_results': [],
            'overall_status': 'skipped',
            'error': 'No ratio details found'
        }
    
    detail_results = []
    overall_passed = True
    
    # Process each ratio detail
    for detail_idx, detail in enumerate(details):
        detail_result = _processRatioDetailWithSources(
            detail=detail,
            detail_idx=detail_idx,
            ratio_config=ratio_config,
            client_id=client_id,
            fund_id=fund_id,
            period_dates=period_dates,
            source_mapping=source_mapping,
            include_full_data=include_full_data,
            db_manager=db_manager
        )
        
        detail_results.append(detail_result)
        
        # Update overall status
        if detail_result.get('status') == 'failed' or detail_result.get('status') == 'error':
            overall_passed = False
    
    # Compile overall result for this ratio
    overall_status = 'passed' if overall_passed else 'failed'
    
    return {
        'ratio_info': {
            'intratiomasterid': ratio_id,
            'vcrationame': ratio_name,
            'vctype': ratio_type,
            'vcdescription': ratio_config.get('vcdescription'),
            'threshold': threshold,
            'filter_condition': filter_condition
        },
        'detail_results': detail_results,
        'overall_status': overall_status,
        'total_details': len(details),
        'passed_details': sum(1 for r in detail_results if r.get('status') == 'passed'),
        'failed_details': sum(1 for r in detail_results if r.get('status') == 'failed')
        }


def _processRatio(
    ratio_config: Dict[str, Any],
    client_id: int,
    fund_id: int,
    period_dates: Dict[str, str],
    include_full_data: bool,
    db_manager
) -> Dict[str, Any]:
    """
    Process a single ratio configuration
    
    Note: Ratios produce single aggregated values per period/source, so alignment
    is not needed (unlike validations which compare individual records).
    
    Args:
        ratio_config: Ratio configuration dictionary
        client_id: Client ID
        fund_id: Fund ID
        period_dates: Dictionary mapping period names to dates
        include_full_data: Whether to include full result DataFrames/Series
        db_manager: Database manager instance
    
    Returns:
        Dictionary containing ratio result
    """
    ratio_id = ratio_config.get('intratiomasterid')
    ratio_name = ratio_config.get('vcrationame', 'Unknown')
    ratio_type = ratio_config.get('vctype', 'Unknown')
    
    # Get threshold from config (prefer config threshold over master threshold)
    threshold = ratio_config.get('config_threshold')
    if threshold is None:
        threshold = ratio_config.get('intthreshold')
    
    # Get filter condition from config
    filter_condition = ratio_config.get('vccondition')
    
    print(f"\n{'='*80}")
    print(f"Processing Ratio: {ratio_name} (ID: {ratio_id}, Type: {ratio_type})")
    print(f"Threshold: {threshold}")
    print(f"Filter Condition: {filter_condition}")
    print(f"{'='*80}")
    
    # Get ratio details (formulas and filters)
    details = ratio_config.get('details', [])
    
    if not details:
        print(f"  Warning: No details found for ratio {ratio_name}")
        return {
            'ratio_info': {
                'intratiomasterid': ratio_id,
                'vcrationame': ratio_name,
                'vctype': ratio_type,
                'threshold': threshold,
                'filter_condition': filter_condition
            },
            'detail_results': [],
            'overall_status': 'skipped',
            'error': 'No ratio details found'
        }
    
    detail_results = []
    overall_passed = True
    
    # Process each ratio detail
    for detail_idx, detail in enumerate(details):
        detail_result = _processRatioDetail(
            detail=detail,
            detail_idx=detail_idx,
            ratio_config=ratio_config,
            client_id=client_id,
            fund_id=fund_id,
            period_dates=period_dates,
            include_full_data=include_full_data,
            db_manager=db_manager
        )
        
        detail_results.append(detail_result)
        
        # Update overall status
        if detail_result.get('status') == 'failed' or detail_result.get('status') == 'error':
            overall_passed = False
    
    # Compile overall result for this ratio
    overall_status = 'passed' if overall_passed else 'failed'
    
    return {
        'ratio_info': {
            'intratiomasterid': ratio_id,
            'vcrationame': ratio_name,
            'vctype': ratio_type,
            'vcdescription': ratio_config.get('vcdescription'),
            'threshold': threshold,
            'filter_condition': filter_condition
        },
        'detail_results': detail_results,
        'overall_status': overall_status,
        'total_details': len(details),
        'passed_details': sum(1 for r in detail_results if r.get('status') == 'passed'),
        'failed_details': sum(1 for r in detail_results if r.get('status') == 'failed')
    }


def executeAllRatios(
    client_id: int,
    fund_id: int,
    period_dates: Dict[str, str],
    include_full_data: bool = False
) -> List[Dict[str, Any]]:
    """
    Execute all active ratios for a given client and fund
    
    Note: Ratios produce single aggregated values per period/source, so alignment
    keys and data alignment are not needed (unlike validations which compare individual records).
    
    Args:
        client_id: Client ID
        fund_id: Fund ID
        period_dates: Dictionary mapping period names to dates
            Example: {'Period1': '2024-01-31', 'Period2': '2024-02-29'}
        include_full_data: Whether to include full result DataFrames/Series (default: False)
            If False, only summary statistics are included
    
    Returns:
        List of ratio results, each containing:
        - ratio_info: Ratio master and configuration info
        - detail_results: List of results for each ratio detail
        - overall_status: 'passed' or 'failed' based on any failures
    """
    
    db_validation_service = DatabaseValidationService()
    
    # Get all active ratio configurations
    ratio_configs = db_validation_service.get_active_ratio_config_details(
        client_id=client_id,
        fund_id=fund_id
    )
    
    if not ratio_configs:
        print(f"No active ratios found for client_id={client_id}, fund_id={fund_id}")
        return []
    
    print(f"Found {len(ratio_configs)} active ratio(s)")
    
    all_results = []
    
    # Get database manager once for all ratios
    db_manager = get_database_manager()
    
    # Process each ratio
    for ratio_config in ratio_configs:
        ratio_result = _processRatio(
            ratio_config=ratio_config,
            client_id=client_id,
            fund_id=fund_id,
            period_dates=period_dates,
            include_full_data=include_full_data,
            db_manager=db_manager
        )
        all_results.append(ratio_result)
    
    return all_results


def executeAllRatiosWithSources(
    client_id: int,
    fund_id: int,
    period_dates: Dict[str, str],
    source_a: str,
    source_b: Optional[str] = None,
    include_full_data: bool = False
) -> List[Dict[str, Any]]:
    """
    Execute all active ratios with vcsourcetype='Dual' for a given client and fund
    Supports both Case 1 (Period with Source) and Case 2 (Source only) scenarios
    
    Note: Ratios produce single aggregated values per period/source, so alignment
    keys and data alignment are not needed (unlike validations which compare individual records).
    
    Args:
        client_id: Client ID
        fund_id: Fund ID
        period_dates: Dictionary mapping period names to dates
            Example Case 1: {'Period1': '2024-01-31', 'Period2': '2024-02-29'}
            Example Case 2: {'Period1': '2024-01-31'}  # Single period for Source comparison
        source_a: Source A name (e.g., 'Bluefield')
        source_b: Optional Source B name (e.g., 'Harborview'). If None, only source_a is used
        include_full_data: Whether to include full result DataFrames/Series (default: False)
            If False, only summary statistics are included
    
    Returns:
        List of ratio results, each containing:
        - ratio_info: Ratio master and configuration info
        - detail_results: List of results for each ratio detail
        - overall_status: 'passed' or 'failed' based on any failures
    """
    
    db_validation_service = DatabaseValidationService()
    
    # Check if source_b is provided (not None and not empty string)
    has_source_b = source_b is not None and str(source_b).strip() != ''
    num_periods = len(period_dates) if period_dates else 0
    
    # Determine which case we're in:
    # Case 1: 2 sources, 1 period -> SourceA vs SourceB (Dual Source)
    # Case 2: 1 source, 2 periods -> Period1 vs Period2 (Single Source)
    is_case_1 = has_source_b and num_periods == 1
    is_case_2 = not has_source_b and num_periods > 1
    
    # Get all active ratio configurations
    all_ratio_configs = db_validation_service.get_active_ratio_config_details(
        client_id=client_id,
        fund_id=fund_id
    )
    
    # Filter ratios based on case:
    # Case 1: Filter for Dual Source ratios (vcsourcetype='Dual')
    # Case 2: Filter for Single Source ratios (vcsourcetype='Single')
    if is_case_1:
        # Case 1: 2 sources, 1 period - need Dual Source ratios
        ratio_configs = [
            config for config in all_ratio_configs 
            if config.get('vcsourcetype', '').lower() == 'dual'
        ]
        case_description = "dual source"
    elif is_case_2:
        # Case 2: 1 source, 2 periods - need Single Source ratios
        ratio_configs = [
            config for config in all_ratio_configs 
            if config.get('vcsourcetype', '').lower() == 'single'
        ]
        case_description = "single source"
    else:
        # Fallback: use Dual Source if source_b is provided, otherwise Single Source
        if has_source_b:
            ratio_configs = [
                config for config in all_ratio_configs 
                if config.get('vcsourcetype', '').lower() == 'dual'
            ]
            case_description = "dual source"
        else:
            ratio_configs = [
                config for config in all_ratio_configs 
                if config.get('vcsourcetype', '').lower() == 'single'
            ]
            case_description = "single source"
    
    if not ratio_configs:
        print(f"No active {case_description} ratios found for client_id={client_id}, fund_id={fund_id}")
        return []
    
    print(f"Found {len(ratio_configs)} active {case_description} ratio(s)")
    print(f"Source A: {source_a}, Source B: {source_b}")
    
    # Build source_mapping based on scenario
    source_mapping = {}
    
    if has_source_b and num_periods == 1:
        # Case 1: 2 sources, 1 period (SourceA vs SourceB) - Dual Source
        source_mapping = {
            'SourceA': str(source_a).strip(),
            'SourceB': str(source_b).strip()
        }
        print(f"Using Case 1: 2 sources, 1 period (SourceA={source_a} vs SourceB={source_b}) - Dual Source")
        print(f"DEBUG - source_mapping: {source_mapping}")
    elif not has_source_b and num_periods > 1:
        # Case 2: 1 source, 2 periods (Period1 vs Period2) - Single Source
        periods = sorted(period_dates.keys())
        source_mapping = {
            'SourceA': source_a  # Same source for both periods
        }
        print(f"Using Case 2: 1 source, 2 periods (Period1 vs Period2 with source={source_a}) - Single Source")
    elif has_source_b and num_periods > 1:
        # Special case: 2 sources AND 2 periods -> Period1->SourceA, Period2->SourceB
        periods = sorted(period_dates.keys())
        source_mapping = {
            'Period1': 'SourceA' if len(periods) > 0 else None,
            'Period2': 'SourceB' if len(periods) > 1 else None,
            'SourceA': source_a,
            'SourceB': source_b
        }
        print(f"Using special case: 2 sources, 2 periods (Period1->SourceA={source_a}, Period2->SourceB={source_b})")
    else:
        # Single source, single period (not dual source scenario)
        source_mapping = {
            'SourceA': source_a
        }
        print(f"Using single source, single period: {source_a}")
    
    all_results = []
    
    # Get database manager once for all ratios
    db_manager = get_database_manager()
    
    # Process each ratio
    for ratio_config in ratio_configs:
        ratio_result = _processRatioWithSources(
            ratio_config=ratio_config,
            client_id=client_id,
            fund_id=fund_id,
            period_dates=period_dates,
            source_mapping=source_mapping,
            include_full_data=include_full_data,
            db_manager=db_manager
        )
        all_results.append(ratio_result)
    
    return all_results


def printRatioSummary(results: List[Dict[str, Any]]):
    """
    Print a summary of ratio execution results
    
    Args:
        results: List of ratio results from executeAllRatios
    """
    if not results:
        print("\nNo ratio results to display")
        return
    
    print("\n" + "="*80)
    print("RATIO EXECUTION SUMMARY")
    print("="*80)
    
    total_ratios = len(results)
    passed_ratios = sum(1 for r in results if r.get('overall_status') == 'passed')
    failed_ratios = sum(1 for r in results if r.get('overall_status') == 'failed')
    
    print(f"\nTotal Ratios: {total_ratios}")
    print(f"  Passed: {passed_ratios}")
    print(f"  Failed: {failed_ratios}")
    
    # Print details for each ratio
    for idx, result in enumerate(results, 1):
        ratio_info = result.get('ratio_info', {})
        ratio_name = ratio_info.get('vcrationame', 'Unknown')
        overall_status = result.get('overall_status', 'unknown')
        detail_results = result.get('detail_results', [])
        
        status_symbol = "✓" if overall_status == 'passed' else "✗"
        print(f"\n{idx}. {status_symbol} {ratio_name} ({overall_status.upper()})")
        print(f"   Details: {len(detail_results)} detail(s)")
        
        for detail_idx, detail_result in enumerate(detail_results, 1):
            detail_status = detail_result.get('status', 'unknown')
            passed_count = detail_result.get('passed_count', 0)
            failed_count = detail_result.get('failed_count', 0)
            total_count = detail_result.get('total_count', 0)
            
            detail_symbol = "✓" if detail_status == 'passed' else "✗"
            print(f"      {detail_idx}. {detail_symbol} Detail {detail_result.get('intratiodetailid')} ({detail_status})")
            if total_count > 0:
                print(f"         Passed: {passed_count}/{total_count}, Failed: {failed_count}")
            if detail_result.get('error'):
                print(f"         Error: {detail_result.get('error')}")


# Import shared utility functions from execute_validations
from execute_validations import (
    saveResultsToFiles,
    _getClientSchema,
    _createProcessInstance,
    _createProcessInstanceDetail,
    _updateProcessInstanceStatus,
    convertToSerializable
)
from sqlalchemy import text


def saveRatioResultsToDatabaseWithSources(
    results: List[Dict[str, Any]],
    client_id: int,
    fund_id: int,
    period_dates: Dict[str, str],
    source_a: str,
    source_b: Optional[str] = None,
    db_manager = None
) -> bool:
    """
    Save ratio results to database with source information
    
    Args:
        results: List of ratio results from executeAllRatiosWithSources
        client_id: Client ID
        fund_id: Fund ID
        period_dates: Dictionary mapping period names to dates
        source_a: Source A name
        source_b: Optional Source B name
        db_manager: Database manager instance
    
    Returns:
        True if successful, False otherwise
    """
    if not db_manager:
        db_manager = get_database_manager()
    
    # Get client schema
    schema_name = _getClientSchema(client_id, db_manager)
    if not schema_name:
        print(f"Error: Could not get client schema for client_id={client_id}")
        return False
    
    # Extract dates from period_dates for process instance
    # For Case 2 (Source only with single period), only date_a should be set
    date_a = None
    date_b = None
    if period_dates:
        periods = sorted(period_dates.keys())
        if len(periods) > 0:
            date_a = period_dates.get(periods[0])
            # Only set date_b if there are at least 2 periods with valid dates
            if len(periods) > 1:
                date_b = period_dates.get(periods[1])
            else:
                # Explicitly set to None when only one period is provided
                date_b = None
        else:
            date_a = None
            date_b = None
    
    # Create process instance with source information
    process_instance_id = _createProcessInstance(
        client_id=client_id,
        fund_id=fund_id,
        validus_type='Ratio',
        source_type='Dual' if source_b else 'Single',
        source_a=source_a,
        source_b=source_b,
        date_a=date_a,
        date_b=date_b,
        currency=None,
        user_id=None,
        db_manager=db_manager
    )
    
    if not process_instance_id:
        print("Error: Could not create process instance")
        return False
    
    print(f"Created process instance: {process_instance_id}")
    
    # Pass the created process_instance_id to saveRatioResultsToDatabase
    return saveRatioResultsToDatabase(results, client_id, fund_id, period_dates, db_manager, process_instance_id=process_instance_id)


def saveRatioResultsToDatabase(
    results: List[Dict[str, Any]],
    client_id: int,
    fund_id: int,
    period_dates: Dict[str, str],
    db_manager,
    process_instance_id: Optional[int] = None
) -> bool:
    """
    Save ratio results to {client_schema}.tbl_ratio_result table
    Also creates entries in validus.tbl_process_instance and validus.tbl_process_instance_details
    
    Args:
        results: List of ratio results from executeAllRatios
        client_id: Client ID
        fund_id: Fund ID
        period_dates: Dictionary mapping period names to dates (e.g., {'Period1': '2024-01-31', 'Period2': '2024-02-29'})
        db_manager: Database manager instance
        process_instance_id: Optional process instance ID. If provided, uses this instead of creating a new one.
    
    Returns:
        True if successful, False otherwise
    """
    if not db_manager:
        print("Error: Database manager not available")
        return False
    
    # Debug: Print period_dates
    # print(f"DEBUG - saveRatioResultsToDatabase called with period_dates: {period_dates}")
    
    # Get client schema
    schema_name = _getClientSchema(client_id, db_manager)
    if not schema_name:
        print(f"Error: Could not get client schema for client_id={client_id}")
        return False
    
    # Extract dates from period_dates for process instance
    date_a = None
    date_b = None
    if period_dates:
        periods = sorted(period_dates.keys())
        # print(f"DEBUG - Extracted periods: {periods}")
        if len(periods) > 0:
            date_a = period_dates.get(periods[0])
            # print(f"DEBUG - date_a: {date_a}")
            # Only set date_b if there are at least 2 periods with valid dates
            if len(periods) > 1:
                date_b = period_dates.get(periods[1])
                # print(f"DEBUG - date_b: {date_b}")
            else:
                # Explicitly set to None when only one period is provided
                date_b = None
                # print(f"DEBUG - /date_b: None (only one period provided)")
        else:
            date_a = None
            date_b = None
    
    # Create process instance only if not provided
    if not process_instance_id:
        process_instance_id = _createProcessInstance(
            client_id=client_id,
            fund_id=fund_id,
            validus_type='Ratio',
            source_type=None,
            source_a=None,
            source_b=None,
            date_a=date_a,
            date_b=date_b,
            currency=None,
            user_id=None,
            db_manager=db_manager
        )
        
        if not process_instance_id:
            print("Error: Could not create process instance")
            return False
        
        print(f"Created process instance: {process_instance_id}")
    else:
        print(f"Using provided process instance: {process_instance_id}")
    
    # Get database session
    session = db_manager.get_session_with_schema(schema_name)
    
    try:
        records_inserted = 0
        
        # Counters for multi-period tracking
        match_id_counter = 0
        side_a_counter = 0
        side_b_counter = 0
        
        # Process each ratio result
        for ratio_result in results:
            ratio_info = ratio_result.get('ratio_info', {})
            ratio_id = ratio_info.get('intratiomasterid')
            detail_results = ratio_result.get('detail_results', [])
            
            # Get ratio configuration ID
            session_validus = db_manager.get_session_with_schema('validus')
            try:
                from database_models import RatioConfiguration
                ratio_config = session_validus.query(RatioConfiguration).filter(
                    RatioConfiguration.intclientid == client_id,
                    RatioConfiguration.intfundid == fund_id,
                    RatioConfiguration.intratiomasterid == ratio_id,
                    RatioConfiguration.isactive == True
                ).first()
                
                intratioconfigurationid = ratio_config.intratioconfigurationid if ratio_config else None
            except Exception as e:
                print(f"Warning: Could not get ratio configuration ID: {e}")
                intratioconfigurationid = None
            finally:
                session_validus.close()
            
            # Process each detail result
            for detail_result in detail_results:
                detail_id = detail_result.get('intratiodetailid')
                combined_df = detail_result.get('combined_df')
                # Use substituted formula if available, otherwise use original formula
                formula = detail_result.get('substituted_formula') or detail_result.get('formula', '')
                
                # print(f"DEBUG - Processing ratio detail {detail_id}, combined_df shape: {combined_df.shape if combined_df is not None and isinstance(combined_df, pd.DataFrame) else 'None/Not DataFrame'}")
                
                if combined_df is None or not isinstance(combined_df, pd.DataFrame) or combined_df.empty:
                    print(f"DEBUG - Skipping detail {detail_id}: combined_df is None, empty, or not a DataFrame")
                    continue
                
                # Create process instance detail
                _createProcessInstanceDetail(
                    process_instance_id=process_instance_id,
                    data_load_instance_id=None,
                    db_manager=db_manager
                )
                
                # Get data model ID from detail
                session_validus = db_manager.get_session_with_schema('validus')
                try:
                    from database_models import RatioDetails
                    ratio_detail = session_validus.query(RatioDetails).filter(
                        RatioDetails.intratiodetailid == detail_id
                    ).first()
                    
                    intdatamodelid = ratio_detail.intdatamodelid if ratio_detail else None
                except Exception as e:
                    print(f"Warning: Could not get data model ID for detail {detail_id}: {e}")
                    intdatamodelid = None
                finally:
                    session_validus.close()
                
                # Process each row in combined_df
                # print(f"DEBUG - Starting to process {len(combined_df)} rows from combined_df")
                for idx, row in combined_df.iterrows():
                    # print(f"DEBUG - Processing row {idx} (type: {type(idx)})")
                    
                    # Get formula output (result column)
                    result_value = row.get('result')
                    
                    # Extract scalar value if result_value is array/Series
                    if hasattr(result_value, '__len__') and not isinstance(result_value, str):
                        try:
                            if isinstance(result_value, pd.Series):
                                result_value = result_value.iloc[0] if len(result_value) > 0 else None
                            elif isinstance(result_value, np.ndarray):
                                result_value = result_value[0] if len(result_value) > 0 else None
                            elif isinstance(result_value, (list, tuple)):
                                result_value = result_value[0] if len(result_value) > 0 else None
                        except (IndexError, TypeError):
                            result_value = None
                    
                    # Convert to float if possible
                    intformulaoutput = None
                    vcformulaoutput = None
                    if result_value is not None:
                        try:
                            if pd.notna(result_value):
                                float_value = float(result_value)
                                
                                # Check for infinity and NaN values - set to None (NULL) for database
                                if np.isinf(float_value) or np.isnan(float_value):
                                    intformulaoutput = None  # Set to NULL for database
                                    # Store string representation for vcformulaoutput
                                    if np.isinf(float_value):
                                        vcformulaoutput = 'inf' if float_value > 0 else '-inf'
                                    else:  # NaN
                                        vcformulaoutput = 'nan'
                                else:
                                    # Valid finite number
                                    intformulaoutput = float_value
                                vcformulaoutput = str(result_value)
                        except (ValueError, TypeError):
                            vcformulaoutput = str(result_value) if result_value is not None else None
                    
                    # Get numerator and denominator values from detail_result (calculated per period)
                    numerator_values = detail_result.get('numerator_values', {})
                    denominator_values = detail_result.get('denominator_values', {})
                    
                    # Get current datetime for dtactiontime
                    current_datetime = datetime.now()
                    
                    # Determine status based on threshold
                    threshold = ratio_info.get('threshold')
                    vcstatus = 'Passed'
                    if threshold is not None and intformulaoutput is not None:
                        try:
                            threshold_value = float(threshold)
                            result_abs = abs(intformulaoutput)
                            vcstatus = 'Passed' if result_abs <= threshold_value else 'Failed'
                        except (ValueError, TypeError):
                            vcstatus = 'Unknown'
                    elif intformulaoutput is None:
                        # Set to 'Failed' for infinity/NaN values (stored as NULL), 'Unknown' for other invalid values
                        if result_value is not None and pd.notna(result_value):
                            try:
                                float_value = float(result_value)
                                if np.isinf(float_value) or np.isnan(float_value):
                                    vcstatus = 'Failed'  # Infinity or NaN is considered a failure
                                else:
                                    vcstatus = 'Unknown'
                            except (ValueError, TypeError):
                                vcstatus = 'Unknown'
                        else:
                            vcstatus = 'Unknown'
                    
                    # # Determine which periods are actually used in the formula
                    # # Extract periods from the formula string itself (not from period_dates)
                    # # Formula is already retrieved from detail_result above
                    # periods_used_in_formula = []
                    # if formula:
                    #     import re
                    #     # Find all Period references in the formula (e.g., Period1, Period2)
                    #     period_matches = re.findall(r'Period(\d+)', formula, re.IGNORECASE)
                    #     if period_matches:
                    #         # Get unique period names
                    #         period_names = [f'Period{num}' for num in period_matches]
                    #         periods_used_in_formula = sorted(list(set(period_names)))
                    
                    # # If no periods found in formula, check if we can infer from period_dates
                    # # But only use periods that actually exist in period_dates
                    # if not periods_used_in_formula and period_dates:
                    #     periods_used_in_formula = sorted(period_dates.keys())
                    
                    # Filter to only include periods that exist in period_dates
                    # periods = [p for p in periods_used_in_formula if p in period_dates] if period_dates else periods_used_in_formula
                    periods = [p for p in period_dates] if period_dates else []
                    is_multi_period = len(periods) > 1
                    
                    # Check if this is a dual source scenario
                    is_dual_source = detail_result.get('is_dual_source', False)
                    source_mapping = detail_result.get('source_mapping', {})
                    
                    # print(f"DEBUG - Row {idx}: periods={periods}, is_multi_period={is_multi_period}, is_dual_source={is_dual_source}")
                    
                    # For dual source (2 sources, 1 period), create separate rows for SourceA and SourceB
                    if is_dual_source:
                        match_id_counter += 1
                        intmatchid = match_id_counter
                        
                        # Helper function to sanitize numeric values
                        def sanitize_numeric_value(value):
                            """Convert infinity/NaN to None for database storage"""
                            if value is None:
                                return None
                            try:
                                float_val = float(value)
                                if np.isinf(float_val) or np.isnan(float_val):
                                    return None
                                return float_val
                            except (ValueError, TypeError):
                                return None
                        
                        # Create row for SourceA
                        side_a_counter += 1
                        vcside_a = 'A'
                        intsideuniqueid_a = side_a_counter
                        
                        # Get numerator and denominator for SourceA
                        intnumeratoroutput_a = sanitize_numeric_value(numerator_values.get('SourceA'))
                        intdenominatoroutput_a = sanitize_numeric_value(denominator_values.get('SourceA'))
                        
                        # Create row for SourceB
                        side_b_counter += 1
                        vcside_b = 'B'
                        intsideuniqueid_b = side_b_counter
                        
                        # Get numerator and denominator for SourceB
                        intnumeratoroutput_b = sanitize_numeric_value(numerator_values.get('SourceB'))
                        intdenominatoroutput_b = sanitize_numeric_value(denominator_values.get('SourceB'))
                        
                        # Insert row for SourceA
                        insert_sql = text(f"""
                            INSERT INTO {schema_name}.tbl_ratio_result (
                                intprocessinstanceid,
                                intdatamodelid,
                                intratioconfigurationid,
                                vcside,
                                intsideuniqueid,
                                intmatchid,
                                intnumeratoroutput,
                                intdenominatoroutput,
                                intformulaoutput,
                                vcformulaoutput,
                                vcstatus,
                                vcaction,
                                intactionuserid,
                                dtactiontime,
                                vccomment,
                                isactive
                            ) VALUES (
                                :intprocessinstanceid,
                                :intdatamodelid,
                                :intratioconfigurationid,
                                :vcside,
                                :intsideuniqueid,
                                :intmatchid,
                                :intnumeratoroutput,
                                :intdenominatoroutput,
                                :intformulaoutput,
                                :vcformulaoutput,
                                :vcstatus,
                                :vcaction,
                                :intactionuserid,
                                :dtactiontime,
                                :vccomment,
                                :isactive
                            )
                        """)
                        
                        insert_params_a = {
                            'intprocessinstanceid': process_instance_id,
                            'intdatamodelid': intdatamodelid,
                            'intratioconfigurationid': intratioconfigurationid,
                            'vcside': vcside_a,
                            'intsideuniqueid': intsideuniqueid_a,
                            'intmatchid': intmatchid,
                            'intnumeratoroutput': intnumeratoroutput_a,
                            'intdenominatoroutput': intdenominatoroutput_a,
                            'intformulaoutput': intformulaoutput,
                            'vcformulaoutput': vcformulaoutput,
                            'vcstatus': vcstatus,
                            'vcaction': None,
                            'intactionuserid': None,
                            'dtactiontime': current_datetime,
                            'vccomment': None,
                            'isactive': True
                        }
                        
                        try:
                            session.execute(insert_sql, insert_params_a)
                            records_inserted += 1
                            # print(f"DEBUG - Inserted ratio record for SourceA: numerator={intnumeratoroutput_a}, denominator={intdenominatoroutput_a}, result={intformulaoutput}")
                        except Exception as insert_error:
                            print(f"ERROR - Failed to insert ratio record for SourceA: {insert_error}")
                            raise
                        
                        # Insert row for SourceB
                        insert_params_b = {
                            'intprocessinstanceid': process_instance_id,
                            'intdatamodelid': intdatamodelid,
                            'intratioconfigurationid': intratioconfigurationid,
                            'vcside': vcside_b,
                            'intsideuniqueid': intsideuniqueid_b,
                            'intmatchid': intmatchid,
                            'intnumeratoroutput': intnumeratoroutput_b,
                            'intdenominatoroutput': intdenominatoroutput_b,
                            'intformulaoutput': intformulaoutput,
                            'vcformulaoutput': vcformulaoutput,
                            'vcstatus': vcstatus,
                            'vcaction': None,
                            'intactionuserid': None,
                            'dtactiontime': current_datetime,
                            'vccomment': None,
                            'isactive': True
                        }
                        
                        try:
                            session.execute(insert_sql, insert_params_b)
                            records_inserted += 1
                            print(f"DEBUG - Inserted ratio record for SourceB: numerator={intnumeratoroutput_b}, denominator={intdenominatoroutput_b}, result={intformulaoutput}")
                        except Exception as insert_error:
                            print(f"ERROR - Failed to insert ratio record for SourceB: {insert_error}")
                            raise
                    
                    # For ratios, we typically have single aggregated values per period
                    # If multi-period, create entries for each period that is actually used in the formula
                    elif is_multi_period:
                        match_id_counter += 1
                        intmatchid = match_id_counter
                        
                        # Insert entry for each period that is actually used in the formula
                        # The 'periods' list now contains only periods actually referenced in the formula
                        for period_idx, period_name in enumerate(periods):
                            # Map period index to side: first period = 'A', second period = 'B', etc.
                            # But only if we have exactly 2 periods, otherwise use index-based mapping
                            if len(periods) == 2:
                                vcside = 'A' if period_idx == 0 else 'B'  # Period1 = 'A', Period2 = 'B'
                            else:
                                # For more than 2 periods, use A, B, C, etc.
                                vcside = chr(ord('A') + period_idx) if period_idx < 26 else 'Z'
                            
                            # Increment the appropriate side counter
                            if vcside == 'A':
                                side_a_counter += 1
                                intsideuniqueid = side_a_counter
                            else:
                                side_b_counter += 1
                                intsideuniqueid = side_b_counter
                            
                            # Get numerator and denominator values for this period
                            # Sanitize infinity and NaN values to None (NULL)
                            def sanitize_numeric_value(value):
                                """Convert infinity/NaN to None for database storage"""
                                if value is None:
                                    return None
                                try:
                                    float_val = float(value)
                                    if np.isinf(float_val) or np.isnan(float_val):
                                        return None
                                    return float_val
                                except (ValueError, TypeError):
                                    return None
                            
                            intnumeratoroutput = sanitize_numeric_value(numerator_values.get(period_name))
                            intdenominatoroutput = sanitize_numeric_value(denominator_values.get(period_name))

                            
                            # Build INSERT statement
                            insert_sql = text(f"""
                                INSERT INTO {schema_name}.tbl_ratio_result (
                                    intprocessinstanceid,
                                    intdatamodelid,
                                    intratioconfigurationid,
                                    vcside,
                                    intsideuniqueid,
                                    intmatchid,
                                    intnumeratoroutput,
                                    intdenominatoroutput,
                                    intformulaoutput,
                                    vcformulaoutput,
                                    vcstatus,
                                    vcaction,
                                    intactionuserid,
                                    dtactiontime,
                                    vccomment,
                                    isactive
                                ) VALUES (
                                    :intprocessinstanceid,
                                    :intdatamodelid,
                                    :intratioconfigurationid,
                                    :vcside,
                                    :intsideuniqueid,
                                    :intmatchid,
                                    :intnumeratoroutput,
                                    :intdenominatoroutput,
                                    :intformulaoutput,
                                    :vcformulaoutput,
                                    :vcstatus,
                                    :vcaction,
                                    :intactionuserid,
                                    :dtactiontime,
                                    :vccomment,
                                    :isactive
                                )
                            """)
                            
                            insert_params = {
                                'intprocessinstanceid': process_instance_id,
                                'intdatamodelid': intdatamodelid,
                                'intratioconfigurationid': intratioconfigurationid,
                                'vcside': vcside,
                                'intsideuniqueid': intsideuniqueid,
                                'intmatchid': intmatchid,
                                'intnumeratoroutput': intnumeratoroutput,
                                'intdenominatoroutput': intdenominatoroutput,
                                'intformulaoutput': intformulaoutput,
                                'vcformulaoutput': vcformulaoutput,
                                'vcstatus': vcstatus,
                                'vcaction': None,
                                'intactionuserid': None,
                                'dtactiontime': current_datetime,
                                'vccomment': None,
                                'isactive': True
                            }
                            
                            try:
                                session.execute(insert_sql, insert_params)
                                records_inserted += 1
                                # print(f"DEBUG - Successfully inserted ratio record {records_inserted} for {vcside} side")
                            except Exception as insert_error:
                                print(f"ERROR - Failed to insert ratio record for {vcside} side: {insert_error}")
                                raise
                    else:
                        # Single period - insert single entry
                        # print(f"DEBUG - Row {idx}: Using single entry path (is_multi_period={is_multi_period})")
                        vcside = None
                        intsideuniqueid = None
                        intmatchid = None
                        
                        # Get numerator and denominator values for the single period
                        # Sanitize infinity and NaN values to None (NULL)
                        def sanitize_numeric_value(value):
                            """Convert infinity/NaN to None for database storage"""
                            if value is None:
                                return None
                            try:
                                float_val = float(value)
                                if np.isinf(float_val) or np.isnan(float_val):
                                    return None
                                return float_val
                            except (ValueError, TypeError):
                                return None
                        
                        period_name = periods[0] if periods else None
                        intnumeratoroutput = sanitize_numeric_value(numerator_values.get(period_name) if period_name else None)
                        intdenominatoroutput = sanitize_numeric_value(denominator_values.get(period_name) if period_name else None)
                        
                        # Build INSERT statement
                        insert_sql = text(f"""
                            INSERT INTO {schema_name}.tbl_ratio_result (
                                intprocessinstanceid,
                                intdatamodelid,
                                intratioconfigurationid,
                                vcside,
                                intsideuniqueid,
                                intmatchid,
                                intnumeratoroutput,
                                intdenominatoroutput,
                                intformulaoutput,
                                vcformulaoutput,
                                vcstatus,
                                vcaction,
                                intactionuserid,
                                dtactiontime,
                                vccomment,
                                isactive
                            ) VALUES (
                                :intprocessinstanceid,
                                :intdatamodelid,
                                :intratioconfigurationid,
                                :vcside,
                                :intsideuniqueid,
                                :intmatchid,
                                :intnumeratoroutput,
                                :intdenominatoroutput,
                                :intformulaoutput,
                                :vcformulaoutput,
                                :vcstatus,
                                :vcaction,
                                :intactionuserid,
                                :dtactiontime,
                                :vccomment,
                                :isactive
                            )
                        """)
                        
                        insert_params = {
                            'intprocessinstanceid': process_instance_id,
                            'intdatamodelid': intdatamodelid,
                            'intratioconfigurationid': intratioconfigurationid,
                            'vcside': vcside,
                            'intsideuniqueid': intsideuniqueid,
                            'intmatchid': intmatchid,
                            'intnumeratoroutput': intnumeratoroutput,
                            'intdenominatoroutput': intdenominatoroutput,
                            'intformulaoutput': intformulaoutput,
                            'vcformulaoutput': vcformulaoutput,
                            'vcstatus': vcstatus,
                            'vcaction': None,
                            'intactionuserid': None,
                            'dtactiontime': current_datetime,
                            'vccomment': None,
                            'isactive': True
                        }
                        
                        try:
                            session.execute(insert_sql, insert_params)
                            records_inserted += 1
                            # print(f"DEBUG - Successfully inserted ratio record {records_inserted}")
                        except Exception as insert_error:
                            print(f"ERROR - Failed to insert ratio record: {insert_error}")
                            raise
        
        # Commit all inserts
        session.commit()
        print(f"Successfully inserted {records_inserted} ratio result record(s)")
        
        # Update process instance status
        _updateProcessInstanceStatus(
            process_instance_id=process_instance_id,
            status='Completed',
            status_description=f'Ratio execution completed. {records_inserted} record(s) inserted.',
            db_manager=db_manager
        )
        
        return True
        
    except Exception as e:
        session.rollback()
        print(f"Error saving ratio results to database: {e}")
        import traceback
        traceback.print_exc()
        
        # Update process instance status to Failed
        _updateProcessInstanceStatus(
            process_instance_id=process_instance_id,
            status='Failed',
            status_description=f'Ratio execution failed: {str(e)}',
            db_manager=db_manager
        )
        
        return False
    finally:
        session.close()


# Example usage
if __name__ == "__main__":
    # Example configuration
    client_id = 2
    fund_id = 1
    period_dates = {
        'Period1': '2024-01-31',
        'Period2': '2024-02-29'
    }
    fund_id_dual = 2

    period_dates_dual = {
        'Period1': '2024-01-31',
    }
    sources = {
        'source_a': "Bluefield",
    }
    sources_dual = {
        'source_a': "Harborview",
        'source_b': "Clearledger",
    }
    align_keys = ['investmentdescription', 'investmenttype']
    
    try:
        print("Starting ratio execution...")
        print(f"Client ID: {client_id}")
        print(f"Fund ID: {fund_id}")
        print(f"Period Dates: {period_dates}")
        print("Note: Ratios use single aggregated values per period, no alignment needed")
        
        # results = executeAllRatios(
        #     client_id=client_id,
        #     fund_id=fund_id,
        #     period_dates=period_dates
        # )

        results = executeAllRatiosWithSources(
            client_id=client_id,
            fund_id=fund_id_dual,
            period_dates=period_dates_dual,
            source_a=sources_dual['source_a'],
            source_b=sources_dual['source_b'],
        )
        # results = executeAllRatiosWithSources(
        #     client_id=client_id,
        #     fund_id=fund_id,
        #     period_dates=period_dates,
        #     source_a=sources['source_a'],
        #     source_b=None,
        # )
        
        # Print summary
        printRatioSummary(results)
        
        # Save results to database
        db_manager = get_database_manager()
        if db_manager:
            # saveRatioResultsToDatabase(results, client_id, fund_id, period_dates, db_manager)
            # saveRatioResultsToDatabaseWithSources(results, client_id, fund_id, period_dates, sources['source_a'], None, db_manager)
            saveRatioResultsToDatabaseWithSources(results, client_id, fund_id_dual, period_dates_dual, sources_dual['source_a'], sources_dual['source_b'], db_manager)
        
        # Save results to CSV and JSON files
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        # saveResultsToFiles(results, client_id, fund_id, timestamp, prefix="ratio")
        
    except Exception as e:
        print(f"Error executing ratios: {e}")
        import traceback
        traceback.print_exc()


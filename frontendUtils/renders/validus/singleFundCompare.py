from storage import STORAGE
from utils.unclassified import getFundUniqueId, getFundId
from fastapi import HTTPException
from frontendUtils.renders.utils.subPage import getSubPageRender
from frontendUtils.renders.utils.groupedStatsCard import getGroupedStatsCardRender
from frontendUtils.renders.utils.nestedTable import getSimpleTableRenderFromRows,getNestedTableFromRenderStructure,createTreeDataFromRows
from frontendUtils.renders.utils.statCard import getStatCard
from utils.NAVFetchUtil import getSourceNAV
from frontendUtils.renders.utils.statsRepresentation import getStatsRepresentation,getStatsRepresentation2
from utils.dateUtil import convertDateToFormat
from frontendUtils.renders.validus.pnlTrendsRenderer import getPnlByAssetClass, getMvOfInvestments
from clients.validusDemo.compliance.utils.checks import userHasFundReadPerm
from utils.generalValidations import countNoneFields
from server.APIServerUtils.db_validation_service import db_validation_service
from clients.validusDemo.customFunctions.db_nav_validations import nav_validations
from clients.validusDemo.customFunctions.db_ratio_validations import ratio_validations
from typing import Optional, List, Dict
from clients.validusDemo.customFunctions.db_file_validations import file_validations
from database_models import DatabaseManager, NavPackVersion, NavPack, Source, TrialBalance, PortfolioValuation, Dividend

import pandas as pd
from math import fabs, log10, floor
from datetime import datetime
import asyncio
import concurrent.futures
import json

def _convertDatesToYmdFormat(params: dict) -> dict:
    """
    Helper function to convert dateA and dateB in params to Y-m-d format
    
    IMPORTANT: Returns a copy of params to avoid mutating the original object
    This prevents React re-render issues caused by parameter mutation
    """
    import copy
    
    # Create a deep copy to avoid mutating the original params
    params_copy = copy.deepcopy(params)
    
    # Handle both direct params and nested query params
    query = params_copy.get('query', params_copy)
    
    # Convert dateA and dateB to Y-m-d format
    for date_key in ['dateA', 'dateB']:
        if date_key in query:
            try:
                # Try to parse the date and convert to Y-m-d format
                date_str = query[date_key]
                # Try different common date formats
                for fmt in ['%m-%d-%Y', '%Y-%m-%d', '%m/%d/%Y', '%Y/%m/%d', '%b %Y', '%B %Y']:
                    try:
                        date_obj = datetime.strptime(date_str, fmt)
                        query[date_key] = date_obj.strftime('%Y-%m-%d')
                        break
                    except ValueError:
                        continue
            except Exception as e:
                print(f"Failed to convert {date_key}: {query.get(date_key)}. Error: {e}")

    return params_copy


def _calculateRNavFromDatabase(params: dict, source: str) -> float:
    """
    Calculate R.Nav from database using enhanced versioning logic
    
    R.Nav uses LATEST VERSION (base + override combined) data
    This method uses the enhanced DatabaseValidationService that properly:
    1. Fetches the latest nav version
    2. Gets all data from base version (if exists) 
    3. Overlays/replaces with override data from latest version
    4. Calculates R.Nav from combined dataset
    
    Args:
        params: Request parameters containing fund, source, and date information
        source: 'A' or 'B' to indicate which source to calculate R.Nav for
    
    Returns:
        R.Nav value as float, or None if calculation fails
    """
    try:
        query = params.get('query', params)
        fund_name = query.get('fundName', '')
        
        if source == 'A':
            source_name = query.get('sourceA', '')
            date = query.get('dateA', '')
        else:  # source == 'B'
            source_name = query.get('sourceB', '')
            date = query.get('dateB', '')
        
        if not all([fund_name, source_name, date]):
            return None
        
        # Use enhanced R.Nav calculation with proper base+override versioning
        rnav_value = db_validation_service.calculate_rnav(fund_name, source_name, date)
        
        return rnav_value
        
    except Exception as e:
        print(f"Error calculating R.Nav from database for source {source}: {e}")
        return None

def _calculateNAVFromDatabase(params: dict, source: str) -> float:
    """
    Calculate NAV from database using BASE VERSION ONLY (no override data)
    
    NAV uses BASE VERSION ONLY data (ignores overrides)
    This is different from R.Nav which uses latest version (base + override)
    
    Args:
        params: Request parameters containing fund, source, and date information
        source: 'A' or 'B' to indicate which source to calculate NAV for
    
    Returns:
        NAV value as float, or None if calculation fails
    """
    try:
        query = params.get('query', params)
        fund_name = query.get('fundName', '')
        
        if source == 'A':
            source_name = query.get('sourceA', '')
            date = query.get('dateA', '')
        else:  # source == 'B'
            source_name = query.get('sourceB', '')
            date = query.get('dateB', '')
        
        if not all([fund_name, source_name, date]):
            return None
        
        # Use NAV calculation with BASE VERSION ONLY (no overrides)
        nav_value = db_validation_service.calculate_nav(fund_name, source_name, date)
        
        return nav_value
        
    except Exception as e:
        print(f"Error calculating NAV from database for source {source}: {e}")
        return None


def getSingleFundCompareTrendsCombinedOutput(params:dict):
    return {
        'pnlByAssetClassTrends':getPnlByAssetClass(params),
        'mvOfInvestments':getMvOfInvestments(params),
    }

async def getSingleFundComparePageCombinedOutput(params:dict):
    """
    OPTIMIZED VERSION: Consolidates validation data fetching and parallelizes independent operations
    """
    # Convert dates without mutating original params (prevents React re-render issues)
    params = _convertDatesToYmdFormat(params)
    
    # OPTIMIZATION 1: Get validation data ONCE and reuse it (using cache system)
    # This is the most expensive operation, so we fetch it once and pass to all functions
    cache_key = _createValidationCacheKey(params)
    myValidationOutput = _getCachedValidationData(cache_key)
    
    if myValidationOutput is None:
        try:
            # NOTE: This function is now async but we're calling it synchronously
            # This is a temporary workaround - ideally all calling functions should be async
            import asyncio
            myValidationOutput = await getValidationOutput(params)
            # Cache the fresh data
            if myValidationOutput is not None:
                _setCachedValidationData(cache_key, myValidationOutput)
        except Exception as e:
            print(f"Error fetching validation output: {e}")
            myValidationOutput = []

    
    # OPTIMIZATION 2: Use ThreadPoolExecutor to parallelize independent operations
    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
        # Submit all independent tasks in parallel
        nav_a_future = executor.submit(getNAVValueSourceA, params)
        nav_b_future = executor.submit(getNAVValueSourceB, params)
        checkpoints_future = executor.submit(getDummyCHECKPOINTS, params)
        
        # Submit tasks that can use the shared validation data
        summary_stats_future = executor.submit(getSummaryStatsCard, params, myValidationOutput)
        data_validations_future = executor.submit(dataValidationsPageOutput, params, myValidationOutput)
        file_validations_future = executor.submit(getFileValidations, params, myValidationOutput)
        
        # Wait for all tasks to complete
        nav_a_result = nav_a_future.result()
        nav_b_result = nav_b_future.result()
        checkpoints_result = checkpoints_future.result()
        summary_stats_result = summary_stats_future.result()
        data_validations_result = data_validations_future.result()
        file_validations_result = file_validations_future.result()
    
    return {
        'NAVValueSourceA': nav_a_result,
        'NAVValueSourceB': nav_b_result,
        # Removed RNAV: 'NAVValueAfterEdits':getNAVValueAfterEdits(params),
        'summaryStatsCard': summary_stats_result,
        'checkPoints': checkpoints_result,
        'dataValidations': data_validations_result,
        'fileValidationSummaryTable': file_validations_result
    }

def getNAVValueSourceA(params:dict):
    # Try database-driven approach first
    try:
        nav_value = _calculateNAVFromDatabase(params, 'A')
        if nav_value is not None:
            query = params.get('query', params)
            date_a = query.get('dateA', '')
            # Convert date format for display
            try:
                from datetime import datetime
                if '-' in date_a and len(date_a.split('-')[0]) == 4:  # YYYY-MM-DD format
                    date_obj = datetime.strptime(date_a, '%Y-%m-%d')
                else:  # MM-DD-YYYY format
                    date_obj = datetime.strptime(date_a, '%m-%d-%Y')
                formatted_date = date_obj.strftime('%b %Y')
            except:
                formatted_date = date_a
            
            myRender = {
                'label': formatted_date,
                'value': f"${nav_value:,.2f}",
            }
            return getStatsRepresentation2(myRender)
    except Exception as e:
        print(f"Database NAV calculation failed for Source A: {e}")
    
    # Fallback to original file-based approach
    myStorage=_getStorage()
    queryConfig=getConfigFromQuery(_getClient(),params)
    sourceANAV=getSourceNAV(myStorage,queryConfig['fundUniqueId'],queryConfig['sourceA'])
    myRender={
        'label':convertDateToFormat(queryConfig['sourceA']['processDate'],'MMM YYYY'),
        'value':f"${sourceANAV:,.2f}",
    }
    return getStatsRepresentation2(myRender)

def getNAVValueSourceB(params:dict):
    # Try database-driven approach first
    try:
        nav_value = _calculateNAVFromDatabase(params, 'B')
        if nav_value is not None:
            query = params.get('query', params)
            date_b = query.get('dateB', '')
            # Convert date format for display
            try:
                from datetime import datetime
                if '-' in date_b and len(date_b.split('-')[0]) == 4:  # YYYY-MM-DD format
                    date_obj = datetime.strptime(date_b, '%Y-%m-%d')
                else:  # MM-DD-YYYY format
                    date_obj = datetime.strptime(date_b, '%m-%d-%Y')
                formatted_date = date_obj.strftime('%b %Y')
            except:
                formatted_date = date_b
            
            myRender = {
                'label': formatted_date,
                'value': f"${nav_value:,.2f}",
            }
            return getStatsRepresentation2(myRender)
    except Exception as e:
        print(f"Database NAV calculation failed for Source B: {e}")
    
    # Fallback to original file-based approach
    myStorage=_getStorage()
    queryConfig=getConfigFromQuery(_getClient(),params)
    sourceBNAV=getSourceNAV(myStorage,queryConfig['fundUniqueId'],queryConfig['sourceB'])
    myRender={
        'label':convertDateToFormat(queryConfig['sourceB']['processDate'],'MMM YYYY'),
        'value':f"${sourceBNAV:,.2f}",
    }
    return getStatsRepresentation2(myRender)

def getNAVValueAfterEdits(params:dict):
    # Try database-driven approach first (R.Nav with overrides)
    try:
        rnav_value = _calculateRNavFromDatabase(params, 'B')
        if rnav_value is not None:
            myRender = {
                'label': "R. NAV",
                'value': f"${rnav_value:,.2f}",
                "changeInPercentage": "0.00%",
                "changeInValue": "$0.00",
                "colorWhenPositive": "#16A34A",
                "colorWhenNegative": "#DC2626",
                "showPercentByDefault": True
            }
            return getStatsRepresentation2(myRender)
    except Exception as e:
        print(f"Database R.Nav calculation failed for After Edits: {e}")
    
    # Fallback to original file-based approach
    myStorage=_getStorage()
    queryConfig=getConfigFromQuery(_getClient(),params)
    sourceBNAV=getSourceNAV(myStorage,queryConfig['fundUniqueId'],queryConfig['sourceB'])
    myRender={
        'label':"R. NAV",
        'value':f"${sourceBNAV:,.2f}",
        "changeInPercentage": "0.00%",
        "changeInValue": "$0.00",
        "colorWhenPositive": "#16A34A",
        "colorWhenNegative": "#DC2626",
        "showPercentByDefault": True
    }
    return getStatsRepresentation2(myRender)

def getNAVValidationsPageCombinedOutput(params:dict): 
    """
    Get hierarchical NAV validations in existing format with stat cards and tree table
    
    OPTIMIZATION: Uses caching to prevent unnecessary reloads
    """
    # Convert dates without mutating original params (prevents React re-render issues)
    params = _convertDatesToYmdFormat(params)
    
    # Try to get cached validation data first
    cache_key = _createValidationCacheKey(params)
    myValidationOutput = _getCachedValidationData(cache_key)
    
    if myValidationOutput is None:
        # Get validation data from database-driven approach (expensive operation)
        try:
            import asyncio
            # Check if we're in an event loop
            try:
                loop = asyncio.get_running_loop()
                # We're in an event loop, run in a thread
                import concurrent.futures
                import threading
                future = concurrent.futures.Future()
                def run_in_thread():
                    try:
                        result = asyncio.run(getValidationOutput(params))
                        future.set_result(result)
                    except Exception as e:
                        future.set_exception(e)
                thread = threading.Thread(target=run_in_thread)
                thread.start()
                thread.join()
                myValidationOutput = future.result()
            except RuntimeError:
                # No event loop running, safe to use asyncio.run()
                myValidationOutput = asyncio.run(getValidationOutput(params))
        except Exception as e:
            print(f"Error fetching validation output: {e}")
            myValidationOutput = []
        
        # Cache database results
        if myValidationOutput is not None:
            _setCachedValidationData(cache_key, myValidationOutput)
        else:
            print("WARNING: getValidationOutput returned None, not caching")
    
    # Filter to only PnL and Non-Trading validations (matching database format)
    if myValidationOutput is None:
        NAVValidations = []
    else:
        # Handle both 'PnL' and 'P&L' variants (backend sometimes returns 'P&L')
        NAVValidations = [item for item in myValidationOutput if item["type"].lower() in ['pnl','p&l','non-trading']]
        ratio_validations = [item for item in myValidationOutput if item.get('type', '').lower() == 'ratio']
    
    NAVValidations = [item for item in NAVValidations if item.get('subType2') != 'Performance Fees']
    
    # Filter out 'Stale Price' from NAVValidations entries when sourceB is available
    if params.get('query', {}).get('sourceB', ''):
        stale_price_filtered = len([item for item in NAVValidations if item.get('subType2') == 'Stale Price'])
        NAVValidations = [item for item in NAVValidations if item.get('subType2') != 'Stale Price']
    
    # Handle Total Expenses: Replace Total Expenses row with its individual expense children
    processed_validations = []
    for item in NAVValidations:
        if item.get('subType2') == 'Total Expense':
            # Get the children from Total Expenses and add them as individual validations
            data = item.get('data', {})
            children = data.get('failed_items', []) + data.get('passed_items', [])
            
            for child in children:
                extra_data_children = child.get('extra_data_children', [])
                if extra_data_children:
                    # Create individual expense validation for each child
                    for expense_child in extra_data_children:
                        individual_expense = {
                            'type': item.get('type', 'Non-Trading'),
                            'subType': item.get('subType', 'Expenses'),
                            'subType2': expense_child.get('transaction_description', 'Unknown Expense'),
                            'message': 1 if expense_child.get('is_exception', 0) else 0,
                            'data': {
                                'failed_items': [expense_child] if expense_child.get('is_exception', 0) else [],
                                'passed_items': [expense_child] if not expense_child.get('is_exception', 0) else [],
                                'threshold': data.get('threshold', 5.0),
                                'precision_type': data.get('precision_type', 'PERCENTAGE'),
                                'kpi_code': data.get('kpi_code', ''),
                                'kpi_name': expense_child.get('transaction_description', 'Unknown Expense'),
                                'kpi_id': data.get('kpi_id', ''),
                                'kpi_description': data.get('kpi_description', '')
                            }
                        }
                        processed_validations.append(individual_expense)
        else:
            # Keep all other validations as-is
            processed_validations.append(item)
    
    NAVValidations = processed_validations
    
    # Calculate summary statistics
    total_validations = len(NAVValidations)
    total_passed = sum([el['message']==0 for el in NAVValidations])
    total_failed = sum([el['message']!=0 for el in NAVValidations])
    
    # Calculate exceptions safely for both database and JSON-based approaches
    # CONSISTENCY FIX: Count ALL exception items (failed + passed that exceed thresholds) 
    # to match detailed table counting logic
    total_exceptions = 0
    
    for el in NAVValidations:
        data = el.get('data')
        if data is not None and isinstance(data, dict):
            # Count all items that would be flagged as exceptions in detailed table
            # This includes both failed items and passed items that exceed thresholds
            failed_items = data.get('failed_items', [])
            passed_items = data.get('passed_items', [])
            threshold = data.get('threshold')
            precision_type = data.get('precision_type', 'PERCENTAGE')
            
            # FIXED: Only count failed items as exceptions (not passed items)
            # The KPI validation system already correctly categorizes failed vs passed items
            # based on the threshold, so we should only count failed_items
            exceptions_count = len(failed_items)
            
            
            total_exceptions += exceptions_count
            
                    
        else:
            # Fallback: use absolute value of message
            total_exceptions += abs(el.get('message', 0))
    
    # Create stat cards with click actions
    total_validations_card = getStatCard('TOTAL VALIDATIONS', str(total_validations))
    total_validations_card['clickAction'] = {
        "type": "navigation",
        "to": "/validus",
        "parameters": [
            {
                "key": "page",
                "value": "nav-validations",
            }
        ]
    }
    
    total_passed_card = getStatCard('TOTAL PASSED', str(total_passed))
    total_passed_card['clickAction'] = {
        "type": "navigation",
        "to": "/validus",
        "parameters": [
            {
                "key": "page",
                "value": "nav-validations",
            },
            {
                "key": "filter",
                "value": "status-passed",
            }
        ]
    }
    
    total_failed_card = getStatCard('TOTAL FAILED', str(total_failed))
    total_failed_card['clickAction'] = {
        "type": "navigation",
        "to": "/validus",
        "parameters": [
            {
                "key": "page",
                "value": "nav-validations",
            },
            {
                "key": "filter",
                "value": "status-failed",
            }
        ]
    }
    
    total_exceptions_card = getStatCard('TOTAL EXCEPTIONS', str(total_exceptions))
    total_exceptions_card['clickAction'] = {
          "type": "navigation",
          "to": "/validus",
          "parameters": [
            {
              "key": "page",
              "value": "nav-validations",
            },
            {
              "key": "groupDefaultExpand",
              "value": -1,
            },
          ],
        }
    
    output={
        'NAVValueAfterEdits': getNAVValueAfterEdits(params),
        'totalValidations': total_validations_card,
        'totalPassed': total_passed_card,
        'totalFailed': total_failed_card,
        'totalExceptions': total_exceptions_card,
        'pnlValidationsLevel1Table': _createHierarchicalValidationTable(NAVValidations, params, total_validations, total_exceptions),
    }

    return output


def _createHierarchicalValidationTable(NAVValidations, params, total_validations, total_exceptions):
    """
    Create hierarchical validation table in tree format matching existing structure
    """
    import pandas as pd
    
    # Convert validations to rows with proper structure
    myRows = []
    
    for validation in NAVValidations:
        # Get threshold value and precision_type from validation data
        threshold_value = None
        validation_precision_type = 'PERCENTAGE'  # Default
        if 'data' in validation and isinstance(validation['data'], dict):
            threshold_value = validation['data'].get('threshold')
            validation_precision_type = _convert_precision_type_to_string(validation['data'].get('precision_type', 'PERCENTAGE'))
        
        # Format threshold based on precision_type (same logic as detailed table)
        threshold_str = '-'
        if threshold_value is not None:
            try:
                if _convert_precision_type_to_string(validation_precision_type) == 'PERCENTAGE':
                    threshold_str = f"{threshold_value}%"
                else:  # ABSOLUTE
                    threshold_str = f"${threshold_value:,.0f}"
            except (ValueError, TypeError):
                threshold_str = str(threshold_value)
        
        # Calculate exceptions count from validation message and data
        # FIXED: Only count failed items as exceptions (not passed items)
        # The KPI validation system already correctly categorizes failed vs passed items
        exceptions_count = 0
        if 'data' in validation and isinstance(validation['data'], dict):
            data = validation['data']
            failed_items = data.get('failed_items', [])
            
            # Only count failed items as exceptions
            exceptions_count = len(failed_items)
        elif validation.get('message', 0) != 0:
            # Fallback for validations without detailed data
                exceptions_count = -validation.get('message', 0)
        
        # Get description from validation data
        description = ''
        if 'data' in validation and isinstance(validation['data'], dict):
            description = validation['data'].get('kpi_description', '')
        
        # Replace threshold placeholders in description for tooltip
        tooltip_description = description
        if description and threshold_value is not None:
            try:
                if _convert_precision_type_to_string(validation_precision_type) == 'PERCENTAGE':
                    formatted_threshold = f"{threshold_value}%"
                else:  # ABSOLUTE
                    formatted_threshold = f"${threshold_value:,.0f}"
                
                # Replace both "threshold_value%" and "threshold_value" placeholders
                tooltip_description = description.replace('threshold_value%', formatted_threshold)
                tooltip_description = tooltip_description.replace('threshold_value', formatted_threshold)
            except (ValueError, TypeError):
                tooltip_description = description
        
        # Convert PnL to P&L for display
        display_type = validation.get('type', '-')
        if display_type == 'PnL':
            display_type = 'P&L'
        
        # Get subType3 from validation data for 4th level hierarchy
        # Only add subType3 if it's different from subType2 (to avoid duplication)
        sub_type3 = None
        if 'data' in validation and isinstance(validation['data'], dict):
            data_subtype3 = validation['data'].get('subType3')
            if data_subtype3 is not None and data_subtype3 != validation.get('subType2'):
                sub_type3 = data_subtype3
        
        row_data = {
            'type': display_type,
            'subType': validation.get('subType', '-'),
            'subType2': validation.get('subType2', '-'),
            'description': tooltip_description,  # Use the processed description with formatted thresholds
            'threshold': threshold_str,
            'Status': "Passed" if validation.get('message', 0) == 0 else "Failed",
            "Validations": 1,
            "Exceptions": exceptions_count,
            "tooltipInfo": tooltip_description  # Use processed description for tooltip
        }
        
        # Only add subType3 if it exists and is different
        if sub_type3 is not None:
            row_data['subType3'] = sub_type3
        
        myRows.append(row_data)
    
    # Create DataFrame for tree processing
    myDF = pd.DataFrame(myRows)
    
    # Calculate total exceptions from the data for column header
    calculated_total_exceptions = sum(row.get('Exceptions', 0) for row in myRows)
    
    # Create table properties matching existing format
    myTableProperties = {
        'treeData': True,
        "treeDataChildrenField": "_children",
        "colsToShow": ["threshold", "Status", "Validations", "Exceptions"],
        "rowGroupings": [],
        "autoSizeColumns" : False,
        "columnConfig": {
            "Validations": {
                "name": f"Validations ({total_validations})",
                "agg": "sum",
                "type": "numericColumn",
                "cellStyle": {"textAlign": "right", "paddingRight": "8px"},
                "customCellRenderer": "tooltipAggregator",
                "valueFormatter": [
                    "function", 
                    "(params) => { if (params.value === null || params.value === undefined) return '0'; return params.value.toLocaleString('en-US'); }"
                ]
            },
            "Exceptions": {
                "name": f"Exceptions ({calculated_total_exceptions})",
                "agg": "sum",
                "customCellRenderer":"exceptionRenderer",
                "type": "numericColumn",
                "cellStyle": {"textAlign": "right", "paddingRight": "8px"},
                "valueFormatter": [
                    "function", 
                    "(params) => { if (params.value === null || params.value === undefined) return '0'; return params.value.toLocaleString('en-US'); }"
                ]
            },
            "threshold": {
                "name": "Threshold",
                "cellStyle": {"textAlign": "right", "paddingRight": "8px"},
                "customCellRenderer": "tooltipAggregator"
            },
            "Status": {
                "customCellRenderer": "labelToNumberAggregator",
                "filter": "agTextColumnFilter"
            }
        },
        "autoGroupColumnDef": {
            "headerName": "Type",
            "field": "_title",
            "cellRenderer": "agGroupCellRenderer",
            "innerRenderer": "exceptionHighlightAggregator",
        },
        "groupDefaultExpanded":0,
        "rowData": _createMixedLevelTreeData(myDF, ['threshold', 'Status', 'Validations', 'Exceptions', 'tooltipInfo']),

    }

    return getNestedTableFromRenderStructure(myTableProperties)


def _createMixedLevelTreeData(aDF, colsToShow: list):
    """
    Create tree data that handles mixed 3-level and 4-level hierarchies
    """
    import pandas as pd
    
    # Separate rows based on whether they have subType3
    rows_with_subtype3 = aDF[aDF['subType3'].notna()].copy() if 'subType3' in aDF.columns else pd.DataFrame()
    rows_without_subtype3 = aDF[~aDF['subType3'].notna()].copy() if 'subType3' in aDF.columns else aDF.copy()
    
    tree_data = []
    
    # Process 4-level hierarchy (with subType3)
    if not rows_with_subtype3.empty:
        tree_4_level = _createTreeDataWithClickActions(
            rows_with_subtype3, 
            ['type', 'subType', 'subType2', 'subType3'], 
            colsToShow
        )
        tree_data.extend(tree_4_level)
    
    # Process 3-level hierarchy (without subType3)  
    if not rows_without_subtype3.empty:
        tree_3_level = _createTreeDataWithClickActions(
            rows_without_subtype3,
            ['type', 'subType', 'subType2'], 
            colsToShow
        )
        tree_data.extend(tree_3_level)
    
    # Merge trees with same top-level keys
    merged_tree = {}
    for item in tree_data:
        title = item['_title']
        if title in merged_tree:
            # Merge children
            merged_tree[title]['_children'].extend(item.get('_children', []))
        else:
            merged_tree[title] = item
    
    return list(merged_tree.values())


def _convert_precision_type_to_string(precision_type):
    """
    Convert precision type integer to string for backward compatibility
    0 = PERCENTAGE, 1 = ABSOLUTE
    """
    if isinstance(precision_type, int):
        return 'PERCENTAGE' if precision_type == 0 else 'ABSOLUTE'
    return precision_type  # Already a string

def _createTreeDataWithClickActions(aDF, rowGroups: list, colsToShow: list, currentDepth=0, newColName='_title'):
    """
    Create tree data with click actions for navigation to details pages
    Based on createTreeDataFromRows but adds clickAction to each row following the exact structure
    """
    if currentDepth == len(rowGroups) - 1:
        # Leaf level - individual validations with click actions
        myRows = aDF.to_dict(orient='records')
        finalData = []
        for row in myRows:
            myData = {
                newColName: row[rowGroups[currentDepth]],
            }
            for col in colsToShow:
                myData[col] = row[col]
            
            # Add click action for leaf level (subType2 or subType3)
            if len(rowGroups) >= 3:  # At least 3 levels (type, subType, subType2)
                # Get parent values for navigation parameters
                level2_value = row.get(rowGroups[1], '') if len(rowGroups) > 1 else ''
                level2_tab_key = level2_value.lower().replace(' ', '').replace('&', '') if level2_value else ''
                
                # Current level value for group parameter
                current_level_value = row[rowGroups[currentDepth]]
                
                myData['clickAction'] = {
                    "type": "navigation",
                    "to": "/validus",
                    "parameters": [
                        {
                            "key": "page",
                            "value": "nav-validation-details"
                        },
                        {
                            "key": "tab",
                            "value": level2_tab_key
                        },
                        {
                            "key": "group",
                            "value": current_level_value
                        }
                    ]
                }
            
            finalData.append(myData)
        return finalData

    # Non-leaf levels
    finalData = []
    grouped_dfs = {group: data for group, data in aDF.groupby(rowGroups[currentDepth])}

    for keyValue, subRows in grouped_dfs.items():
        myData = {
            newColName: keyValue,
            '_children': _createTreeDataWithClickActions(subRows, rowGroups, colsToShow, currentDepth=currentDepth+1, newColName=newColName),
        }
        
        # Add click actions based on hierarchy level following the exact example structure
        if currentDepth == 0:  # Level 1 (P&L, Non-Trading) - navigation to details page
            myData['clickAction'] = {
                "type": "navigation",
                "to": "/validus",
                "parameters": [
                    {
                        "key": "page",
                        "value": "nav-validation-details"
                    }
                ]
            }
        elif currentDepth == 1:  # Level 2 (subType: Expenses, Pricing, Positions, etc.) - navigation with tab
            # For level 2, use "tab" parameter with dynamic tab mapping
            tab_value = keyValue
            tab_internal_key = tab_value.lower().replace(' ', '').replace('&', '')
            
            myData['clickAction'] = {
                "type": "navigation",
                "to": "/validus", 
                "parameters": [
                    {
                        "key": "page",
                        "value": "nav-validation-details"
                    },
                    {
                        "key": "tab",
                        "value": tab_internal_key
                    }
                ]
            }
        elif currentDepth == 2:  # Level 3 (subType2: Major Price Changes, etc.) - navigation with tab and group
            # Get the parent level 2 value for tab parameter
            # This is a bit tricky since we need the parent group value
            parent_level2 = rowGroups[1] if len(rowGroups) > 1 else ""
            if not subRows.empty and parent_level2 in subRows.columns:
                parent_tab_value = subRows[parent_level2].iloc[0]
                parent_tab_internal_key = parent_tab_value.lower().replace(' ', '').replace('&', '')
                
                myData['clickAction'] = {
                    "type": "navigation",
                    "to": "/validus",
                    "parameters": [
                        {
                            "key": "page",
                            "value": "nav-validation-details"
                        },
                        {
                            "key": "tab",
                            "value": parent_tab_internal_key
                        },
                        {
                            "key": "group",
                            "value": keyValue
                        }
                    ]
                }
        
        finalData.append(myData)

    return finalData


def _createHierarchicalValidationStructure(validations):
    """
    Create hierarchical validation structure for 3-level navigation
    """
    hierarchy = {}
    
    for validation in validations:
        level1 = validation.get('type', 'Unknown')
        level2 = validation.get('subType', 'Unknown') 
        level3 = validation.get('subType2', 'Unknown')
        
        # Initialize hierarchy levels
        if level1 not in hierarchy:
            hierarchy[level1] = {
                'name': level1,
                'total_validations': 0,
                'failed_validations': 0,
                'children': {}
            }
        
        if level2 not in hierarchy[level1]['children']:
            hierarchy[level1]['children'][level2] = {
                'name': level2,
                'total_validations': 0,
                'failed_validations': 0,
                'children': {}
            }
        
        if level3 not in hierarchy[level1]['children'][level2]['children']:
            hierarchy[level1]['children'][level2]['children'][level3] = {
                'name': level3,
                'validations': [],
                'total_count': 0,
                'failed_count': 0,
                'passed_count': 0
            }
        
        # Add validation to level 3
        validation_entry = {
            'validation': validation,
            'message': validation.get('message', 0),
            'data': validation.get('data', {}),
            'is_failed': validation.get('message', 0) > 0
        }
        
        hierarchy[level1]['children'][level2]['children'][level3]['validations'].append(validation_entry)
        
        # Update counts
        data = validation.get('data', {})
        failed_count = data.get('count', 0) if validation.get('message', 0) > 0 else 0
        total_count = data.get('total_checked', 1) if 'total_checked' in data else 1
        passed_count = total_count - failed_count
        
        # Level 3 counts
        hierarchy[level1]['children'][level2]['children'][level3]['total_count'] += total_count
        hierarchy[level1]['children'][level2]['children'][level3]['failed_count'] += failed_count
        hierarchy[level1]['children'][level2]['children'][level3]['passed_count'] += passed_count
        
        # Level 2 counts
        hierarchy[level1]['children'][level2]['total_validations'] += 1
        if validation.get('message', 0) > 0:
            hierarchy[level1]['children'][level2]['failed_validations'] += 1
        
        # Level 1 counts  
        hierarchy[level1]['total_validations'] += 1
        if validation.get('message', 0) > 0:
            hierarchy[level1]['failed_validations'] += 1
    
    return {
        'hierarchy': hierarchy,
        'summary': {
            'total_types': len(hierarchy),
            'total_validations': len(validations),
            'failed_validations': sum(1 for v in validations if v.get('message', 0) > 0)
        }
    }


def getNAVValidationDetailsTabs(params: dict):
    """
    Get detailed NAV validation data for drill-down functionality in tabular format
    Supports filtering by level1, level2, level3 parameters
    
    OPTIMIZATION: 
    - Reuses cached validation data from getNAVValidationsPageCombinedOutput
    - Only performs filtering on cached data (fast operation)
    - No duplicate expensive getValidationOutput calls
    """
    # Convert dates without mutating original params (prevents React re-render issues)
    params = _convertDatesToYmdFormat(params)
    
    # Extract filter parameters
    query = params.get('query', {})
    level1_filter = query.get('level1')  # e.g., 'PnL'
    level2_filter = query.get('level2')  # e.g., 'Pricing'  
    level3_filter = query.get('level3')  # e.g., 'Major Price Change'
    
    # Create cache key based on core parameters (excluding filter parameters)
    cache_key = _createValidationCacheKey(params)
    
    # Try to get cached validation data first (reuse from getNAVValidationsPageCombinedOutput)
    all_validations = _getCachedValidationData(cache_key)
    
    if all_validations is None:
        # If no cached data, get fresh data and cache it
        all_validations = getValidationOutput(params)
        
        # Cache database results
        if all_validations is not None:
            _setCachedValidationData(cache_key, all_validations)
    
    # Filter validations based on hierarchy levels (this is fast)
    filtered_validations = []
    ratio_validations_filtered = 0
    empty_validations_filtered = 0
    
    if all_validations is not None:
        for validation in all_validations:
            include = True
            filter_reason = ""
            
            # FIRST: Only include NAV validations (exclude ratio validations)
            validation_type = validation.get('type', '').lower()
            if validation_type not in ['pnl', 'p&l', 'non-trading']:
                include = False
                filter_reason = f"Not NAV validation (type: {validation.get('type', '')})"
                ratio_validations_filtered += 1
            
            # Check if validation has actual data (failed_items/passed_items or count data)
            validation_data = validation.get('data', {})
            has_detailed_data = (
                (validation_data.get('failed_items') and len(validation_data.get('failed_items', [])) > 0) or
                (validation_data.get('passed_items') and len(validation_data.get('passed_items', [])) > 0)
            )
            has_count_data = (
                validation_data.get('count', 0) > 0 or
                validation_data.get('total_checked', 0) > 0
            )
            
            # CONSISTENCY FIX: Include validations with either detailed data OR count data
            # This ensures both KPI-based (detailed) and cascading (count-only) validations are included
            has_data = has_detailed_data or has_count_data
            
            if not has_data:
                include = False
                empty_validations_filtered += 1
            
            if level1_filter and validation.get('type') != level1_filter:
                include = False
            if level2_filter and validation.get('subType') != level2_filter:
                include = False
            if level3_filter and validation.get('subType2') != level3_filter:
                include = False
                
            if include:
                filtered_validations.append(validation)


    
    # Create detailed tabular response
    detailed_data = _createDetailedTabularResponse(filtered_validations, params)
    
    return detailed_data


def getNAVValidationDetailsConfig(params: dict):
    """
    Get NAV validation details page configuration
    Returns the configuration structure for the NAV validation details page
    """
    return {
        "topNavBarParams": [
            {
                "moduleName": "breadcrumb",
                "isShow": True,
                "data": {
                    "title": "NAV Validations Details",
                    "breadcrumb": [
                        {
                            "name": "VALIDUS"
                        },
                        {
                            "name": "Single Fund",
                            "route": "/validus?page=singleFundCompare"
                        },
                        {
                            "name": "NAV Validations",
                            "route": "/validus?page=nav-validations"
                        }
                    ],
                    "isShowBackButton": True
                }
            },
            {
                "moduleName": "notificationIcon",
                "isShow": True
            }
        ],
        "moduleDisplayConfig": [
            {
                "moduleName": "_validusSF_dynamicFilters",
                "layout": {
                    "x": 0,
                    "y": 0,
                    "w": 12,
                    "h": 4
                },
                "id": 0
            },
            {
                "overridenModuleMeta": {
                    "moduleType": "subPage",
                    "modules": [
                        {
                            "overridenModuleMeta": {
                                "moduleType": "textHeader",
                                "header": "NAV Validations Details",
                                "cssProperties": {
                                    "fontSize": "16px",
                                    "fontWeight": "600",
                                    "textTransform": "uppercase",
                                    "color": "#475569",
                                    "padding": "24px",
                                    "borderRadius": "24px"
                                }
                            },
                            "width": "60%"
                        },
                        {
                            "overridenModuleMeta": {
                                "moduleType": "toggle",
                                "label": "Show non-exceptions",
                                "clickAction": {
                                    "type": "storeValues",
                                    "store": {
                                        "key": "filterTable",
                                        "value": "isException"
                                    }
                                }
                            },
                            "width": "19%"
                        },
                        {
                            "overridenModuleMeta": {
                                "moduleType": "searchModule",
                                "placeholder": "Search",
                                "cssProperties": {
                                    "height": "32px"
                                }
                            },
                            "width": "20%"
                        },
                        {
                            "moduleName": "SingleSourceTabs",
                            "width": "100%",
                            "height": "93%"
                        }
                    ],
                    "cssProperties": {
                        "gap": "12px",
                        "backgroundColor": "white",
                        "borderRadius": "24px",
                        "padding": "24px"
                    }
                },
                "layout": {
                    "x": 0,
                    "y": 4,
                    "w": 12,
                    "h": 45
                },
                "id": 1
            }
        ],
    }


# Simple in-memory cache for validation data (prevents expensive recalculations)
_validation_cache = {}
_cache_max_size = 50  # Maximum number of cached entries

def _createValidationCacheKey(params: dict) -> str:
    """
    Create a cache key based on core parameters (excluding filter levels)
    Only fund, sources, and dates affect the underlying validation calculations
    """
    query = params.get('query', params)
    
    # Core parameters that affect validation calculations
    fund_name = query.get('fundName', '')
    source_a = query.get('sourceA', '')
    source_b = query.get('sourceB', '')
    date_a = query.get('dateA', '')
    date_b = query.get('dateB', '')
    
    # Create deterministic cache key
    cache_key = f"{fund_name}|{source_a}|{source_b}|{date_a}|{date_b}"
    return cache_key

def _getCachedValidationData(cache_key: str):
    """Get cached validation data if available and fresh"""
    cached_data = _validation_cache.get(cache_key)
    if cached_data is None:
        return None
    
    # Check if data is still fresh
    if not _checkDataFreshness(cache_key):
        # Data is stale, remove from cache
        _invalidateValidationCache()
        return None
    
    return cached_data

def _checkDataFreshness(cache_key: str) -> bool:
    """
    Check if cached data is still fresh by comparing with database modification times
    Returns True if data is fresh, False if it needs refresh
    """
    try:
        # Parse cache key to get parameters
        key_parts = cache_key.split('|')
        if len(key_parts) < 5:
            return False
            
        fund_name, source_a, source_b, date_a, date_b = key_parts[0], key_parts[1], key_parts[2], key_parts[3], key_parts[4]
        
        # Get cache timestamp (we'll store this when caching)
        cache_timestamp = _validation_cache.get(f"{cache_key}_timestamp")
        if cache_timestamp is None:
            return False
        
        # Get the latest modification time from relevant database tables
        from server.APIServerUtils.db_validation_service import DatabaseValidationService
        db_service = DatabaseValidationService()
        
        if not db_service.db_manager:
            # If we can't check database, assume cache is stale for safety
            return False
            
        # Check modification times of relevant tables
        latest_modification = db_service.get_latest_data_modification_time(fund_name, source_a, source_b, date_a, date_b)
        
        if latest_modification is None:
            # If we can't determine modification time, assume cache is stale for safety
            return False
            
        # Data is fresh if cache is newer than latest database modification
        is_fresh = cache_timestamp >= latest_modification
        return is_fresh
        
    except Exception as e:
        print(f"Error checking data freshness: {e}")
        # If there's an error, assume cache is stale for safety
        return False

def _setCachedValidationData(cache_key: str, validation_data):
    """Set cached validation data with size limit and timestamp"""
    global _validation_cache
    
    # Simple LRU: if cache is full, remove oldest entry
    if len(_validation_cache) >= _cache_max_size:
        # Remove the first (oldest) entry
        oldest_key = next(iter(_validation_cache))
        del _validation_cache[oldest_key]
        # Also remove timestamp if it exists
        if f"{oldest_key}_timestamp" in _validation_cache:
            del _validation_cache[f"{oldest_key}_timestamp"]
    
    # Store data with current timestamp
    from datetime import datetime
    current_time = datetime.now()
    _validation_cache[cache_key] = validation_data
    _validation_cache[f"{cache_key}_timestamp"] = current_time

def _clearValidationCache():
    """Clear the validation cache (useful for testing or memory management)"""
    global _validation_cache
    _validation_cache.clear()

def _invalidateValidationCache(fund_name: str = None, source: str = None, date: str = None):
    """
    Invalidate specific cache entries or all cache if no parameters provided
    Call this when data is updated/overridden to ensure fresh calculations
    """
    global _validation_cache
    
    if not fund_name and not source and not date:
        # Clear all cache
        _clearValidationCache()
        return
    
    # Find and remove cache entries that match the criteria
    keys_to_remove = []
    for cache_key in _validation_cache.keys():
        key_parts = cache_key.split('|')  # fund|sourceA|sourceB|dateA|dateB
        if len(key_parts) >= 5:
            key_fund, key_source_a, key_source_b, key_date_a, key_date_b = key_parts[0], key_parts[1], key_parts[2], key_parts[3], key_parts[4]
            
            should_remove = False
            if fund_name and key_fund == fund_name:
                should_remove = True
            if source and (key_source_a == source or key_source_b == source):
                should_remove = True
            if date and (key_date_a == date or key_date_b == date):
                should_remove = True
                
            if should_remove:
                keys_to_remove.append(cache_key)
    
    # Remove the identified keys
    for key in keys_to_remove:
        del _validation_cache[key]

def getCacheInfo():
    """Get information about the current cache state (useful for debugging)"""
    global _validation_cache
    return {
        "cache_size": len(_validation_cache),
        "cache_keys": list(_validation_cache.keys()),
        "max_size": _cache_max_size
    }

def clearValidationCache():
    """Clear all validation cache entries - useful for forcing fresh data"""
    global _validation_cache
    _validation_cache.clear()
    return {"success": True, "message": "Validation cache cleared"}

def invalidateValidationCache(fund_name: str = None, source: str = None, date: str = None):
    """
    Invalidate specific cache entries based on fund, source, or date
    This should be called when database data is updated
    """
    _invalidateValidationCache(fund_name, source, date)
    return {"success": True, "message": f"Cache invalidated for fund={fund_name}, source={source}, date={date}"}

def _createDetailedTabularResponse(filtered_validations, params):
    """
    Create detailed tabular response with tabs structure for NAV validations
    Includes both passed and failed cases, organized by validation category (Pricing, Positions, Market Value)
    """
    query = params.get('query', {})
    
    # Get source and date information for dynamic column naming
    source_a = query.get('sourceA', 'Bluefield')
    source_b = query.get('sourceB', source_a)
    date_a = query.get('dateA', '')
    date_b = query.get('dateB', date_a)
    
    # Determine if this is dual source (different sources) or single source (same source, different dates)
    is_dual_source = (source_a != source_b)
    
    if is_dual_source:
        # For dual source: use actual source names as column headers
        source_a_label = source_a
        source_b_label = source_b
    else:
        # For single source: use formatted dates as column headers
        source_a_label = _formatDateForDisplay(date_a) if date_a else "Period A"
        source_b_label = _formatDateForDisplay(date_b) if date_b else "Period B"
    
    # Organize data by tabs (categories) with SubType2 as parent rows and optional SubType3 (By Trade - Qty) as intermediate level
    tabs_data = {}
    category_counts = {}
    
    for validation in filtered_validations:
        validation_data = validation.get('data', {})
        sub_type = validation.get('subType', '')
        sub_type2 = validation.get('subType2', '')
        sub_type3 = validation_data.get('subType3', '')  # Get By Trade level from validation data
        
        # Determine tab category
        tab_key = sub_type
        if tab_key not in tabs_data:
            tabs_data[tab_key] = {}
            category_counts[tab_key] = 0
        
        # Create SubType2 as parent category if it doesn't exist
        if sub_type2 and sub_type2 not in tabs_data[tab_key]:
            tabs_data[tab_key][sub_type2] = {
                'children': [],
                'validation_data': validation_data,
                'has_breakdown': False,
                'subtype3_groups': {}  # For grouping by subType3 (By Trade)
            }
        
        # Get threshold information for this validation
        threshold_value = validation_data.get('threshold')
        
        # Get precision_type from validation data
        validation_precision_type = _convert_precision_type_to_string(validation_data.get('precision_type', 'PERCENTAGE'))
        
        # Format threshold based on precision type
        threshold_str = '-'
        if threshold_value is not None:
            try:
                if _convert_precision_type_to_string(validation_precision_type) == 'PERCENTAGE':
                    threshold_str = f"{threshold_value}%"
                else:  # ABSOLUTE
                    threshold_str = f"${threshold_value:,.2f}"
            except (ValueError, TypeError):
                threshold_str = str(threshold_value)
        
        # Get KPI description from validation data for tooltips
        kpi_description = validation_data.get('kpi_description', '')
        
        # Check if this validation has breakdown data (expenses/fees with children)
        has_breakdown_data = False
        failed_items = validation_data.get('failed_items', [])
        passed_items = validation_data.get('passed_items', [])
        
        # CONSISTENCY FIX: Handle count-only validations (cascading validations without detailed items)
        if not failed_items and not passed_items:
            count = validation_data.get('count', 0)
            total_checked = validation_data.get('total_checked', 0)
            
            if count > 0 or total_checked > 0:
                # Create synthetic items for count-only validations
                # This allows them to appear in detailed table with summary information
                if count > 0:
                    failed_items = [{
                        'identifier': f"Summary ({count} exceptions)",
                        'issue': f"{sub_type2.lower().replace(' ', '_')}",
                        'count': count,
                        'total_checked': total_checked,
                        'is_summary': True,
                        'validation_type': f"{sub_type} - {sub_type2}"
                    }]
                else:
                    passed_items = [{
                        'identifier': f"Summary ({total_checked} checked, 0 exceptions)", 
                        'issue': f"{sub_type2.lower().replace(' ', '_')}",
                        'count': 0,
                        'total_checked': total_checked,
                        'is_summary': True,
                        'validation_type': f"{sub_type} - {sub_type2}"
                    }]
        
        # Check if any items have extra_data_children with actual children
        for item in failed_items + passed_items:
            if item.get('extra_data_children') and len(item.get('extra_data_children', [])) > 0:
                has_breakdown_data = True
                break
        
        if has_breakdown_data:
            # For validations with breakdown data, create parent-child structure
            tabs_data[tab_key][sub_type2]['has_breakdown'] = True
            
            # Process failed items with breakdown
            for item in failed_items:
                if item.get('extra_data_children') and len(item.get('extra_data_children', [])) > 0:
                    for child_item in item.get('extra_data_children', []):
                        child_row = _createChildRowForTabs(child_item, sub_type, sub_type2, source_a_label, source_b_label, threshold_str, is_failed=True, validation_precision_type=validation_precision_type, kpi_description=kpi_description)
                        if child_row:
                            # Process grandchildren (nested children) for dividend structure
                            if child_item.get('extra_data_children') and len(child_item.get('extra_data_children', [])) > 0:
                                child_row['_children'] = []  # Add children array to child row
                                for grandchild_item in child_item.get('extra_data_children', []):
                                    grandchild_row = _createChildRowForTabs(grandchild_item, sub_type, sub_type2, source_a_label, source_b_label, threshold_str, is_failed=True, validation_precision_type=validation_precision_type, kpi_description=kpi_description)
                                    if grandchild_row:
                                        child_row['_children'].append(grandchild_row)
                                        # Note: Grandchildren (3rd level securities) are NOT counted in tab counts
                            
                            tabs_data[tab_key][sub_type2]['children'].append(child_row)
                            # Count children as exceptions only if they are actual exceptions and NOT for Expenses/Fees tabs
                            if tab_key not in ['Expenses', 'Fees'] and child_row.get('isException', False):
                                category_counts[tab_key] += 1
            
            # Process passed items with breakdown
            for item in passed_items:
                if item.get('extra_data_children') and len(item.get('extra_data_children', [])) > 0:
                    for child_item in item.get('extra_data_children', []):
                        child_row = _createChildRowForTabs(child_item, sub_type, sub_type2, source_a_label, source_b_label, threshold_str, is_failed=False, validation_precision_type=validation_precision_type, kpi_description=kpi_description)
                        if child_row:
                            # Process grandchildren (nested children) for dividend structure
                            if child_item.get('extra_data_children') and len(child_item.get('extra_data_children', [])) > 0:
                                child_row['_children'] = []  # Add children array to child row
                                for grandchild_item in child_item.get('extra_data_children', []):
                                    grandchild_row = _createChildRowForTabs(grandchild_item, sub_type, sub_type2, source_a_label, source_b_label, threshold_str, is_failed=False, validation_precision_type=validation_precision_type, kpi_description=kpi_description)
                                    if grandchild_row:
                                        child_row['_children'].append(grandchild_row)
                                        # Note: Grandchildren (3rd level securities) are NOT counted in tab counts
                            
                            tabs_data[tab_key][sub_type2]['children'].append(child_row)
                            # Count passed children as exceptions only if they are actual exceptions and NOT for Expenses/Fees tabs
                            if tab_key not in ['Expenses', 'Fees'] and child_row.get('isException', False):
                                category_counts[tab_key] += 1
        
        elif tab_key in ['Expenses', 'Fees']:
            # For Expenses/Fees without breakdown data, use flat structure
            tabs_data[tab_key][sub_type2]['has_breakdown'] = False
            tabs_data[tab_key][sub_type2]['is_flat'] = True
            
            # Process failed items as direct rows (not children)
            for item in failed_items:
                # Add KPI description to item for tooltip
                item_with_kpi = item.copy()
                item_with_kpi['kpi_description'] = kpi_description
                row = _createEntityRowForTabs(item_with_kpi, sub_type, sub_type2, source_a_label, source_b_label, threshold_str, is_failed=True, validation_precision_type=validation_precision_type)
                if row:
                    # For flat expenses/fees, set security as subType2 (the expense/fee name)
                    row['security'] = sub_type2
                    # Store as flat rows, not children
                    tabs_data[tab_key][sub_type2]['flat_rows'] = tabs_data[tab_key][sub_type2].get('flat_rows', [])
                    tabs_data[tab_key][sub_type2]['flat_rows'].append(row)
                    # Count only if it's an actual exception (based on calculated isException)
                    if row.get('isException', False):
                        category_counts[tab_key] += 1
            
            # Process passed items as direct rows (not children)
            for item in passed_items:
                # Add KPI description to item for tooltip
                item_with_kpi = item.copy()
                item_with_kpi['kpi_description'] = kpi_description
                row = _createEntityRowForTabs(item_with_kpi, sub_type, sub_type2, source_a_label, source_b_label, threshold_str, is_failed=False, validation_precision_type=validation_precision_type)
                if row:
                    # For flat expenses/fees, set security as subType2 (the expense/fee name)
                    row['security'] = sub_type2
                    # Store as flat rows, not children
                    tabs_data[tab_key][sub_type2]['flat_rows'] = tabs_data[tab_key][sub_type2].get('flat_rows', [])
                    tabs_data[tab_key][sub_type2]['flat_rows'].append(row)
                    # Count only if it's an actual exception (based on calculated isException)
                    if row.get('isException', False):
                        category_counts[tab_key] += 1
        else:
            # For other tabs (Pricing, Positions, Market Value), support SubType3 (By Trade) level if present
            tabs_data[tab_key][sub_type2]['has_breakdown'] = False
            
            # Check if this validation has subType3 (By Trade) level
            if sub_type3:
                # Initialize subType3 group if it doesn't exist
                if sub_type3 not in tabs_data[tab_key][sub_type2]['subtype3_groups']:
                    tabs_data[tab_key][sub_type2]['subtype3_groups'][sub_type3] = {
                        'children': [],
                        'validation_data': validation_data
                    }
                
                # Process failed items as children under SubType3 (By Trade)
                for item in failed_items:
                    # Add KPI description to item for tooltip
                    item_with_kpi = item.copy()
                    item_with_kpi['kpi_description'] = kpi_description
                    row = _createEntityRowForTabs(item_with_kpi, sub_type, sub_type2, source_a_label, source_b_label, threshold_str, is_failed=True, validation_precision_type=validation_precision_type)
                    if row:
                        tabs_data[tab_key][sub_type2]['subtype3_groups'][sub_type3]['children'].append(row)
                        # Count only if it's an actual exception (based on calculated isException)
                        if row.get('isException', False):
                            category_counts[tab_key] += 1
                
                # Process passed items as children under SubType3 (By Trade)
                for item in passed_items:
                    # Add KPI description to item for tooltip
                    item_with_kpi = item.copy()
                    item_with_kpi['kpi_description'] = kpi_description
                    row = _createEntityRowForTabs(item_with_kpi, sub_type, sub_type2, source_a_label, source_b_label, threshold_str, is_failed=False, validation_precision_type=validation_precision_type)
                    if row:
                        tabs_data[tab_key][sub_type2]['subtype3_groups'][sub_type3]['children'].append(row)
                        # Count only if it's an actual exception (based on calculated isException)
                        if row.get('isException', False):
                            category_counts[tab_key] += 1
            else:
                # Normal SubType2 parent-child structure (no SubType3)
                # Process failed items as children under SubType2
                for item in failed_items:
                    # Add KPI description to item for tooltip
                    item_with_kpi = item.copy()
                    item_with_kpi['kpi_description'] = kpi_description
                    row = _createEntityRowForTabs(item_with_kpi, sub_type, sub_type2, source_a_label, source_b_label, threshold_str, is_failed=True, validation_precision_type=validation_precision_type)
                    if row:
                        tabs_data[tab_key][sub_type2]['children'].append(row)
                        # Count only if it's an actual exception (based on calculated isException)
                        if row.get('isException', False):
                            category_counts[tab_key] += 1
                
                # Process passed items as children under SubType2
                for item in passed_items:
                    # Add KPI description to item for tooltip
                    item_with_kpi = item.copy()
                    item_with_kpi['kpi_description'] = kpi_description
                    row = _createEntityRowForTabs(item_with_kpi, sub_type, sub_type2, source_a_label, source_b_label, threshold_str, is_failed=False, validation_precision_type=validation_precision_type)
                    if row:
                        tabs_data[tab_key][sub_type2]['children'].append(row)
                        # Count only if it's an actual exception (based on calculated isException)
                        if row.get('isException', False):
                            category_counts[tab_key] += 1
    
    # Create tabs structure with SubType2 as parent rows
    tabs = []
    for tab_key, subtype2_data in tabs_data.items():
        # Convert hierarchical data to parent-child format
        row_data = []
        
        for sub_type2, data_info in subtype2_data.items():
            children = data_info.get('children', [])
            validation_data = data_info['validation_data']
            has_breakdown = data_info.get('has_breakdown', False)
            is_flat = data_info.get('is_flat', False)
            flat_rows = data_info.get('flat_rows', [])
            subtype3_groups = data_info.get('subtype3_groups', {})
            
            if is_flat:
                # For flat structure (no breakdown), add rows directly without parent
                for flat_row in flat_rows:
                    row_data.append(flat_row)
            elif children and len(children) > 0:
                # For hierarchical structure (with breakdown), create parent-child
                threshold_value = validation_data.get('threshold')
                threshold_str = str(threshold_value) if threshold_value is not None else '-'
                validation_precision_type = _convert_precision_type_to_string(validation_data.get('precision_type', 'PERCENTAGE'))
                
                # Format threshold for parent row based on precision type
                parent_threshold_str = '-'
                if threshold_value is not None:
                    try:
                        if _convert_precision_type_to_string(validation_precision_type) == 'PERCENTAGE':
                            parent_threshold_str = f"{threshold_value}%"
                        else:  # ABSOLUTE
                            parent_threshold_str = f"${threshold_value:,.2f}"
                    except (ValueError, TypeError):
                        parent_threshold_str = str(threshold_value)
                
                # Calculate aggregated values for parent row - only for Expenses and Fees
                parent_source_a = None
                parent_source_b = None
                parent_change = None
                parent_threshold = parent_threshold_str
                
                if tab_key in ['Expenses', 'Fees']:
                    # For expenses/fees, show aggregated main KPI values (not breakdown totals)
                    all_items = validation_data.get('failed_items', []) + validation_data.get('passed_items', [])
                    if all_items:
                        # Take the main validation values (main KPI level)
                        main_item = all_items[0]  # First item represents the main KPI
                        parent_source_a = main_item.get('value_a')
                        parent_source_b = main_item.get('value_b') 
                        parent_change = main_item.get('change_value')
                        parent_threshold = threshold_str
                        # Use isParentException from the main item if available, otherwise calculate it
                        parent_is_exception = main_item.get('isParentException', False)
                
                # Create change tooltip for parent row (will be reformatted later with consistent formatting)
                parent_change_tooltip = ""
                if parent_source_a is not None and parent_source_b is not None:
                    try:
                        absolute_change = float(parent_source_b) - float(parent_source_a)
                        parent_change_tooltip = _formatNumericValue(absolute_change, 'ABSOLUTE', significant_figures=3, show_currency=True)
                    except (ValueError, TypeError):
                        parent_change_tooltip = validation_data.get('kpi_description', '')
                else:
                    parent_change_tooltip = validation_data.get('kpi_description', '')
                
                # Create parent row with appropriate fields based on tab type
                parent_row = {
                    "security": sub_type2,  # SubType2 becomes the security name
                    "subType": sub_type2,
                    "subType2": "",
                    "tooltipInfo": parent_change_tooltip,  # Show actual change values
                    "precision_type": validation_precision_type,
                    "validation_precision_type": validation_precision_type,
                    "isEditable": False,
                    "isRemarkOnlyEditable": False,
                    "_children": children  # Add children
                }

                # Add threshold to parent row for all tabs
                parent_row["threshold"] = parent_threshold_str
                
                # Add data fields only for tabs that have meaningful parent values
                if tab_key in ['Expenses', 'Fees']:
                    # Format threshold with proper units
                    formatted_parent_threshold = parent_threshold
                    if parent_threshold and parent_threshold != '-':
                        try:
                            # Clean the threshold string and convert to float
                            clean_threshold = parent_threshold.replace('%', '').replace('$', '').replace(',', '').replace('>', '').strip()
                            threshold_num = float(clean_threshold)
                            
                            # Use consistent formatting with _formatNumericValue to match other expenses
                            formatted_parent_threshold = _formatNumericValue(threshold_num, validation_precision_type, significant_figures=3, show_currency=True)
                            #Set assetType to parent assetType
                            parent_row["assetType"] = validation_data.get('asset_type', '-')
                        except (ValueError, AttributeError, TypeError):
                            formatted_parent_threshold = parent_threshold
                    
                    # Use isParentException from validation data, with fallback to children check
                    if 'parent_is_exception' not in locals():
                        # Fallback: check if any children are exceptions if isParentException not available
                        parent_is_exception = bool(any(child.get('isException', False) for child in children))
                    
                    # Format parent values using consistent formatting
                    formatted_parent_source_a = _formatNumericValue(parent_source_a, 'ABSOLUTE', significant_figures=3, show_currency=True) if parent_source_a is not None else '-'
                    formatted_parent_source_b = _formatNumericValue(parent_source_b, 'ABSOLUTE', significant_figures=3, show_currency=True) if parent_source_b is not None else '-'
                    
                    # Format change tooltip with consistent formatting
                    if parent_source_a is not None and parent_source_b is not None:
                        try:
                            absolute_change = float(parent_source_b) - float(parent_source_a)
                            formatted_parent_change_tooltip = _formatNumericValue(absolute_change, 'ABSOLUTE', significant_figures=3, show_currency=True)
                            
                            # Calculate percentage change for expenses parent
                            if validation_precision_type == 'PERCENTAGE' and float(parent_source_a) != 0:
                                percentage_change = ((float(parent_source_b) - float(parent_source_a)) / abs(float(parent_source_a))) * 100
                                parent_change = percentage_change
                        except (ValueError, TypeError):
                            formatted_parent_change_tooltip = validation_data.get('kpi_description', '')
                    else:
                        formatted_parent_change_tooltip = validation_data.get('kpi_description', '')
                    
                    parent_row.update({
                        "threshold": formatted_parent_threshold,
                        "sourceAValue": formatted_parent_source_a,
                        "sourceBValue": formatted_parent_source_b,
                        "change": parent_change,  # Use raw numeric value without formatting
                        "changeTooltip": formatted_parent_change_tooltip,  # Use formatted change tooltip
                        "action": None,
                        "selectedAction": None,
                        "newValue": None,
                        "remark": None,
                        "comments": None,
                        "assignedOn": None,
                        "age": None,
                        "storedAge": None,
                        "isParentException": parent_is_exception  # Parent exception status
                    })
                    
                    # Count parent rows for Expenses/Fees if they are exceptions
                    if parent_is_exception:
                        category_counts[tab_key] += 1
                        
                # For other tabs, parent rows are just category headers with no data fields
                
                row_data.append(parent_row)
            elif subtype3_groups and len(subtype3_groups) > 0:
                # Handle SubType3 (By Trade) level - create intermediate level between SubType2 and individual items
                for subtype3_name, subtype3_data in subtype3_groups.items():
                    subtype3_children = subtype3_data.get('children', [])
                    if subtype3_children and len(subtype3_children) > 0:
                        # Create SubType2 parent row first
                        subtype2_parent_row = {
                            "security": sub_type2,  # SubType2 becomes the security name
                            "subType": sub_type2,
                            "subType2": "",
                            "tooltipInfo": validation_data.get('kpi_description', ''),
                            "precision_type": _convert_precision_type_to_string(validation_data.get('precision_type', 'PERCENTAGE')),
                            "validation_precision_type": _convert_precision_type_to_string(validation_data.get('precision_type', 'PERCENTAGE')),
                            "isEditable": False,
                            "isRemarkOnlyEditable": False,
                            "_children": []  # Will contain SubType3 rows
                        }
                        
                        # Create SubType3 (By Trade) intermediate row
                        subtype3_row = {
                            "security": subtype3_name,  # SubType3 (By Trade) becomes the security name
                            "subType": subtype3_name,
                            "subType2": "",
                            "tooltipInfo": f"{subtype3_name} level validation",
                            "precision_type": _convert_precision_type_to_string(validation_data.get('precision_type', 'PERCENTAGE')),
                            "validation_precision_type": _convert_precision_type_to_string(validation_data.get('precision_type', 'PERCENTAGE')),
                            "isEditable": False,
                            "isRemarkOnlyEditable": False,
                            "_children": subtype3_children  # Individual items go here
                        }
                        
                        # Add SubType3 row as child of SubType2
                        subtype2_parent_row["_children"].append(subtype3_row)
                        row_data.append(subtype2_parent_row)
            # Skip empty categories (no children and not flat)
        
        # Check if this tab has any tree data (rows with _children)
        has_tree_data = any(row.get("_children") for row in row_data)
        
        # Dynamically determine which columns to show based on what data exists in rows
        cols_to_show = []
        all_row_data = []
        
        # Collect all rows including children to analyze what fields have data
        for row in row_data:
            all_row_data.append(row)
            if row.get("_children"):
                for child in row["_children"]:
                    all_row_data.append(child)
                    # Handle nested children (SubType3 level)
                    if child.get("_children"):
                        all_row_data.extend(child["_children"])
        
        # Check which fields have meaningful data (not null/undefined for all rows)
        field_checks = {
            "threshold": any(row.get("threshold") is not None for row in all_row_data),
            "assetType": any(row.get("assetType") is not None for row in all_row_data),
            "sourceAValue": any(row.get("sourceAValue") is not None for row in all_row_data),
            "sourceBValue": any(row.get("sourceBValue") is not None for row in all_row_data),
            "change": any(row.get("change") is not None for row in all_row_data),
            "selectedAction": any(row.get("selectedAction") is not None or row.get("action") for row in all_row_data),
            "newValue": True,  # Always include newValue for workflow
            "remark": True,  # Always include remark for workflow
            "comments": True,  # Always include comments for workflow
            "assignedOn": True,  # Always include assignedOn for workflow
            "age": True,  # Always include age for workflow
        }
        
        # Check if Asset Type should be shown for this tab
        show_asset_type = False
        if all_row_data:
            # Import the helper function to determine if Asset Type should be shown
            from clients.validusDemo.customFunctions.validation_utils import should_show_asset_type
            
            # Get the first row to determine validation category
            first_row = all_row_data[0]
            validation_category = first_row.get('subType', '')
            sub_type2 = first_row.get('subType2', '')
            
            # Check if any row has asset_type data
            has_asset_type_data = any(row.get("asset_type") is not None for row in all_row_data)
            
            # Determine if Asset Type should be shown based on validation category and source type
            show_asset_type = should_show_asset_type(validation_category, sub_type2, is_dual_source) and has_asset_type_data
        
        if show_asset_type:
            field_checks["assetType"] = True
        
        # Add columns that have meaningful data
        for field, has_data in field_checks.items():
            if has_data:
                cols_to_show.append(field)
        
        # Always include security field for flat Expenses/Fees tabs (even if no tree data)
        if tab_key in ['Expenses', 'Fees'] and not has_tree_data:
            if "security" not in cols_to_show:
                cols_to_show.insert(0, "security")  # Add at the beginning
        
        # Define header name for the security column
        security_header_name = tab_key if tab_key in ['Expenses', 'Fees'] else "Security"
        
        tab = {
            "overridenModuleMeta": {
                "moduleType": "nestedTable",
                "treeData": has_tree_data,
                "treeDataChildrenField": "_children" if has_tree_data else None,
                "isExternalFilterPresent": True,
                "groupDefaultExpanded": "0" if has_tree_data else None,
                "isGroupOpenByDefault":True,
                
            "colsToShow": cols_to_show,
            "autoSizeColumns" : False,
            "columnConfig": {
                "security": {
                    "name": security_header_name,
                    "cellStyle": {"textAlign": "left", "paddingLeft": "8px"},
                    "pinned": "left",
                    "minWidth": 200,
                    "flex": 1,
                    "customCellRenderer": "exceptionHighlightAggregator"
                },
                "assetType": {
                    "name": "Asset Type",
                    "cellStyle": {"textAlign": "left", "paddingLeft": "8px"},
                    "minWidth": 150,
                },
                "threshold": {
                    "name": "Threshold",
                    "cellStyle": {"textAlign": "center"},
                    "minWidth": 150,
                    "valueFormatter": [
                        "function",
                        "(params) => { if (params.value === null || params.value === undefined) return ''; return params.value; }"
                    ]
                },
                "sourceAValue": {
                    "name": source_a_label or "2024-01-31",
                     "minWidth": 150,
                    "cellStyle": {"textAlign": "right", "paddingRight": "8px"},
                    "valueFormatter": [
                        "function",
                        r"(params) => { if (params.value === null || params.value === undefined) return ''; const num = parseFloat(params.value); if (isNaN(num)) return params.value; return num.toFixed(2).replace(/\B(?=(\d{3})+(?!\d))/g, ','); }"
                    ]
                },
                "sourceBValue": {
                    "name": source_b_label,
                     "minWidth": 150,
                    "cellStyle": {"textAlign": "right", "paddingRight": "8px"},
                    "valueFormatter": [
                        "function",
                        r"(params) => { if (params.value === null || params.value === undefined) return ''; const num = parseFloat(params.value); if (isNaN(num)) return params.value; return num.toFixed(2).replace(/\B(?=(\d{3})+(?!\d))/g, ','); }"
                    ]
                },
                "change": {
                    "name": "Change",
                     "minWidth": 150,
                    "cellStyle": {"textAlign": "right", "paddingRight": "8px"},
                    "customCellRenderer": "tooltipAggregator",
                    "valueFormatter": [
                        "function",
                        r"(params) => { if (params.data?.subType2 === 'Missing Price') return '-'; if (params.value === null || params.value === undefined) return ''; const num = parseFloat(params.value); if (isNaN(num)) return params.value; const precisionType = params.data?.validation_precision_type; const isPercentage = precisionType === 'PERCENTAGE' || precisionType === 0; if (isPercentage) { return num.toFixed(2) + '%'; } return '$' + num.toFixed(2).replace(/\B(?=(\d{3})+(?!\d))/g, ','); }"
                    ]
                },
                "selectedAction": {
                    "name": "Actions",
                    "customCellRenderer": "validationDetailActionRenderer",
                    "minWidth": 150,
                    "suppressMenu": True,
                    "sortable": False,
                    "filter": False
                },
                "newValue": {
                    "name": "New Value",
                     "minWidth": 150,
                    "editable": [
                        "function",
                        "(params: any) => params.data?.isEditable"
                    ],
                    "customCellRenderer": "newValue"
                },
                "remark": {
                    "name": "Remark",
                     "minWidth": 150,
                    "editable": False,
                    "customCellRenderer": "validationDetailRemarkRenderer"
                },
                "comments": {
                    "name": "Comments",
                     "minWidth": 150,
                    "editable": True,
                    "cellEditor": "agTextCellEditor",
                    "cellStyle": {"textAlign": "left", "paddingLeft": "8px"},
                    "width": 200
                },
                "assignedOn": {
                    "minWidth": 150,
                    "name": "Assigned On",
                    "customCellRenderer": "assignedOn"
                },
                "age": {
                    "name": "Age",
                     "minWidth": 150,
                    "valueGetter": [
                        "function",
                        "(params: any) => {return params.data?.isAssignedOnEditable? params.data?.storedAge: '-';}"
                    ],
                    "customCellRenderer": "ageCellRenderer"
                }
            },
            "rowData": row_data,
            "uiConfig": {
                    "actions": ["No Change", "Override", "Assign"],
                    "users": ["Dan J", "Sarah M", "Harry O", "Peter H", "Sonia V"],
                "remarkOptions": [
                    "Good Data",
                    "Bad Data", 
                    "Analyst need to check"
                ],
                "actionBehavior": {
                    "no change": {
                        "isEditable": False,
                        "isRemarkOnlyEditable": True,
                        "isAssignedOnEditable": False
                    },
                    "override": {
                        "isEditable": True,
                        "isRemarkOnlyEditable": False,
                        "isAssignedOnEditable": False
                    },
                    "assign": {
                        "isEditable": False,
                        "isRemarkOnlyEditable": True,
                        "isAssignedOnEditable": True
                    }
                },
                "logConfig": {
                    "title": "Action Logs",
                    "totalAgeLabel": "Total Age",
                    "remarkLabel": "Remark",
                    "ageLabel": "Age",
                    "actorLabel": "Acted by"
                }
            }
            },
            "tabInternalKey": tab_key.lower().replace(' ', '').replace('&', ''),
            "nameOfTab": tab_key,
            "CountKeyInAPI": [tab_key.lower().replace(' ', '').replace('&', '')],
            "isActive": True,
            "showCount": True
        }
        
        # Add autoGroupColumnDef for tree data or for Expenses/Fees tabs
        if has_tree_data or tab_key in ['Expenses', 'Fees']:
            # For Fees and Expenses, always use agGroupCellRenderer even without tree data
            # This ensures the innerRenderer (exceptionHighlightAggregator) works properly
            cell_renderer = "agGroupCellRenderer"
            cell_renderer_params = {
                "suppressCount": True,
            }
            
            # If no tree data, we still need the group cell renderer for proper inner renderer support
            if not has_tree_data and tab_key in ['Expenses', 'Fees']:
                cell_renderer_params["suppressCount"] = True  # Hide count for flat data
            
            tab["overridenModuleMeta"]["autoGroupColumnDef"] = {
                "headerName": security_header_name,
                "field": "security",
                "innerRenderer": "exceptionHighlightAggregator",
                "pinned": "left",
                "minWidth": 200,
                "cellRenderer": cell_renderer,
                "cellRendererParams": cell_renderer_params
            }
        
        # Add the rowData to the tab
        tab["rowData"] = row_data
        
        # Filter out tabs with no meaningful data
        should_include_tab = True
        
        # General check: exclude completely empty tabs
        if not row_data or len(row_data) == 0:
            should_include_tab = False
            print(f"Excluding empty tab: {tab_key}")
        
        
        if should_include_tab:
            tabs.append(tab)
        else:
            # Remove from category_counts as well since we're not including the tab
            if tab_key in category_counts:
                del category_counts[tab_key]
    
    # Create response with tabs structure and count summary (dynamic)
    response = {
        "tabs": tabs,
        "noDataMessage": "No campaigns created yet"
    }
    
    # Add dynamic count keys based on actual categories
    for tab_key, count in category_counts.items():
        count_key = tab_key.lower().replace(' ', '').replace('&', '')
        response[count_key] = str(count)
    
    return response


def _formatSourceValue(value, show_currency=True):
    """
    Format source A and source B values to always show exactly 2 decimal places
    
    Args:
        value: Numeric value to format
        show_currency: Whether to show $ symbol for absolute values
    
    Returns:
        Formatted string with exactly 2 decimal places
    """
    if value is None or (isinstance(value, float) and value != value):  # Check for NaN
        return "-"
    
    try:
        numeric_value = float(value)
        
        if show_currency:
            # Handle negative numbers correctly - sign before dollar symbol
            if numeric_value < 0:
                return f"-${abs(numeric_value):,.2f}"
            else:
                return f"${numeric_value:,.2f}"
        else:
            return f"{numeric_value:,.2f}"
    except (ValueError, TypeError):
        return "-"

def _formatRatioValue(value):
    """
    Format ratio values to show exactly 3 significant digits without $ or % symbols
    
    Args:
        value: Numeric value to format
    
    Returns:
        Formatted string with 3 significant digits
    """
    if value is None or (isinstance(value, float) and value != value):  # Check for NaN
        return "-"
    
    try:
        numeric_value = float(value)
        
        # Handle zero case
        if numeric_value == 0:
            return "0.000"
        
        # Calculate significant figures formatting
        import math
        from decimal import Decimal, getcontext
        
        # Use Decimal for precise decimal arithmetic
        getcontext().prec = 28
        decimal_value = Decimal(str(abs(numeric_value)))
        
        # Convert to string to work with decimal places
        value_str = str(decimal_value)
        
        # Find the decimal point
        if '.' in value_str:
            integer_part, decimal_part = value_str.split('.', 1)
            # Remove trailing zeros
            decimal_part = decimal_part.rstrip('0')
            
            # Count leading zeros after decimal point
            leading_zeros = len(decimal_part) - len(decimal_part.lstrip('0'))
            # Take only the first 3 significant digits after decimal (skip leading zeros)
            significant_decimal = decimal_part.lstrip('0')[:3]
            # Calculate total decimal places: leading zeros + actual significant digits
            decimal_places = leading_zeros + len(significant_decimal)
        else:
            # No decimal point, add 3 decimal places
            decimal_places = 3
        
        # Format with calculated decimal places
        if numeric_value < 0:
            return f"-{abs(numeric_value):,.{decimal_places}f}"
        else:
            return f"{numeric_value:,.{decimal_places}f}"
    except (ValueError, TypeError):
        return "-"

def _formatNumericValue(value, precision_type='ABSOLUTE', significant_figures=2, show_currency=True):
    """
    Format numeric values consistently with US formatting (comma separators, 2 decimal places)
    
    Args:
        value: Numeric value to format
        precision_type: 'ABSOLUTE' for currency, 'PERCENTAGE' for percentage, or integer (0=PERCENTAGE, 1=ABSOLUTE)
        significant_figures: Number of decimal places (default 2, kept for backward compatibility)
        show_currency: Whether to show $ symbol for absolute values
    
    Returns:
        Formatted string
    """
    if value is None or (isinstance(value, float) and value != value):  # Check for NaN
        return "-"
    
    try:
        numeric_value = float(value)
        
        # Convert integer precision_type to string
        precision_type_str = _convert_precision_type_to_string(precision_type)
        
        # Always use 2 decimal places
        decimal_places = 2
        
        if precision_type_str == 'PERCENTAGE':
            return f"{numeric_value:.{decimal_places}f}%"
        else:  # ABSOLUTE
            if show_currency:
                # Handle negative numbers correctly - sign before dollar symbol
                if numeric_value < 0:
                    return f"-${abs(numeric_value):,.{decimal_places}f}"
                else:
                    return f"${numeric_value:,.{decimal_places}f}"
            else:
                return f"{numeric_value:,.{decimal_places}f}"
    except (ValueError, TypeError):
        return "-"

def _createEntityRow(item_data, sub_type, sub_type2, source_a_label, source_b_label, threshold_str, is_failed=True, validation_precision_type='PERCENTAGE'):
    """
    Create a row for an individual entity/security
    """
    # OPTIMIZATION: Use validation data directly instead of recalculating
    # Extract identifier (security name)
    identifier = item_data.get('identifier', '-')
    
    # Use validation-provided precision type
    precision_type = _convert_precision_type_to_string(item_data.get('precision_type', 'PERCENTAGE'))
    
    # Override precision_type for Unchanged Price validations to always show as percentage
    if sub_type2 == 'Unchanged Price':
        precision_type = 'PERCENTAGE'
    
    # OPTIMIZATION: Use pre-calculated values from validation engine
    source_a_value = item_data.get('value_a')
    source_b_value = item_data.get('value_b')
    change = item_data.get('change')  # Already calculated by validation engine
    tooltip_info = item_data.get('tooltip_change', item_data.get('display_change', ''))  # Use tooltip_change (opposite precision)
    
    # Try to get KPI description for tooltip (fallback for validations without change calculations)
    kpi_description = item_data.get('kpi_description', '')
    
    # Calculate tooltip_info if not already set (for fee validations)
    if not tooltip_info and source_a_value is not None and source_b_value is not None:
        try:
            absolute_change = float(source_b_value) - float(source_a_value)
            tooltip_info = _formatNumericValue(absolute_change, 'ABSOLUTE', significant_figures=3, show_currency=True)
        except (ValueError, TypeError):
            tooltip_info = ''
    
    # Handle single value validations (null/missing, zero, etc.)
    if source_a_value is None and source_b_value is None and 'value' in item_data:
        value = item_data.get('value')
        if 'raw_data_b' in item_data:
            source_b_value = value
        else:
            source_a_value = value
    
    # Handle special cases based on raw data
    if 'raw_data_a' in item_data and 'raw_data_b' in item_data:
        raw_a = item_data.get('raw_data_a', {})
        raw_b = item_data.get('raw_data_b', {})
        
        # For pricing validations, use market price
        if sub_type == 'Pricing':
            if source_a_value is None:
                source_a_value = raw_a.get('End Local Market Price') if raw_a else None
            if source_b_value is None:
                source_b_value = raw_b.get('End Local Market Price') if raw_b else None
        
        # For position validations, use quantity or market value
        elif sub_type == 'Positions':
            if 'Large Trades' in sub_type2:
                source_a_value = raw_a.get('End Book MV') if raw_a else None
                source_b_value = raw_b.get('End Book MV') if raw_b else None
            else:
                source_a_value = raw_a.get('End Qty') if raw_a else None
                source_b_value = raw_b.get('End Qty') if raw_b else None
        
        # For market value validations
        elif sub_type == 'Market Value':
            source_a_value = raw_a.get('End Book MV') if raw_a else None
            source_b_value = raw_b.get('End Book MV') if raw_b else None
        
        # For expense validations, use ending balance
        elif sub_type in ['Expenses', 'Fees']:
            source_a_value = raw_a.get('Ending Balance') if raw_a else None
            source_b_value = raw_b.get('Ending Balance') if raw_b else None
            
            # Use pre-calculated change_value if available, otherwise calculate based on precision_type
            if change is None and source_a_value is not None and source_b_value is not None:
                try:
                    if precision_type == 'ABSOLUTE':
                        change = float(source_b_value) - float(source_a_value)
                    else:  # PERCENTAGE 
                        if float(source_a_value) != 0:
                            change = ((float(source_b_value) - float(source_a_value)) / float(source_a_value)) * 100
                        else:
                            change = 999999.99 if float(source_b_value) != 0 else 0  # Use large number instead of inf
                except (ValueError, TypeError, ZeroDivisionError):
                    change = float(source_b_value) - float(source_a_value) if source_a_value and source_b_value else None
    
    # Special handling for Unchanged Price validations - always calculate as percentage
    if sub_type2 == 'Unchanged Price' and change is None and source_a_value is not None and source_b_value is not None:
        try:
            if float(source_a_value) != 0:
                change = ((float(source_b_value) - float(source_a_value)) / float(source_a_value)) * 100
            else:
                change = 999999.99 if float(source_b_value) != 0 else 0  # Use large number instead of inf
        except (ValueError, TypeError, ZeroDivisionError):
            change = None
    
    # Use identifier as security name for all validation types
    security_name = identifier
    display_sub_type2 = sub_type2
    
    # Format threshold based on validation precision_type with consistent decimal places
    formatted_threshold = threshold_str
    if threshold_str and threshold_str != '-':
        try:
            # Clean the threshold string and convert to float
            clean_threshold = threshold_str.replace('%', '').replace('$', '').replace(',', '').replace('>', '').strip()
            threshold_num = float(clean_threshold)
            
            # Use consistent formatting with 3 significant figures after decimal
            formatted_threshold = _formatNumericValue(threshold_num, validation_precision_type, significant_figures=3, show_currency=True)
        except (ValueError, AttributeError, TypeError):
            formatted_threshold = threshold_str
    
    # Note: Children handling is now done at the tabs level in _createDetailedTabularResponse
    # No longer creating children here since SubType2 becomes parent and items become children
    children = []

    # OPTIMIZATION: Use validation-provided exception status instead of recalculating
    # Ensure boolean values are JSON-serializable
    is_failed_value = item_data.get('is_failed')
    threshold_exceeded_value = item_data.get('threshold_exceeded')
    
    if is_failed_value is not None:
        calculated_is_exception = bool(is_failed_value)
    elif threshold_exceeded_value is not None:
        calculated_is_exception = bool(threshold_exceeded_value)
    else:
        calculated_is_exception = bool(is_failed)

    # Format source values consistently with US formatting
    display_source_a_value = _formatSourceValue(source_a_value, show_currency=True)
    display_source_b_value = _formatSourceValue(source_b_value, show_currency=True)
    
    # Special handling for Missing Price validations - show '-' for missing values
    if sub_type2 == 'Missing Price':
        if source_a_value is None or (isinstance(source_a_value, float) and source_a_value != source_a_value):  # Check for None or NaN
            display_source_a_value = "-"
        if source_b_value is None or (isinstance(source_b_value, float) and source_b_value != source_b_value):  # Check for None or NaN
            display_source_b_value = "-"

    if sub_type == 'Positions':
        display_source_a_value = _formatSourceValue(source_a_value, show_currency=False)
        display_source_b_value = _formatSourceValue(source_b_value, show_currency=False)

    # Extract Asset Type from item data and format for display
    from clients.validusDemo.customFunctions.validation_utils import format_asset_type_display
    asset_type = format_asset_type_display(item_data.get('asset_type', '-'))

    # OPTIMIZATION: Create row structure using validation-provided data
    row = {
        "security": security_name,
        "assetType": asset_type,
        "threshold": formatted_threshold,
        "subType": sub_type,
        "subType2": display_sub_type2,
        "sourceAValue": display_source_a_value,  # Use display values (may show '-' for missing)
        "sourceBValue": display_source_b_value,  # Use display values (may show '-' for missing)
        "actualSourceAValue": source_a_value,    # Preserve actual values for backend tracking
        "actualSourceBValue": source_b_value,    # Preserve actual values for backend tracking
        "change": change,  # Already calculated by validation engine
        "changeTooltip": tooltip_info,  # Pre-formatted by validation engine
        "tooltipInfo": tooltip_info if tooltip_info else kpi_description,  # Use validation-provided tooltip or KPI description
        "precision_type": precision_type,  # Use validation-provided precision type
        "validation_precision_type": validation_precision_type,  # Keep for compatibility
        "action": [
            "No Change",
            "Override",
            "Assign"
        ],
        "selectedAction": None,
        "newValue": None,
        "remark": "",
        "comments": "",
        "assignedOn": "-",
        "age": "-",
        "storedAge": "0 day",
        "isEditable": False,
        "isRemarkOnlyEditable": False,
        "isException": calculated_is_exception  # Already calculated by validation engine
    }
    
    # Pass through custom properties from validation items (e.g., isCorpAction, corpActionInfo)
    custom_properties = ['isCorpAction', 'corpActionInfo', 'raw_data_a', 'raw_data_b', 'issue', 'comparison']
    for prop in custom_properties:
        if prop in item_data:
            row[prop] = item_data[prop]
    
    
    return row



def _createEntityRowForTabs(item_data, sub_type, sub_type2, source_a_label, source_b_label, threshold_str, is_failed=True, validation_precision_type='PERCENTAGE'):
    """
    Create a row for an individual entity/security for tabs format
    Preserves ALL data from original _createEntityRow function
    """
    # CONSISTENCY FIX: Handle summary items from count-only validations
    if item_data.get('is_summary', False):
        # Create a summary row for count-only validations
        count = item_data.get('count', 0)
        total_checked = item_data.get('total_checked', 0)
        
        return {
            "security": item_data.get('identifier', 'Summary'),
            "threshold": threshold_str,
            "subType": sub_type,
            "subType2": sub_type2,
            "sourceAValue": "-",
            "sourceBValue": "-", 
            "change": count,
            "changeTooltip": f"{count} exceptions - Total checked: {total_checked}",
            "tooltipInfo": f"Summary validation: {count} exceptions out of {total_checked} items checked",
            "precision_type": "SUMMARY",
            "validation_precision_type": "SUMMARY",
            "action": ["View Details"],
            "selectedAction": None,
            "newValue": None,
            "remark": "",
            "comments": "",
            "assignedOn": "-",
            "age": "-",
            "storedAge": "0 day",
            "isEditable": False,
            "isRemarkOnlyEditable": False,
            "isException": count > 0,  # Summary item is exception if count > 0
            "isSummary": True
        }
    
    # Use the original _createEntityRow function to get complete data
    original_row = _createEntityRow(item_data, sub_type, sub_type2, source_a_label, source_b_label, threshold_str, is_failed, validation_precision_type)
    
    # Keep isException in tabs format
    tabs_row = original_row.copy()
    
    return tabs_row


def _createChildRowForTabs(child_item, sub_type, sub_type2, source_a_label, source_b_label, threshold_str, is_failed=True, validation_precision_type='PERCENTAGE', kpi_description=''):
    """
    Create a row for breakdown children (like expense/fee details) with full properties
    """
    # Get source values from child data - handle both expense/fee format and dividend format
    source_a_val = child_item.get('source_a_value', child_item.get('value_a', 0))
    source_b_val = child_item.get('source_b_value', child_item.get('value_b', 0))
    
    # Calculate change for children with consistent formatting
    change = None
    tooltip_info = ""
    
    # Special handling for dividend children and grandchildren
    if child_item.get('is_child', False):
        if child_item.get('issue') == 'dividend_source':
            # Individual security grandchildren - no change/threshold comparisons
            change = '-'
            tooltip_info = '-'
            formatted_source_a = '-' if source_a_val == '-' else _formatNumericValue(source_a_val, 'ABSOLUTE', significant_figures=3, show_currency=True)
            formatted_source_b = '-' if source_b_val == '-' else _formatNumericValue(source_b_val, 'ABSOLUTE', significant_figures=3, show_currency=True)
            child_threshold = '-'
            child_is_exception = False
            child_subtype = "Dividend Source"
        elif child_item.get('issue') == 'total_dividend_change':
            # Total Dividend child - has change/threshold comparisons
            if source_a_val != '-' and source_b_val != '-':
                try:
                    # Calculate both absolute and percentage changes
                    absolute_change = float(source_b_val) - float(source_a_val)
                    
                    if float(source_a_val) != 0:
                        percentage_change = ((float(source_b_val) - float(source_a_val)) / abs(float(source_a_val))) * 100
                    else:
                        percentage_change = 999999.99 if float(source_b_val) != 0 else 0
                    
                    # Use percentage as default for breakdown children with consistent formatting
                    change = percentage_change
                    tooltip_info = _formatNumericValue(absolute_change, 'ABSOLUTE', significant_figures=3, show_currency=True)
                except (ValueError, TypeError):
                    change = None
            
            # Format values for display with consistent US formatting
            formatted_source_a = _formatNumericValue(source_a_val, 'ABSOLUTE', significant_figures=3, show_currency=True)
            formatted_source_b = _formatNumericValue(source_b_val, 'ABSOLUTE', significant_figures=3, show_currency=True)
            
            # Total Dividend child inherits parent threshold
            child_threshold = threshold_str if threshold_str and threshold_str != '-' else '-'
            child_subtype = "Total Dividend"
            
            # Calculate exception status for Total Dividend child
            child_is_exception = bool(is_failed) if is_failed is not None else False
            if change is not None and threshold_str and threshold_str != '-':
                try:
                    # Clean the threshold string and convert to float
                    clean_threshold = threshold_str.replace('%', '').replace('$', '').replace(',', '').replace('>', '').strip()
                    threshold_num = float(clean_threshold)
                    
                    if _convert_precision_type_to_string(validation_precision_type) == 'PERCENTAGE':
                        child_is_exception = bool(abs(change) > threshold_num)
                    else:  # ABSOLUTE
                        if source_a_val != '-' and source_b_val != '-':
                            absolute_change = abs(float(source_b_val) - float(source_a_val))
                            child_is_exception = bool(absolute_change > threshold_num)
                except (ValueError, AttributeError, TypeError):
                    child_is_exception = bool(is_failed) if is_failed is not None else False
        else:
            # Other dividend children - use original logic
            change = '-'
            tooltip_info = '-'
            formatted_source_a = '-' if source_a_val == '-' else _formatNumericValue(source_a_val, 'ABSOLUTE', significant_figures=3, show_currency=True)
            formatted_source_b = '-' if source_b_val == '-' else _formatNumericValue(source_b_val, 'ABSOLUTE', significant_figures=3, show_currency=True)
            child_threshold = '-'
            child_is_exception = False
            child_subtype = "Dividend Source"
    else:
        # Original logic for expenses/fees
        if source_a_val != 0 and source_b_val != 0:
            try:
                # Calculate both absolute and percentage changes
                absolute_change = float(source_b_val) - float(source_a_val)
                
                if float(source_a_val) != 0:
                    percentage_change = ((float(source_b_val) - float(source_a_val)) / abs(float(source_a_val))) * 100
                else:
                    percentage_change = 999999.99 if float(source_b_val) != 0 else 0
                
                # Use percentage as default for breakdown children with consistent formatting
                change = percentage_change
                tooltip_info = _formatNumericValue(absolute_change, 'ABSOLUTE', significant_figures=3, show_currency=True)
            except (ValueError, TypeError):
                change = None
        
        # Format values for display with consistent US formatting
        formatted_source_a = _formatNumericValue(source_a_val, 'ABSOLUTE', significant_figures=3, show_currency=True)
        formatted_source_b = _formatNumericValue(source_b_val, 'ABSOLUTE', significant_figures=3, show_currency=True)
        
        # Determine subType based on the child item type
        child_subtype = "Fee Detail" if child_item.get('type') == 'fee_detail' else "Expense Detail"
    
        # For Expenses/Fees, children should inherit parent threshold
        child_threshold = "-"
        if sub_type in ['Expenses', 'Fees'] and threshold_str and threshold_str != '-':
            # Format threshold with proper units for children using consistent formatting
            try:
                # Clean the threshold string and convert to float
                clean_threshold = threshold_str.replace('%', '').replace('$', '').replace(',', '').replace('>', '').strip()
                threshold_num = float(clean_threshold)
                
                # Use consistent formatting with _formatNumericValue to match other expenses
                child_threshold = _formatNumericValue(threshold_num, validation_precision_type, significant_figures=3, show_currency=True)
            except (ValueError, AttributeError, TypeError):
                child_threshold = threshold_str
        
        # Calculate individual child's exception status based on threshold
        # Use pre-calculated exception status from child data if available
        if child_item.get('is_exception') is not None:
            child_is_exception = bool(child_item.get('is_exception'))
        else:
            child_is_exception = bool(is_failed) if is_failed is not None else False
            if change is not None and threshold_str and threshold_str != '-':
                try:
                    # Clean the threshold string and convert to float
                    clean_threshold = threshold_str.replace('%', '').replace('$', '').replace(',', '').replace('>', '').strip()
                    threshold_num = float(clean_threshold)
                    
                    # For major price change, major fx change, and major position change validations
                    if sub_type2 in ['Major Price Change', 'Major FX Change', 'Major Position Changes'] or 'By Trade - Qty' in str(child_item.get('subType3', '')):
                        if _convert_precision_type_to_string(validation_precision_type) == 'PERCENTAGE':
                            # For percentage thresholds, compare absolute percentage change
                            # change already includes * 100 (as percentage format), so compare directly with threshold_num
                            child_is_exception = bool(abs(change) > threshold_num) if change is not None else bool(is_failed)
                        else:  # ABSOLUTE
                            # For absolute thresholds, compare absolute value change
                            if source_a_val != 0 and source_b_val != 0:
                                absolute_change = abs(float(source_b_val) - float(source_a_val))
                                child_is_exception = bool(absolute_change > threshold_num)
                    else:
                        # For other validation types (like Expenses/Fees), use existing logic
                        if _convert_precision_type_to_string(validation_precision_type) == 'PERCENTAGE':
                            # For percentage thresholds, compare absolute percentage change
                            child_is_exception = bool(abs(change) > threshold_num)
                        else:  # ABSOLUTE
                            # For absolute thresholds, compare absolute value change
                            if source_a_val != 0 and source_b_val != 0:
                                absolute_change = abs(float(source_b_val) - float(source_a_val))
                                child_is_exception = bool(absolute_change > threshold_num)
                except (ValueError, AttributeError, TypeError):
                    # If we can't parse threshold, fall back to parent's determination
                    child_is_exception = bool(is_failed) if is_failed is not None else False
    
    # Create child row with full properties
    # Note: We should send all children regardless of exception status
    # The frontend will handle displaying them appropriately
    
    # Determine security name based on child type
    if child_item.get('is_child', False):
        if child_item.get('issue') == 'dividend_source':
            # Individual security grandchildren
            security_name = child_item.get('identifier', child_item.get('description', 'Unknown Security'))
        elif child_item.get('issue') == 'total_dividend_change':
            # Total Dividend child
            security_name = child_item.get('identifier', 'Total Dividends')
        else:
            # Other dividend children
            security_name = child_item.get('identifier', child_item.get('description', 'Unknown Security'))
    else:
        security_name = child_item.get('transaction_description', 'Unknown Transaction')
    
    child_row = {
        "security": security_name,
        "assetType": "-",  # Children rows don't have asset types
        "threshold": child_threshold,
        "subType": child_subtype,
        "subType2": "",
        "sourceAValue": formatted_source_a,
        "sourceBValue": formatted_source_b,
        "change": change,
        "changeTooltip": tooltip_info,
        "tooltipInfo": tooltip_info if tooltip_info else kpi_description,
        "precision_type": "PERCENTAGE",  # Default for breakdown children
        "validation_precision_type": "PERCENTAGE",
        "action": [
            "No Change",
            "Override", 
            "Assign"
        ],  # Actions available for children
        "selectedAction": None,
        "newValue": None,
        "remark": "",
        "comments": "",
        "assignedOn": "-",
        "age": "-",
        "storedAge": "0 day",
        "isEditable": False,
        "isRemarkOnlyEditable": False,
        "isException": child_is_exception,  # Child's own exception status based on individual threshold check
        "gl_account": child_item.get('gl_account', ''),
        "type": child_item.get('type', 'expense_detail')
    }
    
    return child_row


def _formatDateForDisplay(date_str):
    """
    Format date string for display in column headers
    Converts both YYYY-MM-DD and MM-DD-YYYY formats to "Jan 2024" format
    """
    if not date_str:
        return ""
    
    try:
        if '-' in date_str:
            parts = date_str.split('-')
            if len(parts) == 3:
                # Check if it's YYYY-MM-DD format (year first)
                if len(parts[0]) == 4:
                    year = parts[0]
                    month = int(parts[1])
                # Or MM-DD-YYYY format (month first)
                else:
                    month = int(parts[0])
                    year = parts[2]
                
                month_names = ['', 'Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun',
                             'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
                if 1 <= month <= 12:
                    return f"{month_names[month]} {year}"
    except (ValueError, IndexError):
        pass
    
    return date_str


def _createDetailedValidationResponse(validations, level1, level2, level3):
    """
    Create detailed validation response with entity-level information
    """
    response = {
        'filters': {
            'level1': level1,
            'level2': level2, 
            'level3': level3
        },
        'summary': {
            'total_validations': len(validations),
            'failed_validations': sum(1 for v in validations if v.get('message', 0) > 0),
            'passed_validations': sum(1 for v in validations if v.get('message', 0) == 0)
        },
        'validations': [],
        'entities': {
            'failed_entities': [],
            'passed_entities': [],
            'all_entities': []
        }
    }
    
    for validation in validations:
        validation_detail = {
            'type': validation.get('type'),
            'subType': validation.get('subType'),
            'subType2': validation.get('subType2'),
            'message': validation.get('message', 0),
            'status': 'FAILED' if validation.get('message', 0) > 0 else 'PASSED',
            'data': validation.get('data', {})
        }
        
        # Extract entity-level details from enhanced data
        data = validation.get('data', {})
        if 'failed_items' in data:
            for item in data['failed_items']:
                entity = {
                    'identifier': item.get('identifier', '-'),
                    'validation_type': f"{validation.get('subType', '')} - {validation.get('subType2', '')}",
                    'status': 'FAILED',
                    'details': item,
                    'validation_data': validation_detail
                }
                response['entities']['failed_entities'].append(entity)
                response['entities']['all_entities'].append(entity)
        
        if 'passed_items' in data:
            for item in data['passed_items']:
                entity = {
                    'identifier': item.get('identifier', '-'),
                    'validation_type': f"{validation.get('subType', '')} - {validation.get('subType2', '')}",
                    'status': 'PASSED',
                    'details': item,
                    'validation_data': validation_detail
                }
                response['entities']['passed_entities'].append(entity)
                response['entities']['all_entities'].append(entity)
        
        response['validations'].append(validation_detail)
    
    # Add summary statistics
    response['entity_summary'] = {
        'total_entities': len(response['entities']['all_entities']),
        'failed_entities': len(response['entities']['failed_entities']),
        'passed_entities': len(response['entities']['passed_entities']),
        'unique_identifiers': len(set(e['identifier'] for e in response['entities']['all_entities']))
    }
    
    return response


def _getOldNAVValidationsOutput(params: dict):
    """
    Legacy function for backward compatibility - to be cleaned up
    """
    myValidationOutput = getValidationOutput(params)
    NAVValidations = [item for item in myValidationOutput if item["type"] in ['PnL','NON-TRADING']]
    NAVValidations = [item for item in NAVValidations if item.get('subType2') != 'Performance Fees']
    
    # Filter out 'Stale Price' from NAVValidations entries when sourceB is available
    if params.get('query', {}).get('sourceB', ''):
        NAVValidations = [item for item in NAVValidations if item.get('subType2') != 'Stale Price']

    output={
        'totalValidations':getStatCard('TOTAL VALIDATIONS',str(len(NAVValidations))),
        'totalPassed':getStatCard('TOTAL PASSED',str(sum([el['message']==0 for el in NAVValidations]))),
        'totalFailed':getStatCard('TOTAL FAILED',str(sum([el['message']!=0 for el in NAVValidations]))),
        'totalExceptions':getStatCard('TOTAL EXCEPTIONS',str(-sum([el['message'] for el in NAVValidations]))),
        'pnlValidationsLevel1Table':getPnLValidationsLevel1Table(params),
    }

    return output

def dataValidationsPageOutput(params:dict, myValidationOutput: Optional[List[Dict]] = None):
    # Convert dates without mutating original params (prevents React re-render issues)
    params = _convertDatesToYmdFormat(params)
    

    noneCount = countNoneFields(myValidationOutput,'Missing Price')
    cashTypes = ['CASH', 'CASHF', 'INTONCASH', 'MMKTEQ']
    uniqueInstruments = set()

    for item in myValidationOutput:
        data = item.get('data')
        if isinstance(data, dict) and 'rows' in data:
            for row in data['rows']:
                assetClass = row.get('productAssetClass', '').upper()
                if assetClass not in cashTypes:
                    uniqueInstruments.add(row.get('productName'))

    uniqueInstrumentsCount = len(uniqueInstruments) 
    
    # Extract tab count from validation output
    tabCount = 0
    for item in myValidationOutput:
        if (item.get('type') == 'File' and 
            item.get('subType') == 'Tab Count' and 
            item.get('data') and 
            isinstance(item['data'], dict) and 
            'tabCount' in item['data']):
            tabCount = max(tabCount, item['data']['tabCount'])
    
    # If no tab count found, default to 0
    if tabCount == 0:
        tabCount = "0"
    else:
        tabCount = str(tabCount)
    
    output={
        "type": "secondary",
        "items": [
            {
            "label": "Time",
            "breakdown": [
                {
                "label": "Time for ingestion",
                "value": "5.05s"
                },
                {
                "label": "Preliminary check time",
                "value": "7.12s"
                },
                {
                "label": "Ratio calculation time",
                "value": "3.84s"
                },
                {
                "label": "Comparison time",
                "value": "2.03s"
                }
            ]
            },
            {
            "label": "Ingestion",
            "breakdown": [
                {
                "label": "Files ingested",
                "value": "2",
                "action": {
                    "type": "navigation",
                    "to": "/validus",
                    "parameters": [
                    {
                        "key": "page",
                        "value": "data-validations"
                    },
                    {
                        "key": "tab",
                        "value": "ingestion"
                    },
                    {
                        "key": "tab",
                        "value": "ingestion"
                    }
                    ]
                }
                },
                # removed funds ingested
                {
                "label": "Ingested reports",
                "value": tabCount,
                "action": {
                    "type": "navigation",
                    "to": "/validus",
                    "parameters": [
                    {
                        "key": "page",
                        "value": "data-validations"
                    },
                    {
                        "key": "tab",
                        "value": "ingestion"
                    }
                    ]
                }
                },
                {
                "label": "Market benchmarks",
                "value": "2",
                "action": {
                    "type": "navigation",
                    "to": "/validus",
                    "parameters": [
                    {
                        "key": "page",
                        "value": "data-validations"
                    },
                    {
                        "key": "tab",
                        "value": "ingestion"
                    }
                    ]
                }
                }
            ]
            },
            {
            "label": "Count",
            "breakdown": [
                {
                "label": "Null data fields",
                "value": noneCount,
                "action": {
                    "type": "navigation",
                    "to": "/validus",
                    "parameters": [
                    {
                        "key": "page",
                        "value": "data-validations"
                    },
                    {
                        "key": "tab",
                        "value": "count"
                    }
                    ]
                }
                },
                {
                "label": "Position line items as per the last NAV",
                "value": uniqueInstrumentsCount,
                "action": {
                    "type": "navigation",
                    "to": "/validus",
                    "parameters": [
                    {
                        "key": "page",
                        "value": "data-validations"
                    },
                    {
                        "key": "tab",
                        "value": "count"
                    }
                    ]
                }
                },
                {
                "label": "Months compared",
                "value": "2",
                "action": {
                    "type": "navigation",
                    "to": "/validus",
                    "parameters": [
                    {
                        "key": "page",
                        "value": "data-validations"
                    },
                    {
                        "key": "tab",
                        "value": "count"
                    }
                    ]
                }
                },
                {
                "label": "3rd-party accounts (PB / CP / Custodians)",
                "value": "3",
                "action": {
                    "type": "navigation",
                    "to": "/validus",
                    "parameters": [
                    {
                        "key": "page",
                        "value": "data-validations"
                    },
                    {
                        "key": "tab",
                        "value": "count"
                    }
                    ]
                }
                }
            ]
            }
        ]
    }
    return output

def getCheckPointsCombinedOutput(params:dict):
    # Convert dates without mutating original params (prevents React re-render issues)
    params = _convertDatesToYmdFormat(params)
    
    myRender = {
        "title": "CHECKPOINTS",
        "data": [
            {
                "title": {
                    "label": "Pricing Checks",
                    "status": "Completed",
                    "statusColor": "#22C55E"
                },
                "content": [
                    {
                        "label": {
                            "text": "Null / Missing Price Check",
                            "icon": "badgeCheck",
                            "iconColor": "text-green-600"
                        },
                        "info": {
                            "text": "Null / Missing Price Check",
                            "icon": "helpCircle",
                            "iconColor": "text-neutral-800"
                        },
                        "status": "Completed"
                    },
                    {
                        "label": {
                            "text": "Unchanged Price Check",
                            "icon": "badgeCheck",
                            "iconColor": "text-green-600"
                        },
                        "info": {
                            "text": "Unchanged Price Check",
                            "icon": "helpCircle",
                            "iconColor": "text-neutral-800"
                        },
                        "status": "Completed"
                    },
                    {
                        "label": {
                            "text": "Price Movement Threshold",
                            "icon": "badgeCheck",
                            "iconColor": "text-green-600"
                        },
                        "info": {
                            "text": "Price Movement Threshold",
                            "icon": "helpCircle",
                            "iconColor": "text-neutral-800"
                        },
                        "status": "Completed"
                    },
                    {
                        "label": {
                            "text": "Major FX Changes",
                            "icon": "badgeCheck",
                            "iconColor": "text-green-600"
                        },
                        "info": {
                            "text": "Major FX Changes",
                            "icon": "helpCircle",
                            "iconColor": "text-neutral-800"
                        },
                        "status": "Completed"
                    },
                    {
                        "label": {
                            "text": "Source Consistency Check",
                            "icon": "badgeCheck",
                            "iconColor": "text-green-600"
                        },
                        "info": {
                            "text": "Source Consistency Check",
                            "icon": "helpCircle",
                            "iconColor": "text-neutral-800"
                        },
                        "status": "Completed"
                    },
                    {
                        "label": {
                            "text": "Round Lot/Price Format Validation",
                            "icon": "badgeCheck",
                            "iconColor": "text-green-600"
                        },
                        "info": {
                            "text": "Round Lot/Price Format Validation",
                            "icon": "helpCircle",
                            "iconColor": "text-neutral-800"
                        },
                        "status": "Completed"
                    },
                    {
                        "label": {
                            "text": "Currency Consistency Check",
                            "icon": "badgeCheck",
                            "iconColor": "text-green-600"
                        },
                        "info": {
                            "text": "Currency Consistency Check",
                            "icon": "helpCircle",
                            "iconColor": "text-neutral-800"
                        },
                        "status": "Completed"
                    }
                ]
            },
            {
                "title": {
                    "label": "Positon Checks",
                    "status": "Passed",
                    "statusColor": "#22C55E"
                },
                "content": [
                    {
                        "label": {
                            "text": "Stale Price Check",
                            "icon": "badgeCheck",
                            "iconColor": "text-green-600"
                        },
                        "info": {
                            "text": "Stale Price Check",
                            "icon": "helpCircle",
                            "iconColor": "text-neutral-800"
                        },
                        "status": "Completed",

                    },
                    {
                        "label": {
                            "text": "Price Movement Threshold",
                            "icon": "badgeCheck",
                            "iconColor": "text-green-600"
                        },
                        "info": {
                            "text": "Price Movement Threshold",
                            "icon": "helpCircle",
                            "iconColor": "text-neutral-800"
                        },
                        "status": "Completed"
                    }
                ]
            },
            {
                "title": {
                    "label": "Trial Balance Checks",
                    "status": "Completed",
                    "statusColor": "#22C55E"
                },
                "content": [
                    {
                        "label": {
                            "text": "Stale Price Check",
                            "icon": "badgeCheck",
                            "iconColor": "text-green-600"
                        },
                        "info": {
                            "text": "Stale Price Check",
                            "icon": "helpCircle",
                            "iconColor": "text-neutral-800"
                        },
                        "status": "Completed"
                    },
                    {
                        "label": {
                            "text": "Price Movement Threshold",
                            "icon": "badgeCheck",
                            "iconColor": "text-green-600"
                        },
                        "info": {
                            "text": "Price Movement Threshold",
                            "icon": "helpCircle",
                            "iconColor": "text-neutral-800"
                        },
                        "status": "Completed"
                    }
                ]
            },
            {
                "title": {
                    "label": "Market Value",
                    "status": "Completed",
                    "statusColor": "#22C55E"
                },
                "content": [
                    {
                        "label": {
                            "text": "Threshold Review",
                            "icon": "badgeCheck",
                            "iconColor": "text-green-600"
                        },
                        "info": {
                            "text": "Flags income/expense entries above a specified threshold (e.g., >$100k or >0.5% NAV)",
                            "icon": "helpCircle",
                            "iconColor": "text-neutral-800"
                        },
                        "status": "Completed"
                    }
                ]
            },
            {
                "title": {
                    "label": "Non Trading Items Check",
                    "status": "Completed",
                    "statusColor": "#22C55E"
                },
                "content": [
                    {
                        "label": {
                            "text": "Threshold Review",
                            "icon": "badgeCheck",
                            "iconColor": "text-green-600"
                        },
                        "info": {
                            "text": "Flags income/expense entries above a specified threshold (e.g., >$100k or >0.5% NAV)",
                            "icon": "helpCircle",
                            "iconColor": "text-neutral-800"
                        },
                        "status": "Completed"
                    }
                ]
            }
        ],
        "cssProperties": {
            "padding": "24px",
            "borderRadius": "24px",
            "backgroundColor": "white"
        }
    }
    return myRender

def getRatioValidationsPageCombinedOutput(params:dict):
    """
    Generate ratio validations page output with flow visualization - Updated version
    This function calls the new implementation with correct structure
    """
    return ratioValidationsPageCombinedOutput(params)

def _getRatioValidationsTable(params: dict):
    """
    Create ratio validations table structure
    """
    myValidationOutput = getValidationOutput(params)
    ratioValidations = [it for it in myValidationOutput if it['type'] == 'Ratio']
    
    # Create simple table structure for ratio validations
    table_data = {
        'colsToShow': ['ratioType', 'ratioSubType', 'sourceA', 'sourceB', 'change', 'status'],
        "autoSizeColumns" : False,
        'columnConfig': {
            'ratioType': {'name': 'Ratio Type'},
            'ratioSubType': {'name': 'Sub Type'},
            'sourceA': {'name': 'Source A'},
            'sourceB': {'name': 'Source B'},
            'change': {'name': 'Change'},
            'status': {'name': 'Status'}
        },
        'rowData': []
    }
    
    for validation in ratioValidations:
        data = validation.get('data', {})
        row = {
            'ratioType': data.get('ratioType', validation.get('subType', '')),
            'ratioSubType': data.get('ratioSubType', validation.get('subType2', '')),
            'sourceA': data.get('sourceA', 0),
            'sourceB': data.get('sourceB', 0),
            'change': data.get('change', 0),
            'status': 'Major' if validation.get('message', 0) != 0 else 'Minor'
        }
        table_data['rowData'].append(row)
    
    return table_data

async def getValidationOutput(params:dict):
    """
    Get validation output - database approach for both NAV and Ratio validations
    Returns: validation_results
    OPTIMIZED: Now uses async parallel data fetching
    """
    # Convert dates to YYYY-MM-DD format for database operations (without mutating original)
    params = _convertDatesToYmdFormat(params)
    
    # Get validation output from database
    try:
        db_validation_output = await getValidationOutputFromDatabase(params)
        if db_validation_output:
            return db_validation_output
    except Exception as e:
        print(f"Error in database validation approach: {str(e)}")
    
    # If everything failed, raise error
    raise HTTPException(status_code=404, detail="No validation data found for the given parameters.")

async def getValidationOutputFromDatabase(params: dict):
    """
    Calculate validation output using database data and KPI library
    OPTIMIZED: Fetch all data in parallel to reduce response time
    """
    # Handle both direct params and nested query params
    myQuery = params.get('query', params)
    fund_name = myQuery.get('fundName')
    source_a = myQuery.get('sourceA', 'Bluefield')  # Default to Bluefield
    source_b = myQuery.get('sourceB', source_a)
    date_a = myQuery.get('dateA')
    date_b = myQuery.get('dateB', date_a)
    
    if not all([fund_name, source_a, date_a]):
        return None
    
    # Determine if this is a dual-source comparison (needed for both paths)
    is_dual_source = (source_a != source_b)
    
    # OPTIMIZATION: Fetch all data in parallel using the new parallel method
    try:
        data = await db_validation_service.get_parallel_validation_data(
            fund_name, source_a, source_b, date_a, date_b
        )
        
        trial_balance_a = data['trial_balance_a']
        portfolio_a = data['portfolio_a']
        dividend_a = data['dividend_a']
        trial_balance_b = data['trial_balance_b']
        portfolio_b = data['portfolio_b']
        dividend_b = data['dividend_b']
        
        print(f"Parallel data fetch completed for {fund_name}")
        
    except Exception as e:
        print(f"Error in parallel data fetch, falling back to sequential: {e}")
        # Fallback to sequential fetching
        trial_balance_a = db_validation_service.get_trial_balance_data(fund_name, source_a, date_a)
        portfolio_a = db_validation_service.get_portfolio_valuation_data(fund_name, source_a, date_a)
        dividend_a = db_validation_service.get_dividend_data(fund_name, source_a, date_a)

        print(f"is_dual_source: {is_dual_source}")
        
        trial_balance_b = []
        portfolio_b = []
        dividend_b = []
        
        if is_dual_source:
            # For dual-source comparisons, ALWAYS fetch both datasets (even if same date)
            # because we're comparing different sources for the same period
            trial_balance_b = db_validation_service.get_trial_balance_data(fund_name, source_b, date_b)
            portfolio_b = db_validation_service.get_portfolio_valuation_data(fund_name, source_b, date_b)
            dividend_b = db_validation_service.get_dividend_data(fund_name, source_b, date_b)
        elif date_b != date_a:
            # For single-source comparisons with different dates (period-over-period)
            trial_balance_b = db_validation_service.get_trial_balance_data(fund_name, source_b, date_b)
            portfolio_b = db_validation_service.get_portfolio_valuation_data(fund_name, source_b, date_b)
            dividend_b = db_validation_service.get_dividend_data(fund_name, source_b, date_b)
        else:            
            # For single-source same-date comparisons, use same data
            trial_balance_b = trial_balance_a
            portfolio_b = portfolio_a
            dividend_b = dividend_a

    if not trial_balance_a and not portfolio_a:
        return None
    
    # OPTIMIZATION 2: Get ALL KPIs ONCE with their thresholds
    fund_id = db_validation_service.get_fund_id_from_name(fund_name)
    all_kpis_with_thresholds = _get_all_kpis_with_thresholds(fund_id)
    print(f"fund_id: {fund_id}")
    # Filter KPIs by type and source_type
    if is_dual_source:
        # For dual-source comparisons, use DUAL_SOURCE KPIs
        nav_kpis = [kpi for kpi in all_kpis_with_thresholds 
                   if kpi.get('kpi_type') == 'NAV_VALIDATION' and kpi.get('source_type') == 'DUAL_SOURCE']
        ratio_kpis = [kpi for kpi in all_kpis_with_thresholds 
                     if kpi.get('kpi_type') == 'RATIO_VALIDATION' and kpi.get('source_type') == 'DUAL_SOURCE'
                     and kpi.get('numerator_field') and kpi.get('denominator_field')]
    else:
        # For single-source comparisons, use SINGLE_SOURCE KPIs
        nav_kpis = [kpi for kpi in all_kpis_with_thresholds 
                   if kpi.get('kpi_type') == 'NAV_VALIDATION' and kpi.get('source_type') == 'SINGLE_SOURCE']
        ratio_kpis = [kpi for kpi in all_kpis_with_thresholds 
                     if kpi.get('kpi_type') == 'RATIO_VALIDATION' and kpi.get('source_type') == 'SINGLE_SOURCE'
                     and kpi.get('numerator_field') and kpi.get('denominator_field')]
    
    validation_results = []
    
    # OPTIMIZATION 3: Pass pre-fetched data to validation engine
    try:
        nav_validation_results = nav_validations(
            fund_name, source_a, source_b, date_a, date_b, nav_kpis, fund_id,
            trial_balance_a=trial_balance_a, trial_balance_b=trial_balance_b,
            portfolio_a=portfolio_a, portfolio_b=portfolio_b,
            dividend_a=dividend_a, dividend_b=dividend_b,
            is_dual_source=is_dual_source
        )
        # Convert VALIDATION_STATUS objects to dictionaries
        for validation in nav_validation_results:
            validation_results.append(_convert_validation_to_dict(validation))
    except Exception as e:
        validation_results.append(_create_error_dict('NAV Validations', str(e)))
    
    # Calculate ratio validations using modular functions  
    try:
        ratio_validation_results = ratio_validations(
            fund_name, source_a, source_b, date_a, date_b, ratio_kpis, fund_id,
            trial_balance_a=trial_balance_a, trial_balance_b=trial_balance_b,
            portfolio_a=portfolio_a, portfolio_b=portfolio_b,
            dividend_a=dividend_a, dividend_b=dividend_b,
            is_dual_source=is_dual_source
        )
        # Convert VALIDATION_STATUS objects to dictionaries
        for validation in ratio_validation_results:
            validation_results.append(_convert_validation_to_dict(validation))
    except Exception as e:
        validation_results.append(_create_error_dict('Ratio Validations', str(e)))
    
    # Calculate file validations using modular functions
    # try:
    #     file_validation_results = file_validations(fund_name, source_a, source_b, date_a, date_b)
    #     # Convert VALIDATION_STATUS objects to dictionaries
    #     for validation in file_validation_results:
    #         validation_results.append(_convert_validation_to_dict(validation))
    # except Exception as e:
    #     print(f"Error in file validations: {e}")
    #     validation_results.append(_create_error_dict('File Validations', str(e)))
    
    return validation_results


def _get_all_kpis_with_thresholds(fund_id: Optional[int] = None) -> List[Dict]:
    """
    OPTIMIZATION: Fetch all active KPIs with their thresholds in a single batch operation
    Eliminates repeated database calls for KPI data and thresholds
    """
    try:
        # Get all active KPIs in one call
        all_kpis = db_validation_service.get_active_kpis()
        
        # Enrich each KPI with its threshold value
        kpis_with_thresholds = []
        for kpi in all_kpis:
            threshold = db_validation_service.get_kpi_threshold(kpi['id'], fund_id)
            if threshold is not None:  # Only include KPIs that have thresholds
                kpi_with_threshold = kpi.copy()
                kpi_with_threshold['threshold'] = threshold
                kpis_with_thresholds.append(kpi_with_threshold)
        
        return kpis_with_thresholds
    except Exception as e:
        print(f"Error fetching KPIs with thresholds: {e}")
        return []


def _convert_validation_to_dict(validation_status):
    """Convert VALIDATION_STATUS object to dictionary format"""
    return {
        'type': validation_status.type,
        'subType': validation_status.subType,
        'subType2': validation_status.subType2,
        'message': validation_status.message,
        'data': validation_status.data
    }


def _create_error_dict(validation_type: str, error_message: str):
    """Create error validation dictionary"""
    return {
        'type': 'Error',
        'subType': validation_type,
        'subType2': 'Processing Error',
        'message': -1,
        'data': {'error': error_message}
    }

# Old calculation functions removed - now using modular functions from customFunctions

def getFlowDataForRatioValidation(validation):
    """
    Dynamic function to create flow data for any ratio validation
    Converts field names to capitalized, spaced format and handles nav conversion
    """
    return _create_ratio_flow_data(validation, 
                                 validation.get('data', {}).get('sourceA', 0),
                                 validation.get('data', {}).get('sourceB', 0))
    
# Removed getSimpleFlowDataForRatio - now using dynamic _create_ratio_flow_data

def _parseExtraDataToRowMetaData(subType2,extraData, sourceAColName, sourceBColName):
    """
    Parse extraData JSON format and convert to rowMetaData format
    """
    if not extraData:
        return []
    
    rowMetaData = []    

    if subType2 == 'Debt-To-Equity Ratio':
        if 'sourceALiabilities' in extraData:
            rowMetaData.append({
                'particular': 'Liabilities',
                'value1': roundToSigDigits(extraData['sourceALiabilities'], 2),
                'value2': roundToSigDigits(extraData['sourceBLiabilities'], 2)
            })

        if 'sourceAAssets' in extraData:
            rowMetaData.append({
                'particular': 'Assets',
                'value1': roundToSigDigits(extraData['sourceAAssets'], 2),
                'value2': roundToSigDigits(extraData['sourceBAssets'], 2)
            })
    
    if subType2 == 'Gross Leverage Ratio': 

        if 'sourceAAssets' in extraData:
            rowMetaData.append({
                'particular': 'Assets',
                'value1': roundToSigDigits(extraData['sourceAAssets'], 2),
                'value2': roundToSigDigits(extraData['sourceBAssets'], 2)
            })

        if 'sourceANav' in extraData:
            rowMetaData.append({
                'particular': 'Nav',
                'value1': roundToSigDigits(extraData['sourceANav'], 2),
                'value2': roundToSigDigits(extraData['sourceBNav'], 2)
            })

    return rowMetaData




def roundToSigDigits(num, aSigDigits):
    if num == 0:
        return 0
    if num is None:
        return None

    return round(num, max(2,aSigDigits - 1 - floor(log10(abs(num)))))

# Removed old getNAVValidationDetailsTabs function - replaced by enhanced version below

def getPricingValidationsLevel2Table(params:dict):

    showUnchangedPrice = True
        
    if params['query'].get('sourceB', '') != '':
        showUnchangedPrice=False

    myValidationOutput = getValidationOutput(params)

    navValidations=[it for it in myValidationOutput if it['type']=='PnL' or it['type']=='NON-TRADING']
    # Filter out 'Performance Fees' from navValidations
    navValidations = [item for item in navValidations if item.get('subType2') != 'Performance Fees']

    myRows=[]

    rowActions=["No Change","Override","Assigne"]

    for aPricingValidation in navValidations:
        if aPricingValidation['message']==0:
            continue
        myProducts=aPricingValidation['data']['rows']
        for aProduct in myProducts:
            if aPricingValidation['subType']=='Pricing':
                # Handle missing ImpliedDirtyPrice values since we removed division-based calculation
                # Use market values as fallback for display purposes
                try:
                    a=roundToSigDigits(aProduct['ImpliedDirtyPriceBase___sourceA'],4)
                except (KeyError, TypeError):
                    a=roundToSigDigits(aProduct.get('periodEndMV_InBase___sourceA', 0),4)
                try:
                    b=roundToSigDigits(aProduct['ImpliedDirtyPriceBase___sourceB'],4)
                except (KeyError, TypeError):
                    b=roundToSigDigits(aProduct.get('periodEndMV_InBase___sourceB', 0),4)
            elif aPricingValidation['subType'] in ['Market Value','Positions','Expenses','Fees']:
                a=roundToSigDigits(aProduct['periodEndMV_InBase___sourceA'],4)
                b=roundToSigDigits(aProduct['periodEndMV_InBase___sourceB'],4)
              
            else:
                a=None
                b=None
            
            if aPricingValidation['type']=='NON-TRADING':
                securityName=aPricingValidation['subType2']
            else:
                securityName=aProduct['productName']
     
            if showUnchangedPrice == False and aPricingValidation['subType']=='Pricing' and aPricingValidation['subType2'] == 'Stale Price':
                continue
            else:
                # Calculate percentage change for Legal Fees, Admin Fees, and Management Fees
                if aPricingValidation['subType2'] in ['Legal Fees', 'Admin Fees', 'Management Fees']:
                    if a is None or b is None or a == 0:
                        change = None
                    else:
                        change = roundToSigDigits((b / a) - 1, 4)  # Percentage change
                else:
                    change = None if a is None or b is None else roundToSigDigits(b - a,4)  # Absolute change
                myRows.append({
                    'security': securityName,
                    'subType': aPricingValidation['subType'],
                    'subType2': (
                        None if aPricingValidation['subType'] == 'Expenses'
                        else ('Unchanged Price' if aPricingValidation['subType2'] == 'Stale Price' else aPricingValidation['subType2'])
                    ),
                    'sourceAValue': a,
                    'sourceBValue': b,
                    'change': change,
                    'action':rowActions,
                    'selectedAction':None,
                    "newValue": None,
                    "remark": "",
                    "assignedOn": "-",
                    "age": "-",
                    "storedAge": "0 day",
                    "isEditable": False,
                    "isRemarkOnlyEditable": False
                })
    # Determine column names based on dual source vs single source mode
    query = params.get('query', params)
    source_a = query.get('sourceA', 'Bluefield')
    source_b = query.get('sourceB', source_a)
    date_a = query.get('dateA', '')
    date_b = query.get('dateB', date_a)
    
    # Check if this is dual source (different sources) or single source (same source, different dates)
    is_dual_source = (source_a != source_b)
    
    if is_dual_source:
        # For dual source: use actual source names as column headers
        sourceAColName = source_a
        sourceBColName = source_b
    else:
        # For single source: use formatted dates as column headers
        sourceAColName = convertDateToFormat(date_a, 'MMM YYYY') if date_a else 'Period A'
        sourceBColName = convertDateToFormat(date_b, 'MMM YYYY') if date_b else 'Period B'

    myTableProperties={
        'sideBar':True,
        'tableType':"",
        "tableBottomButtons":True,
        "rowGroupings":[],
        "colsToShow":["security","sourceAValue","sourceBValue","change","selectedAction","newValue","remark","comments","assignedOn","age"],
       "autoSizeColumns" : False,
        "columnConfig":{
            "security": {
                "name": "Security",
                "flex": 1
            },
            "sourceAValue": {
                "name": sourceAColName 
            },
            "sourceBValue": {
                "name": sourceBColName
            },
            "change": {
                "name": "Change",
                "customCellRenderer": "tooltipAggregator",
                "valueFormatter": [
                    "function",
                    "(params) => { if (params.value === null || params.value === undefined) return '-'; const num = parseFloat(params.value); if (isNaN(num)) return params.value; if (params.data?.precision_type === 'PERCENTAGE') { return num.toFixed(2) + '%'; } return num.toLocaleString('en-US'); }"
                ]
            },
            "selectedAction": {
                "name": "Actions",
                "customCellRenderer": "validationDetailActionRenderer",
                "width": 100,
                "suppressMenu": True,
                "sortable": False,
                "filter": False
            },
            "newValue": {
                "name": "New Value",
                "editable": [
                    "function",
                    "(params: any) => params.data?.isEditable"
                    ],
                "customCellRenderer": "editableValueRenderer"
            },
            "remark": {
                "name": "Remark",
                "editable": False,
                "customCellRenderer": "validationDetailRemarkRenderer"
            },
            "assignedOn": {
                "name": "Assigned On",
                "customCellRenderer": "assignedOn"
            },
            "age": {
                "name": "Age",
                "valueGetter": [
                    "function",
                    "(params: any) => {return params.data?.isAssignedOnEditable? params.data?.storedAge: '-';}"
                ],
                "customCellRenderer": "ageCellRenderer"
            }
        },
        "rowData":myRows,
        "actionLogs":[],
        "uiConfig":{
            "actions": ["No Change", "Override", "Assign"],
            "users": ["Dan J", "Sarah M", "Harry O", "Peter H", "Sonia V"],
            "remarkOptions": [
                "Good Data",
                "Bad Data", 
                "Analyst need to check"
            ],
            "actionBehavior": {
                "no change": {
                    "isEditable": False,
                    "isRemarkOnlyEditable": True,
                    "isAssignedOnEditable": False
                },
                "override": {
                    "isEditable": True,
                    "isRemarkOnlyEditable": False,
                    "isAssignedOnEditable": False
                },
                "assign": {
                    "isEditable": False,
                    "isRemarkOnlyEditable": True,
                    "isAssignedOnEditable": True
                }
            },
            "logConfig": {
                "title": "Action Logs",
                "totalAgeLabel": "Total Age",
                "remarkLabel": "Remark",
                "ageLabel": "Age",
                "actorLabel": "Acted by"
            }
        }
    }

    return getNestedTableFromRenderStructure(myTableProperties)

def getPnLValidationsLevel1Table(params:dict):

    myValidationOutput = getValidationOutput(params)
    NAVValidations=[it for it in myValidationOutput if it['type']=='PnL' or it['type']=='NON-TRADING']
    # Filter out 'Performance Fees' from NAVValidations
    NAVValidations = [item for item in NAVValidations if item.get('subType2') != 'Performance Fees']
    myRows=[]
    for aPnLValidation in NAVValidations:
        # Convert PnL to P&L for display
        display_type = aPnLValidation['type']
        if display_type == 'PnL':
            display_type = 'P&L'
        
        myRows.append({
            'type':display_type,
            'subType':aPnLValidation['subType'],
            'subType2':aPnLValidation['subType2'],
            'threshold':getThresholdString(aPnLValidation['threshold']),
            'Status':"Passed" if aPnLValidation['message']==0 else "Failed",
            "Validations":1,
            "Exceptions":-aPnLValidation['message']
        })
    
    # Filter out 'Stale Price' entries when sourceB is available
    if params.get('query', {}).get('sourceB', ''):
        myRows = [row for row in myRows if row['subType2'] != 'Stale Price']
    
    myDF=pd.DataFrame(myRows)
    myTableProperties={
        'treeData':True,
        "groupDefaultExpanded":0,
        "treeDataChildrenField": "_children",
        "colsToShow":["threshold","Status","Validations","Exceptions"],
        "rowGroupings":[],
        "autoSizeColumns" : False,
        "columnConfig":{
            "Validations":{
                "name":"Validations",
                "agg":"sum",
                "type": "numericColumn",
                "cellStyle": {"textAlign": "right", "paddingRight": "8px"},
                "valueFormatter": [
                    "function", 
                    "(params) => { if (params.value === null || params.value === undefined) return '0'; return params.value.toLocaleString('en-US'); }"
                ]
            },
            "Exceptions":{
                "name":"Exceptions",
                "agg":"sum",
                "customCellRenderer":"exceptionRenderer",
                "type": "numericColumn",
                "cellStyle": {"textAlign": "right", "paddingRight": "8px"},
                "valueFormatter": [
                    "function", 
                    "(params) => { if (params.value === null || params.value === undefined) return '0'; return params.value.toLocaleString('en-US'); }"
                ]
            },
            "threshold": {
                "name": "Threshold",
                "cellStyle": {"textAlign": "center"}
            },            
            "Status":{
                "customCellRenderer":"labelToNumberAggregator",
                "filter": "agTextColumnFilter",
                "cellStyle": {"textAlign": "center"}
            }
        },
        "autoGroupColumnDef": {
            "headerName": "Type",
            "field": "_title",
            "cellRenderer": "agGroupCellRenderer",
            "cellRendererParams": {
                "suppressCount": True
            }
        },
        "rowData":createTreeDataFromRows(myDF,['type','subType','subType2'],['threshold','Status','Validations','Exceptions']),

    }

    return getNestedTableFromRenderStructure(myTableProperties)

def getThresholdString(aThreshold):
    if aThreshold is None:
        return ''
    else:
        return f"> ${aThreshold:.2f}"

def getFileValidationsSubPage(params:dict):
    myModules=[
            {
                "moduleName": "ValidationFilesHeader",
                # "TODO":"we should be able to provide the header here only if needed",
                "width": "100%"
            },
            {
                "moduleName": "_validusSF_nestedTable",
                "overrridenParam": {
                    "_funcName": "fileValidationSummaryTable"
                },
                "width": "100%",
                "height": "84%"
            }
        ]
    return getSubPageRender(myModules,"toBeNamedSubPage")

def ratioValidationsPageCombinedOutput(params: dict):
    """
    Generate ratio validations page output with flow visualization
    Format: totalRatios, majorDeviation, minorDeviation, ratioValidationsTable with flowData
    """
    try:
        # Convert dates to YMD format for database operations (without mutating original)
        params = _convertDatesToYmdFormat(params)
        
        # Get dynamic column labels - use source names for dual source, dates for single source
        query = params.get('query', params)
        source_a = query.get('sourceA', 'Bluefield')
        source_b = query.get('sourceB', source_a)
        date_a = query.get('dateA', '')
        date_b = query.get('dateB', date_a)
        
        # Determine if this is dual source (different sources) or single source (same source, different dates)
        is_dual_source = (source_a != source_b)
        
        if is_dual_source:
            # For dual source: use actual source names as column headers
            source_a_label = source_a
            source_b_label = source_b
        else:
            # For single source: use formatted dates as column headers
            source_a_label = _formatDateForDisplay(date_a) if date_a else "Period A"
            source_b_label = _formatDateForDisplay(date_b) if date_b else "Period B"
        
        # Try to get cached validation data first
        cache_key = _createValidationCacheKey(params)
        all_validations = _getCachedValidationData(cache_key)
        
        if all_validations is None:
            # Get validation data from database-driven approach (expensive operation)
            try:
                import asyncio
                # Check if we're in an event loop
                try:
                    loop = asyncio.get_running_loop()
                    # We're in an event loop, run in a thread
                    import concurrent.futures
                    import threading
                    future = concurrent.futures.Future()
                    def run_in_thread():
                        try:
                            result = asyncio.run(getValidationOutput(params))
                            future.set_result(result)
                        except Exception as e:
                            future.set_exception(e)
                    thread = threading.Thread(target=run_in_thread)
                    thread.start()
                    thread.join()
                    all_validations = future.result()
                except RuntimeError:
                    # No event loop running, safe to use asyncio.run()
                    all_validations = asyncio.run(getValidationOutput(params))
            except Exception as e:
                print(f"Error fetching validation output: {e}")
                all_validations = []
            
            # Cache database results
            if all_validations is not None:
                _setCachedValidationData(cache_key, all_validations)
            else:
                print("WARNING: getValidationOutput returned None, not caching")
        
        # Filter ratio validations from all validations
        if all_validations is None:
            ratio_validations = []
        else:
            ratio_validations = [item for item in all_validations if item.get('type', '').lower() == 'ratio']
        
        # Calculate summary stats
        total_ratios = len(ratio_validations)
        major_deviations = len([item for item in ratio_validations if item.get('message', 0) != 0])
        minor_deviations = total_ratios - major_deviations
        
        # Build response
        response = {
            "totalRatios": {
                "label": "TOTAL RATIOS",
                "value": str(total_ratios),
                "cssProperties": {
                    "borderRadius": "24px",
                    "padding": "24px"
                },
                "clickAction": {
                    "type": "navigation",
                    "to": "/validus",
                    "parameters": [
                        {
                            "key": "page",
                            "value": "ratio-validations"
                        }
                    ]
                }
            },
            "majorDeviation": {
                "label": "MAJOR DEVIATION", 
                "value": str(major_deviations),
                "cssProperties": {
                    "borderRadius": "24px",
                    "padding": "24px"
                },
                "clickAction": {
                    "type": "navigation",
                    "to": "/validus",
                    "parameters": [
                        {
                            "key": "page",
                            "value": "ratio-validations"
                        },
                        {
                            "key": "filter",
                            "value": "deviation-major"
                        }
                    ]
                }
            },
            "minorDeviation": {
                "label": "MINOR DEVIATION",
                "value": str(minor_deviations),
                "cssProperties": {
                    "borderRadius": "24px", 
                    "padding": "24px"
                },
                "clickAction": {
                    "type": "navigation",
                    "to": "/validus",
                    "parameters": [
                        {
                            "key": "page",
                            "value": "ratio-validations"
                        },
                        {
                            "key": "filter",
                            "value": "deviation-minor"
                        }
                    ]
                }
            },
            "ratioValidationsTable": {
                "tableType": "rowGrouping",
                "groupDefaultExpanded": 1,
                "masterDetail": True,
                "rowGroupings": ["type"],
                "autoSizeColumns" : False,
                "colsToShow": ["subType", "sourceAValue", "sourceBValue", "change", "threshold", "deviation", "action"],
                "columnConfig": {
                    "subType": {
                        "name": "Sub Type"
                    },
                    "deviation": {
                        "name": "Deviation",
                        "customCellRenderer": "statusAggregator",
                        "filter": "agTextColumnFilter"
                    },
                    "sourceAValue": {"name": source_a_label},
                    "sourceBValue": {"name": source_b_label},
                    "change": {
                        "name": "Change",
                        "cellStyle": {"textAlign": "right", "paddingRight": "8px"},
                        "valueFormatter": [
                            "function",
                            "(params) => { if (params.node && params.node.group) return ''; if (params.value === null || params.value === undefined) return '-'; return Number(params.value).toFixed(2); }"
                        ]
                    },
                    "threshold": {
                        "name": "Threshold",
                        "cellStyle": {"textAlign": "right", "paddingRight": "8px"},
                        "customCellRenderer": "tooltipAggregator"
                    },
                    "action": {
                        "name": "Computations",
                        "customCellRenderer": "flowModalIconAggregator",
                        "valueGetter": ["function", "(params: any) => params.data?.action || false"]
                    }
                },
                "autoGroupColumnDef": {
                    "headerName": "Type",
                    "tooltipField": "tooltipInfo"
                },
                "detailCellRendererParams": {
                    "detailGridOptions": {
                        "columnDefs": [
                            {
                                "field": "particular",
                                "headerName": "Particular"
                            },
                            {
                                "field": "value1",
                                "formatter": {
                                    "formatBy": "numberFormat",
                                    "options": {
                                        "decimalPoints": 2,
                                        "thousandsSeparator": True,
                                    }
                                },
                                "headerName": source_a_label
                            },
                            {
                                "field": "value2",
                                "formatter": {
                                    "formatBy": "numberFormat",
                                    "options": {
                                        "decimalPoints": 2,
                                        "thousandsSeparator": True,
                                    }
                                },
                                "headerName": source_b_label
                            }
                        ],
                        "defaultColDef": {
                            "flex": 1
                        },
                        "detailRowHeight": "500"
                    },
                    "getDetailRowData": [
                        "function",
                        "(params: any) => { if (params.data.hasDetailView) { params.successCallback(params.data.rowMetaData); } else { params.successCallback([]); } }"
                    ]
                },
                "rowData": []
            }
        }
        
        # Process ratio validations into table format
        for validation in ratio_validations:
            data = validation.get('data', {})
            source_a = data.get('sourceA', 0)
            source_b = data.get('sourceB', 0)
            change_value = data.get('change', 0)
            
            # Set change to null if both source A and source B values are zero
            if source_a == 0 and source_b == 0:
                change_value = None
            
            deviation_status = "Major" if validation.get('message', 0) != 0 else "Minor"
            
            # Enhance data with additional metric information for flow visualization
            enhanced_data = data.copy()
            enhanced_data.update({
                'numeratorA': data.get('sourceANumerator', 0),
                'numeratorB': data.get('sourceBNumerator', 0),
                'denominatorA': data.get('sourceADenominator', 1),
                'denominatorB': data.get('sourceBDenominator', 1),
                'numeratorDisplay': data.get('numeratorDescription', 'Numerator'),
                'denominatorDisplay': data.get('denominatorDescription', 'Denominator')
            })
            
            # Create flow visualization data with enhanced information
            enhanced_validation = validation.copy()
            enhanced_validation['data'] = enhanced_data
            flow_data = _create_ratio_flow_data(enhanced_validation, source_a, source_b)
            
            # Create row metadata for detailed view with actual metric values
            row_metadata = _create_ratio_row_metadata(validation, enhanced_data)
            
            # Get threshold value from validation data
            threshold_value = data.get('threshold', 0)
            threshold_str = f"{threshold_value:.2f}%" if threshold_value else "-"
            
            # Get KPI name and description for response (even if unused)
            kpi_name = data.get('kpi_name', validation.get('subType2', 'Unknown Ratio'))
            kpi_description = data.get('description', '') or data.get('kpi_description', '')
            
            # Replace threshold placeholders in description for tooltip
            if kpi_description and threshold_value:
                try:
                    formatted_threshold = f"{threshold_value:.2f}%"
                    # Replace both "threshold_value%" and "threshold_value" placeholders
                    kpi_description = kpi_description.replace('threshold_value%', formatted_threshold)
                    kpi_description = kpi_description.replace('threshold_value', formatted_threshold)
                except (ValueError, TypeError):
                    pass  # Keep original description if formatting fails
            
            # Format source values using ratio-specific formatting (3 significant digits, no symbols)
            formatted_source_a = _formatRatioValue(source_a)
            formatted_source_b = _formatRatioValue(source_b)
            
            # Handle zero values and set change to None when appropriate
            change_output = round(change_value, 2) if change_value is not None else None
            if (source_a == 0 or source_a is None) and (source_b == 0 or source_b is None):
                formatted_source_a = '-'
                formatted_source_b = '-'
                change_output = None
            if source_a is None or source_b is None:
                change_output = None
            
            # Format change with % symbol
            change_formatted = f"{change_output}%" if change_output is not None else '-'
            
            row = {
                "type": validation.get('subType', 'Financial'),
                "subType": validation.get('subType2', 'Unknown Ratio'),
                "sourceAValue": formatted_source_a,  # Use formatted source A value
                "sourceBValue": formatted_source_b,  # Use formatted source B value
                "change": change_formatted, # Formatted with % symbol
                "threshold": threshold_str,
                "deviation": deviation_status,
                "action": True,
                "flowData": flow_data,
                "rowMetaData": row_metadata,
                "hasDetailView": True,
                "kpiName": kpi_name,  # Keep in response (even if unused)
                "description": kpi_description,  # Keep description in response (even if unused)
                "tooltipInfo": kpi_description  # Add tooltipInfo for tooltipAggregator renderer
            }
            
            response["ratioValidationsTable"]["rowData"].append(row)
        
        return response
        
    except Exception as e:
        print(f"Error in ratioValidationsPageCombinedOutput: {e}")
        return {
            "totalRatios": {"label": "TOTAL RATIOS", "value": "0", "cssProperties": {"borderRadius": "24px", "padding": "24px"}},
            "majorDeviation": {"label": "MAJOR DEVIATION", "value": "0", "cssProperties": {"borderRadius": "24px", "padding": "24px"}},
            "minorDeviation": {"label": "MINOR DEVIATION", "value": "0", "cssProperties": {"borderRadius": "24px", "padding": "24px"}},
            "ratioValidationsTable": {
                "tableType": "rowGrouping", 
                "masterDetail": True, 
                "rowGroupings": ["type"], 
                "colsToShow": [], 
                "autoSizeColumns" : False,
                "columnConfig": {}, 
                "autoGroupColumnDef": {"headerName": "Type"}, 
                "detailCellRendererParams": {
                    "detailGridOptions": {
                        "columnDefs": [
                            {
                                "field": "particular",
                                "headerName": "Particular"
                            },
                            {
                                "field": "value1",
                                "formatter": {
                                    "formatBy": "numberFormat",
                                    "options": {
                                        "decimalPoints": 2,
                                        "thousandsSeparator": True,
                                        "trimTrailingZeros": True
                                    }
                                },
                                "headerName": source_a_label
                            },
                            {
                                "field": "value2",
                                "formatter": {
                                    "formatBy": "numberFormat",
                                    "options": {
                                        "decimalPoints": 2,
                                        "thousandsSeparator": True,
                                        "trimTrailingZeros": True
                                    }
                                },
                                "headerName": source_b_label
                            }
                        ],
                        "defaultColDef": {
                            "flex": 1
                        },
                        "detailRowHeight": "500"
                    },
                    "getDetailRowData": [
                        "function",
                        "(params: any) => { if (params.data.hasDetailView) { params.successCallback(params.data.rowMetaData); } else { params.successCallback([]); } }"
                    ]
                },
                "rowData": []
            }
        }

def _convert_field_to_display_name(field_name):
    """
    Convert database field names to capitalized, spaced display names
    Examples: 
    - 'total_assets' -> 'Total Assets'
    - 'nav' -> 'Net Asset Value'  
    - 'current_liabilities' -> 'Current Liabilities'
    - 'management_fees' -> 'Management Fees'
    - 'gross_leverage_ratio' -> 'Gross Leverage Ratio'
    - 'total equity' -> 'Total Equity'
    - 'Current Assets' -> 'Current Assets' (already formatted)
    """
    if not field_name:
        return field_name
    
    # Handle special case for NAV
    if field_name.lower() == 'nav':
        return 'Net Asset Value'
    
    # Convert snake_case to spaced and capitalize each word
    words = field_name.replace('_', ' ').split()
    capitalized_words = [word.capitalize() for word in words]
    return ' '.join(capitalized_words)

def _create_ratio_flow_data(validation, source_a, source_b):
    """Create flow diagram data for ratio visualization with dynamic field name conversion"""
    try:
        data = validation.get('data', {})
        ratio_name = validation.get('subType2', 'Unknown Ratio')
        
        # Get raw field names and convert to display format
        raw_numerator_name = data.get('numeratorDescription', 'Numerator')
        raw_denominator_name = data.get('denominatorDescription', 'Denominator')
        
        # Convert to capitalized, spaced format
        numerator_name = _convert_field_to_display_name(raw_numerator_name)
        denominator_name = _convert_field_to_display_name(raw_denominator_name)
        
        # Get actual metric values from validation data
        numerator_value_a = data.get('numeratorA', 0)
        numerator_value_b = data.get('numeratorB', 0)
        denominator_value_a = data.get('denominatorA', 1)
        denominator_value_b = data.get('denominatorB', 1)
        
        # Check if custom formula is provided from financial metrics, otherwise use standard division
        custom_formula = data.get('formula')
        if custom_formula:
            formula = custom_formula
        else:
            formula = f"= {numerator_name} / {denominator_name}"
        
        # Format values for display - use 3 significant digits without symbols
        def format_value(value, is_percentage=False):
            return _formatRatioValue(value)
            
        # Determine if this is a percentage-based ratio (like returns)
        is_percentage_ratio = any(keyword in ratio_name.lower() for keyword in ['return', 'excess', 'yield', 'rate'])
        
        # Create specific node IDs based on ratio type
        numerator_id = _get_node_id_for_field(numerator_name)
        denominator_id = _get_node_id_for_field(denominator_name)
        
        return {
            "nodes": [
                {
                    "id": numerator_id,
                    "type": "card",
                    "data": {
                        "label": numerator_name,
                        "value": format_value(numerator_value_b, is_percentage_ratio)
                    },
                    "position": {"x": 0, "y": 100}
                },
                {
                    "id": denominator_id,
                    "type": "card",
                    "data": {
                        "label": denominator_name,
                        "value": format_value(denominator_value_b, is_percentage_ratio)
                    },
                    "position": {"x": 200, "y": 100}
                },
                {
                    "id": "ratioFormula",
                    "type": "formula",
                    "data": {
                        "label": ratio_name,
                        "formula": formula
                    },
                    "position": {"x": 100, "y": 200}
                },
                {
                    "id": "ratioResult",
                    "type": "result",
                    "data": {
                        "label": ratio_name,
                        "value": format_value(source_b, is_percentage_ratio) if source_b is not None else (format_value(0, is_percentage_ratio))
                    },
                    "position": {"x": 100, "y": 300}
                }
            ],
            "edges": [
                {
                    "id": "e1",
                    "source": numerator_id,
                    "target": "ratioFormula",
                    "sourceHandle": "bottomCenter",
                    "targetHandle": "topLeft",
                    "animated": True
                },
                {
                    "id": "e2",
                    "source": denominator_id,
                    "target": "ratioFormula",
                    "sourceHandle": "bottomCenter",
                    "targetHandle": "topRight",
                    "animated": True
                },
                {
                    "id": "e3",
                    "source": "ratioFormula",
                    "target": "ratioResult",
                    "animated": True
                }
            ]
        }
    except Exception as e:
        print(f"Error creating flow data: {e}")
        return {"nodes": [], "edges": []}

def _get_node_id_for_field(field_name):
    """Convert field names to camelCase node IDs"""
    if not field_name:
        return '-'
    
    # Handle special cases
    field_mapping = {
        'Net Asset Value': 'nav',
    }
    
    # Check predefined mapping first
    if field_name in field_mapping:
        return field_mapping[field_name]
    
    # Dynamic conversion: convert to camelCase
    # Remove special characters and split into words
    words = field_name.replace('&', 'And').replace('-', ' ').split()
    if not words:
        return '-'
    
    # First word lowercase, rest capitalized
    camel_case = words[0].lower() + ''.join(word.capitalize() for word in words[1:])
    return camel_case

def _create_ratio_row_metadata(validation, data):
    """Create row metadata for detailed ratio view matching sample structure"""
    try:
        # Get raw field names and convert to display format
        raw_numerator_name = data.get('numeratorDescription', 'Numerator')
        raw_denominator_name = data.get('denominatorDescription', 'Denominator')
        
        # Convert to capitalized, spaced format
        numerator_name = _convert_field_to_display_name(raw_numerator_name)
        denominator_name = _convert_field_to_display_name(raw_denominator_name)
        
        # Get raw metric values
        numerator_a = data.get('numeratorA', 0)
        numerator_b = data.get('numeratorB', 0)
        denominator_a = data.get('denominatorA', 0)
        denominator_b = data.get('denominatorB', 0)
        
        # Create metadata that matches sample structure
        metadata = []
        
        # Add numerator row
        if numerator_name and (numerator_a != 0 or numerator_b != 0):
            metadata.append({
                "particular": numerator_name,
                "value1": f"{numerator_a:,.2f}" if numerator_a != 0 else "0.00",
                "value2": f"{numerator_b:,.2f}" if numerator_b != 0 else "0.00"
            })
        
        # Add denominator row
        if denominator_name and (denominator_a != 0 or denominator_b != 0):
            metadata.append({
                "particular": denominator_name,
                "value1": f"{denominator_a:,.2f}" if denominator_a != 0 else "0.00",
                "value2": f"{denominator_b:,.2f}" if denominator_b != 0 else "0.00"
            })
        
        # If no specific metric data, return empty metadata
        # This ensures we don't display misleading hardcoded values
        
        return metadata
        
    except Exception as e:
        print(f"Error creating row metadata: {e}")
        return []

def getSummaryStatsCard(params:dict, myValidationOutput: Optional[List[Dict]] = None): #TODO Rename
    # Convert dates without mutating original params (prevents React re-render issues)
    params = _convertDatesToYmdFormat(params)
    if myValidationOutput is None:
        myValidationOutput = getValidationOutput(params)

    NAVValidations = [item for item in myValidationOutput if item["type"].lower() in ['pnl','p&l','non-trading']]
    # Filter out 'Performance Fees' from NAVValidations
    NAVValidations = [item for item in NAVValidations if item.get('subType2') != 'Performance Fees']
    
    # Filter out 'Stale Price' from NAVValidations entries when sourceB is available
    if params.get('query', {}).get('sourceB', ''):
        NAVValidations = [item for item in NAVValidations if item.get('subType2') != 'Stale Price']
    # Calculate exceptions safely for both database and JSON-based approaches
    # FIXED: Only count failed items as exceptions (not passed items)
    # The KPI validation system already correctly categorizes failed vs passed items
    total_exceptions = 0
    for item in NAVValidations:
        data = item.get('data')
        if data is not None and isinstance(data, dict):
            # Only count failed items as exceptions
            failed_items = data.get('failed_items', [])
            exceptions_count = len(failed_items)
            total_exceptions += exceptions_count
        else:
            # Fallback: use absolute value of message
            total_exceptions += abs(item.get('message', 0))
    
    ValidationsStats={
        "total":len(NAVValidations),
        "passed":len([item for item in NAVValidations if item['message']==0]),
        "failed":len([item for item in NAVValidations if item['message']!=0]),
        "Exceptions":total_exceptions,
    }
    ratioValidations=[item for item in myValidationOutput if item["type"] in [ 'Ratio']]
    ratioStats={
        "total":len(ratioValidations),
        "majorDiff":len([item for item in ratioValidations if item['message']!=0]),
        "minorDiff":len([item for item in ratioValidations if item['message']==0]),
    }
    
    validationDetailsAction={
        "type": "navigation",
        "to": "/validus",
        "parameters": [
            {
                "key": "page",
                "value": "nav-validations"
            }
        ]
    }

    validationDetailsActionPassed={
        "type": "navigation",
        "to": "/validus",
        "parameters": [
            {
                "key": "page",
                "value": "nav-validations"
            },
            {
                "key": "filter",
                "value": "status-passed"
            }
        ]
    }

    validationDetailsActionFailed={
        "type": "navigation",
        "to": "/validus",
        "parameters": [
            {
                "key": "page",
                "value": "nav-validations"
            },
            {
                "key": "filter",
                "value": "status-failed"
            },
        ]
    }

    validationDetailsActionExceptions={
        "type": "navigation",
        "to": "/validus",
        "parameters": [
            {
                "key": "page",
                "value": "nav-validations"
            },
            {
              "key": "groupDefaultExpand",
              "value": -1,
            },
        ]
    }
    
    
    ratioDetailsAction={
        "type": "navigation",
        "to": "/validus",
        "parameters": [
            {
                "key": "page",
                "value": "ratio-validations"
            }
        ]
    }
    
    
    ratioDetailsActionMajor={
        "type": "navigation",
        "to": "/validus",
        "parameters": [
            {
                "key": "page",
                "value": "ratio-validations"
            },
            {
                "key": "filter",
                "value": "deviation-major"
            }
        ]
    }
    
    ratioDetailsActionMinor={
        "type": "navigation",
        "to": "/validus",
        "parameters": [
            {
                "key": "page",
                "value": "ratio-validations"
            },
            {
                "key": "filter",
                "value": "deviation-minor"
            }
        ]
    }
    
    myItemsStats=[ #TODO better way to do this?
        {
            "label": "NAV Validations",
            "breakdown": [
                {
                    "label": "Validations",
                    "value": str(ValidationsStats['total']),
                    "action": validationDetailsAction,
                },
                {
                    "label": "Passed",
                    "value": str(ValidationsStats['passed']),
                    "action": validationDetailsActionPassed,
                },
                {
                    "label": "Failed",
                    "value": str(ValidationsStats['failed']),
                    "action": validationDetailsActionFailed,
                },
                {
                    "label": "Exceptions",
                    "value": str(ValidationsStats['Exceptions']),
                    "action": validationDetailsActionExceptions,
                }
            ]
        },
        {
            "label": "Ratio Health Check",
            "breakdown": [
                {
                    "label": "Total ratios",
                    "value": str(ratioStats['total']),
                    "action": ratioDetailsAction
                },
                {
                    "label": "Major deviation",
                    "value": str(ratioStats['majorDiff']),
                    "action": ratioDetailsActionMajor
                },
                {
                    "label": "Minor deviation",
                    "value": str(ratioStats['minorDiff']),
                    "action": ratioDetailsActionMinor
                }
            ]
        }
    ]

    return getGroupedStatsCardRender(myItemsStats)

def getFileValidations(params:dict, myValidationOutput: Optional[List[Dict]] = None):
    """
    Get file validations dynamically from database instead of hardcoded values.
    Returns unique file names from NavPackVersion table sorted by upload_time in reverse order.
    """
    # Get validation output from database
    if myValidationOutput is None:
        myValidationOutput = getValidationOutput(params)

    filesValidation = [item for item in myValidationOutput if item["type"] == 'file_revieved']

    # Get parameters for period and source information
    myQuery = params.get('query', params)
    fund_name = myQuery.get('fundName', 'Bluefield')
    fund_id = getFundId(fund_name)
    source_a = myQuery.get('sourceA', 'Bluefield')
    source_b = myQuery.get('sourceB', None)
    date_a = myQuery.get('dateA', '09-08-2025')
    date_b = myQuery.get('dateB', None)
    
    
    def format_date_for_filename(date_str):
        """Convert date from MM-DD-YYYY to MM-DD-YYYY format for filename"""
        if not date_str:
            return '09-08-2025'
        
        # Handle different date formats
        if '/' in date_str:
            # Convert MM/DD/YYYY to MM-DD-YYYY
            return date_str.replace('/', '-')
        elif len(date_str) == 10 and date_str.count('-') == 2:
            # Check if it's YYYY-MM-DD format and convert to MM-DD-YYYY
            parts = date_str.split('-')
            if len(parts[0]) == 4:  # Year first
                return f"{parts[1]}-{parts[2]}-{parts[0]}"
            else:  # Already MM-DD-YYYY
                return date_str
        else:
            return date_str
    
    # Format dates for filename
    formatted_date_a = format_date_for_filename(date_a)
    formatted_date_b = format_date_for_filename(date_b) if date_b else None
    
    rows = []
    
    try:
        # Get database manager from validation service
        db_manager = db_validation_service.db_manager
        if not db_manager:
            # Fallback to hardcoded values if database is not available
            return None
        
        # Create database session with nexbridge schema
        session = db_manager.get_session_with_schema('nexbridge')
        
        try:
            # Query to get unique file names from NavPackVersion table
            # Join with NavPack and Source to get source information
            # Order by uploaded_on in reverse order (most recent first)
            query_result = session.query(
                NavPackVersion.navpack_version_id,
                NavPackVersion.file_name,
                NavPackVersion.uploaded_on,
                Source.name.label('source_name'),
                NavPack.file_date,
                NavPack.fund_id
            ).join(
                NavPack, NavPackVersion.navpack_id == NavPack.navpack_id
            ).join(
                Source, NavPack.source_id == Source.id
            ).order_by(
                NavPackVersion.uploaded_on.desc()
            ).where(
                NavPack.fund_id == fund_id
            )

            # Add file_date filter for date_a_obj and/or date_b_obj (use .in_ if both valid)
            file_dates = []
            if date_a:
                file_dates.append(datetime.strptime(date_a, '%Y-%m-%d').date())
            if date_b and (date_b != date_a):
                file_dates.append(datetime.strptime(date_b, '%Y-%m-%d').date())

            if file_dates:
                # If one, this works as expected; if two unique, does "OR" logic
                query_result = query_result.filter(NavPack.file_date.in_(file_dates))
            
            query_result = query_result.all()

            # Get sets of navpack_version_ids that exist in each table
            trial_balance_ids = set(row[0] for row in session.query(TrialBalance.navpack_version_id).all())
            portfolio_valuation_ids = set(row[0] for row in session.query(PortfolioValuation.navpack_version_id).all())
            dividend_ids = set(row[0] for row in session.query(Dividend.navpack_version_id).all())

            # Process query results and create unique file entries
            seen_files = set()
            for row in query_result:
                navpack_version_id = row.navpack_version_id
                source_name = row.source_name
                uploaded_on = row.uploaded_on
                file_date = row.file_date
                
                # Format uploaded date for display
                date_received = uploaded_on.strftime('%m-%d-%Y') if uploaded_on else 'Unknown'
                
                # Format file_date for display in file names
                file_date_str = file_date.strftime('%m-%d-%Y') if file_date else 'unknown'
                
                # Check if navpack_version_id exists in trial balance table
                if navpack_version_id in trial_balance_ids:
                    file_key = f"trial_balance_{file_date_str}_{source_name}"
                    if file_key not in seen_files:
                        seen_files.add(file_key)
                        rows.append({
                            'fileName': f"Trial Balance Report - {file_date_str} - {source_name}",
                            'fileType': file_key,
                            'source': source_name,
                            'dateReceived': date_received,
                            'ingested': "Completed",
                            'fundName': fund_name
                        })
                        # if fund_id == 1:
                        #     rows.append({
                        #         'fileName': f"Detailed General Ledger Report - {file_date_str} - {source_name}",
                        #         'fileType': file_key,
                        #         'source': source_name,
                        #         'dateReceived': date_received,
                        #         'ingested': "Completed",
                        #         'fundName': fund_name
                        #     })
                
                # Check if navpack_version_id exists in portfolio valuation table
                if navpack_version_id in portfolio_valuation_ids:
                    file_key = f"portfolio_valuation_{file_date_str}_{source_name}"
                    if file_key not in seen_files:
                        seen_files.add(file_key)
                        rows.append({
                            'fileName': f"Portfolio Valuation Report - {file_date_str} - {source_name}",
                            'fileType': file_key,
                            'source': source_name,
                            'dateReceived': date_received,
                            'ingested': "Completed",
                            'fundName': fund_name
                        })
                
                # Check if navpack_version_id exists in dividend table
                if navpack_version_id in dividend_ids:
                    file_key = f"dividend_{file_date_str}_{source_name}"
                    if file_key not in seen_files:
                        seen_files.add(file_key)
                        rows.append({
                            'fileName': f"Dividend Report - {file_date_str} - {source_name}",
                            'fileType': file_key,
                            'source': source_name,
                            'dateReceived': date_received,
                            'ingested': "Completed",
                            'fundName': fund_name
                        })
                

        finally:
            session.close()
            
    except Exception as e:
        print(f"Error querying file validations from database: {e}")
        return None


    columnsConfig = {
        "fileType": {
            "name": "File Name"
        },
        "source": {
            "name": "Source"
        },
        "dateReceived": {
            "name": "Date Received"
        },
        "ingested": {
            "customCellRenderer": "labelToNumberAggregator",
            "name": "Ingestion"
        },

    }
    colsToShow = ["fileType", "source", "dateReceived", "ingested"]
    extraProperties = {
        "rowClickEnabled": True,
        "rowClickAction": {
        "type": "navigation",
        "to": "/validus",
        "parameters": [
            {
            "key": "page",
            "value": "report-details"
            },
            {
            "key": "fileName",
            "value": "",
            "dynamicValue": {
                "enabled": True,
                "id": "fileType"
            }
            },
            {
            "key": "fundName",
            "value": "",
            "dynamicValue": {
                "enabled": True,
                "id": "fundName"
            }
            },
            {
            "key": "name",
            "value": "",
            "dynamicValue": {
                "enabled": True,
                "id": "fileName"
            }
            },
            {
            "key":"page_size",
            "value":50,
            },
            {
            "key":"page",
            "value":"1"
            },
            {
            "key":"totalpages",
            "value":"0",
            "dynamicValue": {
                "enabled": True,
                "id": "totalpages"
            }
            },
        ]
        }
    }
    
    return getSimpleTableRenderFromRows(rows, columnsConfig, colsToShow, extraProperties=extraProperties)


def getReportViewData(params: dict):
    """
    Get report view data dynamically based on fileName parameter.
    Returns structured data for different report types (Trial Balance, Portfolio Valuation, Dividend).
    
    API Structure: GET /FE?_funcName=reportViewData&_type=moduleRender&_subType=singleFundCompare&fundName=NexBridge&fileName=trial_balance_01-31-2024_Bluefield
    """
    # Get parameters
    myQuery = params.get('query', params)
    fund_name = myQuery.get('fundName', 'NexBridge')
    fund_id = getFundId(fund_name)
    fileName = myQuery.get('fileName', '')
    report_type = fileName.rsplit('_',maxsplit=2)[0]
    report_date = fileName.rsplit('_',maxsplit=2)[1]
    source_name = fileName.rsplit('_',maxsplit=2)[2]
    page_size = myQuery.get('page_size', 50)
    current_page = myQuery.get('page', 1)
    total_pages = myQuery.get('totalpages', 0)
    # Convert date to proper format
    try:
        from datetime import datetime
        date_obj = datetime.strptime(report_date, '%m-%d-%Y').date()
    except ValueError:
        return {
            "error": "Invalid date format",
            "colsToShow": [],
            "columnConfig": {},
            "rowData": [],
            "pagination": {"current_page": 1, "page_size": 50, "total_pages": 0}
        }
    
    try:
        # Get database manager
        db_manager = db_validation_service.db_manager
        if not db_manager:
            return {
                "error": "Database not available",
                "colsToShow": [],
                "columnConfig": {},
                "rowData": [],
                "pagination": {"current_page": 1, "page_size": 50, "total_pages": 0}
            }
        
        session = db_manager.get_session_with_schema('nexbridge')
        
        try:
            # Get latest navpack_version_id for the specific fund, date, and source
            navpack_version = session.query(NavPackVersion).join(
                NavPack, NavPackVersion.navpack_id == NavPack.navpack_id
            ).join(
                Source, NavPack.source_id == Source.id
            ).filter(
                NavPack.fund_id == fund_id,
                NavPack.file_date == date_obj,
                Source.name == source_name
            ).order_by(NavPackVersion.version.desc()).first()
            
            if not navpack_version:
                return {
                    "error": "No data found for the specified parameters",
                    "colsToShow": [],
                    "columnConfig": {},
                    "rowData": [],
                    "pagination": {"current_page": 1, "page_size": 50, "total_pages": 0}
                }
            
            navpack_version_id = navpack_version.navpack_version_id
            
            # Get data based on report type
            if report_type == 'trial_balance':
                return _getTrialBalanceReportData(session, navpack_version_id, page_size, current_page, total_pages)
            elif report_type == 'portfolio_valuation':
                return _getPortfolioValuationReportData(session, navpack_version_id, page_size, current_page, total_pages)
            elif report_type == 'dividend':
                return _getDividendReportData(session, navpack_version_id, page_size, current_page, total_pages)
            # elif report_type == 'detailed_general_ledger':
            #     return _getDetailedGeneralLedgerReportData(session, navpack_version_id)
            else:
                return {
                    "error": f"Unknown report type: {report_type}",
                    "colsToShow": [],
                    "columnConfig": {},
                    "rowData": [],
                    "pagination": {"current_page": 1, "page_size": 50, "total_pages": 0}
                }
                
        finally:
            session.close()
            
    except Exception as e:
        print(f"Error getting report view data: {e}")
        return {
            "error": f"Database error: {str(e)}",
            "colsToShow": [],
            "columnConfig": {},
            "rowData": [],
            "pagination": {"current_page": 1, "page_size": 50, "total_pages": 0}
        }


def _format_numeric_value(value):
    """
    Safely format a value as a numeric string with comma separators and 2 decimal places.
    Handles both numeric and string inputs.
    
    Args:
        value: Value to format (can be numeric or string)
    
    Returns:
        Formatted string or "-" if value cannot be converted
    """
    if value is None or value == "-":
        return "-"
    
    try:
        numeric_value = float(value)
        return f"{numeric_value:,.2f}"
    except (ValueError, TypeError):
        return "-"

def _getTrialBalanceReportData(session, navpack_version_id, page_size, current_page, total_pages):
    """Get trial balance report data"""
    try:
        # Get trial balance data
        trial_balance_data = session.query(TrialBalance).filter(
            TrialBalance.navpack_version_id == navpack_version_id
        ).order_by(TrialBalance.type.asc()).all()
        
        # Define columns and configuration
        colsToShow = ["row_no", "type", "category", "accountingHead", "financialAccount", "description", "beginningBalance", "activity", "endingBalance"]
        columnConfig = {
            "row_no": {
                "name": "Row No.",
                "filter": "true",
                "maxWidth": 100
            },
            "type": {
                "name": "Type",
                "filter": "true",
                "maxWidth": 100
            },
            "category": {
                "name": "Category",
                "filter": "true",
            },
            "accountingHead": {
                "name": "Accounting Head",
                "filter": "true"
            },
            "financialAccount": {
                "name": "Financial Account",
                "filter": "true",
            },
            "description": {
                "name": "Description",
                "filter": "true",
                "maxWidth": 130
            },
            "beginningBalance": {
                "name": "Beginning Balance",
                "filter": "true",
                "cellStyle": {"textAlign": "right", "paddingRight": "8px"},
                "valueFormatter": [
                    "function", 
                    "(params) => { if (params.value === null || params.value === undefined) return ''; const num = parseFloat(params.value); if (isNaN(num)) return params.value; return num.toFixed(2).replace(/\\B(?=(\\d{3})+(?!\\d))/g, ','); }"
                ]
            },
            "activity": {
                "name": "Activity",
                "filter": "true",
                "cellStyle": {"textAlign": "right", "paddingRight": "8px"},
                "valueFormatter": [
                    "function", 
                    "(params) => { if (params.value === null || params.value === undefined) return ''; const num = parseFloat(params.value); if (isNaN(num)) return params.value; return num.toFixed(2).replace(/\\B(?=(\\d{3})+(?!\\d))/g, ','); }"
                ]
            },
            "endingBalance": {
                "name": "Ending Balance",
                "filter": "true",
                "cellStyle": {"textAlign": "right", "paddingRight": "8px"},
                "valueFormatter": [
                    "function", 
                    "(params) => { if (params.value === null || params.value === undefined) return ''; const num = parseFloat(params.value); if (isNaN(num)) return params.value; return num.toFixed(2).replace(/\\B(?=(\\d{3})+(?!\\d))/g, ','); }"
                ]
            }
        }
        
        # Convert data to row format
        rowData = []
        row_no = 0
        for record in trial_balance_data:
            row_no += 1
            rowData.append({
                "row_no": row_no,
                "type": record.type or "-",
                "category": record.category or "-",
                "accountingHead": record.accounting_head or "-",
                "financialAccount": record.financial_account or "-",
                "description": json.loads(record.extra_data).get("description", "-") if record.extra_data else "-",
                "beginningBalance": _format_numeric_value(json.loads(record.extra_data).get("beginning_balance", "-")) if record.extra_data else "-",
                "activity": _format_numeric_value(json.loads(record.extra_data).get("activity", "-")) if record.extra_data else "-",
                "endingBalance": _format_numeric_value(record.ending_balance) if record.ending_balance else "-"
            })
        
        # Calculate pagination
        total_records = len(rowData)
        page_size = int(page_size)
        total_pages = (total_records + page_size - 1) // page_size
        current_page = int(current_page)
        rowData = rowData[(current_page-1)*page_size:(current_page)*page_size]

        return {
            "colsToShow": colsToShow,
            "columnConfig": columnConfig,
            "rowData": rowData,
            "pagination": {
                "current_page": current_page,
                "page_size": page_size,
                "total_pages": total_pages
            }
        }
        
    except Exception as e:
        print(f"Error getting trial balance data: {e}")
        return {
            "error": f"Error getting trial balance data: {str(e)}",
            "colsToShow": [],
            "columnConfig": {},
            "rowData": [],
            "pagination": {"current_page": 1, "page_size": 50, "total_pages": 0}
        }


def _getPortfolioValuationReportData(session, navpack_version_id, page_size, current_page, total_pages):
    """Get portfolio valuation report data"""
    try:
        # Get portfolio valuation data
        portfolio_data = session.query(PortfolioValuation).filter(
            PortfolioValuation.navpack_version_id == navpack_version_id
        ).all()
        
        # Define columns and configuration
        colsToShow = ["row_no", "invType", "invId", "description", "endQty", "endLocalMarketPrice", "endLocalMv", "endBookMv"]
        columnConfig = {
            "row_no": {
                "name": "Row No.",
                "filter": "true",
                "maxWidth": 100
            },
            "invType": {
                "name": "Investment Type",
                "filter": "true",
            },
            "invId": {
                "name": "Investment ID",
                "filter": "true"
            },
            "description": {
                "name": "Description",
                "filter": "true"
            },
            "endQty": {
                "name": "End Quantity",
                "filter": "true",
                "cellStyle": {"textAlign": "right", "paddingRight": "8px"},
                "valueFormatter": [
                    "function", 
                    "(params) => { if (params.value === null || params.value === undefined) return ''; const num = parseFloat(params.value); if (isNaN(num)) return params.value; return num.toFixed(2).replace(/\\B(?=(\\d{3})+(?!\\d))/g, ','); }"
                ]
            },
            "endLocalMarketPrice": {
                "name": "End Local Market Price",
                "filter": "true",
                "cellStyle": {"textAlign": "right", "paddingRight": "8px"},
                # value formatting to decimal places
                "valueFormatter": [
                    "function", 
                    "(params) => { if (params.value === null || params.value === undefined) return ''; const num = parseFloat(params.value); if (isNaN(num)) return params.value; return num.toFixed(2).replace(/\\B(?=(\\d{3})+(?!\\d))/g, ','); }"
                ]
            },
            "endLocalMv": {
                "name": "End Local Market Value",
                "filter": "true",
                "cellStyle": {"textAlign": "right", "paddingRight": "8px"},
                "valueFormatter": [
                    "function", 
                    "(params) => { if (params.value === null || params.value === undefined) return ''; const num = parseFloat(params.value); if (isNaN(num)) return params.value; return num.toFixed(2).replace(/\\B(?=(\\d{3})+(?!\\d))/g, ','); }"
                ]
            },
            "endBookMv": {
                "name": "End Book Market Value",
                "filter": "true",
                "cellStyle": {"textAlign": "right", "paddingRight": "8px"},
                "valueFormatter": [
                    "function", 
                    "(params) => { if (params.value === null || params.value === undefined) return ''; const num = parseFloat(params.value); if (isNaN(num)) return params.value; return num.toFixed(2).replace(/\\B(?=(\\d{3})+(?!\\d))/g, ','); }"
                ]
            },

        }
        
        # Convert data to row format
        rowData = []
        row_no = 0
        for record in portfolio_data:
            row_no += 1
            rowData.append({
                "row_no": row_no,
                "invType": record.inv_type or "-",
                "invId": record.inv_id or "-",
                "description": json.loads(record.extra_data).get("description", "-") if record.extra_data else "-",
                "endQty": _format_numeric_value(record.end_qty) if record.end_qty else "-",
                "endLocalMarketPrice": _format_numeric_value(record.end_local_market_price) if record.end_local_market_price else "-",
                "endLocalMv": _format_numeric_value(record.end_local_mv) if record.end_local_mv else "-",
                "endBookMv": _format_numeric_value(record.end_book_mv) if record.end_book_mv else "-",
            })
        
        # Calculate pagination
        total_records = len(rowData)
        page_size = int(page_size)
        total_pages = (total_records + page_size - 1) // page_size
        current_page = int(current_page)
        rowData = rowData[(current_page-1)*page_size:(current_page)*page_size]

        return {
            "colsToShow": colsToShow,
            "columnConfig": columnConfig,
            "rowData": rowData,
            "pagination": {
                "current_page": current_page,
                "page_size": page_size,
                "total_pages": total_pages
            }
        }
        
    except Exception as e:
        print(f"Error getting portfolio valuation data: {e}")
        return {
            "error": f"Error getting portfolio valuation data: {str(e)}",
            "colsToShow": [],
            "columnConfig": {},
            "rowData": [],
            "pagination": {"current_page": 1, "page_size": 50, "total_pages": 0}
        }


def _getDividendReportData(session, navpack_version_id, page_size, current_page, total_pages):
    """Get dividend report data"""
    try:
        # Get dividend data
        dividend_data = session.query(Dividend).filter(
            Dividend.navpack_version_id == navpack_version_id
        ).all()
        
        # Define columns and configuration
        colsToShow = ["row_no", "securityId", "securityName", "amount"]
        columnConfig = {
            "row_no": {
                "name": "Row No.",
                "filter": "true"
            },
            "securityId": {
                "name": "Security ID",
                "filter": "true",
                "maxWidth": 150
            },
            "securityName": {
                "name": "Security Name",
                "filter": "true"
            },
            "amount": {
                "name": "Amount",
                "filter": "true",
                "cellStyle": {"textAlign": "right", "paddingRight": "8px"},
                "valueFormatter": [
                    "function", 
                    "(params) => { if (params.value === null || params.value === undefined) return ''; const num = parseFloat(params.value); if (isNaN(num)) return params.value; return num.toFixed(2).replace(/\\B(?=(\\d{3})+(?!\\d))/g, ','); }"
                ]
            }
        }
        
        # Convert data to row format
        rowData = []
        row_no = 0
        for record in dividend_data:
            row_no += 1
            rowData.append({
                "row_no": row_no,
                "securityId": record.security_id or "-",
                "securityName": record.security_name or "-",
                "amount": _format_numeric_value(record.amount) if record.amount else "-",
            })
        
        # Calculate pagination
        total_records = len(rowData)
        page_size = int(page_size)
        total_pages = (total_records + page_size - 1) // page_size
        current_page = int(current_page)
        rowData = rowData[(current_page-1)*page_size:(current_page)*page_size]

        return {
            "colsToShow": colsToShow,
            "columnConfig": columnConfig,
            "rowData": rowData,
            "pagination": {
                "current_page": current_page,
                "page_size": page_size,
                "total_pages": total_pages
            }
        }
        
    except Exception as e:
        print(f"Error getting dividend data: {e}")
        return {
            "error": f"Error getting dividend data: {str(e)}",
            "colsToShow": [],
            "columnConfig": {},
            "rowData": [],
            "pagination": {"current_page": 1, "page_size": 50, "total_pages": 0}
        }


def _getDetailedGeneralLedgerReportData(session, navpack_version_id, page_size, current_page, total_pages):
    """Get detailed general ledger report data (same as trial balance for now)"""
    # For now, detailed general ledger uses the same data as trial balance
    # This can be extended later if there's a separate table
    return _getTrialBalanceReportData(session, navpack_version_id, page_size, current_page, total_pages)


def getConfigFromQuery(aClient:str,params:dict):
    myQuery=params.get('query',{})
    if 'fundName' not in myQuery:
        raise HTTPException(status_code=400, detail="fundName is required")
    username=params.get('username',None)
    fundUniqueId=getFundUniqueId(myQuery['fundName'])
    if not userHasFundReadPerm(username,fundUniqueId):
        raise HTTPException(status_code=403, detail=f"User does not have permission to access {fundUniqueId}")
    
    if 'sourceA' not in myQuery:
        raise HTTPException(status_code=400, detail="sourceA is required")
    
    if 'dateA' not in myQuery:
        raise HTTPException(status_code=400, detail="dateA is required")    
    if 'dateB' not in myQuery:
        if 'sourceB' not in myQuery:
            raise HTTPException(status_code=400, detail="dateB or sourceB is required")
        elif myQuery['sourceA'] == myQuery['sourceB']:
            raise HTTPException(status_code=400, detail="dateB is required")
        else:
            myQuery['dateB']=myQuery['dateA']
    elif 'sourceB' not in myQuery:
        if myQuery['dateA'] == myQuery['dateB']:
            raise HTTPException(status_code=400, detail="sourceB is required for same date")
        myQuery['sourceB']=myQuery['sourceA']

    myConfig={
            'client':aClient,
            'fundUniqueId':fundUniqueId,
            'sourceA':{
                'source':myQuery['sourceA'].replace(" ", "_"), #bug frontend
                'processDate':myQuery['dateA']
            },
            'sourceB':{
                'source':myQuery['sourceB'].replace(" ", "_"), #bug frontend
                'processDate':myQuery['dateB']
            },
            'runDate':myQuery['dateB']
        }
    
    return myConfig

def _getClient():
    return 'validusDemo' # check for perms here?

def _getStorage():
    myStorageConfig={
        'defaultFileStorage':'onPrem',
    }
    client=_getClient() 
    return STORAGE(client,myStorageConfig)

def getDummyCHECKPOINTS(params:dict):
    myRender= {
        "title": "CHECKPOINTS",
        "data": [
            {
            "title": {
                "label": "Pricing Checks",
                "status": "Completed",
                "statusColor": "#22C55E"
            },
            "content": [
                {
                "label": {
                    "text": "Null / Missing Price Check",
                    "icon": "badgeCheck",
                    "iconColor": "text-green-600"
                },
                "info": {
                    "text": "Ensures every asset has a price on the given valuation date",
                    "icon": "helpCircle",
                    "iconColor": "text-neutral-800"
                },
                "status": "Completed"
                },
                {
                "label": {
                    "text": "Price Movement Threshold",
                    "icon": "badgeCheck",
                    "iconColor": "text-green-600"
                },
                "info": {
                    "text": "Flags prices that change more than 10% or 20% Month-over-Month (MoM) or Quarter-over-Quarter (QoQ)",
                    "icon": "helpCircle",
                    "iconColor": "text-neutral-800"
                },
                "status": "Completed",
                },
                {
                "label": {
                    "text": "Major FX Changes",
                    "icon": "badgeCheck",
                    "iconColor": "text-green-600"
                },
                "info": {
                    "text": "Flags large movements in foreign exchange rates to ensure they are accurately reflected in P&L, valuation, and exposure calculations",
                    "icon": "helpCircle",
                    "iconColor": "text-neutral-800"
                },
                "status": "Completed"
                },
                {
                "label": {
                    "text": "Round Lot/Price Format Validation",
                    "icon": "badgeCheck",
                    "iconColor": "text-green-600"
                },
                "info": {
                    "text": "Ensures prices conform to expected decimal places or tick sizes, and are rounded correctly based on expected decimal places or minimums",
                    "icon": "helpCircle",
                    "iconColor": "text-neutral-800"
                },
                "status": "Completed"

                },
                {
                "label": {
                    "text": "Currency Consistency Check",
                    "icon": "badgeCheck",
                    "iconColor": "text-green-600"
                },
                "info": {
                    "text": "Validates that the pricing currency matches the expected currency for the asset",
                    "icon": "helpCircle",
                    "iconColor": "text-neutral-800"
                },
                "status": "Completed"
                }
            ]
            },
            {
            "title": {
                "label": "Position Checks",
                "status": "Completed",
                "statusColor": "#22C55E"
            },
            "content": [
                {
                "label": {
                    "text": "Missing Positions Check",
                    "icon": "badgeCheck",
                    "iconColor": "text-green-600"
                },
                "info": {
                    "text": "Verifies that all expected positions are included",
                    "icon": "helpCircle",
                    "iconColor": "text-neutral-800"
                },
                "status": "Completed"
                },
                {
                "label": {
                    "text": "Zero Quantity Check",
                    "icon": "badgeCheck",
                    "iconColor": "text-green-600"
                },
                "info": {
                    "text": "Flags positions with a quantity of 0 that still appear in the file",
                    "icon": "helpCircle",
                    "iconColor": "text-neutral-800"
                },
                "status": "Completed"
                },
                {
                "label": {
                    "text": "Negative Quantity Check",
                    "icon": "badgeCheck",
                    "iconColor": "text-green-600"
                },
                "info": {
                    "text": "Identifies shorts or unintended negative positions",
                    "icon": "helpCircle",
                    "iconColor": "text-neutral-800"
                },
                "status": "Completed"
                },
                {
                "label": {
                    "text": "Market Value Calculation Check",
                    "icon": "badgeCheck",
                    "iconColor": "text-green-600"
                },
                "info": {
                    "text": "Ensures that quantity multiplied by price equals market value",
                    "icon": "helpCircle",
                    "iconColor": "text-neutral-800"
                },
                "status": "Completed"
                },
                {
                "label": {
                    "text": "Position Movement from Trades",
                    "icon": "badgeCheck",
                    "iconColor": "text-green-600"
                },
                "info": {
                    "text": "Ensures that large changes in position size between periods (T-1 to T) are accurately driven by trades and not errors in booking, pricing, or settlement",
                    "icon": "helpCircle",
                    "iconColor": "text-neutral-800"
                },
                "status": "Completed"
                }
            ]
            },
            {
            "title": {
                "label": "Trial Balance Checks",
                "status": "Completed",
                "statusColor": "#22C55E"
            },
            "content": [
                {
                "label": {
                    "text": "Debits = Credits Check",
                    "icon": "badgeCheck",
                    "iconColor": "text-green-600"
                },
                "info": {
                    "text": "Ensures the total debits and credits for the period are equal",
                    "icon": "helpCircle",
                    "iconColor": "text-neutral-800"
                },
                "status": "Completed"
                },
                {
                "label": {
                    "text": "Fund Balance Rollforward Check",
                    "icon": "badgeCheck",
                    "iconColor": "text-green-600"
                },
                "info": {
                    "text": "Validates that opening balance plus activity equals closing balance",
                    "icon": "helpCircle",
                    "iconColor": "text-neutral-800"
                },
                "status": "Completed"
                }
            ]
            },
            {
            "title": {
                "label": "Trading I&E (Income & Expense)",
                "status": "Completed",
                "statusColor": "#22C55E"
            },
            "content": [
                {
                "label": {
                    "text": "Major Dividend",
                    "icon": "badgeCheck",
                    "iconColor": "text-green-600"
                },
                "info": {
                    "text": "Confirms large dividend income matches holdings, ex-date, pay date, and rate",
                    "icon": "helpCircle",
                    "iconColor": "text-neutral-800"
                },
                "status": "Completed"
                },
                {
                "label": {
                    "text": "Swap Financing",
                    "icon": "badgeCheck",
                    "iconColor": "text-green-600"
                },
                "info": {
                    "text": "Reviews cash flows from swap contracts (e.g., fixed/floating leg differences)",
                    "icon": "helpCircle",
                    "iconColor": "text-neutral-800"
                },
                "status": "Completed"
                },
                {
                "label": {
                    "text": "Interest Accrual Validation",
                    "icon": "badgeCheck",
                    "iconColor": "text-green-600"
                },
                "info": {
                    "text": "Ensures bond/loan interest accruals align with coupon rate, day count, and notional",
                    "icon": "helpCircle",
                    "iconColor": "text-neutral-800"
                },
                "status": "Completed"
                },
                {
                "label": {
                    "text": "Threshold Review",
                    "icon": "badgeCheck",
                    "iconColor": "text-green-600"
                },
                "info": {
                    "text": "Flags income/expense entries above a specified threshold (e.g., >$100k or >0.5% NAV)",
                    "icon": "helpCircle",
                    "iconColor": "text-neutral-800"
                },
                "status": "Completed"
                }
            ]
            },
            {
            "title": {
                "label": "Market Value",
                "status": "Completed",
                "statusColor": "#22C55E"
            },
            "content": [
                {
                "label": {
                    "text": "Threshold Review",
                    "icon": "badgeCheck",
                    "iconColor": "text-green-600"
                },
                "info": {
                    "text": "Flags income/expense entries above a specified threshold (e.g., >$100k or >0.5% NAV)",
                    "icon": "helpCircle",
                    "iconColor": "text-neutral-800"
                },
                "status": "Completed"
                }
            ]
            },
            {
            "title": {
                "label": "Non Trading Items Check",
                "status": "Completed",
                "statusColor": "#22C55E"
            },
            "content": [
                {
                "label": {
                    "text": "Threshold Review",
                    "icon": "badgeCheck",
                    "iconColor": "text-green-600"
                },
                "info": {
                    "text": "Flags income/expense entries above a specified threshold (e.g., >$100k or >0.5% NAV)",
                    "icon": "helpCircle",
                    "iconColor": "text-neutral-800"
                },
                "status": "Completed"
                }
            ]
            }
        ],
        "cssProperties": {
            "padding": "24px",
            "borderRadius": "24px",
            "backgroundColor": "white"
        }
    }

    return myRender

def getDummyNAVValidationDetailsTabs(params:dict):
    myRender={
        "tabs": [
            {
            "tabInternalKey": "pricing",
            "nameOfTab": "Pricing",
            "CountKeyInAPI": ["stalePriceSec_count"],
            "moduleName": "_validusSF_nestedTable",
            "overrridenParam": {
                "_funcName": "pricingValidationsLevel2Table"
            },
            "subFilterGroups": [
                {
                "key": "group1",
                "filters": [
                    {
                    "tabInternalKey": "allPositions",
                    "nameOfTab": "All",
                    "CountKeyInAPI": ["_otherData","counts","all"],
                    "showCount": True
                    },
                    {
                    "tabInternalKey": "stalePriceSec",
                    "nameOfTab": "Stale Price Sec",
                    "CountKeyInAPI": ["stalePriceSec_count"],
                    "showCount": True
                    },
                    {
                    "tabInternalKey": "missingPriceSec",
                    "nameOfTab": "Missing Price Sec",
                    "CountKeyInAPI": ["missing_price_sec_count"],
                    "showCount": True
                    },
                    {
                    "tabInternalKey": "majorPriceChanges",
                    "nameOfTab": "Major price changes",
                    "CountKeyInAPI": ["Major_price_changes_count"],
                    "showCount": True
                    },
                    {
                    "tabInternalKey": "majorFXChanges",
                    "nameOfTab": "Major FX changes ",
                    "CountKeyInAPI": ["major_FX_changes_count"],
                    "showCount": True
                    }
                ]
                }
            ],
            "isActive": True,
            "showCount": True
            },
            {
            "tabInternalKey": "positions",
            "nameOfTab": "Positions",
            "CountKeyInAPI": ["positions_count"],
            "overrridenParam": {
                "status": "DELIVERED"
            },
            "moduleName": "ValidationDetailsTable",
            "subFilterGroups": [
                {
                "key": "group1",
                "filters": [
                    {
                    "tabInternalKey": "byTrade",
                    "nameOfTab": "By Trade - Qty",
                    "CountKeyInAPI": ["by_trade_count"],
                    "moduleName": "ValidationDetailsTable",
                    "isActive": True,
                    "showCount": True
                    },
                    {
                    "tabInternalKey": "by_corp_actions",
                    "nameOfTab": "By Corp Actions",
                    "CountKeyInAPI": ["stalePriceSec_count"],
                    "moduleName": "ValidationDetailsTable",
                    "isActive": True,
                    "showCount": True
                    }
                ]
                }
            ],
            "isActive": True,
            "showCount": True
            },
            {
            "tabInternalKey": "tradingI&E",
            "nameOfTab": "Trading I&E",
            "CountKeyInAPI": ["tradingI&E_count"],
            "overrridenParam": {
                "status": "TradingI&E"
            },
            "moduleName": "ValidationDetailsTable",
            "subFilterGroups": [
                {
                "key": "group1",
                "filters": [
                    {
                    "tabInternalKey": "majorDividends",
                    "nameOfTab": "Major Dividends",
                    "CountKeyInAPI": ["major_dividends_count"],
                    "moduleName": "ValidationDetailsTable",
                    "isActive": True,
                    "showCount": True
                    },
                    {
                    "tabInternalKey": "materialSwapFinancing",
                    "nameOfTab": "Material Swap Financing",
                    "CountKeyInAPI": ["material_swap_financing_count"],
                    "moduleName": "ValidationDetailsTable",
                    "isActive": True,
                    "showCount": True
                    },
                    {
                    "tabInternalKey": "materialInterestAccruals",
                    "nameOfTab": "Material Interest Accruals",
                    "CountKeyInAPI": ["material_interest_accruals_count"],
                    "moduleName": "ValidationDetailsTable",
                    "isActive": True,
                    "showCount": True
                    }
                ]
                }
            ],
            "isActive": True,
            "showCount": True
            },

            {
            "tabInternalKey": "marketValue",
            "nameOfTab": "Market Value",
            "CountKeyInAPI": ["marketValue_count"],
            "overrridenParam": {
                "status": "MARKETVAlUE"
            },
            "moduleName": "ValidationDetailsTable",

            "subFilterGroups": [
                {
                "key": "group1",
                "filters": [
                    {
                    "tabInternalKey": "majorMVChange",
                    "nameOfTab": "Major MV change",
                    "CountKeyInAPI": ["major_MV_change_count"],
                    "moduleName": "ValidationDetailsTable",
                    "isActive": True,
                    "showCount": True
                    }
                ]
                }
            ],
            "isActive": False,
            "showCount": True
            }
        ],
        "cssProperties": {
            "height": "98%"
        }
    }

    return myRender


"""
Execute Validations - Batch execution of all active validations
Gets validation configurations from database and executes them using formula_validator.py
"""

import sys
import os
from typing import Dict, Any, List, Optional
from datetime import datetime
import pandas as pd
import numpy as np
import json

# Add the project root to the path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from formula_validator import validateFormulaWithDatabase
from server.APIServerUtils.db_validation_service import DatabaseValidationService
from database_models import get_database_manager, DataModelDetails, Client
from sqlalchemy import text



def convertToSerializable(obj):
    """Convert pandas objects and other non-serializable objects to JSON-compatible format"""
    if isinstance(obj, pd.DataFrame):
        return {
            '__type__': 'DataFrame',
            'data': obj.to_dict('records'),
            'columns': list(obj.columns),
            'shape': list(obj.shape)
        }
    elif isinstance(obj, pd.Series):
        return {
            '__type__': 'Series',
            'data': obj.tolist(),
            'index': obj.index.tolist() if hasattr(obj.index, 'tolist') else list(obj.index),
            'name': str(obj.name) if obj.name else None
        }
    elif isinstance(obj, dict):
        return {k: convertToSerializable(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [convertToSerializable(item) for item in obj]
    elif hasattr(obj, '__float__'):  # numpy types
        try:
            return float(obj)
        except (ValueError, TypeError):
            return str(obj)
    else:
        return obj


def saveResultsToFiles(
    results: List[Dict[str, Any]],
    client_id: int,
    fund_id: int,
    timestamp: str,
    prefix: str = "validation"
):
    """
    Save validation/ratio results to CSV and JSON files
    
    Args:
        results: List of validation/ratio results
        client_id: Client ID
        fund_id: Fund ID
        timestamp: Timestamp string for filename
        prefix: Prefix for filename (default: "validation")
    """
    # Collect all DataFrames for single CSV
    all_dataframes = []
    
    try:
        for result_item in results:
            item_info = result_item.get('validation_info') or result_item.get('ratio_info', {})
            item_id = item_info.get('intvalidationmasterid') or item_info.get('intratiomasterid', 'Unknown')
            item_name = item_info.get('vcvalidationname') or item_info.get('vcrationame', 'Unknown')
            threshold = item_info.get('threshold')
            detail_results = result_item.get('detail_results', [])
            
            for detail_idx, detail_result in enumerate(detail_results, 1):
                detail_id = detail_result.get('intvalidationdetailid') or detail_result.get('intratiodetailid', 'Unknown')
                combined_df = detail_result.get('combined_df')
                
                if combined_df is not None and isinstance(combined_df, pd.DataFrame) and not combined_df.empty:
                    export_df = combined_df.copy()
                    
                    # Add item-level metadata columns
                    id_column = 'Validation_ID' if 'intvalidationmasterid' in item_info else 'Ratio_ID'
                    name_column = 'Validation_Name' if 'vcvalidationname' in item_info else 'Ratio_Name'
                    detail_id_column = 'Detail_ID' if 'intvalidationdetailid' in detail_result else 'Detail_ID'
                    
                    export_df.insert(0, id_column, item_id)
                    export_df.insert(1, name_column, item_name)
                    export_df.insert(2, detail_id_column, detail_id)
                    export_df.insert(3, 'Formula', detail_result.get('formula', ''))
                    export_df.insert(4, 'Filter', detail_result.get('filter', ''))
                    export_df.insert(5, 'Filter_Type', detail_result.get('filter_type', 'I'))
                    export_df.insert(6, 'Threshold', threshold)
                    export_df.insert(7, 'Detail_Status', detail_result.get('status', 'unknown'))
                    export_df.insert(8, 'Passed_Count', detail_result.get('passed_count', 0))
                    export_df.insert(9, 'Failed_Count', detail_result.get('failed_count', 0))
                    export_df.insert(10, 'Total_Count', detail_result.get('total_count', 0))
                    
                    # Calculate per-row Status based on result vs threshold
                    if 'result' in export_df.columns and threshold is not None:
                        try:
                            threshold_value = float(threshold)
                            result_series = export_df['result']
                            
                            # Extract scalar values if result column contains arrays/Series
                            def extract_scalar(x):
                                """Extract scalar value from potentially array-like value"""
                                if pd.isna(x):
                                    return None
                                if hasattr(x, '__len__') and not isinstance(x, str):
                                    try:
                                        if hasattr(x, 'iloc'):
                                            return x.iloc[0] if len(x) > 0 else None
                                        elif hasattr(x, '__getitem__'):
                                            return x[0] if len(x) > 0 else None
                                    except (IndexError, TypeError):
                                        pass
                                return x
                            
                            # Convert result series to scalars
                            result_scalars = result_series.apply(extract_scalar)
                            
                            # Handle NaN values
                            status_series = pd.Series(['unknown'] * len(export_df), index=export_df.index)
                            
                            mask_not_na = result_scalars.notna()
                            if mask_not_na.any():
                                if threshold_value == 0:
                                    # Boolean truthness check
                                    status_series[mask_not_na] = result_scalars[mask_not_na].apply(
                                        lambda x: 'failed' if bool(x) else 'passed'
                                    )
                                else:
                                    # Regular numeric threshold check - ensure values are numeric
                                    def compare_with_threshold(x):
                                        try:
                                            x_float = float(x)
                                            return 'passed' if abs(x_float) < threshold_value else 'failed'
                                        except (ValueError, TypeError):
                                            return 'unknown'
                                    
                                    status_series[mask_not_na] = result_scalars[mask_not_na].apply(compare_with_threshold)
                            
                            export_df.insert(11, 'Status', status_series.values)
                        except (ValueError, TypeError) as e:
                            # If threshold or result can't be compared, use detail status
                            print(f"  Warning: Could not calculate per-row status: {e}")
                            export_df.insert(11, 'Status', detail_result.get('status', 'unknown'))
                    else:
                        # If no result column or threshold, use detail status
                        export_df.insert(11, 'Status', detail_result.get('status', 'unknown'))
                    
                    all_dataframes.append(export_df)
                else:
                    print(f"  Warning: No combined_df available for {prefix} {item_id}, detail {detail_id}")
        
        if all_dataframes:
            # Combine all DataFrames into a single CSV
            combined_all_df = pd.concat(all_dataframes, ignore_index=True)
            csv_filename = f"{prefix}_results_{client_id}_{fund_id}_{timestamp}.csv"
            combined_all_df.to_csv(csv_filename, index=False)
            print(f"\nCSV file saved: {csv_filename} ({len(combined_all_df)} total rows)")
        else:
            print(f"\nWarning: No data to save to CSV")
        
    except Exception as e:
        print(f"\nWarning: Could not save results to CSV file: {e}")
        import traceback
        traceback.print_exc()
    
    # Save summary as JSON (for metadata)
    json_filename = f"{prefix}_results_{client_id}_{fund_id}_{timestamp}.json"
    
    try:
        # Create summary without large DataFrames
        summary_results = []
        for result_item in results:
            item_info = result_item.get('validation_info') or result_item.get('ratio_info', {})
            summary_item = {
                'item_info': item_info,
                'overall_status': result_item.get('overall_status'),
                'total_details': result_item.get('total_details'),
                'passed_details': result_item.get('passed_details'),
                'failed_details': result_item.get('failed_details'),
                'detail_results': []
            }
            
            for detail_result in result_item.get('detail_results', []):
                summary_detail = {
                    'detail_id': detail_result.get('intvalidationdetailid') or detail_result.get('intratiodetailid'),
                    'formula': detail_result.get('formula'),
                    'filter': detail_result.get('filter'),
                    'filter_type': detail_result.get('filter_type'),
                    'status': detail_result.get('status'),
                    'passed_count': detail_result.get('passed_count'),
                    'failed_count': detail_result.get('failed_count'),
                    'total_count': detail_result.get('total_count')
                }
                summary_item['detail_results'].append(summary_detail)
            
            summary_results.append(summary_item)
        
        json_results = convertToSerializable(summary_results)
        with open(json_filename, 'w') as f:
            json.dump(json_results, f, indent=2, default=str)
        print(f"JSON file saved: {json_filename}")
    except Exception as e:
        print(f"\nWarning: Could not save JSON file: {e}")


def _getClientSchema(client_id: int, db_manager) -> Optional[str]:
    """
    Get client schema name from client_id
    
    Args:
        client_id: Client ID
        db_manager: Database manager instance
    
    Returns:
        Client schema name (client.code) or None if not found
    """
    if not db_manager:
        return None
    
    try:
        session = db_manager.get_session_with_schema('public')
        try:
            client = session.query(Client).filter(Client.id == client_id).first()
            if not client:
                print(f"Warning: Client with id {client_id} not found")
                return None
            return client.code
        finally:
            session.close()
    except Exception as e:
        print(f"Error getting client schema for client_id {client_id}: {e}")
        return None


def _createProcessInstance(
    client_id: int,
    fund_id: int,
    validus_type: str,
    source_type: Optional[str] = None,
    source_a: Optional[str] = None,
    source_b: Optional[str] = None,
    date_a: Optional[str] = None,
    date_b: Optional[str] = None,
    currency: Optional[str] = None,
    user_id: Optional[int] = None,
    db_manager = None
) -> Optional[int]:
    """
    Create a process instance entry in validus.tbl_process_instance
    
    Args:
        client_id: Client ID
        fund_id: Fund ID
        validus_type: Type of validus process (e.g., 'Validation', 'Ratio')
        source_type: Source type (optional)
        source_a: Source A name (optional)
        source_b: Source B name (optional)
        date_a: Date A (optional, format: 'YYYY-MM-DD')
        date_b: Date B (optional, format: 'YYYY-MM-DD')
        currency: Currency code (optional)
        user_id: User ID who initiated the process (optional)
        db_manager: Database manager instance
    
    Returns:
        Process instance ID if successful, None otherwise
    """
    if not db_manager:
        return None
    
    try:
        session = db_manager.get_session_with_schema('validus')
        try:
            # Parse dates if provided
            dtdate_a = None
            dtdate_b = None
            if date_a:
                try:
                    dtdate_a = datetime.strptime(date_a, '%Y-%m-%d').date()
                except ValueError:
                    pass
            if date_b:
                try:
                    dtdate_b = datetime.strptime(date_b, '%Y-%m-%d').date()
                except ValueError:
                    pass
            
            # Insert into tbl_process_instance
            insert_sql = text("""
                INSERT INTO validus.tbl_process_instance (
                    intclientid,
                    intfundid,
                    vccurrency,
                    vcvalidustype,
                    vcsourcetype,
                    vcsource_a,
                    vcsource_b,
                    dtdate_a,
                    dtdate_b,
                    dtprocesstime_start,
                    vcprocessstats,
                    vcstatusdescription,
                    intuserid
                ) VALUES (
                    :intclientid,
                    :intfundid,
                    :vccurrency,
                    :vcvalidustype,
                    :vcsourcetype,
                    :vcsource_a,
                    :vcsource_b,
                    :dtdate_a,
                    :dtdate_b,
                    :dtprocesstime_start,
                    :vcprocessstats,
                    :vcstatusdescription,
                    :intuserid
                ) RETURNING intprocessinstanceid
            """)
            
            result = session.execute(insert_sql, {
                'intclientid': client_id,
                'intfundid': fund_id,
                'vccurrency': currency,
                'vcvalidustype': validus_type,
                'vcsourcetype': source_type,
                'vcsource_a': source_a,
                'vcsource_b': source_b,
                'dtdate_a': dtdate_a,
                'dtdate_b': dtdate_b,
                'dtprocesstime_start': datetime.now(),
                'vcprocessstats': 'In Progress',
                'vcstatusdescription': f'{validus_type} execution started',
                'intuserid': user_id
            })
            
            process_instance_id = result.scalar()
            session.commit()
            return process_instance_id
            
        except Exception as e:
            session.rollback()
            print(f"Warning: Could not create process instance: {e}")
            return None
        finally:
            session.close()
    except Exception as e:
        print(f"Error creating process instance: {e}")
        return None


def _createProcessInstanceDetail(
    process_instance_id: int,
    data_load_instance_id: Optional[int] = None,
    db_manager = None
) -> Optional[int]:
    """
    Create a process instance detail entry in validus.tbl_process_instance_details
    
    Args:
        process_instance_id: Process instance ID
        data_load_instance_id: Data load instance ID (optional)
        db_manager: Database manager instance
    
    Returns:
        Process instance detail ID if successful, None otherwise
    """
    if not db_manager or not process_instance_id:
        return None
    
    try:
        session = db_manager.get_session_with_schema('validus')
        try:
            # Insert into tbl_process_instance_details
            insert_sql = text("""
                INSERT INTO validus.tbl_process_instance_details (
                    intprocessinstanceid,
                    intdataloadinstanceid,
                    dtprocesstime
                ) VALUES (
                    :intprocessinstanceid,
                    :intdataloadinstanceid,
                    :dtprocesstime
                ) RETURNING intprocessinstancedetailid
            """)
            
            result = session.execute(insert_sql, {
                'intprocessinstanceid': process_instance_id,
                'intdataloadinstanceid': data_load_instance_id,
                'dtprocesstime': datetime.now()
            })
            
            detail_id = result.scalar()
            session.commit()
            return detail_id
            
        except Exception as e:
            session.rollback()
            print(f"Warning: Could not create process instance detail: {e}")
            return None
        finally:
            session.close()
    except Exception as e:
        print(f"Error creating process instance detail: {e}")
        return None


def _updateProcessInstanceStatus(
    process_instance_id: int,
    status: str,
    status_description: Optional[str] = None,
    db_manager = None
) -> bool:
    """
    Update process instance status
    
    Args:
        process_instance_id: Process instance ID
        status: New status (e.g., 'Completed', 'Failed', 'In Progress')
        status_description: Status description (optional)
        db_manager: Database manager instance
    
    Returns:
        True if successful, False otherwise
    """
    if not db_manager or not process_instance_id:
        return False
    
    try:
        session = db_manager.get_session_with_schema('validus')
        try:
            update_sql = text("""
                UPDATE validus.tbl_process_instance
                SET vcprocessstats = :vcprocessstats,
                    vcstatusdescription = :vcstatusdescription,
                    dtprocesstime_end = :dtprocesstime_end
                WHERE intprocessinstanceid = :intprocessinstanceid
            """)
            
            session.execute(update_sql, {
                'intprocessinstanceid': process_instance_id,
                'vcprocessstats': status,
                'vcstatusdescription': status_description or f'Process {status.lower()}',
                'dtprocesstime_end': datetime.now()
            })
            
            session.commit()
            return True
            
        except Exception as e:
            session.rollback()
            print(f"Warning: Could not update process instance status: {e}")
            return False
        finally:
            session.close()
    except Exception as e:
        print(f"Error updating process instance status: {e}")
        return False


def saveValidationResultsToDatabaseWithSources(
    results: List[Dict[str, Any]],
    client_id: int,
    fund_id: int,
    period_dates: Dict[str, str],
    source_a: str,
    source_b: Optional[str] = None,
    db_manager = None
) -> bool:
    """
    Save validation results to database with source information
    This is a wrapper around saveValidationResultsToDatabase that includes source info
    
    Args:
        results: List of validation results from executeAllValidationsWithSources
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
        validus_type='Validation',
        source_type='Dual' if source_b else 'Single',
        source_a=source_a,
        source_b=source_b,
        date_a=date_a,
        date_b=date_b,
        db_manager=db_manager
    )
    
    if not process_instance_id:
        print("Warning: Could not create process instance, continuing without it")
    
    # Use the existing saveValidationResultsToDatabase logic but with the process_instance_id
    # We'll call the main function but need to ensure it uses our process instance
    # For now, let's duplicate the logic but with source support
    return _saveValidationResultsToDatabaseInternal(
        results=results,
        client_id=client_id,
        fund_id=fund_id,
        process_instance_id=process_instance_id,
        db_manager=db_manager
    )


def _saveValidationResultsToDatabaseInternal(
    results: List[Dict[str, Any]],
    client_id: int,
    fund_id: int,
    process_instance_id: Optional[int],
    db_manager
) -> bool:
    """
    Internal function to save validation results (shared by both single and dual source)
    """
    if not db_manager:
        print("Error: Database manager not available")
        return False
    
    # Get client schema
    schema_name = _getClientSchema(client_id, db_manager)
    if not schema_name:
        print(f"Error: Could not get client schema for client_id={client_id}")
        return False
    
    try:
        # Get session with client schema
        session = db_manager.get_session_with_schema(schema_name)
        
        try:
            records_inserted = 0
            validation_count = 0
            total_details = 0
            
            # Track counters for intsideuniqueid (separate for side A and B) and intmatchid
            side_a_counter = 0
            side_b_counter = 0
            match_id_counter = 0
            
            # Process each validation result
            for result_item in results:
                validation_count += 1
                item_info = result_item.get('validation_info', {})
                validation_config_id = item_info.get('intvalidationmasterid')
                validation_name = item_info.get('vcvalidationname', 'Unknown')
                threshold = item_info.get('threshold')
                detail_results = result_item.get('detail_results', [])
                total_details += len(detail_results)
                
                # Add process instance detail for this validation
                if process_instance_id:
                    _createProcessInstanceDetail(
                        process_instance_id=process_instance_id,
                        data_load_instance_id=None,
                        db_manager=db_manager
                    )
                
                # Get validation configuration ID
                session_validus = db_manager.get_session_with_schema('validus')
                try:
                    from database_models import ValidationConfiguration
                    validation_config = session_validus.query(ValidationConfiguration).filter(
                        ValidationConfiguration.intclientid == client_id,
                        ValidationConfiguration.intfundid == fund_id,
                        ValidationConfiguration.intvalidationmasterid == validation_config_id,
                        ValidationConfiguration.isactive == True
                    ).first()
                    
                    intvalidationconfigurationid = validation_config.intvalidationconfigurationid if validation_config else None
                except Exception as e:
                    print(f"Warning: Could not get validation configuration ID: {e}")
                    intvalidationconfigurationid = None
                finally:
                    session_validus.close()
                
                # Process each detail result
                for detail_result in detail_results:
                    detail_id = detail_result.get('intvalidationdetailid')
                    combined_df = detail_result.get('combined_df')
                    formula = detail_result.get('formula', '')
                    
                    if combined_df is None or not isinstance(combined_df, pd.DataFrame) or combined_df.empty:
                        continue
                    
                    # Get data model ID from detail
                    session_validus = db_manager.get_session_with_schema('validus')
                    try:
                        from database_models import ValidationDetails
                        validation_detail = session_validus.query(ValidationDetails).filter(
                            ValidationDetails.intvalidationdetailid == detail_id
                        ).first()
                        
                        intdatamodelid = validation_detail.intdatamodelid if validation_detail else None
                    except Exception as e:
                        print(f"Warning: Could not get data model ID for detail {detail_id}: {e}")
                        intdatamodelid = None
                    finally:
                        session_validus.close()
                    
                    # Process each row in combined_df
                    for idx, row in combined_df.iterrows():
                        result_value = row.get('result')
                        
                        # Extract scalar value if result_value is array/Series
                        if hasattr(result_value, '__len__') and not isinstance(result_value, str):
                            try:
                                if hasattr(result_value, 'iloc'):
                                    result_value = result_value.iloc[0] if len(result_value) > 0 else None
                                elif hasattr(result_value, '__getitem__'):
                                    result_value = result_value[0] if len(result_value) > 0 else None
                            except (IndexError, TypeError):
                                pass
                        
                        # Convert to numeric if possible
                        intformulaoutput = None
                        vcformulaoutput = None
                        is_valid_value = False
                        try:
                            if result_value is not None and pd.notna(result_value):
                                is_valid_value = True
                        except (ValueError, TypeError):
                            pass
                        
                        if is_valid_value:
                            try:
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
                                vcformulaoutput = str(result_value)
                        
                        # Determine status based on threshold
                        vcstatus = 'Unknown'
                        if threshold is not None and is_valid_value and intformulaoutput is not None:
                            try:
                                threshold_value = float(threshold)
                                result_scalar = float(result_value) if is_valid_value else None
                                
                                if result_scalar is not None and not (np.isinf(result_scalar) or np.isnan(result_scalar)):
                                    result_abs = abs(result_scalar)
                                    
                                    if threshold_value == 0:
                                        vcstatus = 'Failed' if bool(result_scalar) else 'Passed'
                                    else:
                                        vcstatus = 'Passed' if result_abs < threshold_value else 'Failed'
                            except (ValueError, TypeError):
                                vcstatus = 'Unknown'
                        elif not is_valid_value or intformulaoutput is None:
                            # Set to 'Failed' for infinity/NaN values, 'Unknown' for invalid values
                            if is_valid_value and intformulaoutput is None:
                                vcstatus = 'Failed'  # Infinity or NaN is considered a failure
                            else:
                                vcstatus = 'Unknown'
                        else:
                            vcstatus = 'Passed'
                        
                        # Check for intrecid_a and intrecid_b
                        intrecid_a = None
                        intrecid_b = None
                        base_intrecid = None
                        
                        if 'intrecid_a' in combined_df.columns:
                            try:
                                recid_a_value = row.get('intrecid_a') if hasattr(row, 'get') else (row['intrecid_a'] if 'intrecid_a' in row.index else None)
                                if recid_a_value is not None and pd.notna(recid_a_value):
                                    intrecid_a = int(recid_a_value)
                            except (ValueError, TypeError):
                                pass
                        
                        if 'intrecid_b' in combined_df.columns:
                            try:
                                recid_b_value = row.get('intrecid_b') if hasattr(row, 'get') else (row['intrecid_b'] if 'intrecid_b' in row.index else None)
                                if recid_b_value is not None and pd.notna(recid_b_value):
                                    intrecid_b = int(recid_b_value)
                            except (ValueError, TypeError):
                                pass
                        
                        # Fallback: Check for single intrecid column
                        if intrecid_a is None and intrecid_b is None:
                            for col in combined_df.columns:
                                if col.lower() == 'intrecid' or col.lower() == 'recid' or col == 'intrecid':
                                    try:
                                        recid_value = row.get(col) if hasattr(row, 'get') else (row[col] if col in row.index else None)
                                        if recid_value is not None and pd.notna(recid_value):
                                            base_intrecid = int(recid_value)
                                            intrecid_a = base_intrecid
                                    except Exception:
                                        pass
                                    break
                        
                        # Determine if multi-period/source
                        has_both_recids = intrecid_a is not None and intrecid_b is not None
                        
                        if has_both_recids:
                            # Multi-period/source: create entries for both sides
                            match_id_counter += 1
                            intmatchid = match_id_counter
                            
                            # Side A
                            side_a_counter += 1
                            insert_sql = text(f"""
                                INSERT INTO {schema_name}.tbl_validation_result (
                                    intprocessinstanceid, intdatamodelid, intvalidationconfigurationid,
                                    intdmrecid, vcside, intsideuniqueid, intmatchid,
                                    intformulaoutput, vcformulaoutput, vcstatus,
                                    vcaction, intactionuserid, dtactiontime,
                                    intassignedtouserid, vcassignedstatus, intnewvalue, vccomment, isactive
                                ) VALUES (
                                    :intprocessinstanceid, :intdatamodelid, :intvalidationconfigurationid,
                                    :intdmrecid, :vcside, :intsideuniqueid, :intmatchid,
                                    :intformulaoutput, :vcformulaoutput, :vcstatus,
                                    :vcaction, :intactionuserid, :dtactiontime,
                                    :intassignedtouserid, :vcassignedstatus, :intnewvalue, :vccomment, :isactive
                                )
                            """)
                            
                            session.execute(insert_sql, {
                                'intprocessinstanceid': process_instance_id,
                                'intdatamodelid': intdatamodelid,
                                'intvalidationconfigurationid': intvalidationconfigurationid,
                                'intdmrecid': intrecid_a,
                                'vcside': 'A',
                                'intsideuniqueid': side_a_counter,
                                'intmatchid': intmatchid,
                                'intformulaoutput': intformulaoutput,
                                'vcformulaoutput': vcformulaoutput,
                                'vcstatus': vcstatus,
                                'vcaction': None,
                                'intactionuserid': None,
                                'dtactiontime': None,
                                'intassignedtouserid': None,
                                'vcassignedstatus': None,
                                'intnewvalue': None,
                                'vccomment': None,
                                'isactive': True
                            })
                            records_inserted += 1
                            
                            # Side B
                            side_b_counter += 1
                            session.execute(insert_sql, {
                                'intprocessinstanceid': process_instance_id,
                                'intdatamodelid': intdatamodelid,
                                'intvalidationconfigurationid': intvalidationconfigurationid,
                                'intdmrecid': intrecid_b,
                                'vcside': 'B',
                                'intsideuniqueid': side_b_counter,
                                'intmatchid': intmatchid,
                                'intformulaoutput': intformulaoutput,
                                'vcformulaoutput': vcformulaoutput,
                                'vcstatus': vcstatus,
                                'vcaction': None,
                                'intactionuserid': None,
                                'dtactiontime': None,
                                'intassignedtouserid': None,
                                'vcassignedstatus': None,
                                'intnewvalue': None,
                                'vccomment': None,
                                'isactive': True
                            })
                            records_inserted += 1
                        else:
                            # Single period/source: insert single entry
                            intdmrecid = intrecid_a if intrecid_a is not None else base_intrecid
                            
                            insert_sql = text(f"""
                                INSERT INTO {schema_name}.tbl_validation_result (
                                    intprocessinstanceid, intdatamodelid, intvalidationconfigurationid,
                                    intdmrecid, vcside, intsideuniqueid, intmatchid,
                                    intformulaoutput, vcformulaoutput, vcstatus,
                                    vcaction, intactionuserid, dtactiontime,
                                    intassignedtouserid, vcassignedstatus, intnewvalue, vccomment, isactive
                                ) VALUES (
                                    :intprocessinstanceid, :intdatamodelid, :intvalidationconfigurationid,
                                    :intdmrecid, :vcside, :intsideuniqueid, :intmatchid,
                                    :intformulaoutput, :vcformulaoutput, :vcstatus,
                                    :vcaction, :intactionuserid, :dtactiontime,
                                    :intassignedtouserid, :vcassignedstatus, :intnewvalue, :vccomment, :isactive
                                )
                            """)
                            
                            session.execute(insert_sql, {
                                'intprocessinstanceid': process_instance_id,
                                'intdatamodelid': intdatamodelid,
                                'intvalidationconfigurationid': intvalidationconfigurationid,
                                'intdmrecid': intdmrecid,
                                'vcside': None,
                                'intsideuniqueid': None,
                                'intmatchid': None,
                                'intformulaoutput': intformulaoutput,
                                'vcformulaoutput': vcformulaoutput,
                                'vcstatus': vcstatus,
                                'vcaction': None,
                                'intactionuserid': None,
                                'dtactiontime': None,
                                'intassignedtouserid': None,
                                'vcassignedstatus': None,
                                'intnewvalue': None,
                                'vccomment': None,
                                'isactive': True
                            })
                            records_inserted += 1
            
            # Commit all inserts
            session.commit()
            print(f"\nSuccessfully saved {records_inserted} validation result record(s) to {schema_name}.tbl_validation_result")
            
            # Update process instance status
            if process_instance_id:
                _updateProcessInstanceStatus(
                    process_instance_id=process_instance_id,
                    status='Completed',
                    status_description=f'Validation execution completed. Validations: {validation_count}, Details: {total_details}, Records: {records_inserted}',
                    db_manager=db_manager
                )
            
            return True
            
        except Exception as e:
            session.rollback()
            print(f"Error saving validation results to database: {e}")
            import traceback
            traceback.print_exc()
            
            if process_instance_id:
                _updateProcessInstanceStatus(
                    process_instance_id=process_instance_id,
                    status='Failed',
                    status_description=f'Validation execution failed: {str(e)[:200]}',
                    db_manager=db_manager
                )
            
            return False
        finally:
            session.close()
            
    except Exception as e:
        print(f"Error saving validation results to database: {e}")
        import traceback
        traceback.print_exc()
        
        if process_instance_id:
            _updateProcessInstanceStatus(
                process_instance_id=process_instance_id,
                status='Failed',
                db_manager=db_manager
            )
        
        return False


def saveValidationResultsToDatabase(
    results: List[Dict[str, Any]],
    client_id: int,
    fund_id: int,
    period_dates: Dict[str, str],
    db_manager
) -> bool:
    """
    Save validation results to {client_schema}.tbl_validation_result table
    Also creates entries in validus.tbl_process_instance and validus.tbl_process_instance_details
    
    Args:
        results: List of validation results from executeAllValidations
        client_id: Client ID
        fund_id: Fund ID
        period_dates: Dictionary mapping period names to dates (e.g., {'Period1': '2024-01-31', 'Period2': '2024-02-29'})
        db_manager: Database manager instance
    
    Returns:
        True if successful, False otherwise
    """
    if not db_manager:
        print("Error: Database manager not available")
        return False
    
    # Debug: Print period_dates
    print(f"DEBUG - saveValidationResultsToDatabase called with period_dates: {period_dates}")
    
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
        print(f"DEBUG - Extracted periods: {periods}")
        if len(periods) > 0:
            date_a = period_dates.get(periods[0])
            print(f"DEBUG - date_a: {date_a}")
            # Only set date_b if there are at least 2 periods with valid dates
            if len(periods) > 1:
                date_b = period_dates.get(periods[1])
                print(f"DEBUG - date_b: {date_b}")
            else:
                # Explicitly set to None when only one period is provided
                date_b = None
                print(f"DEBUG - date_b: None (only one period provided)")
        else:
            date_a = None
            date_b = None
    
    # Create process instance
    process_instance_id = _createProcessInstance(
        client_id=client_id,
        fund_id=fund_id,
        validus_type='Validation',
        source_type='Single',
        source_a=None,
        source_b=None,
        date_a=date_a,
        date_b=date_b,
        db_manager=db_manager
    )
    
    if not process_instance_id:
        print("Warning: Could not create process instance, continuing without it")
    
    # Use internal function to save results
    return _saveValidationResultsToDatabaseInternal(
        results=results,
        client_id=client_id,
        fund_id=fund_id,
        process_instance_id=process_instance_id,
        db_manager=db_manager
    )


def _getDetailAlignKeys(detail: Dict[str, Any], db_manager) -> List[str]:
    """
    Get align keys from validation detail's intgroup_attributeid and intassettypeid
    
    Args:
        detail: Validation detail dictionary
        db_manager: Database manager instance
    
    Returns:
        List of column names for alignment
    """
    detail_align_keys = []
    if not db_manager:
        return detail_align_keys
    
    try:
        session = db_manager.get_session_with_schema('validus')
        try:
            # Get group attribute column name if intgroup_attributeid is available
            intgroup_attributeid = detail.get('intgroup_attributeid')
            if intgroup_attributeid:
                group_attr_detail = session.query(DataModelDetails).filter(
                    DataModelDetails.intdatamodeldetailid == intgroup_attributeid
                ).first()
                if group_attr_detail and group_attr_detail.vcdmcolumnname:
                    detail_align_keys.append(group_attr_detail.vcdmcolumnname)
                    print(f"    Group Attribute: {group_attr_detail.vcdmcolumnname}")
            
            # Get asset type column name if intassettypeid is available
            intassettypeid = detail.get('intassettypeid')
            if intassettypeid:
                asset_type_detail = session.query(DataModelDetails).filter(
                    DataModelDetails.intdatamodeldetailid == intassettypeid
                ).first()
                if asset_type_detail and asset_type_detail.vcdmcolumnname:
                    detail_align_keys.append(asset_type_detail.vcdmcolumnname)
                    print(f"    Asset Type: {asset_type_detail.vcdmcolumnname}")
        finally:
            session.close()
    except Exception as e:
        print(f"    Warning: Could not get align_keys from detail: {e}")
    
    return detail_align_keys


def _processValidationDetailWithSources(
    detail: Dict[str, Any],
    detail_idx: int,
    validation_config: Dict[str, Any],
    client_id: int,
    fund_id: int,
    period_dates: Dict[str, str],
    source_mapping: Dict[str, str],
    align_key: Optional[List[str]],
    align_data: bool,
    include_full_data: bool,
    db_manager
) -> Dict[str, Any]:
    """
    Process a single validation detail with source mapping support
    
    Args:
        detail: Validation detail dictionary
        detail_idx: Index of detail in list
        validation_config: Validation configuration dictionary
        client_id: Client ID
        fund_id: Fund ID
        period_dates: Dictionary mapping period names to dates
        source_mapping: Dictionary mapping source names to actual source values
        align_key: Optional list of column names for alignment
        align_data: Whether to align DataFrames
        include_full_data: Whether to include full result DataFrames/Series
        db_manager: Database manager instance
    
    Returns:
        Dictionary containing detail result
    """
    detail_id = detail.get('intvalidationdetailid')
    formula = detail.get('vcformula')
    detail_filter = detail.get('vcfilter')
    detail_filter_type = detail.get('vcfiltertype', 'I')  # Default to 'I' (Include)
    
    # Get align_keys dynamically from intgroup_attributeid and intassettypeid
    detail_align_keys = _getDetailAlignKeys(detail, db_manager)
    
    # Use detail-specific align_keys if available, otherwise fall back to provided align_key
    final_align_key = detail_align_keys if detail_align_keys else align_key
    
    # Get threshold from config (prefer config threshold over master threshold)
    threshold = validation_config.get('config_threshold')
    if threshold is None:
        threshold = validation_config.get('intthreshold')
    
    # Use detail filter if available, otherwise use config filter
    filter_condition = validation_config.get('vccondition')
    final_filter = detail_filter if detail_filter else filter_condition
    
    print(f"\n  Detail {detail_idx + 1} (ID: {detail_id})")
    print(f"    Formula: {formula}")
    print(f"    Filter: {final_filter}")
    print(f"    Filter Type: {detail_filter_type} ({'Exclude' if detail_filter_type == 'E' else 'Include'})")
    print(f"    Align Keys: {final_align_key}")
    print(f"    Source Mapping: {source_mapping}")
    
    if not formula:
        print(f"    Warning: No formula found for detail {detail_id}")
        return {
            'intvalidationdetailid': detail_id,
            'formula': formula,
            'filter': final_filter,
            'filter_type': detail_filter_type,
            'status': 'skipped',
            'error': 'No formula found'
        }
    
    try:
        # Execute the validation with source_mapping
        result = validateFormulaWithDatabase(
            formula=formula,
            client_id=client_id,
            fund_id=fund_id,
            period_dates=period_dates,
            threshold=threshold,
            align_data=align_data,
            align_key=final_align_key,
            filter_condition=final_filter,
            filter_type=detail_filter_type,
            source_mapping=source_mapping
        )
        
        # Extract results
        passed_count = result.get('passed_count', 0)
        failed_count = result.get('failed_count', 0)
        total_count = result.get('total_count', 0)
        
        detail_status = 'passed' if failed_count == 0 else 'failed'
        
        print(f"    Result: {detail_status.upper()} - {passed_count}/{total_count} passed, {failed_count} failed")
        
        # Store summary information
        detail_result = {
            'intvalidationdetailid': detail_id,
            'formula': formula,
            'filter': final_filter,
            'filter_type': detail_filter_type,
            'status': detail_status,
            'passed_count': passed_count,
            'failed_count': failed_count,
            'total_count': total_count,
            'passed_items': result.get('passed_items', []),
            'failed_items': result.get('failed_items', []),
            'combined_df': result.get('combined_df')  # Always include combined_df for CSV export
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
            'intvalidationdetailid': detail_id,
            'formula': formula,
            'filter': final_filter,
            'filter_type': detail_filter_type,
            'status': 'error',
            'error': str(e)
        }


def _processValidationDetail(
    detail: Dict[str, Any],
    detail_idx: int,
    validation_config: Dict[str, Any],
    client_id: int,
    fund_id: int,
    period_dates: Dict[str, str],
    align_key: Optional[List[str]],
    align_data: bool,
    include_full_data: bool,
    db_manager
) -> Dict[str, Any]:
    """
    Process a single validation detail
    
    Args:
        detail: Validation detail dictionary
        detail_idx: Index of detail in list
        validation_config: Validation configuration dictionary
        client_id: Client ID
        fund_id: Fund ID
        period_dates: Dictionary mapping period names to dates
        align_key: Optional list of column names for alignment
        align_data: Whether to align DataFrames
        include_full_data: Whether to include full result DataFrames/Series
        db_manager: Database manager instance
    
    Returns:
        Dictionary containing detail result
    """
    detail_id = detail.get('intvalidationdetailid')
    formula = detail.get('vcformula')
    detail_filter = detail.get('vcfilter')
    detail_filter_type = detail.get('vcfiltertype', 'I')  # Default to 'I' (Include)
    
    # Get align_keys dynamically from intgroup_attributeid and intassettypeid
    detail_align_keys = _getDetailAlignKeys(detail, db_manager)
    
    # Use detail-specific align_keys if available, otherwise fall back to provided align_key
    final_align_key = detail_align_keys if detail_align_keys else align_key
    
    # Get threshold from config (prefer config threshold over master threshold)
    threshold = validation_config.get('config_threshold')
    if threshold is None:
        threshold = validation_config.get('intthreshold')
    
    # Use detail filter if available, otherwise use config filter
    filter_condition = validation_config.get('vccondition')
    final_filter = detail_filter if detail_filter else filter_condition
    
    print(f"\n  Detail {detail_idx + 1} (ID: {detail_id})")
    print(f"    Formula: {formula}")
    print(f"    Filter: {final_filter}")
    print(f"    Filter Type: {detail_filter_type} ({'Exclude' if detail_filter_type == 'E' else 'Include'})")
    print(f"    Align Keys: {final_align_key}")
    
    if not formula:
        print(f"    Warning: No formula found for detail {detail_id}")
        return {
            'intvalidationdetailid': detail_id,
            'formula': formula,
            'filter': final_filter,
            'filter_type': detail_filter_type,
            'status': 'skipped',
            'error': 'No formula found'
        }
    
    try:
        # Execute the validation
        result = validateFormulaWithDatabase(
            formula=formula,
            client_id=client_id,
            fund_id=fund_id,
            period_dates=period_dates,
            threshold=threshold,
            align_data=align_data,
            align_key=final_align_key,
            filter_condition=final_filter,
            filter_type=detail_filter_type
        )
        
        # Extract results
        passed_count = result.get('passed_count', 0)
        failed_count = result.get('failed_count', 0)
        total_count = result.get('total_count', 0)
        
        detail_status = 'passed' if failed_count == 0 else 'failed'
        
        print(f"    Result: {detail_status.upper()} - {passed_count}/{total_count} passed, {failed_count} failed")
        
        # Store summary information
        detail_result = {
            'intvalidationdetailid': detail_id,
            'formula': formula,
            'filter': final_filter,
            'filter_type': detail_filter_type,
            'status': detail_status,
            'passed_count': passed_count,
            'failed_count': failed_count,
            'total_count': total_count,
            'passed_items': result.get('passed_items', []),
            'failed_items': result.get('failed_items', []),
            'combined_df': result.get('combined_df')  # Always include combined_df for CSV export
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
            'intvalidationdetailid': detail_id,
            'formula': formula,
            'filter': final_filter,
            'filter_type': detail_filter_type,
            'status': 'error',
            'error': str(e)
        }


def _processValidationWithSources(
    validation_config: Dict[str, Any],
    client_id: int,
    fund_id: int,
    period_dates: Dict[str, str],
    source_mapping: Dict[str, str],
    align_key: Optional[List[str]],
    align_data: bool,
    include_full_data: bool,
    db_manager
) -> Dict[str, Any]:
    """
    Process a single validation configuration with source mapping support
    
    Args:
        validation_config: Validation configuration dictionary
        client_id: Client ID
        fund_id: Fund ID
        period_dates: Dictionary mapping period names to dates
        source_mapping: Dictionary mapping source names to actual source values
        align_key: Optional list of column names for alignment
        align_data: Whether to align DataFrames
        include_full_data: Whether to include full result DataFrames/Series
        db_manager: Database manager instance
    
    Returns:
        Dictionary containing validation result
    """
    validation_id = validation_config.get('intvalidationmasterid')
    validation_name = validation_config.get('vcvalidationname', 'Unknown')
    validation_type = validation_config.get('vctype', 'Unknown')
    
    # Get threshold from config (prefer config threshold over master threshold)
    threshold = validation_config.get('config_threshold')
    if threshold is None:
        threshold = validation_config.get('intthreshold')
    
    # Get filter condition from config
    filter_condition = validation_config.get('vccondition')
    
    print(f"\n{'='*80}")
    print(f"Processing Validation: {validation_name} (ID: {validation_id}, Type: {validation_type})")
    print(f"Threshold: {threshold}")
    print(f"Filter Condition: {filter_condition}")
    print(f"Source Mapping: {source_mapping}")
    print(f"{'='*80}")
    
    # Get validation details (formulas and filters)
    details = validation_config.get('details', [])
    
    if not details:
        print(f"  Warning: No details found for validation {validation_name}")
        return {
            'validation_info': {
                'intvalidationmasterid': validation_id,
                'vcvalidationname': validation_name,
                'vctype': validation_type,
                'threshold': threshold,
                'filter_condition': filter_condition
            },
            'detail_results': [],
            'overall_status': 'skipped',
            'error': 'No validation details found'
        }
    
    detail_results = []
    overall_passed = True
    
    # Process each validation detail
    for detail_idx, detail in enumerate(details):
        detail_result = _processValidationDetailWithSources(
            detail=detail,
            detail_idx=detail_idx,
            validation_config=validation_config,
            client_id=client_id,
            fund_id=fund_id,
            period_dates=period_dates,
            source_mapping=source_mapping,
            align_key=align_key,
            align_data=align_data,
            include_full_data=include_full_data,
            db_manager=db_manager
        )
        
        detail_results.append(detail_result)
        
        # Update overall status
        if detail_result.get('status') == 'failed' or detail_result.get('status') == 'error':
            overall_passed = False
    
    # Compile overall result for this validation
    overall_status = 'passed' if overall_passed else 'failed'
    
    return {
        'validation_info': {
            'intvalidationmasterid': validation_id,
            'vcvalidationname': validation_name,
            'vctype': validation_type,
            'vcsubtype': validation_config.get('vcsubtype'),
            'vcdescription': validation_config.get('vcdescription'),
            'threshold': threshold,
            'filter_condition': filter_condition
        },
        'detail_results': detail_results,
        'overall_status': overall_status,
        'total_details': len(details),
        'passed_details': sum(1 for r in detail_results if r.get('status') == 'passed'),
        'failed_details': sum(1 for r in detail_results if r.get('status') == 'failed')
    }


def _processValidation(
    validation_config: Dict[str, Any],
    client_id: int,
    fund_id: int,
    period_dates: Dict[str, str],
    align_key: Optional[List[str]],
    align_data: bool,
    include_full_data: bool,
    db_manager
) -> Dict[str, Any]:
    """
    Process a single validation configuration
    
    Args:
        validation_config: Validation configuration dictionary
        client_id: Client ID
        fund_id: Fund ID
        period_dates: Dictionary mapping period names to dates
        align_key: Optional list of column names for alignment
        align_data: Whether to align DataFrames
        include_full_data: Whether to include full result DataFrames/Series
        db_manager: Database manager instance
    
    Returns:
        Dictionary containing validation result
    """
    validation_id = validation_config.get('intvalidationmasterid')
    validation_name = validation_config.get('vcvalidationname', 'Unknown')
    validation_type = validation_config.get('vctype', 'Unknown')
    
    # Get threshold from config (prefer config threshold over master threshold)
    threshold = validation_config.get('config_threshold')
    if threshold is None:
        threshold = validation_config.get('intthreshold')
    
    # Get filter condition from config
    filter_condition = validation_config.get('vccondition')
    
    print(f"\n{'='*80}")
    print(f"Processing Validation: {validation_name} (ID: {validation_id}, Type: {validation_type})")
    print(f"Threshold: {threshold}")
    print(f"Filter Condition: {filter_condition}")
    print(f"{'='*80}")
    
    # Get validation details (formulas and filters)
    details = validation_config.get('details', [])
    
    if not details:
        print(f"  Warning: No details found for validation {validation_name}")
        return {
            'validation_info': {
                'intvalidationmasterid': validation_id,
                'vcvalidationname': validation_name,
                'vctype': validation_type,
                'threshold': threshold,
                'filter_condition': filter_condition
            },
            'detail_results': [],
            'overall_status': 'skipped',
            'error': 'No validation details found'
        }
    
    detail_results = []
    overall_passed = True
    
    # Process each validation detail
    for detail_idx, detail in enumerate(details):
        detail_result = _processValidationDetail(
            detail=detail,
            detail_idx=detail_idx,
            validation_config=validation_config,
            client_id=client_id,
            fund_id=fund_id,
            period_dates=period_dates,
            align_key=align_key,
            align_data=align_data,
            include_full_data=include_full_data,
            db_manager=db_manager
        )
        
        detail_results.append(detail_result)
        
        # Update overall status
        if detail_result.get('status') == 'failed' or detail_result.get('status') == 'error':
            overall_passed = False
    
    # Compile overall result for this validation
    overall_status = 'passed' if overall_passed else 'failed'
    
    return {
        'validation_info': {
            'intvalidationmasterid': validation_id,
            'vcvalidationname': validation_name,
            'vctype': validation_type,
            'vcsubtype': validation_config.get('vcsubtype'),
            'vcdescription': validation_config.get('vcdescription'),
            'threshold': threshold,
            'filter_condition': filter_condition
        },
        'detail_results': detail_results,
        'overall_status': overall_status,
        'total_details': len(details),
        'passed_details': sum(1 for r in detail_results if r.get('status') == 'passed'),
        'failed_details': sum(1 for r in detail_results if r.get('status') == 'failed')
    }


def executeAllValidations(
    client_id: int,
    fund_id: int,
    period_dates: Dict[str, str],
    align_key: Optional[list] = None,
    align_data: bool = True,
    include_full_data: bool = False
) -> List[Dict[str, Any]]:
    """
    Execute all active validations for a given client and fund
    
    Args:
        client_id: Client ID
        fund_id: Fund ID
        period_dates: Dictionary mapping period names to dates
            Example: {'Period1': '2024-01-31', 'Period2': '2024-02-29'}
        align_key: Optional list of column names for alignment
            Example: ['investmentdescription', 'investmenttype']
        align_data: Whether to align DataFrames (default: True)
        include_full_data: Whether to include full result DataFrames/Series (default: False)
            If False, only summary statistics are included
    
    Returns:
        List of validation results, each containing:
        - validation_info: Validation master and configuration info
        - detail_results: List of results for each validation detail
        - overall_status: 'passed' or 'failed' based on any failures
    """
    
    db_validation_service = DatabaseValidationService()
    
    # Get all active validation configurations
    validation_configs = db_validation_service.get_active_validation_config_details(
        client_id=client_id,
        fund_id=fund_id
    )
    
    if not validation_configs:
        print(f"No active validations found for client_id={client_id}, fund_id={fund_id}")
        return []
    
    print(f"Found {len(validation_configs)} active validation(s)")
    
    all_results = []
    
    # Get database manager once for all validations
    db_manager = get_database_manager()
    
    # Process each validation
    for validation_config in validation_configs:
        validation_result = _processValidation(
            validation_config=validation_config,
            client_id=client_id,
            fund_id=fund_id,
            period_dates=period_dates,
            align_key=align_key,
            align_data=align_data,
            include_full_data=include_full_data,
            db_manager=db_manager
        )
        all_results.append(validation_result)
    
    return all_results


def executeAllValidationsWithSources(
    client_id: int,
    fund_id: int,
    period_dates: Dict[str, str],
    source_a: str,
    source_b: Optional[str] = None,
    align_key: Optional[list] = None,
    align_data: bool = True,
    include_full_data: bool = False
) -> List[Dict[str, Any]]:
    """
    Execute all active validations with vcsourcetype='Dual' for a given client and fund
    Supports both Case 1 (Period with Source) and Case 2 (Source only) scenarios
    
    Args:
        client_id: Client ID
        fund_id: Fund ID
        period_dates: Dictionary mapping period names to dates
            Example Case 1: {'Period1': '2024-01-31', 'Period2': '2024-02-29'}
            Example Case 2: {'Period1': '2024-01-31'}  # Single period for Source comparison
        source_a: Source A name (e.g., 'Bluefield')
        source_b: Optional Source B name (e.g., 'Harborview'). If None, only source_a is used
        align_key: Optional list of column names for alignment
            Example: ['investmentdescription', 'investmenttype']
        align_data: Whether to align DataFrames (default: True)
        include_full_data: Whether to include full result DataFrames/Series (default: False)
            If False, only summary statistics are included
    
    Returns:
        List of validation results, each containing:
        - validation_info: Validation master and configuration info
        - detail_results: List of results for each validation detail
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
    
    # Get all active validation configurations
    all_validation_configs = db_validation_service.get_active_validation_config_details(
        client_id=client_id,
        fund_id=fund_id
    )
    
    # Filter validations based on case:
    # Case 1: Filter for Dual Source validations (vcsourcetype='Dual')
    # Case 2: Filter for Single Source validations (vcsourcetype='Single')
    if is_case_1:
        # Case 1: 2 sources, 1 period - need Dual Source validations
        validation_configs = [
            config for config in all_validation_configs 
            if config.get('vcsourcetype', '').lower() == 'dual'
        ]
        case_description = "dual source"
    elif is_case_2:
        # Case 2: 1 source, 2 periods - need Single Source validations
        validation_configs = [
            config for config in all_validation_configs 
            if config.get('vcsourcetype', '').lower() == 'single'
        ]
        case_description = "single source"
    else:
        # Fallback: use Dual Source if source_b is provided, otherwise Single Source
        if has_source_b:
            validation_configs = [
                config for config in all_validation_configs 
                if config.get('vcsourcetype', '').lower() == 'dual'
            ]
            case_description = "dual source"
        else:
            validation_configs = [
                config for config in all_validation_configs 
                if config.get('vcsourcetype', '').lower() == 'single'
            ]
            case_description = "single source"
    
    if not validation_configs:
        print(f"No active {case_description} validations found for client_id={client_id}, fund_id={fund_id}")
        return []
    
    print(f"Found {len(validation_configs)} active {case_description} validation(s)")
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
    
    # Get database manager once for all validations
    db_manager = get_database_manager()
    
    # Process each validation
    for validation_config in validation_configs:
        validation_result = _processValidationWithSources(
            validation_config=validation_config,
            client_id=client_id,
            fund_id=fund_id,
            period_dates=period_dates,
            source_mapping=source_mapping,
            align_key=align_key,
            align_data=align_data,
            include_full_data=include_full_data,
            db_manager=db_manager
        )
        all_results.append(validation_result)
    
    return all_results


def printValidationSummary(results: List[Dict[str, Any]]):
    """
    Print a summary of validation execution results
    
    Args:
        results: List of validation results from executeAllValidations
    """
    if not results:
        print("\nNo validation results to display")
        return
    
    print("\n" + "="*80)
    print("VALIDATION EXECUTION SUMMARY")
    print("="*80)
    
    total_validations = len(results)
    passed_validations = sum(1 for r in results if r.get('overall_status') == 'passed')
    failed_validations = sum(1 for r in results if r.get('overall_status') == 'failed')
    
    print(f"\nTotal Validations: {total_validations}")
    print(f"  Passed: {passed_validations}")
    print(f"  Failed: {failed_validations}")
    
    # Print details for each validation
    for idx, result in enumerate(results, 1):
        validation_info = result.get('validation_info', {})
        validation_name = validation_info.get('vcvalidationname', 'Unknown')
        overall_status = result.get('overall_status', 'unknown')
        detail_results = result.get('detail_results', [])
        
        status_symbol = "✓" if overall_status == 'passed' else "✗"
        print(f"\n{idx}. {status_symbol} {validation_name} ({overall_status.upper()})")
        print(f"   Details: {len(detail_results)} detail(s)")
        
        for detail_idx, detail_result in enumerate(detail_results, 1):
            detail_status = detail_result.get('status', 'unknown')
            passed_count = detail_result.get('passed_count', 0)
            failed_count = detail_result.get('failed_count', 0)
            total_count = detail_result.get('total_count', 0)
            
            detail_symbol = "✓" if detail_status == 'passed' else "✗"
            print(f"      {detail_idx}. {detail_symbol} Detail {detail_result.get('intvalidationdetailid')} ({detail_status})")
            if total_count > 0:
                print(f"         Passed: {passed_count}/{total_count}, Failed: {failed_count}")
            if detail_result.get('error'):
                print(f"         Error: {detail_result.get('error')}")


# Example usage
if __name__ == "__main__":
    # Example configuration
    client_id = 2
    fund_id = 1
    fund_id_dual = 2

    # Single source
    period_dates = {
        'Period1': '2024-01-31',
        'Period2': '2024-02-29'
    }
    sources = {
        'source_a': "Bluefield",
    }

    # Dual source
    period_dates_dual = {
        'Period1': '2024-01-31',
    }
    sources_dual = {
        'source_a': "Harborview",
        'source_b': "Clearledger",
    }
    # Optional: specify alignment keys
    align_keys = ['investmentdescription', 'investmenttype']
    
    try:
        print("Starting validation execution...")
        print(f"Client ID: {client_id}")
        print(f"Fund ID: {fund_id}")
        print(f"Period Dates: {period_dates}")
        print(f"Align Keys: {align_keys}")
        
        # results = executeAllValidations(
        #     client_id=client_id,
        #     fund_id=fund_id,
        #     period_dates=period_dates,
        #     align_key=align_keys,
        #     align_data=True
        # )

        # results = executeAllValidationsWithSources(
        #     client_id=client_id,
        #     fund_id=fund_id_dual,
        #     period_dates=period_dates_dual,
        #     source_a=sources_dual['source_a'],
        #     source_b=sources_dual['source_b'],
        #     align_key=align_keys,
        #     align_data=True
        # )

        results = executeAllValidationsWithSources(
            client_id=client_id,
            fund_id=fund_id,
            period_dates=period_dates,
            source_a=sources['source_a'],
            source_b=None,
            align_key=align_keys,
            align_data=True
        )
        
        # Print summary
        printValidationSummary(results)
        
        # Save results to database
        db_manager = get_database_manager()
        # saveValidationResultsToDatabase(results, client_id, fund_id, period_dates, db_manager)
        # saveValidationResultsToDatabaseWithSources(results, client_id, fund_id_dual, period_dates_dual, sources_dual['source_a'], sources_dual['source_b'], db_manager)
        saveValidationResultsToDatabaseWithSources(results, client_id, fund_id, period_dates, sources['source_a'], None, db_manager)
        # Save results to CSV and JSON files
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        # saveResultsToFiles(results, client_id, fund_id, timestamp, prefix="validation")
        
    except Exception as e:
        print(f"Error executing validations: {e}")
        import traceback
        traceback.print_exc()

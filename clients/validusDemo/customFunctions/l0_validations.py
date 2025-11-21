from storage import STORAGE
from validations import VALIDATION_STATUS
from utils.unclassified import getStructToFilterLambda
from utils.generalValidations import validateNonZeroDFSize
import pandas as pd
import os

def check_files_recieved(storage:STORAGE,fundName:str,sourceA:dict,sourceB:dict,validationParams:dict):
    myValidations=[]

    tablesToCheck = ['trail_balance','balance_sheet','positions_with_fx','AP_AR_journal','dividend_journal']
    for source in [sourceA,sourceB]:
        for table in tablesToCheck:
            myValidations.append(_varifyNonZeroRows(storage,fundName,source,table))

    # Add tab count validation
    tab_count_validations = check_tab_count(storage, fundName, sourceA, sourceB, validationParams)
    myValidations.extend(tab_count_validations)

    return myValidations

def check_tab_count(storage:STORAGE,fundName:str,sourceA:dict,sourceB:dict,validationParams:dict):
    """
    Validation function to count tabs in XLS files and add tab count to validation output
    """
    myValidations = []
    
    # Create a mapping from short source names to full company names in file names
    source_name_mapping = {
        'StratusGA': 'Stratus Global Administrators',
        'VeridexAS': 'Veridex Accounting Services',
        'Harborview': 'Harborview Fund Services',
        'Bluefield': 'Bluefield Investor Services',
        'ClearLedger': 'ClearLedger Solutions'
    }
    
    # Create a list of sources to check, handling cases where sourceB might not be present
    sources_to_check = [('sourceA', sourceA)]
    
    # Only add sourceB if it's present and not None
    if sourceB is not None and isinstance(sourceB, dict) and 'source' in sourceB:
        sources_to_check.append(('sourceB', sourceB))
    
    # Check each available source for XLS files and count tabs
    for source_name, source_config in sources_to_check:
        try:
            # Get the l0 directory path (files are stored directly in l0, not in subdirectories)
            l0_path = storage.getDir('l0', [])
            
            # Look for XLS/XLSX files that match the source name pattern
            xls_files = []
            source_name_pattern = source_config['source']
            process_date = source_config.get('processDate', '')
            
            # Get the full company name for matching
            full_company_name = source_name_mapping.get(source_name_pattern, source_name_pattern)
            
            # Debug: Print all files in l0 directory
            all_files = os.listdir(l0_path)
            xls_files_debug = [f for f in all_files if f.lower().endswith(('.xls', '.xlsx'))]
            
            for file in all_files:
                if file.lower().endswith(('.xls', '.xlsx')):
                    # Check if the file name contains either the short source name or the full company name
                    if (source_name_pattern.lower() in file.lower() or 
                        full_company_name.lower() in file.lower()):
                        xls_files.append(os.path.join(l0_path, file))
            
            # Debug information
            debug_info = {
                'l0_path': l0_path,
                'source_name_pattern': source_name_pattern,
                'full_company_name': full_company_name,
                'process_date': process_date,
                'all_xls_files': xls_files_debug,
                'matching_source_files': [os.path.basename(f) for f in xls_files]
            }
            
            if xls_files:
                # Try to find the file that matches the process date
                matching_file = None
                month_year = None
                
                if process_date:
                    try:
                        # Convert process date to month/year format for matching
                        from datetime import datetime
                        process_dt = datetime.strptime(process_date, '%Y-%m-%d')
                        month_year = process_dt.strftime('%b %Y')  # e.g., "Feb 2024"
                        
                        debug_info['converted_month_year'] = month_year
                        
                        # Look for file with matching month/year
                        for file_path in xls_files:
                            file_name = os.path.basename(file_path)
                            debug_info['checking_file'] = file_name
                            if month_year in file_name:
                                matching_file = file_path
                                debug_info['matched_file'] = file_name
                                break
                    except Exception as e:
                        # If date parsing fails, fall back to first file
                        matching_file = xls_files[0]
                        debug_info['date_parse_error'] = str(e)
                
                # If no matching file found by date, use the first one
                if matching_file is None:
                    matching_file = xls_files[0]
                    debug_info['using_first_file'] = os.path.basename(matching_file)
                
                try:
                    xlsx_file = pd.ExcelFile(matching_file)
                    tab_count = len(xlsx_file.sheet_names)
                    
                    # Create validation status with tab count
                    validation = VALIDATION_STATUS().setProductName('validus').setType('File').setSubType('Tab Count').setSubType2(source_name).setMessage(tab_count).setData({
                        'tabCount': tab_count,
                        'fileName': os.path.basename(matching_file),
                        'sheetNames': xlsx_file.sheet_names,
                        'sourceName': source_name_pattern,
                        'processDate': process_date,
                        'matchedByDate': process_date and month_year and month_year in os.path.basename(matching_file),
                        'debug': debug_info
                    })
                    myValidations.append(validation)
                    
                except Exception as e:
                    # If there's an error reading the file, create an error validation
                    validation = VALIDATION_STATUS().setProductName('validus').setType('File').setSubType('Tab Count').setSubType2(f'{source_name}_error').setMessage(-1).setData({
                        'error': str(e),
                        'fileName': os.path.basename(matching_file) if 'matching_file' in locals() else 'unknown',
                        'sourceName': source_name_pattern,
                        'processDate': process_date,
                        'debug': debug_info
                    })
                    myValidations.append(validation)
            else:
                # No XLS files found for this source
                validation = VALIDATION_STATUS().setProductName('validus').setType('File').setSubType('Tab Count').setSubType2(f'{source_name}_no_files').setMessage(0).setData({
                    'tabCount': 0,
                    'message': f'No XLS/XLSX files found for source: {source_name_pattern}',
                    'sourceName': source_name_pattern,
                    'processDate': process_date,
                    'debug': debug_info
                })
                myValidations.append(validation)
                
        except Exception as e:
            # General error handling
            validation = VALIDATION_STATUS().setProductName('validus').setType('File').setSubType('Tab Count').setSubType2(f'{source_name}_error').setMessage(-1).setData({
                'error': str(e),
                'sourceName': source_config['source'] if 'source_config' in locals() else 'unknown',
                'processDate': source_config.get('processDate', '') if 'source_config' in locals() else '',
                'debug': {'error_location': 'general_exception'}
            })
            myValidations.append(validation)
    
    return myValidations

def _varifyNonZeroRows(storage:STORAGE,fundName:str,source:dict,table:str):
    myFilter=getStructToFilterLambda({'fundName':fundName,'source':source['source'],'processDate':source['processDate']})
    myDF=storage.getFilteredTableAsDF('l1',table,myFilter)
    return validateNonZeroDFSize(myDF,'frame','file_revieved',table,'')


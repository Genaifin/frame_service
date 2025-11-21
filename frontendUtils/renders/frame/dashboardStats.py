import json
import os
import sys
import json

from storage import STORAGE
# print(STORAGE)

storage = STORAGE('frameDemo', {'defaultFileStorage': 'onPrem'})

def _load_all_file_meta():
    return storage.getJSONDump('ldummy', '', 'allFileMeta')

def _load_l2_state():
    return storage.getState("l2")


def _load_ldummy_json_dump():
    return storage.getJSONDump("ldummy","", "allFileMeta")

def _apply_filters(files_dict, params):
    """
    Apply filters to the new grouped file structure
    """
    query_params = params.get('query', {})
    
    extractor_filter = params.get('extractor') or query_params.get('extractor', None)
    file_type_filter = params.get('fileType') or query_params.get('fileType', None)
    source_filter = params.get('source') or query_params.get('source', None)
    
    filtered_files = {}
    
    # Handle new grouped structure
    for file_type_group, files_group in files_dict.items():
        if isinstance(files_group, dict):
            for file_key, file_data in files_group.items():
                if isinstance(file_data, dict):
                    include_file = True
                    
                    if extractor_filter and extractor_filter != 'All':
                        file_extractor = file_data.get('extract', '').upper()
                        if extractor_filter.upper() != file_extractor:
                            include_file = False
                    
                    if file_type_filter and include_file:
                        file_type = file_data.get('fileType', '')
                        alt_file_type = file_data.get('File Type', '')
                        if file_type_filter not in [file_type, alt_file_type]:
                            include_file = False

                    if source_filter and include_file:
                        file_source = file_data.get('source', '')
                        if source_filter != file_source:
                            include_file = False
                    
                    if include_file:
                        filtered_files[file_key] = file_data
    
    return filtered_files


def getTotalReceivedFilesCount(params):
    try:
        all_files = _load_ldummy_json_dump()
        # Count total files across all groups
        total_count = 0
        for file_type_group, files_group in all_files.items():
            if isinstance(files_group, dict):
                total_count += len(files_group)
        
        count = max(total_count, int(getProcessedFilesCount(params)["value"]))
        return {
            "label": "Total Received Files",
            "value": str(count),
            "cssProperties": {
                "padding": "24px",
                "borderRadius": "24px"
            }
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "data": {
                "value": "0",
                "count": 0,
                "label": "Total Received Files"
            }
        }

def getProcessedFilesCount(params):
    try:
        all_files = _load_l2_state()
        count1=len(all_files["processedFiles"])
        return {
            "label": "Processed Files",
            "value": str(count1),
            "cssProperties": {
                "padding": "0 16px 0 0"
            }
        }
    except Exception as e:
        return {
            "label": "Processed Files",
            "value": "0",
            "cssProperties": {
                "padding": "0 16px 0 0"
            }
        }
def getInProgressFilesCount(params):
    try:
        all_files = _load_all_file_meta()
        filtered_files = _apply_filters(all_files, params)
        count = sum(1 for v in filtered_files.values() if v.get("status") in ("In Progress", "In Review"))
        return {
            "title": "Processed",
            "count": str(count),
            "type": "secondary",
            "cssProperties": {
                "backgroundColor": "#F8FAFC"
            }
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "data": {
                "value": "0",
                "count": 0,
                "label": "In Progress Files"
            }
        }

def getFailedFilesCount(params):
    try:
        all_files = _load_all_file_meta()
        filtered_files = _apply_filters(all_files, params)
        count = sum(1 for v in filtered_files.values() if v.get("status") == "Failed")
        return {
            "success": True,
            "data": {
                "value": str(count),
                "count": count,
                "label": "Failed Files"
            }
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "data": {
                "value": "0",
                "count": 0,
                "label": "Failed Files"
            }
        }

def getIgnoredFilesCount(params):
    try:
        all_files = _load_all_file_meta()
        filtered_files = _apply_filters(all_files, params)
        count = sum(1 for v in filtered_files.values() if v.get("status") == "Ignored")
        return {
            "label": "Ignored Files",
            "value": str(count),
            "breakdown": [
                {"label": "Configured but ignored", "value": str(count)},
                {"label": "Non-configured", "value": "0"}
            ],
            "cssProperties": {
                "padding": "24px",
                "borderRadius": "24px"
            }
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "data": {
                "value": "0",
                "count": 0,
                "label": "Ignored Files",
                "breakdown": [
                    {"label": "Configured but ignored", "value": "0"},
                    {"label": "Non-configured", "value": "0"}
                ]
            }
        }

def getDeliveryStats(params):

    # no enough data to  calculate the delivery stats, so as of now just assume below parameters to calculate this one 

    try:
        all_files = _load_all_file_meta()
        filtered_files = _apply_filters(all_files, params)
        processed_files = [k for k, v in filtered_files.items() if v.get("status") == "Processed"]
        total_processed = len(processed_files)
        extraction_only = total_processed // 3
        extraction_and_delivery = total_processed // 3
        delivery_only = total_processed - extraction_only - extraction_and_delivery
        return {
            "title": "Delivery",
            "count": str(total_processed),
            "cssProperties": {
                "backgroundColor": "#FFFFFF",
                "borderRadius": "24px",
                "padding": "24px",
                "chartPadding": "12px 0 0 20px",
                "title": {
                    "fontSize": "16px",
                    "fontWeight": "600",
                    "color": "#4B566B",
                    "textTransform": "uppercase"
                },
                "count": {
                    "fontSize": "24px",
                    "fontWeight": "600",
                    "color": "#181D2E",
                    "fontFamily": '"Chakra Petch", sans-serif'
                }
            },
            "chartConfig": {
                "series": [
                    {"name": "Extraction Only", "data": extraction_only, "action": {"type": "navigation", "to": "/frame/file-manager"}},
                    {"name": "Extraction + File  Delivery", "data": extraction_and_delivery, "action": {"type": "navigation", "to": "/frame/file-manager"}},
                    {"name": "File  Delivery Only", "data": delivery_only, "action": {"type": "navigation", "to": "/frame/file-manager"}}
                ],
                "options": {"colors": ["#E86002", "#6576F6", "#F6C30A"]}
            }
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "data": {
                "value": "0",
                "count": 0,
                "title": "Delivery",
                "chartConfig": {
                    "series": [
                        {"name": "Extraction Only", "data": 0, "action": {"type": "navigation", "to": "/frame/file-manager"}},
                        {"name": "Extraction + File Delivery", "data": 0, "action": {"type": "navigation", "to": "/frame/file-manager"}},
                        {"name": "File Delivery Only", "data": 0, "action": {"type": "navigation", "to": "/frame/file-manager"}}
                    ],
                    "options": {"colors": ["#E86002", "#6576F6", "#F6C30A"]}
                }
            }
        }

# New functions for detailed breakdown by status with filtering
def getFileCountsByStatus(params):
    """
    Get breakdown of files by status with filtering applied
    """
    try:
        all_files = _load_all_file_meta()
        filtered_files = _apply_filters(all_files, params)
        
        status_counts = {
            "Processed": 0,
            "In Progress": 0,
            "In Review": 0,
            "Failed": 0,
            "Ignored": 0,
            "Duplicates": 0
        }
        
        for file_data in filtered_files.values():
            status = file_data.get("status", "Unknown")
            if status in status_counts:
                status_counts[status] += 1
            elif status == "Duplicate":
                status_counts["Duplicates"] += 1
        
        return {
            "success": True,
            "data": status_counts,
            "filters_applied": {
                "extractor": params.get('query', {}).get('extractor'),
                "fileType": params.get('query', {}).get('fileType'),
                "source": params.get('query', {}).get('source')
            }
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "data": {
                "Processed": 0,
                "In Progress": 0,
                "In Review": 0,
                "Failed": 0,
                "Ignored": 0,
                "Duplicates": 0
            }
        }

def getInReviewFilesCount(params):
    """
    Get count of files in review status with filtering
    """
    try:
        all_files = _load_all_file_meta()
        filtered_files = _apply_filters(all_files, params)
        count = sum(1 for v in filtered_files.values() if v.get("status") == "In Review")
        return {
            "success": True,
            "data": {
                "value": str(count),
                "count": count,
                "label": "In Review Files"
            }
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "data": {
                "value": "0",
                "count": 0,
                "label": "In Review Files"
            }
        }

def getDuplicateFilesCount(params):
    """
    Get count of duplicate files with filtering
    """
    try:
        all_files = _load_all_file_meta()
        filtered_files = _apply_filters(all_files, params)
        count = sum(1 for v in filtered_files.values() if v.get("status") == "Duplicate")
        return {
            "success": True,
            "data": {
                "value": str(count),
                "count": count,
                "label": "Duplicate Files"
            }
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "data": {
                "value": "0",
                "count": 0,
                "label": "Duplicate Files"
            }
        }

def getProcessedFilesProgressBar(params):
    try:
        
        all_files = _load_all_file_meta()
        
        # Extract filter parameters from multiple possible sources
        query_params = params.get('query', {})
        
        # Priority order: direct params > query nested > empty string
        extractor_filter = params.get('extractor') or query_params.get('extractor') or None
        file_type_filter = params.get('fileType') or query_params.get('fileType') or None  
        source_filter = params.get('source') or query_params.get('source') or None
        
        
        # Apply filters manually
        filtered_files = {}
        total_files_checked = 0
        files_included = 0
        
        # Handle new grouped structure
        for file_type_group, files_group in all_files.items():
            if isinstance(files_group, dict):
                for file_key, file_data in files_group.items():
                    total_files_checked += 1
                    include_file = True
                    
                    # Filter by extractor
                    if extractor_filter and extractor_filter.strip() and extractor_filter not in ['All', 'all', '']:
                        file_extractor = file_data.get('extract', '').upper()
                        filter_extractor = extractor_filter.upper()
                        if filter_extractor != file_extractor:
                            include_file = False
                    
                    # Filter by file type
                    if file_type_filter and file_type_filter.strip() and include_file and file_type_filter not in ['All', 'all', '']:
                        file_type = file_data.get('fileType', '')
                        alt_file_type = file_data.get('File Type', '')
                        if file_type_filter not in [file_type, alt_file_type]:
                            include_file = False
                    
                    # Filter by source
                    if source_filter and source_filter.strip() and include_file and source_filter not in ['All', 'all', '']:
                        file_source = file_data.get('source', '')
                        if source_filter != file_source:
                            include_file = False
                    
                    if include_file:
                        filtered_files[file_key] = file_data
                        files_included += 1
        
        
        # Get total processed files count from filtered results
        processed_files = [v for v in filtered_files.values() if v.get("status") == "Processed"]
        total_processed = len(processed_files)
        
        
        # Calculate breakdown for chart series
        # These percentages can be adjusted based on real data patterns, just assume 
        completely_automated = int(total_processed * 0.75) 
        replayed = int(total_processed * 0.15)              
        manually_edited = total_processed - completely_automated - replayed  
        return {
            "title": "Processed",
            "count": str(total_processed),
            "cssProperties": {
                "backgroundColor": "#F8FAFC",
                "borderRadius": "8px"
            },
            "chartConfig": {
                "series": [
                    {
                        "name": "Completely Automated",
                        "data": completely_automated,
                        "action": {
                            "type": "navigation",
                            "to": "/frame",
                            "parameters": [
                                {
                                    "key": "page",
                                    "value": "FileManager"
                                }
                            ]
                        }
                    },
                    {
                        "name": "Replayed",
                        "data": replayed,
                        "action": {
                            "type": "navigation",
                            "to": "/frame",
                            "parameters": [
                                {
                                    "key": "page",
                                    "value": "FileManager"
                                }
                            ]
                        }
                    },
                    {
                        "name": "Manually Edited / Updated",
                        "data": manually_edited,
                        "action": {
                            "type": "navigation",
                            "to": "/frame",
                            "parameters": [
                                {
                                    "key": "page",
                                    "value": "FileManager"
                                }
                            ]
                        }
                    }
                ],
                "options": {
                    "colors": ["#E86002", "#6576F6", "#F6C30A"]
                }
            },
        }
        
    except Exception as e:  
        return {
            "success": False,
            "error": str(e),
            "title": "Processed",
            "count": "0",
            "cssProperties": {
                "backgroundColor": "#F8FAFC",
                "borderRadius": "8px"
            },
            "chartConfig": {
                "series": [
                    {
                        "name": "Completely Automated",
                        "data": 0,
                        "action": {
                            "type": "navigation",
                            "to": "/frame",
                            "parameters": [
                                {
                                    "key": "page",
                                    "value": "FileManager"
                                }
                            ]
                        }
                    },
                    {
                        "name": "Replayed",
                        "data": 0,
                        "action": {
                            "type": "navigation",
                            "to": "/frame",
                            "parameters": [
                                {
                                    "key": "page",
                                    "value": "FileManager"
                                }
                            ]
                        }
                    },
                    {
                        "name": "Manually Edited / Updated",
                        "data": 0,
                        "action": {
                            "type": "navigation",
                            "to": "/frame",
                            "parameters": [
                                {
                                    "key": "page",
                                    "value": "FileManager"
                                }
                            ]
                        }
                    }
                ],
                "options": {
                    "colors": ["#E86002", "#6576F6", "#F6C30A"]
                }
            }
        }

def getFailedFilesProgressBar(params):
    try:
        all_files = _load_all_file_meta()
        filtered_files = _apply_filters(all_files, params)
        
        failed_files = [v for v in filtered_files.values() if v.get("status") == "Failed"]
        total_failed = len(failed_files)
        
        failed_extraction = int(total_failed * 0.75)
        failed_account_linking = total_failed - failed_extraction
        
        if failed_account_linking < 0:
            failed_account_linking = 0
        
        return {
            "title": "Failed",
            "count": str(total_failed),
            "cssProperties": {
                "backgroundColor": "#F8FAFC",
                "borderRadius": "8px"
            },
            "chartConfig": {
                "series": [
                    {
                        "name": "Failed Extraction",
                        "data": failed_extraction,
                        "action": {
                            "type": "navigation",
                            "to": "/frame/file-manager"
                        }
                    },
                    {
                        "name": "Failed Account Linking",
                        "data": failed_account_linking,
                        "action": {
                            "type": "navigation",
                            "to": "/frame/file-manager"
                        }
                    }
                ],
                "options": {
                    "colors": ["#E86002", "#6576F6"]
                }
            }
        }
        
    except Exception as e:
        return {
            "title": "Failed",
            "count": "0",
            "cssProperties": {
                "backgroundColor": "#F8FAFC",
                "borderRadius": "8px"
            },
            "chartConfig": {
                "series": [
                    {
                        "name": "Failed Extraction",
                        "data": 0,
                        "action": {
                            "type": "navigation",
                            "to": "/frame/file-manager"
                        }
                    },
                    {
                        "name": "Failed Account Linking",
                        "data": 0,
                        "action": {
                            "type": "navigation",
                            "to": "/frame/file-manager"
                        }
                    }
                ],
                "options": {
                    "colors": ["#E86002", "#6576F6"]
                }
            }
        }

def getInReviewFilesProgressBar(params):
    try:
        all_files = _load_all_file_meta()
        filtered_files = _apply_filters(all_files, params)
        
        in_review_files = [v for v in filtered_files.values() if v.get("status") == "In Review"]
        total_in_review = len(in_review_files)
        
        return {
            "title": "In Review",
            "count": str(total_in_review),
            "cssProperties": {
                "backgroundColor": "#F8FAFC",
                "borderRadius": "8px"
            },
            "chartConfig": {
                "series": [
                    {
                        "name": "To be reviewed",
                        "data": total_in_review,
                        "action": {
                            "type": "navigation",
                            "to": "/frame/file-manager"
                        }
                    }
                ],
                "options": {
                    "colors": ["#E86002", "#6576F6"]
                }
            }
        }
        
    except Exception as e:
        return {
            "title": "In Review",
            "count": "0",
            "cssProperties": {
                "backgroundColor": "#F8FAFC",
                "borderRadius": "8px"
            },
            "chartConfig": {
                "series": [
                    {
                        "name": "To be reviewed",
                        "data": 0,
                        "action": {
                            "type": "navigation",
                            "to": "/frame/file-manager"
                        }
                    }
                ],
                "options": {
                    "colors": ["#E86002", "#6576F6"]
                }
            }
        }

def getDuplicateFilesProgressBar(params):
    try:
        all_files = _load_all_file_meta()
        filtered_files = _apply_filters(all_files, params)
        
        duplicate_files = [v for v in filtered_files.values() if v.get("status") == "Duplicate"]
        total_duplicates = len(duplicate_files)
        
        return {
            "title": "Duplicates",
            "count": str(total_duplicates),
            "cssProperties": {
                "backgroundColor": "#F8FAFC",
                "borderRadius": "8px"
            },
            "chartConfig": {
                "series": [
                    {
                        "name": "Duplicates",
                        "data": total_duplicates,
                        "action": {
                            "type": "navigation",
                            "to": "/frame/file-manager"
                        }
                    }
                ],
                "options": {
                    "colors": ["#E86002"]
                }
            }
        }
        
    except Exception as e:
        return {
            "title": "Duplicates",
            "count": "0",
            "cssProperties": {
                "backgroundColor": "#F8FAFC",
                "borderRadius": "8px"
            },
            "chartConfig": {
                "series": [
                    {
                        "name": "Duplicates",
                        "data": 0,
                        "action": {
                            "type": "navigation",
                            "to": "/frame/file-manager"
                        }
                    }
                ],
                "options": {
                    "colors": ["#E86002"]
                }
            }
        }

def getInProgressFilesProgressBar(params):
    try:
        all_files = _load_all_file_meta()
        filtered_files = _apply_filters(all_files, params)
        
        # Get all in-progress files (In Progress + In Review)
        #todo: need to add more data to calculate this one 
        in_progress_files = [v for v in filtered_files.values() if v.get("status") in ["In Progress", "In Review"]]
        total_in_progress = len(in_progress_files)
        
        # just assume below parameters to calculate this one , not actual one will update later 
        under_extraction = int(total_in_progress * 0.6)  
        under_account_linking = total_in_progress - under_extraction  
        
        return {
            "title": "In Progress",
            "count": str(total_in_progress),
            "cssProperties": {
                "backgroundColor": "#F8FAFC",
                "borderRadius": "8px"
            },
            "chartConfig": {
                "series": [
                    {
                        "name": "Under Extraction",
                        "data": under_extraction,
                        "action": {
                            "type": "navigation",
                            "to": "/frame",
                            "parameters": [
                                {
                                    "key": "page",
                                    "value": "FileManager"
                                }
                            ]
                        }
                    },
                    {
                        "name": "Under Account Linking",
                        "data": under_account_linking,
                        "action": {
                            "type": "navigation",
                            "to": "/frame",
                            "parameters": [
                                {
                                    "key": "page",
                                    "value": "FileManager"
                                }
                            ]
                        }
                    }
                ],
                "options": {
                    "colors": ["#E86002", "#6576F6"]
                }
            }
        }
        
    except Exception as e:
        return {
            "title": "In Progress",
            "count": "0",
            "cssProperties": {
                "backgroundColor": "#F8FAFC",
                "borderRadius": "8px"
            },
            "chartConfig": {
                "series": [
                    {
                        "name": "Under Extraction",
                        "data": 0,
                        "action": {
                            "type": "navigation",
                            "to": "/frame",
                            "parameters": [
                                {
                                    "key": "page",
                                    "value": "FileManager"
                                }
                            ]
                        }
                    },
                    {
                        "name": "Under Account Linking",
                        "data": 0,
                        "action": {
                            "type": "navigation",
                            "to": "/frame",
                            "parameters": [
                                {
                                    "key": "page",
                                    "value": "FileManager"
                                }
                            ]
                        }
                    }
                ],
                "options": {
                    "colors": ["#E86002", "#6576F6"]
                }
            }
        }
  
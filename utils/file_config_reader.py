import json
import os
from pathlib import Path
from typing import Dict, Any, Optional


def readFileConfigs() -> Dict[str, Any]:
    """
    Read all JSON configuration files from the utils/fileConfigs folder.
    
    Returns:
        Dict containing file configurations, metadata, and summary information.
        
    Raises:
        FileNotFoundError: If the fileConfigs folder doesn't exist
        ValueError: If no JSON files are found in the folder
    """
    # Get the path to the fileConfigs folder
    current_file = Path(__file__).resolve()
    file_configs_path = current_file.parent / "fileConfigs"
    
    if not file_configs_path.exists():
        raise FileNotFoundError(f"File configs folder not found at: {file_configs_path}")
    
    # Get all JSON files in the fileConfigs folder
    json_files = list(file_configs_path.glob("*.json"))
    
    if not json_files:
        raise ValueError("No configuration files found in fileConfigs folder")
    
    # Read all JSON files
    files_data = {}
    total_files = len(json_files)
    
    for json_file in json_files:
        try:
            with open(json_file, 'r', encoding='utf-8') as f:
                content = json.load(f)
            
            # Extract metadata from the first response item
            file_info = {
                "content": content
            }
            
            # Add additional metadata if available
            if content.get("response") and len(content["response"]) > 0:
                first_item = content["response"][0]
                document_type = first_item.get("document_type")
                
                file_info.update({
                    "document_type": document_type,
                    "schema_id": first_item.get("schema_id"),
                    "document_type_id": first_item.get("document_type_id"),
                    "is_active": first_item.get("is_active"),
                    "created_at": first_item.get("created_at"),
                    "updated_at": first_item.get("updated_at")
                })
            
            files_data[json_file.name] = file_info
            
        except Exception as e:
            # Log error but continue with other files
            files_data[json_file.name] = {
                "error": f"Failed to read file: {str(e)}",
                "file_size": f"{json_file.stat().st_size / 1024:.1f}KB"
            }
    
    # Create summary information
    document_types = []
    schema_ids = []
    all_active = True
    creation_dates = []
    
    for file_info in files_data.values():
        if "document_type" in file_info:
            document_types.append(file_info["document_type"])
            schema_ids.append(file_info["schema_id"])
            if not file_info.get("is_active", True):
                all_active = False
            if file_info.get("created_at"):
                creation_dates.append(file_info["created_at"])
    
    summary = {
        "document_types": document_types,
        "total_schemas": len(schema_ids),
        "schema_ids": schema_ids,
        "all_active": all_active,
        "creation_date_range": f"{min(creation_dates)} to {max(creation_dates)}" if creation_dates else "N/A"
    }
    
    # Return the complete response
    return {      
        "total_files": total_files,
        "files": files_data,
        "summary": summary
    }


def getFileConfigSummary() -> Dict[str, Any]:
    """
    Get a simplified summary of file configurations.
    
    Returns:
        Dict containing basic summary information about available configurations.
    """
    try:
        config_data = readFileConfigs()
        return {
            "success": True,
            "total_files": config_data["total_files"],
            "document_types": config_data["summary"]["document_types"],
            "total_schemas": config_data["summary"]["total_schemas"],
            "all_active": config_data["summary"]["all_active"]
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e)
        } 


def getDocumentById(document_type_id: str) -> Dict[str, Any]:
    """
    Get information about a specific document type by ID.
    
    Args:
        document_type_id: The ID of the document type to retrieve
        
    Returns:
        Dict containing information about the specific document type.
    """
    try:
        config_data = readFileConfigs()
        
        for file_name, file_info in config_data["files"].items():
            if "document_type" in file_info:
                first_item = file_info["content"]["response"][0]
                
                # Check if this is the document we're looking for
                if first_item.get("document_type_id") == int(document_type_id):
                    # Return the full content from the file
                    return file_info["content"]
        
        # If document not found
        return {
            "success": False,
            "error": f"Document type with ID '{document_type_id}' not found"
        }
        
    except Exception as e:
        return {
            "success": False,
            "error": str(e)
        }


def getDocumentTypes(document_type_id: Optional[str] = None) -> Dict[str, Any]:
    """
    Get basic information about available document types.
    
    Args:
        document_type_id: Optional ID to get a specific document type
        
    Returns:
        Dict containing basic document type information without full schema details.
    """
    # If a specific ID is provided, return that document only
    if document_type_id:
        return getDocumentById(document_type_id)
    
    try:
        config_data = readFileConfigs()
        document_types = []
        
        for file_name, file_info in config_data["files"].items():
            if "document_type" in file_info:
                # Extract basic info from the first response item
                first_item = file_info["content"]["response"][0]
                
                document_type_info = {
                    "created_at": first_item.get("created_at"),
                    "description": first_item.get("document_description"),
                    "document_format": "Short",
                    "document_type": first_item.get("document_type"),
                    "document_type_id": first_item.get("document_type_id"),                 
                    "is_active": first_item.get("is_active"),
                    "meta_data": "null",
                    "processing_method": "Comprehensive",                  
                    "updated_at": first_item.get("updated_at")                   
                }
                
                document_types.append(document_type_info)
        
        return {        
            "response": sorted(document_types, key=lambda x: x["document_type_id"])
        }
        
    except Exception as e:
        return {
            "success": False,
            "error": str(e)
        } 


def createDocumentType(document_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Create a new document type configuration.
    
    Args:
        document_data: Dictionary containing the document type information including:
            - document_type: Name of the document type
            - document_description: Description of the document type
            - schema_blob: JSON schema for the document type
            - is_active: Whether the document type is active (default: True)
            
    Returns:
        Dict containing success status and the created document type information.
    """
    try:
        from datetime import datetime
        import json
        
        # Get the path to the fileConfigs folder
        current_file = Path(__file__).resolve()
        file_configs_path = current_file.parent / "fileConfigs"
        
        if not file_configs_path.exists():
            raise FileNotFoundError(f"File configs folder not found at: {file_configs_path}")
        
        # Read existing configs to get the next available ID
        config_data = readFileConfigs()
        existing_ids = []
        
        for file_info in config_data["files"].values():
            if "document_type_id" in file_info:
                existing_ids.append(file_info["document_type_id"])
        
        # Generate next available ID
        next_id = max(existing_ids) + 1 if existing_ids else 1
        
        # Get current timestamp
        current_time = datetime.now().isoformat()
        
        # Create the document type data structure
        new_document = {
            "schema_id": next_id,
            "document_type_id": next_id,
            "document_type": document_data["document_type"],
            "document_description": document_data["document_description"],
            "is_active": document_data.get("is_active", True),
            "created_at": current_time,
            "updated_at": current_time,
            "schema_blob": document_data["schema_blob"]
        }
        
        # Create the response structure
        response_data = {
            "response": [new_document]
        }
        
        # Create filename based on document type
        filename = f"{document_data['document_type']}.json"
        file_path = file_configs_path / filename
        
        # Check if file already exists
        if file_path.exists():
            return {
                "success": False,
                "error": f"Document type '{document_data['document_type']}' already exists"
            }
        
        # Write the new document type to file
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(response_data, f, indent=2, ensure_ascii=False)
        
        return {
            "success": True,
            "message": f"Document type '{document_data['document_type']}' created successfully",
            "document_type_id": next_id,
            "created_document": new_document
        }
        
    except Exception as e:
        return {
            "success": False,
            "error": str(e)
        } 


def deleteDocumentType(document_type_id: str) -> Dict[str, Any]:
    """
    Delete a document type configuration by ID.
    
    Args:
        document_type_id: The ID of the document type to delete
        
    Returns:
        Dict containing success status and deletion information.
    """
    try:
        import json
        import os
        
        # Get the path to the fileConfigs folder
        current_file = Path(__file__).resolve()
        file_configs_path = current_file.parent / "fileConfigs"
        
        if not file_configs_path.exists():
            raise FileNotFoundError(f"File configs folder not found at: {file_configs_path}")
        
        # Read existing configs to find the document type
        config_data = readFileConfigs()
        target_file = None
        document_info = None
        
        # Find the document type and its file
        for file_name, file_info in config_data["files"].items():
            if "document_type_id" in file_info:
                first_item = file_info["content"]["response"][0]
                if first_item.get("document_type_id") == int(document_type_id):
                    target_file = file_name
                    document_info = first_item
                    break
        
        # Check if document type exists
        if not target_file or not document_info:
            return {
                "success": False,
                "error": f"Document type with ID '{document_type_id}' not found"
            }
        
        # Check if document type is active (optional safety check)
        if document_info.get("is_active", True):
            # You might want to add additional checks here
            # For example, check if there are any files being processed with this document type
            pass
        
        # Construct the full file path
        file_path = file_configs_path / target_file
        
        # Store document info before deletion for response
        deleted_document = {
            "document_type_id": document_info.get("document_type_id"),
            "document_type": document_info.get("document_type"),
            "document_description": document_info.get("document_description"),
            "schema_id": document_info.get("schema_id"),
            "is_active": document_info.get("is_active"),
            "created_at": document_info.get("created_at"),
            "updated_at": document_info.get("updated_at")
        }
        
        # Delete the file
        if file_path.exists():
            os.remove(file_path)
        else:
            return {
                "success": False,
                "error": f"File '{target_file}' not found on disk"
            }
        
        return {
            "success": True,
            "message": f"Document type '{document_info.get('document_type')}' deleted successfully",
            "deleted_document": deleted_document
        }
        
    except Exception as e:
        return {
            "success": False,
            "error": str(e)
        } 
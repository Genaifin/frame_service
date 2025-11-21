#!/usr/bin/env python3
"""
GraphQL Error Helpers - Provides consistent, user-friendly error messages
"""

from typing import Optional
import logging

logger = logging.getLogger(__name__)


class GraphQLError(Exception):
    """Custom GraphQL error with user-friendly message"""
    def __init__(self, message: str, code: Optional[str] = None):
        self.message = message
        self.code = code
        super().__init__(self.message)


def format_error_message(error: Exception, default_message: str) -> str:
    """
    Format error message to be user-friendly and short
    
    Args:
        error: The exception that occurred
        default_message: Default message if error message is not user-friendly
    
    Returns:
        Formatted error message
    """
    error_str = str(error)
    
    # If error message is already user-friendly (short and clear), use it
    if len(error_str) < 200 and not any(tech_term in error_str.lower() for tech_term in [
        'traceback', 'stack trace', 'file "', 'line ', 'psycopg2', 'sqlalchemy',
        'operationalerror', 'integrityerror', 'programmingerror'
    ]):
        return error_str
    
    # Return default message for technical errors
    return default_message


# Error message constants
ERROR_MESSAGES = {
    # Data Model Errors
    'DATA_MODEL_NOT_FOUND': "Data model not found",
    'DATA_MODEL_NAME_DUPLICATE': "A data model with this name already exists",
    'COLUMN_NAME_DUPLICATE': "Column name already exists in this data model",
    'FIELD_NAME_DUPLICATE': "Field name already exists in this data model",
    'INVALID_DATATYPE_CHANGE': "Data type change is not allowed",
    'NO_COLUMNS_DEFINED': "At least one column must be defined",
    'TABLE_CREATION_FAILED': "Failed to create table",
    'TABLE_UPDATE_FAILED': "Failed to update table",
    
    # Validation Errors
    'VALIDATION_NOT_FOUND': "Validation not found",
    'VALIDATION_NAME_DUPLICATE': "A validation with this name already exists",
    'VALIDATION_EXECUTION_FAILED': "Validation execution failed",
    
    # Ratio Errors
    'RATIO_NOT_FOUND': "Ratio not found",
    'RATIO_NAME_DUPLICATE': "A ratio with this name already exists",
    'RATIO_EXECUTION_FAILED': "Ratio execution failed",
    
    # Configuration Errors
    'CONFIGURATION_NOT_FOUND': "Configuration not found",
    'CONFIGURATION_ALREADY_EXISTS': "Configuration already exists for this client/fund",
    
    # Data Load Errors
    'DATA_LOAD_NOT_FOUND': "Data load record not found",
    'NO_DATA_LOAD_COMBINATIONS': "No data load combinations found",
    
    # Process Instance Errors
    'PROCESS_INSTANCE_NOT_FOUND': "Process instance not found",
    'INVALID_DATE_FORMAT': "Invalid date format. Use YYYY-MM-DD",
    'INVALID_SOURCE_COMBINATION': "Invalid source combination",
    
    # General Errors
    'AUTHENTICATION_REQUIRED': "Authentication required",
    'PERMISSION_DENIED': "You don't have permission to perform this action",
    'INVALID_INPUT': "Invalid input provided",
    'DATABASE_ERROR': "Database operation failed",
    'UNEXPECTED_ERROR': "An unexpected error occurred"
}


def get_error_message(key: str, **kwargs) -> str:
    """
    Get formatted error message with optional parameters
    
    Args:
        key: Error message key
        **kwargs: Parameters to format into message
    
    Returns:
        Formatted error message
    """
    message = ERROR_MESSAGES.get(key, ERROR_MESSAGES['UNEXPECTED_ERROR'])
    
    # Format message with kwargs if provided
    try:
        return message.format(**kwargs)
    except KeyError:
        return message


def handle_database_error(error: Exception, operation: str) -> str:
    """
    Handle database-related errors and return user-friendly message
    
    Args:
        error: Database exception
        operation: Operation being performed (e.g., 'create', 'update', 'delete')
    
    Returns:
        User-friendly error message
    """
    error_str = str(error).lower()
    
    # Check for specific database errors
    if 'duplicate key' in error_str or 'unique constraint' in error_str:
        return get_error_message('DATA_MODEL_NAME_DUPLICATE')
    elif 'foreign key' in error_str or 'constraint' in error_str:
        return f"Cannot {operation}: Record is referenced by other data"
    elif 'not null' in error_str:
        return f"Cannot {operation}: Required field is missing"
    elif 'connection' in error_str or 'timeout' in error_str:
        return "Database connection error. Please try again"
    elif 'permission' in error_str or 'access' in error_str:
        return get_error_message('PERMISSION_DENIED')
    else:
        return get_error_message('DATABASE_ERROR')


def handle_validation_error(error: Exception, field: Optional[str] = None) -> str:
    """
    Handle validation errors and return user-friendly message
    
    Args:
        error: Validation exception
        field: Field name that failed validation
    
    Returns:
        User-friendly error message
    """
    error_str = str(error)
    
    if field:
        return f"Invalid value for '{field}': {error_str}"
    
    return error_str if len(error_str) < 150 else get_error_message('INVALID_INPUT')


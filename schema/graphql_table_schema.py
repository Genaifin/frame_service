#!/usr/bin/env python3
"""
GraphQL Schema for Dynamic Table Creation - Based on Data Model Tables
Creates tables based on metadata in tbl_data_model_master and tbl_data_model_details
Maintains consistency with REST API authentication system
"""

import strawberry
from typing import List, Optional
from strawberry.types import Info
from sqlalchemy import text, func
from database_models import get_database_manager, DataModelMaster, DataModelDetails, Client
from datetime import datetime
import logging

# Import authentication context
from .graphql_auth_context import require_authentication, require_role, get_current_user
# Import common types
from .graphql_common_types import PaginationInfoType, check_data_model_name_duplicate
# Import error helpers
from .graphql_error_helpers import GraphQLError, get_error_message, handle_database_error, format_error_message

logger = logging.getLogger(__name__)

# ==================== GraphQL Types ====================

@strawberry.type
class DataModelColumnType:
    """GraphQL type for data model column details"""
    intdatamodeldetailid: int
    intdatamodelid: int
    vcfieldname: Optional[str]
    vcfielddescription: Optional[str]
    vcdatatype: Optional[str]
    intlength: Optional[int]
    intprecision: Optional[int]
    intscale: Optional[int]
    vcdateformat: Optional[str]
    vcdmcolumnname: Optional[str]
    vcdefaultvalue: Optional[str]
    ismandatory: Optional[bool]
    intdisplayorder: Optional[int]
    intcreatedby: Optional[int]
    dtcreatedat: Optional[str]
    intupdatedby: Optional[int]
    dtupdatedat: Optional[str]

@strawberry.type
class DataModelMasterType:
    """GraphQL type for data model master"""
    intdatamodelid: int
    vcmodelname: Optional[str]
    vcdescription: Optional[str]
    vcmodelid: Optional[str]
    vccategory: Optional[str]
    vcsource: Optional[str]
    vctablename: Optional[str]
    isactive: Optional[bool]
    field_count: Optional[int]
    dtcreatedat: str
    intcreatedby: Optional[int]
    dtupdatedat: Optional[str]
    intupdatedby: Optional[int]

@strawberry.type
class DataModelDetailType:
    """GraphQL type for data model with columns"""
    intdatamodelid: int
    vcmodelname: Optional[str]
    vcdescription: Optional[str]
    isactive: Optional[bool]
    columns: List[DataModelColumnType]

@strawberry.type
class TableCreationResultType:
    """Result of table creation operation"""
    success: bool
    message: str
    table_name: Optional[str]
    schema_name: Optional[str]
    sql_statement: Optional[str]

@strawberry.type
class PaginatedDataModelResponseType:
    """Paginated response for data models"""
    dataModels: List[DataModelMasterType]
    pagination: PaginationInfoType

# ==================== GraphQL Inputs ====================

@strawberry.input
class DataModelMasterInput:
    """Input for creating data model master"""
    vcmodelname: str
    vcdescription: Optional[str] = None
    vcmodelid: Optional[str] = None
    vccategory: Optional[str] = None
    vcsource: Optional[str] = None
    vctablename: Optional[str] = None
    isactive: Optional[bool] = True

@strawberry.input
class DataModelColumnInput:
    """Input for data model column"""
    vcfieldname: str
    vcfielddescription: Optional[str] = None
    vcdatatype: Optional[str] = None
    intlength: Optional[int] = None
    intprecision: Optional[int] = None
    intscale: Optional[int] = None
    vcdateformat: Optional[str] = None
    vcdmcolumnname: Optional[str] = None
    vcdefaultvalue: Optional[str] = None
    ismandatory: Optional[bool] = None
    intdisplayorder: Optional[int] = None

@strawberry.input
class UpdateDataModelMasterInput:
    """Input for updating data model master"""
    intdatamodelid: int
    vcmodelname: Optional[str] = None
    vcdescription: Optional[str] = None
    vcmodelid: Optional[str] = None
    vccategory: Optional[str] = None
    vcsource: Optional[str] = None
    vctablename: Optional[str] = None
    isactive: Optional[bool] = None

@strawberry.input
class UpdateDataModelColumnInput:
    """Input for updating data model column"""
    intdatamodeldetailid: int
    vcfieldname: Optional[str] = None
    vcfielddescription: Optional[str] = None
    vcdatatype: Optional[str] = None
    intlength: Optional[int] = None
    intprecision: Optional[int] = None
    intscale: Optional[int] = None
    vcdateformat: Optional[str] = None
    vcdmcolumnname: Optional[str] = None
    vcdefaultvalue: Optional[str] = None
    ismandatory: Optional[bool] = None
    intdisplayorder: Optional[int] = None

@strawberry.input
class UpdateDataModelCompleteInput:
    """Input for updating data model master and details together"""
    # Master fields
    intdatamodelid: int
    vcmodelname: Optional[str] = None
    vcdescription: Optional[str] = None
    vcmodelid: Optional[str] = None
    vccategory: Optional[str] = None
    vcsource: Optional[str] = None
    vctablename: Optional[str] = None
    isactive: Optional[bool] = None
    # Details - arrays for updates, new, and deletions
    update_columns: Optional[List[UpdateDataModelColumnInput]] = None
    new_columns: Optional[List[DataModelColumnInput]] = None
    delete_column_ids: Optional[List[int]] = None

# ==================== Helper Functions ====================

def _data_model_master_to_graphql(master: DataModelMaster, field_count: Optional[int] = None) -> DataModelMasterType:
    """Convert DataModelMaster model to GraphQL type"""
    return DataModelMasterType(
        intdatamodelid=master.intdatamodelid,
        vcmodelname=master.vcmodelname,
        vcdescription=master.vcdescription,
        vcmodelid=master.vcmodelid,
        vccategory=master.vccategory,
        vcsource=master.vcsource,
        vctablename=master.vctablename,
        isactive=master.isactive if master.isactive is not None else True,
        field_count=field_count,
        dtcreatedat=master.dtcreatedat.isoformat() if master.dtcreatedat else "",
        intcreatedby=master.intcreatedby,
        dtupdatedat=master.dtupdatedat.isoformat() if master.dtupdatedat else None,
        intupdatedby=master.intupdatedby
    )

def _generate_tablename_from_modelname(modelname: str) -> str:
    """Generate vctablename from vcmodelname
    
    Rules:
    1. Convert to lowercase
    2. Replace all special characters (non-alphanumeric, non-underscore) with underscore
    3. Add 'dm_' prefix
    
    Example: 'Receivable-and.Payable Journal' -> 'dm_receivable_and_payable_journal'
    """
    import re
    
    if not modelname:
        return 'dm_untitled'
    
    # Convert to lowercase
    result = modelname.lower()
    
    # Replace all non-alphanumeric, non-underscore characters with underscore
    result = re.sub(r'[^a-z0-9_]', '_', result)
    
    # Replace multiple underscores with single underscore
    result = re.sub(r'_+', '_', result)
    
    # Remove leading/trailing underscores
    result = result.strip('_')
    
    # Add dm_ prefix
    result = f'dm_{result}' if result else 'dm_untitled'
    
    return result

def _generate_columnname_from_fieldname(fieldname: str) -> str:
    """Generate vcdmcolumnname from vcfieldname
    
    Rules:
    1. Convert to lowercase
    2. Remove all spaces and special characters (keep only alphanumeric)
    3. Just concatenate everything - no underscores or prefixes
    
    Example: 'Customer ID' -> 'customerid'
    Example: 'Email Address' -> 'emailaddress'
    Example: 'Registration Date' -> 'registrationdate'
    """
    import re
    
    if not fieldname:
        return 'field1'
    
    # Convert to lowercase
    columnname = fieldname.lower()
    # Remove all spaces and special characters, keep only alphanumeric
    columnname = re.sub(r'[^a-z0-9]', '', columnname)
    
    if not columnname:
        return 'field1'
    
    return columnname

def _check_duplicate_column_names(
    session,
    intdatamodelid: int,
    column_names: List[str],
    exclude_detail_ids: Optional[List[int]] = None
) -> Optional[str]:
    """
    Check for duplicate column names within a data model
    
    Args:
        session: Database session
        intdatamodelid: Data model ID
        column_names: List of column names to check (should be normalized/generated)
        exclude_detail_ids: Optional list of detail IDs to exclude (for updates)
    
    Returns:
        Duplicate column name if found, None otherwise
    """
    if not column_names:
        return None
    
    # Get existing columns for this data model
    query = session.query(DataModelDetails.vcdmcolumnname, DataModelDetails.intdatamodeldetailid).filter(
        DataModelDetails.intdatamodelid == intdatamodelid,
        DataModelDetails.vcdmcolumnname.isnot(None)
    )
    
    if exclude_detail_ids:
        query = query.filter(~DataModelDetails.intdatamodeldetailid.in_(exclude_detail_ids))
    
    existing_columns = query.all()
    
    # Normalize all column names for comparison (lowercase, alphanumeric only)
    import re
    normalized_new = {}
    for col_name in column_names:
        if col_name:
            normalized = re.sub(r'[^a-z0-9]', '', col_name.lower())
            if normalized:
                if normalized in normalized_new:
                    # Duplicate within the new columns themselves
                    return col_name
                normalized_new[normalized] = col_name
    
    # Check against existing columns
    for existing_col_name, detail_id in existing_columns:
        if existing_col_name:
            normalized_existing = re.sub(r'[^a-z0-9]', '', existing_col_name.lower())
            if normalized_existing in normalized_new:
                return normalized_new[normalized_existing]
    
    return None

def _check_duplicate_field_names(
    session,
    intdatamodelid: int,
    field_names: List[str],
    exclude_detail_ids: Optional[List[int]] = None
) -> Optional[str]:
    """
    Check for duplicate field names within a data model (normalized)
    
    Args:
        session: Database session
        intdatamodelid: Data model ID
        field_names: List of field names to check
        exclude_detail_ids: Optional list of detail IDs to exclude (for updates)
    
    Returns:
        Duplicate field name if found, None otherwise
    """
    if not field_names:
        return None
    
    # Get existing field names for this data model
    query = session.query(DataModelDetails.vcfieldname, DataModelDetails.intdatamodeldetailid).filter(
        DataModelDetails.intdatamodelid == intdatamodelid,
        DataModelDetails.vcfieldname.isnot(None)
    )
    
    if exclude_detail_ids:
        query = query.filter(~DataModelDetails.intdatamodeldetailid.in_(exclude_detail_ids))
    
    existing_fields = query.all()
    
    # Normalize all field names for comparison (using same normalization as check_data_model_name_duplicate)
    from .graphql_common_types import normalize_name
    normalized_new = {}
    for field_name in field_names:
        if field_name:
            normalized = normalize_name(field_name)
            if normalized:
                if normalized in normalized_new:
                    # Duplicate within the new fields themselves
                    return field_name
                normalized_new[normalized] = field_name
    
    # Check against existing fields
    for existing_field_name, detail_id in existing_fields:
        if existing_field_name:
            normalized_existing = normalize_name(existing_field_name)
            if normalized_existing in normalized_new:
                return normalized_new[normalized_existing]
    
    return None

def _data_model_column_to_graphql(detail: DataModelDetails) -> DataModelColumnType:
    """Convert DataModelDetails model to GraphQL type"""
    return DataModelColumnType(
        intdatamodeldetailid=detail.intdatamodeldetailid,
        intdatamodelid=detail.intdatamodelid,
        vcfieldname=detail.vcfieldname,
        vcfielddescription=detail.vcfielddescription,
        vcdatatype=detail.vcdatatype,
        intlength=detail.intlength,
        intprecision=detail.intprecision,
        intscale=detail.intscale,
        vcdateformat=detail.vcdateformat,
        vcdmcolumnname=detail.vcdmcolumnname,
        vcdefaultvalue=detail.vcdefaultvalue,
        ismandatory=bool(detail.ismandatory) if detail.ismandatory is not None else False,
        intdisplayorder=detail.intdisplayorder,
        intcreatedby=detail.intcreatedby,
        dtcreatedat=detail.dtcreatedat.isoformat() if detail.dtcreatedat else None,
        intupdatedby=detail.intupdatedby,
        dtupdatedat=detail.dtupdatedat.isoformat() if detail.dtupdatedat else None
    )

def _validate_datatype_change(old_datatype: str, new_datatype: str) -> tuple[bool, str]:
    """
    Validate if datatype change is allowed
    
    Rules:
    - Text/Alphanumeric to any other datatype - NO
    - Date to any other datatype except Text - NO
    - Boolean to any other datatype - NO
    - Any datatype to Date - NO
    - Any datatype to Boolean - NO
    - Effectively, we can only change number and date to Text
    
    Returns:
        (is_valid: bool, error_message: str)
    """
    if not old_datatype or not new_datatype:
        return True, ""  # No change if either is None
    
    # Normalize datatype names (case-insensitive)
    old_dt = old_datatype.strip()
    new_dt = new_datatype.strip()
    
    # If same datatype, allow (no change)
    if old_dt.lower() == new_dt.lower():
        return True, ""
    
    # Normalize to common names
    old_normalized = old_dt.lower()
    new_normalized = new_dt.lower()
    
    # Check if old is Text/Alphanumeric
    if old_normalized in ['text', 'alphanumeric', 'string']:
        return False, f"Cannot change datatype from '{old_datatype}' to '{new_datatype}'. Text/Alphanumeric cannot be changed to any other datatype."
    
    # Check if old is Date
    if old_normalized == 'date':
        if new_normalized not in ['text', 'alphanumeric', 'string']:
            return False, f"Cannot change datatype from '{old_datatype}' to '{new_datatype}'. Date can only be changed to Text."
    
    # Check if old is Boolean
    if old_normalized == 'boolean':
        if new_normalized not in ['text', 'alphanumeric', 'string']:
            return False, f"Cannot change datatype from '{old_datatype}' to '{new_datatype}'. Boolean can only be changed to Text."
    
    # Check if new is Date
    if new_normalized == 'date':
        return False, f"Cannot change datatype from '{old_datatype}' to '{new_datatype}'. Cannot change any datatype to Date."
    
    # Check if new is Boolean
    if new_normalized == 'boolean':
        return False, f"Cannot change datatype from '{old_datatype}' to '{new_datatype}'. Cannot change any datatype to Boolean."

    
    return True, ""

def _cleanup_old_datatype_precision(column: DataModelDetails, old_datatype: str, new_datatype: str):
    """
    Clean up precision/scale/length values when datatype changes
    
    When datatype changes:
    - If changing from Number to Text: set intprecision and intscale to None
    - If changing from Date to Text: set vcdateformat to None
    - If changing from Text to Number: keep intlength (may be used for validation)
    """
    if not old_datatype or not new_datatype:
        return
    
    old_dt = old_datatype.strip().lower()
    new_dt = new_datatype.strip().lower()
    
    # If same datatype, no cleanup needed
    if old_dt == new_dt:
        return
    
    # If changing from Number (Integer/Decimal) to Text
    # Handle 'Numeric(Integer)' and 'Numeric(Decimal)' formats (normalized to lowercase)
    is_numeric_type = (old_dt == 'numeric(integer)' or old_dt == 'numeric(decimal)' or 
                      old_dt in ['integer', 'decimal'] or
                      ('numeric' in old_dt and ('integer' in old_dt or 'decimal' in old_dt)))
    is_text_type = new_dt in ['text', 'alphanumeric', 'string']
    
    if is_numeric_type and is_text_type:
        column.intprecision = None
        column.intscale = None
    
    # If changing from Date to Text
    if old_dt == 'date' and is_text_type:
        column.vcdateformat = None
    
    # Note: Changing from Text to Number or Date is not allowed by validation,
    # but we handle cleanup anyway for safety
    is_old_text = old_dt in ['text', 'alphanumeric', 'string']
    is_new_numeric = (new_dt == 'numeric(integer)' or new_dt == 'numeric(decimal)' or 
                     new_dt in ['integer', 'decimal'] or
                     ('numeric' in new_dt and ('integer' in new_dt or 'decimal' in new_dt)))
    
    if is_old_text and is_new_numeric:
        # Clear length as numbers don't use it
        column.intlength = None
    
    if is_old_text and new_dt == 'date':
        # Clear length as Date doesn't use it
        column.intlength = None

def _mapDataTypeToSql(vcdatatype: str, intlength: int = None, intprecision: int = None, intscale: int = None) -> str:
    """Map data type from tbl_data_model_details to SQL type
    
    Rules:
    - 'Text' → varchar(intlength)
    - 'Date' → date
    - 'Numeric(Integer)' → int
    - 'Numeric(Decimal)' → numeric(intprecision,intscale)
    """
    if vcdatatype == 'Text':
        length = intlength or 255
        return f"varchar({length})"
    elif vcdatatype == 'Date':
        return "date"
    elif vcdatatype == 'Numeric(Integer)':
        return "int"
    elif vcdatatype == 'Numeric(Decimal)':
        precision = intprecision or 12
        scale = intscale or 2
        return f"numeric({precision},{scale})"
    else:
        return "varchar(255)"

def _inferDataTypeFromFieldName(fieldname: str) -> str:
    """Infer data type from field name
    Returns a generic type that can be mapped to SQL
    """
    if not fieldname:
        return "String"
    
    fieldname_lower = fieldname.lower()
    
    # Check for common patterns
    if any(keyword in fieldname_lower for keyword in ['id', 'code', 'key']):
        return "Integer"
    elif any(keyword in fieldname_lower for keyword in ['amount', 'value', 'price', 'total', 'sum']):
        return "Decimal"
    elif any(keyword in fieldname_lower for keyword in ['date', 'time']):
        return "Date"
    elif any(keyword in fieldname_lower for keyword in ['flag', 'is_', 'has_']):
        return "Boolean"
    elif any(keyword in fieldname_lower for keyword in ['name', 'description', 'text', 'message']):
        return "Text"
    else:
        return "String"

def _get_all_client_schemas(session) -> List[str]:
    """
    Get all client schema names from the clients table
    
    Returns:
        List of client schema names (client.code values)
    """
    try:
        clients = session.query(Client.code).filter(
            Client.code != 'all_clients',
            Client.is_active == True
        ).all()
        return [client.code for client in clients if client.code]
    except Exception as e:
        logger.error(f"Error getting client schemas: {e}")
        return []

def _schema_exists(session, schema_name: str) -> bool:
    """
    Check if a schema exists in the database
    
    Args:
        session: Database session
        schema_name: Schema name to check
    
    Returns:
        True if schema exists, False otherwise
    """
    try:
        from sqlalchemy import inspect
        inspector = inspect(session.bind)
        schemas = inspector.get_schema_names()
        return schema_name in schemas
    except Exception as e:
        logger.error(f"Error checking if schema {schema_name} exists: {e}")
        return False

def _updateTableFromDataModel(intdatamodelid: int, session, schema_name: str, old_columns: List[DataModelDetails], new_columns: List[DataModelDetails], deleted_column_ids: List[int]) -> tuple:
    """
    Update an existing physical table based on data model definition changes
    
    Args:
        intdatamodelid: Data model ID
        session: Database session
        schema_name: Schema name where table exists
        old_columns: List of old column definitions (before update)
        new_columns: List of new column definitions (after update)
        deleted_column_ids: List of column detail IDs that were deleted
    
    Returns:
        (success: bool, message: str, sql_statements: List[str])
    """
    try:
        # Get model master data
        master = session.query(DataModelMaster).filter(
            DataModelMaster.intdatamodelid == intdatamodelid
        ).first()
        
        if not master:
            return False, "Data model not found", []
        
        table_name = master.vctablename
        if not table_name:
            return False, "Table name not found", []
        
        # Check if table exists
        from sqlalchemy import inspect
        inspector = inspect(session.bind)
        table_exists = table_name in [t for t in inspector.get_table_names(schema=schema_name)]
        
        if not table_exists:
            # Table doesn't exist, create it instead
            return _createTableFromDataModel(intdatamodelid, session, schema_name)
        
        sql_statements = []
        
        # Create maps for easier lookup
        old_columns_by_id = {col.intdatamodeldetailid: col for col in old_columns}
        new_columns_by_id = {col.intdatamodeldetailid: col for col in new_columns}
        old_columns_by_name = {col.vcdmcolumnname: col for col in old_columns if col.vcdmcolumnname}
        new_columns_by_name = {col.vcdmcolumnname: col for col in new_columns if col.vcdmcolumnname}
        
        # Get existing table columns
        existing_columns = {col['name']: col for col in inspector.get_columns(table_name, schema=schema_name)}
        
        # 1. Handle deleted columns - DROP COLUMN
        for deleted_id in deleted_column_ids:
            deleted_col = old_columns_by_id.get(deleted_id)
            if deleted_col and deleted_col.vcdmcolumnname:
                col_name = deleted_col.vcdmcolumnname
                if col_name in existing_columns:
                    # Don't drop intrecid or intdataloadinstanceid
                    if col_name not in ['intrecid', 'intdataloadinstanceid']:
                        drop_sql = f'ALTER TABLE {schema_name}.{table_name} DROP COLUMN IF EXISTS {col_name}'
                        sql_statements.append(drop_sql)
                        try:
                            session.execute(text(drop_sql))
                            logger.info(f"Dropped column {col_name} from {schema_name}.{table_name}")
                        except Exception as e:
                            logger.warning(f"Failed to drop column {col_name}: {e}")
        
        # 2. Handle new columns - ADD COLUMN
        for new_col in new_columns:
            col_name = new_col.vcdmcolumnname
            if not col_name:
                continue
            
            if col_name not in existing_columns:
                # New column - add it
                col_datatype = new_col.vcdatatype or _inferDataTypeFromFieldName(new_col.vcfieldname)
                sql_type = _mapDataTypeToSql(col_datatype, new_col.intlength, new_col.intprecision, new_col.intscale)
                
                col_def = f"{col_name} {sql_type}"
                
                # Add default value if specified
                if new_col.vcdefaultvalue:
                    if col_datatype in ['Text', 'String']:
                        col_def += f" DEFAULT '{new_col.vcdefaultvalue}'"
                    else:
                        col_def += f" DEFAULT {new_col.vcdefaultvalue}"
                
                add_sql = f'ALTER TABLE {schema_name}.{table_name} ADD COLUMN {col_def}'
                sql_statements.append(add_sql)
                try:
                    session.execute(text(add_sql))
                    logger.info(f"Added column {col_name} to {schema_name}.{table_name}")
                except Exception as e:
                    logger.warning(f"Failed to add column {col_name}: {e}")
        
        # 3. Handle modified columns - ALTER COLUMN
        for new_col in new_columns:
            col_name = new_col.vcdmcolumnname
            if not col_name or col_name not in existing_columns:
                continue
            
            # Check if column was modified
            old_col = old_columns_by_name.get(col_name)
            if not old_col:
                continue  # New column, already handled
            
            # Check if datatype, length, precision, scale, or default changed
            old_datatype = old_col.vcdatatype or _inferDataTypeFromFieldName(old_col.vcfieldname)
            new_datatype = new_col.vcdatatype or _inferDataTypeFromFieldName(new_col.vcfieldname)
            old_sql_type = _mapDataTypeToSql(old_datatype, old_col.intlength, old_col.intprecision, old_col.intscale)
            new_sql_type = _mapDataTypeToSql(new_datatype, new_col.intlength, new_col.intprecision, new_col.intscale)
            
            if old_sql_type != new_sql_type:
                # Datatype changed - ALTER COLUMN TYPE
                alter_sql = f'ALTER TABLE {schema_name}.{table_name} ALTER COLUMN {col_name} TYPE {new_sql_type}'
                sql_statements.append(alter_sql)
                try:
                    session.execute(text(alter_sql))
                    logger.info(f"Altered column {col_name} type in {schema_name}.{table_name}")
                except Exception as e:
                    logger.warning(f"Failed to alter column {col_name} type: {e}")
            
            # Check if default value changed
            if old_col.vcdefaultvalue != new_col.vcdefaultvalue:
                if new_col.vcdefaultvalue:
                    if new_datatype in ['Text', 'String']:
                        default_val = f"'{new_col.vcdefaultvalue}'"
                    else:
                        default_val = str(new_col.vcdefaultvalue)
                    alter_default_sql = f'ALTER TABLE {schema_name}.{table_name} ALTER COLUMN {col_name} SET DEFAULT {default_val}'
                else:
                    alter_default_sql = f'ALTER TABLE {schema_name}.{table_name} ALTER COLUMN {col_name} DROP DEFAULT'
                sql_statements.append(alter_default_sql)
                try:
                    session.execute(text(alter_default_sql))
                    logger.info(f"Altered column {col_name} default in {schema_name}.{table_name}")
                except Exception as e:
                    logger.warning(f"Failed to alter column {col_name} default: {e}")
        
        session.commit()
        
        if sql_statements:
            return True, f"Successfully updated table {schema_name}.{table_name}", sql_statements
        else:
            return True, f"No changes needed for table {schema_name}.{table_name}", []
        
    except Exception as e:
        try:
            session.rollback()
        except Exception:
            pass
        logger.error(f"Error updating table from data model {intdatamodelid}: {e}")
        return False, f"Error: {str(e)}", []

def _createTableFromDataModel(intdatamodelid: int, session, schema_name: str = 'validus') -> tuple:
    """Create a physical table based on data model definition using ORM
    
    Returns: (success: bool, message: str, sql_statement: str)
    """
    try:
        # Get model master data using ORM
        master = session.query(DataModelMaster).filter(
            DataModelMaster.intdatamodelid == intdatamodelid
        ).first()
        
        if not master:
            return False, "Data model not found", ""
        
        schema_name = schema_name
        table_name = master.vctablename
        model_name = master.vcmodelname
        
        if not table_name:
            return False, "Table name could not be generated from model name", ""
        
        # Get column definitions using ORM
        columns_query = session.query(DataModelDetails).filter(
            DataModelDetails.intdatamodelid == intdatamodelid
        ).order_by(DataModelDetails.intdisplayorder)
        
        columns_rows = columns_query.all()
        
        if not columns_rows:
            return False, "No columns defined for this data model", ""
        
        # Build CREATE TABLE statement
        column_defs = []

        # Append record id and instance id columns intrecid and intinstanceid as bigint and int respectively
        column_defs.extend([
            "intrecid bigint GENERATED ALWAYS AS IDENTITY",
            "intdataloadinstanceid int"
        ])

        for col in columns_rows:
            col_name = col.vcdmcolumnname
            col_fieldname = col.vcfieldname  # Use fieldname as hint for data type
            col_length = col.intlength
            col_precision = col.intprecision
            col_scale = col.intscale
            col_default = col.vcdefaultvalue
            
            if not col_name:
                continue
            
            # Use vcdatatype if available, otherwise infer from field name
            if col.vcdatatype:
                sql_type = _mapDataTypeToSql(col.vcdatatype, col_length, col_precision, col_scale)
            else:
                inferred_type = _inferDataTypeFromFieldName(col_fieldname)
                sql_type = _mapDataTypeToSql(inferred_type, col_length, col_precision, col_scale)
            
            # Build column definition
            col_def = f"{col_name} {sql_type}"
            
            # Add default value if specified
            if col_default:
                if inferred_type in ['Text', 'String']:
                    col_def += f" DEFAULT '{col_default}'"
                else:
                    col_def += f" DEFAULT {col_default}"
            
            column_defs.append(col_def)
        
        # Build CREATE TABLE SQL
        columns_sql = ',\n    '.join(column_defs)
        create_sql = f"""CREATE TABLE IF NOT EXISTS {schema_name}.{table_name} (
        {columns_sql}
        )"""
        
        # Execute CREATE TABLE
        session.execute(text(create_sql))
        session.commit()
        
        logger.info(f"Successfully created table: {schema_name}.{table_name}")
        return True, f"Successfully created table {schema_name}.{table_name} for model '{model_name}'", create_sql
        
    except Exception as e:
        try:
            session.rollback()
        except Exception:
            pass
        logger.error(f"Error creating table from data model {intdatamodelid}: {e}")
        return False, f"Error: {str(e)}", ""

# ==================== Queries ====================

@strawberry.type
class TableSchemaQuery:
    """GraphQL Query root for data model management"""
    
    @strawberry.field
    def dataModels(self, info: Info,
                   intclientid: Optional[int] = None,
                   vcmodelname: Optional[str] = None,
                   limit: Optional[int] = 50,
                   offset: Optional[int] = 0) -> List[DataModelMasterType]:
        """Get all data models with optional filtering - requires authentication"""
        require_authentication(info)
        
        db_manager = get_database_manager()
        session = db_manager.get_session()
        
        try:
            # Build base query
            query = session.query(DataModelMaster)
            
            if intclientid:
                query = query.filter(DataModelMaster.intclientid == intclientid)
            
            if vcmodelname:
                query = query.filter(DataModelMaster.vcmodelname.ilike(f"%{vcmodelname}%"))
            
            query = query.order_by(DataModelMaster.dtcreatedat.desc()).offset(offset).limit(limit)
            models = query.all()
            
            # Get field counts for each model using a subquery
            model_ids = [model.intdatamodelid for model in models]
            field_counts = {}
            if model_ids:
                count_query = session.query(
                    DataModelDetails.intdatamodelid,
                    func.count(DataModelDetails.intdatamodeldetailid).label('count')
                ).filter(
                    DataModelDetails.intdatamodelid.in_(model_ids)
                ).group_by(DataModelDetails.intdatamodelid)
                
                for datamodelid, count in count_query.all():
                    field_counts[datamodelid] = count
            
            # Convert to GraphQL types with field counts
            return [_data_model_master_to_graphql(model, field_counts.get(model.intdatamodelid, 0)) for model in models]
            
        except Exception as e:
            logger.error(f"GraphQL dataModels query error: {e}")
            # Return empty list instead of raising error for queries
            return []
            
        finally:
            session.close()
    
    @strawberry.field
    def dataModelDetails(self, info: Info, intdatamodelid: int) -> Optional[DataModelDetailType]:
        """Get data model with column details - requires authentication"""
        require_authentication(info)
        
        db_manager = get_database_manager()
        session = db_manager.get_session()
        
        try:
            # Get master data using ORM
            master = session.query(DataModelMaster).filter(
                DataModelMaster.intdatamodelid == intdatamodelid
            ).first()
            
            if not master:
                return None
            
            # Get column details using ORM
            columns_query = session.query(DataModelDetails).filter(
                DataModelDetails.intdatamodelid == intdatamodelid
            ).order_by(DataModelDetails.intdisplayorder)
            
            columns = [_data_model_column_to_graphql(col) for col in columns_query.all()]
            
            return DataModelDetailType(
                intdatamodelid=master.intdatamodelid,
                vcmodelname=master.vcmodelname,
                vcdescription=master.vcdescription,
                isactive=master.isactive if master.isactive is not None else True,
                columns=columns
            )
            
        except Exception as e:
            logger.error(f"GraphQL dataModelDetails query error: {e}")
            return None
            
        finally:
            session.close()

    @strawberry.field
    def getAllDataModels(self, info: Info,
                        intclientid: Optional[int] = None,
                        vcmodelname: Optional[str] = None,
                        pageNumber: Optional[int] = 1,
                        pageSize: Optional[int] = 50) -> PaginatedDataModelResponseType:
        """Get all data models with pagination and optional filtering - requires authentication
        
        Args:
            intclientid: Optional client ID filter
            vcmodelname: Optional model name filter (partial match)
            pageNumber: Page number (default: 1)
            pageSize: Number of items per page (default: 20)
        """
        require_authentication(info)

        # Validate and set defaults for pagination parameters
        if pageNumber is None or pageNumber < 1:
            pageNumber = 1
        if pageSize is None or pageSize < 1:
            pageSize = 20

        try:
            db_manager = get_database_manager()
            session = db_manager.get_session()
            
            try:
                # Build base query
                query = session.query(DataModelMaster)
                
                if intclientid is not None:
                    query = query.filter(DataModelMaster.intclientid == intclientid)
                
                if vcmodelname:
                    query = query.filter(DataModelMaster.vcmodelname.ilike(f"%{vcmodelname}%"))
                
                # Get total count before pagination
                totalCount = query.count()
                
                # Apply pagination
                currentPage = pageNumber
                pageSizeValue = pageSize
                offset = (currentPage - 1) * pageSizeValue
                
                paginatedQuery = query.order_by(DataModelMaster.dtcreatedat.desc()).offset(offset).limit(pageSizeValue)
                models = paginatedQuery.all()
                
                # Sort models by intdatamodelid ascending before sending
                models_sorted = sorted(models, key=lambda x: x.intdatamodelid)
                
                # Get field counts for each model using a subquery
                model_ids = [model.intdatamodelid for model in models_sorted]
                field_counts = {}
                if model_ids:
                    count_query = session.query(
                        DataModelDetails.intdatamodelid,
                        func.count(DataModelDetails.intdatamodeldetailid).label('count')
                    ).filter(
                        DataModelDetails.intdatamodelid.in_(model_ids)
                    ).group_by(DataModelDetails.intdatamodelid)
                    
                    for datamodelid, count in count_query.all():
                        field_counts[datamodelid] = count
                
                # Calculate total pages
                totalPages = (totalCount + pageSizeValue - 1) // pageSizeValue if totalCount > 0 else 0
                
                # Build response
                return PaginatedDataModelResponseType(
                    dataModels=[_data_model_master_to_graphql(model, field_counts.get(model.intdatamodelid, 0)) for model in models_sorted],
                    pagination=PaginationInfoType(
                        pageNumber=currentPage,
                        pageSize=pageSizeValue,
                        currentPage=currentPage,
                        totalPages=totalPages,
                        totalCount=totalCount
                    )
                )
            finally:
                session.close()
                
        except Exception as e:
            logger.error(f"Error fetching data models: {str(e)}")
            raise GraphQLError(get_error_message('DATABASE_ERROR'))

# ==================== Mutations ====================

@strawberry.type
class TableSchemaMutation:
    """GraphQL Mutation root for data model management"""
    
    @strawberry.field
    def createDataModel(self, info: Info, 
                       master: DataModelMasterInput,
                       columns: List[DataModelColumnInput]) -> Optional[DataModelDetailType]:
        """Create a new data model with columns - requires authentication"""
        user = require_authentication(info)
        
        db_manager = get_database_manager()
        session = db_manager.get_session()
        
        try:
            user_id = user.get('id', 0)
            
            # Import text for BIT conversion
            from sqlalchemy import text
            
            # Check for duplicate data model name
            if master.vcmodelname:
                if check_data_model_name_duplicate(session, master.vcmodelname):
                    raise GraphQLError(get_error_message('DATA_MODEL_NAME_DUPLICATE'))
            
            # Auto-generate vctablename if not provided
            vctablename = master.vctablename
            if not vctablename:
                vctablename = _generate_tablename_from_modelname(master.vcmodelname)
            
            # Create master record using ORM
            new_master = DataModelMaster(
                vcmodelname=master.vcmodelname,
                vcdescription=master.vcdescription,
                vcmodelid=master.vcmodelid,
                vccategory=master.vccategory,
                vcsource=master.vcsource,
                vctablename=vctablename,
                isactive=master.isactive if master.isactive is not None else True,
                intcreatedby=user_id
            )
            
            session.add(new_master)
            session.flush()  # Get the ID without committing
            
            # Generate column names and validate duplicates before creating
            column_names_to_check = []
            field_names_to_check = []
            
            for col in columns:
                # Auto-generate vcdmcolumnname if not provided
                if not col.vcdmcolumnname:
                    col.vcdmcolumnname = _generate_columnname_from_fieldname(col.vcfieldname)
                
                column_names_to_check.append(col.vcdmcolumnname)
                field_names_to_check.append(col.vcfieldname)
            
            # Check for duplicate column names
            duplicate_column = _check_duplicate_column_names(
                session,
                new_master.intdatamodelid,
                column_names_to_check
            )
            if duplicate_column:
                raise GraphQLError(f"Column name '{duplicate_column}' already exists in this data model")
            
            # Check for duplicate field names
            duplicate_field = _check_duplicate_field_names(
                session,
                new_master.intdatamodelid,
                field_names_to_check
            )
            if duplicate_field:
                raise GraphQLError(f"Field name '{duplicate_field}' already exists in this data model")
            
            # Create column details using ORM
            for col in columns:
                new_detail = DataModelDetails(
                    intdatamodelid=new_master.intdatamodelid,
                    vcfieldname=col.vcfieldname,
                    vcfielddescription=col.vcfielddescription,
                    vcdatatype=col.vcdatatype,
                    intlength=col.intlength,
                    intprecision=col.intprecision,
                    intscale=col.intscale,
                    vcdateformat=col.vcdateformat,
                    vcdmcolumnname=col.vcdmcolumnname,
                    vcdefaultvalue=col.vcdefaultvalue,
                    ismandatory=col.ismandatory,
                    intdisplayorder=col.intdisplayorder,
                    intcreatedby=user_id
                )
                
                session.add(new_detail)
            
            session.commit()
            
            # Create tables in all client schemas (only if schema exists)
            try:
                # Get all client schemas
                client_schemas = _get_all_client_schemas(session)
                
                # Create tables in each schema (only if schema exists)
                for schema_name in client_schemas:
                    try:
                        # Check if schema exists before creating table
                        if not _schema_exists(session, schema_name):
                            logger.info(f"Skipping schema {schema_name} - schema does not exist")
                            continue
                        
                        # Create table in this schema
                        success, message, sql_statement = _createTableFromDataModel(
                            new_master.intdatamodelid,
                            session,
                            schema_name
                        )
                        if success:
                            logger.info(f"Created table for data model {new_master.intdatamodelid} in schema {schema_name}")
                        else:
                            logger.warning(f"Failed to create table for data model {new_master.intdatamodelid} in schema {schema_name}: {message}")
                    except Exception as e:
                        logger.error(f"Error creating table in schema {schema_name} for data model {new_master.intdatamodelid}: {e}")
                        # Continue with other schemas even if one fails
                        continue
            except Exception as e:
                logger.error(f"Error creating tables in client schemas: {e}")
                # Don't fail the mutation if table creation fails
            
            # Return the created model with details
            query_instance = TableSchemaQuery()
            return query_instance.dataModelDetails(info, new_master.intdatamodelid)
            
        except GraphQLError:
            session.rollback()
            raise
        except Exception as e:
            session.rollback()
            logger.error(f"GraphQL createDataModel error: {e}")
            error_msg = handle_database_error(e, 'create data model')
            raise GraphQLError(error_msg)
            
        finally:
            session.close()
    
    @strawberry.field
    def createPhysicalTable(self, info: Info, intdatamodelid: int) -> TableCreationResultType:
        """Create actual database table from data model - requires admin role"""
        user = require_role('admin')
        
        db_manager = get_database_manager()
        session = db_manager.get_session()
        
        try:
            success, message, sql_statement = _createTableFromDataModel(intdatamodelid, session)
            
            if success:
                # Get schema and table name for response using ORM
                master = session.query(DataModelMaster).filter(
                    DataModelMaster.intdatamodelid == intdatamodelid
                ).first()
                
                table_name = f"tbl_{master.vcmodelname.lower().replace(' ', '_')}" if master and master.vcmodelname else None
                
                return TableCreationResultType(
                    success=True,
                    message=message,
                    table_name=table_name,
                    schema_name='validus',
                    sql_statement=sql_statement
                )
            else:
                return TableCreationResultType(
                    success=False,
                    message=message,
                    table_name=None,
                    schema_name=None,
                    sql_statement=None
                )
            
        except Exception as e:
            logger.error(f"GraphQL createPhysicalTable error: {e}")
            return TableCreationResultType(
                success=False,
                message=f"Error: {str(e)}",
                table_name=None,
                schema_name=None,
                sql_statement=None
            )
            
        finally:
            session.close()
    
    @strawberry.field
    def deleteDataModel(self, info: Info, intdatamodelid: int) -> bool:
        """Delete a data model and its columns - requires admin role"""
        user = require_role('admin')
        
        db_manager = get_database_manager()
        session = db_manager.get_session()
        
        try:
            # Get the model to delete
            model = session.query(DataModelMaster).filter(
                DataModelMaster.intdatamodelid == intdatamodelid
            ).first()
            
            if not model:
                raise GraphQLError(get_error_message('DATA_MODEL_NOT_FOUND'))
            
            # Delete using ORM (cascade will handle details)
            session.delete(model)
            session.commit()
            return True
            
        except GraphQLError:
            session.rollback()
            raise
        except Exception as e:
            session.rollback()
            logger.error(f"GraphQL deleteDataModel error: {e}")
            error_msg = handle_database_error(e, 'delete data model')
            raise GraphQLError(error_msg)
            
        finally:
            session.close()
    
    @strawberry.field
    def updateDataModelComplete(self, info: Info, input: UpdateDataModelCompleteInput) -> Optional[DataModelDetailType]:
        """Update data model master and columns together - requires authentication"""
        user = require_authentication(info)
        
        db_manager = get_database_manager()
        session = db_manager.get_session()
        
        try:
            user_id = user.get('id', 0)
            
            # Get existing data model
            master = session.query(DataModelMaster).filter(
                DataModelMaster.intdatamodelid == input.intdatamodelid
            ).first()
            
            if not master:
                raise GraphQLError(get_error_message('DATA_MODEL_NOT_FOUND'))
            
            # Update master fields
            if input.vcmodelname is not None:
                # Check for duplicate data model name (excluding current data model)
                if check_data_model_name_duplicate(
                    session,
                    input.vcmodelname,
                    exclude_datamodel_id=input.intdatamodelid
                ):
                    raise GraphQLError(get_error_message('DATA_MODEL_NAME_DUPLICATE'))
                
                master.vcmodelname = input.vcmodelname
                # Auto-update vctablename if vcmodelname is changed (unless explicitly provided)
                if input.vctablename is None:
                    master.vctablename = _generate_tablename_from_modelname(input.vcmodelname)
            
            if input.vcdescription is not None:
                master.vcdescription = input.vcdescription
            if input.vcmodelid is not None:
                master.vcmodelid = input.vcmodelid
            if input.vccategory is not None:
                master.vccategory = input.vccategory
            if input.vcsource is not None:
                master.vcsource = input.vcsource
            if input.vctablename is not None:
                master.vctablename = input.vctablename
            if input.isactive is not None:
                master.isactive = input.isactive
            
            master.intupdatedby = user_id
            master.dtupdatedat = datetime.now()
            
            # Get old columns before update (for table modification)
            old_columns = session.query(DataModelDetails).filter(
                DataModelDetails.intdatamodelid == input.intdatamodelid
            ).all()
            
            # Collect all column names and field names for duplicate checking
            update_column_names = []
            update_field_names = []
            update_detail_ids = []
            new_column_names = []
            new_field_names = []
            
            # Handle column updates
            if input.update_columns:
                for column_update in input.update_columns:
                    column = session.query(DataModelDetails).filter(
                        DataModelDetails.intdatamodeldetailid == column_update.intdatamodeldetailid,
                        DataModelDetails.intdatamodelid == input.intdatamodelid
                    ).first()
                    if column:
                        update_detail_ids.append(column.intdatamodeldetailid)
                        
                        # Determine final column name and field name after update
                        final_column_name = column_update.vcdmcolumnname if column_update.vcdmcolumnname is not None else column.vcdmcolumnname
                        final_field_name = column_update.vcfieldname if column_update.vcfieldname is not None else column.vcfieldname
                        
                        # If field name changed but column name not provided, regenerate column name
                        if column_update.vcfieldname is not None and column_update.vcdmcolumnname is None:
                            final_column_name = _generate_columnname_from_fieldname(column_update.vcfieldname)
                        
                        update_column_names.append(final_column_name)
                        update_field_names.append(final_field_name)
            
            # Handle new columns - generate column names first
            if input.new_columns:
                for column_input in input.new_columns:
                    # Auto-generate vcdmcolumnname if not provided
                    if not column_input.vcdmcolumnname:
                        column_input.vcdmcolumnname = _generate_columnname_from_fieldname(column_input.vcfieldname)
                    
                    new_column_names.append(column_input.vcdmcolumnname)
                    new_field_names.append(column_input.vcfieldname)
            
            # Check for duplicate column names (combining updates and new columns)
            all_column_names = update_column_names + new_column_names
            if all_column_names:
                duplicate_column = _check_duplicate_column_names(
                    session,
                    input.intdatamodelid,
                    all_column_names,
                    exclude_detail_ids=update_detail_ids if update_detail_ids else None
                )
                if duplicate_column:
                    raise GraphQLError(f"Column name '{duplicate_column}' already exists in this data model")
            
            # Check for duplicate field names (combining updates and new columns)
            all_field_names = update_field_names + new_field_names
            if all_field_names:
                duplicate_field = _check_duplicate_field_names(
                    session,
                    input.intdatamodelid,
                    all_field_names,
                    exclude_detail_ids=update_detail_ids if update_detail_ids else None
                )
                if duplicate_field:
                    raise GraphQLError(f"Field name '{duplicate_field}' already exists in this data model")
            
            # Now perform the actual updates
            if input.update_columns:
                for column_update in input.update_columns:
                    column = session.query(DataModelDetails).filter(
                        DataModelDetails.intdatamodeldetailid == column_update.intdatamodeldetailid,
                        DataModelDetails.intdatamodelid == input.intdatamodelid
                    ).first()
                    if column:
                        # Store old datatype for validation and cleanup
                        old_datatype = column.vcdatatype
                        new_datatype = column_update.vcdatatype
                        
                        # Validate datatype change if datatype is being updated
                        if new_datatype is not None and old_datatype != new_datatype:
                            is_valid, error_message = _validate_datatype_change(old_datatype, new_datatype)
                            if not is_valid:
                                raise GraphQLError(error_message)
                        
                        if column_update.vcfieldname is not None:
                            column.vcfieldname = column_update.vcfieldname
                            # If column name not explicitly provided, regenerate from field name
                            if column_update.vcdmcolumnname is None:
                                column.vcdmcolumnname = _generate_columnname_from_fieldname(column_update.vcfieldname)
                        if column_update.vcfielddescription is not None:
                            column.vcfielddescription = column_update.vcfielddescription
                        if column_update.vcdatatype is not None:
                            # Clean up old datatype precision before updating
                            if old_datatype != new_datatype:
                                _cleanup_old_datatype_precision(column, old_datatype, new_datatype)
                            column.vcdatatype = column_update.vcdatatype
                            
                            # After updating datatype, ensure precision/scale are cleared for Text type
                            new_dt_normalized = new_datatype.strip().lower()
                            if new_dt_normalized in ['text', 'alphanumeric', 'string']:
                                column.intprecision = None
                                column.intscale = None
                        
                        if column_update.intlength is not None:
                            column.intlength = column_update.intlength
                        if column_update.intprecision is not None:
                            column.intprecision = 32
                        if column_update.intscale is not None:
                            column.intscale = column_update.intscale
                        if column_update.vcdateformat is not None:
                            column.vcdateformat = column_update.vcdateformat
                        if column_update.vcdmcolumnname is not None:
                            column.vcdmcolumnname = column_update.vcdmcolumnname
                        if column_update.vcdefaultvalue is not None:
                            column.vcdefaultvalue = column_update.vcdefaultvalue
                        if column_update.ismandatory is not None:
                            column.ismandatory = column_update.ismandatory
                        if column_update.intdisplayorder is not None:
                            column.intdisplayorder = column_update.intdisplayorder
                            
                        
                        column.intupdatedby = user_id
                        column.dtupdatedat = datetime.now()
                    else:
                        logger.warning(f"Data model detail {column_update.intdatamodeldetailid} not found for model {input.intdatamodelid}")
            
            # Handle new columns
            if input.new_columns:
                for column_input in input.new_columns:
                    new_column = DataModelDetails(
                        intdatamodelid=input.intdatamodelid,
                        vcfieldname=column_input.vcfieldname,
                        vcfielddescription=column_input.vcfielddescription,
                        vcdatatype=column_input.vcdatatype,
                        intlength=column_input.intlength,
                        intprecision=column_input.intprecision,
                        intscale=column_input.intscale,
                        vcdateformat=column_input.vcdateformat,
                        vcdmcolumnname=column_input.vcdmcolumnname,
                        vcdefaultvalue=column_input.vcdefaultvalue,
                        ismandatory=column_input.ismandatory,
                        intdisplayorder=column_input.intdisplayorder,
                        intcreatedby=user_id
                    )
                    session.add(new_column)
            
            # Handle column deletions
            if input.delete_column_ids:
                for column_id in input.delete_column_ids:
                    column = session.query(DataModelDetails).filter(
                        DataModelDetails.intdatamodeldetailid == column_id
                    ).first()
                    if column:
                        session.delete(column)
            
            session.commit()
            
            # Update tables in all client schemas (only if schema exists)
            try:
                # Get new columns after update
                new_columns = session.query(DataModelDetails).filter(
                    DataModelDetails.intdatamodelid == input.intdatamodelid
                ).all()
                
                # Get deleted column IDs
                deleted_column_ids = input.delete_column_ids or []
                
                # Get all client schemas
                client_schemas = _get_all_client_schemas(session)
                
                # Update tables in each schema (only if schema exists)
                for schema_name in client_schemas:
                    try:
                        # Check if schema exists before updating table
                        if not _schema_exists(session, schema_name):
                            logger.info(f"Skipping schema {schema_name} - schema does not exist")
                            continue
                        
                        # Update table in this schema
                        success, message, sql_statements = _updateTableFromDataModel(
                            input.intdatamodelid,
                            session,
                            schema_name,
                            old_columns,
                            new_columns,
                            deleted_column_ids
                        )
                        if success:
                            logger.info(f"Updated table for data model {input.intdatamodelid} in schema {schema_name}: {message}")
                        else:
                            logger.warning(f"Failed to update table for data model {input.intdatamodelid} in schema {schema_name}: {message}")
                    except Exception as e:
                        logger.error(f"Error updating table in schema {schema_name} for data model {input.intdatamodelid}: {e}")
                        # Continue with other schemas even if one fails
                        continue
            except Exception as e:
                logger.error(f"Error updating tables in client schemas: {e}")
                # Don't fail the mutation if table update fails
            
            # Return the updated model with details
            query_instance = TableSchemaQuery()
            return query_instance.dataModelDetails(info, input.intdatamodelid)
            
        except GraphQLError:
            session.rollback()
            raise
        except Exception as e:
            session.rollback()
            logger.error(f"GraphQL updateDataModelComplete error: {e}")
            error_msg = handle_database_error(e, 'update data model')
            raise GraphQLError(error_msg)
            
        finally:
            session.close()



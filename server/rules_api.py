"""
Rules API
Provides CRUD endpoints for rules with pagination and search support.
Includes regex and pattern matching logic for rule evaluation.
"""
import logging
import json
import re
import fnmatch
from fastapi import APIRouter, HTTPException, Depends, Query, Body
from typing import Optional, List, Dict, Any
from sqlalchemy import text, and_, or_
from datetime import datetime
from database_models import DatabaseManager
from rbac.utils.auth import getCurrentUser
from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/rules", tags=["Rules API"])


# Utility functions for pattern matching
def _convert_wildcard_to_regex(pattern: str) -> str:
    """
    Convert wildcard pattern (*, ?) to regex pattern
    * matches any sequence of characters
    ? matches any single character
    """
    if not pattern:
        return None
    
    # Escape special regex characters except * and ?
    pattern = re.escape(pattern)
    # Replace escaped \* with .* (match any characters)
    pattern = pattern.replace(r'\*', '.*')
    # Replace escaped \? with . (match single character)
    pattern = pattern.replace(r'\?', '.')
    # Anchor to start and end
    return f"^{pattern}$"


def _match_pattern(pattern: str, text: str, is_regex: bool) -> bool:
    """
    Match text against pattern using either regex or wildcard matching
    
    Args:
        pattern: The pattern to match against
        text: The text to match
        is_regex: If True, treat pattern as regex; if False, treat as wildcard
    
    Returns:
        True if pattern matches text, False otherwise
    """
    if not pattern or not text:
        return False
    
    try:
        if is_regex:
            # Use regex matching
            return bool(re.search(pattern, text, re.IGNORECASE))
        else:
            # Use wildcard matching (fnmatch style)
            # Convert wildcard to regex for consistent matching
            regex_pattern = _convert_wildcard_to_regex(pattern)
            if regex_pattern:
                return bool(re.search(regex_pattern, text, re.IGNORECASE))
            return False
    except re.error as e:
        logger.warning(f"Invalid regex pattern '{pattern}': {e}")
        return False
    except Exception as e:
        logger.error(f"Error matching pattern '{pattern}' against '{text}': {e}")
        return False


def _transform_to_array_fields(
    file_name_pattern: Optional[str],
    file_extension: Optional[str],
    sender_address: Optional[str],
    subject: Optional[str],
    email_body: Optional[str]
) -> tuple:
    """
    Transform single string values to array format for multi-select fields.
    Returns tuple: (fileNamePatterns, senderAddresses, subjects, keywords)
    """
    fileNamePatterns = []
    senderAddresses = []
    subjects = []
    keywords = []
    
    # Handle file_name_pattern and file_extension
    # Check if file_name_pattern is JSON array
    if file_name_pattern:
        try:
            pattern_data = json.loads(file_name_pattern)
            if isinstance(pattern_data, list):
                for idx, item in enumerate(pattern_data):
                    if isinstance(item, dict):
                        fileNamePatterns.append(FileNamePatternItem(
                            id=item.get('id', idx + 1),
                            fileNamePattern=item.get('fileNamePattern', item.get('file_name_pattern', '')),
                            fileExtension=item.get('fileExtension', item.get('file_extension', file_extension))
                        ))
                    else:
                        fileNamePatterns.append(FileNamePatternItem(
                            id=idx + 1,
                            fileNamePattern=str(item),
                            fileExtension=file_extension
                        ))
            else:
                # Single value - convert to array
                fileNamePatterns.append(FileNamePatternItem(
                    id=1,
                    fileNamePattern=file_name_pattern,
                    fileExtension=file_extension
                ))
        except (json.JSONDecodeError, TypeError):
            # Not JSON, treat as single value
            fileNamePatterns.append(FileNamePatternItem(
                id=1,
                fileNamePattern=file_name_pattern,
                fileExtension=file_extension
            ))
    
    # Handle sender_address
    if sender_address:
        try:
            address_data = json.loads(sender_address)
            if isinstance(address_data, list):
                for idx, item in enumerate(address_data):
                    if isinstance(item, dict):
                        senderAddresses.append(SenderAddressItem(
                            id=item.get('id', idx + 1),
                            senderAddress=item.get('senderAddress', item.get('sender_address', ''))
                        ))
                    else:
                        senderAddresses.append(SenderAddressItem(
                            id=idx + 1,
                            senderAddress=str(item)
                        ))
            else:
                senderAddresses.append(SenderAddressItem(
                    id=1,
                    senderAddress=sender_address
                ))
        except (json.JSONDecodeError, TypeError):
            senderAddresses.append(SenderAddressItem(
                id=1,
                senderAddress=sender_address
            ))
    
    # Handle subject
    if subject:
        try:
            subject_data = json.loads(subject)
            if isinstance(subject_data, list):
                for idx, item in enumerate(subject_data):
                    if isinstance(item, dict):
                        subjects.append(SubjectItem(
                            id=item.get('id', idx + 1),
                            subject=item.get('subject', '')
                        ))
                    else:
                        subjects.append(SubjectItem(
                            id=idx + 1,
                            subject=str(item)
                        ))
            else:
                subjects.append(SubjectItem(
                    id=1,
                    subject=subject
                ))
        except (json.JSONDecodeError, TypeError):
            subjects.append(SubjectItem(
                id=1,
                subject=subject
            ))
    
    # Handle email_body (keywords)
    if email_body:
        try:
            keyword_data = json.loads(email_body)
            if isinstance(keyword_data, list):
                for idx, item in enumerate(keyword_data):
                    if isinstance(item, dict):
                        keywords.append(KeywordItem(
                            id=item.get('id', idx + 1),
                            keyword=item.get('keyword', item.get('email_body', ''))
                        ))
                    else:
                        keywords.append(KeywordItem(
                            id=idx + 1,
                            keyword=str(item)
                        ))
            else:
                keywords.append(KeywordItem(
                    id=1,
                    keyword=email_body
                ))
        except (json.JSONDecodeError, TypeError):
            keywords.append(KeywordItem(
                id=1,
                keyword=email_body
            ))
    
    return (
        fileNamePatterns if fileNamePatterns else None,
        senderAddresses if senderAddresses else None,
        subjects if subjects else None,
        keywords if keywords else None
    )


def _rule_matches_file(rule: Dict[str, Any], file_name: str, sender_address: Optional[str] = None,
                       subject: Optional[str] = None, email_body: Optional[str] = None,
                       document_type: Optional[str] = None) -> bool:
    """
    Check if a rule matches the given file/document attributes
    
    Args:
        rule: Rule dictionary from database
        file_name: File name to match
        sender_address: Sender email address (optional)
        subject: Email subject (optional)
        email_body: Email body text (optional)
        document_type: Document type (optional)
    
    Returns:
        True if rule matches, False otherwise
    """
    # Check document_type if specified in rule
    if rule.get('document_type'):
        if not document_type or rule['document_type'].lower() != document_type.lower():
            return False
    
    # Check file_name pattern
    if rule.get('file_name'):
        if not _match_pattern(rule['file_name'], file_name, rule.get('is_regex', False)):
            return False
    
    # Check sender_address if specified
    if rule.get('sender_address'):
        if not sender_address or not _match_pattern(
            rule['sender_address'], sender_address, rule.get('is_regex', False)
        ):
            return False
    
    # Check subject if specified
    if rule.get('subject'):
        if not subject or not _match_pattern(
            rule['subject'], subject, rule.get('is_regex', False)
        ):
            return False
    
    # Check email_body if specified
    if rule.get('email_body'):
        if not email_body or not _match_pattern(
            rule['email_body'], email_body, rule.get('is_regex', False)
        ):
            return False
    
    return True


# Pydantic models for request/response
class RuleCreateRequest(BaseModel):
    rule_name: Optional[str] = Field(None, description="Name of the rule")
    rule_description: Optional[str] = Field(None, description="Description of the rule")
    type: Optional[str] = Field(None, description="Rule type: 'classification' or 'ignore'")
    status: Optional[str] = Field(None, description="Rule status")
    document_type: Optional[str] = Field(None, description="Document type")
    file_name: Optional[str] = Field(None, description="File name pattern")
    file_name_pattern: Optional[str] = Field(None, description="Pattern for matching file names")
    file_extension: Optional[str] = Field(None, description="File extension filter")
    sender_address: Optional[str] = Field(None, description="Sender email address")
    subject: Optional[str] = Field(None, description="Email subject pattern")
    email_body: Optional[str] = Field(None, description="Email body pattern")
    source: Optional[str] = Field(None, description="Source: 'sftp', 'mail', 'portal', 'manual', 'api_invoke'")
    is_regex: bool = Field(False, description="Whether the rule uses regex")
    metadata: Optional[Dict[str, Any]] = Field(None, description="Additional metadata as JSON")


class RuleUpdateRequest(BaseModel):
    rule_name: Optional[str] = None
    rule_description: Optional[str] = None
    type: Optional[str] = None
    status: Optional[str] = None
    document_type: Optional[str] = None
    file_name: Optional[str] = None
    file_name_pattern: Optional[str] = None
    file_extension: Optional[str] = None
    sender_address: Optional[str] = None
    subject: Optional[str] = None
    email_body: Optional[str] = None
    source: Optional[str] = None
    is_regex: Optional[bool] = None
    metadata: Optional[Dict[str, Any]] = None


# Nested models for array fields
class FileNamePatternItem(BaseModel):
    id: int
    fileNamePattern: str
    fileExtension: Optional[str] = None

class SenderAddressItem(BaseModel):
    id: int
    senderAddress: str

class SubjectItem(BaseModel):
    id: int
    subject: str

class KeywordItem(BaseModel):
    id: int
    keyword: str

class RuleResponse(BaseModel):
    id: int
    rule_id: Optional[str]
    rule_name: Optional[str]
    rule_description: Optional[str]
    type: Optional[str]
    status: Optional[str]
    document_type: Optional[str]
    file_name: Optional[str]
    file_name_pattern: Optional[str]  # Keep for backward compatibility
    file_extension: Optional[str]  # Keep for backward compatibility
    sender_address: Optional[str]  # Keep for backward compatibility
    subject: Optional[str]  # Keep for backward compatibility
    email_body: Optional[str]  # Keep for backward compatibility
    source: Optional[str]
    is_regex: bool
    metadata: Optional[Dict[str, Any]]
    created_at: datetime
    updated_at: datetime
    created_by: Optional[int]
    # New array fields
    fileNamePatterns: Optional[List[FileNamePatternItem]] = None
    senderAddresses: Optional[List[SenderAddressItem]] = None
    subjects: Optional[List[SubjectItem]] = None
    keywords: Optional[List[KeywordItem]] = None


class RuleListResponse(BaseModel):
    data: List[RuleResponse]
    total: int
    page: int
    page_size: int
    total_pages: int


async def authenticate_user(username: str = Depends(getCurrentUser)):
    """Authenticate user and return username"""
    return username


def _build_search_conditions(search: Optional[str], params: Dict[str, Any]) -> str:
    """Build SQL WHERE conditions for search"""
    if not search:
        return ""
    
    conditions = """
        AND (
            COALESCE(rule_name, '') ILIKE :search
            OR COALESCE(rule_description, '') ILIKE :search
            OR COALESCE(document_type, '') ILIKE :search
            OR COALESCE(file_name, '') ILIKE :search
            OR COALESCE(file_name_pattern, '') ILIKE :search
            OR COALESCE(file_extension, '') ILIKE :search
            OR COALESCE(sender_address, '') ILIKE :search
            OR COALESCE(subject, '') ILIKE :search
            OR COALESCE(email_body, '') ILIKE :search
            OR COALESCE(status, '') ILIKE :search
        )
    """
    params['search'] = f"%{search}%"
    return conditions


def _build_filter_conditions(
    type_filter: Optional[str],
    status_filter: Optional[str],
    source_filter: Optional[str],
    document_type_filter: Optional[str],
    params: Dict[str, Any]
) -> str:
    """Build SQL WHERE conditions for filters"""
    conditions = []
    
    if type_filter:
        conditions.append("type = :type_filter")
        params['type_filter'] = type_filter
    
    if status_filter:
        conditions.append("status = :status_filter")
        params['status_filter'] = status_filter
    
    if source_filter:
        conditions.append("source = :source_filter")
        params['source_filter'] = source_filter
    
    if document_type_filter:
        conditions.append("document_type = :document_type_filter")
        params['document_type_filter'] = document_type_filter
    
    if conditions:
        return " AND " + " AND ".join(conditions)
    return ""


@router.get("/", response_model=RuleListResponse)
async def get_rules(
    page: int = Query(1, ge=1, description="Page number"),
    page_size: int = Query(50, ge=1, le=100, description="Items per page"),
    search: Optional[str] = Query(None, description="Search term for document_type, file_name, sender_address, subject, email_body, status"),
    type_filter: Optional[str] = Query(None, description="Filter by type: 'classification' or 'ignore'"),
    status_filter: Optional[str] = Query(None, description="Filter by status"),
    source_filter: Optional[str] = Query(None, description="Filter by source: 'sftp', 'mail', 'portal', 'manual', 'api_invoke'"),
    document_type_filter: Optional[str] = Query(None, description="Filter by document type"),
    __username: str = Depends(authenticate_user)
):
    """
    Get all rules with pagination and search support.
    
    Supports:
    - Pagination (page, page_size)
    - Search across multiple fields
    - Filtering by type, status, source, document_type
    """
    try:
        db_manager = DatabaseManager()
        engine = db_manager.engine
        
        params = {}
        
        # Build base query
        sql_base = """
            SELECT 
                id,
                rule_id,
                rule_name,
                rule_description,
                type,
                status,
                document_type,
                file_name,
                file_name_pattern,
                file_extension,
                sender_address,
                subject,
                email_body,
                source,
                is_regex,
                metadata,
                created_at,
                updated_at,
                created_by
            FROM public.rules
            WHERE 1=1
        """
        
        # Add search conditions
        sql_base += _build_search_conditions(search, params)
        
        # Add filter conditions
        sql_base += _build_filter_conditions(
            type_filter, status_filter, source_filter, document_type_filter, params
        )
        
        # Count total records
        count_sql = f"SELECT COUNT(*) FROM ({sql_base}) as count_query"
        with engine.connect() as conn:
            result = conn.execute(text(count_sql), params)
            total_count = result.scalar()
        
        # Add pagination
        sql = sql_base + " ORDER BY created_at DESC OFFSET :offset LIMIT :limit"
        params['offset'] = (page - 1) * page_size
        params['limit'] = page_size
        
        # Execute query
        rules = []
        with engine.connect() as conn:
            result = conn.execute(text(sql), params)
            for row in result:
                rules.append(RuleResponse(
                    id=row.id,
                    rule_id=row.rule_id,
                    rule_name=row.rule_name,
                    rule_description=row.rule_description,
                    type=row.type,
                    status=row.status,
                    document_type=row.document_type,
                    file_name=row.file_name,
                    file_name_pattern=row.file_name_pattern,
                    file_extension=row.file_extension,
                    sender_address=row.sender_address,
                    subject=row.subject,
                    email_body=row.email_body,
                    source=row.source,
                    is_regex=row.is_regex,
                    metadata=row.metadata,
                    created_at=row.created_at,
                    updated_at=row.updated_at,
                    created_by=row.created_by
                ))
        
        total_pages = (total_count + page_size - 1) // page_size
        
        return RuleListResponse(
            data=rules,
            total=total_count,
            page=page,
            page_size=page_size,
            total_pages=total_pages
        )
    
    except Exception as e:
        logger.error(f"Error fetching rules: {e}")
        raise HTTPException(status_code=500, detail=f"Error fetching rules: {str(e)}")


@router.get("/{rule_id}", response_model=RuleResponse)
async def get_rule(
    rule_id: int,
    __username: str = Depends(authenticate_user)
):
    """Get a single rule by ID"""
    try:
        db_manager = DatabaseManager()
        engine = db_manager.engine
        
        sql = """
            SELECT 
                id,
                rule_id,
                rule_name,
                rule_description,
                type,
                status,
                document_type,
                file_name,
                file_name_pattern,
                file_extension,
                sender_address,
                subject,
                email_body,
                source,
                is_regex,
                metadata,
                created_at,
                updated_at,
                created_by
            FROM public.rules
            WHERE id = :rule_id
        """
        
        with engine.connect() as conn:
            result = conn.execute(text(sql), {"rule_id": rule_id})
            row = result.fetchone()
            
            if not row:
                raise HTTPException(status_code=404, detail="Rule not found")
            
            # Transform single values to arrays
            fileNamePatterns, senderAddresses, subjects, keywords = _transform_to_array_fields(
                row.file_name_pattern,
                row.file_extension,
                row.sender_address,
                row.subject,
                row.email_body
            )
            
            return RuleResponse(
                id=row.id,
                rule_id=row.rule_id,
                rule_name=row.rule_name,
                rule_description=row.rule_description,
                type=row.type,
                status=row.status,
                document_type=row.document_type,
                file_name=row.file_name,
                file_name_pattern=row.file_name_pattern,
                file_extension=row.file_extension,
                sender_address=row.sender_address,
                subject=row.subject,
                email_body=row.email_body,
                source=row.source,
                is_regex=row.is_regex,
                metadata=row.metadata,
                created_at=row.created_at,
                updated_at=row.updated_at,
                created_by=row.created_by,
                fileNamePatterns=fileNamePatterns,
                senderAddresses=senderAddresses,
                subjects=subjects,
                keywords=keywords
            )
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching rule {rule_id}: {e}")
        raise HTTPException(status_code=500, detail=f"Error fetching rule: {str(e)}")


@router.post("/", response_model=RuleResponse, status_code=201)
async def create_rule(
    rule: RuleCreateRequest,
    __username: str = Depends(authenticate_user)
):
    """Create a new rule"""
    try:
        db_manager = DatabaseManager()
        engine = db_manager.engine
        
        # Get user ID from username
        user_sql = "SELECT id FROM public.users WHERE username = :username"
        with engine.connect() as conn:
            user_result = conn.execute(text(user_sql), {"username": __username})
            user_row = user_result.fetchone()
            created_by = user_row[0] if user_row else None
        
        # Insert rule
        insert_sql = """
            INSERT INTO public.rules (
                rule_name, rule_description, type, status, document_type, file_name, 
                file_name_pattern, file_extension, sender_address,
                subject, email_body, source, is_regex, metadata, created_by
            )
            VALUES (
                :rule_name, :rule_description, :type, :status, :document_type, :file_name, 
                :file_name_pattern, :file_extension, :sender_address,
                :subject, :email_body, :source, :is_regex, 
                CAST(:metadata AS jsonb), :created_by
            )
            RETURNING 
                id, rule_id, rule_name, rule_description, type, status, document_type, 
                file_name, file_name_pattern, file_extension, sender_address,
                subject, email_body, source, is_regex, metadata,
                created_at, updated_at, created_by
        """
        
        params = {
            "rule_name": rule.rule_name,
            "rule_description": rule.rule_description,
            "type": rule.type,
            "status": rule.status,
            "document_type": rule.document_type,
            "file_name": rule.file_name,
            "file_name_pattern": rule.file_name_pattern,
            "file_extension": rule.file_extension,
            "sender_address": rule.sender_address,
            "subject": rule.subject,
            "email_body": rule.email_body,
            "source": rule.source,
            "is_regex": rule.is_regex,
            "metadata": json.dumps(rule.metadata) if rule.metadata else None,
            "created_by": created_by
        }
        
        with engine.connect() as conn:
            result = conn.execute(text(insert_sql), params)
            conn.commit()
            row = result.fetchone()
            
            return RuleResponse(
                id=row.id,
                rule_id=row.rule_id,
                rule_name=row.rule_name,
                rule_description=row.rule_description,
                type=row.type,
                status=row.status,
                document_type=row.document_type,
                file_name=row.file_name,
                file_name_pattern=row.file_name_pattern,
                file_extension=row.file_extension,
                sender_address=row.sender_address,
                subject=row.subject,
                email_body=row.email_body,
                source=row.source,
                is_regex=row.is_regex,
                metadata=row.metadata,
                created_at=row.created_at,
                updated_at=row.updated_at,
                created_by=row.created_by
            )
    
    except Exception as e:
        logger.error(f"Error creating rule: {e}")
        raise HTTPException(status_code=500, detail=f"Error creating rule: {str(e)}")


@router.put("/{rule_id}", response_model=RuleResponse)
async def update_rule(
    rule_id: int,
    rule: RuleUpdateRequest,
    __username: str = Depends(authenticate_user)
):
    """Update an existing rule"""
    try:
        db_manager = DatabaseManager()
        engine = db_manager.engine
        
        # Check if rule exists
        check_sql = "SELECT id FROM public.rules WHERE id = :rule_id"
        with engine.connect() as conn:
            result = conn.execute(text(check_sql), {"rule_id": rule_id})
            if not result.fetchone():
                raise HTTPException(status_code=404, detail="Rule not found")
        
        # Build update query dynamically
        update_fields = []
        params = {"rule_id": rule_id}
        
        if rule.rule_name is not None:
            update_fields.append("rule_name = :rule_name")
            params["rule_name"] = rule.rule_name
        if rule.rule_description is not None:
            update_fields.append("rule_description = :rule_description")
            params["rule_description"] = rule.rule_description
        if rule.type is not None:
            update_fields.append("type = :type")
            params["type"] = rule.type
        if rule.status is not None:
            update_fields.append("status = :status")
            params["status"] = rule.status
        if rule.document_type is not None:
            update_fields.append("document_type = :document_type")
            params["document_type"] = rule.document_type
        if rule.file_name is not None:
            update_fields.append("file_name = :file_name")
            params["file_name"] = rule.file_name
        if rule.file_name_pattern is not None:
            update_fields.append("file_name_pattern = :file_name_pattern")
            params["file_name_pattern"] = rule.file_name_pattern
        if rule.file_extension is not None:
            update_fields.append("file_extension = :file_extension")
            params["file_extension"] = rule.file_extension
        if rule.sender_address is not None:
            update_fields.append("sender_address = :sender_address")
            params["sender_address"] = rule.sender_address
        if rule.subject is not None:
            update_fields.append("subject = :subject")
            params["subject"] = rule.subject
        if rule.email_body is not None:
            update_fields.append("email_body = :email_body")
            params["email_body"] = rule.email_body
        if rule.source is not None:
            update_fields.append("source = :source")
            params["source"] = rule.source
        if rule.is_regex is not None:
            update_fields.append("is_regex = :is_regex")
            params["is_regex"] = rule.is_regex
        if rule.metadata is not None:
            update_fields.append("metadata = CAST(:metadata AS jsonb)")
            params["metadata"] = json.dumps(rule.metadata)
        
        if not update_fields:
            raise HTTPException(status_code=400, detail="No fields to update")
        
        update_sql = f"""
            UPDATE public.rules
            SET {', '.join(update_fields)}
            WHERE id = :rule_id
            RETURNING 
                id, rule_id, rule_name, rule_description, type, status, document_type, 
                file_name, file_name_pattern, file_extension, sender_address,
                subject, email_body, source, is_regex, metadata,
                created_at, updated_at, created_by
        """
        
        with engine.connect() as conn:
            result = conn.execute(text(update_sql), params)
            conn.commit()
            row = result.fetchone()
            
            return RuleResponse(
                id=row.id,
                rule_id=row.rule_id,
                rule_name=row.rule_name,
                rule_description=row.rule_description,
                type=row.type,
                status=row.status,
                document_type=row.document_type,
                file_name=row.file_name,
                file_name_pattern=row.file_name_pattern,
                file_extension=row.file_extension,
                sender_address=row.sender_address,
                subject=row.subject,
                email_body=row.email_body,
                source=row.source,
                is_regex=row.is_regex,
                metadata=row.metadata,
                created_at=row.created_at,
                updated_at=row.updated_at,
                created_by=row.created_by
            )
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error updating rule {rule_id}: {e}")
        raise HTTPException(status_code=500, detail=f"Error updating rule: {str(e)}")


@router.delete("/{rule_id}", status_code=204)
async def delete_rule(
    rule_id: int,
    __username: str = Depends(authenticate_user)
):
    """Delete a rule"""
    try:
        db_manager = DatabaseManager()
        engine = db_manager.engine
        
        # Check if rule exists
        check_sql = "SELECT id FROM public.rules WHERE id = :rule_id"
        with engine.connect() as conn:
            result = conn.execute(text(check_sql), {"rule_id": rule_id})
            if not result.fetchone():
                raise HTTPException(status_code=404, detail="Rule not found")
        
        # Delete rule
        delete_sql = "DELETE FROM public.rules WHERE id = :rule_id"
        with engine.connect() as conn:
            conn.execute(text(delete_sql), {"rule_id": rule_id})
            conn.commit()
        
        return None
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error deleting rule {rule_id}: {e}")
        raise HTTPException(status_code=500, detail=f"Error deleting rule: {str(e)}")


class RuleMatchRequest(BaseModel):
    file_name: str = Field(..., description="File name to match")
    sender_address: Optional[str] = Field(None, description="Sender email address")
    subject: Optional[str] = Field(None, description="Email subject")
    email_body: Optional[str] = Field(None, description="Email body text")
    document_type: Optional[str] = Field(None, description="Document type")
    source: Optional[str] = Field(None, description="Source: 'sftp', 'mail', 'portal', 'manual', 'api_invoke'")
    rule_type: Optional[str] = Field(None, description="Filter by rule type: 'classification' or 'ignore'")


class RuleMatchResponse(BaseModel):
    matched: bool
    matching_rules: List[RuleResponse]
    classification_rules: List[RuleResponse]
    ignore_rules: List[RuleResponse]


@router.post("/match", response_model=RuleMatchResponse)
async def match_rules(
    match_request: RuleMatchRequest,
    __username: str = Depends(authenticate_user)
):
    """
    Match a file/document against all active rules using regex or wildcard patterns.
    
    Returns matching rules separated by type (classification vs ignore).
    """
    try:
        db_manager = DatabaseManager()
        engine = db_manager.engine
        
        # Build query to get active rules
        sql = """
            SELECT 
                id,
                rule_id,
                rule_name,
                rule_description,
                type,
                status,
                document_type,
                file_name,
                file_name_pattern,
                file_extension,
                sender_address,
                subject,
                email_body,
                source,
                is_regex,
                metadata,
                created_at,
                updated_at,
                created_by
            FROM public.rules
            WHERE status = 'active'
        """
        
        params = {}
        
        # Add source filter if provided
        if match_request.source:
            sql += " AND source = :source"
            params['source'] = match_request.source
        
        # Add type filter if provided
        if match_request.rule_type:
            sql += " AND type = :rule_type"
            params['rule_type'] = match_request.rule_type
        
        # Fetch all matching rules from database
        all_rules = []
        with engine.connect() as conn:
            result = conn.execute(text(sql), params)
            for row in result:
                rule_dict = {
                    'id': row.id,
                    'rule_id': row.rule_id,
                    'rule_name': row.rule_name,
                    'rule_description': row.rule_description,
                    'type': row.type,
                    'status': row.status,
                    'document_type': row.document_type,
                    'file_name': row.file_name,
                    'file_name_pattern': row.file_name_pattern,
                    'file_extension': row.file_extension,
                    'sender_address': row.sender_address,
                    'subject': row.subject,
                    'email_body': row.email_body,
                    'source': row.source,
                    'is_regex': row.is_regex,
                    'metadata': row.metadata,
                    'created_at': row.created_at,
                    'updated_at': row.updated_at,
                    'created_by': row.created_by
                }
                all_rules.append(rule_dict)
        
        # Match against rules
        matching_rules = []
        classification_rules = []
        ignore_rules = []
        
        for rule_dict in all_rules:
            if _rule_matches_file(
                rule_dict,
                match_request.file_name,
                match_request.sender_address,
                match_request.subject,
                match_request.email_body,
                match_request.document_type
            ):
                rule_response = RuleResponse(
                    id=rule_dict['id'],
                    rule_id=rule_dict['rule_id'],
                    rule_name=rule_dict['rule_name'],
                    rule_description=rule_dict['rule_description'],
                    type=rule_dict['type'],
                    status=rule_dict['status'],
                    document_type=rule_dict['document_type'],
                    file_name=rule_dict['file_name'],
                    file_name_pattern=rule_dict['file_name_pattern'],
                    file_extension=rule_dict['file_extension'],
                    sender_address=rule_dict['sender_address'],
                    subject=rule_dict['subject'],
                    email_body=rule_dict['email_body'],
                    source=rule_dict['source'],
                    is_regex=rule_dict['is_regex'],
                    metadata=rule_dict['metadata'],
                    created_at=rule_dict['created_at'],
                    updated_at=rule_dict['updated_at'],
                    created_by=rule_dict['created_by']
                )
                
                matching_rules.append(rule_response)
                
                if rule_dict['type'] == 'classification':
                    classification_rules.append(rule_response)
                elif rule_dict['type'] == 'ignore':
                    ignore_rules.append(rule_response)
        
        return RuleMatchResponse(
            matched=len(matching_rules) > 0,
            matching_rules=matching_rules,
            classification_rules=classification_rules,
            ignore_rules=ignore_rules
        )
    
    except Exception as e:
        logger.error(f"Error matching rules: {e}")
        raise HTTPException(status_code=500, detail=f"Error matching rules: {str(e)}")


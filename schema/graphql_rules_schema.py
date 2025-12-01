#!/usr/bin/env python3
"""
GraphQL Schema for Rules Management
Provides CRUD operations with pagination and search support
"""

import strawberry
from typing import List, Optional
from strawberry.types import Info
from sqlalchemy import text
from database_models import get_database_manager
from datetime import datetime
import logging
import json
import re
import fnmatch

# Import authentication context
from .graphql_auth_context import require_authentication

logger = logging.getLogger(__name__)


# GraphQL Types for array fields
@strawberry.type
class FileNamePatternItemType:
    """GraphQL type for FileNamePattern item"""
    id: int
    fileNamePattern: str
    fileExtension: Optional[str] = None

@strawberry.type
class SenderAddressItemType:
    """GraphQL type for SenderAddress item"""
    id: int
    senderAddress: str

@strawberry.type
class SubjectItemType:
    """GraphQL type for Subject item"""
    id: int
    subject: str

@strawberry.type
class KeywordItemType:
    """GraphQL type for Keyword item"""
    id: int
    keyword: str

# GraphQL Types
@strawberry.type
class RuleType:
    """GraphQL type for Rule"""
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
    metadata: Optional[str]  # JSON string
    created_at: str
    updated_at: str
    created_by: Optional[int]
    # New array fields
    fileNamePatterns: Optional[List[FileNamePatternItemType]] = None
    senderAddresses: Optional[List[SenderAddressItemType]] = None
    subjects: Optional[List[SubjectItemType]] = None
    keywords: Optional[List[KeywordItemType]] = None


@strawberry.type
class RuleListResponse:
    """Response type for paginated rule list"""
    data: List[RuleType]
    total: int
    page: int
    page_size: int
    total_pages: int


@strawberry.type
class RuleMatchResponse:
    """Response type for rule matching"""
    matched: bool
    matching_rules: List[RuleType]
    classification_rules: List[RuleType]
    ignore_rules: List[RuleType]


# Input Types
@strawberry.input
class RuleCreateInput:
    """Input type for creating a rule"""
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
    is_regex: Optional[bool] = False
    metadata: Optional[str] = None  # JSON string


@strawberry.input
class RuleUpdateInput:
    """Input type for updating a rule"""
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
    metadata: Optional[str] = None  # JSON string


@strawberry.input
class RuleMatchInput:
    """Input type for matching rules"""
    file_name: str
    sender_address: Optional[str] = None
    subject: Optional[str] = None
    email_body: Optional[str] = None
    document_type: Optional[str] = None
    source: Optional[str] = None
    rule_type: Optional[str] = None


# Utility functions for pattern matching
def _convert_wildcard_to_regex(pattern: str) -> str:
    """Convert wildcard pattern (*, ?) to regex pattern"""
    if not pattern:
        return None
    pattern = re.escape(pattern)
    pattern = pattern.replace(r'\*', '.*')
    pattern = pattern.replace(r'\?', '.')
    return f"^{pattern}$"


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
    if file_name_pattern:
        try:
            pattern_data = json.loads(file_name_pattern)
            if isinstance(pattern_data, list):
                for idx, item in enumerate(pattern_data):
                    if isinstance(item, dict):
                        fileNamePatterns.append(FileNamePatternItemType(
                            id=item.get('id', idx + 1),
                            fileNamePattern=item.get('fileNamePattern', item.get('file_name_pattern', '')),
                            fileExtension=item.get('fileExtension', item.get('file_extension', file_extension))
                        ))
                    else:
                        fileNamePatterns.append(FileNamePatternItemType(
                            id=idx + 1,
                            fileNamePattern=str(item),
                            fileExtension=file_extension
                        ))
            else:
                fileNamePatterns.append(FileNamePatternItemType(
                    id=1,
                    fileNamePattern=file_name_pattern,
                    fileExtension=file_extension
                ))
        except (json.JSONDecodeError, TypeError):
            fileNamePatterns.append(FileNamePatternItemType(
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
                        senderAddresses.append(SenderAddressItemType(
                            id=item.get('id', idx + 1),
                            senderAddress=item.get('senderAddress', item.get('sender_address', ''))
                        ))
                    else:
                        senderAddresses.append(SenderAddressItemType(
                            id=idx + 1,
                            senderAddress=str(item)
                        ))
            else:
                senderAddresses.append(SenderAddressItemType(
                    id=1,
                    senderAddress=sender_address
                ))
        except (json.JSONDecodeError, TypeError):
            senderAddresses.append(SenderAddressItemType(
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
                        subjects.append(SubjectItemType(
                            id=item.get('id', idx + 1),
                            subject=item.get('subject', '')
                        ))
                    else:
                        subjects.append(SubjectItemType(
                            id=idx + 1,
                            subject=str(item)
                        ))
            else:
                subjects.append(SubjectItemType(
                    id=1,
                    subject=subject
                ))
        except (json.JSONDecodeError, TypeError):
            subjects.append(SubjectItemType(
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
                        keywords.append(KeywordItemType(
                            id=item.get('id', idx + 1),
                            keyword=item.get('keyword', item.get('email_body', ''))
                        ))
                    else:
                        keywords.append(KeywordItemType(
                            id=idx + 1,
                            keyword=str(item)
                        ))
            else:
                keywords.append(KeywordItemType(
                    id=1,
                    keyword=email_body
                ))
        except (json.JSONDecodeError, TypeError):
            keywords.append(KeywordItemType(
                id=1,
                keyword=email_body
            ))
    
    return (
        fileNamePatterns if fileNamePatterns else None,
        senderAddresses if senderAddresses else None,
        subjects if subjects else None,
        keywords if keywords else None
    )


def _match_pattern(pattern: str, text: str, is_regex: bool) -> bool:
    """Match text against pattern using either regex or wildcard matching"""
    if not pattern or not text:
        return False
    try:
        if is_regex:
            return bool(re.search(pattern, text, re.IGNORECASE))
        else:
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


def _rule_matches_file(rule: dict, file_name: str, sender_address: Optional[str] = None,
                       subject: Optional[str] = None, email_body: Optional[str] = None,
                       document_type: Optional[str] = None) -> bool:
    """Check if a rule matches the given file/document attributes"""
    if rule.get('document_type'):
        if not document_type or rule['document_type'].lower() != document_type.lower():
            return False
    if rule.get('file_name'):
        if not _match_pattern(rule['file_name'], file_name, rule.get('is_regex', False)):
            return False
    if rule.get('sender_address'):
        if not sender_address or not _match_pattern(
            rule['sender_address'], sender_address, rule.get('is_regex', False)
        ):
            return False
    if rule.get('subject'):
        if not subject or not _match_pattern(
            rule['subject'], subject, rule.get('is_regex', False)
        ):
            return False
    if rule.get('email_body'):
        if not email_body or not _match_pattern(
            rule['email_body'], email_body, rule.get('is_regex', False)
        ):
            return False
    return True


def _build_search_conditions(search: Optional[str], params: dict) -> str:
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
    params: dict
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


# Query Class
@strawberry.type
class RuleQuery:
    """GraphQL Query root for rules"""
    
    @strawberry.field
    def rules(self, info: Info,
              page: Optional[int] = 1,
              page_size: Optional[int] = 50,
              search: Optional[str] = None,
              type_filter: Optional[str] = None,
              status_filter: Optional[str] = None,
              source_filter: Optional[str] = None,
              document_type_filter: Optional[str] = None) -> RuleListResponse:
        """Get all rules with pagination and search support - requires authentication"""
        require_authentication(info)
        
        db_manager = get_database_manager()
        engine = db_manager.engine
        
        params = {}
        sql_base = """
            SELECT 
                id, rule_id, rule_name, rule_description, type, status, document_type, 
                file_name, file_name_pattern, file_extension, sender_address,
                subject, email_body, source, is_regex, metadata,
                created_at, updated_at, created_by
            FROM public.rules
            WHERE 1=1
        """
        
        sql_base += _build_search_conditions(search, params)
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
                rules.append(RuleType(
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
                    metadata=json.dumps(row.metadata) if row.metadata else None,
                    created_at=row.created_at.isoformat() if row.created_at else "",
                    updated_at=row.updated_at.isoformat() if row.updated_at else "",
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
    
    @strawberry.field
    def rule(self, info: Info, id: int) -> Optional[RuleType]:
        """Get a single rule by ID - requires authentication"""
        require_authentication(info)
        
        db_manager = get_database_manager()
        engine = db_manager.engine
        
        sql = """
            SELECT 
                id, rule_id, rule_name, rule_description, type, status, document_type, 
                file_name, file_name_pattern, file_extension, sender_address,
                subject, email_body, source, is_regex, metadata,
                created_at, updated_at, created_by
            FROM public.rules
            WHERE id = :rule_id
        """
        
        with engine.connect() as conn:
            result = conn.execute(text(sql), {"rule_id": id})
            row = result.fetchone()
            
            if not row:
                return None
            
            # Transform single values to arrays
            fileNamePatterns, senderAddresses, subjects, keywords = _transform_to_array_fields(
                row.file_name_pattern,
                row.file_extension,
                row.sender_address,
                row.subject,
                row.email_body
            )
            
            return RuleType(
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
                metadata=json.dumps(row.metadata) if row.metadata else None,
                created_at=row.created_at.isoformat() if row.created_at else "",
                updated_at=row.updated_at.isoformat() if row.updated_at else "",
                created_by=row.created_by,
                fileNamePatterns=fileNamePatterns,
                senderAddresses=senderAddresses,
                subjects=subjects,
                keywords=keywords
            )
    
    @strawberry.field
    def match_rules(self, info: Info, input: RuleMatchInput) -> RuleMatchResponse:
        """Match a file/document against all active rules - requires authentication"""
        require_authentication(info)
        
        db_manager = get_database_manager()
        engine = db_manager.engine
        
        sql = """
            SELECT 
                id, rule_id, rule_name, rule_description, type, status, document_type, 
                file_name, file_name_pattern, file_extension, sender_address,
                subject, email_body, source, is_regex, metadata,
                created_at, updated_at, created_by
            FROM public.rules
            WHERE status = 'active'
        """
        
        params = {}
        if input.source:
            sql += " AND source = :source"
            params['source'] = input.source
        if input.rule_type:
            sql += " AND type = :rule_type"
            params['rule_type'] = input.rule_type
        
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
        
        matching_rules = []
        classification_rules = []
        ignore_rules = []
        
        for rule_dict in all_rules:
            if _rule_matches_file(
                rule_dict,
                input.file_name,
                input.sender_address,
                input.subject,
                input.email_body,
                input.document_type
            ):
                rule_type = RuleType(
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
                    metadata=json.dumps(rule_dict['metadata']) if rule_dict['metadata'] else None,
                    created_at=rule_dict['created_at'].isoformat() if rule_dict['created_at'] else "",
                    updated_at=rule_dict['updated_at'].isoformat() if rule_dict['updated_at'] else "",
                    created_by=rule_dict['created_by']
                )
                
                matching_rules.append(rule_type)
                if rule_dict['type'] == 'classification':
                    classification_rules.append(rule_type)
                elif rule_dict['type'] == 'ignore':
                    ignore_rules.append(rule_type)
        
        return RuleMatchResponse(
            matched=len(matching_rules) > 0,
            matching_rules=matching_rules,
            classification_rules=classification_rules,
            ignore_rules=ignore_rules
        )


# Mutation Class
@strawberry.type
class RuleMutation:
    """GraphQL Mutation root for rules"""
    
    @strawberry.mutation
    def create_rule(self, info: Info, input: RuleCreateInput) -> RuleType:
        """Create a new rule - requires authentication"""
        user = require_authentication(info)
        
        db_manager = get_database_manager()
        engine = db_manager.engine
        
        # Get user ID from username
        user_sql = "SELECT id FROM public.users WHERE username = :username"
        with engine.connect() as conn:
            user_result = conn.execute(text(user_sql), {"username": user.get("username")})
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
            "rule_name": input.rule_name,
            "rule_description": input.rule_description,
            "type": input.type,
            "status": input.status,
            "document_type": input.document_type,
            "file_name": input.file_name,
            "file_name_pattern": input.file_name_pattern,
            "file_extension": input.file_extension,
            "sender_address": input.sender_address,
            "subject": input.subject,
            "email_body": input.email_body,
            "source": input.source,
            "is_regex": input.is_regex if input.is_regex is not None else False,
            "metadata": input.metadata if input.metadata else None,
            "created_by": created_by
        }
        
        with engine.connect() as conn:
            result = conn.execute(text(insert_sql), params)
            conn.commit()
            row = result.fetchone()
            
            return RuleType(
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
                metadata=json.dumps(row.metadata) if row.metadata else None,
                created_at=row.created_at.isoformat() if row.created_at else "",
                updated_at=row.updated_at.isoformat() if row.updated_at else "",
                created_by=row.created_by
            )
    
    @strawberry.mutation
    def update_rule(self, info: Info, id: int, input: RuleUpdateInput) -> Optional[RuleType]:
        """Update an existing rule - requires authentication"""
        require_authentication(info)
        
        db_manager = get_database_manager()
        engine = db_manager.engine
        
        # Check if rule exists
        check_sql = "SELECT id FROM public.rules WHERE id = :rule_id"
        with engine.connect() as conn:
            result = conn.execute(text(check_sql), {"rule_id": id})
            if not result.fetchone():
                return None
        
        # Build update query dynamically
        update_fields = []
        params = {"rule_id": id}
        
        if input.rule_name is not None:
            update_fields.append("rule_name = :rule_name")
            params["rule_name"] = input.rule_name
        if input.rule_description is not None:
            update_fields.append("rule_description = :rule_description")
            params["rule_description"] = input.rule_description
        if input.type is not None:
            update_fields.append("type = :type")
            params["type"] = input.type
        if input.status is not None:
            update_fields.append("status = :status")
            params["status"] = input.status
        if input.document_type is not None:
            update_fields.append("document_type = :document_type")
            params["document_type"] = input.document_type
        if input.file_name is not None:
            update_fields.append("file_name = :file_name")
            params["file_name"] = input.file_name
        if input.file_name_pattern is not None:
            update_fields.append("file_name_pattern = :file_name_pattern")
            params["file_name_pattern"] = input.file_name_pattern
        if input.file_extension is not None:
            update_fields.append("file_extension = :file_extension")
            params["file_extension"] = input.file_extension
        if input.sender_address is not None:
            update_fields.append("sender_address = :sender_address")
            params["sender_address"] = input.sender_address
        if input.subject is not None:
            update_fields.append("subject = :subject")
            params["subject"] = input.subject
        if input.email_body is not None:
            update_fields.append("email_body = :email_body")
            params["email_body"] = input.email_body
        if input.source is not None:
            update_fields.append("source = :source")
            params["source"] = input.source
        if input.is_regex is not None:
            update_fields.append("is_regex = :is_regex")
            params["is_regex"] = input.is_regex
        if input.metadata is not None:
            update_fields.append("metadata = CAST(:metadata AS jsonb)")
            params["metadata"] = input.metadata
        
        if not update_fields:
            raise Exception("No fields to update")
        
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
            
            return RuleType(
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
                metadata=json.dumps(row.metadata) if row.metadata else None,
                created_at=row.created_at.isoformat() if row.created_at else "",
                updated_at=row.updated_at.isoformat() if row.updated_at else "",
                created_by=row.created_by
            )
    
    @strawberry.mutation
    def delete_rule(self, info: Info, id: int) -> bool:
        """Delete a rule - requires authentication"""
        require_authentication(info)
        
        db_manager = get_database_manager()
        engine = db_manager.engine
        
        # Check if rule exists
        check_sql = "SELECT id FROM public.rules WHERE id = :rule_id"
        with engine.connect() as conn:
            result = conn.execute(text(check_sql), {"rule_id": id})
            if not result.fetchone():
                return False
        
        # Delete rule
        delete_sql = "DELETE FROM public.rules WHERE id = :rule_id"
        with engine.connect() as conn:
            conn.execute(text(delete_sql), {"rule_id": id})
            conn.commit()
        
        return True


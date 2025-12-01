"""
Password Rules API
Provides CRUD endpoints for password rules with pagination and search support.
Password rules are used to define rules for processing password-protected documents.
"""
import logging
import json
from fastapi import APIRouter, HTTPException, Depends, Query, Body
from typing import Optional, List, Dict, Any
from sqlalchemy import text
from datetime import datetime
from database_models import DatabaseManager
from rbac.utils.auth import getCurrentUser
from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/password-rules", tags=["Password Rules API"])


# Pydantic models for request/response
class FileNamePattern(BaseModel):
    pattern: str = Field(..., description="File name pattern")
    extension: Optional[str] = Field(None, description="File extension (PDF, CSV, XLSX, etc.)")
    other_extension: Optional[str] = Field(None, description="Custom extension if 'Other' is selected")


class MailBodyPattern(BaseModel):
    regex: Optional[str] = Field(None, description="Regular expression pattern")
    keywords: Optional[str] = Field(None, description="Keywords for mail body matching")


class FileDetails(BaseModel):
    file_name_patterns: Optional[List[FileNamePattern]] = Field(None, description="List of file name patterns with extensions")
    sender_addresses: Optional[List[str]] = Field(None, description="List of sender email addresses")
    subjects: Optional[List[str]] = Field(None, description="List of email subjects")
    mail_body_patterns: Optional[List[MailBodyPattern]] = Field(None, description="List of mail body patterns with regex and keywords")
    password: Optional[str] = Field(None, description="Password for protected documents")


class PasswordRuleCreateRequest(BaseModel):
    rule_name: str = Field(..., description="Unique rule name")
    rule_description: Optional[str] = Field(None, description="Rule description")
    status: str = Field("Active", description="Rule status: 'Active' or 'Inactive'")
    file_details: Optional[FileDetails] = Field(None, description="File details including patterns, addresses, subjects, mail body, and password")


class PasswordRuleUpdateRequest(BaseModel):
    rule_name: Optional[str] = Field(None, description="Unique rule name")
    rule_description: Optional[str] = Field(None, description="Rule description")
    status: Optional[str] = Field(None, description="Rule status: 'Active' or 'Inactive'")
    file_details: Optional[FileDetails] = Field(None, description="File details including patterns, addresses, subjects, mail body, and password")


class PasswordRuleResponse(BaseModel):
    id: int
    rule_id: Optional[str]
    rule_name: str
    rule_description: Optional[str]
    status: str
    file_details: Optional[Dict[str, Any]]
    created_at: datetime
    updated_at: datetime
    created_by: Optional[int]
    modified_by: Optional[int]
    created_by_name: Optional[str] = None
    modified_by_name: Optional[str] = None


class PasswordRuleListResponse(BaseModel):
    data: List[PasswordRuleResponse]
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
            OR COALESCE(rule_id, '') ILIKE :search
            OR COALESCE(status, '') ILIKE :search
        )
    """
    params['search'] = f"%{search}%"
    return conditions


def _build_filter_conditions(
    status_filter: Optional[str],
    params: Dict[str, Any]
) -> str:
    """Build SQL WHERE conditions for filters"""
    conditions = []
    
    if status_filter:
        conditions.append("status = :status_filter")
        params['status_filter'] = status_filter
    
    if conditions:
        return " AND " + " AND ".join(conditions)
    return ""


@router.get("/", response_model=PasswordRuleListResponse)
async def get_password_rules(
    page: int = Query(1, ge=1, description="Page number"),
    page_size: int = Query(25, ge=1, le=100, description="Items per page (25, 50, or 100)"),
    search: Optional[str] = Query(None, description="Search term for rule_name, rule_description, rule_id, status"),
    status_filter: Optional[str] = Query(None, description="Filter by status: 'Active' or 'Inactive'"),
    __username: str = Depends(authenticate_user)
):
    """
    Get all password rules with pagination and search support.
    
    Supports:
    - Pagination (page, page_size: 25, 50, or 100)
    - Search across multiple fields
    - Filtering by status
    """
    try:
        db_manager = DatabaseManager()
        engine = db_manager.engine
        
        params = {}
        
        # Build base query with user name joins
        sql_base = """
            SELECT 
                pr.id,
                pr.rule_id,
                pr.rule_name,
                pr.rule_description,
                pr.status,
                pr.file_details,
                pr.created_at,
                pr.updated_at,
                pr.created_by,
                pr.modified_by,
                u1.username as created_by_name,
                u2.username as modified_by_name
            FROM public.password_rules pr
            LEFT JOIN public.users u1 ON pr.created_by = u1.id
            LEFT JOIN public.users u2 ON pr.modified_by = u2.id
            WHERE 1=1
        """
        
        # Add search conditions
        sql_base += _build_search_conditions(search, params)
        
        # Add filter conditions
        sql_base += _build_filter_conditions(status_filter, params)
        
        # Count total records
        count_sql = f"SELECT COUNT(*) FROM ({sql_base}) as count_query"
        with engine.connect() as conn:
            result = conn.execute(text(count_sql), params)
            total_count = result.scalar()
        
        # Add pagination
        sql = sql_base + " ORDER BY pr.created_at DESC OFFSET :offset LIMIT :limit"
        params['offset'] = (page - 1) * page_size
        params['limit'] = page_size
        
        # Execute query
        rules = []
        with engine.connect() as conn:
            result = conn.execute(text(sql), params)
            for row in result:
                rules.append(PasswordRuleResponse(
                    id=row.id,
                    rule_id=row.rule_id,
                    rule_name=row.rule_name,
                    rule_description=row.rule_description,
                    status=row.status,
                    file_details=row.file_details,
                    created_at=row.created_at,
                    updated_at=row.updated_at,
                    created_by=row.created_by,
                    modified_by=row.modified_by,
                    created_by_name=row.created_by_name,
                    modified_by_name=row.modified_by_name
                ))
        
        total_pages = (total_count + page_size - 1) // page_size
        
        return PasswordRuleListResponse(
            data=rules,
            total=total_count,
            page=page,
            page_size=page_size,
            total_pages=total_pages
        )
    
    except Exception as e:
        logger.error(f"Error fetching password rules: {e}")
        raise HTTPException(status_code=500, detail=f"Error fetching password rules: {str(e)}")


@router.get("/{rule_id}", response_model=PasswordRuleResponse)
async def get_password_rule(
    rule_id: int,
    __username: str = Depends(authenticate_user)
):
    """Get a single password rule by ID"""
    try:
        db_manager = DatabaseManager()
        engine = db_manager.engine
        
        sql = """
            SELECT 
                pr.id,
                pr.rule_id,
                pr.rule_name,
                pr.rule_description,
                pr.status,
                pr.file_details,
                pr.created_at,
                pr.updated_at,
                pr.created_by,
                pr.modified_by,
                u1.username as created_by_name,
                u2.username as modified_by_name
            FROM public.password_rules pr
            LEFT JOIN public.users u1 ON pr.created_by = u1.id
            LEFT JOIN public.users u2 ON pr.modified_by = u2.id
            WHERE pr.id = :rule_id
        """
        
        with engine.connect() as conn:
            result = conn.execute(text(sql), {"rule_id": rule_id})
            row = result.fetchone()
            
            if not row:
                raise HTTPException(status_code=404, detail="Password rule not found")
            
            return PasswordRuleResponse(
                id=row.id,
                rule_id=row.rule_id,
                rule_name=row.rule_name,
                rule_description=row.rule_description,
                status=row.status,
                file_details=row.file_details,
                created_at=row.created_at,
                updated_at=row.updated_at,
                created_by=row.created_by,
                modified_by=row.modified_by,
                created_by_name=row.created_by_name,
                modified_by_name=row.modified_by_name
            )
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching password rule {rule_id}: {e}")
        raise HTTPException(status_code=500, detail=f"Error fetching password rule: {str(e)}")


@router.post("/", response_model=PasswordRuleResponse, status_code=201)
async def create_password_rule(
    rule: PasswordRuleCreateRequest,
    __username: str = Depends(authenticate_user)
):
    """Create a new password rule"""
    try:
        db_manager = DatabaseManager()
        engine = db_manager.engine
        
        # Validate status
        if rule.status not in ['Active', 'Inactive']:
            raise HTTPException(status_code=400, detail="Status must be 'Active' or 'Inactive'")
        
        # Check if rule_name already exists
        check_sql = "SELECT id FROM public.password_rules WHERE rule_name = :rule_name"
        with engine.connect() as conn:
            result = conn.execute(text(check_sql), {"rule_name": rule.rule_name})
            if result.fetchone():
                raise HTTPException(status_code=400, detail="Rule name already exists")
        
        # Get user ID from username
        user_sql = "SELECT id FROM public.users WHERE username = :username"
        with engine.connect() as conn:
            user_result = conn.execute(text(user_sql), {"username": __username})
            user_row = user_result.fetchone()
            created_by = user_row[0] if user_row else None
        
        # Prepare file_details JSON
        file_details_dict = None
        if rule.file_details:
            file_details_dict = rule.file_details.dict(exclude_none=True)
        
        # Insert rule
        insert_sql = """
            INSERT INTO public.password_rules (
                rule_name, rule_description, status, file_details, created_by
            )
            VALUES (
                :rule_name, :rule_description, :status, 
                CAST(:file_details AS jsonb), :created_by
            )
            RETURNING 
                id, rule_id, rule_name, rule_description, status, file_details,
                created_at, updated_at, created_by, modified_by
        """
        
        params = {
            "rule_name": rule.rule_name,
            "rule_description": rule.rule_description,
            "status": rule.status,
            "file_details": json.dumps(file_details_dict) if file_details_dict else None,
            "created_by": created_by
        }
        
        with engine.connect() as conn:
            result = conn.execute(text(insert_sql), params)
            conn.commit()
            row = result.fetchone()
            
            # Get user names
            user_names_sql = """
                SELECT 
                    u1.username as created_by_name,
                    u2.username as modified_by_name
                FROM public.password_rules pr
                LEFT JOIN public.users u1 ON pr.created_by = u1.id
                LEFT JOIN public.users u2 ON pr.modified_by = u2.id
                WHERE pr.id = :rule_id
            """
            user_names_result = conn.execute(text(user_names_sql), {"rule_id": row.id})
            user_names_row = user_names_result.fetchone()
            
            return PasswordRuleResponse(
                id=row.id,
                rule_id=row.rule_id,
                rule_name=row.rule_name,
                rule_description=row.rule_description,
                status=row.status,
                file_details=row.file_details,
                created_at=row.created_at,
                updated_at=row.updated_at,
                created_by=row.created_by,
                modified_by=row.modified_by,
                created_by_name=user_names_row[0] if user_names_row else None,
                modified_by_name=user_names_row[1] if user_names_row else None
            )
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error creating password rule: {e}")
        raise HTTPException(status_code=500, detail=f"Error creating password rule: {str(e)}")


@router.put("/{rule_id}", response_model=PasswordRuleResponse)
async def update_password_rule(
    rule_id: int,
    rule: PasswordRuleUpdateRequest,
    __username: str = Depends(authenticate_user)
):
    """Update an existing password rule"""
    try:
        db_manager = DatabaseManager()
        engine = db_manager.engine
        
        # Check if rule exists
        check_sql = "SELECT id FROM public.password_rules WHERE id = :rule_id"
        with engine.connect() as conn:
            result = conn.execute(text(check_sql), {"rule_id": rule_id})
            if not result.fetchone():
                raise HTTPException(status_code=404, detail="Password rule not found")
        
        # Check if rule_name already exists (if being updated)
        if rule.rule_name:
            check_name_sql = "SELECT id FROM public.password_rules WHERE rule_name = :rule_name AND id != :rule_id"
            with engine.connect() as conn:
                result = conn.execute(text(check_name_sql), {"rule_name": rule.rule_name, "rule_id": rule_id})
                if result.fetchone():
                    raise HTTPException(status_code=400, detail="Rule name already exists")
        
        # Validate status if provided
        if rule.status and rule.status not in ['Active', 'Inactive']:
            raise HTTPException(status_code=400, detail="Status must be 'Active' or 'Inactive'")
        
        # Get user ID from username for modified_by
        user_sql = "SELECT id FROM public.users WHERE username = :username"
        with engine.connect() as conn:
            user_result = conn.execute(text(user_sql), {"username": __username})
            user_row = user_result.fetchone()
            modified_by = user_row[0] if user_row else None
        
        # Build update query dynamically
        update_fields = []
        params = {"rule_id": rule_id}
        
        if rule.rule_name is not None:
            update_fields.append("rule_name = :rule_name")
            params["rule_name"] = rule.rule_name
        if rule.rule_description is not None:
            update_fields.append("rule_description = :rule_description")
            params["rule_description"] = rule.rule_description
        if rule.status is not None:
            update_fields.append("status = :status")
            params["status"] = rule.status
        if rule.file_details is not None:
            file_details_dict = rule.file_details.dict(exclude_none=True)
            update_fields.append("file_details = CAST(:file_details AS jsonb)")
            params["file_details"] = json.dumps(file_details_dict) if file_details_dict else None
        
        # Always update modified_by
        update_fields.append("modified_by = :modified_by")
        params["modified_by"] = modified_by
        
        if not update_fields:
            raise HTTPException(status_code=400, detail="No fields to update")
        
        update_sql = f"""
            UPDATE public.password_rules
            SET {', '.join(update_fields)}
            WHERE id = :rule_id
            RETURNING 
                id, rule_id, rule_name, rule_description, status, file_details,
                created_at, updated_at, created_by, modified_by
        """
        
        with engine.connect() as conn:
            result = conn.execute(text(update_sql), params)
            conn.commit()
            row = result.fetchone()
            
            # Get user names
            user_names_sql = """
                SELECT 
                    u1.username as created_by_name,
                    u2.username as modified_by_name
                FROM public.password_rules pr
                LEFT JOIN public.users u1 ON pr.created_by = u1.id
                LEFT JOIN public.users u2 ON pr.modified_by = u2.id
                WHERE pr.id = :rule_id
            """
            user_names_result = conn.execute(text(user_names_sql), {"rule_id": row.id})
            user_names_row = user_names_result.fetchone()
            
            return PasswordRuleResponse(
                id=row.id,
                rule_id=row.rule_id,
                rule_name=row.rule_name,
                rule_description=row.rule_description,
                status=row.status,
                file_details=row.file_details,
                created_at=row.created_at,
                updated_at=row.updated_at,
                created_by=row.created_by,
                modified_by=row.modified_by,
                created_by_name=user_names_row[0] if user_names_row else None,
                modified_by_name=user_names_row[1] if user_names_row else None
            )
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error updating password rule {rule_id}: {e}")
        raise HTTPException(status_code=500, detail=f"Error updating password rule: {str(e)}")


@router.delete("/{rule_id}", status_code=204)
async def delete_password_rule(
    rule_id: int,
    __username: str = Depends(authenticate_user)
):
    """Delete a password rule"""
    try:
        db_manager = DatabaseManager()
        engine = db_manager.engine
        
        # Check if rule exists
        check_sql = "SELECT id FROM public.password_rules WHERE id = :rule_id"
        with engine.connect() as conn:
            result = conn.execute(text(check_sql), {"rule_id": rule_id})
            if not result.fetchone():
                raise HTTPException(status_code=404, detail="Password rule not found")
        
        # Delete rule
        delete_sql = "DELETE FROM public.password_rules WHERE id = :rule_id"
        with engine.connect() as conn:
            conn.execute(text(delete_sql), {"rule_id": rule_id})
            conn.commit()
        
        return None
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error deleting password rule {rule_id}: {e}")
        raise HTTPException(status_code=500, detail=f"Error deleting password rule: {str(e)}")


@router.post("/{rule_id}/duplicate", response_model=PasswordRuleResponse, status_code=201)
async def duplicate_password_rule(
    rule_id: int,
    __username: str = Depends(authenticate_user)
):
    """Duplicate an existing password rule"""
    try:
        db_manager = DatabaseManager()
        engine = db_manager.engine
        
        # Get the original rule
        get_sql = """
            SELECT 
                rule_name, rule_description, status, file_details, created_by
            FROM public.password_rules
            WHERE id = :rule_id
        """
        
        with engine.connect() as conn:
            result = conn.execute(text(get_sql), {"rule_id": rule_id})
            original_rule = result.fetchone()
            
            if not original_rule:
                raise HTTPException(status_code=404, detail="Password rule not found")
        
        # Get user ID from username
        user_sql = "SELECT id FROM public.users WHERE username = :username"
        with engine.connect() as conn:
            user_result = conn.execute(text(user_sql), {"username": __username})
            user_row = user_result.fetchone()
            created_by = user_row[0] if user_row else None
        
        # Create new rule with duplicated data
        # Append " (Copy)" to rule name and check for uniqueness
        new_rule_name = f"{original_rule.rule_name} (Copy)"
        counter = 1
        while True:
            check_sql = "SELECT id FROM public.password_rules WHERE rule_name = :rule_name"
            with engine.connect() as conn:
                result = conn.execute(text(check_sql), {"rule_name": new_rule_name})
                if not result.fetchone():
                    break
            new_rule_name = f"{original_rule.rule_name} (Copy {counter})"
            counter += 1
        
        # Insert duplicated rule
        insert_sql = """
            INSERT INTO public.password_rules (
                rule_name, rule_description, status, file_details, created_by
            )
            VALUES (
                :rule_name, :rule_description, :status, 
                CAST(:file_details AS jsonb), :created_by
            )
            RETURNING 
                id, rule_id, rule_name, rule_description, status, file_details,
                created_at, updated_at, created_by, modified_by
        """
        
        params = {
            "rule_name": new_rule_name,
            "rule_description": original_rule.rule_description,
            "status": original_rule.status,
            "file_details": json.dumps(original_rule.file_details) if original_rule.file_details else None,
            "created_by": created_by
        }
        
        with engine.connect() as conn:
            result = conn.execute(text(insert_sql), params)
            conn.commit()
            row = result.fetchone()
            
            # Get user names
            user_names_sql = """
                SELECT 
                    u1.username as created_by_name,
                    u2.username as modified_by_name
                FROM public.password_rules pr
                LEFT JOIN public.users u1 ON pr.created_by = u1.id
                LEFT JOIN public.users u2 ON pr.modified_by = u2.id
                WHERE pr.id = :rule_id
            """
            user_names_result = conn.execute(text(user_names_sql), {"rule_id": row.id})
            user_names_row = user_names_result.fetchone()
            
            return PasswordRuleResponse(
                id=row.id,
                rule_id=row.rule_id,
                rule_name=row.rule_name,
                rule_description=row.rule_description,
                status=row.status,
                file_details=row.file_details,
                created_at=row.created_at,
                updated_at=row.updated_at,
                created_by=row.created_by,
                modified_by=row.modified_by,
                created_by_name=user_names_row[0] if user_names_row else None,
                modified_by_name=user_names_row[1] if user_names_row else None
            )
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error duplicating password rule {rule_id}: {e}")
        raise HTTPException(status_code=500, detail=f"Error duplicating password rule: {str(e)}")


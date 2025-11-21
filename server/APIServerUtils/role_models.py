#!/usr/bin/env python3
"""
Pydantic models for Role Management API
Defines request/response schemas for validation
"""

from pydantic import BaseModel, Field, field_validator
from typing import Optional, List, Any
from datetime import datetime

# Request Models

class ModulePermission(BaseModel):
    """Model for module-level permissions"""
    module: str = Field(..., description="Module name (e.g., 'Frame', 'NAV Validus')")
    create: Optional[bool] = Field(None, description="Create permission")
    read: Optional[bool] = Field(None, description="Read permission")
    update: Optional[bool] = Field(None, description="Update permission")
    delete: Optional[bool] = Field(None, description="Delete permission")
    children: Optional[List['ModulePermission']] = Field(None, alias="_children", description="Child modules (processed recursively)")
    
    @field_validator('*', mode='before')
    @classmethod
    def validate_permission_fields(cls, v, info):
        """Validate that only allowed permission fields are provided"""
        field_name = info.field_name
        
        if field_name in ['create', 'read', 'update', 'delete']:
            return v
        
        # Check for invalid permission fields
        if field_name.startswith('permission_') or field_name in ['view', 'edit', 'modify', 'remove']:
            raise ValueError(f"Invalid permission field '{field_name}'. Only 'create', 'read', 'update', 'delete' are allowed.")
        
        return v

class RoleCreateRequest(BaseModel):
    """Request model for creating a new role with permissions"""
    role_name: str = Field(..., min_length=2, max_length=50, description="Role name")
    role_code: Optional[str] = Field(None, min_length=2, max_length=50, description="Unique role code (auto-generated from role_name if not provided)")
    description: Optional[str] = Field(None, max_length=255, description="Role description")
    is_active: bool = Field(default=True, description="Whether role is active")
    permissions: Optional[List[ModulePermission]] = Field(None, description="List of module permissions")

class RoleUpdateRequest(BaseModel):
    """Request model for updating an existing role with optional permissions (role_code is not editable)"""
    role_name: Optional[str] = Field(None, min_length=2, max_length=50, description="Role name")
    description: Optional[str] = Field(None, max_length=255, description="Role description")
    is_active: Optional[bool] = Field(None, description="Whether role is active")
    permissions: Optional[List[ModulePermission]] = Field(None, description="List of module permissions")

# Update ModulePermission to handle self-reference
ModulePermission.model_rebuild()

# Export only the models we actually use
__all__ = [
    'ModulePermission',
    'RoleCreateRequest',
    'RoleUpdateRequest'
]


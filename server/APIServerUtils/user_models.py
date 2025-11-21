#!/usr/bin/env python3
"""
Pydantic models for User Management API
Defines request/response schemas for validation
"""

from pydantic import BaseModel, Field, EmailStr, RootModel
from typing import List, Optional, Dict, Any
from datetime import datetime

# Request Models

class UserCreateRequest(BaseModel):
    """Request model for creating a new user (legacy format - deprecated)"""
    username: str = Field(..., min_length=3, max_length=50, description="Unique username")
    display_name: str = Field(..., min_length=2, max_length=100, description="User's display name")
    email: Optional[EmailStr] = Field(None, description="User's email address")
    password: str = Field(..., min_length=6, description="User's password")
    role_id: int = Field(..., description="User's role ID")
    client_id: Optional[int] = Field(default=None, description="Client ID to assign user to")
    is_active: bool = Field(default=True, description="Whether user is active")

class UserCreateRequestV2(BaseModel):
    """Request model for creating a new user (new format with first/last name)"""
    first_name: str = Field(..., min_length=1, max_length=50, description="User's first name")
    last_name: str = Field(..., min_length=1, max_length=50, description="User's last name")
    email: EmailStr = Field(..., description="User's email address")
    job_title: str = Field(..., description="User's job title")
    role_name: str = Field(..., description="User's role name (case-insensitive, e.g., 'admin', 'user', 'dev', 'manager')")
    password: str = Field(..., min_length=6, description="User's password")

class UserToggleStatusRequest(BaseModel):
    """Request model for toggling user active/inactive status"""
    user_id: int = Field(..., description="User ID")
    active: bool = Field(..., description="True to activate, False to deactivate")

class UserToggleStatusResponse(BaseModel):
    """Response model for user status toggle"""
    success: bool = Field(..., description="Whether the operation was successful")
    message: str = Field(..., description="Success or error message")
    user_id: int = Field(..., description="User ID that was updated")
    is_active: bool = Field(..., description="New active status of the user")

class UnifiedUserCreateRequest(BaseModel):
    """Request model for unified user creation (single or multiple users)"""
    form_1: UserCreateRequestV2 = Field(..., description="First user data")
    form_2: Optional[UserCreateRequestV2] = Field(None, description="Second user data (optional)")
    form_3: Optional[UserCreateRequestV2] = Field(None, description="Third user data (optional)")
    form_4: Optional[UserCreateRequestV2] = Field(None, description="Fourth user data (optional)")
    form_5: Optional[UserCreateRequestV2] = Field(None, description="Fifth user data (optional)")


class UserUpdateRequest(BaseModel):
    """Request model for updating an existing user (uses underscore fields only)"""
    first_name: Optional[str] = Field(None, min_length=1, max_length=50, description="User's first name")
    last_name: Optional[str] = Field(None, min_length=1, max_length=50, description="User's last name")
    email: Optional[EmailStr] = Field(None, description="User's email address")
    job_title: Optional[str] = Field(None, description="User's job title")
    role_name: Optional[str] = Field(None, description="User's role code (e.g., 'admin', 'user', 'manager')")
    client_id: Optional[int] = Field(None, description="Client ID to assign user to")
    is_active: Optional[bool] = Field(None, description="Whether user is active")
    password: Optional[str] = Field(None, min_length=6, description="User's password")

class BulkUpdateRequest(BaseModel):
    """Request model for bulk updating users"""
    user_ids: List[int] = Field(..., description="List of user IDs to update")
    updates: dict = Field(..., description="Fields to update for all users")

class UserSearchRequest(BaseModel):
    """Request model for searching users"""
    search: Optional[str] = Field(None, description="Search term for username, display name, or email")
    status_filter: Optional[str] = Field(None, description="Filter by status: 'active', 'inactive', or 'all'")
    page: int = Field(default=1, ge=1, description="Page number for pagination")
    page_size: int = Field(default=10, ge=1, le=100, description="Number of users per page")

# Response Models

class ClientInfo(BaseModel):
    """Client information model"""
    id: int
    name: str
    code: str
    description: Optional[str] = None

class RoleInfo(BaseModel):
    """Role information model"""
    id: int
    name: str
    code: str
    description: Optional[str] = None

class UserListItem(BaseModel):
    """User list item model for table display"""
    id: int
    username: str
    display_name: str
    first_name: str
    last_name: str
    email: Optional[str]
    clients: List[str]
    roles: List[str]
    role_str: str
    role: str
    status: str
    temp_password: Optional[bool]
    created_at: Optional[str]
    updated_at: Optional[str]

class UserDetail(BaseModel):
    """Detailed user information model"""
    id: int
    username: str
    display_name: str
    first_name: str
    last_name: str
    email: Optional[str]
    clients: List[ClientInfo]
    roles: List[RoleInfo]
    role_str: str
    role: str
    is_active: bool
    temp_password: Optional[bool]
    created_at: Optional[datetime]
    updated_at: Optional[datetime]

class PaginationInfo(BaseModel):
    """Pagination information model"""
    current_page: int
    page_size: int
    total_count: int
    total_pages: int
    start_record: int
    end_record: int

class UserListResponse(BaseModel):
    """Response model for user list with pagination"""
    success: bool
    data: List[UserListItem]
    pagination: PaginationInfo

class UserDetailResponse(BaseModel):
    """Response model for single user details"""
    success: bool
    data: UserDetail

class UserCreateResponse(BaseModel):
    """Response model for user creation"""
    success: bool
    message: str
    data: dict

class UserCreateResponseV2(BaseModel):
    """Response model for user creation with generated credentials"""
    success: bool
    message: str
    data: dict
    credentials: dict  # Contains generated username and temporary password

class BulkUserCreateResponseV2(BaseModel):
    """Response model for bulk user creation"""
    success: bool
    message: str
    total_users: int
    successful_creations: int
    failed_creations: int
    results: Dict[str, dict]  # form_id -> result (success/error)
    credentials: Dict[str, dict]  # form_id -> credentials for successful users

class UserUpdateResponse(BaseModel):
    """Response model for user update"""
    success: bool
    message: str
    data: dict

class UserDeleteResponse(BaseModel):
    """Response model for user deletion"""
    success: bool
    message: str

class BulkUpdateResponse(BaseModel):
    """Response model for bulk user update"""
    success: bool
    message: str
    updated_count: int

class ClientListResponse(BaseModel):
    """Response model for available clients"""
    success: bool
    data: List[ClientInfo]

class RoleListResponse(BaseModel):
    """Response model for available roles"""
    success: bool
    data: List[RoleInfo]

# Role Permission Models

class ModulePermission(BaseModel):
    """Model for module-level permissions"""
    module: str = Field(..., description="Module name (e.g., 'Frame', 'NAV Validus')")
    create: Optional[bool] = Field(None, description="Create permission")
    view: Optional[bool] = Field(None, description="View permission")
    update: Optional[bool] = Field(None, description="Update permission")
    delete: Optional[bool] = Field(None, description="Delete permission")
    children: Optional[List['ModulePermission']] = Field(None, alias="_children", description="Child modules (ignored for now)")

class RolePermissionsRequest(BaseModel):
    """Request model for creating role permissions"""
    role_name: str = Field(..., min_length=1, max_length=50, description="Role name")
    permissions: List[ModulePermission] = Field(..., description="List of module permissions")

class RolePermissionsResponse(BaseModel):
    """Response model for role permissions creation"""
    success: bool
    message: str
    role_id: Optional[int] = None
    created_permissions: List[Dict[str, Any]] = Field(default_factory=list)
    ignored_modules: List[str] = Field(default_factory=list)

# Role Inactivation Models

class UserRoleAssignment(BaseModel):
    """Model for individual user role assignment"""
    nameOfUsers: str = Field(..., description="User's display name")
    role: str = Field(..., description="New role code to assign")

class RoleInactivationRequest(BaseModel):
    """Request model for inactivating a role with user reassignment"""
    role_id: str = Field(..., description="Role ID to inactivate (supports both 'ROLE0001' format and raw ID)")
    assignSame: bool = Field(..., description="Whether to assign the same role to all users")
    role: Optional[str] = Field(None, description="New role code (required when assignSame=true)")
    users: Optional[List[UserRoleAssignment]] = Field(None, description="List of user assignments (required when assignSame=false)")
    
    class Config:
        # Add validation to ensure required fields are present based on assignSame
        @staticmethod
        def validate_assign_same_fields(cls, values):
            assign_same = values.get('assignSame')
            role = values.get('role')
            users = values.get('users')
            
            if assign_same is True:
                if not role:
                    raise ValueError("role is required when assignSame is true")
            elif assign_same is False:
                if not users:
                    raise ValueError("users array is required when assignSame is false")
            
            return values

class RoleInactivationResponse(BaseModel):
    """Response model for role inactivation with user reassignment"""
    success: bool
    message: str
    data: Dict[str, Any]  # Contains role info, user count, and reassignment results

# Role Details Models

class RoleDetailField(BaseModel):
    """Model for role detail field"""
    label: str = Field(..., description="Field label")
    value: str = Field(..., description="Field value")
    sameLine: Optional[bool] = Field(False, description="Whether field should be on same line")
    width: Optional[str] = Field(None, description="Field width")
    type: Optional[str] = Field(None, description="Field type (e.g., 'status-badge')")

class RoleDetailSection(BaseModel):
    """Model for role detail section"""
    fields: List[RoleDetailField] = Field(..., description="List of fields in the section")

class RoleDetailButton(BaseModel):
    """Model for role detail button"""
    type: str = Field(..., description="Button type")
    buttonText: str = Field(..., description="Button text")
    buttonType: str = Field(..., description="Button type")
    buttonColor: str = Field(..., description="Button color")

class RoleDetailConfirmation(BaseModel):
    """Model for role detail confirmation"""
    title: str = Field(..., description="Confirmation title")
    description: str = Field(..., description="Confirmation description")
    buttonText: str = Field(..., description="Confirmation button text")
    buttonColor: str = Field(..., description="Confirmation button color")
    clickAction: Dict[str, Any] = Field(..., description="Click action details")

class RoleDetailOnConfirmation(BaseModel):
    """Model for role detail on confirmation"""
    title: str = Field(..., description="Confirmation title")
    description: str = Field(..., description="Confirmation description")
    buttonText: str = Field(..., description="Confirmation button text")
    buttonColor: str = Field(..., description="Confirmation button color")
    clickAction: Dict[str, Any] = Field(..., description="Click action details")

class RoleDetailFooterField(BaseModel):
    """Model for role detail footer field"""
    type: str = Field(..., description="Field type")
    buttonText: str = Field(..., description="Button text")
    buttonType: str = Field(..., description="Button type")
    buttonColor: str = Field(..., description="Button color")
    onConfirmation: RoleDetailOnConfirmation = Field(..., description="Confirmation details")

class RoleDetailFooter(BaseModel):
    """Model for role detail footer"""
    fields: List[RoleDetailFooterField] = Field(..., description="List of footer fields")

class RoleDetailPermission(BaseModel):
    """Model for role detail permission (base level modules only)"""
    module: str = Field(..., description="Module name")
    create: Optional[bool] = Field(None, description="Create permission")
    view: Optional[bool] = Field(None, description="View permission")
    update: Optional[bool] = Field(None, description="Update permission")
    delete: Optional[bool] = Field(None, description="Delete permission")
    # Note: _children are ignored as per requirements

class RoleDetailResponse(BaseModel):
    """Response model for role details"""
    title: str = Field(..., description="Page title")
    isEditable: bool = Field(..., description="Whether role is editable")
    onEditClick: Dict[str, Any] = Field(..., description="Edit click action")
    sections: List[RoleDetailSection] = Field(..., description="Role detail sections")
    makeInActive: bool = Field(..., description="Whether role can be made inactive")
    footer: RoleDetailFooter = Field(..., description="Role detail footer")
    users: List[str] = Field(..., description="List of users with this role")
    permissions: List[RoleDetailPermission] = Field(..., description="List of base-level module permissions")

# Update ModulePermission to handle self-reference
ModulePermission.model_rebuild()

# Export all models
__all__ = [
    'UserCreateRequest',
    'UserCreateRequestV2',
    'UserToggleStatusRequest',
    'UserToggleStatusResponse',
    'UnifiedUserCreateRequest',
    'UserUpdateRequest',
    'BulkUpdateRequest',
    'UserSearchRequest',
    'ClientInfo',
    'RoleInfo',
    'UserListItem',
    'UserDetail',
    'PaginationInfo',
    'UserListResponse',
    'UserDetailResponse',
    'UserCreateResponse',
    'UserCreateResponseV2',
    'BulkUserCreateResponseV2',
    'UserUpdateResponse',
    'UserDeleteResponse',
    'BulkUpdateResponse',
    'ClientListResponse',
    'RoleListResponse',
    'ModulePermission',
    'RolePermissionsRequest',
    'RolePermissionsResponse',
    'UserRoleAssignment',
    'RoleInactivationRequest',
    'RoleInactivationResponse',
    'RoleDetailField',
    'RoleDetailSection',
    'RoleDetailButton',
    'RoleDetailConfirmation',
    'RoleDetailOnConfirmation',
    'RoleDetailFooterField',
    'RoleDetailFooter',
    'RoleDetailPermission',
    'RoleDetailResponse'
]

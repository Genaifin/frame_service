#!/usr/bin/env python3
"""
Pydantic models for Client Management API
Defines request/response schemas for validation
"""

from pydantic import BaseModel, Field
from typing import List, Optional
from datetime import datetime

# Request Models

class ClientCreateRequest(BaseModel):
    """Request model for creating a new client"""
    client_name: str = Field(..., min_length=2, max_length=100, description="Client name")
    client_code: str = Field(..., min_length=2, max_length=50, description="Unique client code")
    description: Optional[str] = Field(None, description="Client description")
    is_active: bool = Field(default=True, description="Whether client is active")

class ClientUpdateRequest(BaseModel):
    """Request model for updating an existing client"""
    client_name: Optional[str] = Field(None, min_length=2, max_length=100, description="Client name")
    client_code: Optional[str] = Field(None, min_length=2, max_length=50, description="Unique client code")
    description: Optional[str] = Field(None, description="Client description")
    is_active: Optional[bool] = Field(None, description="Whether client is active")

class BulkClientUpdateRequest(BaseModel):
    """Request model for bulk updating clients"""
    client_ids: List[int] = Field(..., description="List of client IDs to update")
    updates: dict = Field(..., description="Fields to update for all clients")

class ClientSearchRequest(BaseModel):
    """Request model for searching clients"""
    search: Optional[str] = Field(None, description="Search term for client name or code")
    status_filter: Optional[str] = Field(None, description="Filter by status: 'active', 'inactive', or 'all'")
    page: int = Field(default=1, ge=1, description="Page number for pagination")
    page_size: int = Field(default=10, ge=1, le=100, description="Number of clients per page")

# Response Models

class UserInfo(BaseModel):
    """User information model for client associations"""
    id: int
    username: str
    display_name: str
    email: Optional[str] = None

class ClientListItem(BaseModel):
    """Client list item model for table display"""
    id: int
    client_name: str
    client_code: str
    description: Optional[str]
    status: str
    user_count: int
    created_at: Optional[str]
    updated_at: Optional[str]

class ClientDetail(BaseModel):
    """Detailed client information model"""
    id: int
    client_name: str
    client_code: str
    description: Optional[str]
    is_active: bool
    users: List[UserInfo]
    user_count: int
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

class ClientListResponse(BaseModel):
    """Response model for client list with pagination"""
    success: bool
    data: List[ClientListItem]
    pagination: PaginationInfo

class ClientDetailResponse(BaseModel):
    """Response model for single client details"""
    success: bool
    data: ClientDetail

class ClientCreateResponse(BaseModel):
    """Response model for client creation"""
    success: bool
    message: str
    data: dict

class ClientUpdateResponse(BaseModel):
    """Response model for client update"""
    success: bool
    message: str
    data: dict

class ClientDeleteResponse(BaseModel):
    """Response model for client deletion"""
    success: bool
    message: str

class BulkClientUpdateResponse(BaseModel):
    """Response model for bulk client update"""
    success: bool
    message: str
    updated_count: int

# Fund Assignment Models
class FundAssignmentItem(BaseModel):
    """Model for individual fund in assignment request"""
    fund_id: str = Field(..., description="Fund ID")
    fundName: Optional[str] = Field(None, description="Fund name")
    fundType: Optional[str] = Field(None, description="Fund type")
    contactName: Optional[str] = Field(None, description="Contact name")
    baseCurrency: Optional[str] = Field(None, description="Base currency")
    createdDate: Optional[str] = Field(None, description="Created date in MM/DD/YYYY format")
    status: Optional[str] = Field("Active", description="Fund status")

class FundAssignmentRequest(BaseModel):
    """Model for fund assignment request"""
    client_id: str = Field(..., description="Client ID")
    funds: List[FundAssignmentItem] = Field(..., min_items=1, description="List of funds to assign")

# Export all models
__all__ = [
    'ClientCreateRequest',
    'ClientUpdateRequest',
    'BulkClientUpdateRequest',
    'ClientSearchRequest',
    'UserInfo',
    'ClientListItem',
    'ClientDetail',
    'PaginationInfo',
    'ClientListResponse',
    'ClientDetailResponse',
    'ClientCreateResponse',
    'ClientUpdateResponse',
    'ClientDeleteResponse',
    'BulkClientUpdateResponse',
    'FundAssignmentItem',
    'FundAssignmentRequest'
]

#!/usr/bin/env python3
"""
Fund Management API Models
Defines request and response models for fund management operations
"""

from pydantic import BaseModel, Field, validator
from typing import List, Dict, Any, Optional
from datetime import datetime, date

class AdminInfo(BaseModel):
    """Model for fund admin or shadow admin information"""
    id: str = Field(..., description="Admin ID")
    type: str = Field("text", description="Admin type")
    label: str = Field(..., description="Admin label")
    placeholder: Optional[str] = Field(None, description="Placeholder text")
    isActive: bool = Field(True, description="Whether admin is active")
    value: Optional[str] = Field(None, description="Admin value/name")

class FundCreateRequest(BaseModel):
    """Request model for creating a new fund"""
    fund_name: str = Field(..., min_length=1, max_length=150, description="Fund name", alias="fund-name")
    fund_type: Optional[str] = Field(None, description="Fund type", alias="fund-type")
    fund_manager: Optional[str] = Field(None, description="Fund manager", alias="fund-manager")
    base_currency: Optional[str] = Field(None, description="Base currency", alias="base-currency")
    fund_admins: Optional[List[AdminInfo]] = Field(None, description="Fund administrators", alias="fund-admins")
    shadow_admins: Optional[List[AdminInfo]] = Field(None, description="Shadow administrators", alias="shadow-admins")
    title: Optional[str] = Field(None, description="Contact title", alias="title")
    first_name: Optional[str] = Field(None, description="Contact first name", alias="first-name")
    last_name: Optional[str] = Field(None, description="Contact last name", alias="last-name")
    email: Optional[str] = Field(None, description="Contact email", alias="email")
    contact_number: Optional[str] = Field(None, description="Contact number", alias="contact-number")
    sector: Optional[str] = Field(None, description="Fund sector", alias="sector")
    geography: Optional[str] = Field(None, description="Geographic focus", alias="geography")
    strategy: Optional[List[str]] = Field(None, description="Investment strategy", alias="strategy")
    market_cap: Optional[str] = Field(None, description="Market capitalization focus", alias="market-cap")
    benchmarks: Optional[List[str]] = Field(None, description="Benchmark indices", alias="benchmarks")
    
    # Lifecycle fields
    stage: Optional[str] = Field(None, description="Fund stage", alias="stage")
    inception_date: Optional[date] = Field(None, description="Inception date", alias="inception-date")
    investment_start_date: Optional[date] = Field(None, description="Investment start date", alias="investment-start-date")
    commitment_subscription: Optional[float] = Field(None, description="Commitment/subscription amount", alias="commitment-subscription")
    
    class Config:
        populate_by_name = True  # Allow both field names and aliases

class FundUpdateRequest(BaseModel):
    """Request model for updating an existing fund"""
    name: Optional[str] = Field(None, min_length=1, max_length=150, description="Fund name")
    code: Optional[str] = Field(None, min_length=1, max_length=80, description="Fund code")
    description: Optional[str] = Field(None, description="Fund description")
    is_active: Optional[bool] = Field(None, description="Whether the fund is active")
    
    # Lifecycle fields
    stage: Optional[str] = Field(None, description="Fund stage")
    inception_date: Optional[date] = Field(None, description="Inception date")
    investment_start_date: Optional[date] = Field(None, description="Investment start date")
    commitment_subscription: Optional[float] = Field(None, description="Commitment/subscription amount")

class FundSearchRequest(BaseModel):
    """Request model for searching funds"""
    search: Optional[str] = Field(None, description="Search term for fund name or code")
    status_filter: Optional[str] = Field(None, description="Filter by status (active/inactive)")
    page: int = Field(1, ge=1, description="Page number")
    page_size: int = Field(10, ge=1, le=100, description="Number of items per page")

class FundResponse(BaseModel):
    """Response model for fund data"""
    id: int
    fundManagerID: str  # Maps to fund code
    fundManagerName: str  # Maps to fund name
    contactName: Optional[str] = None  # Will be populated from related data
    createdDate: str  # Formatted date
    status: str  # Active/Inactive based on is_active

class FundDetailResponse(BaseModel):
    """Detailed response model for fund data"""
    id: int
    name: str
    code: str
    description: Optional[str]
    is_active: bool
    created_at: Optional[datetime]
    updated_at: Optional[datetime]

class FundListResponse(BaseModel):
    """Response model for fund list with pagination"""
    topNavBarParams: List[Dict[str, Any]]
    moduleDisplayConfig: List[Dict[str, Any]]

class BulkUpdateRequest(BaseModel):
    """Request model for bulk updating funds"""
    fund_ids: List[int] = Field(..., description="List of fund IDs to update")
    updates: FundUpdateRequest = Field(..., description="Updates to apply to all funds")

class FundStatsResponse(BaseModel):
    """Response model for fund statistics"""
    total_funds: int
    active_funds: int
    inactive_funds: int
    recent_funds: int  # Funds created in last 30 days

class FundManagerStatusRequest(BaseModel):
    """Request model for toggling fund manager status"""
    fundManagerId: int = Field(..., description="Fund Manager ID (integer)")
    active: bool = Field(..., description="True to activate, false to inactivate")

class FundManagerStatusResponse(BaseModel):
    """Response model for fund manager status toggle"""
    success: bool
    message: str
    fundManagerId: int
    active: bool

class AddFundManagerRequest(BaseModel):
    """Request model for adding a new fund manager"""
    fund_manager_name: str = Field(..., min_length=1, max_length=150, description="Fund Manager company name")
    title: Optional[str] = Field(None, description="Contact title (Mr., Mrs., Dr., etc.)")
    first_name: str = Field(..., min_length=1, max_length=50, description="Contact first name")
    last_name: str = Field(..., min_length=1, max_length=50, description="Contact last name")
    email: str = Field(..., description="Contact email address")
    contact_number: Optional[str] = Field(None, description="Contact phone number")

class AddFundManagerResponse(BaseModel):
    """Response model for adding a new fund manager"""
    success: bool
    message: str
    client_id: int
    user_id: int
    fund_manager_id: int  # Raw fund manager ID

class EditFundManagerRequest(BaseModel):
    """Request model for editing a fund manager (all fields optional for partial updates)"""
    fund_manager_name: Optional[str] = Field(None, min_length=1, max_length=150, description="Fund Manager company name")
    title: Optional[str] = Field(None, description="Contact title (Mr., Mrs., Dr., etc.)")
    first_name: Optional[str] = Field(None, min_length=1, max_length=50, description="Contact first name")
    last_name: Optional[str] = Field(None, min_length=1, max_length=50, description="Contact last name")
    email: Optional[str] = Field(None, description="Contact email address")
    contact_number: Optional[str] = Field(None, description="Contact phone number")
    status: Optional[str] = Field(None, description="Fund manager status - must be 'active' or 'inactive'")
    
    @validator('status')
    def validate_status(cls, v):
        if v is not None and v.lower() not in ['active', 'inactive']:
            raise ValueError('Status must be either "active" or "inactive"')
        return v.lower() if v is not None else v

class EditFundManagerResponse(BaseModel):
    """Response model for editing a fund manager"""
    success: bool
    message: str
    client_id: int
    fund_manager_id: int  # Raw fund manager ID
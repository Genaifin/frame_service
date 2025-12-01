#!/usr/bin/env python3
"""
Pydantic models for KPI Management API
"""

from pydantic import BaseModel, Field, validator, root_validator
from typing import List, Optional, Dict, Any
from decimal import Decimal
from datetime import datetime

class KpiCreateRequest(BaseModel):
    """Model for creating a new KPI with enhanced validation"""
    kpi_code: str = Field(..., min_length=1, max_length=100, description="Unique KPI code")
    kpi_name: str = Field(..., min_length=1, max_length=200, description="KPI display name")
    kpi_type: str = Field(..., description="KPI type: NAV_VALIDATION or RATIO_VALIDATION")
    category: Optional[str] = Field(None, max_length=100, description="KPI category")
    description: Optional[str] = Field(None, description="KPI description")
    source_type: str = Field(..., description="Source type: SINGLE_SOURCE or DUAL_SOURCE")
    precision_type: str = Field(..., description="Precision type: PERCENTAGE or ABSOLUTE")
    numerator_field: Optional[str] = Field(None, max_length=200, description="Numerator field (required for ratios)")
    denominator_field: Optional[str] = Field(None, max_length=200, description="Denominator field (required for ratios)")
    numerator_description: Optional[str] = Field(None, description="Numerator description")
    denominator_description: Optional[str] = Field(None, description="Denominator description")
    is_active: Optional[bool] = Field(True, description="Whether KPI is active")
    
    @validator('kpi_type')
    def validate_kpi_type(cls, v):
        valid_types = ['NAV_VALIDATION', 'RATIO_VALIDATION']
        if v not in valid_types:
            raise ValueError(f'kpi_type must be one of: {valid_types}')
        return v
    
    @validator('source_type')
    def validate_source_type(cls, v):
        valid_types = ['SINGLE_SOURCE', 'DUAL_SOURCE']
        if v not in valid_types:
            raise ValueError(f'source_type must be one of: {valid_types}')
        return v
    
    @validator('precision_type')
    def validate_precision_type(cls, v):
        valid_types = ['PERCENTAGE', 'ABSOLUTE']
        if v not in valid_types:
            raise ValueError(f'precision_type must be one of: {valid_types}')
        return v
    
    @root_validator(skip_on_failure=True)
    def validate_ratio_requirements(cls, values):
        """Validate that ratio validations have required fields"""
        kpi_type = values.get('kpi_type')
        numerator = values.get('numerator_field')
        denominator = values.get('denominator_field')
        
        if kpi_type == 'RATIO_VALIDATION':
            if not numerator or not denominator:
                raise ValueError('numerator_field and denominator_field are required for RATIO_VALIDATION')
        
        return values

class KpiUpdateRequest(BaseModel):
    """Model for updating an existing KPI"""
    kpi_code: Optional[str] = Field(None, min_length=1, max_length=100, description="Unique KPI code")
    kpi_name: Optional[str] = Field(None, min_length=1, max_length=200, description="KPI display name")
    kpi_type: Optional[str] = Field(None, description="KPI type: NAV_VALIDATION or RATIO_VALIDATION")
    category: Optional[str] = Field(None, max_length=100, description="KPI category")
    description: Optional[str] = Field(None, description="KPI description")
    source_type: Optional[str] = Field(None, description="Source type: SINGLE_SOURCE or DUAL_SOURCE")
    precision_type: Optional[str] = Field(None, description="Precision type: PERCENTAGE or ABSOLUTE")
    numerator_field: Optional[str] = Field(None, max_length=200, description="Numerator field (for ratios)")
    denominator_field: Optional[str] = Field(None, max_length=200, description="Denominator field (for ratios)")
    numerator_description: Optional[str] = Field(None, description="Numerator description")
    denominator_description: Optional[str] = Field(None, description="Denominator description")
    is_active: Optional[bool] = Field(None, description="Whether KPI is active")
    
    @validator('kpi_type')
    def validate_kpi_type(cls, v):
        if v is not None:
            valid_types = ['NAV_VALIDATION', 'RATIO_VALIDATION']
            if v not in valid_types:
                raise ValueError(f'kpi_type must be one of: {valid_types}')
        return v
    
    @validator('source_type')
    def validate_source_type(cls, v):
        if v is not None:
            valid_types = ['SINGLE_SOURCE', 'DUAL_SOURCE']
            if v not in valid_types:
                raise ValueError(f'source_type must be one of: {valid_types}')
        return v
    
    @validator('precision_type')
    def validate_precision_type(cls, v):
        if v is not None:
            valid_types = ['PERCENTAGE', 'ABSOLUTE']
            if v not in valid_types:
                raise ValueError(f'precision_type must be one of: {valid_types}')
        return v

class ThresholdCreateRequest(BaseModel):
    """Model for creating a new threshold"""
    kpi_id: int = Field(..., description="KPI ID this threshold belongs to")
    fund_id: Optional[str] = Field(None, max_length=100, description="Fund ID (null for global default)")
    threshold_value: float = Field(..., description="Threshold value")
    is_active: Optional[bool] = Field(True, description="Whether threshold is active")
    
    @validator('threshold_value')
    def validate_threshold_value(cls, v):
        if v < 0:
            raise ValueError('threshold_value must be non-negative')
        return v

class ThresholdUpdateRequest(BaseModel):
    """Model for updating an existing threshold"""
    fund_id: Optional[str] = Field(None, max_length=100, description="Fund ID (null for global default)")
    threshold_value: Optional[float] = Field(None, description="Threshold value")
    is_active: Optional[bool] = Field(None, description="Whether threshold is active")
    
    @validator('threshold_value')
    def validate_threshold_value(cls, v):
        if v is not None and v < 0:
            raise ValueError('threshold_value must be non-negative')
        return v

class KpiSearchRequest(BaseModel):
    """Model for KPI search parameters"""
    search: Optional[str] = Field(None, description="Search term")
    kpi_type: Optional[str] = Field(None, description="Filter by KPI type")
    category: Optional[str] = Field(None, description="Filter by category")
    is_active: Optional[bool] = Field(True, description="Filter by active status")
    page: Optional[int] = Field(1, ge=1, description="Page number")
    page_size: Optional[int] = Field(20, ge=1, le=100, description="Page size")
    
    @validator('kpi_type')
    def validate_kpi_type(cls, v):
        if v is not None:
            valid_types = ['NAV_VALIDATION', 'RATIO_VALIDATION']
            if v not in valid_types:
                raise ValueError(f'kpi_type must be one of: {valid_types}')
        return v

class BulkKpiUpdateRequest(BaseModel):
    """Model for bulk updating multiple KPIs"""
    kpi_ids: List[int] = Field(..., description="List of KPI IDs to update")
    updates: dict = Field(..., description="Fields to update for all KPIs")
    
    @validator('kpi_ids')
    def validate_kpi_ids(cls, v):
        if not v:
            raise ValueError('kpi_ids cannot be empty')
        if len(v) > 50:
            raise ValueError('Cannot update more than 50 KPIs at once')
        return v


# Add these new models at the end of the file

class KpiBulkItem(BaseModel):
    """Individual KPI item for bulk operations"""
    kpi_code: Optional[str] = Field(None, description="KPI code (for lookup)")
    kpi_id: Optional[int] = Field(None, description="KPI ID (for lookup)")
    kpi_name: str = Field(..., description="KPI name")
    kpi_type: str = Field(..., description="KPI type: NAV_VALIDATION or RATIO_VALIDATION")
    category: Optional[str] = Field(None, description="KPI category")
    description: Optional[str] = Field(None, description="KPI description")
    source_type: str = Field(..., description="Source type: SINGLE_SOURCE or DUAL_SOURCE")
    precision_type: str = Field(..., description="Precision type: PERCENTAGE or ABSOLUTE")
    numerator_field: Optional[str] = Field(None, description="Numerator field (required for ratio)")
    denominator_field: Optional[str] = Field(None, description="Denominator field (required for ratio)")
    numerator_description: Optional[str] = Field(None, description="Numerator description")
    denominator_description: Optional[str] = Field(None, description="Denominator description")
    is_active: Optional[bool] = Field(True, description="Whether KPI is active")
    
    @root_validator(skip_on_failure=True)
    def validate_lookup_field(cls, values):
        """Ensure either kpi_code or kpi_id is provided"""
        kpi_code = values.get('kpi_code')
        kpi_id = values.get('kpi_id')
        if not kpi_code and not kpi_id:
            raise ValueError('Either kpi_code or kpi_id must be provided')
        return values
    
    @root_validator(skip_on_failure=True)
    def validate_ratio_requirements(cls, values):
        """Validate that ratio validations have required fields"""
        kpi_type = values.get('kpi_type')
        numerator = values.get('numerator_field')
        denominator = values.get('denominator_field')
        if kpi_type == 'RATIO_VALIDATION':
            if not numerator or not denominator:
                raise ValueError('numerator_field and denominator_field are required for RATIO_VALIDATION')
        return values

class ThresholdBulkItem(BaseModel):
    """Individual threshold item for bulk operations"""
    threshold_id: Optional[int] = Field(None, description="Threshold ID (for updates)")
    kpi_code: Optional[str] = Field(None, description="KPI code to associate threshold with")
    kpi_id: Optional[int] = Field(None, description="KPI ID to associate threshold with")
    fund_id: Optional[str] = Field(None, description="Fund ID (null for global)")
    threshold_value: float = Field(..., description="Threshold value")
    is_active: Optional[bool] = Field(True, description="Whether threshold is active")
    
    @root_validator(skip_on_failure=True)
    def validate_kpi_reference(cls, values):
        """Ensure either kpi_code or kpi_id is provided"""
        kpi_code = values.get('kpi_code')
        kpi_id = values.get('kpi_id')
        if not kpi_code and not kpi_id:
            raise ValueError('Either kpi_code or kpi_id must be provided to associate threshold')
        return values

class KpiBulkUpdateRequest(BaseModel):
    """Model for bulk KPI and threshold operations"""
    kpis: List[KpiBulkItem] = Field(default=[], description="List of KPIs to create/update")
    thresholds: List[ThresholdBulkItem] = Field(default=[], description="List of thresholds to create/update")
    delete_kpi_codes: List[str] = Field(default=[], description="List of KPI codes to delete")

class KpiSubmitRequest(BaseModel):
    """Model for UI submit operation"""
    selected_kpis: List[int] = Field(..., description="Selected KPI IDs to activate")
    kpi_updates: List[dict] = Field(default=[], description="KPI modifications")
    threshold_updates: List[dict] = Field(default=[], description="Threshold modifications")
    new_thresholds: List[dict] = Field(default=[], description="New thresholds to create")

class KpiBulkResponse(BaseModel):
    """Response model for bulk operations"""
    success: bool
    message: str
    created_kpis: int = Field(default=0)
    updated_kpis: int = Field(default=0)
    created_thresholds: int = Field(default=0)
    updated_thresholds: int = Field(default=0)
    deleted_kpis: int = Field(default=0)
    errors: List[str] = Field(default=[])

# Response Models (following user/client patterns)
class ThresholdInfo(BaseModel):
    """Threshold information model"""
    id: int
    kpi_id: int
    fund_id: Optional[str] = None
    threshold_value: float
    is_active: bool
    created_at: Optional[datetime]
    updated_at: Optional[datetime]
    created_by: Optional[str]

class KpiListItem(BaseModel):
    """KPI list item model for table display"""
    id: int
    kpi_code: str
    kpi_name: str
    kpi_type: str
    category: Optional[str]
    source_type: str
    precision_type: str
    status: str
    threshold_count: int
    created_at: Optional[str]
    updated_at: Optional[str]

class KpiDetail(BaseModel):
    """Detailed KPI information model"""
    id: int
    kpi_code: str
    kpi_name: str
    kpi_type: str
    category: Optional[str]
    description: Optional[str]
    source_type: str
    precision_type: str
    numerator_field: Optional[str]
    denominator_field: Optional[str]
    numerator_description: Optional[str]
    denominator_description: Optional[str]
    is_active: bool
    thresholds: List[ThresholdInfo]
    created_at: Optional[datetime]
    updated_at: Optional[datetime]
    created_by: Optional[str]

class PaginationInfo(BaseModel):
    """Pagination information model"""
    current_page: int
    page_size: int
    total_count: int
    total_pages: int
    start_record: int
    end_record: int

class KpiListResponse(BaseModel):
    """Response model for KPI list with pagination"""
    success: bool
    data: List[KpiListItem]
    pagination: PaginationInfo

class KpiDetailResponse(BaseModel):
    """Response model for single KPI details"""
    success: bool
    data: KpiDetail

class KpiCreateResponse(BaseModel):
    """Response model for KPI creation"""
    success: bool
    message: str
    data: dict

class KpiUpdateResponse(BaseModel):
    """Response model for KPI update"""
    success: bool
    message: str
    data: dict

class KpiDeleteResponse(BaseModel):
    """Response model for KPI deletion"""
    success: bool
    message: str

class BulkKpiUpdateResponse(BaseModel):
    """Response model for bulk KPI update"""
    success: bool
    message: str
    updated_count: int

class ThresholdCreateResponse(BaseModel):
    """Response model for threshold creation"""
    success: bool
    message: str
    data: dict

class ThresholdUpdateResponse(BaseModel):
    """Response model for threshold update"""
    success: bool
    message: str
    data: dict

class ThresholdDeleteResponse(BaseModel):
    """Response model for threshold deletion"""
    success: bool
    message: str

class CategoryListResponse(BaseModel):
    """Response model for available categories"""
    success: bool
    data: List[str]

# Export all models
__all__ = [
    'KpiCreateRequest',
    'KpiUpdateRequest',
    'ThresholdCreateRequest',
    'ThresholdUpdateRequest',
    'KpiSearchRequest',
    'BulkKpiUpdateRequest',
    'ThresholdInfo',
    'KpiListItem',
    'KpiDetail',
    'PaginationInfo',
    'KpiListResponse',
    'KpiDetailResponse',
    'KpiCreateResponse',
    'KpiUpdateResponse',
    'KpiDeleteResponse',
    'BulkKpiUpdateResponse',
    'ThresholdCreateResponse',
    'ThresholdUpdateResponse',
    'ThresholdDeleteResponse',
    'CategoryListResponse'
]

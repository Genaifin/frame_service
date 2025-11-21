"""
Data Source API Models
Pydantic models for data source management endpoints
"""

from pydantic import BaseModel, Field, validator
from typing import List, Optional, Dict, Any, Union
from datetime import datetime


class S3SourceDetails(BaseModel):
    """S3 source configuration details"""
    shareName: str = Field(..., description="S3 share name")
    connectionString: str = Field(..., description="S3 connection string")
    details: Optional[str] = Field(None, description="Additional S3 details")


class EmailSourceDetails(BaseModel):
    """Email source configuration details"""
    smtpServer: str = Field(..., description="SMTP server address")
    port: str = Field(..., description="SMTP port")
    fromAddress: str = Field(..., description="From email address")
    username: str = Field(..., description="Email username")
    password: str = Field(..., description="Email password")
    details: Optional[str] = Field(None, description="Additional email details")


class ApiInvokeSourceDetails(BaseModel):
    """API Invoke source configuration details"""
    connectionCode: str = Field(..., description="Connection code")
    url: str = Field(..., description="API URL")
    payload: str = Field(..., description="Request payload")
    contentType: str = Field(..., description="Content type")
    method: str = Field(..., description="HTTP method")
    timeout: str = Field(..., description="Timeout in seconds")
    path: str = Field(..., description="API path")
    count: str = Field(..., description="Count parameter")
    recentBefore: str = Field(..., description="Recent before parameter")
    details: Optional[str] = Field(None, description="Additional API details")
    isOptional: bool = Field(False, description="Whether this API call is optional")


class PortalSourceDetails(BaseModel):
    """Portal source configuration details"""
    url: str = Field(..., description="Portal URL")
    username: str = Field(..., description="Portal username")
    password: str = Field(..., description="Portal password")
    scriptType: str = Field(..., description="Script type (e.g., 'js')")
    details: Optional[str] = Field(None, description="Additional portal details")
    scriptFile: dict = Field(default_factory=dict, description="Script file configuration")


class SFTPSourceDetails(BaseModel):
    """SFTP source configuration details"""
    connectionCode: str = Field(..., description="Connection code")
    sftpConnectionCode: str = Field(..., description="SFTP connection code")
    path: str = Field(..., description="SFTP path")
    count: str = Field(..., description="Count parameter")
    recentBefore: str = Field(..., description="Recent before parameter")
    destination: str = Field(..., description="Destination path")
    serverCheck: str = Field(..., description="Server check configuration")
    expectedAt: str = Field(..., description="Expected at time")
    sourceActionId: str = Field(..., description="Source action ID")
    details: Optional[str] = Field(None, description="Additional SFTP details")
    isOptional: bool = Field(False, description="Whether this SFTP operation is optional")
    includeHolidayFiles: bool = Field(False, description="Whether to include holiday files")


class AddDataSourceRequest(BaseModel):
    """Request model for adding a new data source"""
    frameDocuments: Optional[List[str]] = Field(None, description="Frame documents list")
    validusDocuments: Optional[List[str]] = Field(None, description="Validus documents list")
    frameDocument: Optional[List[str]] = Field(None, description="Frame document (singular) list")
    validusDocument: Optional[List[str]] = Field(None, description="Validus document (singular) list")
    name: str = Field(..., description="Data source name")
    source: str = Field(..., description="Source type (Email, S3 Bucket, Portal, API, SFTP)")
    holidayCalendar: str = Field(..., description="Holiday calendar (US, Europe)")
    s3: Optional[S3SourceDetails] = Field(None, description="S3 configuration details")
    email: Optional[EmailSourceDetails] = Field(None, description="Email configuration details")
    apiInvoke: Optional[ApiInvokeSourceDetails] = Field(None, description="API Invoke configuration details")
    portal: Optional[PortalSourceDetails] = Field(None, description="Portal configuration details")
    sftp: Optional[SFTPSourceDetails] = Field(None, description="SFTP configuration details")
    additional_details: Optional[str] = Field(None, description="Additional details")
    
    @validator('source')
    def validate_source(cls, v):
        # Map "API Invoke" to "API"
        source_mapping = {
            'API Invoke': 'API',
            'Email': 'Email',
            'S3 Bucket': 'S3 Bucket',
            'Portal': 'Portal',
            'API': 'API',
            'SFTP': 'SFTP'
        }
        
        if v not in source_mapping:
            allowed_sources = ['Email', 'S3 Bucket', 'Portal', 'API', 'SFTP', 'API Invoke']
            raise ValueError(f'Source must be one of: {", ".join(allowed_sources)}')
        
        return source_mapping[v]
    
    @validator('holidayCalendar')
    def validate_holiday_calendar(cls, v):
        # Map full names to codes
        calendar_mapping = {
            'US Holiday Calendar': 'US',
            'Europe Holiday Calendar': 'Europe',
            'US': 'US',
            'Europe': 'Europe'
        }
        
        if v not in calendar_mapping:
            allowed_calendars = ['US', 'Europe', 'US Holiday Calendar', 'Europe Holiday Calendar']
            raise ValueError(f'Holiday calendar must be one of: {", ".join(allowed_calendars)}')
        
        return calendar_mapping[v]
    
    @validator('s3')
    def validate_s3_source(cls, v, values):
        if v is not None and values.get('source') != 'S3 Bucket':
            raise ValueError('S3 details can only be provided when source is "S3 Bucket"')
        return v
    
    @validator('email')
    def validate_email_source(cls, v, values):
        if v is not None and values.get('source') != 'Email':
            raise ValueError('Email details can only be provided when source is "Email"')
        return v
    
    @validator('apiInvoke')
    def validate_api_invoke_source(cls, v, values):
        if v is not None and values.get('source') != 'API':
            raise ValueError('API Invoke details can only be provided when source is "API" or "API Invoke"')
        return v
    
    @validator('portal')
    def validate_portal_source(cls, v, values):
        if v is not None and values.get('source') != 'Portal':
            raise ValueError('Portal details can only be provided when source is "Portal"')
        return v
    
    @validator('sftp')
    def validate_sftp_source(cls, v, values):
        if v is not None and values.get('source') != 'SFTP':
            raise ValueError('SFTP details can only be provided when source is "SFTP"')
        return v


class DataSourceResponse(BaseModel):
    """Response model for data source operations"""
    id: int = Field(..., description="Data source ID")
    fund_id: Optional[int] = Field(None, description="Associated fund ID")
    name: str = Field(..., description="Data source name")
    source: str = Field(..., description="Source type")
    holiday_calendar: str = Field(..., description="Holiday calendar")
    source_details: Optional[Dict[str, Any]] = Field(None, description="Source-specific details")
    document_for: Optional[Dict[str, Any]] = Field(None, description="Document associations")
    additional_details: Optional[str] = Field(None, description="Additional details")
    is_active: bool = Field(..., description="Active status")
    created_at: Optional[datetime] = Field(None, description="Creation timestamp")
    updated_at: Optional[datetime] = Field(None, description="Last update timestamp")
    created_by: str = Field(..., description="Created by user")
    updated_by: Optional[str] = Field(None, description="Last updated by user")


class AddDataSourceResponse(BaseModel):
    """Response model for adding a data source"""
    success: bool = Field(..., description="Operation success status")
    message: str = Field(..., description="Response message")
    data_source: Optional[DataSourceResponse] = Field(None, description="Created data source details")
    fund_id: Optional[int] = Field(None, description="Associated fund ID")


class EditDataSourceRequest(BaseModel):
    """Request model for editing an existing data source"""
    frameDocuments: Optional[List[str]] = Field(None, description="Frame documents list")
    validusDocuments: Optional[List[str]] = Field(None, description="Validus documents list")
    frameDocument: Optional[List[str]] = Field(None, description="Frame document (singular) list")
    validusDocument: Optional[List[str]] = Field(None, description="Validus document (singular) list")
    name: str = Field(..., description="Data source name")
    source: str = Field(..., description="Source type (Email, S3 Bucket, Portal, API, SFTP)")
    holidayCalendar: str = Field(..., description="Holiday calendar (US, Europe)")
    s3: Optional[S3SourceDetails] = Field(None, description="S3 configuration details")
    email: Optional[EmailSourceDetails] = Field(None, description="Email configuration details")
    apiInvoke: Optional[ApiInvokeSourceDetails] = Field(None, description="API Invoke configuration details")
    portal: Optional[PortalSourceDetails] = Field(None, description="Portal configuration details")
    sftp: Optional[SFTPSourceDetails] = Field(None, description="SFTP configuration details")
    additional_details: Optional[str] = Field(None, description="Additional details")
    
    @validator('source')
    def validate_source(cls, v):
        # Map "API Invoke" to "API"
        source_mapping = {
            'API Invoke': 'API',
            'Email': 'Email',
            'S3 Bucket': 'S3 Bucket',
            'Portal': 'Portal', 
            'API': 'API',
            'SFTP': 'SFTP'
        }
        
        if v not in source_mapping:
            allowed_sources = ['Email', 'S3 Bucket', 'Portal', 'API', 'SFTP', 'API Invoke']
            raise ValueError(f'Source must be one of: {", ".join(allowed_sources)}')
        
        return source_mapping[v]
    
    @validator('holidayCalendar')
    def validate_holiday_calendar(cls, v):
        # Map full names to codes
        calendar_mapping = {
            'US Holiday Calendar': 'US',
            'Europe Holiday Calendar': 'Europe',
            'US': 'US',
            'Europe': 'Europe'
        }
        
        if v not in calendar_mapping:
            allowed_calendars = ['US', 'Europe', 'US Holiday Calendar', 'Europe Holiday Calendar']
            raise ValueError(f'Holiday calendar must be one of: {", ".join(allowed_calendars)}')
        
        return calendar_mapping[v]
    
    @validator('s3')
    def validate_s3_source(cls, v, values):
        if v is not None and values.get('source') != 'S3 Bucket':
            raise ValueError('S3 details can only be provided when source is "S3 Bucket"')
        return v
    
    @validator('email')
    def validate_email_source(cls, v, values):
        if v is not None and values.get('source') != 'Email':
            raise ValueError('Email details can only be provided when source is "Email"')
        return v
    
    @validator('apiInvoke')
    def validate_api_invoke_source(cls, v, values):
        if v is not None and values.get('source') != 'API':
            raise ValueError('API Invoke details can only be provided when source is "API" or "API Invoke"')
        return v
    
    @validator('portal')
    def validate_portal_source(cls, v, values):
        if v is not None and values.get('source') != 'Portal':
            raise ValueError('Portal details can only be provided when source is "Portal"')
        return v
    
    @validator('sftp')
    def validate_sftp_source(cls, v, values):
        if v is not None and values.get('source') != 'SFTP':
            raise ValueError('SFTP details can only be provided when source is "SFTP"')
        return v


class EditDataSourceResponse(BaseModel):
    """Response model for editing a data source"""
    success: bool = Field(..., description="Operation success status")
    message: str = Field(..., description="Response message")
    data_source: Optional[DataSourceResponse] = Field(None, description="Updated data source details")


class DataSourceListResponse(BaseModel):
    """Response model for listing data sources"""
    success: bool = Field(..., description="Operation success status")
    total: int = Field(..., description="Total number of data sources")
    page: int = Field(..., description="Current page number")
    page_size: int = Field(..., description="Number of items per page")
    data_sources: List[DataSourceResponse] = Field(..., description="List of data sources")


class DataSourceField(BaseModel):
    """Field model for data source details"""
    label: str = Field(..., description="Field label")
    value: str = Field(..., description="Field value")
    sameLine: bool = Field(True, description="Whether field should be on same line")


class DataSourceSection(BaseModel):
    """Section model for data source details"""
    title: Optional[str] = Field(None, description="Section title")
    fields: List[DataSourceField] = Field(..., description="Section fields")


class DropdownOptions(BaseModel):
    """Dropdown options for edit form"""
    sources: List[str] = Field(..., description="Available source types")
    holidayCalendar: List[str] = Field(..., description="Available holiday calendars")
    frameDocument: List[str] = Field(..., description="Available frame documents")
    validusDocument: List[str] = Field(..., description="Available validus documents")


class ClickAction(BaseModel):
    """Click action configuration"""
    type: str = Field(..., description="Action type")
    putAPIURL: str = Field(..., description="PUT API URL")
    actionAfterAPICall: Dict[str, Any] = Field(..., description="Action after API call")


class FormData(BaseModel):
    """Form data for edit form"""
    frameDocument: List[str] = Field(..., description="Frame documents")
    validusDocument: List[str] = Field(..., description="Validus documents")
    name: str = Field(..., description="Source name")
    source: str = Field(..., description="Source type")
    holidayCalendar: str = Field(..., description="Holiday calendar")
    shareName: Optional[str] = Field(None, description="Share name")
    connectionString: Optional[str] = Field(None, description="Connection string")
    details: Optional[str] = Field(None, description="Additional details")


class EditClickData(BaseModel):
    """Edit click data configuration"""
    dropdownOptions: DropdownOptions = Field(..., description="Dropdown options")
    open: bool = Field(True, description="Whether form is open")
    buttonType: str = Field(..., description="Button type")
    formData: FormData = Field(..., description="Form data")
    clickAction: ClickAction = Field(..., description="Click action")


class OnEditClick(BaseModel):
    """On edit click configuration"""
    type: str = Field(..., description="Click type")
    key: str = Field(..., description="Form key")
    data: EditClickData = Field(..., description="Edit click data")


class ConfirmationAction(BaseModel):
    """Confirmation action configuration"""
    title: str = Field(..., description="Confirmation title")
    description: str = Field(..., description="Confirmation description")
    buttonText: str = Field(..., description="Button text")
    buttonColor: str = Field(..., description="Button color")
    clickAction: Dict[str, Any] = Field(..., description="Click action")


class OnConfirmation(BaseModel):
    """On confirmation configuration"""
    title: str = Field(..., description="Confirmation title")
    description: str = Field(..., description="Confirmation description")
    buttonText: str = Field(..., description="Button text")
    buttonColor: str = Field(..., description="Button color")
    clickAction: Dict[str, Any] = Field(..., description="Click action")


class FooterField(BaseModel):
    """Footer field configuration"""
    type: str = Field(..., description="Field type")
    buttonText: str = Field(..., description="Button text")
    buttonType: str = Field(..., description="Button type")
    buttonColor: str = Field(..., description="Button color")
    onConfirmation: OnConfirmation = Field(..., description="On confirmation action")


class Footer(BaseModel):
    """Footer configuration"""
    fields: List[FooterField] = Field(..., description="Footer fields")


class DataSourceDetailResponse(BaseModel):
    """Response model for data source details with UI configuration"""
    title: str = Field(..., description="Page title")
    isEditable: bool = Field(True, description="Whether source is editable")
    onEditClick: OnEditClick = Field(..., description="Edit click configuration")
    sections: List[DataSourceSection] = Field(..., description="Data sections")
    footer: Footer = Field(..., description="Footer configuration")
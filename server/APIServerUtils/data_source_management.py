"""
Data Source Management Service
Handles data source CRUD operations and business logic
"""

import logging
from typing import Optional, Dict, Any, List
from datetime import datetime

from database_models import DatabaseManager, DataSource, Fund
from server.APIServerUtils.data_source_models import (
    AddDataSourceRequest, AddDataSourceResponse, DataSourceResponse, DataSourceListResponse,
    EditDataSourceRequest, EditDataSourceResponse
)

logger = logging.getLogger(__name__)


class DataSourceManagementService:
    """Service class for data source management operations"""
    
    def __init__(self):
        self.db_manager = DatabaseManager()
    
    async def add_data_source(
        self, 
        request: AddDataSourceRequest, 
        fund_id: int, 
        created_by: str
    ) -> AddDataSourceResponse:
        """
        Add a new data source with fund association
        
        Args:
            request: Data source creation request
            fund_id: Associated fund ID
            created_by: Username of the creator
            
        Returns:
            AddDataSourceResponse with operation result
        """
        try:
            # Validate fund exists
            fund = await self._validate_fund_exists(fund_id)
            if not fund:
                return AddDataSourceResponse(
                    success=False,
                    message=f"Fund with ID {fund_id} not found"
                )
            
            # Prepare document_for JSON
            document_for = self._prepare_document_for(request)
            
            # Prepare source_details JSON
            source_details = self._prepare_source_details(request)
            
            # Create data source data
            data_source_data = {
                'fund_id': fund_id,
                'name': request.name,
                'source': request.source,
                'holiday_calendar': request.holidayCalendar,
                'source_details': source_details,
                'document_for': document_for,
                'additional_details': request.additional_details,
                'created_by': created_by,
                'is_active': True
            }
            
            # Create data source
            data_source = self.db_manager.create_data_source(data_source_data)
            
            if not data_source:
                return AddDataSourceResponse(
                    success=False,
                    message="Failed to create data source"
                )
            
            # Convert to response model
            data_source_response = DataSourceResponse(
                id=data_source.id,
                fund_id=data_source.fund_id,
                name=data_source.name,
                source=data_source.source,
                holiday_calendar=data_source.holiday_calendar,
                source_details=data_source.source_details,
                document_for=data_source.document_for,
                additional_details=data_source.additional_details,
                is_active=data_source.is_active,
                created_at=data_source.created_at,
                updated_at=data_source.updated_at,
                created_by=data_source.created_by,
                updated_by=data_source.updated_by
            )
            
            return AddDataSourceResponse(
                success=True,
                message=f"Data source '{request.name}' created successfully",
                data_source=data_source_response,
                fund_id=fund_id
            )
            
        except Exception as e:
            logger.error(f"Error adding data source: {str(e)}")
            return AddDataSourceResponse(
                success=False,
                message=f"Error creating data source: {str(e)}"
            )
    
    async def _validate_fund_exists(self, fund_id: int) -> Optional[Fund]:
        """Validate that the fund exists and is active"""
        try:
            session = self.db_manager.get_session()
            try:
                fund = session.query(Fund).filter(
                    Fund.id == fund_id,
                    Fund.is_active == True
                ).first()
                return fund
            finally:
                session.close()
        except Exception as e:
            logger.error(f"Error validating fund: {str(e)}")
            return None
    
    def _prepare_document_for(self, request: AddDataSourceRequest) -> Dict[str, Any]:
        """Prepare document_for JSON from request"""
        document_for = {}
        
        # Handle frame documents (both plural and singular)
        frame_docs = request.frameDocuments or request.frameDocument or []
        if frame_docs:
            document_for['frameDocuments'] = frame_docs
        
        # Handle validus documents (both plural and singular)
        validus_docs = request.validusDocuments or request.validusDocument or []
        if validus_docs:
            document_for['validusDocuments'] = validus_docs
        
        return document_for if document_for else None
    
    def _prepare_source_details(self, request) -> Dict[str, Any]:
        """Prepare source_details JSON from request"""
        source_details = {}
        
        if request.source == 'S3 Bucket' and request.s3:
            source_details = {
                'shareName': request.s3.shareName,
                'connectionString': request.s3.connectionString,
                'details': request.s3.details
            }
        elif request.source == 'Email' and request.email:
            source_details = {
                'smtpServer': request.email.smtpServer,
                'port': request.email.port,
                'fromAddress': request.email.fromAddress,
                'username': request.email.username,
                'password': request.email.password,
                'details': request.email.details
            }
        elif request.source == 'API' and request.apiInvoke:
            source_details = {
                'connectionCode': request.apiInvoke.connectionCode,
                'url': request.apiInvoke.url,
                'payload': request.apiInvoke.payload,
                'contentType': request.apiInvoke.contentType,
                'method': request.apiInvoke.method,
                'timeout': request.apiInvoke.timeout,
                'path': request.apiInvoke.path,
                'count': request.apiInvoke.count,
                'recentBefore': request.apiInvoke.recentBefore,
                'details': request.apiInvoke.details,
                'isOptional': request.apiInvoke.isOptional
            }
        elif request.source == 'Portal' and request.portal:
            source_details = {
                'url': request.portal.url,
                'username': request.portal.username,
                'password': request.portal.password,
                'scriptType': request.portal.scriptType,
                'details': request.portal.details,
                'scriptFile': request.portal.scriptFile
            }
        elif request.source == 'SFTP' and request.sftp:
            source_details = {
                'connectionCode': request.sftp.connectionCode,
                'sftpConnectionCode': request.sftp.sftpConnectionCode,
                'path': request.sftp.path,
                'count': request.sftp.count,
                'recentBefore': request.sftp.recentBefore,
                'destination': request.sftp.destination,
                'serverCheck': request.sftp.serverCheck,
                'expectedAt': request.sftp.expectedAt,
                'sourceActionId': request.sftp.sourceActionId,
                'details': request.sftp.details,
                'isOptional': request.sftp.isOptional,
                'includeHolidayFiles': request.sftp.includeHolidayFiles
            }
        
        return source_details if source_details else None
    
    async def get_data_sources(
        self,
        page: int = 1,
        page_size: int = 20,
        search: Optional[str] = None,
        source_type: Optional[str] = None,
        holiday_calendar: Optional[str] = None,
        is_active: Optional[bool] = True
    ) -> DataSourceListResponse:
        """
        Get data sources with pagination and filtering
        
        Args:
            page: Page number
            page_size: Number of items per page
            search: Search term for name
            source_type: Filter by source type
            holiday_calendar: Filter by holiday calendar
            is_active: Filter by active status
            
        Returns:
            DataSourceListResponse with paginated results
        """
        try:
            # Get data sources from database
            data_sources = self.db_manager.get_all_data_sources(
                source_type=source_type,
                holiday_calendar=holiday_calendar
            )
            
            # Apply search filter
            if search:
                search_lower = search.lower()
                data_sources = [
                    ds for ds in data_sources 
                    if search_lower in ds.name.lower()
                ]
            
            # Apply active filter
            if is_active is not None:
                data_sources = [ds for ds in data_sources if ds.is_active == is_active]
            
            # Calculate pagination
            total = len(data_sources)
            start_idx = (page - 1) * page_size
            end_idx = start_idx + page_size
            paginated_sources = data_sources[start_idx:end_idx]
            
            # Convert to response models
            data_source_responses = [
                DataSourceResponse(
                    id=ds.id,
                    fund_id=ds.fund_id,
                    name=ds.name,
                    source=ds.source,
                    holiday_calendar=ds.holiday_calendar,
                    source_details=ds.source_details,
                    document_for=ds.document_for,
                    additional_details=ds.additional_details,
                    is_active=ds.is_active,
                    created_at=ds.created_at,
                    updated_at=ds.updated_at,
                    created_by=ds.created_by,
                    updated_by=ds.updated_by
                )
                for ds in paginated_sources
            ]
            
            return DataSourceListResponse(
                success=True,
                total=total,
                page=page,
                page_size=page_size,
                data_sources=data_source_responses
            )
            
        except Exception as e:
            logger.error(f"Error getting data sources: {str(e)}")
            return DataSourceListResponse(
                success=False,
                total=0,
                page=page,
                page_size=page_size,
                data_sources=[]
            )
    
    async def get_data_source_by_id(self, source_id: int) -> Optional[DataSourceResponse]:
        """Get a specific data source by ID"""
        try:
            data_source = self.db_manager.get_data_source_by_id(source_id)
            
            if not data_source:
                return None
            
            return DataSourceResponse(
                id=data_source.id,
                fund_id=data_source.fund_id,
                name=data_source.name,
                source=data_source.source,
                holiday_calendar=data_source.holiday_calendar,
                source_details=data_source.source_details,
                document_for=data_source.document_for,
                additional_details=data_source.additional_details,
                is_active=data_source.is_active,
                created_at=data_source.created_at,
                updated_at=data_source.updated_at,
                created_by=data_source.created_by,
                updated_by=data_source.updated_by
            )
            
        except Exception as e:
            logger.error(f"Error getting data source by ID: {str(e)}")
            return None
    
    async def edit_data_source(
        self, 
        source_id: int,
        request: EditDataSourceRequest, 
        updated_by: str
    ) -> EditDataSourceResponse:
        """
        Edit an existing data source
        
        Args:
            source_id: ID of the data source to edit
            request: Data source edit request
            updated_by: Username of the updater
            
        Returns:
            EditDataSourceResponse with operation result
        """
        try:
            # Check if data source exists
            existing_data_source = self.db_manager.get_data_source_by_id(source_id)
            if not existing_data_source:
                return EditDataSourceResponse(
                    success=False,
                    message=f"Data source with ID {source_id} not found"
                )
            
            # Prepare document_for JSON
            document_for = self._prepare_document_for(request)
            
            # Prepare source_details JSON
            source_details = self._prepare_source_details(request)
            
            # Update data source data
            update_data = {
                'name': request.name,
                'source': request.source,
                'holiday_calendar': request.holidayCalendar,
                'source_details': source_details,
                'document_for': document_for,
                'additional_details': request.additional_details,
                'updated_by': updated_by,
                'updated_at': datetime.now()
            }
            
            # Update data source
            updated_data_source = self.db_manager.update_data_source(source_id, update_data)
            
            if not updated_data_source:
                return EditDataSourceResponse(
                    success=False,
                    message="Failed to update data source"
                )
            
            # Convert to response model
            data_source_response = DataSourceResponse(
                id=updated_data_source.id,
                fund_id=updated_data_source.fund_id,
                name=updated_data_source.name,
                source=updated_data_source.source,
                holiday_calendar=updated_data_source.holiday_calendar,
                source_details=updated_data_source.source_details,
                document_for=updated_data_source.document_for,
                additional_details=updated_data_source.additional_details,
                is_active=updated_data_source.is_active,
                created_at=updated_data_source.created_at,
                updated_at=updated_data_source.updated_at,
                created_by=updated_data_source.created_by,
                updated_by=updated_data_source.updated_by
            )
            
            return EditDataSourceResponse(
                success=True,
                message=f"Data source '{request.name}' updated successfully",
                data_source=data_source_response
            )
            
        except Exception as e:
            logger.error(f"Error editing data source: {str(e)}")
            return EditDataSourceResponse(
                success=False,
                message=f"Error updating data source: {str(e)}"
            )
    
    async def delete_data_source(self, source_id: int) -> Dict[str, Any]:
        """
        Delete (soft delete) a data source
        
        Args:
            source_id: ID of the data source to delete
            
        Returns:
            Dictionary with success status and message
        """
        try:
            # Check if data source exists
            existing_data_source = self.db_manager.get_data_source_by_id(source_id)
            if not existing_data_source:
                return {
                    "success": False,
                    "message": f"Data source with ID {source_id} not found"
                }
            
            # Soft delete the data source
            success = self.db_manager.delete_data_source(source_id)
            
            if success:
                return {
                    "success": True,
                    "message": f"Data source '{existing_data_source.name}' deleted successfully"
                }
            else:
                return {
                    "success": False,
                    "message": "Failed to delete data source"
                }
            
        except Exception as e:
            logger.error(f"Error deleting data source: {str(e)}")
            return {
                "success": False,
                "message": f"Error deleting data source: {str(e)}"
            }
    
    async def get_data_sources_by_fund(self, fund_id: int) -> Dict[str, Any]:
        """
        Get all data sources for a specific fund, organized by Frame and Validus document types
        
        Args:
            fund_id: The fund ID to get data sources for
            
        Returns:
            Dictionary with rowDataForFrame and rowDataForValidus arrays
        """
        try:
            # Get all data sources for the fund filtered by fund_id
            fund_data_sources = self.db_manager.get_all_data_sources(fund_id=fund_id)
            
            row_data_for_frame = []
            row_data_for_validus = []
            
            for data_source in fund_data_sources:
                # Create base data source info
                base_info = {
                    "source_id": data_source.id,
                    "code": f"{data_source.source}#{data_source.id}",
                    "frameDocument": [],
                    "validusDocument": [],
                    "name": data_source.name,
                    "source": data_source.source,
                    "holidayCalendar": self._map_calendar_code_to_name(data_source.holiday_calendar),
                    "document": []
                }
                
                # Add document information
                if data_source.document_for:
                    if isinstance(data_source.document_for, dict):
                        frame_docs = data_source.document_for.get('frameDocuments', [])
                        validus_docs = data_source.document_for.get('validusDocuments', [])
                        
                        # Handle both plural and singular forms
                        if not frame_docs:
                            frame_docs = data_source.document_for.get('frameDocument', [])
                        if not validus_docs:
                            validus_docs = data_source.document_for.get('validusDocument', [])
                        
                        base_info["frameDocument"] = frame_docs
                        base_info["validusDocument"] = validus_docs
                        
                        # Create document list for display
                        all_docs = frame_docs + validus_docs
                        base_info["document"] = list(set(all_docs))  # Remove duplicates
                
                # Add source-specific details
                source_details = data_source.source_details or {}
                base_info.update(source_details)
                
                # Add additional details
                if data_source.additional_details:
                    base_info["details"] = data_source.additional_details
                
                # Determine which array to add to based on documents
                frame_docs = base_info.get("frameDocument", [])
                validus_docs = base_info.get("validusDocument", [])
                
                # Add to Frame array if it has Frame documents
                if frame_docs:
                    # Create Frame-specific object without validusDocument field
                    frame_info = {
                        "source_id": base_info["source_id"],
                        "code": base_info["code"],
                        "document": base_info["frameDocument"],
                        "name": base_info["name"],
                        "source": base_info["source"],
                        "holidayCalendar": base_info["holidayCalendar"]
                    }
                    
                    # Add source-specific fields if they exist
                    if "shareName" in base_info:
                        frame_info["shareName"] = base_info["shareName"]
                    if "connectionString" in base_info:
                        frame_info["connectionString"] = base_info["connectionString"]
                    if "details" in base_info:
                        frame_info["details"] = base_info["details"]
                    
                    row_data_for_frame.append(frame_info)
                
                # Add to Validus array if it has Validus documents
                if validus_docs:
                    # Create Validus-specific object without frameDocument field
                    validus_info = {
                        "source_id": base_info["source_id"],
                        "code": base_info["code"],
                        "document": base_info["validusDocument"],
                        "name": base_info["name"],
                        "source": base_info["source"],
                        "holidayCalendar": base_info["holidayCalendar"]
                    }
                    
                    # Add source-specific fields if they exist
                    if "smtpServer" in base_info:
                        validus_info["smtpServer"] = base_info["smtpServer"]
                    if "port" in base_info:
                        validus_info["port"] = base_info["port"]
                    if "fromAddress" in base_info:
                        validus_info["fromAddress"] = base_info["fromAddress"]
                    if "username" in base_info:
                        validus_info["username"] = base_info["username"]
                    if "password" in base_info:
                        validus_info["password"] = base_info["password"]
                    if "details" in base_info:
                        validus_info["details"] = base_info["details"]
                    
                    row_data_for_validus.append(validus_info)
            
            return {
                "data": {
                    "rowDataForFrame": row_data_for_frame,
                    "rowDataForValidus": row_data_for_validus
                }
            }
            
        except Exception as e:
            logger.error(f"Error getting data sources by fund: {str(e)}")
            return {
                "data": {
                    "rowDataForFrame": [],
                    "rowDataForValidus": []
                }
            }
    
    def _map_calendar_code_to_name(self, calendar_code: str) -> str:
        """Map calendar code to full name"""
        calendar_mapping = {
            'US': 'US Holiday Calendar',
            'Europe': 'Europe Holiday Calendar'
        }
        return calendar_mapping.get(calendar_code, calendar_code)


# Create service instance
data_source_service = DataSourceManagementService()

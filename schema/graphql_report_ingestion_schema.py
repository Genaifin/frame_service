#!/usr/bin/env python3
"""
GraphQL Schema for Report Ingestion - Report Ingested Section
Provides data for ingested reports grouped by sources/dates
"""

import strawberry
from typing import List, Optional
from strawberry.types import Info
from datetime import datetime, date
import logging
import os
import sys

# Add the project root to the path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database_models import get_database_manager, DataLoadInstance, DataModelMaster
from server.APIServerUtils.db_validation_service import DatabaseValidationService

# Import authentication context
from .graphql_auth_context import require_authentication
# Import error helpers
from .graphql_error_helpers import GraphQLError, get_error_message, handle_database_error

logger = logging.getLogger(__name__)

@strawberry.type
class IngestedFileType:
    """Type for ingested file information"""
    category: str
    file_name: str
    file_format: str
    source: str
    time: str
    status: str

@strawberry.type
class DataSectionGroupType:
    """Type for data section grouped by source/date"""
    group_key: str  # Source name or date
    files: List[IngestedFileType]

@strawberry.type
class VersionFileType:
    """Type for version file information"""
    file_name: str
    file_format: str
    source: str
    time: str
    status: str
    version: str  # Version number (1, 2, 3, etc.) for same fund, client, source, date, datamodel

@strawberry.type
class CategoryGroupType:
    """Type for category group in version section"""
    category: str  # Data model name
    files: List[VersionFileType]  # Files for this category

@strawberry.type
class VersionGroupType:
    """Type for version section grouped by source/date"""
    group_key: str  # Source name or date
    categories: List[CategoryGroupType]  # Grouped by category (data model)

@strawberry.type
class ReportIngestedType:
    """Type for report ingested response"""
    data_section: List[DataSectionGroupType]  # Grouped by source/date
    version_section: List[VersionGroupType]  # Grouped by source/date first, then by category

@strawberry.type
class DataLoadCombinationType:
    """Type for unique data load combination"""
    client_id: int
    fund_id: int
    source: str
    date: str  # ISO format date string

@strawberry.type
class ReportIngestionQuery:
    """Query class for report ingestion operations"""
    
    @strawberry.field
    def get_report_ingested(
        self,
        info: Info,
        client_id: int,
        fund_id: int,
        source_a: Optional[str] = None,
        source_b: Optional[str] = None,
        date_a: Optional[str] = None,
        date_b: Optional[str] = None
    ) -> Optional[ReportIngestedType]:
        """
        Get ingested reports for a particular client, fund, source(s) and date(s)
        
        Args:
            client_id: Client ID
            fund_id: Fund ID
            source_a: Source A (required for dual source)
            source_b: Source B (optional, for dual source)
            date_a: Date A (required)
            date_b: Date B (optional, for single source with 2 dates)
        
        Returns:
            ReportIngestedType with data_section and version_section
        """
        require_authentication(info)
        
        try:
            db_service = DatabaseValidationService()
            result = db_service.get_report_ingested_data(
                client_id=client_id,
                fund_id=fund_id,
                source_a=source_a,
                source_b=source_b,
                date_a=date_a,
                date_b=date_b
            )
            
            if not result:
                return None
            
            # Convert to GraphQL types
            data_section = [
                DataSectionGroupType(
                    group_key=group['group_key'],
                    files=[
                        IngestedFileType(
                            category=file['category'],
                            file_name=file['file_name'],
                            file_format=file['file_format'],
                            source=file['source'],
                            time=file['time'],
                            status=file['status']
                        )
                        for file in group['files']
                    ]
                )
                for group in result.get('data_section', [])
            ]
            
            version_section = [
                VersionGroupType(
                    group_key=group['group_key'],
                    categories=[
                        CategoryGroupType(
                            category=cat['category'],
                            files=[
                                VersionFileType(
                                    file_name=file['file_name'],
                                    file_format=file['file_format'],
                                    source=file['source'],
                                    time=file['time'],
                                    status=file['status'],
                                    version=file['version']
                                )
                                for file in cat['files']
                            ]
                        )
                        for cat in group['categories']
                    ]
                )
                for group in result.get('version_section', [])
            ]
            
            return ReportIngestedType(
                data_section=data_section,
                version_section=version_section
            )
            
        except GraphQLError:
            raise
        except Exception as e:
            logger.error(f"Error fetching report ingested data: {str(e)}")
            import traceback
            traceback.print_exc()
            raise GraphQLError(get_error_message('DATABASE_ERROR'))
    
    @strawberry.field
    def get_data_load_combinations(
        self,
        info: Info,
        client_id: Optional[int] = None,
        fund_id: Optional[int] = None
    ) -> List[DataLoadCombinationType]:
        """
        Get unique combinations of (client, fund, source, date) from tbl_data_load_instance
        
        Args:
            client_id: Optional client ID to filter by
            fund_id: Optional fund ID to filter by
        
        Returns:
            List of unique combinations
        """
        require_authentication(info)
        
        try:
            db_service = DatabaseValidationService()
            result = db_service.get_unique_data_load_combinations(
                client_id=client_id,
                fund_id=fund_id
            )
            
            return [
                DataLoadCombinationType(
                    client_id=combo['client_id'],
                    fund_id=combo['fund_id'],
                    source=combo['source'],
                    date=combo['date']
                )
                for combo in result
            ]
            
        except GraphQLError:
            raise
        except Exception as e:
            logger.error(f"Error fetching data load combinations: {str(e)}")
            import traceback
            traceback.print_exc()
            raise GraphQLError(get_error_message('DATABASE_ERROR'))


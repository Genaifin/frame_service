#!/usr/bin/env python3
"""
GraphQL Schema for Validation and Ratio Management
Manages subproducts, subproduct details, validations, and ratios
"""

import strawberry
from typing import List, Optional
from strawberry.types import Info
from sqlalchemy import text
from sqlalchemy.orm import joinedload
from database_models import get_database_manager, SubproductMaster, SubproductDetails, ValidationMaster, RatioMaster, ValidationDetails, RatioDetails, DataModelMaster, DataModelDetails, ValidationConfiguration, RatioConfiguration, Client, Fund, DataLoadInstance
from server.APIServerUtils.db_validation_service import DatabaseValidationService
from datetime import datetime
import logging

# Import authentication context
from .graphql_auth_context import require_authentication, require_role, get_current_user
# Import DataModelColumnType from table schema to avoid duplication
from .graphql_table_schema import DataModelColumnType, _createTableFromDataModel
# Import common types
from .graphql_common_types import PaginationInfoType, check_validation_name_duplicate, check_ratio_name_duplicate
# Import error helpers
from .graphql_error_helpers import GraphQLError, get_error_message, handle_database_error, format_error_message

logger = logging.getLogger(__name__)

# ==================== GraphQL Types ====================

@strawberry.type
class SubproductMasterType:
    """GraphQL type for subproduct master"""
    intsubproductid: int
    vcsubproductname: str
    vcdescription: Optional[str]
    isactive: bool
    intcreatedby: Optional[int]
    dtcreatedat: Optional[str]
    intupdatedby: Optional[int]
    dtupdatedat: Optional[str]

@strawberry.type
class SubproductDetailsType:
    """GraphQL type for subproduct details"""
    intsubproductdetailid: int
    intsubproductid: int
    vcvalidustype: Optional[str]
    vctype: Optional[str]
    vcsubtype: Optional[str]
    vcdescription: Optional[str]
    isactive: bool
    intcreatedby: Optional[int]
    dtcreatedat: Optional[str]
    intupdatedby: Optional[int]
    dtupdatedat: Optional[str]

@strawberry.type
class ValidationMasterType:
    """GraphQL type for validation master"""
    intvalidationmasterid: int
    intsubproductid: int
    vcsourcetype: Optional[str]
    vctype: Optional[str]
    vcsubtype: Optional[str]
    issubtype_subtotal: Optional[bool]
    vcvalidationname: Optional[str]
    isvalidation_subtotal: Optional[bool]
    vcdescription: Optional[str]
    intthreshold: Optional[float]
    vcthresholdtype: Optional[str]
    vcthreshold_abs_range: Optional[str]
    intthresholdmin: Optional[float]
    intthresholdmax: Optional[float]
    intprecision: Optional[float]
    isactive: bool
    intcreatedby: Optional[int]
    dtcreatedat: Optional[str]
    intupdatedby: Optional[int]
    dtupdatedat: Optional[str]

@strawberry.type
class RatioMasterType:
    """GraphQL type for ratio master"""
    intratiomasterid: int
    intsubproductid: int
    vcsourcetype: Optional[str]
    vctype: Optional[str]
    vcrationame: Optional[str]
    isratio_subtotal: Optional[bool]
    vcdescription: Optional[str]
    intthreshold: Optional[float]
    vcthresholdtype: Optional[str]
    vcthreshold_abs_range: Optional[str]
    intthresholdmin: Optional[float]
    intthresholdmax: Optional[float]
    intprecision: Optional[float]
    isactive: bool
    intcreatedby: Optional[int]
    dtcreatedat: Optional[str]
    intupdatedby: Optional[int]
    dtupdatedat: Optional[str]

@strawberry.type
class ValidationDetailsType:
    """GraphQL type for validation details"""
    intvalidationdetailid: int
    intvalidationmasterid: int
    intdatamodelid: str  # Changed to str to support ranged validations (can contain comma-separated IDs)
    intgroup_attributeid: Optional[int]
    intassettypeid: Optional[int]
    intcalc_attributeid: Optional[int]
    vcaggregationtype: Optional[str]
    vcfilter: Optional[str]
    vcfiltertype: Optional[str]
    vcformula: Optional[str]
    intcreatedby: Optional[int]
    dtcreatedat: Optional[str]
    intupdatedby: Optional[int]
    dtupdatedat: Optional[str]

@strawberry.type
class RatioDetailsType:
    """GraphQL type for ratio details"""
    intratiodetailid: int
    intratiomasterid: int
    intdatamodelid: str  # Changed to str to support ranged ratios (can contain comma-separated IDs)
    vcfilter: Optional[str]
    vcfiltertype: Optional[str]
    vcnumerator: Optional[str]
    vcdenominator: Optional[str]
    vcformula: Optional[str]
    intcreatedby: Optional[int]
    dtcreatedat: Optional[str]
    intupdatedby: Optional[int]
    dtupdatedat: Optional[str]

@strawberry.type
class ValidationWithDetailsType:
    """GraphQL type for validation master with details"""
    intvalidationmasterid: int
    intsubproductid: int
    vcsourcetype: Optional[str]
    vctype: Optional[str]
    vcsubtype: Optional[str]
    issubtype_subtotal: Optional[bool]
    vcvalidationname: Optional[str]
    isvalidation_subtotal: Optional[bool]
    vcdescription: Optional[str]
    intthreshold: Optional[float]
    vcthresholdtype: Optional[str]
    vcthreshold_abs_range: Optional[str]
    intthresholdmin: Optional[float]
    intthresholdmax: Optional[float]
    intprecision: Optional[float]
    isactive: bool
    intcreatedby: Optional[int]
    dtcreatedat: Optional[str]
    intupdatedby: Optional[int]
    dtupdatedat: Optional[str]
    details: List[ValidationDetailsType]

@strawberry.type
class RatioWithDetailsType:
    """GraphQL type for ratio master with details"""
    intratiomasterid: int
    intsubproductid: int
    vcsourcetype: Optional[str]
    vctype: Optional[str]
    vcrationame: Optional[str]
    isratio_subtotal: Optional[bool]
    vcdescription: Optional[str]
    intthreshold: Optional[float]
    vcthresholdtype: Optional[str]
    vcthreshold_abs_range: Optional[str]
    intthresholdmin: Optional[float]
    intthresholdmax: Optional[float]
    intprecision: Optional[float]
    isactive: bool
    intcreatedby: Optional[int]
    dtcreatedat: Optional[str]
    intupdatedby: Optional[int]
    dtupdatedat: Optional[str]
    details: List[RatioDetailsType]

@strawberry.type
class ClientInfoType:
    """Simple client information"""
    id: Optional[int]
    name: Optional[str]

@strawberry.type
class FundInfoType:
    """Simple fund information"""
    id: Optional[int]
    name: Optional[str]

@strawberry.type
class SubproductInfoType:
    """Simple subproduct information"""
    intsubproductid: Optional[int]
    vcsubproductname: Optional[str]

@strawberry.type
class ValidationConfigurationType:
    """GraphQL type for validation configuration"""
    intvalidationconfigurationid: int
    intclientid: Optional[int]
    intfundid: Optional[int]
    intvalidationmasterid: int
    isactive: bool
    vccondition: Optional[str]
    intthreshold: Optional[float]
    vcthresholdtype: Optional[str]
    vcthreshold_abs_range: Optional[str]
    intthresholdmin: Optional[float]
    intthresholdmax: Optional[float]
    intprecision: Optional[float]
    intcreatedby: Optional[int]
    dtcreatedat: Optional[str]
    intupdatedby: Optional[int]
    dtupdatedat: Optional[str]
    # Related information
    subproduct: Optional[SubproductInfoType]
    client: Optional[ClientInfoType]
    fund: Optional[FundInfoType]
    vcvalidationname: Optional[str]

@strawberry.type
class RatioConfigurationType:
    """GraphQL type for ratio configuration"""
    intratioconfigurationid: int
    intclientid: Optional[int]
    intfundid: Optional[int]
    intratiomasterid: int
    isactive: bool
    vccondition: Optional[str]
    intthreshold: Optional[float]
    vcthresholdtype: Optional[str]
    vcthreshold_abs_range: Optional[str]
    intthresholdmin: Optional[float]
    intthresholdmax: Optional[float]
    intprecision: Optional[float]
    intcreatedby: Optional[int]
    dtcreatedat: Optional[str]
    intupdatedby: Optional[int]
    dtupdatedat: Optional[str]
    # Related information
    subproduct: Optional[SubproductInfoType]
    client: Optional[ClientInfoType]
    fund: Optional[FundInfoType]
    vcrationame: Optional[str]

@strawberry.type
class DataModelSimpleType:
    """GraphQL type for simple data model (for dropdowns)"""
    intdatamodelid: int
    vcmodelname: str
    vcdescription: Optional[str]

@strawberry.type
class CreateValidationResultType:
    """Result of validation creation"""
    success: bool
    message: str
    validation: Optional[ValidationMasterType]

@strawberry.type
class CreateRatioResultType:
    """Result of ratio creation"""
    success: bool
    message: str
    ratio: Optional[RatioMasterType]

@strawberry.type
class UpdateValidationResultType:
    """Result of validation update"""
    success: bool
    message: str
    validation: Optional[ValidationMasterType]

@strawberry.type
class UpdateRatioResultType:
    """Result of ratio update"""
    success: bool
    message: str
    ratio: Optional[RatioMasterType]

@strawberry.type
class ValidationInSubcheckType:
    """GraphQL type for validation within a subcheck"""
    validationName: str
    description: Optional[str]
    status: str  # "Completed" or "Not Completed"
    passFail: str  # "Pass" or "Fail"
    datetime: Optional[str]  # ISO format datetime string

@strawberry.type
class SubcheckType:
    """GraphQL type for subcheck (validation group by subtype)"""
    subtype: str
    status: str  # "Completed" or "Not Completed"
    validations: List[ValidationInSubcheckType]

@strawberry.type
class ProcessInstanceSummaryType:
    """GraphQL type for process instance summary"""
    validation_total: int
    validation_failed: int
    validation_passed: int
    validation_exceptions: int
    ratio_total: int
    ratio_failed: int
    ratio_passed: int
    validation_process_instance_id: Optional[int]
    ratio_process_instance_id: Optional[int]
    subchecks: Optional[List[SubcheckType]] = None

@strawberry.type
class ValidationAggregatedDataType:
    """GraphQL type for validation aggregated data"""
    vcvalidationname: Optional[str]
    type: Optional[str]
    subtype: Optional[str]
    config_threshold: Optional[float]
    status: str
    exception: int

@strawberry.type
class ValidationComparisonDataType:
    """GraphQL type for validation comparison data with side A and B"""
    intprocessinstanceid: int
    validations: str
    intmatchid: Optional[int]
    # Dynamic fields will be in a JSON object
    data: strawberry.scalars.JSON

@strawberry.type
class RatioComparisonDataType:
    """GraphQL type for ratio comparison data with side A and B"""
    intprocessinstanceid: int
    ratios: str
    intmatchid: Optional[int]
    # Dynamic fields will be in a JSON object
    data: strawberry.scalars.JSON

@strawberry.type
class DataLoadDateType:
    """GraphQL type for data load dates"""
    date: str  # ISO format date string (dtdataasof)

@strawberry.type
class DeleteValidationResultType:
    """Result of validation deletion"""
    success: bool
    message: str

@strawberry.type
class DeleteRatioResultType:
    """Result of ratio deletion"""
    success: bool
    message: str

@strawberry.type
class CreateValidationConfigurationResultType:
    """Result of validation configuration creation"""
    success: bool
    message: str
    validationconfiguration: Optional[ValidationConfigurationType]

@strawberry.type
class CreateRatioConfigurationResultType:
    """Result of ratio configuration creation"""
    success: bool
    message: str
    ratioconfiguration: Optional[RatioConfigurationType]

@strawberry.type
class UpdateValidationConfigurationResultType:
    """Result of validation configuration update"""
    success: bool
    message: str
    validationconfiguration: Optional[ValidationConfigurationType]

@strawberry.type
class UpdateRatioConfigurationResultType:
    """Result of ratio configuration update"""
    success: bool
    message: str
    ratioconfiguration: Optional[RatioConfigurationType]

@strawberry.type
class DeleteValidationConfigurationResultType:
    """Result of validation configuration deletion"""
    success: bool
    message: str

@strawberry.type
class DeleteRatioConfigurationResultType:
    """Result of ratio configuration deletion"""
    success: bool
    message: str

@strawberry.type
class BulkUpsertValidationConfigResultType:
    """Result of bulk upsert of validation configurations"""
    success: bool
    message: str
    createdCount: int
    updatedCount: int
    skippedCount: int
    configurations: List[ValidationConfigurationType]

@strawberry.type
class BulkUpsertRatioConfigResultType:
    """Result of bulk upsert of ratio configurations"""
    success: bool
    message: str
    createdCount: int
    updatedCount: int
    skippedCount: int
    configurations: List[RatioConfigurationType]

@strawberry.type
class PaginatedValidationResponseType:
    """Paginated response for validations"""
    validations: List[ValidationMasterType]
    pagination: PaginationInfoType

@strawberry.type
class ValidationWithConfigurationsType:
    """Validation with its configuration fields (flat structure)"""
    # All validation master fields
    intvalidationmasterid: int
    intsubproductid: int
    vcsourcetype: Optional[str]
    vctype: Optional[str]
    vcsubtype: Optional[str]
    issubtypeSubtotal: Optional[bool]
    vcvalidationname: Optional[str]
    isvalidationSubtotal: Optional[bool]
    vcdescription: Optional[str]
    intthreshold: Optional[float]
    vcthresholdtype: Optional[str]
    vcthreshold_abs_range: Optional[str]
    intthresholdmin: Optional[float]
    intthresholdmax: Optional[float]
    intprecision: Optional[float]
    isactive: bool
    intcreatedby: Optional[int]
    dtcreatedat: Optional[str]
    intupdatedby: Optional[int]
    dtupdatedat: Optional[str]
    # Configuration fields (null if no config exists for client/fund)
    configIsactive: Optional[bool]
    vccondition: Optional[str]
    configThreshold: Optional[float]
    configThresholdtype: Optional[str]
    configThreshold_abs_range: Optional[str]
    configThresholdmin: Optional[float]
    configThresholdmax: Optional[float]

@strawberry.type
class RatioWithConfigurationsType:
    """Ratio with its configuration fields (flat structure)"""
    # All ratio master fields
    intratiomasterid: int
    intsubproductid: int
    vcsourcetype: Optional[str]
    vctype: Optional[str]
    vcrationame: Optional[str]
    isratioSubtotal: Optional[bool]
    vcdescription: Optional[str]
    intthreshold: Optional[float]
    vcthresholdtype: Optional[str]
    vcthreshold_abs_range: Optional[str]
    intthresholdmin: Optional[float]
    intthresholdmax: Optional[float]
    intprecision: Optional[float]
    isactive: bool
    intcreatedby: Optional[int]
    dtcreatedat: Optional[str]
    intupdatedby: Optional[int]
    dtupdatedat: Optional[str]
    # Configuration fields (null if no config exists for client/fund)
    configIsactive: Optional[bool]
    vccondition: Optional[str]
    configThreshold: Optional[float]
    configThresholdtype: Optional[str]
    configThreshold_abs_range: Optional[str]
    configThresholdmin: Optional[float]
    configThresholdmax: Optional[float]

@strawberry.type
class PaginatedRatioResponseType:
    """Paginated response for ratios"""
    ratios: List[RatioMasterType]
    pagination: PaginationInfoType


@strawberry.type
class PaginatedRatioConfigurationResponseType:
    """Paginated response for ratio configurations"""
    ratioconfigurations: List[RatioConfigurationType]
    pagination: PaginationInfoType

class ValidationConfigurationResponseType:
    """Response for validation configuration"""
    validationconfiguration: ValidationConfigurationType

@strawberry.input
class ValidationConfigurationInput:
    """Input for validation configuration"""
    intclientid: Optional[int] = None
    intfundid: Optional[int] = None
    intvalidationmasterid: int
    isactive: Optional[bool] = False
    vccondition: Optional[str] = None
    intthreshold: Optional[float] = None
    vcthresholdtype: Optional[str] = None
    vcthreshold_abs_range: Optional[str] = None
    intthresholdmin: Optional[float] = None
    intthresholdmax: Optional[float] = None
    intprecision: Optional[float] = None

@strawberry.input
class RatioConfigurationInput:
    """Input for ratio configuration"""
    intclientid: Optional[int] = None
    intfundid: Optional[int] = None
    intratiomasterid: int
    isactive: Optional[bool] = False
    vccondition: Optional[str] = None
    intthreshold: Optional[float] = None
    vcthresholdtype: Optional[str] = None
    vcthreshold_abs_range: Optional[str] = None
    intthresholdmin: Optional[float] = None
    intthresholdmax: Optional[float] = None
    intprecision: Optional[float] = None

@strawberry.input
class UpdateRatioConfigurationInput:
    """Input for updating ratio configuration"""
    intratioconfigurationid: int
    intclientid: Optional[int] = None
    intfundid: Optional[int] = None
    intratiomasterid: Optional[int] = None
    isactive: Optional[bool] = None
    vccondition: Optional[str] = None
    intthreshold: Optional[float] = None
    vcthresholdtype: Optional[str] = None
    vcthreshold_abs_range: Optional[str] = None
    intthresholdmin: Optional[float] = None
    intthresholdmax: Optional[float] = None
    intprecision: Optional[float] = None

@strawberry.input
class UpdateValidationConfigurationInput:
    """Input for updating validation configuration"""
    intvalidationconfigurationid: int
    intclientid: Optional[int] = None
    intfundid: Optional[int] = None
    intvalidationmasterid: Optional[int] = None
    vccondition: Optional[str] = None
    intthreshold: Optional[float] = None
    vcthresholdtype: Optional[str] = None
    vcthreshold_abs_range: Optional[str] = None
    intthresholdmin: Optional[float] = None
    intthresholdmax: Optional[float] = None
    intprecision: Optional[float] = None
    isactive: Optional[bool] = None

#pagination for validation configuration
@strawberry.type
class PaginatedValidationConfigurationResponseType:
    """Paginated response for validation configurations"""
    validationconfigurations: List[ValidationConfigurationType]
    pagination: PaginationInfoType

@strawberry.input
class BulkValidationConfigurationItemInput:
    """Input item for bulk validation configuration upsert"""
    intvalidationmasterid: int
    intclientid: Optional[int] = None
    intfundid: Optional[int] = None
    vcsourcetype: Optional[str] = None
    isactive: Optional[bool] = None
    vccondition: Optional[str] = None
    intthreshold: Optional[float] = None
    vcthresholdtype: Optional[str] = None
    vcthreshold_abs_range: Optional[str] = None
    intthresholdmin: Optional[float] = None
    intthresholdmax: Optional[float] = None
    intprecision: Optional[float] = None

@strawberry.input
class BulkValidationConfigurationUpsertInput:
    """Bulk payload for validation configuration upsert"""
    items: List[BulkValidationConfigurationItemInput]

@strawberry.input
class BulkRatioConfigurationItemInput:
    """Input item for bulk ratio configuration upsert"""
    intratiomasterid: int
    intclientid: Optional[int] = None
    intfundid: Optional[int] = None
    vcsourcetype: Optional[str] = None
    isactive: Optional[bool] = None
    vccondition: Optional[str] = None
    intthreshold: Optional[float] = None
    vcthresholdtype: Optional[str] = None
    vcthreshold_abs_range: Optional[str] = None
    intthresholdmin: Optional[float] = None
    intthresholdmax: Optional[float] = None
    intprecision: Optional[float] = None

@strawberry.input
class BulkRatioConfigurationUpsertInput:
    """Bulk payload for ratio configuration upsert"""
    items: List[BulkRatioConfigurationItemInput]


# ==================== GraphQL Inputs ====================

@strawberry.input
class ValidationMasterInput:
    """Input for creating validation entry"""
    intsubproductid: int
    vcsourcetype: Optional[str] = None
    vctype: Optional[str] = None
    vcsubtype: Optional[str] = None
    issubtype_subtotal: Optional[bool] = None
    vcvalidationname: Optional[str] = None
    isvalidation_subtotal: Optional[bool] = None
    vcdescription: Optional[str] = None
    intthreshold: Optional[float] = None
    vcthresholdtype: Optional[str] = None
    vcthreshold_abs_range: Optional[str] = None
    intthresholdmin: Optional[float] = None
    intthresholdmax: Optional[float] = None
    intprecision: Optional[float] = None
    isactive: Optional[bool] = None

@strawberry.input
class RatioMasterInput:
    """Input for creating ratio entry"""
    intsubproductid: int
    vcsourcetype: Optional[str] = None
    vctype: Optional[str] = None
    vcrationame: Optional[str] = None
    isratio_subtotal: Optional[bool] = None
    vcdescription: Optional[str] = None
    intthreshold: Optional[float] = None
    vcthresholdtype: Optional[str] = None
    vcthreshold_abs_range: Optional[str] = None
    intthresholdmin: Optional[float] = None
    intthresholdmax: Optional[float] = None
    intprecision: Optional[float] = None
    isactive: Optional[bool] = None

@strawberry.input
class UpdateValidationMasterInput:
    """Input for updating validation entry"""
    intvalidationmasterid: int
    intsubproductid: Optional[int] = None
    vcsourcetype: Optional[str] = None
    vctype: Optional[str] = None
    vcsubtype: Optional[str] = None
    issubtype_subtotal: Optional[bool] = None
    vcvalidationname: Optional[str] = None
    isvalidation_subtotal: Optional[bool] = None
    vcdescription: Optional[str] = None
    intthreshold: Optional[float] = None
    vcthresholdtype: Optional[str] = None
    vcthreshold_abs_range: Optional[str] = None
    intthresholdmin: Optional[float] = None
    intthresholdmax: Optional[float] = None
    intprecision: Optional[float] = None
    isactive: Optional[bool] = None

@strawberry.input
class UpdateRatioMasterInput:
    """Input for updating ratio entry"""
    intratiomasterid: int
    intsubproductid: Optional[int] = None
    vcsourcetype: Optional[str] = None
    vctype: Optional[str] = None
    vcrationame: Optional[str] = None
    isratio_subtotal: Optional[bool] = None
    vcdescription: Optional[str] = None
    intthreshold: Optional[float] = None
    vcthresholdtype: Optional[str] = None
    vcthreshold_abs_range: Optional[str] = None
    intthresholdmin: Optional[float] = None
    intthresholdmax: Optional[float] = None
    intprecision: Optional[float] = None
    isactive: Optional[bool] = None

@strawberry.input
class ValidationDetailsInput:
    """Input for creating validation details entry"""
    intdatamodelid: str  # Changed to str to support ranged validations
    intgroup_attributeid: Optional[int] = None
    intassettypeid: Optional[int] = None
    intcalc_attributeid: Optional[int] = None
    vcaggregationtype: Optional[str] = None  # sum/avg/max/min/etc
    vcfilter: Optional[str] = None
    vcfiltertype: Optional[str] = None
    vcformula: Optional[str] = None

@strawberry.input
class UpdateValidationDetailsInput:
    """Input for updating validation details entry"""
    intvalidationdetailid: int
    intdatamodelid: Optional[str] = None  # Changed to str to support ranged validations
    intgroup_attributeid: Optional[int] = None
    intassettypeid: Optional[int] = None
    intcalc_attributeid: Optional[int] = None
    vcaggregationtype: Optional[str] = None
    vcfilter: Optional[str] = None
    vcfiltertype: Optional[str] = None
    vcformula: Optional[str] = None

@strawberry.input
class UpdateValidationCompleteInput:
    """Input for updating validation master and details together"""
    # Master fields
    intvalidationmasterid: int
    intsubproductid: Optional[int] = None
    vcsourcetype: Optional[str] = None
    vctype: Optional[str] = None
    vcsubtype: Optional[str] = None
    issubtype_subtotal: Optional[bool] = None
    vcvalidationname: Optional[str] = None
    isvalidation_subtotal: Optional[bool] = None
    vcdescription: Optional[str] = None
    intthreshold: Optional[float] = None
    vcthresholdtype: Optional[str] = None
    vcthreshold_abs_range: Optional[str] = None
    intthresholdmin: Optional[float] = None
    intthresholdmax: Optional[float] = None
    intprecision: Optional[float] = None
    isactive: Optional[bool] = None
    # Details - arrays for updates, new, and deletions
    update_details: Optional[List[UpdateValidationDetailsInput]] = None
    new_details: Optional[List[ValidationDetailsInput]] = None
    delete_detail_ids: Optional[List[int]] = None

@strawberry.input
class RatioDetailsInput:
    """Input for creating ratio details entry"""
    intdatamodelid: str  # Changed to str to support ranged ratios
    vcfilter: Optional[str] = None
    vcfiltertype: Optional[str] = None
    vcnumerator: Optional[str] = None
    vcdenominator: Optional[str] = None
    vcformula: Optional[str] = None

@strawberry.input
class UpdateRatioDetailsInput:
    """Input for updating ratio details entry"""
    intratiodetailid: int
    intdatamodelid: Optional[str] = None  # Changed to str to support ranged ratios
    vcfilter: Optional[str] = None
    vcfiltertype: Optional[str] = None
    vcnumerator: Optional[str] = None
    vcdenominator: Optional[str] = None
    vcformula: Optional[str] = None

@strawberry.input
class UpdateRatioCompleteInput:
    """Input for updating ratio master and details together"""
    # Master fields
    intratiomasterid: int
    intsubproductid: Optional[int] = None
    vcsourcetype: Optional[str] = None
    vctype: Optional[str] = None
    vcrationame: Optional[str] = None
    isratio_subtotal: Optional[bool] = None
    vcdescription: Optional[str] = None
    intthreshold: Optional[float] = None
    vcthresholdtype: Optional[str] = None
    vcthreshold_abs_range: Optional[str] = None
    intthresholdmin: Optional[float] = None
    intthresholdmax: Optional[float] = None
    intprecision: Optional[float] = None
    isactive: Optional[bool] = None
    # Details - arrays for updates, new, and deletions
    update_details: Optional[List[UpdateRatioDetailsInput]] = None
    new_details: Optional[List[RatioDetailsInput]] = None
    delete_detail_ids: Optional[List[int]] = None

# ==================== Helper Functions ====================

def _subproduct_master_to_graphql(subproduct: SubproductMaster) -> SubproductMasterType:
    """Convert SubproductMaster model to GraphQL type"""
    return SubproductMasterType(
        intsubproductid=subproduct.intsubproductid,
        vcsubproductname=subproduct.vcsubproductname,
        vcdescription=subproduct.vcdescription,
        isactive=bool(subproduct.isactive) if subproduct.isactive is not None else False,
        intcreatedby=subproduct.intcreatedby,
        dtcreatedat=subproduct.dtcreatedat.isoformat() if subproduct.dtcreatedat else None,
        intupdatedby=subproduct.intupdatedby,
        dtupdatedat=subproduct.dtupdatedat.isoformat() if subproduct.dtupdatedat else None
    )

def _subproduct_details_to_graphql(detail: SubproductDetails) -> SubproductDetailsType:
    """Convert SubproductDetails model to GraphQL type"""
    return SubproductDetailsType(
        intsubproductdetailid=detail.intsubproductdetailid,
        intsubproductid=detail.intsubproductid,
        vcvalidustype=detail.vcvalidustype,
        vctype=detail.vctype,
        vcsubtype=detail.vcsubtype,
        vcdescription=detail.vcdescription,
        isactive=bool(detail.isactive) if detail.isactive is not None else False,
        intcreatedby=detail.intcreatedby,
        dtcreatedat=detail.dtcreatedat.isoformat() if detail.dtcreatedat else None,
        intupdatedby=detail.intupdatedby,
        dtupdatedat=detail.dtupdatedat.isoformat() if detail.dtupdatedat else None
    )

def _validation_master_to_graphql(validation: ValidationMaster) -> ValidationMasterType:
    """Convert ValidationMaster model to GraphQL type"""
    return ValidationMasterType(
        intvalidationmasterid=validation.intvalidationmasterid,
        intsubproductid=validation.intsubproductid,
        vcsourcetype=validation.vcsourcetype,
        vctype=validation.vctype,
        vcsubtype=validation.vcsubtype,
        issubtype_subtotal=bool(validation.issubtype_subtotal) if validation.issubtype_subtotal is not None else None,
        vcvalidationname=validation.vcvalidationname,
        isvalidation_subtotal=bool(validation.isvalidation_subtotal) if validation.isvalidation_subtotal is not None else None,
        vcdescription=validation.vcdescription,
        intthreshold=float(validation.intthreshold) if validation.intthreshold else None,
        vcthresholdtype=validation.vcthresholdtype,
        vcthreshold_abs_range=validation.vcthreshold_abs_range,
        intthresholdmin=float(validation.intthresholdmin) if validation.intthresholdmin else None,
        intthresholdmax=float(validation.intthresholdmax) if validation.intthresholdmax else None,
        intprecision=float(validation.intprecision) if validation.intprecision else None,
        isactive=bool(validation.isactive) if validation.isactive is not None else False,
        intcreatedby=validation.intcreatedby,
        dtcreatedat=validation.dtcreatedat.isoformat() if validation.dtcreatedat else None,
        intupdatedby=validation.intupdatedby,
        dtupdatedat=validation.dtupdatedat.isoformat() if validation.dtupdatedat else None
    )

def _ratio_master_to_graphql(ratio: RatioMaster) -> RatioMasterType:
    """Convert RatioMaster model to GraphQL type"""
    return RatioMasterType(
        intratiomasterid=ratio.intratiomasterid,
        intsubproductid=ratio.intsubproductid,
        vcsourcetype=ratio.vcsourcetype,
        vctype=ratio.vctype,
        vcrationame=ratio.vcrationame,
        isratio_subtotal=bool(ratio.isratio_subtotal) if ratio.isratio_subtotal is not None else None,
        vcdescription=ratio.vcdescription,
        intthreshold=float(ratio.intthreshold) if ratio.intthreshold else None,
        vcthresholdtype=ratio.vcthresholdtype,
        vcthreshold_abs_range=ratio.vcthreshold_abs_range,
        intthresholdmin=float(ratio.intthresholdmin) if ratio.intthresholdmin else None,
        intthresholdmax=float(ratio.intthresholdmax) if ratio.intthresholdmax else None,
        intprecision=float(ratio.intprecision) if ratio.intprecision else None,
        isactive=bool(ratio.isactive) if ratio.isactive is not None else False,
        intcreatedby=ratio.intcreatedby,
        dtcreatedat=ratio.dtcreatedat.isoformat() if ratio.dtcreatedat else None,
        intupdatedby=ratio.intupdatedby,
        dtupdatedat=ratio.dtupdatedat.isoformat() if ratio.dtupdatedat else None
    )

def _validation_details_to_graphql(detail: ValidationDetails) -> ValidationDetailsType:
    """Convert ValidationDetails model to GraphQL type"""
    return ValidationDetailsType(
        intvalidationdetailid=detail.intvalidationdetailid,
        intvalidationmasterid=detail.intvalidationmasterid,
        intdatamodelid=str(detail.intdatamodelid),
        intgroup_attributeid=detail.intgroup_attributeid,
        intassettypeid=detail.intassettypeid,
        intcalc_attributeid=detail.intcalc_attributeid,
        vcaggregationtype=detail.vcaggregationtype,
        vcfilter=detail.vcfilter,
        vcfiltertype=detail.vcfiltertype,
        vcformula=detail.vcformula,
        intcreatedby=detail.intcreatedby,
        dtcreatedat=detail.dtcreatedat.isoformat() if detail.dtcreatedat else None,
        intupdatedby=detail.intupdatedby,
        dtupdatedat=detail.dtupdatedat.isoformat() if detail.dtupdatedat else None
    )

def _ratio_details_to_graphql(detail: RatioDetails) -> RatioDetailsType:
    """Convert RatioDetails model to GraphQL type"""
    return RatioDetailsType(
        intratiodetailid=detail.intratiodetailid,
        intratiomasterid=detail.intratiomasterid,
        intdatamodelid=str(detail.intdatamodelid),
        vcfilter=detail.vcfilter,
        vcfiltertype=detail.vcfiltertype,
        vcnumerator=detail.vcnumerator,
        vcdenominator=detail.vcdenominator,
        vcformula=detail.vcformula,
        intcreatedby=detail.intcreatedby,
        dtcreatedat=detail.dtcreatedat.isoformat() if detail.dtcreatedat else None,
        intupdatedby=detail.intupdatedby,
        dtupdatedat=detail.dtupdatedat.isoformat() if detail.dtupdatedat else None
    )

def _validation_with_details_to_graphql(validation: ValidationMaster) -> ValidationWithDetailsType:
    """Convert ValidationMaster with details to GraphQL type"""
    return ValidationWithDetailsType(
        intvalidationmasterid=validation.intvalidationmasterid,
        intsubproductid=validation.intsubproductid,
        vcsourcetype=validation.vcsourcetype,
        vctype=validation.vctype,
        vcsubtype=validation.vcsubtype,
        issubtype_subtotal=bool(validation.issubtype_subtotal) if validation.issubtype_subtotal is not None else None,
        vcvalidationname=validation.vcvalidationname,
        isvalidation_subtotal=bool(validation.isvalidation_subtotal) if validation.isvalidation_subtotal is not None else None,
        vcdescription=validation.vcdescription,
        intthreshold=float(validation.intthreshold) if validation.intthreshold else None,
        vcthresholdtype=validation.vcthresholdtype,
        vcthreshold_abs_range=validation.vcthreshold_abs_range,
        intthresholdmin=float(validation.intthresholdmin) if validation.intthresholdmin else None,
        intthresholdmax=float(validation.intthresholdmax) if validation.intthresholdmax else None,
        intprecision=float(validation.intprecision) if validation.intprecision else None,
        isactive=bool(validation.isactive) if validation.isactive is not None else False,
        intcreatedby=validation.intcreatedby,
        dtcreatedat=validation.dtcreatedat.isoformat() if validation.dtcreatedat else None,
        intupdatedby=validation.intupdatedby,
        dtupdatedat=validation.dtupdatedat.isoformat() if validation.dtupdatedat else None,
        details=[_validation_details_to_graphql(d) for d in validation.details]
    )

def _ratio_with_details_to_graphql(ratio: RatioMaster) -> RatioWithDetailsType:
    """Convert RatioMaster with details to GraphQL type"""
    return RatioWithDetailsType(
        intratiomasterid=ratio.intratiomasterid,
        intsubproductid=ratio.intsubproductid,
        vcsourcetype=ratio.vcsourcetype,
        vctype=ratio.vctype,
        vcrationame=ratio.vcrationame,
        isratio_subtotal=bool(ratio.isratio_subtotal) if ratio.isratio_subtotal is not None else None,
        vcdescription=ratio.vcdescription,
        intthreshold=float(ratio.intthreshold) if ratio.intthreshold else None,
        vcthresholdtype=ratio.vcthresholdtype,
        vcthreshold_abs_range=ratio.vcthreshold_abs_range,
        intthresholdmin=float(ratio.intthresholdmin) if ratio.intthresholdmin else None,
        intthresholdmax=float(ratio.intthresholdmax) if ratio.intthresholdmax else None,
        intprecision=float(ratio.intprecision) if ratio.intprecision else None,
        isactive=bool(ratio.isactive) if ratio.isactive is not None else False,
        intcreatedby=ratio.intcreatedby,
        dtcreatedat=ratio.dtcreatedat.isoformat() if ratio.dtcreatedat else None,
        intupdatedby=ratio.intupdatedby,
        dtupdatedat=ratio.dtupdatedat.isoformat() if ratio.dtupdatedat else None,
        details=[_ratio_details_to_graphql(d) for d in ratio.details]
    )

def _validation_configuration_to_graphql(config: ValidationConfiguration, session=None) -> ValidationConfigurationType:
    """Convert ValidationConfiguration to GraphQL type"""
    # Get related data if session is provided
    subproduct_info = None
    client_info = None
    fund_info = None
    validation_name = None
    
    if session:
        # Get validation master to extract validation name and subproduct
        if config.validation:
            validation_master = config.validation
            validation_name = validation_master.vcvalidationname
            if validation_master.subproduct:
                subproduct_info = SubproductInfoType(
                    intsubproductid=validation_master.subproduct.intsubproductid,
                    vcsubproductname=validation_master.subproduct.vcsubproductname
                )
        
        # Get client info if intclientid is set
        if config.intclientid:
            client = session.query(Client).filter(Client.id == config.intclientid).first()
            if client:
                client_info = ClientInfoType(
                    id=client.id,
                    name=client.name
                )
        
        # Get fund info if intfundid is set
        if config.intfundid:
            fund = session.query(Fund).filter(Fund.id == config.intfundid).first()
            if fund:
                fund_info = FundInfoType(
                    id=fund.id,
                    name=fund.name
                )
    
    return ValidationConfigurationType(
        intvalidationconfigurationid=config.intvalidationconfigurationid,
        intclientid=config.intclientid,
        intfundid=config.intfundid,
        intvalidationmasterid=config.intvalidationmasterid,
        isactive=config.isactive,
        vccondition=config.vccondition,
        intthreshold=float(config.intthreshold) if config.intthreshold else None,
        vcthresholdtype=config.vcthresholdtype,
        vcthreshold_abs_range=config.vcthreshold_abs_range,
        intthresholdmin=float(config.intthresholdmin) if config.intthresholdmin else None,
        intthresholdmax=float(config.intthresholdmax) if config.intthresholdmax else None,
        intprecision=float(config.intprecision) if config.intprecision else None,
        intcreatedby=config.intcreatedby,
        dtcreatedat=config.dtcreatedat.isoformat() if config.dtcreatedat else None,
        intupdatedby=config.intupdatedby,
        dtupdatedat=config.dtupdatedat.isoformat() if config.dtupdatedat else None,
        subproduct=subproduct_info,
        client=client_info,
        fund=fund_info,
        vcvalidationname=validation_name
    )

def _ratio_configuration_to_graphql(config: RatioConfiguration, session=None) -> RatioConfigurationType:
    """Convert RatioConfiguration to GraphQL type"""
    # Get related data if session is provided
    subproduct_info = None
    client_info = None
    fund_info = None
    ratio_name = None
    
    if session:
        # Get ratio master to extract ratio name and subproduct
        if config.ratio:
            ratio_master = config.ratio
            ratio_name = ratio_master.vcrationame
            if ratio_master.subproduct:
                subproduct_info = SubproductInfoType(
                    intsubproductid=ratio_master.subproduct.intsubproductid,
                    vcsubproductname=ratio_master.subproduct.vcsubproductname
                )
        
        # Get client info if intclientid is set
        if config.intclientid:
            client = session.query(Client).filter(Client.id == config.intclientid).first()
            if client:
                client_info = ClientInfoType(
                    id=client.id,
                    name=client.name
                )
        
        # Get fund info if intfundid is set
        if config.intfundid:
            fund = session.query(Fund).filter(Fund.id == config.intfundid).first()
            if fund:
                fund_info = FundInfoType(
                    id=fund.id,
                    name=fund.name
                )
    
    return RatioConfigurationType(
        intratioconfigurationid=config.intratioconfigurationid,
        intclientid=config.intclientid,
        intfundid=config.intfundid,
        intratiomasterid=config.intratiomasterid,
        isactive=config.isactive,
        vccondition=config.vccondition,
        intthreshold=float(config.intthreshold) if config.intthreshold else None,
        vcthresholdtype=config.vcthresholdtype,
        vcthreshold_abs_range=config.vcthreshold_abs_range,
        intthresholdmin=float(config.intthresholdmin) if config.intthresholdmin else None,
        intthresholdmax=float(config.intthresholdmax) if config.intthresholdmax else None,
        intprecision=float(config.intprecision) if config.intprecision else None,
        intcreatedby=config.intcreatedby,
        dtcreatedat=config.dtcreatedat.isoformat() if config.dtcreatedat else None,
        intupdatedby=config.intupdatedby,
        dtupdatedat=config.dtupdatedat.isoformat() if config.dtupdatedat else None,
        subproduct=subproduct_info,
        client=client_info,
        fund=fund_info,
        vcrationame=ratio_name
    )

# ==================== GraphQL Queries ====================

@strawberry.type
class ValidationQuery:
    """Query class for validation and ratio operations"""
    
    @strawberry.field
    def get_subproducts(self, info: Info) -> List[SubproductMasterType]:
        """
        Get all subproducts for dropdowns
        """
        # Require authentication
        require_authentication(info)
        
        try:
            db_manager = get_database_manager()
            session = db_manager.get_session()
            
            try:
                query = session.query(SubproductMaster)
                subproducts = query.all()
                return [_subproduct_master_to_graphql(sp) for sp in subproducts]
            finally:
                session.close()
                
        except Exception as e:
            logger.error(f"Error fetching subproducts: {str(e)}")
            raise GraphQLError(get_error_message('DATABASE_ERROR'))
    
    @strawberry.field
    def get_subproduct_details_for_validation(
        self, 
        info: Info, 
        intsubproductid: Optional[int] = None
    ) -> List[SubproductDetailsType]:
        """
        Get subproduct details for validation dropdowns
        Args:
            intsubproductid: Optional filter by subproduct ID
        """
        # Require authentication
        require_authentication(info)
        
        try:
            db_manager = get_database_manager()
            session = db_manager.get_session()
            
            try:
                query = session.query(SubproductDetails).filter(
                    SubproductDetails.vcvalidustype == 'Validation'
                )
                
                if intsubproductid is not None:
                    query = query.filter(SubproductDetails.intsubproductid == intsubproductid)
                
                details = query.all()
                return [_subproduct_details_to_graphql(d) for d in details]
            finally:
                session.close()
                
        except Exception as e:
            logger.error(f"Error fetching validation subproduct details: {str(e)}")
            raise GraphQLError(get_error_message('DATABASE_ERROR'))
    
    @strawberry.field
    def get_subproduct_details_for_ratio(
        self, 
        info: Info, 
        intsubproductid: Optional[int] = None
    ) -> List[SubproductDetailsType]:
        """
        Get subproduct details for ratio dropdowns
        Args:
            intsubproductid: Optional filter by subproduct ID
        """
        # Require authentication
        require_authentication(info)
        
        try:
            db_manager = get_database_manager()
            session = db_manager.get_session()
            
            try:
                query = session.query(SubproductDetails).filter(
                    SubproductDetails.vcvalidustype == 'Ratio'
                )
                
                if intsubproductid is not None:
                    query = query.filter(SubproductDetails.intsubproductid == intsubproductid)
                
                details = query.all()
                return [_subproduct_details_to_graphql(d) for d in details]
            finally:
                session.close()
                
        except Exception as e:
            logger.error(f"Error fetching ratio subproduct details: {str(e)}")
            raise GraphQLError(get_error_message('DATABASE_ERROR'))
    
    @strawberry.field
    def get_all_validations(
        self, 
        info: Info, 
        intsubproductid: Optional[int] = None,
        pageNumber: Optional[int] = 1,
        pageSize: Optional[int] = 50
    ) -> PaginatedValidationResponseType:
        """
        Get all validation entries with pagination
        Args:
            intsubproductid: Optional filter by subproduct ID
            pageNumber: Page number (default: 1)
            pageSize: Number of items per page (default: 10)
        """
        # Require authentication
        require_authentication(info)
        
        # Validate and set defaults for pagination parameters
        if pageNumber is None or pageNumber < 1:
            pageNumber = 1
        if pageSize is None or pageSize < 1:
            pageSize = 20
        
        try:
            db_manager = get_database_manager()
            session = db_manager.get_session()
            
            try:
                # Build base query
                query = session.query(ValidationMaster)
                
                if intsubproductid is not None:
                    query = query.filter(ValidationMaster.intsubproductid == intsubproductid)
                
                # Get total count
                totalCount = query.count()
                
                # Apply pagination (pageNumber and pageSize are already validated above)
                currentPage = pageNumber
                pageSizeValue = pageSize
                offset = (currentPage - 1) * pageSizeValue
                
                paginatedQuery = query.offset(offset).limit(pageSizeValue)
                validations = paginatedQuery.all()
                
                # Calculate total pages
                totalPages = (totalCount + pageSizeValue - 1) // pageSizeValue if totalCount > 0 else 0
                
                # Build response
                return PaginatedValidationResponseType(
                    validations=[_validation_master_to_graphql(v) for v in validations],
                    pagination=PaginationInfoType(
                        pageNumber=currentPage,
                        pageSize=pageSizeValue,
                        currentPage=currentPage,
                        totalPages=totalPages,
                        totalCount=totalCount
                    )
                )
            finally:
                session.close()
                
        except Exception as e:
            logger.error(f"Error fetching validations: {str(e)}")
            raise GraphQLError(get_error_message('DATABASE_ERROR'))
        
    @strawberry.field
    def get_all_ratios(
        self,
        info: Info,
        intsubproductid: Optional[int] = None,
        pageNumber: Optional[int] = 1,
        pageSize: Optional[int] = 50
    ) -> PaginatedRatioResponseType:
        """
        Get all ratio entries with pagination
        Args:
            intsubproductid: Optional filter by subproduct ID
            pageNumber: Page number (default: 1)
            pageSize: Number of items per page (default: 20)
        """
        # Require authentication
        require_authentication(info)

        # Validate and set defaults for pagination parameters
        if pageNumber is None or pageNumber < 1:
            pageNumber = 1
        if pageSize is None or pageSize < 1:
            pageSize = 20

        try:
            db_manager = get_database_manager()
            session = db_manager.get_session()

            try:
                # Build base query
                query = session.query(RatioMaster)

                if intsubproductid is not None:
                    query = query.filter(RatioMaster.intsubproductid == intsubproductid)

                # Get total count
                totalCount = query.count()

                # Apply pagination
                currentPage = pageNumber
                pageSizeValue = pageSize
                offset = (currentPage - 1) * pageSizeValue

                paginatedQuery = query.offset(offset).limit(pageSizeValue)
                ratios = paginatedQuery.all()

                # Calculate total pages
                totalPages = (totalCount + pageSizeValue - 1) // pageSizeValue if totalCount > 0 else 0

                # Build response
                return PaginatedRatioResponseType(
                    ratios=[_ratio_master_to_graphql(r) for r in ratios],
                    pagination=PaginationInfoType(
                        pageNumber=currentPage,
                        pageSize=pageSizeValue,
                        currentPage=currentPage,
                        totalPages=totalPages,
                        totalCount=totalCount
                    )
                )
            finally:
                session.close()

        except Exception as e:
            logger.error(f"Error fetching ratios: {str(e)}")
            raise GraphQLError(get_error_message('DATABASE_ERROR'))

    
    @strawberry.field
    def get_data_model_columns(
        self, 
        info: Info, 
        intdatamodelid: int
    ) -> List[DataModelColumnType]:
        """
        Get data model columns for Attribute and Asset Type dropdowns
        Args:
            intdatamodelid: Data model ID to get columns for
        """
        # Require authentication
        require_authentication(info)
        
        try:
            db_manager = get_database_manager()
            session = db_manager.get_session()
            
            try:
                # Get column details for the specified data model
                columns_query = session.query(DataModelDetails).filter(
                    DataModelDetails.intdatamodelid == intdatamodelid
                ).order_by(DataModelDetails.intdisplayorder)
                
                columns = columns_query.all()
                
                # Import the helper function from graphql_table_schema
                from schema.graphql_table_schema import _data_model_column_to_graphql
                
                return [_data_model_column_to_graphql(col) for col in columns]
            finally:
                session.close()
                
        except Exception as e:
            logger.error(f"Error fetching data model columns: {str(e)}")
            raise GraphQLError(get_error_message('DATABASE_ERROR'))
    
    @strawberry.field
    def get_data_models(
        self, 
        info: Info
    ) -> List[DataModelSimpleType]:
        """
        Get all data models for dropdown selection
        """
        # Require authentication
        require_authentication(info)
        
        try:
            db_manager = get_database_manager()
            session = db_manager.get_session()
            
            try:
                data_models = session.query(DataModelMaster).all()
                return [
                    DataModelSimpleType(
                        intdatamodelid=dm.intdatamodelid,
                        vcmodelname=dm.vcmodelname,
                        vcdescription=dm.vcdescription
                    ) for dm in data_models
                ]
            finally:
                session.close()
                
        except Exception as e:
            logger.error(f"Error fetching data models: {str(e)}")
            raise GraphQLError(get_error_message('DATABASE_ERROR'))
    
    @strawberry.field
    def get_validation_by_id(
        self, 
        info: Info, 
        intvalidationmasterid: int
    ) -> Optional[ValidationWithDetailsType]:
        """
        Get a single validation by ID with all details
        Args:
            intvalidationmasterid: Validation master ID
        """
        require_authentication(info)
        
        try:
            db_manager = get_database_manager()
            session = db_manager.get_session()
            
            try:
                validation = session.query(ValidationMaster).filter(
                    ValidationMaster.intvalidationmasterid == intvalidationmasterid
                ).first()
                
                if not validation:
                    return None
                
                return _validation_with_details_to_graphql(validation)
            finally:
                session.close()
                
        except Exception as e:
            logger.error(f"Error fetching validation: {str(e)}")
            raise GraphQLError(get_error_message('DATABASE_ERROR'))
    
    @strawberry.field
    def get_ratio_by_id(
        self, 
        info: Info, 
        intratiomasterid: int
    ) -> Optional[RatioWithDetailsType]:
        """
        Get a single ratio by ID with all details
        Args:
            intratiomasterid: Ratio master ID
        """
        require_authentication(info)
        
        try:
            db_manager = get_database_manager()
            session = db_manager.get_session()
            
            try:
                ratio = session.query(RatioMaster).filter(
                    RatioMaster.intratiomasterid == intratiomasterid
                ).first()
                
                if not ratio:
                    return None
                
                return _ratio_with_details_to_graphql(ratio)
            finally:
                session.close()
                
        except Exception as e:
            logger.error(f"Error fetching ratio: {str(e)}")
            raise GraphQLError(get_error_message('DATABASE_ERROR'))
    
    @strawberry.field
    def get_validation_configurations(
        self,
        info: Info,
        intvalidationmasterid: Optional[int] = None,
        intclientid: Optional[int] = None,
        intfundid: Optional[int] = None,
        pageNumber: Optional[int] = 1,
        pageSize: Optional[int] = 50
    ) -> PaginatedValidationConfigurationResponseType:
        """
        Get validation configurations with pagination and optional filters
        Args:
            intvalidationmasterid: Optional filter by validation master ID
            intclientid: Optional filter by client ID
            intfundid: Optional filter by fund ID
            pageNumber: Page number (default: 1)
            pageSize: Page size (default: 10)
        """
        require_authentication(info)
        
        try:
            db_manager = get_database_manager()
            session = db_manager.get_session()
            
            try:
                query = session.query(ValidationConfiguration).options(
                    joinedload(ValidationConfiguration.validation).joinedload(ValidationMaster.subproduct)
                )
                
                # Apply filters
                if intvalidationmasterid is not None:
                    query = query.filter(ValidationConfiguration.intvalidationmasterid == intvalidationmasterid)
                if intclientid is not None:
                    query = query.filter(ValidationConfiguration.intclientid == intclientid)
                if intfundid is not None:
                    query = query.filter(ValidationConfiguration.intfundid == intfundid)
                
                # Get total count
                totalCount = query.count()
                
                # Apply pagination
                currentPage = max(1, pageNumber or 1)
                pageSizeValue = max(1, min(100, pageSize or 10))
                offset = (currentPage - 1) * pageSizeValue
                
                configurations = query.offset(offset).limit(pageSizeValue).all()
                
                totalPages = (totalCount + pageSizeValue - 1) // pageSizeValue if totalCount > 0 else 0
                
                return PaginatedValidationConfigurationResponseType(
                    validationconfigurations=[_validation_configuration_to_graphql(c, session) for c in configurations],
                    pagination=PaginationInfoType(
                        pageNumber=currentPage,
                        pageSize=pageSizeValue,
                        currentPage=currentPage,
                        totalPages=totalPages,
                        totalCount=totalCount
                    )
                )
            finally:
                session.close()
                
        except Exception as e:
            logger.error(f"Error fetching validation configurations: {str(e)}")
            raise GraphQLError(get_error_message('DATABASE_ERROR'))
    
    @strawberry.field
    def get_active_validations_with_configurations(
        self,
        info: Info,
        intsubproductid: int,
        vcsourcetype: Optional[str] = None,
        intclientid: Optional[int] = None,
        intfundid: Optional[int] = None
    ) -> List[ValidationWithConfigurationsType]:
        """
        Get all active validations filtered by subproduct and optional source type,
        and include their configurations filtered by optional client and fund.
        """
        require_authentication(info)
        
        try:
            db_manager = get_database_manager()
            session = db_manager.get_session()
            
            try:
                # Query active validations by subproduct and optional source type
                v_query = session.query(ValidationMaster).filter(
                    ValidationMaster.isactive == True,
                    ValidationMaster.intsubproductid == intsubproductid
                )
                if vcsourcetype is not None:
                    v_query = v_query.filter(ValidationMaster.vcsourcetype == vcsourcetype)
                validations = v_query.all()

                if not validations:
                    return []

                validation_ids = [v.intvalidationmasterid for v in validations]

                # Query configurations in one go, filtered by provided client/fund
                # Map by validation master id for quick lookup (one config per validation)
                configs_by_validation = {}
                if intclientid is not None or intfundid is not None:
                    c_query = session.query(ValidationConfiguration).filter(
                        ValidationConfiguration.intvalidationmasterid.in_(validation_ids)
                    )
                    if intclientid is not None:
                        c_query = c_query.filter(ValidationConfiguration.intclientid == intclientid)
                    if intfundid is not None:
                        c_query = c_query.filter(ValidationConfiguration.intfundid == intfundid)
                    
                    configurations = c_query.all()
                    # Map by validation master id (assuming one config per validation for client/fund)
                    for cfg in configurations:
                        configs_by_validation[cfg.intvalidationmasterid] = cfg

                # Build response objects for all validations
                results: List[ValidationWithConfigurationsType] = []
                for v in validations:
                    # Get config if exists
                    config = configs_by_validation.get(v.intvalidationmasterid)
                    
                    # Build flat response object
                    validation_data = _validation_master_to_graphql(v)
                    results.append(
                        ValidationWithConfigurationsType(
                            # Validation master fields (using snake_case for Python object access)
                            intvalidationmasterid=validation_data.intvalidationmasterid,
                            intsubproductid=validation_data.intsubproductid,
                            vcsourcetype=validation_data.vcsourcetype,
                            vctype=validation_data.vctype,
                            vcsubtype=validation_data.vcsubtype,
                            issubtypeSubtotal=validation_data.issubtype_subtotal,
                            vcvalidationname=validation_data.vcvalidationname,
                            isvalidationSubtotal=validation_data.isvalidation_subtotal,
                            vcdescription=validation_data.vcdescription,
                            intthreshold=validation_data.intthreshold,
                            vcthresholdtype=validation_data.vcthresholdtype,
                            vcthreshold_abs_range=validation_data.vcthreshold_abs_range,
                            intthresholdmin=validation_data.intthresholdmin,
                            intthresholdmax=validation_data.intthresholdmax,
                            intprecision=validation_data.intprecision,
                            isactive=validation_data.isactive,
                            intcreatedby=validation_data.intcreatedby,
                            dtcreatedat=validation_data.dtcreatedat,
                            intupdatedby=validation_data.intupdatedby,
                            dtupdatedat=validation_data.dtupdatedat,
                            # Configuration fields (null if no config)
                            configIsactive=config.isactive if config else None,
                            vccondition=config.vccondition if config else None,
                            configThreshold=float(config.intthreshold) if config and config.intthreshold else None,
                            configThresholdtype=config.vcthresholdtype if config else None,
                            configThreshold_abs_range=config.vcthreshold_abs_range if config else None,
                            configThresholdmin=float(config.intthresholdmin) if config and config.intthresholdmin else None,
                            configThresholdmax=float(config.intthresholdmax) if config and config.intthresholdmax else None
                        )
                    )

                return results

            finally:
                session.close()
        except Exception as e:
            logger.error(f"Error fetching active validations with configurations: {str(e)}")
            raise GraphQLError(get_error_message('DATABASE_ERROR'))

    @strawberry.field
    def get_active_ratios_with_configurations(
        self,
        info: Info,
        intsubproductid: int,
        vcsourcetype: Optional[str] = None,
        intclientid: Optional[int] = None,
        intfundid: Optional[int] = None
    ) -> List[RatioWithConfigurationsType]:
        """
        Get all active ratios filtered by subproduct and optional source type,
        and include their configurations filtered by optional client and fund.
        """
        require_authentication(info)
        
        try:
            db_manager = get_database_manager()
            session = db_manager.get_session()
            
            try:
                # Query active ratios by subproduct and optional source type
                r_query = session.query(RatioMaster).filter(
                    RatioMaster.isactive == True,
                    RatioMaster.intsubproductid == intsubproductid
                )
                if vcsourcetype is not None:
                    r_query = r_query.filter(RatioMaster.vcsourcetype == vcsourcetype)
                ratios = r_query.all()

                if not ratios:
                    return []

                ratio_ids = [r.intratiomasterid for r in ratios]

                # Query configurations in one go, filtered by provided client/fund
                # Map by ratio master id for quick lookup (one config per ratio)
                configs_by_ratio = {}
                if intclientid is not None or intfundid is not None:
                    c_query = session.query(RatioConfiguration).filter(
                        RatioConfiguration.intratiomasterid.in_(ratio_ids)
                    )
                    if intclientid is not None:
                        c_query = c_query.filter(RatioConfiguration.intclientid == intclientid)
                    if intfundid is not None:
                        c_query = c_query.filter(RatioConfiguration.intfundid == intfundid)
                    
                    configurations = c_query.all()
                    # Map by ratio master id (assuming one config per ratio for client/fund)
                    for cfg in configurations:
                        configs_by_ratio[cfg.intratiomasterid] = cfg

                # Build response objects for all ratios
                results: List[RatioWithConfigurationsType] = []
                for r in ratios:
                    # Get config if exists
                    config = configs_by_ratio.get(r.intratiomasterid)
                    
                    # Build flat response object
                    ratio_data = _ratio_master_to_graphql(r)
                    results.append(
                        RatioWithConfigurationsType(
                            # Ratio master fields (using snake_case for Python object access)
                            intratiomasterid=ratio_data.intratiomasterid,
                            intsubproductid=ratio_data.intsubproductid,
                            vcsourcetype=ratio_data.vcsourcetype,
                            vctype=ratio_data.vctype,
                            vcrationame=ratio_data.vcrationame,
                            isratioSubtotal=ratio_data.isratio_subtotal,
                            vcdescription=ratio_data.vcdescription,
                            intthreshold=ratio_data.intthreshold,
                            vcthresholdtype=ratio_data.vcthresholdtype,
                            vcthreshold_abs_range=ratio_data.vcthreshold_abs_range,
                            intthresholdmin=ratio_data.intthresholdmin,
                            intthresholdmax=ratio_data.intthresholdmax,
                            intprecision=ratio_data.intprecision,
                            isactive=ratio_data.isactive,
                            intcreatedby=ratio_data.intcreatedby,
                            dtcreatedat=ratio_data.dtcreatedat,
                            intupdatedby=ratio_data.intupdatedby,
                            dtupdatedat=ratio_data.dtupdatedat,
                            # Configuration fields (null if no config)
                            configIsactive=config.isactive if config else None,
                            vccondition=config.vccondition if config else None,
                            configThreshold=float(config.intthreshold) if config and config.intthreshold else None,
                            configThresholdtype=config.vcthresholdtype if config else None,
                            configThreshold_abs_range=config.vcthreshold_abs_range if config else None,
                            configThresholdmin=float(config.intthresholdmin) if config and config.intthresholdmin else None,
                            configThresholdmax=float(config.intthresholdmax) if config and config.intthresholdmax else None
                        )
                    )

                return results

            finally:
                session.close()
        except Exception as e:
            logger.error(f"Error fetching active ratios with configurations: {str(e)}")
            raise GraphQLError(get_error_message('DATABASE_ERROR'))

    @strawberry.field
    def get_ratio_configurations(
        self,
        info: Info,
        intratiomasterid: Optional[int] = None,
        intclientid: Optional[int] = None,
        intfundid: Optional[int] = None,
        pageNumber: Optional[int] = 1,
        pageSize: Optional[int] = 50
    ) -> PaginatedRatioConfigurationResponseType:
        """
        Get ratio configurations with pagination and optional filters
        Args:
            intratiomasterid: Optional filter by ratio master ID
            intclientid: Optional filter by client ID
            intfundid: Optional filter by fund ID
            pageNumber: Page number (default: 1)
            pageSize: Page size (default: 10)
        """
        require_authentication(info)
        
        try:
            db_manager = get_database_manager()
            session = db_manager.get_session()
            
            try:
                query = session.query(RatioConfiguration).options(
                    joinedload(RatioConfiguration.ratio).joinedload(RatioMaster.subproduct)
                )
                
                # Apply filters
                if intratiomasterid is not None:
                    query = query.filter(RatioConfiguration.intratiomasterid == intratiomasterid)
                if intclientid is not None:
                    query = query.filter(RatioConfiguration.intclientid == intclientid)
                if intfundid is not None:
                    query = query.filter(RatioConfiguration.intfundid == intfundid)
                
                # Get total count
                totalCount = query.count()
                
                # Apply pagination
                currentPage = max(1, pageNumber or 1)
                pageSizeValue = max(1, min(100, pageSize or 10))
                offset = (currentPage - 1) * pageSizeValue
                
                configurations = query.offset(offset).limit(pageSizeValue).all()
                
                totalPages = (totalCount + pageSizeValue - 1) // pageSizeValue if totalCount > 0 else 0
                
                return PaginatedRatioConfigurationResponseType(
                    ratioconfigurations=[_ratio_configuration_to_graphql(c, session) for c in configurations],
                    pagination=PaginationInfoType(
                        pageNumber=currentPage,
                        pageSize=pageSizeValue,
                        currentPage=currentPage,
                        totalPages=totalPages,
                        totalCount=totalCount
                    )
                )
            finally:
                session.close()
                
        except Exception as e:
            logger.error(f"Error fetching ratio configurations: {str(e)}")
            raise GraphQLError(get_error_message('DATABASE_ERROR'))
    
    @strawberry.field
    def get_fund_compare_validation_summary(
        self,
        info: Info,
        client_id: int,
        fund_id: int,
        subproduct_id: int,
        source_a: str,
        date_a: str,
        source_b: Optional[str] = None,
        date_b: Optional[str] = None
    ) -> Optional[ProcessInstanceSummaryType]:
        """
        Get single fund compare validation summary
        Returns validation and ratio counts for the latest process instances matching the criteria
        """
        require_authentication(info)
        
        try:
            validation_service = DatabaseValidationService()
            result = validation_service.get_latest_process_instance_summary(
                client_id=client_id,
                fund_id=fund_id,
                subproduct_id=subproduct_id,
                source_a=source_a,
                source_b=source_b,
                date_a=date_a,
                date_b=date_b
            )
            
            if result is None:
                return None
            
            # Convert subchecks from service layer to GraphQL types
            subchecks_data = result.get('subchecks', [])
            subchecks_list = None
            if subchecks_data:
                subchecks_list = []
                for subcheck in subchecks_data:
                    validations_list = [
                        ValidationInSubcheckType(
                            validationName=val.get('validation_name', ''),
                            description=val.get('description'),
                            status=val.get('status', 'Not Completed'),
                            passFail=val.get('pass_fail', 'Pass'),
                            datetime=val.get('datetime')
                        )
                        for val in subcheck.get('validations', [])
                    ]
                    subchecks_list.append(
                        SubcheckType(
                            subtype=subcheck.get('subtype', ''),
                            status=subcheck.get('status', 'Not Completed'),
                            validations=validations_list
                        )
                    )
            
            return ProcessInstanceSummaryType(
                validation_total=result.get('validation_total', 0),
                validation_failed=result.get('validation_failed', 0),
                validation_passed=result.get('validation_passed', 0),
                validation_exceptions=result.get('validation_exceptions', 0),
                ratio_total=result.get('ratio_total', 0),
                ratio_failed=result.get('ratio_failed', 0),
                ratio_passed=result.get('ratio_passed', 0),
                validation_process_instance_id=result.get('validation_process_instance_id'),
                ratio_process_instance_id=result.get('ratio_process_instance_id'),
                subchecks=subchecks_list
            )
            
        except Exception as e:
            logger.error(f"Error fetching fund compare validation summary: {str(e)}")
            raise GraphQLError(get_error_message('DATABASE_ERROR'))
    
    @strawberry.field
    def get_validation_aggregated_data(
        self,
        info: Info,
        client_id: int,
        process_instance_id: Optional[int] = None,
        fund_id: Optional[int] = None,
        subproduct_id: Optional[int] = None,
        source_a: Optional[str] = None,
        date_a: Optional[str] = None,
        source_b: Optional[str] = None,
        date_b: Optional[str] = None
    ) -> List[ValidationAggregatedDataType]:
        """
        Get validation aggregated data from validation results
        Returns list of validation data with:
        - vcvalidationname (from ValidationMaster)
        - type (from ValidationMaster)
        - subtype (from ValidationMaster)
        - config_threshold (from ValidationConfiguration)
        - status (Failed if any failed entry for that intprocessinstanceid and intvalidationconfigurationid, else Passed)
        - exception (Count of Failed Status for that intprocessinstanceid and intvalidationconfigurationid)
        
        Either process_instance_id must be provided, OR (fund_id, subproduct_id, source_a, date_a) must be provided
        to find the latest process instance.
        """
        require_authentication(info)
        
        try:
            validation_service = DatabaseValidationService()
            result = validation_service.get_validation_aggregated_data(
                client_id=client_id,
                process_instance_id=process_instance_id,
                fund_id=fund_id,
                subproduct_id=subproduct_id,
                source_a=source_a,
                date_a=date_a,
                source_b=source_b,
                date_b=date_b
            )
            
            return [
                ValidationAggregatedDataType(
                    vcvalidationname=item.get('vcvalidationname'),
                    type=item.get('type'),
                    subtype=item.get('subtype'),
                    config_threshold=item.get('config_threshold'),
                    status=item.get('status', 'Passed'),
                    exception=item.get('exception', 0)
                )
                for item in result
            ]
            
        except Exception as e:
            logger.error(f"Error fetching validation aggregated data: {str(e)}")
            raise GraphQLError(get_error_message('DATABASE_ERROR'))
    
    @strawberry.field
    def get_validation_comparison_data(
        self,
        info: Info,
        client_id: int,
        process_instance_id: Optional[int] = None,
        fund_id: Optional[int] = None,
        subproduct_id: Optional[int] = None,
        source_a: Optional[str] = None,
        date_a: Optional[str] = None,
        source_b: Optional[str] = None,
        date_b: Optional[str] = None
    ) -> List[ValidationComparisonDataType]:
        """
        Get validation comparison data with side A and side B joined
        Returns list of validation results with matched sides, including:
        - intprocessinstanceid
        - validations (validation name)
        - intmatchid
        - Dynamic description column (from align keys)
        - Dynamic formula column with _A and _B suffixes
        - status_A and status_B
        
        Either process_instance_id must be provided, OR (fund_id, subproduct_id, date_a) must be provided
        to find the latest process instance.
        """
        require_authentication(info)
        
        try:
            validation_service = DatabaseValidationService()
            result = validation_service.get_validation_comparison_data(
                client_id=client_id,
                process_instance_id=process_instance_id,
                fund_id=fund_id,
                subproduct_id=subproduct_id,
                source_a=source_a,
                date_a=date_a,
                source_b=source_b,
                date_b=date_b
            )
            
            return [
                ValidationComparisonDataType(
                    intprocessinstanceid=item.get('intprocessinstanceid'),
                    validations=item.get('validations', ''),
                    intmatchid=item.get('intmatchid'),
                    data={
                        # Include validation_name in data
                        'validation_name': item.get('validations', ''),
                        # Include all other dynamic fields in the data JSON
                        **{k: v for k, v in item.items() 
                            if k not in ['intprocessinstanceid', 'validations', 'intmatchid']}
                    }
                )
                for item in result
            ]
            
        except Exception as e:
            logger.error(f"Error fetching validation comparison data: {str(e)}")
            raise GraphQLError(get_error_message('DATABASE_ERROR'))
    
    @strawberry.field
    def get_ratio_comparison_data(
        self,
        info: Info,
        client_id: int,
        process_instance_id: Optional[int] = None,
        fund_id: Optional[int] = None,
        subproduct_id: Optional[int] = None,
        source_a: Optional[str] = None,
        date_a: Optional[str] = None,
        source_b: Optional[str] = None,
        date_b: Optional[str] = None
    ) -> List[RatioComparisonDataType]:
        """
        Get ratio comparison data with side A and side B joined
        Returns list of ratio results with matched sides, including:
        - intprocessinstanceid
        - ratios (ratio name)
        - intmatchid
        - Dynamic fields from ratio configuration, master, details, and results
        - tooltipinfo
        
        Either process_instance_id must be provided, OR (fund_id, subproduct_id, date_a) must be provided
        to find the latest process instance.
        """
        require_authentication(info)
        
        try:
            validation_service = DatabaseValidationService()
            result = validation_service.get_ratio_comparison_data(
                client_id=client_id,
                process_instance_id=process_instance_id,
                fund_id=fund_id,
                subproduct_id=subproduct_id,
                source_a=source_a,
                date_a=date_a,
                source_b=source_b,
                date_b=date_b
            )
            
            return [
                RatioComparisonDataType(
                    intprocessinstanceid=item.get('intprocessinstanceid'),
                    ratios=item.get('ratios', ''),
                    intmatchid=item.get('intmatchid'),
                    data={
                        # Include all other dynamic fields in the data JSON
                        k: v for k, v in item.items() 
                        if k not in ['intprocessinstanceid', 'ratios', 'intmatchid']
                    }
                )
                for item in result
            ]
            
        except Exception as e:
            logger.error(f"Error fetching ratio comparison data: {str(e)}")
            raise GraphQLError(get_error_message('DATABASE_ERROR'))
    
    @strawberry.field
    def get_data_load_dates(
        self,
        info: Info,
        client_id: int,
        fund_id: int,
        source: Optional[str] = None,
        source2: Optional[str] = None
    ) -> List[DataLoadDateType]:
        """
        Get list of dates (dtdataasof) from tbl_dataload_instance table
        Filters by client_id, fund_id, and source(s) (max two sources)
        If multiple sources are provided, returns only dates that exist in BOTH sources
        
        Args:
            client_id: Client ID
            fund_id: Fund ID
            source: First source name (vcdatasourcename)
            source2: Optional second source name (vcdatasourcename)
        
        Returns:
            List of dates that match the criteria
        """
        require_authentication(info)
        
        try:
            db_manager = get_database_manager()
            session = db_manager.get_session_with_schema('validus')
            
            try:
                # Validate that at least one source is provided
                if not source:
                    raise GraphQLError("At least one source must be provided")
                
                # If two sources provided, find intersection (dates in both)
                if source2:
                    # Get dates for source 1
                    query1 = session.query(DataLoadInstance.dtdataasof).filter(
                        DataLoadInstance.intclientid == client_id,
                        DataLoadInstance.intfundid == fund_id,
                        DataLoadInstance.vcdatasourcename == source,
                        DataLoadInstance.dtdataasof.isnot(None)
                    ).distinct()
                    
                    dates1 = {row[0] for row in query1.all() if row[0] is not None}
                    
                    # Get dates for source 2
                    query2 = session.query(DataLoadInstance.dtdataasof).filter(
                        DataLoadInstance.intclientid == client_id,
                        DataLoadInstance.intfundid == fund_id,
                        DataLoadInstance.vcdatasourcename == source2,
                        DataLoadInstance.dtdataasof.isnot(None)
                    ).distinct()
                    
                    dates2 = {row[0] for row in query2.all() if row[0] is not None}
                    
                    # Find intersection (dates in both sources)
                    common_dates = sorted(list(dates1.intersection(dates2)))
                    
                    return [
                        DataLoadDateType(date=date.isoformat())
                        for date in common_dates
                    ]
                else:
                    # Single source - return all dates for that source
                    query = session.query(DataLoadInstance.dtdataasof).filter(
                        DataLoadInstance.intclientid == client_id,
                        DataLoadInstance.intfundid == fund_id,
                        DataLoadInstance.vcdatasourcename == source,
                        DataLoadInstance.dtdataasof.isnot(None)
                    ).distinct().order_by(DataLoadInstance.dtdataasof)
                    
                    dates = [row[0] for row in query.all() if row[0] is not None]
                    
                    return [
                        DataLoadDateType(date=date.isoformat())
                        for date in dates
                    ]
                    
            finally:
                session.close()
                
        except GraphQLError:
            raise
        except Exception as e:
            logger.error(f"Error fetching data load dates: {str(e)}")
            raise GraphQLError(get_error_message('DATABASE_ERROR'))


# ==================== GraphQL Mutations ====================

@strawberry.type
class ValidationMutation:
    """Mutation class for validation and ratio operations"""
    
    @strawberry.mutation
    def upsert_validation_configurations_bulk(
        self,
        info: Info,
        input: BulkValidationConfigurationUpsertInput
    ) -> BulkUpsertValidationConfigResultType:
        """
        Create or update validation configurations in bulk.
        - Process only items where isactive is True; skip inactive items
        - If a configuration exists for (intclientid, intfundid, intvalidationmasterid), update it
        - Otherwise create a new configuration
        - Optionally validate vcsourcetype against the validation master when provided
        """
        require_authentication(info)
        
        created_count = 0
        updated_count = 0
        skipped_count = 0
        result_configs: List[ValidationConfigurationType] = []
        
        try:
            db_manager = get_database_manager()
            session = db_manager.get_session()
            
            try:
                current_user = get_current_user(info)
                user_id = current_user.get('id') if current_user else None
                
                # Preload validation masters for ids in payload
                unique_master_ids = list({item.intvalidationmasterid for item in input.items})
                if unique_master_ids:
                    masters = session.query(ValidationMaster).filter(
                        ValidationMaster.intvalidationmasterid.in_(unique_master_ids)
                    ).all()
                else:
                    masters = []
                masters_by_id = {m.intvalidationmasterid: m for m in masters}
                
                for item in input.items:
                    # Validate master existence
                    vm = masters_by_id.get(item.intvalidationmasterid)
                    if not vm:
                        skipped_count += 1
                        continue
                    
                    # Optional vcsourcetype validation against master
                    if item.vcsourcetype is not None and vm.vcsourcetype is not None:
                        if item.vcsourcetype != vm.vcsourcetype:
                            skipped_count += 1
                            continue
                    
                    # Find existing configuration by composite key
                    existing = session.query(ValidationConfiguration).filter(
                        ValidationConfiguration.intvalidationmasterid == item.intvalidationmasterid,
                        ValidationConfiguration.intclientid == item.intclientid,
                        ValidationConfiguration.intfundid == item.intfundid
                    ).first()
                    
                    if existing:
                        # Update existing configuration
                        if item.vccondition is not None:
                            existing.vccondition = item.vccondition
                        if item.intthreshold is not None:
                            existing.intthreshold = item.intthreshold
                        if item.vcthresholdtype is not None:
                            existing.vcthresholdtype = item.vcthresholdtype
                        if item.vcthreshold_abs_range is not None:
                            existing.vcthreshold_abs_range = item.vcthreshold_abs_range
                        if item.intthresholdmin is not None:
                            existing.intthresholdmin = item.intthresholdmin
                        if item.intthresholdmax is not None:
                            existing.intthresholdmax = item.intthresholdmax
                        if item.intprecision is not None:
                            existing.intprecision = item.intprecision
                        if item.isactive is not None:
                            existing.isactive = item.isactive
                        existing.intupdatedby = user_id
                        existing.dtupdatedat = datetime.now()
                        updated_count += 1
                        session.flush()
                        result_configs.append(_validation_configuration_to_graphql(existing, session))
                    else:
                        # Create new configuration
                        new_cfg = ValidationConfiguration(
                            intclientid=item.intclientid,
                            intfundid=item.intfundid,
                            intvalidationmasterid=item.intvalidationmasterid,
                            isactive=item.isactive if item.isactive is not None else True,
                            vccondition=item.vccondition,
                            intthreshold=item.intthreshold,
                            vcthresholdtype=item.vcthresholdtype,
                            vcthreshold_abs_range=item.vcthreshold_abs_range,
                            intthresholdmin=item.intthresholdmin,
                            intthresholdmax=item.intthresholdmax,
                            intprecision=item.intprecision,
                            intcreatedby=user_id,
                            dtcreatedat=datetime.now()
                        )
                        session.add(new_cfg)
                        session.flush()
                        created_count += 1
                        result_configs.append(_validation_configuration_to_graphql(new_cfg, session))
                
                # Commit configuration changes before table creation to avoid losing updates
                session.commit()

                # Get unique client IDs from active items we processed
                processed_client_ids = list({
                    item.intclientid for item in input.items
                    if item.intclientid is not None
                })

                # Perform table creation in a separate session to isolate errors
                if processed_client_ids:
                    session_tables = db_manager.get_session()
                    try:
                        # Determine schema from client code (defaults to 'validus')
                        schema_rows = session_tables.query(Client.code).filter(Client.id.in_(processed_client_ids)).all()
                        raw_schema_name = schema_rows[0].code if schema_rows else 'validus'
                        # Prefer DatabaseManager to derive and create schema
                        try:
                            schema_name = db_manager.create_client_schema(raw_schema_name)
                        except Exception as e:
                            logger.warning(f"db_manager.create_client_schema failed for '{raw_schema_name}': {e}")
                            schema_name = raw_schema_name
                        # Fallback sanitize
                        import re
                        schema_name = re.sub(r"[^a-zA-Z0-9_]", "_", (schema_name or 'validus')).lower() or 'validus'

                        # Ensure schema exists on this connection too
                        try:
                            session_tables.execute(text(f"CREATE SCHEMA IF NOT EXISTS \"{schema_name}\""))
                            session_tables.commit()
                        except Exception as e:
                            logger.warning(f"CREATE SCHEMA failed for '{schema_name}' on session_tables: {e}")

                        # Optionally set search path (not strictly required since we qualify in CREATE)
                        try:
                            session_tables.execute(text(f"SET search_path TO \"{schema_name}\", public"))
                        except Exception as e:
                            logger.warning(f"Could not set search_path to '{schema_name}': {e}")

                        all_data_models = session_tables.query(DataModelMaster).all()
                        logger.info(f"Creating physical tables for {len(all_data_models)} data models for clients: {processed_client_ids} in schema: {schema_name}")
                        
                        # Create tables from data models
                        for data_model in all_data_models:
                            try:
                                success, message, sql_statement = _createTableFromDataModel(
                                    data_model.intdatamodelid,
                                    session_tables,
                                    schema_name
                                )
                                if success:
                                    logger.info(f"Created table for data model {data_model.intdatamodelid} ({data_model.vcmodelname})")
                                else:
                                    logger.warning(f"Failed to create table for data model {data_model.intdatamodelid}: {message}")
                            except Exception as e:
                                # _createTableFromDataModel already rolls back on error
                                logger.error(f"Error creating table for data model {data_model.intdatamodelid}: {str(e)}")
                        
                        # Create validation result table directly (has fixed structure)
                        validation_result_table_sql = f"""
Create table if not exists {schema_name}.tbl_validation_result(
	intvalidationresultid bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
	intprocessinstanceid bigint,
	intdatamodelid int,
	intvalidationconfigurationid INT, 
	intdmrecid bigint,
	vcside varchar(1), --A or B
	intsideuniqueid bigint,
	intmatchid bigint,
	intformulaoutput decimal(32,6),
	vcformulaoutput text,
	vcstatus varchar(100), -- Passed / Failed etc
	vcaction varchar(100), -- No change/ Override / Assign etc
	intactionuserid int,
	dtactiontime timestamp,
	intassignedtouserid int,
	vcassignedstatus varchar(100), -- Open, completed etc
	intnewvalue numeric(32,6),
	vccomment varchar(500),
	isactive boolean DEFAULT true,
	CONSTRAINT tbl_validation_result_details_intprocessinstanceid_fkey FOREIGN KEY (intprocessinstanceid)
        REFERENCES validus.tbl_process_instance (intprocessinstanceid),
	CONSTRAINT tbl_validation_result_details_intdatamodelid_fkey FOREIGN KEY (intdatamodelid)
        REFERENCES validus.tbl_data_model_master (intdatamodelid),
	CONSTRAINT tbl_validation_result_details_intvalidationconfigurationid_fkey FOREIGN KEY (intvalidationconfigurationid)
        REFERENCES validus.tbl_validation_configuration (intvalidationconfigurationid)
)"""
                        try:
                            session_tables.execute(text(validation_result_table_sql))
                            session_tables.commit()
                            logger.info(f"Created validation result table in schema '{schema_name}'")
                        except Exception as e:
                            logger.warning(f"Failed to create validation result table in schema '{schema_name}': {str(e)}")
                            try:
                                session_tables.rollback()
                            except Exception:
                                pass
                    finally:
                        session_tables.close()
                
                return BulkUpsertValidationConfigResultType(
                    success=True,
                    message="Bulk upsert completed",
                    createdCount=created_count,
                    updatedCount=updated_count,
                    skippedCount=skipped_count,
                    configurations=result_configs
                )
            except Exception as e:
                session.rollback()
                raise e
            finally:
                session.close()
        except Exception as e:
            logger.error(f"Error in bulk upsert of validation configurations: {str(e)}")
            error_msg = handle_database_error(e, 'bulk upsert validation configurations')
            return BulkUpsertValidationConfigResultType(
                success=False,
                message=error_msg,
                createdCount=0,
                updatedCount=0,
                skippedCount=0,
                configurations=[]
            )
    
    @strawberry.mutation
    def upsert_ratio_configurations_bulk(
        self,
        info: Info,
        input: BulkRatioConfigurationUpsertInput
    ) -> BulkUpsertRatioConfigResultType:
        """
        Create or update ratio configurations in bulk.
        - If a configuration exists for (intclientid, intfundid, intratiomasterid), update it
        - Otherwise create a new configuration
        - Optionally validate vcsourcetype against the ratio master when provided
        """
        require_authentication(info)
        
        created_count = 0
        updated_count = 0
        skipped_count = 0
        result_configs: List[RatioConfigurationType] = []
        
        try:
            db_manager = get_database_manager()
            session = db_manager.get_session()
            
            try:
                current_user = get_current_user(info)
                user_id = current_user.get('id') if current_user else None
                
                # Preload ratio masters for ids in payload
                unique_master_ids = list({item.intratiomasterid for item in input.items})
                if unique_master_ids:
                    masters = session.query(RatioMaster).filter(
                        RatioMaster.intratiomasterid.in_(unique_master_ids)
                    ).all()
                else:
                    masters = []
                masters_by_id = {m.intratiomasterid: m for m in masters}
                
                for item in input.items:                    
                    # Validate master existence
                    rm = masters_by_id.get(item.intratiomasterid)
                    if not rm:
                        skipped_count += 1
                        continue
                    
                    # Optional vcsourcetype validation against master
                    if item.vcsourcetype is not None and rm.vcsourcetype is not None:
                        if item.vcsourcetype != rm.vcsourcetype:
                            skipped_count += 1
                            continue
                    
                    # Find existing configuration by composite key
                    existing = session.query(RatioConfiguration).filter(
                        RatioConfiguration.intratiomasterid == item.intratiomasterid,
                        RatioConfiguration.intclientid == item.intclientid,
                        RatioConfiguration.intfundid == item.intfundid
                    ).first()
                    
                    if existing:
                        # Update existing configuration
                        if item.vccondition is not None:
                            existing.vccondition = item.vccondition
                        if item.intthreshold is not None:
                            existing.intthreshold = item.intthreshold
                        if item.vcthresholdtype is not None:
                            existing.vcthresholdtype = item.vcthresholdtype
                        if item.vcthreshold_abs_range is not None:
                            existing.vcthreshold_abs_range = item.vcthreshold_abs_range
                        if item.intthresholdmin is not None:
                            existing.intthresholdmin = item.intthresholdmin
                        if item.intthresholdmax is not None:
                            existing.intthresholdmax = item.intthresholdmax
                        if item.intprecision is not None:
                            existing.intprecision = item.intprecision
                        if item.isactive is not None:
                            existing.isactive = item.isactive
                        existing.intupdatedby = user_id
                        existing.dtupdatedat = datetime.now()
                        updated_count += 1
                        session.flush()
                        result_configs.append(_ratio_configuration_to_graphql(existing, session))
                    else:
                        # Create new configuration
                        new_cfg = RatioConfiguration(
                            intclientid=item.intclientid,
                            intfundid=item.intfundid,
                            intratiomasterid=item.intratiomasterid,
                            isactive=item.isactive if item.isactive is not None else False,
                            vccondition=item.vccondition,
                            intthreshold=item.intthreshold,
                            vcthresholdtype=item.vcthresholdtype,
                            vcthreshold_abs_range=item.vcthreshold_abs_range,
                            intthresholdmin=item.intthresholdmin,
                            intthresholdmax=item.intthresholdmax,
                            intprecision=item.intprecision,
                            intcreatedby=user_id,
                            dtcreatedat=datetime.now()
                        )
                        session.add(new_cfg)
                        session.flush()
                        created_count += 1
                        result_configs.append(_ratio_configuration_to_graphql(new_cfg, session))
                
                # Commit configuration changes
                session.commit()
                
                
                # Get unique client IDs from all successfully processed configurations
                processed_client_ids = list({
                    config.intclientid for config in result_configs
                    if config.intclientid is not None
                })

                
                # Perform table creation in a separate session to isolate errors
                if processed_client_ids:
                    session_tables = db_manager.get_session()
                    try:
                        # Determine schema from client code (defaults to 'validus')
                        schema_rows = session_tables.query(Client.code).filter(Client.id.in_(processed_client_ids)).all()
                        raw_schema_name = schema_rows[0].code if schema_rows else 'validus'
                        # Prefer DatabaseManager to derive and create schema
                        try:
                            schema_name = db_manager.create_client_schema(raw_schema_name)
                        except Exception as e:
                            logger.warning(f"db_manager.create_client_schema failed for '{raw_schema_name}': {e}")
                            schema_name = raw_schema_name
                        # Fallback sanitize
                        import re
                        schema_name = re.sub(r"[^a-zA-Z0-9_]", "_", (schema_name or 'validus')).lower() or 'validus'

                        # Ensure schema exists on this connection too
                        try:
                            session_tables.execute(text(f"CREATE SCHEMA IF NOT EXISTS \"{schema_name}\""))
                            session_tables.commit()
                        except Exception as e:
                            logger.warning(f"CREATE SCHEMA failed for '{schema_name}' on session_tables: {e}")

                        # Optionally set search path (not strictly required since we qualify in CREATE)
                        try:
                            session_tables.execute(text(f"SET search_path TO \"{schema_name}\", public"))
                        except Exception as e:
                            logger.warning(f"Could not set search_path to '{schema_name}': {e}")

                        all_data_models = session_tables.query(DataModelMaster).all()
                        logger.info(f"Creating physical tables for {len(all_data_models)} data models for clients: {processed_client_ids} in schema: {schema_name}")
                        
                        # Create tables from data models
                        for data_model in all_data_models:
                            try:
                                success, message, sql_statement = _createTableFromDataModel(
                                    data_model.intdatamodelid,
                                    session_tables,
                                    schema_name
                                )
                                if success:
                                    logger.info(f"Created table for data model {data_model.intdatamodelid} ({data_model.vcmodelname})")
                                else:
                                    logger.warning(f"Failed to create table for data model {data_model.intdatamodelid}: {message}")
                            except Exception as e:
                                # _createTableFromDataModel already rolls back on error
                                logger.error(f"Error creating table for data model {data_model.intdatamodelid}: {str(e)}")
                        
                        # Create validation result table directly (has fixed structure)
                        validation_result_table_sql = f"""
Create table if not exists {schema_name}.tbl_ratio_result
(
	intratioresultid bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
	intprocessinstanceid bigint,
	intdatamodelid int,
	intratioconfigurationid INT, 
	vcside varchar(1), --A or B
	intsideuniqueid bigint,
	intmatchid bigint,
	intnumeratoroutput numeric(32,6),
	intdenominatoroutput numeric(32,6),
	intformulaoutput numeric(32,6),
	vcformulaoutput text,
	vcstatus varchar(100), -- Passed / Failed etc
	vcaction varchar(100), -- No change/ Override / Assign etc
	intactionuserid int,
	dtactiontime timestamp,
	vccomment varchar(500),
	isactive boolean DEFAULT true,
	CONSTRAINT tbl_tbl_ratio_result_details_intprocessinstanceid_fkey FOREIGN KEY (intprocessinstanceid)
        REFERENCES validus.tbl_process_instance (intprocessinstanceid),
	CONSTRAINT tbl_tbl_ratio_result_details_intdatamodelid_fkey FOREIGN KEY (intdatamodelid)
        REFERENCES validus.tbl_data_model_master (intdatamodelid),
	CONSTRAINT tbl_tbl_ratio_result_details_intratioconfigurationid_fkey FOREIGN KEY (intratioconfigurationid)
        REFERENCES validus.tbl_ratio_configuration (intratioconfigurationid)
)
"""
                        try:
                            session_tables.execute(text(validation_result_table_sql))
                            session_tables.commit()
                            logger.info(f"Created validation result table in schema '{schema_name}'")
                        except Exception as e:
                            logger.warning(f"Failed to create validation result table in schema '{schema_name}': {str(e)}")
                            try:
                                session_tables.rollback()
                            except Exception:
                                pass
                    finally:
                        session_tables.close()
                

                return BulkUpsertRatioConfigResultType(
                    success=True,
                    message="Bulk upsert completed",
                    createdCount=created_count,
                    updatedCount=updated_count,
                    skippedCount=skipped_count,
                    configurations=result_configs
                )
            except Exception as e:
                session.rollback()
                raise e
            finally:
                session.close()
        except Exception as e:
            logger.error(f"Error in bulk upsert of ratio configurations: {str(e)}")
            error_msg = handle_database_error(e, 'bulk upsert ratio configurations')
            return BulkUpsertRatioConfigResultType(
                success=False,
                message=error_msg,
                createdCount=0,
                updatedCount=0,
                skippedCount=0,
                configurations=[]
            )
    
    @strawberry.mutation
    def create_validation(
        self, 
        info: Info, 
        validation_input: ValidationMasterInput,
        details_input: List[ValidationDetailsInput]
    ) -> CreateValidationResultType:
        """
        Create a new validation entry with details
        Args:
            validation_input: Validation master data
            details_input: List of validation details (at least one required)
        """
        # Require authentication
        require_authentication(info)
        
        if not details_input or len(details_input) == 0:
            return CreateValidationResultType(
                success=False,
                message="At least one validation detail is required",
                validation=None
            )
        
        try:
            db_manager = get_database_manager()
            session = db_manager.get_session()
            
            try:
                # Get current user from context
                current_user = get_current_user(info)
                user_id = current_user.get('id') if current_user else None
                
                # Check for duplicate validation name
                if validation_input.vcvalidationname:
                    if check_validation_name_duplicate(session, validation_input.vcvalidationname):
                        return CreateValidationResultType(
                            success=False,
                            message=get_error_message('VALIDATION_NAME_DUPLICATE'),
                            validation=None
                        )
                
                # Create new validation with boolean values directly
                new_validation = ValidationMaster(
                    intsubproductid=validation_input.intsubproductid,
                    vcsourcetype=validation_input.vcsourcetype,
                    vctype=validation_input.vctype,
                    vcsubtype=validation_input.vcsubtype,
                    issubtype_subtotal=validation_input.issubtype_subtotal,
                    vcvalidationname=validation_input.vcvalidationname,
                    isvalidation_subtotal=validation_input.isvalidation_subtotal,
                    vcdescription=validation_input.vcdescription,
                    intthreshold=validation_input.intthreshold,
                    vcthresholdtype=validation_input.vcthresholdtype,
                    vcthreshold_abs_range=validation_input.vcthreshold_abs_range,
                    intthresholdmin=validation_input.intthresholdmin,
                    intthresholdmax=validation_input.intthresholdmax,
                    intprecision=validation_input.intprecision,
                    isactive=validation_input.isactive if validation_input.isactive is not None else True,
                    intcreatedby=user_id,
                    dtcreatedat=datetime.now()
                )
                
                session.add(new_validation)
                session.flush()  # Flush to get the ID
                
                # Create validation details
                for detail in details_input:
                    new_detail = ValidationDetails(
                        intvalidationmasterid=new_validation.intvalidationmasterid,
                        intdatamodelid=detail.intdatamodelid,
                        intgroup_attributeid=detail.intgroup_attributeid,
                        intassettypeid=detail.intassettypeid,
                        intcalc_attributeid=detail.intcalc_attributeid,
                        vcaggregationtype=detail.vcaggregationtype,
                        vcfilter=detail.vcfilter,
                        vcfiltertype=detail.vcfiltertype,
                        vcformula=detail.vcformula,
                        intcreatedby=user_id,
                        dtcreatedat=datetime.now()
                    )
                    session.add(new_detail)
                
                session.commit()
                session.refresh(new_validation)
                
                return CreateValidationResultType(
                    success=True,
                    message="Validation created successfully",
                    validation=_validation_master_to_graphql(new_validation)
                )
            except Exception as e:
                session.rollback()
                raise e
            finally:
                session.close()
                
        except GraphQLError:
            raise
        except Exception as e:
            logger.error(f"Error creating validation: {str(e)}")
            error_msg = handle_database_error(e, 'create validation')
            return CreateValidationResultType(
                success=False,
                message=error_msg,
                validation=None
            )
    
    @strawberry.mutation
    def create_ratio(
        self, 
        info: Info, 
        ratio_input: RatioMasterInput,
        details_input: List[RatioDetailsInput]
    ) -> CreateRatioResultType:
        """
        Create a new ratio entry with details
        Args:
            ratio_input: Ratio master data
            details_input: List of ratio details (at least one required)
        """
        # Require authentication
        require_authentication(info)
        
        if not details_input or len(details_input) == 0:
            return CreateRatioResultType(
                success=False,
                message="At least one ratio detail is required",
                ratio=None
            )
        
        try:
            db_manager = get_database_manager()
            session = db_manager.get_session()
            
            try:
                # Get current user from context
                current_user = get_current_user(info)
                user_id = current_user.get('id') if current_user else None
                
                # Check for duplicate ratio name
                if ratio_input.vcrationame:
                    if check_ratio_name_duplicate(session, ratio_input.vcrationame):
                        return CreateRatioResultType(
                            success=False,
                            message=get_error_message('RATIO_NAME_DUPLICATE'),
                            ratio=None
                        )
                
                # Create new ratio with boolean values directly
                new_ratio = RatioMaster(
                    intsubproductid=ratio_input.intsubproductid,
                    vcsourcetype=ratio_input.vcsourcetype,
                    vctype=ratio_input.vctype,
                    vcrationame=ratio_input.vcrationame,
                    isratio_subtotal=ratio_input.isratio_subtotal,
                    vcdescription=ratio_input.vcdescription,
                    intthreshold=ratio_input.intthreshold,
                    vcthresholdtype=ratio_input.vcthresholdtype,
                    vcthreshold_abs_range=ratio_input.vcthreshold_abs_range,
                    intthresholdmin=ratio_input.intthresholdmin,
                    intthresholdmax=ratio_input.intthresholdmax,
                    intprecision=ratio_input.intprecision,
                    isactive=ratio_input.isactive if ratio_input.isactive is not None else True,
                    intcreatedby=user_id,
                    dtcreatedat=datetime.now()
                )
                
                session.add(new_ratio)
                session.flush()  # Flush to get the ID
                
                # Create ratio details
                for detail in details_input:
                    new_detail = RatioDetails(
                        intratiomasterid=new_ratio.intratiomasterid,
                        intdatamodelid=detail.intdatamodelid,
                        vcfilter=detail.vcfilter,
                        vcfiltertype=detail.vcfiltertype,
                        vcnumerator=detail.vcnumerator,
                        vcdenominator=detail.vcdenominator,
                        vcformula=detail.vcformula,
                        intcreatedby=user_id,
                        dtcreatedat=datetime.now()
                    )
                    session.add(new_detail)
                
                session.commit()
                session.refresh(new_ratio)
                
                return CreateRatioResultType(
                    success=True,
                    message="Ratio created successfully",
                    ratio=_ratio_master_to_graphql(new_ratio)
                )
            except Exception as e:
                session.rollback()
                raise e
            finally:
                session.close()
                
        except GraphQLError:
            raise
        except Exception as e:
            logger.error(f"Error creating ratio: {str(e)}")
            error_msg = handle_database_error(e, 'create ratio')
            return CreateRatioResultType(
                success=False,
                message=error_msg,
                ratio=None
            )
    
    @strawberry.mutation
    def update_validation(
        self, 
        info: Info, 
        validation_input: UpdateValidationMasterInput
    ) -> UpdateValidationResultType:
        """
        Update an existing validation entry
        Args:
            validation_input: Updated validation data with ID
        """
        # Require authentication
        require_authentication(info)
        
        try:
            db_manager = get_database_manager()
            session = db_manager.get_session()
            
            try:
                # Get current user from context
                current_user = get_current_user(info)
                user_id = current_user.get('id') if current_user else None
                
                # Get existing validation
                validation = session.query(ValidationMaster).filter(
                    ValidationMaster.intvalidationmasterid == validation_input.intvalidationmasterid
                ).first()
                
                if not validation:
                    return UpdateValidationResultType(
                        success=False,
                        message=get_error_message('VALIDATION_NOT_FOUND'),
                        validation=None
                    )
                
                # Update fields if provided using ORM
                if validation_input.intsubproductid is not None:
                    validation.intsubproductid = validation_input.intsubproductid
                
                if validation_input.vcsourcetype is not None:
                    validation.vcsourcetype = validation_input.vcsourcetype
                
                if validation_input.vctype is not None:
                    validation.vctype = validation_input.vctype
                
                if validation_input.vcsubtype is not None:
                    validation.vcsubtype = validation_input.vcsubtype
                
                if validation_input.issubtype_subtotal is not None:
                    validation.issubtype_subtotal = validation_input.issubtype_subtotal
                
                if validation_input.vcvalidationname is not None:
                    # Check for duplicate validation name (excluding current validation)
                    if check_validation_name_duplicate(
                        session, 
                        validation_input.vcvalidationname,
                        exclude_validation_id=validation.intvalidationmasterid
                    ):
                        return UpdateValidationResultType(
                            success=False,
                            message=get_error_message('VALIDATION_NAME_DUPLICATE'),
                            validation=None
                        )
                    validation.vcvalidationname = validation_input.vcvalidationname
                
                if validation_input.isvalidation_subtotal is not None:
                    validation.isvalidation_subtotal = validation_input.isvalidation_subtotal
                
                if validation_input.vcdescription is not None:
                    validation.vcdescription = validation_input.vcdescription
                
                if validation_input.intthreshold is not None:
                    validation.intthreshold = validation_input.intthreshold
                
                if validation_input.vcthresholdtype is not None:
                    validation.vcthresholdtype = validation_input.vcthresholdtype
                
                if validation_input.vcthreshold_abs_range is not None:
                    validation.vcthreshold_abs_range = validation_input.vcthreshold_abs_range
                
                if validation_input.intthresholdmin is not None:
                    validation.intthresholdmin = validation_input.intthresholdmin
                
                if validation_input.intthresholdmax is not None:
                    validation.intthresholdmax = validation_input.intthresholdmax
                
                if validation_input.intprecision is not None:
                    validation.intprecision = validation_input.intprecision
                
                if validation_input.isactive is not None:
                    validation.isactive = validation_input.isactive
                
                # Update metadata
                validation.intupdatedby = user_id
                validation.dtupdatedat = datetime.now()
                
                session.commit()
                session.refresh(validation)
                
                return UpdateValidationResultType(
                    success=True,
                    message="Validation updated successfully",
                    validation=_validation_master_to_graphql(validation)
                )
            except Exception as e:
                session.rollback()
                raise e
            finally:
                session.close()
                
        except GraphQLError:
            raise
        except Exception as e:
            logger.error(f"Error updating validation: {str(e)}")
            error_msg = handle_database_error(e, 'update validation')
            return UpdateValidationResultType(
                success=False,
                message=error_msg,
                validation=None
            )
    
    @strawberry.mutation
    def update_ratio(
        self, 
        info: Info, 
        ratio_input: UpdateRatioMasterInput
    ) -> UpdateRatioResultType:
        """
        Update an existing ratio entry
        Args:
            ratio_input: Updated ratio data with ID
        """
        # Require authentication
        require_authentication(info)
        
        try:
            db_manager = get_database_manager()
            session = db_manager.get_session()
            
            try:
                # Get current user from context
                current_user = get_current_user(info)
                user_id = current_user.get('id') if current_user else None
                
                # Get existing ratio
                ratio = session.query(RatioMaster).filter(
                    RatioMaster.intratiomasterid == ratio_input.intratiomasterid
                ).first()
                
                if not ratio:
                    return UpdateRatioResultType(
                        success=False,
                        message=get_error_message('RATIO_NOT_FOUND'),
                        ratio=None
                    )
                
                # Update fields if provided using ORM
                if ratio_input.intsubproductid is not None:
                    ratio.intsubproductid = ratio_input.intsubproductid
                
                if ratio_input.vcsourcetype is not None:
                    ratio.vcsourcetype = ratio_input.vcsourcetype
                
                if ratio_input.vctype is not None:
                    ratio.vctype = ratio_input.vctype
                
                if ratio_input.vcrationame is not None:
                    # Check for duplicate ratio name (excluding current ratio)
                    if check_ratio_name_duplicate(
                        session,
                        ratio_input.vcrationame,
                        exclude_ratio_id=ratio.intratiomasterid
                    ):
                        return UpdateRatioResultType(
                            success=False,
                            message=get_error_message('RATIO_NAME_DUPLICATE'),
                            ratio=None
                        )
                    ratio.vcrationame = ratio_input.vcrationame
                
                if ratio_input.isratio_subtotal is not None:
                    ratio.isratio_subtotal = ratio_input.isratio_subtotal
                
                if ratio_input.vcdescription is not None:
                    ratio.vcdescription = ratio_input.vcdescription
                
                if ratio_input.intthreshold is not None:
                    ratio.intthreshold = ratio_input.intthreshold
                
                if ratio_input.vcthresholdtype is not None:
                    ratio.vcthresholdtype = ratio_input.vcthresholdtype
                
                if ratio_input.vcthreshold_abs_range is not None:
                    ratio.vcthreshold_abs_range = ratio_input.vcthreshold_abs_range
                
                if ratio_input.intthresholdmin is not None:
                    ratio.intthresholdmin = ratio_input.intthresholdmin
                
                if ratio_input.intthresholdmax is not None:
                    ratio.intthresholdmax = ratio_input.intthresholdmax
                
                if ratio_input.intprecision is not None:
                    ratio.intprecision = ratio_input.intprecision
                
                if ratio_input.isactive is not None:
                    ratio.isactive = ratio_input.isactive
                
                # Update metadata
                ratio.intupdatedby = user_id
                ratio.dtupdatedat = datetime.now()
                
                session.commit()
                session.refresh(ratio)
                
                return UpdateRatioResultType(
                    success=True,
                    message="Ratio updated successfully",
                    ratio=_ratio_master_to_graphql(ratio)
                )
            except Exception as e:
                session.rollback()
                raise e
            finally:
                session.close()
                
        except GraphQLError:
            raise
        except Exception as e:
            logger.error(f"Error updating ratio: {str(e)}")
            error_msg = handle_database_error(e, 'update ratio')
            return UpdateRatioResultType(
                success=False,
                message=error_msg,
                ratio=None
            )
    
    @strawberry.mutation
    def update_validation_complete(
        self, 
        info: Info, 
        input: UpdateValidationCompleteInput
    ) -> UpdateValidationResultType:
        """
        Update validation master and details together
        Args:
            input: Complete validation update data with master + details
        """
        require_authentication(info)
        
        try:
            db_manager = get_database_manager()
            session = db_manager.get_session()
            
            try:
                current_user = get_current_user(info)
                user_id = current_user.get('id') if current_user else None
                
                # Get existing validation
                validation = session.query(ValidationMaster).filter(
                    ValidationMaster.intvalidationmasterid == input.intvalidationmasterid
                ).first()
                
                if not validation:
                    return UpdateValidationResultType(
                        success=False,
                        message=get_error_message('VALIDATION_NOT_FOUND'),
                        validation=None
                    )
                
                # Update master fields
                if input.intsubproductid is not None:
                    validation.intsubproductid = input.intsubproductid
                if input.vcsourcetype is not None:
                    validation.vcsourcetype = input.vcsourcetype
                if input.vctype is not None:
                    validation.vctype = input.vctype
                if input.vcsubtype is not None:
                    validation.vcsubtype = input.vcsubtype
                if input.issubtype_subtotal is not None:
                    validation.issubtype_subtotal = input.issubtype_subtotal
                if input.vcvalidationname is not None:
                    # Check for duplicate validation name (excluding current validation)
                    if check_validation_name_duplicate(
                        session,
                        input.vcvalidationname,
                        exclude_validation_id=input.intvalidationmasterid
                    ):
                        return UpdateValidationResultType(
                            success=False,
                            message=get_error_message('VALIDATION_NAME_DUPLICATE'),
                            validation=None
                        )
                    validation.vcvalidationname = input.vcvalidationname
                if input.isvalidation_subtotal is not None:
                    validation.isvalidation_subtotal = input.isvalidation_subtotal
                if input.vcdescription is not None:
                    validation.vcdescription = input.vcdescription
                if input.intthreshold is not None:
                    validation.intthreshold = input.intthreshold
                if input.vcthresholdtype is not None:
                    validation.vcthresholdtype = input.vcthresholdtype
                if input.vcthreshold_abs_range is not None:
                    validation.vcthreshold_abs_range = input.vcthreshold_abs_range
                if input.intthresholdmin is not None:
                    validation.intthresholdmin = input.intthresholdmin
                if input.intthresholdmax is not None:
                    validation.intthresholdmax = input.intthresholdmax
                if input.intprecision is not None:
                    validation.intprecision = input.intprecision
                if input.isactive is not None:
                    validation.isactive = input.isactive
                
                validation.intupdatedby = user_id
                validation.dtupdatedat = datetime.now()
                
                # Handle details updates
                if input.update_details:
                    for detail_update in input.update_details:
                        detail = session.query(ValidationDetails).filter(
                            ValidationDetails.intvalidationdetailid == detail_update.intvalidationdetailid,
                            ValidationDetails.intvalidationmasterid == input.intvalidationmasterid
                        ).first()
                        if detail:
                            if detail_update.intdatamodelid is not None:
                                detail.intdatamodelid = detail_update.intdatamodelid
                            if detail_update.intgroup_attributeid is not None:
                                detail.intgroup_attributeid = detail_update.intgroup_attributeid
                            if detail_update.intassettypeid is not None:
                                detail.intassettypeid = detail_update.intassettypeid
                            if detail_update.intcalc_attributeid is not None:
                                detail.intcalc_attributeid = detail_update.intcalc_attributeid
                            if detail_update.vcaggregationtype is not None:
                                detail.vcaggregationtype = detail_update.vcaggregationtype
                            if detail_update.vcfilter is not None:
                                detail.vcfilter = detail_update.vcfilter
                            if detail_update.vcfiltertype is not None:
                                detail.vcfiltertype = detail_update.vcfiltertype
                            if detail_update.vcformula is not None:
                                detail.vcformula = detail_update.vcformula
                            detail.intupdatedby = user_id
                            detail.dtupdatedat = datetime.now()
                        else:
                            logger.warning(f"Validation detail {detail_update.intvalidationdetailid} not found for master {input.intvalidationmasterid}")
                
                # Handle new details
                if input.new_details:
                    for detail_input in input.new_details:
                        new_detail = ValidationDetails(
                            intvalidationmasterid=input.intvalidationmasterid,
                            intdatamodelid=detail_input.intdatamodelid,
                            intgroup_attributeid=detail_input.intgroup_attributeid,
                            intassettypeid=detail_input.intassettypeid,
                            intcalc_attributeid=detail_input.intcalc_attributeid,
                            vcaggregationtype=detail_input.vcaggregationtype,
                            vcfilter=detail_input.vcfilter,
                            vcfiltertype=detail_input.vcfiltertype,
                            vcformula=detail_input.vcformula,
                            intcreatedby=user_id,
                            dtcreatedat=datetime.now()
                        )
                        session.add(new_detail)
                
                # Handle deletions
                if input.delete_detail_ids:
                    for detail_id in input.delete_detail_ids:
                        detail = session.query(ValidationDetails).filter(
                            ValidationDetails.intvalidationdetailid == detail_id
                        ).first()
                        if detail:
                            session.delete(detail)
                
                session.commit()
                session.refresh(validation)
                
                return UpdateValidationResultType(
                    success=True,
                    message="Validation updated successfully",
                    validation=_validation_master_to_graphql(validation)
                )
            except Exception as e:
                session.rollback()
                raise e
            finally:
                session.close()
                
        except Exception as e:
            logger.error(f"Error updating validation complete: {str(e)}")
            error_msg = handle_database_error(e, 'update validation')
            return UpdateValidationResultType(
                success=False,
                message=error_msg,
                validation=None
            )
    
    @strawberry.mutation
    def update_ratio_complete(
        self, 
        info: Info, 
        input: UpdateRatioCompleteInput
    ) -> UpdateRatioResultType:
        """
        Update ratio master and details together
        Args:
            input: Complete ratio update data with master + details
        """
        require_authentication(info)
        
        try:
            db_manager = get_database_manager()
            session = db_manager.get_session()
            
            try:
                current_user = get_current_user(info)
                user_id = current_user.get('id') if current_user else None
                
                # Get existing ratio
                ratio = session.query(RatioMaster).filter(
                    RatioMaster.intratiomasterid == input.intratiomasterid
                ).first()
                
                if not ratio:
                    return UpdateRatioResultType(
                        success=False,
                        message=get_error_message('RATIO_NOT_FOUND'),
                        ratio=None
                    )
                
                # Update master fields
                if input.intsubproductid is not None:
                    ratio.intsubproductid = input.intsubproductid
                if input.vcsourcetype is not None:
                    ratio.vcsourcetype = input.vcsourcetype
                if input.vctype is not None:
                    ratio.vctype = input.vctype
                if input.vcrationame is not None:
                    # Check for duplicate ratio name (excluding current ratio)
                    if check_ratio_name_duplicate(
                        session,
                        input.vcrationame,
                        exclude_ratio_id=input.intratiomasterid
                    ):
                        return UpdateRatioResultType(
                            success=False,
                            message=get_error_message('RATIO_NAME_DUPLICATE'),
                            ratio=None
                        )
                    ratio.vcrationame = input.vcrationame
                if input.isratio_subtotal is not None:
                    ratio.isratio_subtotal = input.isratio_subtotal
                if input.vcdescription is not None:
                    ratio.vcdescription = input.vcdescription
                if input.intthreshold is not None:
                    ratio.intthreshold = input.intthreshold
                if input.vcthresholdtype is not None:
                    ratio.vcthresholdtype = input.vcthresholdtype
                if input.vcthreshold_abs_range is not None:
                    ratio.vcthreshold_abs_range = input.vcthreshold_abs_range
                if input.intthresholdmin is not None:
                    ratio.intthresholdmin = input.intthresholdmin
                if input.intthresholdmax is not None:
                    ratio.intthresholdmax = input.intthresholdmax
                if input.intprecision is not None:
                    ratio.intprecision = input.intprecision
                if input.isactive is not None:
                    ratio.isactive = input.isactive
                
                ratio.intupdatedby = user_id
                ratio.dtupdatedat = datetime.now()
                
                # Handle details updates
                if input.update_details:
                    for detail_update in input.update_details:
                        detail = session.query(RatioDetails).filter(
                            RatioDetails.intratiodetailid == detail_update.intratiodetailid,
                            RatioDetails.intratiomasterid == input.intratiomasterid
                        ).first()
                        if detail:
                            if detail_update.intdatamodelid is not None:
                                detail.intdatamodelid = detail_update.intdatamodelid
                            if detail_update.vcfilter is not None:
                                detail.vcfilter = detail_update.vcfilter
                            if detail_update.vcfiltertype is not None:
                                detail.vcfiltertype = detail_update.vcfiltertype
                            if detail_update.vcnumerator is not None:
                                detail.vcnumerator = detail_update.vcnumerator
                            if detail_update.vcdenominator is not None:
                                detail.vcdenominator = detail_update.vcdenominator
                            if detail_update.vcformula is not None:
                                detail.vcformula = detail_update.vcformula
                            detail.intupdatedby = user_id
                            detail.dtupdatedat = datetime.now()
                        else:
                            logger.warning(f"Ratio detail {detail_update.intratiodetailid} not found for master {input.intratiomasterid}")
                
                # Handle new details
                if input.new_details:
                    for detail_input in input.new_details:
                        new_detail = RatioDetails(
                            intratiomasterid=input.intratiomasterid,
                            intdatamodelid=detail_input.intdatamodelid,
                            vcfilter=detail_input.vcfilter,
                            vcfiltertype=detail_input.vcfiltertype,
                            vcnumerator=detail_input.vcnumerator,
                            vcdenominator=detail_input.vcdenominator,
                            vcformula=detail_input.vcformula,
                            intcreatedby=user_id,
                            dtcreatedat=datetime.now()
                        )
                        session.add(new_detail)
                
                # Handle deletions
                if input.delete_detail_ids:
                    for detail_id in input.delete_detail_ids:
                        detail = session.query(RatioDetails).filter(
                            RatioDetails.intratiodetailid == detail_id
                        ).first()
                        if detail:
                            session.delete(detail)
                
                session.commit()
                session.refresh(ratio)
                
                return UpdateRatioResultType(
                    success=True,
                    message="Ratio updated successfully",
                    ratio=_ratio_master_to_graphql(ratio)
                )
            except Exception as e:
                session.rollback()
                raise e
            finally:
                session.close()
                
        except Exception as e:
            logger.error(f"Error updating ratio complete: {str(e)}")
            error_msg = handle_database_error(e, 'update ratio')
            return UpdateRatioResultType(
                success=False,
                message=error_msg,
                ratio=None
            )
    
    @strawberry.mutation
    def delete_validation(
        self, 
        info: Info, 
        intvalidationmasterid: int
    ) -> DeleteValidationResultType:
        """
        Delete a validation master and all its details
        Args:
            intvalidationmasterid: Validation master ID to delete
        """
        require_authentication(info)
        
        try:
            db_manager = get_database_manager()
            session = db_manager.get_session()
            
            try:
                # Get existing validation
                validation = session.query(ValidationMaster).filter(
                    ValidationMaster.intvalidationmasterid == intvalidationmasterid
                ).first()
                
                if not validation:
                    return DeleteValidationResultType(
                        success=False,
                        message=get_error_message('VALIDATION_NOT_FOUND')
                    )
                
                # Delete the validation (cascade will handle details)
                session.delete(validation)
                session.commit()
                
                return DeleteValidationResultType(
                    success=True,
                    message=f"Validation {intvalidationmasterid} and all its details deleted successfully"
                )
            except Exception as e:
                session.rollback()
                raise e
            finally:
                session.close()
                
        except Exception as e:
            logger.error(f"Error deleting validation: {str(e)}")
            return DeleteValidationResultType(
                success=False,
                message=f"Failed to delete validation: {str(e)}"
            )
    
    @strawberry.mutation
    def delete_ratio(
        self, 
        info: Info, 
        intratiomasterid: int
    ) -> DeleteRatioResultType:
        """
        Delete a ratio master and all its details
        Args:
            intratiomasterid: Ratio master ID to delete
        """
        require_authentication(info)
        
        try:
            db_manager = get_database_manager()
            session = db_manager.get_session()
            
            try:
                # Get existing ratio
                ratio = session.query(RatioMaster).filter(
                    RatioMaster.intratiomasterid == intratiomasterid
                ).first()
                
                if not ratio:
                    return DeleteRatioResultType(
                        success=False,
                        message=get_error_message('RATIO_NOT_FOUND')
                    )
                
                # Delete the ratio (cascade will handle details)
                session.delete(ratio)
                session.commit()
                
                return DeleteRatioResultType(
                    success=True,
                    message=f"Ratio {intratiomasterid} and all its details deleted successfully"
                )
            except Exception as e:
                session.rollback()
                raise e
            finally:
                session.close()
                
        except Exception as e:
            logger.error(f"Error deleting ratio: {str(e)}")
            return DeleteRatioResultType(
                success=False,
                message=f"Failed to delete ratio: {str(e)}"
            )
    
    @strawberry.mutation
    def create_validation_configuration(
        self,
        info: Info,
        input: ValidationConfigurationInput
    ) -> CreateValidationConfigurationResultType:
        """
        Create a new validation configuration
        Args:
            input: Validation configuration input data
        """
        require_authentication(info)
        
        try:
            db_manager = get_database_manager()
            session = db_manager.get_session()
            
            try:
                # Get current user from context
                current_user = get_current_user(info)
                user_id = current_user.get('id') if current_user else None
                
                # Verify validation master exists
                validation_master = session.query(ValidationMaster).filter(
                    ValidationMaster.intvalidationmasterid == input.intvalidationmasterid
                ).first()
                
                if not validation_master:
                    return CreateValidationConfigurationResultType(
                        success=False,
                        message=get_error_message('VALIDATION_NOT_FOUND'),
                        validationconfiguration=None
                    )
                
                # Create new configuration
                new_config = ValidationConfiguration(
                    intclientid=input.intclientid,
                    intfundid=input.intfundid,
                    intvalidationmasterid=input.intvalidationmasterid,
                    isactive=input.isactive if input.isactive is not None else False,
                    vccondition=input.vccondition,
                    intthreshold=input.intthreshold,
                    vcthresholdtype=input.vcthresholdtype,
                    vcthreshold_abs_range=input.vcthreshold_abs_range,
                    intthresholdmin=input.intthresholdmin,
                    intthresholdmax=input.intthresholdmax,
                    intprecision=input.intprecision,
                    intcreatedby=user_id,
                    dtcreatedat=datetime.now()
                )
                
                session.add(new_config)
                session.commit()
                session.refresh(new_config)
                
                return CreateValidationConfigurationResultType(
                    success=True,
                    message="Validation configuration created successfully",
                    validationconfiguration=_validation_configuration_to_graphql(new_config, session)
                )
            except Exception as e:
                session.rollback()
                raise e
            finally:
                session.close()
                
        except Exception as e:
            logger.error(f"Error creating validation configuration: {str(e)}")
            return CreateValidationConfigurationResultType(
                success=False,
                message=f"Failed to create validation configuration: {str(e)}",
                validationconfiguration=None
            )
    
    @strawberry.mutation
    def update_validation_configuration(
        self,
        info: Info,
        input: UpdateValidationConfigurationInput
    ) -> UpdateValidationConfigurationResultType:
        """
        Update an existing validation configuration
        Args:
            input: Validation configuration update input data
        """
        require_authentication(info)
        
        try:
            db_manager = get_database_manager()
            session = db_manager.get_session()
            
            try:
                # Get current user from context
                current_user = get_current_user(info)
                user_id = current_user.get('id') if current_user else None
                
                # Get existing configuration
                config = session.query(ValidationConfiguration).filter(
                    ValidationConfiguration.intvalidationconfigurationid == input.intvalidationconfigurationid
                ).first()
                
                if not config:
                    return UpdateValidationConfigurationResultType(
                        success=False,
                        message=get_error_message('CONFIGURATION_NOT_FOUND'),
                        validationconfiguration=None
                    )
                
                # Update fields if provided
                if input.intclientid is not None:
                    config.intclientid = input.intclientid
                if input.intfundid is not None:
                    config.intfundid = input.intfundid
                if input.intvalidationmasterid is not None:
                    # Verify validation master exists
                    validation_master = session.query(ValidationMaster).filter(
                        ValidationMaster.intvalidationmasterid == input.intvalidationmasterid
                    ).first()
                    if not validation_master:
                        return UpdateValidationConfigurationResultType(
                            success=False,
                            message=get_error_message('VALIDATION_NOT_FOUND'),
                            validationconfiguration=None
                        )
                    config.intvalidationmasterid = input.intvalidationmasterid
                if input.isactive is not None:
                    config.isactive = input.isactive
                if input.vccondition is not None:
                    config.vccondition = input.vccondition
                if input.intthreshold is not None:
                    config.intthreshold = input.intthreshold
                if input.vcthresholdtype is not None:
                    config.vcthresholdtype = input.vcthresholdtype
                if input.vcthreshold_abs_range is not None:
                    config.vcthreshold_abs_range = input.vcthreshold_abs_range
                if input.intthresholdmin is not None:
                    config.intthresholdmin = input.intthresholdmin
                if input.intthresholdmax is not None:
                    config.intthresholdmax = input.intthresholdmax
                if input.intprecision is not None:
                    config.intprecision = input.intprecision
                
                config.intupdatedby = user_id
                config.dtupdatedat = datetime.now()
                
                session.commit()
                session.refresh(config)
                
                return UpdateValidationConfigurationResultType(
                    success=True,
                    message="Validation configuration updated successfully",
                    validationconfiguration=_validation_configuration_to_graphql(config, session)
                )
            except Exception as e:
                session.rollback()
                raise e
            finally:
                session.close()
                
        except Exception as e:
            logger.error(f"Error updating validation configuration: {str(e)}")
            return UpdateValidationConfigurationResultType(
                success=False,
                message=f"Failed to update validation configuration: {str(e)}",
                validationconfiguration=None
            )
    
    @strawberry.mutation
    def delete_validation_configuration(
        self,
        info: Info,
        intvalidationconfigurationid: int
    ) -> DeleteValidationConfigurationResultType:
        """
        Delete a validation configuration
        Args:
            intvalidationconfigurationid: Validation configuration ID to delete
        """
        require_authentication(info)
        
        try:
            db_manager = get_database_manager()
            session = db_manager.get_session()
            
            try:
                # Get existing configuration
                config = session.query(ValidationConfiguration).filter(
                    ValidationConfiguration.intvalidationconfigurationid == intvalidationconfigurationid
                ).first()
                
                if not config:
                    return DeleteValidationConfigurationResultType(
                        success=False,
                        message=get_error_message('CONFIGURATION_NOT_FOUND')
                    )
                
                session.delete(config)
                session.commit()
                
                return DeleteValidationConfigurationResultType(
                    success=True,
                    message=f"Validation configuration {intvalidationconfigurationid} deleted successfully"
                )
            except Exception as e:
                session.rollback()
                raise e
            finally:
                session.close()
                
        except Exception as e:
            logger.error(f"Error deleting validation configuration: {str(e)}")
            return DeleteValidationConfigurationResultType(
                success=False,
                message=f"Failed to delete validation configuration: {str(e)}"
            )
    
    @strawberry.mutation
    def create_ratio_configuration(
        self,
        info: Info,
        input: RatioConfigurationInput
    ) -> CreateRatioConfigurationResultType:
        """
        Create a new ratio configuration
        Args:
            input: Ratio configuration input data
        """
        require_authentication(info)
        
        try:
            db_manager = get_database_manager()
            session = db_manager.get_session()
            
            try:
                # Get current user from context
                current_user = get_current_user(info)
                user_id = current_user.get('id') if current_user else None
                
                # Verify ratio master exists
                ratio_master = session.query(RatioMaster).filter(
                    RatioMaster.intratiomasterid == input.intratiomasterid
                ).first()
                
                if not ratio_master:
                    return CreateRatioConfigurationResultType(
                        success=False,
                        message=get_error_message('RATIO_NOT_FOUND'),
                        ratioconfiguration=None
                    )
                
                # Create new configuration
                new_config = RatioConfiguration(
                    intclientid=input.intclientid,
                    intfundid=input.intfundid,
                    intratiomasterid=input.intratiomasterid,
                    isactive=input.isactive if input.isactive is not None else False,
                    vccondition=input.vccondition,
                    intthreshold=input.intthreshold,
                    vcthresholdtype=input.vcthresholdtype,
                    vcthreshold_abs_range=input.vcthreshold_abs_range,
                    intthresholdmin=input.intthresholdmin,
                    intthresholdmax=input.intthresholdmax,
                    intprecision=input.intprecision,
                    intcreatedby=user_id,
                    dtcreatedat=datetime.now()
                )
                
                session.add(new_config)
                session.commit()
                session.refresh(new_config)
                
                return CreateRatioConfigurationResultType(
                    success=True,
                    message="Ratio configuration created successfully",
                    ratioconfiguration=_ratio_configuration_to_graphql(new_config, session)
                )
            except Exception as e:
                session.rollback()
                raise e
            finally:
                session.close()
                
        except Exception as e:
            logger.error(f"Error creating ratio configuration: {str(e)}")
            return CreateRatioConfigurationResultType(
                success=False,
                message=f"Failed to create ratio configuration: {str(e)}",
                ratioconfiguration=None
            )
    
    @strawberry.mutation
    def update_ratio_configuration(
        self,
        info: Info,
        input: UpdateRatioConfigurationInput
    ) -> UpdateRatioConfigurationResultType:
        """
        Update an existing ratio configuration
        Args:
            input: Ratio configuration update input data
        """
        require_authentication(info)
        
        try:
            db_manager = get_database_manager()
            session = db_manager.get_session()
            
            try:
                # Get current user from context
                current_user = get_current_user(info)
                user_id = current_user.get('id') if current_user else None
                
                # Get existing configuration
                config = session.query(RatioConfiguration).filter(
                    RatioConfiguration.intratioconfigurationid == input.intratioconfigurationid
                ).first()
                
                if not config:
                    return UpdateRatioConfigurationResultType(
                        success=False,
                        message=get_error_message('CONFIGURATION_NOT_FOUND'),
                        ratioconfiguration=None
                    )
                
                # Update fields if provided
                if input.intclientid is not None:
                    config.intclientid = input.intclientid
                if input.intfundid is not None:
                    config.intfundid = input.intfundid
                if input.intratiomasterid is not None:
                    # Verify ratio master exists
                    ratio_master = session.query(RatioMaster).filter(
                        RatioMaster.intratiomasterid == input.intratiomasterid
                    ).first()
                    if not ratio_master:
                        return UpdateRatioConfigurationResultType(
                            success=False,
                            message=get_error_message('RATIO_NOT_FOUND'),
                            ratioconfiguration=None
                        )
                    config.intratiomasterid = input.intratiomasterid
                if input.isactive is not None:
                    config.isactive = input.isactive
                if input.vccondition is not None:
                    config.vccondition = input.vccondition
                if input.intthreshold is not None:
                    config.intthreshold = input.intthreshold
                if input.vcthresholdtype is not None:
                    config.vcthresholdtype = input.vcthresholdtype
                if input.vcthreshold_abs_range is not None:
                    config.vcthreshold_abs_range = input.vcthreshold_abs_range
                if input.intthresholdmin is not None:
                    config.intthresholdmin = input.intthresholdmin
                if input.intthresholdmax is not None:
                    config.intthresholdmax = input.intthresholdmax
                if input.intprecision is not None:
                    config.intprecision = input.intprecision
                
                config.intupdatedby = user_id
                config.dtupdatedat = datetime.now()
                
                session.commit()
                session.refresh(config)
                
                return UpdateRatioConfigurationResultType(
                    success=True,
                    message="Ratio configuration updated successfully",
                    ratioconfiguration=_ratio_configuration_to_graphql(config, session)
                )
            except Exception as e:
                session.rollback()
                raise e
            finally:
                session.close()
                
        except Exception as e:
            logger.error(f"Error updating ratio configuration: {str(e)}")
            return UpdateRatioConfigurationResultType(
                success=False,
                message=f"Failed to update ratio configuration: {str(e)}",
                ratioconfiguration=None
            )
    
    @strawberry.mutation
    def delete_ratio_configuration(
        self,
        info: Info,
        intratioconfigurationid: int
    ) -> DeleteRatioConfigurationResultType:
        """
        Delete a ratio configuration
        Args:
            intratioconfigurationid: Ratio configuration ID to delete
        """
        require_authentication(info)
        
        try:
            db_manager = get_database_manager()
            session = db_manager.get_session()
            
            try:
                # Get existing configuration
                config = session.query(RatioConfiguration).filter(
                    RatioConfiguration.intratioconfigurationid == intratioconfigurationid
                ).first()
                
                if not config:
                    return DeleteRatioConfigurationResultType(
                        success=False,
                        message=get_error_message('CONFIGURATION_NOT_FOUND')
                    )
                
                session.delete(config)
                session.commit()
                
                return DeleteRatioConfigurationResultType(
                    success=True,
                    message=f"Ratio configuration {intratioconfigurationid} deleted successfully"
                )
            except Exception as e:
                session.rollback()
                raise e
            finally:
                session.close()
                
        except Exception as e:
            logger.error(f"Error deleting ratio configuration: {str(e)}")
            return DeleteRatioConfigurationResultType(
                success=False,
                message=f"Failed to delete ratio configuration: {str(e)}"
            )


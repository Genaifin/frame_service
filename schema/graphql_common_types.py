#!/usr/bin/env python3
"""
Common GraphQL Types - Shared across multiple schemas
Avoids duplication and circular dependency issues
"""

import strawberry
import re
from typing import Optional


@strawberry.type
class PaginationInfoType:
    """Pagination information - shared across multiple schemas"""
    pageNumber: int
    pageSize: int
    currentPage: int
    totalPages: int
    totalCount: int


# ==================== Helper Functions for Duplicate Prevention ====================

def normalize_name(name: Optional[str]) -> str:
    """
    Normalize a name by converting to lowercase and removing special characters
    Keeps only alphanumeric characters
    
    Args:
        name: The name to normalize
    
    Returns:
        Normalized name (lowercase, alphanumeric only)
    """
    if not name:
        return ""
    # Convert to lowercase
    normalized = name.lower()
    # Remove all special characters, keep only alphanumeric
    normalized = re.sub(r'[^a-z0-9]', '', normalized)
    return normalized

def check_validation_name_duplicate(
    session,
    vcvalidationname: Optional[str],
    exclude_validation_id: Optional[int] = None
) -> bool:
    """
    Check if a validation name (normalized) already exists
    
    Args:
        session: Database session
        vcvalidationname: Validation name to check
        exclude_validation_id: Optional validation ID to exclude from check (for updates)
    
    Returns:
        True if duplicate exists, False otherwise
    """
    if not vcvalidationname:
        return False
    
    normalized_name = normalize_name(vcvalidationname)
    if not normalized_name:
        return False
    
    # Import here to avoid circular dependency
    from database_models import ValidationMaster
    
    # Query all validations and check normalized names in Python
    query = session.query(ValidationMaster.intvalidationmasterid, ValidationMaster.vcvalidationname)
    
    # Exclude current validation if updating
    if exclude_validation_id is not None:
        query = query.filter(ValidationMaster.intvalidationmasterid != exclude_validation_id)
    
    all_validations = query.all()
    
    # Check if any existing validation has the same normalized name
    for validation_id, existing_name in all_validations:
        if existing_name and normalize_name(existing_name) == normalized_name:
            return True
    
    return False

def check_ratio_name_duplicate(
    session,
    vcrationame: Optional[str],
    exclude_ratio_id: Optional[int] = None
) -> bool:
    """
    Check if a ratio name (normalized) already exists
    
    Args:
        session: Database session
        vcrationame: Ratio name to check
        exclude_ratio_id: Optional ratio ID to exclude from check (for updates)
    
    Returns:
        True if duplicate exists, False otherwise
    """
    if not vcrationame:
        return False
    
    normalized_name = normalize_name(vcrationame)
    if not normalized_name:
        return False
    
    # Import here to avoid circular dependency
    from database_models import RatioMaster
    
    # Query all ratios and check normalized names in Python
    query = session.query(RatioMaster.intratiomasterid, RatioMaster.vcrationame)
    
    # Exclude current ratio if updating
    if exclude_ratio_id is not None:
        query = query.filter(RatioMaster.intratiomasterid != exclude_ratio_id)
    
    all_ratios = query.all()
    
    # Check if any existing ratio has the same normalized name
    for ratio_id, existing_name in all_ratios:
        if existing_name and normalize_name(existing_name) == normalized_name:
            return True
    
    return False

def check_data_model_name_duplicate(
    session,
    vcmodelname: Optional[str],
    exclude_datamodel_id: Optional[int] = None
) -> bool:
    """
    Check if a data model name (normalized) already exists
    
    Args:
        session: Database session
        vcmodelname: Data model name to check
        exclude_datamodel_id: Optional data model ID to exclude from check (for updates)
    
    Returns:
        True if duplicate exists, False otherwise
    """
    if not vcmodelname:
        return False
    
    normalized_name = normalize_name(vcmodelname)
    if not normalized_name:
        return False
    
    # Import here to avoid circular dependency
    from database_models import DataModelMaster
    
    # Query all data models and check normalized names in Python
    query = session.query(DataModelMaster.intdatamodelid, DataModelMaster.vcmodelname)
    
    # Exclude current data model if updating
    if exclude_datamodel_id is not None:
        query = query.filter(DataModelMaster.intdatamodelid != exclude_datamodel_id)
    
    all_data_models = query.all()
    
    # Check if any existing data model has the same normalized name
    for datamodel_id, existing_name in all_data_models:
        if existing_name and normalize_name(existing_name) == normalized_name:
            return True
    
    return False


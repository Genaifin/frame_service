#!/usr/bin/env python3
"""
KPI Management API Utilities
Provides CRUD operations for KPI library and thresholds
"""

from fastapi import HTTPException, status
from fastapi.responses import JSONResponse
from typing import List, Dict, Any, Optional
from datetime import datetime
import logging
from decimal import Decimal
from sqlalchemy.sql import func

# Import database models
from database_models import get_database_manager, KpiLibrary, KpiThreshold

logger = logging.getLogger(__name__)

class KpiManagementService:
    """Service class for KPI management operations"""
    
    def __init__(self):
        self.db_manager = get_database_manager()
    
    async def get_all_kpis(
        self, 
        search: Optional[str] = None,
        kpi_type: Optional[str] = None,
        category: Optional[str] = None,
        is_active: Optional[bool] = True,
        page: int = 1,
        page_size: int = 20
    ) -> Dict[str, Any]:
        """Get all KPIs with pagination, search, and filtering"""
        try:
            session = self.db_manager.get_session()
            
            # Build base query
            query = session.query(KpiLibrary)
            
            # Apply filters
            if is_active is not None:
                query = query.filter(KpiLibrary.is_active == is_active)
            
            if kpi_type:
                query = query.filter(KpiLibrary.kpi_type == kpi_type)
            
            if category:
                query = query.filter(KpiLibrary.category.ilike(f"%{category}%"))
            
            # Apply search filter
            if search:
                search_term = f"%{search}%"
                query = query.filter(
                    (KpiLibrary.kpi_code.ilike(search_term)) |
                    (KpiLibrary.kpi_name.ilike(search_term)) |
                    (KpiLibrary.description.ilike(search_term))
                )
            
            # Get total count for pagination
            total_count = query.count()
            
            # Apply pagination
            offset = (page - 1) * page_size
            kpis = query.offset(offset).limit(page_size).all()
            
            # Convert to list with threshold information
            kpi_list = []
            for kpi in kpis:
                kpi_data = kpi.to_dict()
                
                # Get active thresholds for this KPI
                thresholds = session.query(KpiThreshold).filter(
                    KpiThreshold.kpi_id == kpi.id,
                    KpiThreshold.is_active == True
                ).all()
                
                kpi_data['thresholds'] = [
                    {
                        'id': t.id,
                        'fund_id': t.fund_id,
                        'threshold_value': float(t.threshold_value),
                        'is_default': t.fund_id is None
                    }
                    for t in thresholds
                ]
                
                kpi_list.append(kpi_data)
            
            # Calculate pagination info
            total_pages = (total_count + page_size - 1) // page_size
            start_record = offset + 1
            end_record = min(offset + page_size, total_count)
            
            return {
                'success': True,
                'data': kpi_list,
                'pagination': {
                    'current_page': page,
                    'page_size': page_size,
                    'total_count': total_count,
                    'total_pages': total_pages,
                    'start_record': start_record,
                    'end_record': end_record
                }
            }
            
        except Exception as e:
            logger.error(f"Error getting KPIs: {e}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to retrieve KPIs: {str(e)}"
            )
        finally:
            session.close()
    
    async def get_kpi_by_id(self, kpi_id: int) -> Dict[str, Any]:
        """Get a specific KPI by ID with full details including thresholds"""
        try:
            session = self.db_manager.get_session()
            
            kpi = session.query(KpiLibrary).filter(KpiLibrary.id == kpi_id).first()
            if not kpi:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail="KPI not found"
                )
            
            kpi_data = kpi.to_dict()
            
            # Get all thresholds for this KPI
            thresholds = session.query(KpiThreshold).filter(
                KpiThreshold.kpi_id == kpi.id
            ).all()
            
            kpi_data['thresholds'] = [t.to_dict() for t in thresholds]
            
            return {
                'success': True,
                'data': kpi_data
            }
            
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error getting KPI {kpi_id}: {e}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to retrieve KPI: {str(e)}"
            )
        finally:
            session.close()
    
    async def create_kpi(self, kpi_data: Dict[str, Any], current_user: str = "system") -> Dict[str, Any]:
        """Create a new KPI"""
        try:
            session = self.db_manager.get_session()
            
            # Validate required fields
            required_fields = ['kpi_code', 'kpi_name', 'kpi_type', 'source_type', 'precision_type']
            for field in required_fields:
                if field not in kpi_data or not kpi_data[field]:
                    raise HTTPException(
                        status_code=status.HTTP_400_BAD_REQUEST,
                        detail=f"Missing required field: {field}"
                    )
            
            # Validate enum values
            valid_kpi_types = ['NAV_VALIDATION', 'RATIO_VALIDATION']
            valid_source_types = ['SINGLE_SOURCE', 'DUAL_SOURCE']
            valid_precision_types = ['PERCENTAGE', 'ABSOLUTE']
            
            if kpi_data['kpi_type'] not in valid_kpi_types:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"Invalid kpi_type. Must be one of: {valid_kpi_types}"
                )
            
            if kpi_data['source_type'] not in valid_source_types:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"Invalid source_type. Must be one of: {valid_source_types}"
                )
            
            if kpi_data['precision_type'] not in valid_precision_types:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"Invalid precision_type. Must be one of: {valid_precision_types}"
                )
            
            # Check if KPI code already exists
            existing_kpi = session.query(KpiLibrary).filter(
                KpiLibrary.kpi_code == kpi_data['kpi_code']
            ).first()
            if existing_kpi:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="KPI code already exists"
                )
            
            # Create new KPI
            new_kpi = KpiLibrary(
                kpi_code=kpi_data['kpi_code'],
                kpi_name=kpi_data['kpi_name'],
                kpi_type=kpi_data['kpi_type'],
                category=kpi_data.get('category'),
                description=kpi_data.get('description'),
                source_type=kpi_data['source_type'],
                precision_type=kpi_data['precision_type'],
                numerator_field=kpi_data.get('numerator_field'),
                denominator_field=kpi_data.get('denominator_field'),
                numerator_description=kpi_data.get('numerator_description'),
                denominator_description=kpi_data.get('denominator_description'),
                is_active=kpi_data.get('is_active', True),
                created_by=current_user,
                updated_by=current_user
            )
            
            session.add(new_kpi)
            session.commit()
            session.refresh(new_kpi)
            
            return {
                'success': True,
                'message': 'KPI created successfully',
                'data': new_kpi.to_dict()
            }
            
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error creating KPI: {e}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to create KPI: {str(e)}"
            )
        finally:
            session.close()
    
    async def update_kpi(self, kpi_id: int, kpi_data: Dict[str, Any], current_user: str = "system") -> Dict[str, Any]:
        """Update an existing KPI"""
        try:
            session = self.db_manager.get_session()
            
            # Get existing KPI
            kpi = session.query(KpiLibrary).filter(KpiLibrary.id == kpi_id).first()
            if not kpi:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail="KPI not found"
                )
            
            # Check if KPI code is being changed and if it already exists
            if 'kpi_code' in kpi_data and kpi_data['kpi_code'] != kpi.kpi_code:
                existing_kpi = session.query(KpiLibrary).filter(
                    KpiLibrary.kpi_code == kpi_data['kpi_code']
                ).first()
                if existing_kpi:
                    raise HTTPException(
                        status_code=status.HTTP_400_BAD_REQUEST,
                        detail="KPI code already exists"
                    )
            
            # Validate enum values if provided
            if 'kpi_type' in kpi_data:
                valid_kpi_types = ['NAV_VALIDATION', 'RATIO_VALIDATION']
                if kpi_data['kpi_type'] not in valid_kpi_types:
                    raise HTTPException(
                        status_code=status.HTTP_400_BAD_REQUEST,
                        detail=f"Invalid kpi_type. Must be one of: {valid_kpi_types}"
                    )
            
            if 'source_type' in kpi_data:
                valid_source_types = ['SINGLE_SOURCE', 'DUAL_SOURCE']
                if kpi_data['source_type'] not in valid_source_types:
                    raise HTTPException(
                        status_code=status.HTTP_400_BAD_REQUEST,
                        detail=f"Invalid source_type. Must be one of: {valid_source_types}"
                    )
            
            if 'precision_type' in kpi_data:
                valid_precision_types = ['PERCENTAGE', 'ABSOLUTE']
                if kpi_data['precision_type'] not in valid_precision_types:
                    raise HTTPException(
                        status_code=status.HTTP_400_BAD_REQUEST,
                        detail=f"Invalid precision_type. Must be one of: {valid_precision_types}"
                    )
            
            # Check if is_active is being changed to inactive
            if 'is_active' in kpi_data and not kpi_data['is_active'] and kpi.is_active:
                # KPI is being set to inactive, set all its thresholds to inactive too
                session.query(KpiThreshold).filter(
                    KpiThreshold.kpi_id == kpi.id,
                    KpiThreshold.is_active == True
                ).update({
                    'is_active': False,
                    'updated_by': current_user
                })
            
            # Update KPI fields
            updateable_fields = [
                'kpi_code', 'kpi_name', 'kpi_type', 'category', 'description',
                'source_type', 'precision_type', 'numerator_field',
                'denominator_field', 'numerator_description', 'denominator_description',
                'is_active'
            ]
            
            for field in updateable_fields:
                if field in kpi_data:
                    setattr(kpi, field, kpi_data[field])
            
            # Set updated_by to current user
            kpi.updated_by = current_user
            # The updated_at timestamp will be automatically updated by SQLAlchemy
            
            session.commit()
            
            return {
                'success': True,
                'message': 'KPI updated successfully',
                'data': kpi.to_dict()
            }
            
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error updating KPI {kpi_id}: {e}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to update KPI: {str(e)}"
            )
        finally:
            session.close()
    
    async def delete_kpi(self, kpi_id: int, current_user: str = "system") -> Dict[str, Any]:
        """Delete a KPI (soft delete by setting is_active to False)"""
        try:
            session = self.db_manager.get_session()
            
            kpi = session.query(KpiLibrary).filter(KpiLibrary.id == kpi_id).first()
            if not kpi:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail="KPI not found"
                )
            
            # Soft delete - set is_active to False
            kpi.is_active = False
            kpi.updated_by = current_user
            # The updated_at timestamp will be automatically updated by SQLAlchemy
            
            # Also deactivate associated thresholds using bulk update
            session.query(KpiThreshold).filter(
                KpiThreshold.kpi_id == kpi_id,
                KpiThreshold.is_active == True
            ).update({
                'is_active': False,
                'updated_by': current_user
            })
            
            session.commit()
            
            return {
                'success': True,
                'message': 'KPI deleted successfully'
            }
            
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error deleting KPI {kpi_id}: {e}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to delete KPI: {str(e)}"
            )
        finally:
            session.close()
    
    async def get_kpi_thresholds(self, kpi_id: int) -> Dict[str, Any]:
        """Get all thresholds for a specific KPI"""
        try:
            session = self.db_manager.get_session()
            
            # Verify KPI exists
            kpi = session.query(KpiLibrary).filter(KpiLibrary.id == kpi_id).first()
            if not kpi:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail="KPI not found"
                )
            
            # Get thresholds
            thresholds = session.query(KpiThreshold).filter(
                KpiThreshold.kpi_id == kpi_id,
                KpiThreshold.is_active == True
            ).all()
            
            threshold_list = [t.to_dict() for t in thresholds]
            
            return {
                'success': True,
                'data': {
                    'kpi': kpi.to_dict(),
                    'thresholds': threshold_list
                }
            }
            
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error getting thresholds for KPI {kpi_id}: {e}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to retrieve thresholds: {str(e)}"
            )
        finally:
            session.close()
    
    async def create_threshold(self, threshold_data: Dict[str, Any], current_user: str = "system") -> Dict[str, Any]:
        """Create a new threshold for a KPI"""
        try:
            session = self.db_manager.get_session()
            
            # Validate required fields
            required_fields = ['kpi_id', 'threshold_value']
            for field in required_fields:
                if field not in threshold_data:
                    raise HTTPException(
                        status_code=status.HTTP_400_BAD_REQUEST,
                        detail=f"Missing required field: {field}"
                    )
            
            # Verify KPI exists
            kpi = session.query(KpiLibrary).filter(
                KpiLibrary.id == threshold_data['kpi_id']
            ).first()
            if not kpi:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail="KPI not found"
                )
            
            # Check for existing active threshold with same KPI/fund combination
            existing_threshold = session.query(KpiThreshold).filter(
                KpiThreshold.kpi_id == threshold_data['kpi_id'],
                KpiThreshold.fund_id == threshold_data.get('fund_id'),
                KpiThreshold.is_active == True
            ).first()
            
            if existing_threshold:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="Active threshold already exists for this KPI/fund combination"
                )
            
            # Create new threshold
            new_threshold = KpiThreshold(
                kpi_id=threshold_data['kpi_id'],
                fund_id=threshold_data.get('fund_id'),
                threshold_value=Decimal(str(threshold_data['threshold_value'])),
                is_active=threshold_data.get('is_active', True),
                created_by=current_user,
                updated_by=current_user
            )
            
            session.add(new_threshold)
            session.commit()
            session.refresh(new_threshold)
            
            return {
                'success': True,
                'message': 'Threshold created successfully',
                'data': new_threshold.to_dict()
            }
            
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error creating threshold: {e}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to create threshold: {str(e)}"
            )
        finally:
            session.close()
    
    async def update_threshold(self, threshold_id: int, threshold_data: Dict[str, Any], current_user: str = "system") -> Dict[str, Any]:
        """Update an existing threshold"""
        try:
            session = self.db_manager.get_session()
            
            threshold = session.query(KpiThreshold).filter(
                KpiThreshold.id == threshold_id
            ).first()
            if not threshold:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail="Threshold not found"
                )
            
            # Update threshold fields
            updateable_fields = ['threshold_value', 'fund_id', 'is_active']
            
            for field in updateable_fields:
                if field in threshold_data:
                    if field == 'threshold_value':
                        setattr(threshold, field, Decimal(str(threshold_data[field])))
                    else:
                        setattr(threshold, field, threshold_data[field])
            
            # Set updated_by to current user
            threshold.updated_by = current_user
            # The updated_at timestamp will be automatically updated by SQLAlchemy
            
            session.commit()
            
            return {
                'success': True,
                'message': 'Threshold updated successfully',
                'data': threshold.to_dict()
            }
            
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error updating threshold {threshold_id}: {e}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to update threshold: {str(e)}"
            )
        finally:
            session.close()
    
    async def delete_threshold(self, threshold_id: int, current_user: str = "system") -> Dict[str, Any]:
        """Delete a threshold (soft delete)"""
        try:
            session = self.db_manager.get_session()
            
            threshold = session.query(KpiThreshold).filter(
                KpiThreshold.id == threshold_id
            ).first()
            if not threshold:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail="Threshold not found"
                )
            
            # Soft delete
            threshold.is_active = False
            threshold.updated_by = current_user
            # The updated_at timestamp will be automatically updated by SQLAlchemy
            
            session.commit()
            
            return {
                'success': True,
                'message': 'Threshold deleted successfully'
            }
            
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error deleting threshold {threshold_id}: {e}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to delete threshold: {str(e)}"
            )
        finally:
            session.close()
    
    async def get_kpi_categories(self) -> Dict[str, Any]:
        """Get all unique KPI categories"""
        try:
            session = self.db_manager.get_session()
            
            categories = session.query(KpiLibrary.category).filter(
                KpiLibrary.category.isnot(None),
                KpiLibrary.is_active == True
            ).distinct().all()
            
            category_list = [cat[0] for cat in categories if cat[0]]
            
            return {
                'success': True,
                'data': sorted(category_list)
            }
            
        except Exception as e:
            logger.error(f"Error getting KPI categories: {e}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to retrieve categories: {str(e)}"
            )
        finally:
            session.close()
    
    async def bulk_update_kpis_and_thresholds(self, request: dict, current_user: str) -> Dict[str, Any]:
        """Bulk update/create KPIs and thresholds with code/ID lookup"""
        session = self.db_manager.get_session()
        try:
            created_kpis = 0
            updated_kpis = 0
            created_thresholds = 0 
            updated_thresholds = 0
            deleted_kpis = 0
            errors = []
            
            # Process KPIs - find by code or ID, update if found, create if not
            kpis_data = request.get('kpis', [])
            for kpi_data in kpis_data:
                try:
                    kpi = None
                    kpi_code = kpi_data.get('kpi_code')
                    kpi_id = kpi_data.get('kpi_id')
                    
                    # Find existing KPI by code or ID
                    if kpi_code:
                        kpi = session.query(KpiLibrary).filter(KpiLibrary.kpi_code == kpi_code).first()
                    elif kpi_id:
                        kpi = session.query(KpiLibrary).filter(KpiLibrary.id == kpi_id).first()
                    
                    if kpi:
                        # Update existing KPI
                        updateable_fields = [
                            'kpi_name', 'kpi_type', 'category', 'description', 'source_type',
                            'precision_type', 'numerator_field', 'denominator_field',
                            'numerator_description', 'denominator_description', 'is_active'
                        ]
                        
                        # Check if is_active is being changed to inactive
                        if 'is_active' in kpi_data and not kpi_data['is_active'] and kpi.is_active:
                            # KPI is being set to inactive, set all its thresholds to inactive too
                            session.query(KpiThreshold).filter(
                                KpiThreshold.kpi_id == kpi.id,
                                KpiThreshold.is_active == True
                            ).update({
                                'is_active': False,
                                'updated_by': current_user
                            })
                        
                        for field in updateable_fields:
                            if field in kpi_data:
                                setattr(kpi, field, kpi_data[field])
                        
                        kpi.updated_by = current_user
                        updated_kpis += 1
                        
                    else:
                        # Create new KPI
                        new_kpi = KpiLibrary(
                            kpi_code=kpi_code or f"auto_{len(kpis_data)}_{current_user}",
                            kpi_name=kpi_data['kpi_name'],
                            kpi_type=kpi_data['kpi_type'],
                            category=kpi_data.get('category'),
                            description=kpi_data.get('description'),
                            source_type=kpi_data['source_type'],
                            precision_type=kpi_data['precision_type'],
                            numerator_field=kpi_data.get('numerator_field'),
                            denominator_field=kpi_data.get('denominator_field'),
                            numerator_description=kpi_data.get('numerator_description'),
                            denominator_description=kpi_data.get('denominator_description'),
                            is_active=kpi_data.get('is_active', True),
                            created_by=current_user,
                            updated_by=current_user
                        )
                        session.add(new_kpi)
                        session.flush()  # Get the ID
                        created_kpis += 1
                        
                except Exception as e:
                    errors.append(f"KPI {kpi_code or kpi_id}: {str(e)}")
            
            # Process Thresholds - find KPI first, then update/create threshold
            thresholds_data = request.get('thresholds', [])
            for threshold_data in thresholds_data:
                try:
                    # Find the KPI for this threshold
                    target_kpi = None
                    kpi_code = threshold_data.get('kpi_code')
                    kpi_id = threshold_data.get('kpi_id')
                    
                    if kpi_code:
                        target_kpi = session.query(KpiLibrary).filter(KpiLibrary.kpi_code == kpi_code).first()
                    elif kpi_id:
                        target_kpi = session.query(KpiLibrary).filter(KpiLibrary.id == kpi_id).first()
                    
                    if not target_kpi:
                        errors.append(f"Threshold: KPI not found with code/ID {kpi_code or kpi_id}")
                        continue
                    
                    # Check if threshold already exists
                    threshold_id = threshold_data.get('threshold_id')
                    threshold = None
                    
                    if threshold_id:
                        threshold = session.query(KpiThreshold).filter(KpiThreshold.id == threshold_id).first()
                    else:
                        # Look for existing threshold by KPI and fund
                        fund_id = threshold_data.get('fund_id')
                        threshold = session.query(KpiThreshold).filter(
                            KpiThreshold.kpi_id == target_kpi.id,
                            KpiThreshold.fund_id == fund_id,
                            KpiThreshold.is_active == True
                        ).first()
                    
                    if threshold:
                        # Update existing threshold
                        threshold.threshold_value = threshold_data['threshold_value']
                        threshold.is_active = threshold_data.get('is_active', True)
                        threshold.updated_by = current_user
                        updated_thresholds += 1
                    else:
                        # Create new threshold
                        new_threshold = KpiThreshold(
                            kpi_id=target_kpi.id,
                            fund_id=threshold_data.get('fund_id'),
                            threshold_value=threshold_data['threshold_value'],
                            is_active=threshold_data.get('is_active', True),
                            created_by=current_user,
                            updated_by=current_user
                        )
                        session.add(new_threshold)
                        created_thresholds += 1
                        
                except Exception as e:
                    errors.append(f"Threshold {threshold_data.get('threshold_id', 'new')}: {str(e)}")
            
            # Delete KPIs by code (set inactive)
            delete_kpi_codes = request.get('delete_kpi_codes', [])
            for kpi_code in delete_kpi_codes:
                try:
                    kpi = session.query(KpiLibrary).filter(KpiLibrary.kpi_code == kpi_code).first()
                    if kpi:
                        # Set KPI to inactive
                        kpi.is_active = False
                        kpi.updated_by = current_user
                        
                        # Also set all its thresholds to inactive
                        session.query(KpiThreshold).filter(
                            KpiThreshold.kpi_id == kpi.id,
                            KpiThreshold.is_active == True
                        ).update({
                            'is_active': False,
                            'updated_by': current_user
                        })
                        
                        deleted_kpis += 1
                    else:
                        errors.append(f"Delete: KPI not found with code {kpi_code}")
                except Exception as e:
                    errors.append(f"Delete KPI {kpi_code}: {str(e)}")
            
            session.commit()
            
            return {
                "success": True,
                "message": "Bulk operation completed successfully",
                "created_kpis": created_kpis,
                "updated_kpis": updated_kpis,
                "created_thresholds": created_thresholds,
                "updated_thresholds": updated_thresholds,
                "deleted_kpis": deleted_kpis,
                "errors": errors
            }
            
        except Exception as e:
            session.rollback()
            logger.error(f"Bulk operation failed: {e}")
            raise HTTPException(status_code=500, detail=str(e))
        finally:
            session.close()
    
    async def submit_kpi_configuration(self, request: dict, current_user: str) -> Dict[str, Any]:
        """Handle UI submit operation for KPI configuration (requirement #5)"""
        session = self.db_manager.get_session()
        try:
            # Deactivate all KPIs first
            session.query(KpiLibrary).update({
                "is_active": False,
                "updated_by": current_user,
                "updated_at": func.now()
            })
            
            # Activate selected KPIs
            selected_kpis = request.get('selected_kpis', [])
            if selected_kpis:
                session.query(KpiLibrary).filter(
                    KpiLibrary.id.in_(selected_kpis)
                ).update({
                    "is_active": True,
                    "updated_by": current_user,
                    "updated_at": func.now()
                }, synchronize_session=False)
            
            # Apply KPI updates
            kpi_updates = request.get('kpi_updates', [])
            for kpi_update in kpi_updates:
                kpi_id = kpi_update.get('id')
                if kpi_id:
                    kpi = session.query(KpiLibrary).filter(KpiLibrary.id == kpi_id).first()
                    if kpi:
                        for key, value in kpi_update.items():
                            if hasattr(kpi, key) and key not in ['id', 'created_at', 'created_by']:
                                setattr(kpi, key, value)
                        kpi.updated_by = current_user
                        kpi.updated_at = func.now()
            
            # Apply threshold updates
            threshold_updates = request.get('threshold_updates', [])
            for threshold_update in threshold_updates:
                threshold_id = threshold_update.get('id')
                if threshold_id:
                    threshold = session.query(KpiThreshold).filter(KpiThreshold.id == threshold_id).first()
                    if threshold:
                        for key, value in threshold_update.items():
                            if hasattr(threshold, key) and key not in ['id', 'created_at', 'created_by']:
                                setattr(threshold, key, value)
                        threshold.updated_by = current_user
                        threshold.updated_at = func.now()
            
            # Create new thresholds
            new_thresholds = request.get('new_thresholds', [])
            for new_threshold in new_thresholds:
                threshold = KpiThreshold(
                    **new_threshold,
                    created_by=current_user,
                    updated_by=current_user
                )
                session.add(threshold)
            
            session.commit()
            
            return {
                "success": True,
                "message": "KPI configuration submitted successfully",
                "active_kpis": len(selected_kpis),
                "updated_kpis": len(kpi_updates),
                "updated_thresholds": len(threshold_updates),
                "new_thresholds": len(new_thresholds)
            }
            
        except Exception as e:
            session.rollback()
            logger.error(f"Submit configuration failed: {e}")
            raise HTTPException(status_code=500, detail=str(e))
        finally:
            session.close()
    
    async def get_kpis_for_ui(self) -> Dict[str, Any]:
        """Get all KPIs formatted for UI selection"""
        try:
            session = self.db_manager.get_session()
            
            kpis = session.query(KpiLibrary).all()
            
            kpi_list = []
            for kpi in kpis:
                kpi_data = kpi.to_dict()
                # Get thresholds
                thresholds = session.query(KpiThreshold).filter(
                    KpiThreshold.kpi_id == kpi.id
                ).all()
                kpi_data['thresholds'] = [t.to_dict() for t in thresholds]
                kpi_list.append(kpi_data)
            
            return {
                'success': True,
                'data': kpi_list
            }
            
        except Exception as e:
            logger.error(f"Error getting KPIs for UI: {e}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to retrieve KPIs: {str(e)}"
            )
        finally:
            session.close()
        
# Create service instance
kpi_service = KpiManagementService()

# API Response functions
async def get_kpis_response(
    search: Optional[str] = None,
    kpi_type: Optional[str] = None,
    category: Optional[str] = None,
    is_active: Optional[bool] = True,
    page: int = 1,
    page_size: int = 20
) -> JSONResponse:
    """Get all KPIs with pagination and filtering"""
    result = await kpi_service.get_all_kpis(search, kpi_type, category, is_active, page, page_size)
    return JSONResponse(content=result)

async def get_kpi_response(kpi_id: int) -> JSONResponse:
    """Get a specific KPI by ID"""
    result = await kpi_service.get_kpi_by_id(kpi_id)
    return JSONResponse(content=result)

async def create_kpi_response(kpi_data: Dict[str, Any], current_user: str = "api") -> JSONResponse:
    """Create a new KPI"""
    result = await kpi_service.create_kpi(kpi_data, current_user)
    return JSONResponse(content=result, status_code=201)

async def update_kpi_response(kpi_id: int, kpi_data: Dict[str, Any], current_user: str = "api") -> JSONResponse:
    """Update an existing KPI"""
    result = await kpi_service.update_kpi(kpi_id, kpi_data, current_user)
    return JSONResponse(content=result)

async def delete_kpi_response(kpi_id: int, current_user: str = "api") -> JSONResponse:
    """Delete a KPI"""
    result = await kpi_service.delete_kpi(kpi_id, current_user)
    return JSONResponse(content=result)

async def get_kpi_thresholds_response(kpi_id: int) -> JSONResponse:
    """Get thresholds for a KPI"""
    result = await kpi_service.get_kpi_thresholds(kpi_id)
    return JSONResponse(content=result)

async def create_threshold_response(threshold_data: Dict[str, Any], current_user: str = "api") -> JSONResponse:
    """Create a new threshold"""
    result = await kpi_service.create_threshold(threshold_data, current_user)
    return JSONResponse(content=result, status_code=201)

async def update_threshold_response(threshold_id: int, threshold_data: Dict[str, Any], current_user: str = "api") -> JSONResponse:
    """Update an existing threshold"""
    result = await kpi_service.update_threshold(threshold_id, threshold_data, current_user)
    return JSONResponse(content=result)

async def delete_threshold_response(threshold_id: int, current_user: str = "api") -> JSONResponse:
    """Delete a threshold"""
    result = await kpi_service.delete_threshold(threshold_id, current_user)
    return JSONResponse(content=result)

async def get_categories_response() -> JSONResponse:
    """Get all KPI categories"""
    result = await kpi_service.get_kpi_categories()
    return JSONResponse(content=result)


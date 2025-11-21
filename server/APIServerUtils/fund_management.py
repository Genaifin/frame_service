#!/usr/bin/env python3
"""
Fund Management API Utilities
Provides CRUD operations for funds based on the dashboard requirements
"""

from fastapi import HTTPException, status
from fastapi.responses import JSONResponse
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta
import logging
import sys
import time
from pathlib import Path

# Ensure project root is on sys.path for Docker/runtime compatibility
_project_root = Path(__file__).resolve().parents[2]
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

# Import database models
from database_models import get_database_manager, Fund, FundManager
from sqlalchemy import desc

logger = logging.getLogger(__name__)

class FundManagementService:
    """Service class for fund management operations"""
    
    def __init__(self):
        self.db_manager = get_database_manager()
    
    async def get_funds_dashboard(
        self, 
        search: Optional[str] = None,
        status_filter: Optional[str] = None,
        page: int = 1,
        page_size: int = 10
    ) -> Dict[str, Any]:
        """Get funds dashboard with the specified JSON structure"""
        try:
            # Use proper session management with error handling
            session = self.db_manager.get_session()
            try:
                # Build query
                query = session.query(Fund)
                
                # Apply search filter
                if search:
                    search_term = f"%{search}%"
                    query = query.filter(
                        (Fund.name.ilike(search_term)) | 
                        (Fund.code.ilike(search_term))
                    )
                
                # Apply status filter
                if status_filter:
                    if status_filter.lower() == 'active':
                        query = query.filter(Fund.is_active == True)
                    elif status_filter.lower() == 'inactive':
                        query = query.filter(Fund.is_active == False)
                
                # Get total count
                total_count = query.count()
                
                # Apply pagination
                offset = (page - 1) * page_size
                funds = query.offset(offset).limit(page_size).all()
                
                # Convert to response format
                row_data = []
                for fund in funds:
                    # Format date
                    created_date = fund.created_at.strftime("%m/%d/%Y") if fund.created_at else "N/A"
                    
                    row_data.append({
                        "fundManagerID": fund.code,
                        "fundManagerName": fund.name,
                        "contactName": "N/A",  # Will be populated from related data if available
                        "createdDate": created_date,
                        "status": "Active" if fund.is_active else "Inactive"
                    })
                
                # Calculate pagination
                total_pages = (total_count + page_size - 1) // page_size
                
                # Build the dashboard response
                dashboard_response = {
                    "topNavBarParams": [
                        {
                            "moduleName": "breadcrumb",
                            "isShow": True,
                            "data": {
                                "title": "Fund Managers",
                                "breadcrumb": [
                                    {
                                        "name": "SETUP"
                                    }
                                ],
                                "isShowBackButton": True
                            }
                        },
                        {
                            "moduleName": "notificationIcon",
                            "isShow": True
                        }
                    ],
                    "moduleDisplayConfig": [
                        {
                            "layout": {
                                "x": 0,
                                "y": 10,
                                "w": 12,
                                "h": 58
                            },
                            "id": 0,
                            "overridenModuleMeta": {
                                "moduleType": "subPage",
                                "modules": [
                                    {
                                        "width": "66%",
                                        "overridenModuleMeta": {
                                            "name": "ClientMastersTableHeader",
                                            "moduleType": "textHeader",
                                            "header": "FUND MANAGERS",
                                            "cssProperties": {
                                                "fontSize": "16px",
                                                "fontWeight": "600",
                                                "textTransform": "uppercase",
                                                "color": "#4b5563"
                                            }
                                        }
                                    },
                                    {
                                        "width": "20%",
                                        "overridenModuleMeta": {
                                            "name": "SearchModule",
                                            "moduleType": "searchModule",
                                            "placeholder": "Search",
                                            "cssProperties": {
                                                "justifyContent": "flex-end",
                                                "height": "32px"
                                            }
                                        }
                                    },
                                    {
                                        "width": "14%",
                                        "overridenModuleMeta": {
                                            "name": "dropdownModule",
                                            "moduleType": "dropdown",
                                            "data": {
                                                "trigger": {
                                                    "label": "New Fund Manager",
                                                    "cssProperties": {
                                                        "backgroundColor": "#E65410",
                                                        "color": "#ffffff",
                                                        "fontSize": "14px",
                                                        "fontWeight": "500",
                                                        "padding": "12px",
                                                        "borderRadius": "8px",
                                                        "height": "32px"
                                                    }
                                                },
                                                "menu": {
                                                    "cssProperties": {
                                                        "color": "#1f2937"
                                                    },
                                                    "divider": True,
                                                    "items": [
                                                        {
                                                            "label": "Fill Form",
                                                            "action": {
                                                                "type": "navigation",
                                                                "to": "/frame",
                                                                "parameters": [
                                                                    {
                                                                        "key": "page",
                                                                        "value": "AddFundManager"
                                                                    }
                                                                ]
                                                            }
                                                        },
                                                        {
                                                            "label": "CSV / Excel Upload",
                                                            "action": {
                                                                "type": "storeValues",
                                                                "store": {
                                                                    "key": "openBulkCampaignModal",
                                                                    "value": ""
                                                                }
                                                            }
                                                        }
                                                    ]
                                                },
                                                "cssProperties": {
                                                    "width": "10%",
                                                    "height": "32px",
                                                    "justifyContent": "flex-end"
                                                }
                                            }
                                        }
                                    },
                                    {
                                        "width": "100%",
                                        "height": "92%",
                                        "overridenModuleMeta": {
                                            "name": "ClientMastersTable",
                                            "moduleType": "nestedTable",
                                            "rowClickEnabled": True,
                                            "rowClickAction": {
                                                "type": "storeValues",
                                                "store": {
                                                    "key": "commonRightDrawer",
                                                    "value": {
                                                        "page": "detailedInfo",
                                                        "getAPIURL": f"/api/funds/{{fundManagerID}}",
                                                        "parameters": [
                                                            {
                                                                "name": "fundManagerID",
                                                                "value": "",
                                                                "dynamicValue": {
                                                                    "enabled": True,
                                                                    "id": "fundManagerID"
                                                                }
                                                            }
                                                        ],
                                                        "isOpen": True
                                                    }
                                                }
                                            },
                                            "colsToShow": [
                                                "fundManagerID",
                                                "fundManagerName",
                                                "contactName",
                                                "createdDate",
                                                "status"
                                            ],
                                            "columnConfig": {
                                                "fundManagerID": {
                                                    "name": "Fund Manager ID",
                                                    "filter": "true",
                                                    "suppressHeaderMenuButton": False
                                                },
                                                "fundManagerName": {
                                                    "name": "Fund Manager Name",
                                                    "filter": "true",
                                                    "suppressHeaderMenuButton": False
                                                },
                                                "contactName": {
                                                    "name": "Contact Name",
                                                    "filter": "true",
                                                    "suppressHeaderMenuButton": False
                                                },
                                                "createdDate": {
                                                    "name": "Created Date",
                                                    "filter": "true",
                                                    "suppressHeaderMenuButton": False
                                                },
                                                "status": {
                                                    "name": "Status",
                                                    "filter": "true",
                                                    "customCellRenderer": "statusAggregator",
                                                    "suppressHeaderMenuButton": False
                                                }
                                            },
                                            "rowData": row_data,
                                            "pagination": {
                                                "current_page": page,
                                                "page_size": page_size,
                                                "total_pages": total_pages
                                            }
                                        }
                                    }
                                ],
                                "cssProperties": {
                                    "backgroundColor": "white",
                                    "borderRadius": "24px",
                                    "padding": "24px"
                                }
                            }
                        }
                    ]
                }
                
                return dashboard_response
                
            finally:
                session.close()
                
        except Exception as e:
            logger.error(f"Error getting funds dashboard: {e}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to retrieve funds dashboard: {str(e)}"
            )
    
    async def get_all_funds(
        self, 
        search: Optional[str] = None,
        status_filter: Optional[str] = None,
        page: int = 1,
        page_size: int = 10
    ) -> Dict[str, Any]:
        """Get all funds with pagination, search, and filtering"""
        try:
            session = self.db_manager.get_session()
            try:
                # Build query
                query = session.query(Fund)
                
                # Apply search filter
                if search:
                    search_term = f"%{search}%"
                    query = query.filter(
                        (Fund.name.ilike(search_term)) | 
                        (Fund.code.ilike(search_term))
                    )
                
                # Apply status filter
                if status_filter:
                    if status_filter.lower() == 'active':
                        query = query.filter(Fund.is_active == True)
                    elif status_filter.lower() == 'inactive':
                        query = query.filter(Fund.is_active == False)
                
                # Get total count
                total_count = query.count()
                
                # Apply sorting by created_at in descending order (newest first) and pagination
                offset = (page - 1) * page_size
                funds = query.order_by(desc(Fund.created_at)).offset(offset).limit(page_size).all()
                
                # Convert to response format
                fund_list = [fund.to_dict() for fund in funds]
                
                return {
                    "funds": fund_list,
                    "pagination": {
                        "current_page": page,
                        "page_size": page_size,
                        "total_count": total_count,
                        "total_pages": (total_count + page_size - 1) // page_size
                    }
                }
                
            finally:
                session.close()
                
        except Exception as e:
            logger.error(f"Error getting funds: {e}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to retrieve funds: {str(e)}"
            )
    
    async def get_fund_by_id(self, fund_id: int) -> Dict[str, Any]:
        """Get a specific fund by ID"""
        try:
            session = self.db_manager.get_session()
            try:
                fund = session.query(Fund).filter(Fund.id == fund_id).first()
                
                if not fund:
                    raise HTTPException(
                        status_code=status.HTTP_404_NOT_FOUND,
                        detail="Fund not found"
                    )
                
                return fund.to_dict()
                
            finally:
                session.close()
                
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error getting fund by ID {fund_id}: {e}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to retrieve fund: {str(e)}"
            )
    
    async def get_fund_by_code(self, fund_code: str) -> Dict[str, Any]:
        """Get a specific fund by code"""
        try:
            session = self.db_manager.get_session()
            try:
                fund = session.query(Fund).filter(Fund.code == fund_code).first()
                
                if not fund:
                    raise HTTPException(
                        status_code=status.HTTP_404_NOT_FOUND,
                        detail="Fund not found"
                    )
                
                return fund.to_dict()
                
            finally:
                session.close()
                
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error getting fund by code {fund_code}: {e}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to retrieve fund: {str(e)}"
            )
    
    async def create_fund(self, fund_data: Dict[str, Any]) -> Dict[str, Any]:
        """Create a new fund"""
        try:
            session = self.db_manager.get_session()
            try:
                # Generate fund code from name if not provided
                fund_code = fund_data.get('fund_name', '').replace(' ', '_').upper()[:80]
                if not fund_code:
                    fund_code = f"FUND_{int(time.time())}"
                
                # Check if fund with same code already exists
                existing_fund = session.query(Fund).filter(Fund.code == fund_code).first()
                if existing_fund:
                    raise HTTPException(
                        status_code=status.HTTP_400_BAD_REQUEST,
                        detail="Fund with this code already exists"
                    )
                
                # Check if fund with same name already exists
                existing_fund = session.query(Fund).filter(Fund.name == fund_data['fund_name']).first()
                if existing_fund:
                    raise HTTPException(
                        status_code=status.HTTP_400_BAD_REQUEST,
                        detail="Fund with this name already exists"
                    )
                
                # Prepare contact person name
                contact_person = None
                if fund_data.get('first_name') or fund_data.get('last_name'):
                    parts = []
                    if fund_data.get('title'):
                        parts.append(fund_data['title'])
                    if fund_data.get('first_name'):
                        parts.append(fund_data['first_name'])
                    if fund_data.get('last_name'):
                        parts.append(fund_data['last_name'])
                    contact_person = ' '.join(parts)
                
                # Prepare fund admin data
                fund_admin_data = None
                if fund_data.get('fund_admins'):
                    fund_admin_data = {
                        'admins': [
                            {
                                'id': admin.get('id'),
                                'label': admin.get('label'),
                                'value': admin.get('value'),
                                'isActive': admin.get('isActive', True)
                            }
                            for admin in fund_data['fund_admins']
                        ]
                    }
                
                # Prepare shadow data
                shadow_data = None
                if fund_data.get('shadow_admins'):
                    shadow_data = {
                        'admins': [
                            {
                                'id': admin.get('id'),
                                'label': admin.get('label'),
                                'value': admin.get('value'),
                                'isActive': admin.get('isActive', True)
                            }
                            for admin in fund_data['shadow_admins']
                        ]
                    }
                
                # Prepare strategy data
                strategy_data = None
                if fund_data.get('strategy'):
                    strategy_data = {
                        'strategies': fund_data['strategy']
                    }
                
                # Prepare benchmark data
                benchmark_data = None
                if fund_data.get('benchmarks'):
                    benchmark_data = {
                        'benchmarks': fund_data['benchmarks']
                    }
                
                # Truncate base_currency to fit database column (10 characters max)
                base_currency = fund_data.get('base_currency', '')[:10] if fund_data.get('base_currency') else None
                
                # Create new fund with all the new fields
                new_fund = Fund(
                    name=fund_data['fund_name'],
                    code=fund_code,
                    description=fund_data.get('fund_type'),
                    type=fund_data.get('fund_type'),
                    fund_manager=fund_data.get('fund_manager'),
                    base_currency=base_currency,
                    fund_admin=fund_admin_data,
                    shadow=shadow_data,
                    contact_person=contact_person,
                    contact_email=fund_data.get('email'),
                    contact_number=fund_data.get('contact_number'),
                    sector=fund_data.get('sector'),
                    geography=fund_data.get('geography'),
                    strategy=strategy_data,
                    market_cap=fund_data.get('market_cap'),
                    benchmark=benchmark_data,
                    stage=fund_data.get('stage'),
                    inception_date=fund_data.get('inception_date'),
                    investment_start_date=fund_data.get('investment_start_date'),
                    commitment_subscription=fund_data.get('commitment_subscription'),
                    is_active=True
                )
                
                session.add(new_fund)
                session.commit()
                session.refresh(new_fund)
                
                return new_fund.to_dict()
                
            finally:
                session.close()
                
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error creating fund: {e}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to create fund: {str(e)}"
            )
    
    async def update_fund(self, fund_id: int, fund_data: Dict[str, Any]) -> Dict[str, Any]:
        """Update an existing fund"""
        try:
            session = self.db_manager.get_session()
            try:
                fund = session.query(Fund).filter(Fund.id == fund_id).first()
                
                if not fund:
                    raise HTTPException(
                        status_code=status.HTTP_404_NOT_FOUND,
                        detail="Fund not found"
                    )
                
                # Check for duplicate code if code is being updated
                if 'code' in fund_data and fund_data['code'] != fund.code:
                    existing_fund = session.query(Fund).filter(Fund.code == fund_data['code']).first()
                    if existing_fund:
                        raise HTTPException(
                            status_code=status.HTTP_400_BAD_REQUEST,
                            detail="Fund with this code already exists"
                        )
                
                # Check for duplicate name if name is being updated
                if 'name' in fund_data and fund_data['name'] != fund.name:
                    existing_fund = session.query(Fund).filter(Fund.name == fund_data['name']).first()
                    if existing_fund:
                        raise HTTPException(
                            status_code=status.HTTP_400_BAD_REQUEST,
                            detail="Fund with this name already exists"
                        )
                
                # Update fund fields
                for field, value in fund_data.items():
                    if hasattr(fund, field) and value is not None:
                        setattr(fund, field, value)
                
                fund.updated_at = datetime.utcnow()
                session.commit()
                session.refresh(fund)
                
                return fund.to_dict()
                
            finally:
                session.close()
                
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error updating fund {fund_id}: {e}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to update fund: {str(e)}"
            )
    
    async def delete_fund(self, fund_id: int) -> Dict[str, str]:
        """Delete a fund"""
        try:
            session = self.db_manager.get_session()
            try:
                fund = session.query(Fund).filter(Fund.id == fund_id).first()
                
                if not fund:
                    raise HTTPException(
                        status_code=status.HTTP_404_NOT_FOUND,
                        detail="Fund not found"
                    )
                
                session.delete(fund)
                session.commit()
                
                return {"message": "Fund deleted successfully"}
                
            finally:
                session.close()
                
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error deleting fund {fund_id}: {e}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to delete fund: {str(e)}"
            )
    
    async def bulk_update_funds(self, fund_ids: List[int], updates: Dict[str, Any]) -> Dict[str, Any]:
        """Bulk update multiple funds"""
        try:
            session = self.db_manager.get_session()
            try:
                funds = session.query(Fund).filter(Fund.id.in_(fund_ids)).all()
                
                if len(funds) != len(fund_ids):
                    raise HTTPException(
                        status_code=status.HTTP_400_BAD_REQUEST,
                        detail="One or more funds not found"
                    )
                
                updated_count = 0
                for fund in funds:
                    for field, value in updates.items():
                        if hasattr(fund, field) and value is not None:
                            setattr(fund, field, value)
                    fund.updated_at = datetime.utcnow()
                    updated_count += 1
                
                session.commit()
                
                return {
                    "message": f"Successfully updated {updated_count} funds",
                    "updated_count": updated_count
                }
                
            finally:
                session.close()
                
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error bulk updating funds: {e}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to bulk update funds: {str(e)}"
            )
    
    async def get_fund_stats(self) -> Dict[str, int]:
        """Get fund statistics"""
        try:
            session = self.db_manager.get_session()
            try:
                total_funds = session.query(Fund).count()
                active_funds = session.query(Fund).filter(Fund.is_active == True).count()
                inactive_funds = session.query(Fund).filter(Fund.is_active == False).count()
                
                # Recent funds (last 30 days)
                thirty_days_ago = datetime.utcnow() - timedelta(days=30)
                recent_funds = session.query(Fund).filter(Fund.created_at >= thirty_days_ago).count()
                
                return {
                    "total_funds": total_funds,
                    "active_funds": active_funds,
                    "inactive_funds": inactive_funds,
                    "recent_funds": recent_funds
                }
                
            finally:
                session.close()
                
        except Exception as e:
            logger.error(f"Error getting fund stats: {e}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to retrieve fund statistics: {str(e)}"
            )
    
    async def get_fund_managers(
        self,
        page: int = 1,
        page_size: int = 10,
        search: Optional[str] = None,
        status_filter: Optional[str] = None
    ) -> Dict[str, Any]:
        """Get all fund managers with pagination and filtering using ORM"""
        try:
            session = self.db_manager.get_session()
            try:
                # Build query using ORM
                query = session.query(FundManager)
                
                # Apply search filter
                if search:
                    search_term = f"%{search}%"
                    query = query.filter(
                        (FundManager.fund_manager_name.ilike(search_term)) |
                        (FundManager.contact_first_name.ilike(search_term)) |
                        (FundManager.contact_last_name.ilike(search_term))
                    )
                
                # Apply status filter
                if status_filter:
                    query = query.filter(FundManager.status == status_filter.lower())
                
                # Get total count for pagination
                total_count = query.count()
                
                # Apply pagination
                offset = (page - 1) * page_size
                fund_managers = query.order_by(FundManager.id.asc()).offset(offset).limit(page_size).all()
                
                # Format response data
                row_data = []
                for fm in fund_managers:
                    # Format date as MM/DD/YYYY
                    created_date = fm.created_at.strftime("%m/%d/%Y") if fm.created_at else "N/A"
                    
                    # Build contact name from contact fields
                    contact_name = ""
                    if fm.contact_title:
                        contact_name += fm.contact_title + " "
                    if fm.contact_first_name:
                        contact_name += fm.contact_first_name + " "
                    if fm.contact_last_name:
                        contact_name += fm.contact_last_name
                    contact_name = contact_name.strip() or "No Contact"
                    
                    row_data.append({
                        "fundManagerID": fm.id,
                        "fundManagerName": fm.fund_manager_name or "No Name",
                        "contactName": contact_name,
                        "createdDate": created_date,
                        "status": "Active" if fm.status == 'active' else "Inactive"
                    })
                
                # Calculate pagination info
                total_pages = (total_count + page_size - 1) // page_size
                
                return {
                    "rowData": row_data,
                    "pagination": {
                        "current_page": page,
                        "page_size": page_size,
                        "total_pages": total_pages
                    }
                }
                
            finally:
                session.close()
                
        except Exception as e:
            logger.error(f"Error getting fund managers: {e}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to retrieve fund managers: {str(e)}"
            )

# Service instance
fund_service = FundManagementService()

# Response functions
async def get_fund_managers_response(page: int, page_size: int, search: Optional[str], status_filter: Optional[str]) -> JSONResponse:
    """Get fund managers with pagination and filtering"""
    try:
        # Load template JSON
        import json
        template_path = Path(__file__).parent.parent.parent / "frontendUtils" / "renders" / "fund_managers_template.json"
        
        with open(template_path, 'r', encoding='utf-8') as f:
            template = json.load(f)
        
        # Get fund managers data using ORM
        result = await fund_service.get_fund_managers(page, page_size, search, status_filter)
        
        # Update template with actual data
        template["rowData"] = result["rowData"]
        template["pagination"] = result["pagination"]
        
        return JSONResponse(content=template)
        
    except Exception as e:
        logger.error(f"Error in get_fund_managers_response: {e}")
        return JSONResponse(
            content={"error": f"Failed to retrieve fund managers: {str(e)}"}, 
            status_code=500
        )
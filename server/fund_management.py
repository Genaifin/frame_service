"""
Fund Management API Endpoints
Contains all CRUD operations for fund management
"""

from fastapi import APIRouter, HTTPException, Query, Body, Depends
from sqlalchemy import text
from typing import Optional
import logging

from database_models import DatabaseManager, Calendar
from rbac.utils.auth import getCurrentUser
from rbac.utils.frontend import getUserByUsername

# Import data source management models and service
from server.APIServerUtils.data_source_models import (
    AddDataSourceRequest, AddDataSourceResponse, DataSourceResponse, DataSourceListResponse,
    EditDataSourceRequest, EditDataSourceResponse, DataSourceDetailResponse
)
from server.APIServerUtils.data_source_management import data_source_service

# Import portfolio company management models and service
from server.APIServerUtils.portfolio_company_models import (
    AddPortfolioCompanyRequest, EditPortfolioCompanyRequest, PortfolioCompanyResponse
)
from server.APIServerUtils.portfolio_company_service import PortfolioCompanyService

# Initialize portfolio company service
portfolio_company_service = PortfolioCompanyService()

async def authenticate_user(username: str = Depends(getCurrentUser)):
    """Authenticate user using JWT token"""
    return username

def get_user_role(username: str) -> str:
    """Get user role from username"""
    user = getUserByUsername(username)
    if user:
        return user.get('role', 'unknown')
    return 'unknown'

# Set up logging
logger = logging.getLogger(__name__)

# Create router for fund management endpoints
router = APIRouter(prefix="/funds", tags=["Fund CRUD API"])


@router.get("")
async def get_funds_index(
    page: int = 1,
    page_size: int = 10,
    search: Optional[str] = None,
    status_filter: Optional[str] = None,
    fund_id: Optional[str] = None,
    __username: str = Depends(authenticate_user)
):
    """
    Get funds in the frontend JSON format for the Fund Masters page
    If fund_id is provided, returns specific fund details.
    Otherwise, returns the complete frontend configuration with:
    - Top navigation bar parameters
    - Sub pages configuration  
    - Module display configuration with table data
    """
    try:
        # Initialize database manager
        db_manager = DatabaseManager()
        conn = db_manager.engine.connect()
        
        # If fund_id is provided, return specific fund details
        if fund_id:
            return await get_specific_fund_details(conn, fund_id, __username)
        
        # Build the base query for fund list
        base_query = """
            SELECT 
                f.id as fund_id,
                f.code,
                f.name as fund_name,
                f.type as fund_type,
                f.fund_manager,
                f.base_currency,
                f.contact_person,
                f.stage,
                f.inception_date,
                f.investment_start_date,
                f.commitment_subscription,
                f.created_at,
                f.is_active
            FROM public.funds f
        """
        
        # Add search and filter conditions
        where_conditions = []
        params = {}
        
        if search:
            where_conditions.append("(f.name ILIKE :search OR f.code ILIKE :search OR f.fund_manager ILIKE :search OR f.contact_person ILIKE :search)")
            params['search'] = f"%{search}%"
        
        if status_filter:
            if status_filter.lower() == 'active':
                where_conditions.append("f.is_active = true")
            elif status_filter.lower() == 'inactive':
                where_conditions.append("f.is_active = false")
        
        if where_conditions:
            base_query += " WHERE " + " AND ".join(where_conditions)
        
        # Get total count for pagination
        count_query = f"SELECT COUNT(*) FROM ({base_query}) as count_query"
        total_result = conn.execute(text(count_query), params)
        total_count = total_result.fetchone()[0]
        
        # Add pagination
        offset = (page - 1) * page_size
        paginated_query = f"{base_query} ORDER BY f.created_at DESC LIMIT :page_size OFFSET :offset"
        params.update({'page_size': page_size, 'offset': offset})
        
        # Execute the query
        result = conn.execute(text(paginated_query), params)
        funds_data = result.fetchall()
        
        # Format the data to match the specified format
        row_data = []
        for row in funds_data:
            row_data.append({
                "fund_id": row[0],  # id
                "fundName": row[2],  # name
                "fundType": row[3] or "N/A",  # type
                "contactName": row[6] or "N/A",  # contact_person (renamed to contactName)
                "baseCurrency": row[5] or "N/A",  # base_currency
                "createdDate": row[11].strftime("%m/%d/%Y") if row[11] else "N/A",  # created_at
                "status": "Active" if row[12] else "Inactive"  # is_active
            })
        
        # Calculate pagination
        total_pages = (total_count + page_size - 1) // page_size
        
        # Build the response in the specified format
        response = {
            "rowClickEnabled": True,
            "rowClickAction": {
                "type": "navigation",
                "to": "/frame",
                "parameters": [
                    {
                        "key": "fund_id",
                        "value": "",
                        "dynamicValue": {
                            "enabled": True,
                            "id": "fund_id"
                        }
                    },
                    {
                        "key": "fund_name",
                        "value": "",
                        "dynamicValue": {
                            "enabled": True,
                            "id": "fundName"
                        }
                    },
                    {
                        "key": "page",
                        "value": "FundMasterDetail"
                    }
                ]
            },
            "colsToShow": [
                "fund_id",
                "fundName",
                "fundType",
                "contactName",
                "baseCurrency",
                "createdDate",
                "status"
            ],
            "columnConfig": {
                "fund_id": {
                    "name": "Fund ID",
                    "filter": "true",
                    "suppressHeaderMenuButton": False
                },
                "fundName": {
                    "name": "Fund Name",
                    "filter": "true",
                    "suppressHeaderMenuButton": False
                },
                "fundType": {
                    "name": "Fund Type",
                    "filter": "true",
                    "suppressHeaderMenuButton": False
                },
                "contactName": {
                    "name": "Contact Name",
                    "filter": "true",
                    "suppressHeaderMenuButton": False
                },
                "baseCurrency": {
                    "name": "Base Currency",
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
        
        conn.close()
        return response
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching funds: {e}")
        raise HTTPException(status_code=500, detail=f"Error fetching funds: {str(e)}")


async def get_specific_fund_details(conn, fund_id: str, username: str):
    """Get specific fund details for fund detail view"""
    try:
        # Get user role to conditionally filter fields
        user_role = get_user_role(username)
        is_admin = (user_role == 'admin')
        query = """
            SELECT 
                f.id, f.code, f.name, f.description, f.type, f.fund_manager, 
                f.base_currency, f.fund_admin, f.shadow, f.contact_person,
                f.contact_email, f.contact_number, f.sector, f.geography,
                f.strategy, f.market_cap, f.benchmark, f.stage, f.inception_date,
                f.investment_start_date, f.commitment_subscription, f.is_active, f.created_at
            FROM public.funds f
            WHERE f.id = :fund_id
        """
        
        result = conn.execute(text(query), {"fund_id": fund_id}).fetchone()
        
        if not result:
            raise HTTPException(status_code=404, detail="Fund not found")
       
        # Helper function to format JSON fields
        def format_json_field(field_data):
            if isinstance(field_data, list):
                return ", ".join(str(item) for item in field_data)
            elif isinstance(field_data, dict):
                return field_data.get('value', str(field_data))
            return str(field_data) if field_data else "N/A"
        
        # Helper function to get active fund admin/shadow names
        def get_active_names(field_data):
            if not field_data:
                return "N/A"
            
            if isinstance(field_data, list):
                # Filter for active items and extract names
                active_names = []
                for item in field_data:
                    if isinstance(item, dict):
                        # Check if item is active (handle both 'active' and 'isActive' keys)
                        is_active = item.get('active', item.get('isActive', True))
                        if is_active:
                            # Get name from various possible keys
                            name = item.get('name') or item.get('value') or item.get('label')
                            if name:
                                active_names.append(str(name))
                    elif isinstance(item, str):
                        active_names.append(item)
                
                return ", ".join(active_names) if active_names else "N/A"
            
            elif isinstance(field_data, dict):
                is_active = field_data.get('active', field_data.get('isActive', True))
                if is_active:
                    return field_data.get('name') or field_data.get('value') or str(field_data)
                return "N/A"
            
            return str(field_data) if field_data else "N/A"
        
        # Helper function to format currency
        def format_currency(amount):
            if amount is None:
                return "N/A"
            return f"USD {amount:,.0f}"
        
        # Helper function to format date
        def format_date(date_obj):
            if date_obj is None:
                return "N/A"
            return date_obj.strftime("%d/%m/%Y")
        
        # Build the response in the specified format
        response = {
            "success": True,
            "data": {
                "title": "FUND DETAILS",
                "isEditable": True,
                "onEditClick": {
                    "type": "navigation",
                    "to": "/frame",
                    "parameters": [
                        {
                            "key": "page",
                            "value": "EditFund"
                        },
                        {
                            "key": "fund_id",
                            "value": result[0]  # id
                        }
                    ]
                },
                "sections": [
                    {
                        "fields": [
                            {
                                "label": "Fund Name",
                                "value": result[2] or "N/A",  # name
                                "width": "50%"
                            },
                            {
                                "label": "Fund ID",
                                "value": result[0],  # id
                                "width": "50%"
                            },
                            {
                                "label": "Fund Type",
                                "value": result[4] or "N/A",  # type
                                "width": "50%"
                            },
                            {
                                "label": "Fund Manager",
                                "value": result[5] or "N/A",  # fund_manager
                                "width": "50%"
                            },
                            {
                                "label": "Base Currency",
                                "value": result[6] or "N/A",  # base_currency
                                "width": "50%"
                            },
                            {
                                "label": "Name of Fund Admin",
                                "value": get_active_names(result[7]),  # fund_admin
                                "width": "50%"
                            },
                            {
                                "label": "Name of Shadow",
                                "value": get_active_names(result[8]),  # shadow
                                "width": "50%"
                            },
                            {
                                "label": "Status",
                                "value": "Active" if result[21] else "Inactive",  # is_active
                                "type": "status-badge",
                                "width": "50%"
                            },
                            {
                                "label": "Created Date",
                                "value": format_date(result[22]),  # created_at
                                "width": "50%"
                            }
                        ]
                    },
                    {
                        "title": "LIFECYCLE",
                        "fields": [
                            {
                                "label": "Stage",
                                "value": result[17] or "N/A",  # stage
                                "width": "50%"
                            },
                            {
                                "label": "Inception Date",
                                "value": format_date(result[18]),  # inception_date
                                "width": "50%"
                            }
                        ] + ([] if is_admin else [
                            {
                                "label": "Investment Start Date",
                                "value": format_date(result[19]),  # investment_start_date
                                "width": "50%"
                            },
                            {
                                "label": "Commitment / Subscription",
                                "value": format_currency(result[20]),  # commitment_subscription
                                "width": "50%"
                            }
                        ])
                    },
                    {
                        "title": "CONTACT DETAILS",
                        "fields": [
                            {
                                "label": "Contact Name",
                                "value": result[9] or "N/A",  # contact_person
                                "width": "50%"
                            },
                            {
                                "label": "Email",
                                "value": result[10] or "N/A",  # contact_email
                                "width": "50%"
                            },
                            {
                                "label": "Contact Number",
                                "value": result[11] or "N/A",  # contact_number
                                "width": "50%"
                            }
                        ]
                    },
                    {
                        "title": "REPORTING ATTRIBUTES",
                        "fields": [
                            {
                                "label": "Sector",
                                "value": result[12] or "N/A",  # sector
                                "width": "50%"
                            },
                            {
                                "label": "Geography",
                                "value": result[13] or "N/A",  # geography
                                "width": "50%"
                            },
                            {
                                "label": "Strategy",
                                "value": format_json_field(result[14]),  # strategy
                                "width": "50%"
                            },
                            {
                                "label": "Market Cap",
                                "value": result[15] or "N/A",  # market_cap
                                "width": "50%"
                            },
                            {
                                "label": "Benchmarks for Comparison",
                                "value": format_json_field(result[16]),  # benchmark
                                "width": "50%"
                            }
                        ]
                    }
                ],
"footer": {
                    "fields": [
                        {
                            "type": "button",
                            "buttonText": "Mark as Inactive?" if result[21] else "Mark as Active?",
                            "buttonType": "text",
                            "buttonColor": "destructive",
                            "onConfirmation": {
                                "title": "Make Fund Inactive?" if result[21] else "Make Fund Active?",
                                "description": f"Are you sure you want to mark {result[2]} as {'inactive' if result[21] else 'active'}?",
                                "buttonText": "Mark as Inactive" if result[21] else "Mark as Active",
                                "buttonColor": "destructive",
                                "clickAction": {
                                    "type": "patchData",
                                    "patchAPIURL": "funds/status",
                                    "data": {
                                        "fund_id": result[0],  # id
                                        "active": False if result[21] else True
                                    },
                                    "actionAfterAPICall": {
                                        "type": "refreshModule",
                                        "moduleName": "FundInfoDetails"
                                    }
                                }
                            }
                        }
                    ]
                }
            }
        }
        
        return response
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching fund details: {e}")
        raise HTTPException(status_code=500, detail=f"Error fetching fund details: {str(e)}")


@router.post("")
async def create_fund(
    fund_data: dict,
    __username: str = Depends(authenticate_user)
):
    """
    Create a new fund
    """
    try:
        db_manager = DatabaseManager()
        conn = db_manager.engine.connect()
        
        # Validate required fields
        required_fields = ["fund-name"]
        for field in required_fields:
            if field not in fund_data:
                raise HTTPException(status_code=400, detail=f"Missing required field: {field}")
        
        # Generate fund code if not provided (using fund name)
        if "fund-code" not in fund_data or not fund_data["fund-code"]:
            # Generate fund code from fund name (remove spaces, convert to uppercase)
            fund_code = fund_data["fund-name"].replace(" ", "_").upper()[:80]
        else:
            fund_code = fund_data["fund-code"]
        
        # Check if fund code already exists
        check_query = "SELECT id FROM public.funds WHERE code = :fund_code"
        existing = conn.execute(text(check_query), {"fund_code": fund_code}).fetchone()
        
        if existing:
            conn.close()
            raise HTTPException(status_code=400, detail="Fund code already exists")
        
        # Insert new fund
        insert_query = """
            INSERT INTO public.funds (
                name, code, description, type, fund_manager, base_currency,
                fund_admin, shadow, contact_person, contact_email, contact_number,
                sector, geography, strategy, market_cap, benchmark,
                stage, inception_date, investment_start_date, commitment_subscription,
                is_active, created_at, updated_at
            ) VALUES (
                :fund_name, :fund_code, :description, :fund_type, :fund_manager, :base_currency,
                :fund_admin, :shadow, :contact_person, :contact_email, :contact_number,
                :sector, :geography, :strategy, :market_cap, :benchmark,
                :stage, :inception_date, :investment_start_date, :commitment_subscription,
                :is_active, NOW(), NOW()
            ) RETURNING id
        """
        
        # Prepare contact person name from title, first-name, last-name
        contact_person = None
        if fund_data.get("first-name") or fund_data.get("last-name"):
            parts = []
            if fund_data.get("title"):
                parts.append(fund_data["title"])
            if fund_data.get("first-name"):
                parts.append(fund_data["first-name"])
            if fund_data.get("last-name"):
                parts.append(fund_data["last-name"])
            contact_person = ' '.join(parts)
        
        # Parse date strings to date objects
        inception_date = None
        if fund_data.get("inception-date"):
            try:
                from datetime import datetime
                inception_date = datetime.fromisoformat(fund_data["inception-date"].replace('Z', '+00:00')).date()
            except (ValueError, TypeError):
                inception_date = None
        
        investment_start_date = None
        if fund_data.get("investment-start-date"):
            try:
                from datetime import datetime
                investment_start_date = datetime.fromisoformat(fund_data["investment-start-date"].replace('Z', '+00:00')).date()
            except (ValueError, TypeError):
                investment_start_date = None
        
        # Parse subscription amount
        commitment_subscription = None
        if fund_data.get("subscription"):
            try:
                commitment_subscription = float(fund_data["subscription"])
            except (ValueError, TypeError):
                commitment_subscription = None
        
        # Truncate base_currency to fit database constraint (VARCHAR(10))
        base_currency = fund_data.get("base-currency", "")
        if base_currency and len(base_currency) > 10:
            base_currency = base_currency[:10]
        
        # Convert JSON fields to JSON strings
        import json
        
        fund_admin_json = None
        if fund_data.get("fund-admins"):
            fund_admin_json = json.dumps(fund_data["fund-admins"])
        
        shadow_json = None
        if fund_data.get("shadow-admins"):
            shadow_json = json.dumps(fund_data["shadow-admins"])
        
        strategy_json = None
        if fund_data.get("strategy"):
            strategy_json = json.dumps(fund_data["strategy"])
        
        benchmark_json = None
        if fund_data.get("benchmarks"):
            benchmark_json = json.dumps(fund_data["benchmarks"])
        
        # Prepare the data for insertion
        insert_params = {
            "fund_name": fund_data["fund-name"],
            "fund_code": fund_code,
            "description": fund_data.get("description", fund_data["fund-name"]),
            "fund_type": fund_data.get("fund-type"),
            "fund_manager": fund_data.get("fund-manager"),
            "base_currency": base_currency,
            "fund_admin": fund_admin_json,
            "shadow": shadow_json,
            "contact_person": contact_person,
            "contact_email": fund_data.get("email"),
            "contact_number": fund_data.get("contact-number"),
            "sector": fund_data.get("sector"),
            "geography": fund_data.get("geography"),
            "strategy": strategy_json,
            "market_cap": fund_data.get("market-cap"),
            "benchmark": benchmark_json,
            "stage": fund_data.get("stage"),
            "inception_date": inception_date,
            "investment_start_date": investment_start_date,
            "commitment_subscription": commitment_subscription,
            "is_active": fund_data.get("is-active", True)
        }
        
        result = conn.execute(text(insert_query), insert_params)
        new_fund_id = result.fetchone()[0]
        
        conn.commit()
        conn.close()
        
        return {"success": True, "message": "Fund created successfully", "fund_id": new_fund_id}
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error creating fund: {e}")
        raise HTTPException(status_code=500, detail=f"Error creating fund: {str(e)}")


@router.patch("/status")
async def update_fund_status(
    status_data: dict,
    __username: str = Depends(authenticate_user)
):
    """
    Update fund status (active/inactive)
    """
    try:
        db_manager = DatabaseManager()
        conn = db_manager.engine.connect()
        
        fund_id = status_data.get("fund_id")
        
        if not fund_id:
            raise HTTPException(status_code=400, detail="fund_id is required")
        
        # Handle both "status" and "active" parameters
        if "active" in status_data:
            is_active = bool(status_data.get("active"))
        elif "status" in status_data:
            new_status = status_data.get("status", "active")
            is_active = new_status.lower() == "active"
        else:
            is_active = True  # default to active
        
        # Check if fund exists by code (since fund_id is "FUND0001" format)
        check_query = "SELECT id FROM public.funds WHERE id = :fund_id"
        existing = conn.execute(text(check_query), {"fund_id": fund_id}).fetchone()
        
        if not existing:
            conn.close()
            raise HTTPException(status_code=404, detail="Fund not found")
        
        # Get the actual database ID
        db_fund_id = existing[0]
        update_query = "UPDATE public.funds SET is_active = :is_active, updated_at = NOW() WHERE id = :fund_id"
        result = conn.execute(text(update_query), {"fund_id": db_fund_id, "is_active": is_active})
        
        # Check if any rows were affected
        if result.rowcount == 0:
            conn.close()
            raise HTTPException(status_code=404, detail="No fund was updated")
        
        conn.commit()
        conn.close()
        
        return {
            "fund_id": fund_id,
            "active": is_active,
            "message": f"Fund status updated to {'active' if is_active else 'inactive'}"
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error updating fund status: {e}")
        raise HTTPException(status_code=500, detail=f"Error updating fund status: {str(e)}")


@router.put("")
async def update_fund(
    fund_id: str = Query(..., description="Fund ID to update"),
    fund_data: dict = Body(...),
    __username: str = Depends(authenticate_user)
):
    """
    Update an existing fund
    """
    try:
        db_manager = DatabaseManager()
        conn = db_manager.engine.connect()
        
        # Check if fund exists
        check_query = "SELECT id FROM public.funds WHERE id = :fund_id"
        existing = conn.execute(text(check_query), {"fund_id": fund_id}).fetchone()
        
        if not existing:
            conn.close()
            raise HTTPException(status_code=404, detail="Fund not found")
        
        # Process contact person from title, first-name, last-name
        contact_person = None
        if fund_data.get("first-name") or fund_data.get("last-name"):
            parts = []
            if fund_data.get("title"):
                parts.append(fund_data["title"])
            if fund_data.get("first-name"):
                parts.append(fund_data["first-name"])
            if fund_data.get("last-name"):
                parts.append(fund_data["last-name"])
            contact_person = ' '.join(parts)
        
        # Parse date strings to date objects
        inception_date = None
        if fund_data.get("inception-date"):
            try:
                from datetime import datetime
                inception_date = datetime.fromisoformat(fund_data["inception-date"].replace('Z', '+00:00')).date()
            except (ValueError, TypeError):
                inception_date = None
        
        investment_start_date = None
        if fund_data.get("investment-start-date"):
            try:
                from datetime import datetime
                investment_start_date = datetime.fromisoformat(fund_data["investment-start-date"].replace('Z', '+00:00')).date()
            except (ValueError, TypeError):
                investment_start_date = None
        
        # Parse subscription amount
        commitment_subscription = None
        if fund_data.get("subscription"):
            try:
                commitment_subscription = float(fund_data["subscription"])
            except (ValueError, TypeError):
                commitment_subscription = None
        
        # Truncate base_currency to fit database constraint (VARCHAR(10))
        base_currency = fund_data.get("base-currency", "")
        if base_currency and len(base_currency) > 10:
            base_currency = base_currency[:10]
        
        # Convert JSON fields to JSON strings
        import json
        
        fund_admin_json = None
        if fund_data.get("fund-admins"):
            fund_admin_json = json.dumps(fund_data["fund-admins"])
        
        shadow_json = None
        if fund_data.get("shadow-admins"):
            shadow_json = json.dumps(fund_data["shadow-admins"])
        
        strategy_json = None
        if fund_data.get("strategy"):
            strategy_json = json.dumps(fund_data["strategy"])
        
        benchmark_json = None
        if fund_data.get("benchmarks"):
            benchmark_json = json.dumps(fund_data["benchmarks"])
        
        # Build update query dynamically
        update_fields = []
        params = {"fund_id": fund_id}
        
        # Define field mappings with processed values
        field_updates = {
            "name": fund_data.get("fund-name"),
            "type": fund_data.get("fund-type"),
            "fund_manager": fund_data.get("fund-manager"),
            "base_currency": base_currency,
            "fund_admin": fund_admin_json,
            "shadow": shadow_json,
            "contact_person": contact_person,
            "contact_email": fund_data.get("email"),
            "contact_number": fund_data.get("contact-number"),
            "sector": fund_data.get("sector"),
            "geography": fund_data.get("geography"),
            "strategy": strategy_json,
            "market_cap": fund_data.get("market-cap"),
            "benchmark": benchmark_json,
            "stage": fund_data.get("stage"),
            "inception_date": inception_date,
            "investment_start_date": investment_start_date,
            "commitment_subscription": commitment_subscription,
            "is_active": fund_data.get("is-active")
        }
        
        # Add non-null fields to update query
        for db_field, value in field_updates.items():
            if value is not None:
                safe_param_name = db_field.replace("-", "_")
                update_fields.append(f"{db_field} = :{safe_param_name}")
                params[safe_param_name] = value
        
        if update_fields:
            update_fields.append("updated_at = NOW()")
            update_query = f"UPDATE public.funds SET {', '.join(update_fields)} WHERE id = :fund_id"
            conn.execute(text(update_query), params)
        
        conn.commit()
        conn.close()
        
        return {"success": True, "message": "Fund updated successfully"}
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error updating fund {fund_id}: {e}")
        raise HTTPException(status_code=500, detail=f"Error updating fund: {str(e)}")


@router.delete("/{fund_id}")
async def delete_fund(
    fund_id: str,
    __username: str = Depends(authenticate_user)
):
    """
    Delete a fund (soft delete by setting is_active to false)
    """
    try:
        db_manager = DatabaseManager()
        conn = db_manager.engine.connect()
        
        # Check if fund exists
        check_query = "SELECT id FROM public.funds WHERE id = :fund_id"
        existing = conn.execute(text(check_query), {"fund_id": fund_id}).fetchone()
        
        if not existing:
            conn.close()
            raise HTTPException(status_code=404, detail="Fund not found")
        
        # Soft delete by setting is_active to false
        update_query = "UPDATE public.funds SET is_active = false, updated_at = NOW() WHERE id = :fund_id"
        conn.execute(text(update_query), {"fund_id": fund_id})
        
        conn.commit()
        conn.close()
        
        return {"success": True, "message": "Fund deleted successfully"}
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error deleting fund {fund_id}: {e}")
        raise HTTPException(status_code=500, detail=f"Error deleting fund: {str(e)}")


@router.get("/edit_form_details")
async def get_fund_edit_form_details(
    fund_id: str,
    __username: str = Depends(authenticate_user)
):
    """
    Get fund details for editing form using JSON template
    """
    import json
    from pathlib import Path
    from datetime import datetime
    
    try:
        # Get user role to conditionally filter fields
        user_role = get_user_role(__username)
        is_admin = (user_role == 'admin')
        
        db_manager = DatabaseManager()
        conn = db_manager.engine.connect()
        
        # Get fund details
        fund_query = """
            SELECT id, code, name, description, type, fund_manager, base_currency,
                fund_admin, shadow, contact_person, contact_email, contact_number,
                sector, geography, strategy, market_cap, benchmark, stage,
                inception_date, investment_start_date, commitment_subscription, is_active
            FROM public.funds 
            WHERE id = :fund_id
        """
        
        result = conn.execute(text(fund_query), {"fund_id": fund_id}).fetchone()
        conn.close()
        
        if not result:
            raise HTTPException(status_code=404, detail="Fund not found")
        
        # Load the template from JSON file
        template_path = Path(__file__).parent.parent / "frontendUtils" / "renders" / "edit_funds_form_details.json"
        with open(template_path, 'r') as f:
            response = json.load(f)
        
        # Map database values to template
        fund_data = {
            "id": result[0],
            "code": result[1],
            "name": result[2],
            "description": result[3],
            "type": result[4],
            "fund_manager": result[5],
            "base_currency": result[6],
            "fund_admin": result[7],
            "shadow": result[8],
            "contact_person": result[9],
            "contact_email": result[10],
            "contact_number": result[11],
            "sector": result[12],
            "geography": result[13],
            "strategy": result[14],
            "market_cap": result[15],
            "benchmark": result[16],
            "stage": result[17],
            "inception_date": result[18],
            "investment_start_date": result[19],
            "commitment_subscription": result[20],
            "is_active": result[21]
        }
        
        # Helper function to format date for display (DD/MM/YYYY)
        def format_date(date_obj):
            if date_obj:
                return date_obj.strftime('%d/%m/%Y')
            return None
        
        # Update template with actual database values
        for section in response.get("sections", []):
            section_id = section.get("id")
            
            # Update section idToShow if it's general-details
            if section_id == "genaral-details":
                section["idToShow"] = fund_data.get("code", "")
            
            # Update fields
            for field in section.get("fields", []):
                field_id = field.get("id")
                
                if field_id == "fund-name":
                    field["defaultValue"] = fund_data.get("name") or ""
                elif field_id == "fund-type":
                    field["defaultValue"] = fund_data.get("type") or ""
                elif field_id == "fund-manager":
                    field["defaultValue"] = fund_data.get("fund_manager") or ""
                elif field_id == "base-currency":
                    field["defaultValue"] = fund_data.get("base_currency") or ""
                elif field_id == "fund-admins":
                    # Handle fund_admin JSON array
                    fund_admins = fund_data.get("fund_admin") or []
                    if isinstance(fund_admins, list) and fund_admins:
                        for idx, admin in enumerate(fund_admins):
                            if idx < len(field.get("fields", [])):
                                # Extract value from object if it's a dict, otherwise use the admin directly
                                value_to_set = admin.get("value") if isinstance(admin, dict) else admin
                                field["fields"][idx]["defaultValue"] = value_to_set
                                field["fields"][idx]["isActive"] = (idx == len(fund_admins) - 1)
                elif field_id == "shadow-admins":
                    # Handle shadow JSON array
                    shadows = fund_data.get("shadow") or []
                    if isinstance(shadows, list) and shadows:
                        for idx, shadow in enumerate(shadows):
                            if idx < len(field.get("fields", [])):
                                value_to_set = shadow.get("value") if isinstance(shadow, dict) else shadow
                                field["fields"][idx]["defaultValue"] = value_to_set
                                field["fields"][idx]["isActive"] = (idx == len(shadows) - 1)
                elif field_id == "stage":
                    field["defaultValue"] = fund_data.get("stage") or ""
                elif field_id == "inception-date":
                    field["defaultValue"] = format_date(fund_data.get("inception_date")) or ""
                elif field_id == "investment-start-date":
                    field["defaultValue"] = format_date(fund_data.get("investment_start_date")) or ""
                elif field_id == "subscription":
                    commitment = fund_data.get("commitment_subscription")
                    field["defaultValue"] = str(commitment) if commitment else ""
                elif field_id == "contact-name":
                    field["defaultValue"] = fund_data.get("contact_person") or ""
                elif field_id == "contact-email":
                    field["defaultValue"] = fund_data.get("contact_email") or ""
                elif field_id == "contact-number":
                    field["defaultValue"] = fund_data.get("contact_number") or ""
                elif field_id == "sector":
                    field["defaultValue"] = fund_data.get("sector") or ""
                elif field_id == "geography":
                    field["defaultValue"] = fund_data.get("geography") or ""
                elif field_id == "market-cap":
                    field["defaultValue"] = fund_data.get("market_cap") or ""
                elif field_id == "description":
                    field["defaultValue"] = fund_data.get("description") or ""
                # Handle strategy and benchmark (JSON fields)
                elif field_id == "strategy":
                    strategy = fund_data.get("strategy")
                    if isinstance(strategy, list):
                        field["defaultValue"] = strategy
                elif field_id == "benchmark":
                    benchmark = fund_data.get("benchmark")
                    if isinstance(benchmark, list):
                        field["defaultValue"] = benchmark
        
        # Filter out fields for admin users
        if is_admin:
            for section in response.get("sections", []):
                section["fields"] = [
                    field for field in section.get("fields", [])
                    if field.get("id") not in ["investment-start-date", "subscription"]
                ]
        
        return response
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching fund edit details {fund_id}: {e}")
        raise HTTPException(status_code=500, detail=f"Error fetching fund edit details: {str(e)}")


@router.post("/add_calendar")
async def add_calendar(
    fund_id: str,
    calendar_data: dict,
    __username: str = Depends(authenticate_user)
):
    """
    Add calendar configuration for a fund with publishing frequency and delay
    
    Query Parameters:
    - fund_id: The fund identifier
    
    Expected JSON structure:
    {
        "publishingFrequency": "monthly",
        "delay": "30",
        "documents": [  // Optional
            {
                "document": "Statement",
                "frequency": "quarterly",
                "delay": "30"
            }
        ]
    }
    
    Only 'publishingFrequency' and 'delay' are mandatory. All other fields are optional.
    """
    try:
        db_manager = DatabaseManager()
        conn = db_manager.engine.connect()
        
        # Validate required fields - only frequency and delay are mandatory
        required_fields = ["publishingFrequency", "delay"]
        for field in required_fields:
            if field not in calendar_data:
                raise HTTPException(status_code=400, detail=f"Missing required field: {field}")
        
        frequency = calendar_data["publishingFrequency"]
        delay = calendar_data["delay"]
        documents = calendar_data.get("documents")  # Optional field
        
        # Validate fund_id exists
        fund_check_query = "SELECT id FROM public.funds WHERE id = :fund_id"
        fund_result = conn.execute(text(fund_check_query), {"fund_id": fund_id}).fetchone()
        
        if not fund_result:
            conn.close()
            raise HTTPException(status_code=404, detail="Fund not found")
        
        # Get the actual database ID
        db_fund_id = fund_result[0]
        
        # Check if calendar already exists for this fund
        existing_calendar_query = "SELECT id FROM public.calendars WHERE fund_id = :fund_id AND is_active = true"
        existing_calendar = conn.execute(text(existing_calendar_query), {"fund_id": db_fund_id}).fetchone()
        
        if existing_calendar:
            conn.close()
            raise HTTPException(status_code=400, detail="Calendar already exists for this fund")
        
        # Validate frequency (case insensitive)
        valid_frequencies = ["daily", "weekly", "monthly", "quarterly", "annually"]
        valid_frequencies_caps = ["Daily", "Weekly", "Monthly", "Quarterly", "Annually"]
        all_valid_frequencies = valid_frequencies + valid_frequencies_caps
        
        if frequency not in all_valid_frequencies:
            raise HTTPException(status_code=400, detail=f"Invalid frequency. Must be one of: {', '.join(all_valid_frequencies)}")
        
        # Normalize to lowercase for storage
        frequency = frequency.lower()
        
        # Validate delay is a positive integer
        try:
            delay_int = int(delay)
            if delay_int < 0:
                raise HTTPException(status_code=400, detail="Delay must be a non-negative integer")
        except (ValueError, TypeError):
            raise HTTPException(status_code=400, detail="Delay must be a valid integer")
        
        # Validate documents array if provided (optional)
        if documents is not None:
            if not isinstance(documents, list) or len(documents) == 0:
                raise HTTPException(status_code=400, detail="Documents must be a non-empty array if provided")
            
            # Validate each document
            for doc in documents:
                if not isinstance(doc, dict):
                    raise HTTPException(status_code=400, detail="Each document must be an object")
                
                doc_required_fields = ["document", "frequency", "delay"]
                for field in doc_required_fields:
                    if field not in doc:
                        raise HTTPException(status_code=400, detail=f"Missing required field in document: {field}")
                
                # Validate document frequency (case insensitive)
                if doc["frequency"] not in all_valid_frequencies:
                    raise HTTPException(status_code=400, detail=f"Invalid document frequency '{doc['frequency']}'. Must be one of: {', '.join(all_valid_frequencies)}")
                
                # Normalize document frequency to lowercase for storage
                doc["frequency"] = doc["frequency"].lower()
                
                # Validate document delay
                try:
                    doc_delay_int = int(doc["delay"])
                    if doc_delay_int < 0:
                        raise HTTPException(status_code=400, detail="Document delay must be a non-negative integer")
                except (ValueError, TypeError):
                    raise HTTPException(status_code=400, detail="Document delay must be a valid integer")
        
        # Insert new calendar - handle optional fields
        import json
        
        # Build dynamic insert query based on provided fields
        columns = ["fund_id", "frequency", "delay", "created_at", "updated_at", "is_active"]
        values = [":fund_id", ":frequency", ":delay", "NOW()", "NOW()", ":is_active"]
        
        insert_params = {
            "fund_id": db_fund_id,
            "frequency": frequency,
            "delay": delay_int,
            "is_active": True
        }
        
        # Add optional fields if provided
        if documents is not None:
            columns.append("documents")
            values.append(":documents")
            insert_params["documents"] = json.dumps(documents)
        
        if __username:
            columns.append("created_by")
            values.append(":created_by")
            insert_params["created_by"] = __username
        
        insert_query = f"""
            INSERT INTO public.calendars ({', '.join(columns)})
            VALUES ({', '.join(values)})
            RETURNING id
        """
        
        result = conn.execute(text(insert_query), insert_params)
        new_calendar_id = result.fetchone()[0]
        
        conn.commit()
        conn.close()
        
        return {
            "success": True, 
            "message": "Calendar created successfully", 
            "calendar_id": new_calendar_id,
            "fund_id": fund_id
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error creating calendar: {e}")
        raise HTTPException(status_code=500, detail=f"Error creating calendar: {str(e)}")


@router.put("/edit_calendar")
async def edit_calendar(
    fund_id: str,
    calendar_data: dict,
    __username: str = Depends(authenticate_user)
):
    """
    Edit calendar configuration for a fund with publishing frequency and document schedules
    
    Query Parameters:
    - fund_id: The fund identifier
    
    Expected JSON structure:
    {
        "publishingFrequency": "monthly",
        "delay": "30",
        "documents": [
            {
                "document": "Statement",
                "frequency": "quarterly",
                "delay": "30"
            },
            {
                "document": "K-1",
                "frequency": "quarterly", 
                "delay": "30"
            }
        ]
    }
    """
    try:
        db_manager = DatabaseManager()
        conn = db_manager.engine.connect()
        
        # Validate required fields
        required_fields = ["publishingFrequency", "delay", "documents"]
        for field in required_fields:
            if field not in calendar_data:
                raise HTTPException(status_code=400, detail=f"Missing required field: {field}")
        
        frequency = calendar_data["publishingFrequency"]
        delay = calendar_data["delay"]
        documents = calendar_data["documents"]
        
        # Validate fund_id exists and get calendar for this fund
        fund_check_query = "SELECT id FROM public.funds WHERE id = :fund_id"
        fund_result = conn.execute(text(fund_check_query), {"fund_id": fund_id}).fetchone()
        
        if not fund_result:
            conn.close()
            raise HTTPException(status_code=404, detail="Fund not found")
        
        # Get the actual database ID
        db_fund_id = fund_result[0]
        
        # Get calendar for this fund
        calendar_check_query = "SELECT id FROM public.calendars WHERE fund_id = :fund_id AND is_active = true"
        calendar_result = conn.execute(text(calendar_check_query), {"fund_id": db_fund_id}).fetchone()
        
        if not calendar_result:
            conn.close()
            raise HTTPException(status_code=404, detail="Calendar not found for this fund")
        
        # Get the calendar ID
        db_calendar_id = calendar_result[0]
        
        # Validate frequency (case insensitive)
        valid_frequencies = ["daily", "weekly", "monthly", "quarterly", "annually"]
        valid_frequencies_caps = ["Daily", "Weekly", "Monthly", "Quarterly", "Annually"]
        all_valid_frequencies = valid_frequencies + valid_frequencies_caps
        
        if frequency not in all_valid_frequencies:
            raise HTTPException(status_code=400, detail=f"Invalid frequency. Must be one of: {', '.join(all_valid_frequencies)}")
        
        # Normalize to lowercase for storage
        frequency = frequency.lower()
        
        # Validate delay is a positive integer
        try:
            delay_int = int(delay)
            if delay_int < 0:
                raise HTTPException(status_code=400, detail="Delay must be a non-negative integer")
        except (ValueError, TypeError):
            raise HTTPException(status_code=400, detail="Delay must be a valid integer")
        
        # Validate documents array
        if not isinstance(documents, list) or len(documents) == 0:
            raise HTTPException(status_code=400, detail="Documents must be a non-empty array")
        
        # Validate each document
        for doc in documents:
            if not isinstance(doc, dict):
                raise HTTPException(status_code=400, detail="Each document must be an object")
            
            doc_required_fields = ["document", "frequency", "delay"]
            for field in doc_required_fields:
                if field not in doc:
                    raise HTTPException(status_code=400, detail=f"Missing required field in document: {field}")
            
            # Validate document frequency (case insensitive)
            if doc["frequency"] not in all_valid_frequencies:
                raise HTTPException(status_code=400, detail=f"Invalid document frequency '{doc['frequency']}'. Must be one of: {', '.join(all_valid_frequencies)}")
            
            # Normalize document frequency to lowercase for storage
            doc["frequency"] = doc["frequency"].lower()
            
            # Validate document delay
            try:
                doc_delay_int = int(doc["delay"])
                if doc_delay_int < 0:
                    raise HTTPException(status_code=400, detail="Document delay must be a non-negative integer")
            except (ValueError, TypeError):
                raise HTTPException(status_code=400, detail="Document delay must be a valid integer")
        
        # Update existing calendar
        update_query = """
            UPDATE public.calendars 
            SET frequency = :frequency, 
                delay = :delay, 
                documents = :documents,
                updated_by = :updated_by,
                updated_at = NOW()
            WHERE id = :calendar_id AND is_active = true
            RETURNING id
        """
        
        import json
        update_params = {
            "calendar_id": db_calendar_id,
            "frequency": frequency,
            "delay": delay_int,
            "documents": json.dumps(documents),
            "updated_by": __username
        }
        
        result = conn.execute(text(update_query), update_params)
        updated_calendar = result.fetchone()
        
        if not updated_calendar:
            conn.close()
            raise HTTPException(status_code=404, detail="Calendar not found or could not be updated")
        
        conn.commit()
        conn.close()
        
        return {
            "success": True, 
            "message": "Calendar updated successfully", 
            "calendar_id": updated_calendar[0],
            "fund_id": db_fund_id
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error updating calendar: {e}")
        raise HTTPException(status_code=500, detail=f"Error updating calendar: {str(e)}")


@router.get("/get_calendars")
async def get_calendars(
    fund_id: str,
    __username: str = Depends(authenticate_user)
):
    """
    Get all calendar configurations for a fund
    
    Query Parameters:
    - fund_id: The fund identifier
    
    Returns:
    {
        "data": {
            "fieldsData": {
                "publishingFrequency": "Monthly",
                "Delay": "30 Days"
            },
            "tableData": {
                "rowData": [
                    {
                        "document": "Statement",
                        "publishingFrequency": "Quarterly",
                        "delay": "30 Days"
                    },
                    {
                        "document": "K-1",
                        "publishingFrequency": "Quarterly",
                        "delay": "30 Days"
                    }
                ]
            }
        }
    }
    """
    try:
        db_manager = DatabaseManager()
        conn = db_manager.engine.connect()
        
        # Validate fund_id exists
        fund_check_query = "SELECT id FROM public.funds WHERE id = :fund_id"
        fund_result = conn.execute(text(fund_check_query), {"fund_id": fund_id}).fetchone()
        
        if not fund_result:
            conn.close()
            raise HTTPException(status_code=404, detail="Fund not found")
        
        # Get the actual database ID
        db_fund_id = fund_result[0]
        
        # Get calendar for this fund
        calendar_query = """
            SELECT id, frequency, delay, documents, created_at, updated_at
            FROM public.calendars 
            WHERE fund_id = :fund_id AND is_active = true
            ORDER BY created_at DESC
        """
        
        calendar_result = conn.execute(text(calendar_query), {"fund_id": db_fund_id}).fetchone()
        conn.close()
        
        if not calendar_result:
            # Return empty structure if no calendar found
            return {
                "data": {
                    "fieldsData": {
                        "publishingFrequency": "",
                        "Delay": ""
                    },
                    "tableData": {
                        "rowData": []
                    }
                }
            }
        
        # Parse the calendar data
        calendar_id, frequency, delay, documents_data, created_at, updated_at = calendar_result
        
        import json
        # Handle documents data - it might be a string (JSON) or already a list
        if isinstance(documents_data, str):
            documents = json.loads(documents_data) if documents_data else []
        elif isinstance(documents_data, list):
            documents = documents_data
        else:
            documents = []
        
        # Helper function to format frequency for display
        def format_frequency(freq):
            if not freq:
                return ""
            return freq.capitalize()
        
        # Helper function to format delay for display
        def format_delay(delay_val):
            if delay_val is None or delay_val == "":
                return ""
            return f"{delay_val} Days"
        
        # Build the response structure
        fields_data = {
            "publishingFrequency": format_frequency(frequency),
            "Delay": format_delay(delay)
        }
        
        # Build table data from documents
        row_data = []
        for doc in documents:
            row_data.append({
                "document": doc.get("document", ""),
                "publishingFrequency": format_frequency(doc.get("frequency", "")),
                "delay": format_delay(doc.get("delay", ""))
            })
        
        table_data = {
            "rowData": row_data
        }
        
        return {
            "data": {
                "fieldsData": fields_data,
                "tableData": table_data
            }
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching calendars for fund {fund_id}: {e}")
        raise HTTPException(status_code=500, detail=f"Error fetching calendars: {str(e)}")


# ============================================================================
# DATA SOURCE MANAGEMENT API ENDPOINTS
# ============================================================================

@router.post("/add_source", description="Add a new data source", response_model=AddDataSourceResponse)
async def add_source(
    request: AddDataSourceRequest,
    fund_id: int = Query(..., description="Fund ID to associate with the data source"),
    *, __username: str = Depends(authenticate_user)
):
    """
    Add a new data source with fund association.
    
    This endpoint creates a new data source entry in the database with the provided configuration.
    The data source can be associated with different document types for Frame and Validus platforms.
    
    **Request Body Examples:**
    
    **S3 Bucket Source:**
    ```json
    {
        "frameDocuments": ["Capital Call", "Statement"],
        "validusDocuments": ["F3", "Statement"],
        "name": "S3 Setup",
        "source": "S3 Bucket",
        "holidayCalendar": "US Holiday Calendar",
        "s3": {
            "shareName": "name",
            "connectionString": "connection string",
            "details": "details"
        }
    }
    ```
    
    **Email Source:**
    ```json
    {
        "frameDocument": ["Capital Call"],
        "validusDocument": ["NAV Statement", "Statement"],
        "name": "Email Setup",
        "source": "Email",
        "holidayCalendar": "US Holiday Calendar",
        "email": {
            "smtpServer": "server",
            "port": "port",
            "fromAddress": "from",
            "username": "user",
            "password": "password",
            "details": "Additional Details"
        }
    }
    ```
    
    **API Invoke Source:**
    ```json
    {
        "frameDocument": ["Statement"],
        "validusDocument": ["Statement"],
        "name": "API Invoke Setup",
        "source": "API Invoke",
        "holidayCalendar": "US Holiday Calendar",
        "apiInvoke": {
            "connectionCode": "code",
            "url": "url",
            "payload": "payload",
            "contentType": "content type",
            "method": "method",
            "timeout": "10",
            "path": "path",
            "count": "21",
            "recentBefore": "before",
            "details": "Details",
            "isOptional": true
        }
    }
    ```
    
    **Portal Source:**
    ```json
    {
        "frameDocument": ["Statement"],
        "validusDocument": ["NAV Statement"],
        "name": "Portal Setup",
        "source": "Portal",
        "holidayCalendar": "US Holiday Calendar",
        "portal": {
            "url": "portal",
            "username": "name",
            "password": "password",
            "scriptType": "js",
            "details": "Details",
            "scriptFile": {}
        }
    }
    ```
    
    **SFTP Source:**
    ```json
    {
        "frameDocument": ["Statement"],
        "validusDocument": ["NAV Statement"],
        "name": "SFTP Setup",
        "source": "SFTP",
        "holidayCalendar": "US Holiday Calendar",
        "sftp": {
            "connectionCode": "code",
            "sftpConnectionCode": "sftp code",
            "path": "path",
            "count": "12",
            "recentBefore": "before",
            "destination": "destination",
            "serverCheck": "server",
            "expectedAt": "12",
            "sourceActionId": "21",
            "details": "Details",
            "isOptional": true,
            "includeHolidayFiles": true
        }
    }
    ```
    
    **Parameters:**
    - **fund_id**: The ID of the fund to associate with this data source
    - **name**: Name of the data source (required)
    - **source**: Type of source - must be one of: Email, S3 Bucket, Portal, API, SFTP, API Invoke
    - **holidayCalendar**: Holiday calendar - must be one of: US, Europe, US Holiday Calendar, Europe Holiday Calendar
    - **frameDocuments/frameDocument**: List of Frame documents (optional)
    - **validusDocuments/validusDocument**: List of Validus documents (optional)
    - **s3**: S3 configuration (required if source is "S3 Bucket")
    - **email**: Email configuration (required if source is "Email")
    - **apiInvoke**: API Invoke configuration (required if source is "API Invoke")
    - **portal**: Portal configuration (required if source is "Portal")
    - **sftp**: SFTP configuration (required if source is "SFTP")
    - **additional_details**: Additional details (optional)
    """
    return await data_source_service.add_data_source(request, fund_id, __username)


@router.get("/data_sources", description="Get all data sources with pagination and filtering", response_model=DataSourceListResponse)
async def get_data_sources(
    page: int = Query(1, ge=1, description="Page number"),
    page_size: int = Query(20, ge=1, le=100, description="Number of items per page"),
    search: Optional[str] = Query(None, description="Search term for data source name"),
    source_type: Optional[str] = Query(None, description="Filter by source type"),
    holiday_calendar: Optional[str] = Query(None, description="Filter by holiday calendar"),
    is_active: Optional[bool] = Query(True, description="Filter by active status"),
    *, __username: str = Depends(authenticate_user)
):
    """
    Get all data sources with pagination, search, and filtering.
    
    **Parameters:**
    - **page**: Page number for pagination (default: 1)
    - **page_size**: Number of data sources per page (default: 20, max: 100)
    - **search**: Search term for data source name
    - **source_type**: Filter by source type (Email, S3 Bucket, Portal, API, SFTP)
    - **holiday_calendar**: Filter by holiday calendar (US, Europe)
    - **is_active**: Filter by active status (default: true)
    """
    return await data_source_service.get_data_sources(
        page=page,
        page_size=page_size,
        search=search,
        source_type=source_type,
        holiday_calendar=holiday_calendar,
        is_active=is_active
    )


def _transform_data_source_from_template(data_source, source_id: int, product: str):
    """Transform data source using JSON template file based on product type"""
    import json
    from pathlib import Path
    
    # Load the template from JSON file
    template_path = Path(__file__).parent.parent / "frontendUtils" / "renders" / "fund_source_details.json"
    with open(template_path, 'r') as f:
        response = json.load(f)
    
    # Extract source details
    source_details = data_source.source_details or {}
    document_for = data_source.document_for or {}
    
    # Get documents based on product type
    if product == "Frame":
        selected_docs = document_for.get('frameDocuments', [])
    else:  # Validus
        selected_docs = document_for.get('validusDocuments', [])
    
    # Keep both for form data (for editing)
    frame_docs = document_for.get('frameDocuments', [])
    validus_docs = document_for.get('validusDocuments', [])
    
    # Format selected documents for display based on product
    selected_doc_str = ", ".join(selected_docs) if selected_docs else "None"
    
    # Map source type for display
    source_display = data_source.source
    if source_display == "API":
        source_display = "API Invoke"
    
    # Map holiday calendar for display
    holiday_calendar_display = data_source.holiday_calendar
    if holiday_calendar_display == "US":
        holiday_calendar_display = "US Holiday Calendar"
    elif holiday_calendar_display == "Europe":
        holiday_calendar_display = "Europe Holiday Calendar"
    
    # Create code - use abbreviated source type and ID
    source_abbrev = {
        "S3 Bucket": "S3",
        "Email": "Email", 
        "Portal": "Portal",
        "API Invoke": "API",
        "SFTP": "SFTP"
    }.get(source_display, source_display)
    code_value = f"{source_abbrev}#{source_id}"
    
    # Update the first section with basic info - show only selected product's documents
    document_label = f"Document for NAV {product}" if product == "Validus" else f"Document for {product}"
    response["sections"][0]["fields"] = [
        {"label": document_label, "value": selected_doc_str, "sameLine": True},
        {"label": "Code", "value": code_value, "sameLine": True},
        {"label": "Name", "value": data_source.name, "sameLine": True},
        {"label": "Source", "value": source_display, "sameLine": True},
        {"label": "Holiday Calendar", "value": holiday_calendar_display, "sameLine": True}
    ]
    
    # Update setup details section based on source type - directly update field values
    setup_fields = []
    if data_source.source == "S3 Bucket":
        setup_fields = [
            {"label": "Share Name", "value": source_details.get('shareName', 'N/A'), "sameLine": True},
            {"label": "Connection String", "value": source_details.get('connectionString', 'N/A'), "sameLine": True}
        ]
    elif data_source.source == "Email":
        setup_fields = [
            {"label": "SMTP Server", "value": source_details.get('smtpServer', 'N/A'), "sameLine": True},
            {"label": "Port", "value": source_details.get('port', 'N/A'), "sameLine": True},
            {"label": "From Address", "value": source_details.get('fromAddress', 'N/A'), "sameLine": True},
            {"label": "Username", "value": source_details.get('username', 'N/A'), "sameLine": True},
            {"label": "Password", "value": "********" if source_details.get('password') else 'N/A', "sameLine": True}
        ]
    elif data_source.source == "API":
        setup_fields = [
            {"label": "Connection Code", "value": source_details.get('connectionCode', 'N/A'), "sameLine": True},
            {"label": "URL", "value": source_details.get('url', 'N/A'), "sameLine": True},
            {"label": "Method", "value": source_details.get('method', 'N/A'), "sameLine": True},
            {"label": "Content Type", "value": source_details.get('contentType', 'N/A'), "sameLine": True},
            {"label": "Payload", "value": source_details.get('payload', 'N/A'), "sameLine": True},
            {"label": "Timeout", "value": source_details.get('timeout', 'N/A'), "sameLine": True},
            {"label": "Path", "value": source_details.get('path', 'N/A'), "sameLine": True},
            {"label": "Count", "value": source_details.get('count', 'N/A'), "sameLine": True},
            {"label": "Recent Before", "value": source_details.get('recentBefore', 'N/A'), "sameLine": True},
            {"label": "Is Optional", "value": str(source_details.get('isOptional', False)), "sameLine": True}
        ]
    elif data_source.source == "Portal":
        setup_fields = [
            {"label": "URL", "value": source_details.get('url', 'N/A'), "sameLine": True},
            {"label": "Username", "value": source_details.get('username', 'N/A'), "sameLine": True},
            {"label": "Password", "value": "********" if source_details.get('password') else 'N/A', "sameLine": True},
            {"label": "Script Type", "value": source_details.get('scriptType', 'N/A'), "sameLine": True}
        ]
    elif data_source.source == "SFTP":
        setup_fields = [
            {"label": "Connection Code", "value": source_details.get('connectionCode', 'N/A'), "sameLine": True},
            {"label": "SFTP Connection Code", "value": source_details.get('sftpConnectionCode', 'N/A'), "sameLine": True},
            {"label": "Path", "value": source_details.get('path', 'N/A'), "sameLine": True},
            {"label": "Destination", "value": source_details.get('destination', 'N/A'), "sameLine": True},
            {"label": "Count", "value": source_details.get('count', 'N/A'), "sameLine": True},
            {"label": "Recent Before", "value": source_details.get('recentBefore', 'N/A'), "sameLine": True},
            {"label": "Server Check", "value": source_details.get('serverCheck', 'N/A'), "sameLine": True},
            {"label": "Expected At", "value": source_details.get('expectedAt', 'N/A'), "sameLine": True},
            {"label": "Source Action ID", "value": source_details.get('sourceActionId', 'N/A'), "sameLine": True},
            {"label": "Is Optional", "value": str(source_details.get('isOptional', False)), "sameLine": True},
            {"label": "Include Holiday Files", "value": str(source_details.get('includeHolidayFiles', False)), "sameLine": True}
        ]
    
    # Update or remove setup details section
    if setup_fields:
        response["sections"][1]["fields"] = setup_fields
    else:
        response["sections"].pop(1)
    
    # Update additional details section - use only database column, default to empty string
    details_value = data_source.additional_details or ''
    
    # Adjust index based on whether setup section exists
    additional_details_idx = 2 if len(response["sections"]) > 2 else 1
    if len(response["sections"]) > additional_details_idx:
        response["sections"][additional_details_idx]["fields"][0]["value"] = details_value
    else:
        response["sections"].append({
            "title": "ADDITIONAL DETAILS",
            "fields": [
                {"label": "Details", "value": details_value, "sameLine": True}
            ]
        })
    
    # Update onEditClick formData - send BOTH documents regardless of product
    response["onEditClick"]["data"]["formData"]["frameDocument"] = frame_docs
    response["onEditClick"]["data"]["formData"]["validusDocument"] = validus_docs
    
    response["onEditClick"]["data"]["formData"]["name"] = data_source.name
    response["onEditClick"]["data"]["formData"]["source"] = source_display
    response["onEditClick"]["data"]["formData"]["holidayCalendar"] = holiday_calendar_display
    
    # Update source-specific form data
    if data_source.source == "S3 Bucket":
        response["onEditClick"]["data"]["formData"]["shareName"] = source_details.get('shareName', '')
        response["onEditClick"]["data"]["formData"]["connectionString"] = source_details.get('connectionString', '')
    
    response["onEditClick"]["data"]["formData"]["details"] = details_value or ""
    
    # Update PUT API URL and DELETE API URL with dynamic source_id
    response["onEditClick"]["data"]["clickAction"]["putAPIURL"] = f"funds/data_sources/{source_id}"
    
    # Update delete confirmation description and deleteAPIURL in footer
    response["footer"]["fields"][0]["onConfirmation"]["description"] = f"Are you sure you want to delete {data_source.name} source?"
    response["footer"]["fields"][0]["onConfirmation"]["clickAction"]["deleteAPIURL"] = f"/funds/data_sources/{source_id}"
    
    return response


def _transform_data_source_to_ui_format(data_source, source_id: int):
    """Transform data source data into UI format"""
    from server.APIServerUtils.data_source_models import (
        DataSourceDetailResponse, DataSourceSection, DataSourceField,
        OnEditClick, EditClickData, DropdownOptions, FormData, ClickAction,
        Footer, FooterField, OnConfirmation
    )
    
    # Extract source details
    source_details = data_source.source_details or {}
    document_for = data_source.document_for or {}
    
    # Get frame and validus documents - use correct field names
    frame_docs = document_for.get('frameDocuments', [])
    validus_docs = document_for.get('validusDocuments', [])
    
    # Format document lists for display
    frame_doc_str = ", ".join(frame_docs) if frame_docs else "None"
    
    # Map source type for display
    source_display = data_source.source
    if source_display == "API":
        source_display = "API Invoke"
    
    # Map holiday calendar for display
    holiday_calendar_display = data_source.holiday_calendar
    if holiday_calendar_display == "US":
        holiday_calendar_display = "US Holiday Calendar"
    elif holiday_calendar_display == "Europe":
        holiday_calendar_display = "Europe Holiday Calendar"
    
    # Create code - use abbreviated source type and ID
    source_abbrev = {
        "S3 Bucket": "S3",
        "Email": "Email", 
        "Portal": "Portal",
        "API Invoke": "API",
        "SFTP": "SFTP"
    }.get(source_display, source_display)
    code_value = f"{source_abbrev}#{source_id}"
    
    # Create sections
    sections = [
        DataSourceSection(
            fields=[
                DataSourceField(label="Document for Frame", value=frame_doc_str, sameLine=True),
                DataSourceField(label="Code", value=code_value, sameLine=True),
                DataSourceField(label="Name", value=data_source.name, sameLine=True),
                DataSourceField(label="Source", value=source_display, sameLine=True),
                DataSourceField(label="Holiday Calendar", value=holiday_calendar_display, sameLine=True)
            ]
        )
    ]
    
    # Add setup details section based on source type
    setup_fields = []
    if data_source.source == "S3 Bucket":
        setup_fields = [
            DataSourceField(label="Share Name", value=source_details.get('shareName', 'N/A'), sameLine=True),
            DataSourceField(label="Connection String", value=source_details.get('connectionString', 'N/A'), sameLine=True)
        ]
    elif data_source.source == "Email":
        setup_fields = [
            DataSourceField(label="SMTP Server", value=source_details.get('smtpServer', 'N/A'), sameLine=True),
            DataSourceField(label="Port", value=source_details.get('port', 'N/A'), sameLine=True),
            DataSourceField(label="From Address", value=source_details.get('fromAddress', 'N/A'), sameLine=True)
        ]
    elif data_source.source == "API":
        setup_fields = [
            DataSourceField(label="URL", value=source_details.get('url', 'N/A'), sameLine=True),
            DataSourceField(label="Method", value=source_details.get('method', 'N/A'), sameLine=True),
            DataSourceField(label="Content Type", value=source_details.get('contentType', 'N/A'), sameLine=True)
        ]
    elif data_source.source == "Portal":
        setup_fields = [
            DataSourceField(label="URL", value=source_details.get('url', 'N/A'), sameLine=True),
            DataSourceField(label="Username", value=source_details.get('username', 'N/A'), sameLine=True),
            DataSourceField(label="Script Type", value=source_details.get('scriptType', 'N/A'), sameLine=True)
        ]
    elif data_source.source == "SFTP":
        setup_fields = [
            DataSourceField(label="Connection Code", value=source_details.get('connectionCode', 'N/A'), sameLine=True),
            DataSourceField(label="Path", value=source_details.get('path', 'N/A'), sameLine=True),
            DataSourceField(label="Destination", value=source_details.get('destination', 'N/A'), sameLine=True)
        ]
    
    if setup_fields:
        sections.append(DataSourceSection(
            title="SETUP DETAILS",
            fields=setup_fields
        ))
    
    # Add additional details section - use source_details.details if available, otherwise additional_details
    details_value = source_details.get('details') or data_source.additional_details
    if details_value:
        sections.append(DataSourceSection(
            title="ADDITIONAL DETAILS",
            fields=[
                DataSourceField(label="Details", value=details_value, sameLine=True)
            ]
        ))
    
    # Create edit click configuration
    dropdown_options = DropdownOptions(
        sources=["S3 Bucket", "Email", "Portal", "API Invoke", "SFTP"],
        holidayCalendar=["US Holiday Calendar", "Europe Holiday Calendar"],
        frameDocument=["Capital Call", "Statement", "F3"],
        validusDocument=["NAV Statement", "Statement", "F3"]
    )
    
    form_data = FormData(
        frameDocument=frame_docs,
        validusDocument=validus_docs,
        name=data_source.name,
        source=source_display,
        holidayCalendar=holiday_calendar_display,
        shareName=source_details.get('shareName'),
        connectionString=source_details.get('connectionString'),
        details=details_value
    )
    
    click_action = ClickAction(
        type="putData",
        putAPIURL=f"funds/data_sources/{source_id}",
        actionAfterAPICall={
            "type": "refreshModule",
            "moduleName": "SourcesDetails"
        }
    )
    
    edit_click_data = EditClickData(
        dropdownOptions=dropdown_options,
        open=True,
        buttonType="update",
        formData=form_data,
        clickAction=click_action
    )
    
    on_edit_click = OnEditClick(
        type="openDrawer",
        key="editSourceForm",
        data=edit_click_data
    )
    
    # Create footer with delete button
    footer = Footer(
        fields=[
            FooterField(
                type="button",
                buttonText="Delete source?",
                buttonType="text",
                buttonColor="destructive",
                onConfirmation=OnConfirmation(
                    title="Delete Source",
                    description=f"Are you sure you want to delete {data_source.name} source?",
                    buttonText="Delete Source",
                    buttonColor="destructive",
                    clickAction={
                        "type": "navigation",
                        "to": "/frame",
                        "parameters": [
                            {
                                "key": "page",
                                "value": "FundMasterDetailWithSourceData"
                            }
                        ]
                    }
                )
            )
        ]
    )
    
    return DataSourceDetailResponse(
        title="SOURCE DETAILS",
        isEditable=True,
        onEditClick=on_edit_click,
        sections=sections,
        footer=footer
    )


@router.get("/data_sources/{source_id}", description="Get a specific data source by ID", response_model=DataSourceDetailResponse)
async def get_data_source(
    source_id: int,
    *, __username: str = Depends(authenticate_user)
):
    print("get_data_source", source_id)
    """
    Get detailed information about a specific data source.
    
    **Parameters:**
    - **source_id**: The ID of the data source to retrieve
    """
    data_source = await data_source_service.get_data_source_by_id(source_id)
    
    if not data_source:
        raise HTTPException(
            status_code=404, 
            detail=f"Data source with ID {source_id} not found"
        )
    
    # Transform the data source into the required UI format using Pydantic models
    return _transform_data_source_to_ui_format(data_source, source_id)


@router.get("/source_details", description="Get source details using JSON template")
async def get_source_details(
    source_id: int = Query(..., description="Source ID to retrieve"),
    product: str = Query(..., description="Product type: Frame or Validus"),
    fund_id: Optional[int] = Query(None, description="Optional fund ID for validation"),
    *, __username: str = Depends(authenticate_user)
):
    """
    Get detailed information about a specific data source using JSON template.
    
    **Parameters:**
    - **source_id**: The ID of the data source to retrieve
    - **product**: Product type - must be "Frame" or "Validus"
    - **fund_id**: Optional fund ID to validate source belongs to this fund
    
    **Examples:**
    - `GET /source_details?source_id=1&product=Frame` - Get Frame source details
    - `GET /source_details?source_id=1&product=Validus&fund_id=5` - Get Validus source details with fund validation
    """
    # Validate product parameter
    if product not in ["Frame", "Validus"]:
        raise HTTPException(
            status_code=400,
            detail="Product must be either 'Frame' or 'Validus'"
        )
    
    data_source = await data_source_service.get_data_source_by_id(source_id)
    
    if not data_source:
        raise HTTPException(
            status_code=404, 
            detail=f"Data source with ID {source_id} not found"
        )
    
    # Optional: Validate fund_id if provided
    if fund_id:
        # Validate source belongs to fund
        if data_source.fund_id is None:
            raise HTTPException(
                status_code=400,
                detail=f"Data source with ID {source_id} is not associated with any fund"
            )
        if data_source.fund_id != fund_id:
            raise HTTPException(
                status_code=404,
                detail=f"Data source with ID {source_id} not found for fund {fund_id}"
            )
    
    # Transform the data source into the required UI format using template
    return _transform_data_source_from_template(data_source, source_id, product)


@router.put("/data_sources/{source_id}", description="Edit an existing data source", response_model=EditDataSourceResponse)
async def edit_data_source(
    source_id: int,
    request: EditDataSourceRequest,
    *, __username: str = Depends(authenticate_user)
):
    """
    Edit an existing data source.
    
    **Parameters:**
    - **source_id**: The ID of the data source to edit (path parameter)
    
    **Request Body Example:**
    ```json
    {
        "frameDocuments": ["Capital Call", "Statement"],
        "validusDocuments": ["F3", "Statement"],
        "name": "S3 Setup",
        "source": "S3 Bucket",
        "holidayCalendar": "US Holiday Calendar",
        "s3": {
            "shareName": "name",
            "connectionString": "connection string",
            "details": "details"
        }
    }
    ```
    
    **Parameters:**
    - **name**: Name of the data source (required)
    - **source**: Type of source - must be one of: Email, S3 Bucket, Portal, API, SFTP, API Invoke
    - **holidayCalendar**: Holiday calendar - must be one of: US, Europe, US Holiday Calendar, Europe Holiday Calendar
    - **frameDocuments/frameDocument**: List of Frame documents (optional)
    - **validusDocuments/validusDocument**: List of Validus documents (optional)
    - **s3**: S3 configuration (required if source is "S3 Bucket")
    - **email**: Email configuration (required if source is "Email")
    - **apiInvoke**: API Invoke configuration (required if source is "API Invoke")
    - **portal**: Portal configuration (required if source is "Portal")
    - **sftp**: SFTP configuration (required if source is "SFTP")
    - **additional_details**: Additional details (optional)
    """
    return await data_source_service.edit_data_source(source_id, request, __username)


@router.delete("/data_sources/{source_id}", description="Delete a data source")
async def delete_data_source(
    source_id: int,
    *, __username: str = Depends(authenticate_user)
):
    """
    Delete (soft delete) a data source by ID.
    
    **Parameters:**
    - **source_id**: The ID of the data source to delete
    
    **Example:**
    - `DELETE /funds/data_sources/7`
    
    **Response:**
    ```json
    {
      "success": true,
      "message": "Data source 'S3 Setup' deleted successfully"
    }
    ```
    
    **Note:** This is a soft delete - the record is marked as inactive (is_active=false) but not physically removed from the database.
    """
    return await data_source_service.delete_data_source(source_id)


@router.get("/data_sources_by_fund", description="Get all data sources for a specific fund organized by Frame and Validus")
async def get_data_sources_by_fund(
    fund_id: int = Query(..., description="Fund ID to get data sources for"),
    *, __username: str = Depends(authenticate_user)
):
    """
    Get all data sources for a specific fund, organized by Frame and Validus document types.
    
    **Parameters:**
    - **fund_id**: The ID of the fund to get data sources for
    
    **Returns:**
    ```json
    {
      "data": {
        "rowDataForFrame": [
          {
            "code": "S3#1",
            "frameDocument": ["Capital Call", "F3"],
            "name": "S3 Setup #1",
            "source": "S3 Bucket",
            "holidayCalendar": "US Holiday Calendar",
            "shareName": "xxx",
            "connectionString": "sssss",
            "details": "asdf",
            "document": ["K1", "Statement"]
          }
        ],
        "rowDataForValidus": [
          {
            "code": "Email#1",
            "validusDocument": ["NAV Statement", "F3"],
            "name": "Email Setup #1",
            "source": "Email",
            "holidayCalendar": "US Holiday Calendar",
            "smtpServer": "smtp.gmail.com",
            "port": "587",
            "fromAddress": "noreply@fundmanagement.com",
            "username": "notifications@fundmanagement.com",
            "password": "App@Pass123!",
            "details": "The only email for this fund",
            "document": ["F3", "Statement", "k2"]
          }
        ]
      }
    }
    ```
    """
    return await data_source_service.get_data_sources_by_fund(fund_id)


# ================================
# PORTFOLIO COMPANY ENDPOINTS
# ================================

@router.get("/portfolio_companies", description="Get all portfolio companies under a fund")
async def get_portfolio_companies_by_fund(
    fund_id: int = Query(..., description="Fund ID to retrieve portfolio companies for"),
    *, __username: str = Depends(authenticate_user)
):
    """
    Get all portfolio companies (investors) under a specific fund.
    
    **Parameters:**
    - **fund_id**: The ID of the fund
    
    **Example:**
    - `GET /funds/portfolio_companies?fund_id=1`
    
    **Response:**
    Returns table configuration with portfolio company data:
    ```json
    {
      "rowClickEnabled": true,
      "colsToShow": ["portfolioCompanyID", "portfolioCompanyName", "sector", "geography", "investmentDate", "status"],
      "rowData": [
        {
          "portfolioCompanyID": 1,
          "portfolioCompanyName": "Avitex BioPharma Inc",
          "sector": "Pharma",
          "geography": "USA",
          "investmentDate": "02/06/2025",
          "status": "Invested"
        }
      ]
    }
    ```
    """
    return await portfolio_company_service.get_portfolio_companies_by_fund(fund_id)


@router.post("/portfolio_companies", description="Add new portfolio companies to a fund")
async def add_portfolio_companies(
    fund_id: int = Query(..., description="Fund ID to add portfolio companies to"),
    request: AddPortfolioCompanyRequest = Body(...),
    *, __username: str = Depends(authenticate_user)
):
    """
    Add new portfolio companies (investors) to a fund. Supports bulk creation.
    
    **Parameters:**
    - **fund_id**: The ID of the fund (query parameter)
    - **request**: Portfolio company data (request body)
    
    **Example:**
    - `POST /funds/portfolio_companies?fund_id=1`
    
    **Request Body:**
    ```json
    {
      "portfolio_companies": [
        {
          "company_name": "Avitex BioPharma Inc",
          "sector": ["Energy", "Materials", "Manufacturing"],
          "geography": ["USA"],
          "investment_date": "2025-09-15T18:30:00.000Z",
          "status": "invested"
        },
        {
          "company_name": "Test Company",
          "sector": ["Utilities", "Consumer Discretionary"],
          "geography": ["USA", "India"],
          "investment_date": "2025-10-01T18:30:00.000Z",
          "status": "exited"
        }
      ]
    }
    ```
    
    **Response:**
    ```json
    {
      "success": true,
      "message": "Successfully added 2 portfolio companies to fund",
      "investor_ids": [1, 2]
    }
    ```
    """
    companies = [company.dict() for company in request.portfolio_companies]
    return await portfolio_company_service.add_portfolio_companies(fund_id, companies)


@router.put("/portfolio_companies/{investor_id}", description="Edit a portfolio company")
async def edit_portfolio_company(
    investor_id: int,
    request: EditPortfolioCompanyRequest = Body(...),
    *, __username: str = Depends(authenticate_user)
):
    """
    Edit a portfolio company (investor) details.
    
    **Parameters:**
    - **investor_id**: The ID of the investor/portfolio company to edit
    - **request**: Updated portfolio company data (request body)
    
    **Example:**
    - `PUT /funds/portfolio_companies/1`
    
    **Request Body:**
    ```json
    {
      "portfolio_companies": [
        {
          "company_name": "Avitex BioPharma Inc",
          "sector": ["Energy", "Materials"],
          "geography": ["USA"],
          "investment_date": "2025-09-15T18:30:00.000Z",
          "status": "invested"
        }
      ]
    }
    ```
    
    **Response:**
    ```json
    {
      "success": true,
      "message": "Successfully updated portfolio company 'Avitex BioPharma Inc'"
    }
    ```
    """
    companies = [company.dict() for company in request.portfolio_companies]
    return await portfolio_company_service.edit_portfolio_company(investor_id, companies)


@router.get("/portfolio_companies/details", description="Get portfolio company details")
async def get_portfolio_company_details(
    investor_id: int = Query(..., description="Investor ID to retrieve details for"),
    *, __username: str = Depends(authenticate_user)
):
    """
    Get detailed information about a specific portfolio company.
    
    **Parameters:**
    - **investor_id**: The ID of the investor/portfolio company (query parameter)
    
    **Example:**
    - `GET /funds/portfolio_companies/details?investor_id=1`
    
    **Response:**
    ```json
    {
      "title": "PORTFOLIO COMPANY DETAILS",
      "isEditable": true,
      "sections": [
        {
          "fields": [
            {
              "label": "Portfolio Company ID",
              "value": 1
            },
            {
              "label": "Portfolio Company Name",
              "value": "Avitex BioPharma Inc"
            },
            {
              "label": "Sector",
              "value": "Pharma"
            },
            {
              "label": "Geography",
              "value": "USA"
            },
            {
              "label": "Investment Date",
              "value": "02/08/2025"
            },
            {
              "label": "Status",
              "value": "Invested",
              "type": "status-badge"
            }
          ]
        }
      ]
    }
    ```
    """
    return await portfolio_company_service.get_portfolio_company_details(investor_id)


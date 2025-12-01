"""
Client CRUD API endpoints
Handles all client-related operations including create, read, update, delete, and fund assignments.
"""

from fastapi import APIRouter, HTTPException, Depends, Query, Body
from typing import Optional
import logging
from sqlalchemy import text, case, func
from sqlalchemy.orm import sessionmaker
from datetime import datetime

# Import authentication utilities
from rbac.utils.auth import getCurrentUser
from rbac.utils.frontend import getUserByUsername

# Import database models and utilities
from database_models import DatabaseManager, Client, Fund, User, Role, client_funds, Module
from models.permission_models import RoleOrClientBasedModuleLevelPermission, Master

# Import email service
from utils.email_service import send_client_creation_email

# Import client models
from server.APIServerUtils.client_models import FundAssignmentRequest

# Set up logging
logger = logging.getLogger(__name__)

# Create router
router = APIRouter()

# Authentication dependency
async def authenticate_user(username: str = Depends(getCurrentUser)):
    """Authenticate user using JWT token"""
    return username


def is_admin_user(user_role: str) -> bool:
    """Check if user is an admin"""
    if not user_role:
        return False
    return 'admin' in user_role.lower()




async def get_specific_client_details(conn, client_id: str, user_role: str = "user"):
    """Helper function to get specific client details using SQLAlchemy ORM"""
    try:
        # Create a session
        Session = sessionmaker(bind=conn.engine)
        session = Session()

        try:
            # Get client with fund count and admin user
            admin_user = session.query(User).filter(
                User.client_id == client_id,
                User.role_id == 1,  # Assuming 1 is the admin role ID
                User.is_active == True
            ).first()

            client = session.query(
                Client,
                func.count(client_funds.c.fund_id).label('fund_count'),
                session.query(func.count(User.id))\
                    .join(Role, User.role_id == Role.id)\
                    .filter(
                        Role.role_code == 'fund_manager',
                        User.client_id == Client.id,
                        User.is_active == True
                    ).label('fund_manager_count')
            )\
                .outerjoin(client_funds, Client.id == client_funds.c.client_id)\
                .filter(Client.id == client_id)\
                .group_by(Client.id)\
                .first()

            if not client:
                raise HTTPException(status_code=404, detail="Client not found")

            client_obj, fund_count, fund_manager_count = client

            # Get client data as dict
            client_data = client_obj.to_dict()

            # Get associated funds
            funds = session.query(Fund)\
                .join(client_funds, Fund.id == client_funds.c.fund_id)\
                .filter(client_funds.c.client_id == client_id)\
                .all()

            funds_data = [{
                'name': fund.name,
                'code': fund.code,
                'description': fund.description,
                'is_active': fund.is_active
            } for fund in funds]

            # Get product/module permissions for this client (only enabled ones)
            # Query modules with their parent information
            module_permissions = session.query(
                Module.module_name,
                Module.parent_id
            ).join(
                RoleOrClientBasedModuleLevelPermission,
                Module.id == RoleOrClientBasedModuleLevelPermission.module_id
            ).filter(
                RoleOrClientBasedModuleLevelPermission.client_id == client_id,
                RoleOrClientBasedModuleLevelPermission.client_has_permission == True,
                RoleOrClientBasedModuleLevelPermission.is_active == True,
                Module.is_active == True
            ).all()

            # Get all modules to build parent-child mapping
            all_modules = session.query(Module.id, Module.module_name, Module.parent_id).filter(
                Module.is_active == True
            ).all()
            module_map = {m.id: m.module_name for m in all_modules}
            
            # Build products structure from enabled modules
            # Group modules by parent product
            products_dict = {}
            parent_modules = {}  # Track parent modules
            
            for module_name, parent_id in module_permissions:
                if parent_id is None:
                    # This is a parent module (Frame, NAV Validus)
                    if module_name not in products_dict:
                        products_dict[module_name] = []
                        parent_modules[module_name] = module_name
                else:
                    # This is a child module
                    parent_name = module_map.get(parent_id)
                    if parent_name:
                        if parent_name not in products_dict:
                            products_dict[parent_name] = []
                        if module_name not in products_dict[parent_name]:
                            products_dict[parent_name].append(module_name)
            
            # Default products if none found
            if not products_dict:
                products_dict = {
                    "Frame": ["Dashboard", "File Manager"],
                    "NAV Validus": ["Single Fund", "Multi Fund"]
                }

            # Get master permissions for this client (only enabled ones)
            master_permissions = session.query(
                Master.name
            ).join(
                RoleOrClientBasedModuleLevelPermission,
                Master.id == RoleOrClientBasedModuleLevelPermission.master_id
            ).filter(
                RoleOrClientBasedModuleLevelPermission.client_id == client_id,
                RoleOrClientBasedModuleLevelPermission.client_has_permission == True,
                RoleOrClientBasedModuleLevelPermission.is_active == True,
                Master.is_active == True
            ).all()

            # Create a set of enabled master names for quick lookup
            enabled_masters = {master_name[0] for master_name in master_permissions}

            # Format contact name with title
            contact_full_name = ""
            if client_obj.contact_title or client_obj.contact_first_name or client_obj.contact_last_name:
                contact_full_name = f"{client_obj.contact_title or ''} {client_obj.contact_first_name or ''} {client_obj.contact_last_name or ''}".strip()

            # Format dates
            created_date = client_obj.created_at.strftime("%m/%d/%Y") if client_obj.created_at else "N/A"

            # Prepare client data for response
            client_data = {
                "title": "CLIENT DETAILS",
                "isEditable": True,
                "onEditClick": {
                    "type": "navigation",
                    "to": "/frame",
                    "parameters": [
                        {
                            "key": "page",
                            "value": "EditClient"
                        },
                        {
                            "key": "client_id",
                            "value": client_obj.id
                        }
                    ]
                },
                "sections": [
                    {
                        "fields": [
                            {
                                "label": "Client Name",
                                "value": client_obj.name or "N/A",
                                "sameLine": True
                            },
                            {
                                "label": "Client Type",
                                "value": client_obj.type or "N/A",
                                "sameLine": True
                            },
                            {
                                "label": "Client ID",
                                "value": client_obj.id,
                                "sameLine": True
                            },
                            {
                                "label": "Status",
                                "value": "Active" if client_obj.is_active else "Inactive",
                                "type": "status-badge",
                                "sameLine": True
                            },
                            {
                                "label": "Created Date",
                                "value": created_date,
                                "sameLine": True
                            }
                        ]
                    },
                    {
                        "title": "CONTACT DETAILS",
                        "fields": [
                            {
                                "label": "Contact Name",
                                "value": contact_full_name or "N/A",
                                "sameLine": True
                            },
                            {
                                "label": "Email",
                                "value": client_obj.contact_email or "N/A",
                                "sameLine": True
                            },
                            {
                                "label": "Contact Number",
                                "value": client_obj.contact_number or "N/A",
                                "sameLine": True
                            }
                        ]
                    }
                ],
                "footer": {
                    "fields": [
                        {
                            "type": "button",
                            "buttonText": "Mark as Inactive?" if client_obj.is_active else "Mark as Active?",
                            "buttonType": "text",
                            "buttonColor": "destructive" if client_obj.is_active else None,
                            "onConfirmation": {
                                "title": f"Make Client {'Inactive' if client_obj.is_active else 'Active'}?",
                                "description": f"Are you sure you want to mark {client_obj.name} as {'inactive' if client_obj.is_active else 'active'}?",
                                "buttonText": f"Mark as {'Inactive' if client_obj.is_active else 'Active'}",
                                "buttonColor": "destructive" if client_obj.is_active else None,
                                "clickAction": {
                                    "type": "patchData",
                                    "patchAPIURL": "clients/status",
                                    "data": {
                                        "client_id": client_obj.id,
                                        "active": not client_obj.is_active
                                    },
                                    "actionAfterAPICall": {
                                        "type": "refreshModule",
                                        "moduleName": "ClientInfoDetails"
                                    }
                                }
                            }
                        }
                    ]
                }
            }

            # Return different response format based on user role
            if user_role == 'admin':
                # Admin-specific response format
                admin_response = {
                    "client_name": client_obj.name or "N/A",
                    "client_id": client_obj.id,
                    "first_name": client_obj.contact_first_name or "N/A",
                    "last_name": client_obj.contact_last_name or "N/A",
                    "email": client_obj.contact_email or "N/A",
                    "admin_first_name": admin_user.first_name if admin_user else "",
                    "admin_last_name": admin_user.last_name if admin_user else "",
                    "admin_email": admin_user.email if admin_user else "",
                    "admin_job_title": "Administrator",
                    "admin_status": "Active" if admin_user and admin_user.is_active else "Inactive",
                    "contact_number": client_obj.contact_number or "N/A",
                    "title": client_obj.contact_title or "N/A",
                    "admin_title": client_obj.admin_title or client_obj.contact_title or "N/A",
                    "is_active": "Active" if client_obj.is_active else "Inactive",
                    "admin_is_active": admin_user.is_active if admin_user else False,
                    "created_at": client_obj.created_at.isoformat() if client_obj.created_at else None,
                    "client_type": client_obj.type or "Standard",

                    "products": products_dict,
                    "masters": {
                        "Client Master": {
                            "enabled": "Client Master" in enabled_masters
                        },
                        "Fund Master": {
                            "enabled": "Fund Master" in enabled_masters,
                            "count": fund_count or 0
                        },
                        "Fund Manager Master": {
                            "enabled": "Fund Manager Master" in enabled_masters,
                            "count": fund_manager_count or 0
                        },
                        "Account Master": {
                            "enabled": "Account Master" in enabled_masters
                        },
                        "Process Configuration": {
                            "enabled": "Process Configuration" in enabled_masters,
                            "count": 0  # Default value, update if needed
                        }
                    }
                }
                return {"success": True, "data": admin_response}
            else:
                # Regular user response format
                return {"success": True, "data": client_data}

        finally:
            session.close()

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching client {client_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Error fetching client: {str(e)}")


@router.get("/clients", tags=["Client CRUD API"])
async def get_clients_index(
    page: int = 1,
    page_size: int = 50,
    search: Optional[str] = None,
    status_filter: Optional[str] = None,
    client_id: Optional[str] = None,
    __username: str = Depends(authenticate_user)
):
    
    """
    Get clients in the frontend JSON format for the Client Masters page
    If client_id is provided, returns specific client details.
    Otherwise, returns the complete frontend configuration with:
    - Top navigation bar parameters
    - Sub pages configuration  
    - Module display configuration with table data
    """
    try:
        # Get user role information
        user_data = getUserByUsername(__username)
        user_role = user_data["role"] if user_data else "unknown"
        
        # Initialize database manager
        db_manager = DatabaseManager()
        conn = db_manager.engine.connect()
        
        # If client_id is provided, return specific client details
        if client_id:
            return await get_specific_client_details(conn, client_id, user_role)
        
        # Build the base query for client list
        base_query = """
            SELECT 
                c.id,
                c.code as client_id,
                c.name as client_name,
                c.type as client_type,
                CONCAT(COALESCE(c.contact_first_name, ''), ' ', COALESCE(c.contact_last_name, '')) as contact_name,
                COUNT(cf.fund_id) as funds,
                c.created_at,
                c.is_active
            FROM public.clients c
            LEFT JOIN public.client_funds cf ON c.id = cf.client_id
        """
        
        # Add search and filter conditions
        where_conditions = []
        params = {}
        
        # Always exclude the "all_clients" entry
        where_conditions.append("c.code != 'all_clients'")
        
        if search:
            where_conditions.append("(c.name ILIKE :search OR c.code ILIKE :search OR c.contact_first_name ILIKE :search OR c.contact_last_name ILIKE :search)")
            params['search'] = f"%{search}%"
        
        if status_filter:
            if status_filter.lower() == 'active':
                where_conditions.append("c.is_active = true")
            elif status_filter.lower() == 'inactive':
                where_conditions.append("c.is_active = false")
        
        if where_conditions:
            base_query += " WHERE " + " AND ".join(where_conditions)
        
        base_query += " GROUP BY c.id, c.code, c.name, c.type, c.contact_first_name, c.contact_last_name, c.created_at, c.is_active"
        
        # Get total count for pagination
        count_query = f"SELECT COUNT(*) FROM ({base_query}) as count_query"
        total_result = conn.execute(text(count_query), params)
        total_count = total_result.fetchone()[0]
        
        # Add pagination
        offset = (page - 1) * page_size
        paginated_query = f"{base_query} ORDER BY c.created_at DESC LIMIT :page_size OFFSET :offset"
        params.update({'page_size': page_size, 'offset': offset})
        
        # Execute the query
        result = conn.execute(text(paginated_query), params)
        clients_data = result.fetchall()
        
        # Format the data
        row_data = []
        for row in clients_data:
            row_data.append({
                "client_id": row[0],  # id
                "client_name": row[2],  # name
                "clientType": row[3] or "N/A",  # type
                "contactName": row[4] or "N/A",  # contact_person
                "funds": row[5],  # funds count
                "createdDate": row[6].strftime("%m/%d/%Y") if row[6] else "N/A",  # created_at
                "status": "Active" if row[7] else "Inactive"  # is_active
            })
        
        # Calculate pagination
        total_pages = (total_count + page_size - 1) // page_size
        
        # Build the simplified response
        # Determine rowClickAction based on user role
        if user_role == 'admin':
            row_click_action = {
                "type": "navigation",
                "to": "/frame/client-details",
                "parameters": [
                    {
                        "key": "client_id",
                        "value": "",
                        "dynamicValue": {
                            "enabled": True,
                            "id": "client_id"
                        }
                    },
                    {
                        "key": "client_name",
                        "value": "",
                        "dynamicValue": {
                            "enabled": True,
                            "id": "client_name"
                        }
                    }
                ]
            }
        else:
            row_click_action = {
                "type": "navigation",
                "to": "/frame",
                   "parameters": [
                    {
                        "key": "client_id",
                        "value": "",
                        "dynamicValue": {
                            "enabled": True,
                            "id": "client_id"
                        }
                    },
                    {
                        "key": "client_name",
                        "value": "",
                        "dynamicValue": {
                            "enabled": True,
                            "id": "client_name"
                        }
                    },
                    {
                        "key": "page",
                        "value": "ClientMastersDetails"
                    }                    
                ]
            }
        
        response = {
                                    "rowClickEnabled": True,
                                    "rowClickAction": row_click_action,
                                    "colsToShow": [
                                        "client_id",
                                        "client_name",
                                        "clientType",
                                        "contactName",
                                        "funds",
                                        "createdDate",
                                        "status"
                                    ],
                                    "columnConfig": {
                                        "client_id": {
                                            "name": "Client ID",
                                            "filter": True,
                                            "suppressHeaderMenuButton": False
                                        },
                                        "client_name": {
                                            "name": "Client Name",
                                            "filter": True,
                                            "suppressHeaderMenuButton": False
                                        },
                                        "clientType": {
                                            "name": "Client Type",
                                            "filter": True,
                                            "suppressHeaderMenuButton": False
                                        },
                                        "contactName": {
                                            "name": "Contact Name",
                                            "filter": True,
                                            "suppressHeaderMenuButton": False
                                        },
                                        "funds": {
                                            "name": "Funds",
                                            "filter": True,
                                            "suppressHeaderMenuButton": False
                                        },
                                        "createdDate": {
                                            "name": "Created Date",
                                            "filter": True,
                                            "suppressHeaderMenuButton": False
                                        },
                                        "status": {
                                            "name": "Status",
                                            "filter": True,
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
        
    except Exception as e:
        logger.error(f"Error fetching clients: {e}")
        raise HTTPException(status_code=500, detail=f"Error fetching clients: {str(e)}")


@router.post("/clients/assign_funds", tags=["Client CRUD API"])
async def assign_funds_to_client(
    assignment_data: FundAssignmentRequest,
    __username: str = Depends(authenticate_user)
):
    """
    Assign funds to a client
    
    Payload structure:
    {
        "funds": [
            {
                "fund_id": "FUND0003",
                "fundName": "Vanguard Total Stock...",
                "fundType": "Mutual Fund",
                "contactName": "Ms. Emily Green",
                "baseCurrency": "USD - US Dollar",
                "createdDate": "03/15/2025",
                "status": "Active"
            }
        ],
        "client_id": "CLNT0001"
    }
    """
    try:
        db_manager = DatabaseManager()
        conn = db_manager.engine.connect()
        
        # Validate that client exists
        client_query = "SELECT id FROM public.clients WHERE id = :client_id"
        client_result = conn.execute(text(client_query), {"client_id": assignment_data.client_id}).fetchone()
        
        if not client_result:
            conn.close()
            raise HTTPException(status_code=404, detail="Client not found")
        
        client_db_id = assignment_data.client_id
        
        assigned_funds = []
        skipped_funds = []
        errors = []
        
        for fund_data in assignment_data.funds:
            try:
                fund_code = fund_data.fund_id
                
                # Check if fund exists in database
                fund_query = "SELECT id FROM public.funds WHERE code = :fund_code"
                fund_result = conn.execute(text(fund_query), {"fund_code": fund_code}).fetchone()
                
                if not fund_result:
                    # Create the fund if it doesn't exist
                    fund_name = (fund_data.fundName or fund_code)[:150]  # Truncate to fit VARCHAR(150)
                    fund_type = (fund_data.fundType or "Unknown")[:50]   # Truncate to fit VARCHAR(50)
                    contact_name = (fund_data.contactName or "")[:100]   # Truncate to fit VARCHAR(100)
                    # Truncate base_currency to fit database constraint (VARCHAR(10))
                    base_currency = (fund_data.baseCurrency or "USD")[:10]
                    status = fund_data.status or "Active"
                    
                    # Parse created date
                    created_date = None
                    if fund_data.createdDate:
                        try:
                            # Handle MM/DD/YYYY format
                            created_date = datetime.strptime(fund_data.createdDate, "%m/%d/%Y")
                        except ValueError:
                            # If parsing fails, use current date
                            created_date = datetime.now()
                    
                    # Create fund
                    create_fund_query = """
                        INSERT INTO public.funds (
                            name, code, description, type, contact_person, 
                            base_currency, is_active, created_at, updated_at
                        ) VALUES (
                            :name, :code, :description, :type, :contact_person,
                            :base_currency, :is_active, :created_at, NOW()
                        ) RETURNING id
                    """
                    
                    fund_result = conn.execute(text(create_fund_query), {
                        "name": fund_name,
                        "code": fund_code,
                        "description": f"{fund_type} - {contact_name}",
                        "type": fund_type,
                        "contact_person": contact_name,
                        "base_currency": base_currency,
                        "is_active": status.lower() == "active",
                        "created_at": created_date or datetime.now()
                    })
                    
                    fund_db_id = fund_result.fetchone()[0]
                    
                else:
                    fund_db_id = fund_result[0]
                
                # Check if fund is already assigned to this client
                existing_assignment_query = """
                    SELECT client_id FROM public.client_funds 
                    WHERE client_id = :client_id AND fund_id = :fund_id
                """
                existing = conn.execute(text(existing_assignment_query), {
                    "client_id": client_db_id,
                    "fund_id": fund_db_id
                }).fetchone()
                
                if existing:
                    skipped_funds.append({
                        "fund_id": fund_code,
                        "reason": "Already assigned to client"
                    })
                    continue
                
                # Create client-fund assignment
                assign_fund_query = """
                    INSERT INTO public.client_funds (client_id, fund_id, created_at, updated_at)
                    VALUES (:client_id, :fund_id, NOW(), NOW())
                """
                
                conn.execute(text(assign_fund_query), {
                    "client_id": client_db_id,
                    "fund_id": fund_db_id
                })
                
                assigned_funds.append({
                    "fund_id": fund_code,
                    "fundName": fund_data.fundName or fund_code,
                    "status": "Assigned"
                })
                
            except Exception as e:
                error_msg = f"Error processing fund {fund_data.fund_id}: {str(e)}"
                logger.error(error_msg)
                errors.append(error_msg)
        
        # Commit all changes
        conn.commit()
        conn.close()
        
        return {
            "success": True,
            "message": f"Fund assignment completed. {len(assigned_funds)} funds assigned, {len(skipped_funds)} skipped, {len(errors)} errors.",
            "client_id": assignment_data.client_id,
            "assigned_funds": assigned_funds,
            "skipped_funds": skipped_funds,
            "errors": errors,
            "summary": {
                "total_requested": len(assignment_data.funds),
                "assigned": len(assigned_funds),
                "skipped": len(skipped_funds),
                "errors": len(errors)
            }
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error assigning funds to client: {e}")
        raise HTTPException(status_code=500, detail=f"Error assigning funds to client: {str(e)}")


@router.post("/clients", tags=["Client CRUD API"])
async def create_client(
    client_data: dict,
    __username: str = Depends(authenticate_user)
):
    """
    Create a new client using ORM models
    """
    try:
        db_manager = DatabaseManager()
        session = db_manager.get_session()
        
        # Validate required fields
        required_fields = ["client-name", "admin-first-name", "admin-last-name", "admin-email", "admin-password"]
        for field in required_fields:
            if field not in client_data:
                session.close()
                raise HTTPException(status_code=400, detail=f"Missing required field: {field}")
        
        # Generate client code if not provided (using client name)
        if "client-code" not in client_data or not client_data["client-code"]:
            # Generate client code from client name (remove spaces, convert to lowercase)
            client_code = client_data["client-name"].replace(" ", "").lower()
        else:
            client_code = client_data["client-code"]
        
        # Check if client code already exists using ORM
        existing_client = session.query(Client).filter(Client.code == client_code).first()
        
        if existing_client:
            session.close()
            raise HTTPException(status_code=400, detail="Client code already exists")
        
        # Check if admin email already exists in users table
        existing_user = session.query(User).filter(User.email == client_data["admin-email"]).first()
        if existing_user:
            session.close()
            raise HTTPException(status_code=400, detail="Admin email already exists")
        
        # Create new client using ORM model (without admin fields)
        new_client = Client(
            name=client_data["client-name"],
            code=client_code,
            type=client_data.get("client-type", "individual"),
            contact_title=client_data.get("title"),
            contact_first_name=client_data.get("first-name"),
            contact_last_name=client_data.get("last-name"),
            contact_email=client_data.get("email"),
            contact_number=client_data.get("contact-number"),
            is_active=client_data.get("is-active", True)
        )
        
        session.add(new_client)
        session.flush()  # Flush to get the client ID without committing yet
        session.refresh(new_client)
        
        # Hash the admin password
        import bcrypt
        password_hash = bcrypt.hashpw(client_data["admin-password"].encode('utf-8'), bcrypt.gensalt()).decode('utf-8')
        
        # Generate username from admin email (use part before @)
        admin_username = client_data["admin-email"].split('@')[0]
        
        # Create admin user record
        admin_user = User(
            username=admin_username,
            email=client_data["admin-email"],
            display_name=f"{client_data['admin-first-name']} {client_data['admin-last-name']}",
            first_name=client_data["admin-first-name"],
            last_name=client_data["admin-last-name"],
            job_title=client_data.get("admin-job-title", ""),
            password_hash=password_hash,
            role_id=1,  # Client admin role
            client_id=new_client.id,  # Use the newly created client's ID
            is_active=True
        )
        
        session.add(admin_user)
        session.flush()  # Flush to ensure admin user is added
        
        # Process and save product permissions if provided
        if 'products' in client_data and 'data' in client_data['products']:
            for product_name, product_data in client_data['products']['data'].items():
                if product_data.get('isActive', False):
                    # Try exact match first
                    module = session.query(Module).filter(
                        Module.module_name == product_name,
                        Module.is_active == True
                    ).first()
                    
                    # If not found, try case-insensitive match
                    if not module:
                        module = session.query(Module).filter(
                            func.lower(Module.module_name) == func.lower(product_name),
                            Module.is_active == True
                        ).first()
                    
                    if module:
                        # Create product permission for the client
                        product_permission = RoleOrClientBasedModuleLevelPermission(
                            client_id=new_client.id,
                            module_id=module.id,
                            master_id=None,
                            permission_id=None,  # Explicitly set to None for client-based permissions
                            client_has_permission=True,
                            is_active=True
                        )
                        session.add(product_permission)
                    
                    # Process child modules if any
                    if 'children' in product_data:
                        for child_name, child_data in product_data['children'].items():
                            if child_data.get('isActive', False):
                                child_module = session.query(Module).filter(
                                    Module.module_name == child_name,
                                    Module.is_active == True
                                ).first()
                                
                                if child_module:
                                    child_permission = RoleOrClientBasedModuleLevelPermission(
                                        client_id=new_client.id,
                                        module_id=child_module.id,
                                        master_id=None,
                                        permission_id=None,
                                        client_has_permission=True,
                                        is_active=True
                                    )
                                    session.add(child_permission)
        
        # Process and save master permissions if provided
        if 'masters' in client_data and 'data' in client_data['masters']:
            for master_name, master_data in client_data['masters']['data'].items():
                if master_data.get('isActive', False):
                    master = session.query(Master).filter(
                        Master.name == master_name,
                        Master.is_active == True
                    ).first()
                    
                    if master:
                        master_permission = RoleOrClientBasedModuleLevelPermission(
                            role_id=None,
                            client_id=new_client.id,
                            module_id=None,
                            master_id=master.id,
                            permission_id=None,
                            client_has_permission=True,
                            is_active=True
                        )
                        session.add(master_permission)
                    
                    if 'children' in master_data:
                        for child_name, child_data in master_data['children'].items():
                            if child_data.get('isActive', False):
                                child_master = session.query(Master).filter(
                                    Master.name == child_name,
                                    Master.is_active == True
                                ).first()
                                
                                if child_master:
                                    child_permission = RoleOrClientBasedModuleLevelPermission(
                                        role_id=None,
                                        client_id=new_client.id,
                                        module_id=None,
                                        master_id=child_master.id,
                                        permission_id=None,
                                        client_has_permission=True,
                                        is_active=True
                                    )
                                    session.add(child_permission)
        
        # Validate all permissions before committing
        def validate_permission(permission):
            if permission.client_id and permission.role_id:
                raise ValueError("Cannot set both client_id and role_id")
            if not permission.client_id and not permission.role_id:
                raise ValueError("Either client_id or role_id must be set")
            if permission.client_id and permission.client_has_permission is None:
                raise ValueError("client_has_permission must be set when client_id is set")
            if permission.role_id and not permission.permission_id:
                raise ValueError("permission_id must be set when role_id is set")
            if permission.module_id and permission.master_id:
                raise ValueError("Cannot set both module_id and master_id")
            if not permission.module_id and not permission.master_id:
                raise ValueError("Either module_id or master_id must be set")
            return True

        # Validate all permissions in the session
        for obj in session.new:
            if isinstance(obj, RoleOrClientBasedModuleLevelPermission):
                validate_permission(obj)
        
        # Commit everything in a single transaction
        session.commit()
        session.refresh(new_client)
        session.refresh(admin_user)
        
        new_client_id = new_client.id
        
        logger.info(f"Successfully created client {new_client_id} with admin user and permissions")
        
        # Prepare client data for email notification
        client_data_for_email = {
            "id": new_client.id,
            "name": new_client.name,
            "code": new_client.code,
            "type": new_client.type,
            "is_active": new_client.is_active,
            "contact_title": new_client.contact_title,
            "contact_first_name": new_client.contact_first_name,
            "contact_last_name": new_client.contact_last_name,
            "contact_email": new_client.contact_email,
            "contact_number": new_client.contact_number,
            "admin_title": client_data.get("admin-title", ""),
            "admin_first_name": admin_user.first_name,
            "admin_last_name": admin_user.last_name,
            "admin_email": admin_user.email,
            "admin_job_title": admin_user.job_title
        }
        
        session.close()
        
        # Send email notification (non-blocking - don't fail client creation if email fails)
        try:
            import os
            admin_emails = []
            admin_emails_env = os.getenv('ADMIN_EMAILS', '')
            if admin_emails_env:
                admin_emails = [email.strip() for email in admin_emails_env.split(',') if email.strip()]
            
            email_result = send_client_creation_email(client_data_for_email, admin_emails)
            
            if email_result.get("success"):
                logger.info(f"Client creation email sent successfully for client {new_client_id}")
            else:
                logger.warning(f"Failed to send client creation email for client {new_client_id}: {email_result.get('error', 'Unknown error')}")
                
        except Exception as email_error:
            # Log email error but don't fail the client creation
            logger.error(f"Error sending client creation email for client {new_client_id}: {str(email_error)}")
        
        return {"success": True, "message": "Client created successfully", "client_id": new_client_id}
        
    except HTTPException:
        raise
    except Exception as e:
        if 'session' in locals():
            session.rollback()
            session.close()
        logger.error(f"Error creating client: {e}")
        raise HTTPException(status_code=500, detail=f"Error creating client: {str(e)}")


@router.patch("/clients/status", tags=["Client CRUD API"])
async def update_client_status(
    status_data: dict,
    __username: str = Depends(authenticate_user)
):
    """
    Update client active/inactive status
    """
    try:
        # Validate required fields
        required_fields = ["client_id", "active"]
        for field in required_fields:
            if field not in status_data:
                raise HTTPException(status_code=400, detail=f"Missing required field: {field}")
        
        # Validate active field is boolean
        if not isinstance(status_data["active"], bool):
            raise HTTPException(status_code=400, detail="Field 'active' must be a boolean value")
        
        db_manager = DatabaseManager()
        conn = db_manager.engine.connect()
        
        # Check if client exists
        check_query = "SELECT id FROM public.clients WHERE id = :client_id"
        existing = conn.execute(text(check_query), {"client_id": status_data["client_id"]}).fetchone()
        
        if not existing:
            conn.close()
            raise HTTPException(status_code=404, detail="Client not found")
        
        # Update client status
        update_query = """
            UPDATE public.clients 
            SET is_active = :is_active, updated_at = NOW()
            WHERE id = :client_id
        """
        
        result = conn.execute(text(update_query), {
            "client_id": status_data["client_id"],
            "is_active": status_data["active"]
        })
        
        if result.rowcount == 0:
            conn.close()
            raise HTTPException(status_code=404, detail="Client not found or no changes made")
        
        conn.commit()
        conn.close()
        
        status_text = "activated" if status_data["active"] else "deactivated"
        return {"success": True, "message": f"Client {status_text} successfully"}
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error updating client status: {e}")
        raise HTTPException(status_code=500, detail=f"Error updating client status: {str(e)}")


@router.put("/clients", tags=["Client CRUD API"])
async def update_client(
    client_id: str = Query(..., description="Client ID to update"),
    client_data: dict = Body(...),
    __username: str = Depends(authenticate_user)
):
    """
    Update an existing client
    """
    try:
        db_manager = DatabaseManager()
        session = db_manager.get_session()
        
        try:
            # Check if client exists
            client = session.query(Client).filter(Client.id == client_id).first()
            
            if not client:
                session.close()
                raise HTTPException(status_code=404, detail="Client not found")
            
            # Update client fields
            field_mapping = {
                "client-name": "name",
                "client-type": "type",
                "email": "contact_email",
                "contact-number": "contact_number",
                "title": "contact_title",
                "first-name": "contact_first_name",
                "last-name": "contact_last_name",
                "is-active": "is_active"
            }
            
            for api_field, db_field in field_mapping.items():
                if api_field in client_data:
                    setattr(client, db_field, client_data[api_field])
            
            # Handle contact_person separately - split into first and last name (legacy support)
            if "contact_person" in client_data:
                contact_full_name = client_data["contact_person"]
                if contact_full_name:
                    name_parts = contact_full_name.strip().split(' ', 1)
                    client.contact_first_name = name_parts[0] if name_parts else ""
                    client.contact_last_name = name_parts[1] if len(name_parts) > 1 else ""
                else:
                    client.contact_first_name = None
                    client.contact_last_name = None
            
            # Handle user updates for all users with this client_id
            users_to_update = session.query(User).filter(User.client_id == client_id).all()
            
            if users_to_update:
                from rbac.utils.auth import getPasswordHash
                
                # Update user fields if provided
                for user in users_to_update:
                    # Update first_name and last_name if provided separately
                    if 'admin-first-name' in client_data and client_data['admin-first-name']:
                        user.first_name = client_data['admin-first-name']
                    if 'admin-last-name' in client_data and client_data['admin-last-name']:
                        user.last_name = client_data['admin-last-name']
                    
                    # Update display_name (admin-name) - can be used to update both display_name and split into first/last
                    if 'admin-name' in client_data and client_data['admin-name']:
                        user.display_name = client_data['admin-name']
                        # If first_name/last_name weren't provided separately, split from display_name
                        if 'admin-first-name' not in client_data or not client_data.get('admin-first-name'):
                            name_parts = client_data['admin-name'].strip().split(' ', 1)
                            user.first_name = name_parts[0] if name_parts else ""
                            user.last_name = name_parts[1] if len(name_parts) > 1 else ""
                    
                    # Update email
                    if 'admin-email' in client_data and client_data['admin-email']:
                        user.email = client_data['admin-email']
                    
                    # Update job_title
                    if 'admin-job-title' in client_data and client_data['admin-job-title']:
                        user.job_title = client_data['admin-job-title']
                    
                    # Update password if provided
                    if 'admin-password' in client_data and client_data['admin-password']:
                        user.password_hash = getPasswordHash(client_data['admin-password'])
                        user.temp_password = False
            
            # Handle product/module permissions update
            if 'products' in client_data and 'data' in client_data['products']:
                # Process each product/module individually - update only what's in the request
                for product_name, product_data in client_data['products']['data'].items():
                    # Try exact match first
                    module = session.query(Module).filter(
                        Module.module_name == product_name,
                        Module.is_active == True
                    ).first()
                    
                    # If not found, try case-insensitive match
                    if not module:
                        module = session.query(Module).filter(
                            func.lower(Module.module_name) == func.lower(product_name),
                            Module.is_active == True
                        ).first()
                    
                    if module:
                        # Check if permission exists
                        existing_permission = session.query(RoleOrClientBasedModuleLevelPermission).filter(
                            RoleOrClientBasedModuleLevelPermission.client_id == client_id,
                            RoleOrClientBasedModuleLevelPermission.module_id == module.id,
                            RoleOrClientBasedModuleLevelPermission.master_id.is_(None)
                        ).first()
                        
                        if product_data.get('isActive', False):
                            # Module should be enabled
                            if existing_permission:
                                # Update existing permission
                                existing_permission.client_has_permission = True
                                existing_permission.is_active = True
                            else:
                                # Create new permission
                                product_permission = RoleOrClientBasedModuleLevelPermission(
                                    client_id=client.id,
                                    module_id=module.id,
                                    master_id=None,
                                    permission_id=None,
                                    client_has_permission=True,
                                    is_active=True
                                )
                                session.add(product_permission)
                        else:
                            # Module should be disabled - remove permission
                            if existing_permission:
                                session.delete(existing_permission)
                    
                    # Process child modules if any
                    if 'children' in product_data:
                        for child_name, child_data in product_data['children'].items():
                            child_module = session.query(Module).filter(
                                Module.module_name == child_name,
                                Module.is_active == True
                            ).first()
                            
                            if child_module:
                                # Check if permission exists
                                existing_child_permission = session.query(RoleOrClientBasedModuleLevelPermission).filter(
                                    RoleOrClientBasedModuleLevelPermission.client_id == client_id,
                                    RoleOrClientBasedModuleLevelPermission.module_id == child_module.id,
                                    RoleOrClientBasedModuleLevelPermission.master_id.is_(None)
                                ).first()
                                
                                if child_data.get('isActive', False):
                                    # Child module should be enabled
                                    if existing_child_permission:
                                        existing_child_permission.client_has_permission = True
                                        existing_child_permission.is_active = True
                                    else:
                                        child_permission = RoleOrClientBasedModuleLevelPermission(
                                            client_id=client.id,
                                            module_id=child_module.id,
                                            master_id=None,
                                            permission_id=None,
                                            client_has_permission=True,
                                            is_active=True
                                        )
                                        session.add(child_permission)
                                else:
                                    # Child module should be disabled - remove permission
                                    if existing_child_permission:
                                        session.delete(existing_child_permission)
            
            # Handle master permissions update
            if 'masters' in client_data and 'data' in client_data['masters']:
                # Process each master individually - update only what's in the request
                for master_name, master_data in client_data['masters']['data'].items():
                    master = session.query(Master).filter(
                        Master.name == master_name,
                        Master.is_active == True
                    ).first()
                    
                    if master:
                        # Check if permission exists
                        existing_master_permission = session.query(RoleOrClientBasedModuleLevelPermission).filter(
                            RoleOrClientBasedModuleLevelPermission.client_id == client_id,
                            RoleOrClientBasedModuleLevelPermission.master_id == master.id,
                            RoleOrClientBasedModuleLevelPermission.module_id.is_(None)
                        ).first()
                        
                        if master_data.get('isActive', False):
                            # Master should be enabled
                            if existing_master_permission:
                                # Update existing permission
                                existing_master_permission.client_has_permission = True
                                existing_master_permission.is_active = True
                            else:
                                # Create new permission
                                master_permission = RoleOrClientBasedModuleLevelPermission(
                                    role_id=None,
                                    client_id=client.id,
                                    module_id=None,
                                    master_id=master.id,
                                    permission_id=None,
                                    client_has_permission=True,
                                    is_active=True
                                )
                                session.add(master_permission)
                        else:
                            # Master should be disabled - remove permission
                            if existing_master_permission:
                                session.delete(existing_master_permission)
                    
                    # Process child masters if any
                    if 'children' in master_data:
                        for child_name, child_data in master_data['children'].items():
                            child_master = session.query(Master).filter(
                                Master.name == child_name,
                                Master.is_active == True
                            ).first()
                            
                            if child_master:
                                # Check if permission exists
                                existing_child_master_permission = session.query(RoleOrClientBasedModuleLevelPermission).filter(
                                    RoleOrClientBasedModuleLevelPermission.client_id == client_id,
                                    RoleOrClientBasedModuleLevelPermission.master_id == child_master.id,
                                    RoleOrClientBasedModuleLevelPermission.module_id.is_(None)
                                ).first()
                                
                                if child_data.get('isActive', False):
                                    # Child master should be enabled
                                    if existing_child_master_permission:
                                        existing_child_master_permission.client_has_permission = True
                                        existing_child_master_permission.is_active = True
                                    else:
                                        child_permission = RoleOrClientBasedModuleLevelPermission(
                                            role_id=None,
                                            client_id=client.id,
                                            module_id=None,
                                            master_id=child_master.id,
                                            permission_id=None,
                                            client_has_permission=True,
                                            is_active=True
                                        )
                                        session.add(child_permission)
                                else:
                                    # Child master should be disabled - remove permission
                                    if existing_child_master_permission:
                                        session.delete(existing_child_master_permission)
            
            session.commit()
            session.close()
            
            return {"success": True, "message": "Client updated successfully"}
            
        except HTTPException:
            session.rollback()
            session.close()
            raise
        except Exception as e:
            session.rollback()
            session.close()
            logger.error(f"Error updating client {client_id}: {e}", exc_info=True)
            raise HTTPException(status_code=500, detail=f"Error updating client: {str(e)}")
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error updating client {client_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Error updating client: {str(e)}")


@router.delete("/clients", tags=["Client CRUD API"])
async def delete_client(
    client_id: str = Query(..., description="Client ID to delete"),
    __username: str = Depends(authenticate_user)
):
    """
    Delete a client (soft delete by setting is_active to false)
    """
    try:
        db_manager = DatabaseManager()
        conn = db_manager.engine.connect()
        
        # Check if client exists
        check_query = "SELECT id FROM public.clients WHERE id = :client_id"
        existing = conn.execute(text(check_query), {"client_id": client_id}).fetchone()
        
        if not existing:
            conn.close()
            raise HTTPException(status_code=404, detail="Client not found")
        
        # Soft delete by setting is_active to false
        update_query = "UPDATE public.clients SET is_active = false, updated_at = NOW() WHERE id = :client_id"
        conn.execute(text(update_query), {"client_id": client_id})
        
        conn.commit()
        conn.close()
        
        return {"success": True, "message": "Client deleted successfully"}
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error deleting client {client_id}: {e}")
        raise HTTPException(status_code=500, detail=f"Error deleting client: {str(e)}")


@router.get("/clients/funds", tags=["Client CRUD API"])
async def get_client_funds(
    client_id: str = Query(..., description="Client ID to get funds for"),
    __username: str = Depends(authenticate_user)
):
    """
    Get all funds associated with a client
    """
    try:
        db_manager = DatabaseManager()
        conn = db_manager.engine.connect()
        
        # Validate that client exists
        client_query = "SELECT id FROM public.clients WHERE id = :client_id"
        client_result = conn.execute(text(client_query), {"client_id": client_id}).fetchone()
        
        if not client_result:
            conn.close()
            raise HTTPException(status_code=404, detail="Client not found")
        
        client_db_id = client_id
        
        # Get associated funds
        funds_query = """
            SELECT f.id, f.name, f.code, f.description, f.is_active, f.created_at
            FROM public.funds f
            JOIN public.client_funds cf ON f.id = cf.fund_id
            WHERE cf.client_id = :client_id
            ORDER BY f.created_at DESC
        """
        
        result = conn.execute(text(funds_query), {"client_id": client_db_id})
        funds_data = result.fetchall()
        
        # Transform funds data to match the required table format
        row_data = [
            {
                "fundName": fund[1],  # name
                "fundType": fund[3] if fund[3] else "N/A",  # description as fund type
                "baseCurrency": "USD - US Dollar",  # Default currency
                "action": ""
            }
            for fund in funds_data
        ]
        
        # Calculate pagination
        total_funds = len(row_data)
        page_size = 10
        total_pages = (total_funds + page_size - 1) // page_size if total_funds > 0 else 1
        
        response_data = {
            "colsToShow": ["fundName", "fundType", "baseCurrency", "action"],
            "columnConfig": {
                "fundName": {
                    "name": "Fund Name",
                    "filter": True,
                    "suppressHeaderMenuButton": False
                },
                "fundType": {
                    "name": "Fund Type",
                    "filter": True,
                    "suppressHeaderMenuButton": False
                },
                "baseCurrency": {
                    "name": "Base Currency",
                    "filter": True,
                    "suppressHeaderMenuButton": False
                },
                "action": {
                    "name": "Action",
                    "customCellRenderer": "deleteFundRenderer"
                }
            },
            "rowData": row_data,
            "noDataMessage": "Currently, there are no fund in this client. Kindly add fund",
            "pagination": {
                "current_page": 1,
                "page_size": page_size,
                "total_pages": total_pages
            }
        }
        
        conn.close()
        return response_data
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching client funds {client_id}: {e}")
        raise HTTPException(status_code=500, detail=f"Error fetching client funds: {str(e)}")


@router.delete("/clients/funds", tags=["Client CRUD API"])
async def remove_fund_from_client(
    request_data: dict,
    __username: str = Depends(authenticate_user)
):
    """
    Remove a fund from a client
    """
    try:
        client_id = request_data.get("client_id")
        fund_name = request_data.get("fund_name")
        
        if not client_id or not fund_name:
            raise HTTPException(status_code=400, detail="client_id and fund_name are required")
        
        db_manager = DatabaseManager()
        conn = db_manager.engine.connect()
        
        # Validate that client exists
        client_query = "SELECT id FROM public.clients WHERE id = :client_id"
        client_result = conn.execute(text(client_query), {"client_id": client_id}).fetchone()
        
        if not client_result:
            conn.close()
            raise HTTPException(status_code=404, detail="Client not found")
        
        client_db_id = client_id
        
        # Get fund ID by name
        fund_query = "SELECT id FROM public.funds WHERE name = :fund_name"
        fund_result = conn.execute(text(fund_query), {"fund_name": fund_name}).fetchone()
        
        if not fund_result:
            conn.close()
            raise HTTPException(status_code=404, detail="Fund not found")
        
        fund_db_id = fund_result[0]
        
        # Check if the fund is associated with the client
        association_query = """
            SELECT client_id FROM public.client_funds 
            WHERE client_id = :client_id AND fund_id = :fund_id
        """
        association_result = conn.execute(
            text(association_query), 
            {"client_id": client_db_id, "fund_id": fund_db_id}
        ).fetchone()
        
        if not association_result:
            conn.close()
            raise HTTPException(status_code=404, detail="Fund is not associated with this client")
        
        # Remove the association
        delete_query = """
            DELETE FROM public.client_funds 
            WHERE client_id = :client_id AND fund_id = :fund_id
        """
        conn.execute(text(delete_query), {"client_id": client_db_id, "fund_id": fund_db_id})
        conn.commit()
        
        conn.close()
        
        return {"message": f"Fund '{fund_name}' has been successfully removed from client '{client_id}'"}
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error removing fund from client: {e}")
        raise HTTPException(status_code=500, detail=f"Error removing fund from client: {str(e)}")


@router.get("/clients/edit_form_details", tags=["Client CRUD API"])
async def get_client_edit_form_details(
    client_id: str = Query(..., description="Client ID to get edit form details for"),
    __username: str = Depends(authenticate_user)
):
    """
    Get client edit form details with dynamic data populated
    """
    try:
        db_manager = DatabaseManager()
        conn = db_manager.engine.connect()
        
        # Get client details
        client_query = """
            SELECT 
                c.code,
                c.name,
                c.type,
                c.contact_title,
                c.contact_first_name,
                c.contact_last_name,
                c.contact_email,
                c.contact_number
            FROM public.clients c
            WHERE c.id = :client_id
        """
        
        result = conn.execute(text(client_query), {"client_id": client_id})
        client_row = result.fetchone()
        
        if not client_row:
            conn.close()
            raise HTTPException(status_code=404, detail="Client not found")
        
        # Get user details for this client
        user_query = """
            SELECT 
                u.first_name,
                u.last_name,
                u.email,
                u.display_name,
                u.job_title
            FROM public.users u
            WHERE u.client_id = :client_id
            LIMIT 1
        """
        
        user_result = conn.execute(text(user_query), {"client_id": client_id})
        user_row = user_result.fetchone()
        
        conn.close()
        
        # Build the form response with dynamic data
        form_data = {
            "sections": [
                {
                    "id": "client-details",
                    "title": "CLIENT DETAILS",
                    "idToShow": client_row[0],  # client code
                    "fields": [
                        {
                            "id": "client-name",
                            "label": "Client Name",
                            "placeholder": "Finance Bank National Association",
                            "defaultValue": client_row[1] or "",  # client name
                            "type": "text",
                            "width": "50%",
                            "required": True
                        },
                        {
                            "id": "client-type",
                            "label": "Client Type",
                            "placeholder": "Service Provider",
                            "defaultValue": client_row[2] or "Service Provider",  # client type
                            "type": "select",
                            "width": "50%",
                            "options": ["Service Provider", "Fund Manager"]
                        }
                    ],
                    "withAccordion": False,
                    "width": "100%"
                },
                {
                    "id": "contact-details",
                    "title": "CONTACT DETAILS",
                    "fields": [
                        {
                            "id": "title",
                            "label": "Title",
                            "placeholder": "Ex. Mr.",
                            "defaultValue": client_row[3] or "Mr.",  # contact title
                            "type": "select",
                            "options": ["Mr.", "Ms.", "Mrs."],
                            "width": "12%",
                            "required": True
                        },
                        {
                            "id": "first-name",
                            "label": "First Name",
                            "placeholder": "John",
                            "defaultValue": client_row[4] or "",  # contact first name
                            "type": "text",
                            "width": "38%",
                            "required": True
                        },
                        {
                            "id": "last-name",
                            "label": "Last Name",
                            "placeholder": "Doe",
                            "defaultValue": client_row[5] or "",  # contact last name
                            "type": "text",
                            "width": "50%",
                            "required": True
                        },
                        {
                            "id": "email",
                            "label": "Email",
                            "placeholder": "johndoe@mail.com",
                            "defaultValue": client_row[6] or "",  # contact email
                            "type": "text",
                            "width": "50%",
                            "required": True
                        },
                        {
                            "id": "contact-number",
                            "label": "Contact Number",
                            "placeholder": "+1 (123) 456-7890",
                            "defaultValue": client_row[7] or "",  # contact number
                            "type": "telephone",
                            "width": "50%",
                            "required": True
                        }
                    ],
                    "withAccordion": False,
                    "width": "100%"
                },
                {
                    "id": "admin-details",
                    "title": "ADMIN DETAILS",
                    "fields": [
                        {
                            "id": "admin-name",
                            "label": "Admin Name",
                            "placeholder": "John Doe",
                            "defaultValue": user_row[3] if user_row and user_row[3] else "",  # display_name
                            "type": "text",
                            "width": "50%",
                            "required": False
                        },
                        {
                            "id": "admin-email",
                            "label": "Email",
                            "placeholder": "john.doe@example.com",
                            "defaultValue": user_row[2] if user_row and user_row[2] else "",  # email
                            "type": "text",
                            "width": "50%",
                            "required": False
                        },
                        {
                            "id": "admin-job-title",
                            "label": "Job Title",
                            "placeholder": "Administrator",
                            "defaultValue": user_row[4] if user_row and user_row[4] else "",  # job_title
                            "type": "text",
                            "width": "50%",
                            "required": False
                        },
                        {
                            "id": "admin-password",
                            "label": "Password",
                            "placeholder": "Enter new password",
                            "defaultValue": "",
                            "type": "password",
                            "width": "50%",
                            "required": False
                        }
                    ],
                    "withAccordion": False,
                    "width": "100%"
                }
            ],
            "buttonText": "Update",
            "onConfirmation": {
                "title": "Edit Client",
                "description": f"Are you sure you want to update {client_row[1] or 'this client'}?",  # dynamic client name
                "buttonText": "Update",
                "clickAction": {
                    "type": "putData",
                    "putAPIURL": "clients",
                    "actionAfterAPICall": {
                        "type": "navigation",
                        "to": "/frame",
                        "parameters": [
                            {
                                "key": "page",
                                "value": "ClientMastersDetails"
                            },
                            {
                                "key": "client_id",
                                "value": "",
                                "dynamicValue": {
                                    "enabled": True,
                                    "id": "client_id",
                                    "source": "url"
                                }
                            }
                        ]
                    }
                }
            }
        }
        
        return form_data
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching client edit form details {client_id}: {e}")
        raise HTTPException(status_code=500, detail=f"Error fetching client edit form details: {str(e)}")

@router.get("/clients/view_all_funds", tags=["Client CRUD API"])
async def get_all_funds_for_client(
    client_id: Optional[str] = Query(None, description="Client ID to get all funds for"),
    __username: str = Depends(authenticate_user)
):
    """
    Get all funds associated with a client in the specified JSON format
    - For admin users: client_id must NOT be provided, returns all funds
    - For non-admin users: client_id is required
    """
    try:
        # Get user role information
        user_data = getUserByUsername(__username)
        user_role = user_data.get("role") if user_data else "unknown"
        is_admin = is_admin_user(user_role)

        # Validation logic
        if is_admin and client_id:
            raise HTTPException(
                status_code=400,
                detail="Admin users should not provide client_id parameter. Access /clients/view_all_funds/ without parameters to see all funds."
            )
        if not is_admin and not client_id:
            raise HTTPException(
                status_code=400,
                detail="client_id parameter is required for non-admin users"
            )

        db_manager = DatabaseManager()
        session = db_manager.get_session()

        try:
            # Validate client existence for non-admin
            if client_id:
                client = session.query(Client).filter(Client.id == client_id).first()
                if not client:
                    raise HTTPException(status_code=404, detail="Client not found")

            # Query funds
            if is_admin:
                # For admin, get all funds
                funds = session.query(Fund).order_by(Fund.created_at.desc()).all()
                assigned_fund_ids = set()  # Empty set since we're not checking assignments for admin
            else:
                # For regular users, get all funds and mark those assigned to the client
                # First get the assigned funds
                assigned_funds = session.query(Fund).join(client_funds).filter(client_funds.c.client_id == client_id).all()
                assigned_fund_ids = {fund.id for fund in assigned_funds}
                
                # Then get all funds
                funds = session.query(Fund).order_by(
                    # Order by assignment (assigned first) then creation date
                    case((Fund.id.in_(assigned_fund_ids), 0), else_=1),
                    Fund.created_at.desc()
                ).all()

            # Mapping logic
            fund_type_mapping = {
                "hedge_fund": "Hedge Fund",
                "private_equity": "Private Equity",
                "mutual_fund": "Mutual Fund",
                "etf": "ETF",
                "bond": "Bond Fund",
                "money_market": "Money Market Fund"
            }

            currency_mapping = {
                "usd": "USD - US Dollar",
                "eur": "EUR - Euro",
                "gbp": "GBP - British Pound",
                "jpy": "JPY - Japanese Yen",
                "cad": "CAD - Canadian Dollar",
                "aud": "AUD - Australian Dollar"
            }

            # Transform row data
            row_data = []
            for fund in funds:
                fund_id = fund.code or f"FUND{fund.id:04d}"
                fund_name = fund.name or "Unknown Fund"

                # Fund type detection
                fund_type = fund_type_mapping.get(fund.type.lower() if fund.type else None, 
                                                fund.type if fund.type else "Mutual Fund")
                
                if fund.description:
                    desc = fund.description.lower()
                    if "hedge" in desc:
                        fund_type = "Hedge Fund"
                    elif "private equity" in desc or "pe" in desc:
                        fund_type = "Private Equity"
                    elif "mutual" in desc:
                        fund_type = "Mutual Fund"
                    elif "etf" in desc:
                        fund_type = "ETF"
                    elif "bond" in desc:
                        fund_type = "Bond Fund"
                    elif "money market" in desc:
                        fund_type = "Money Market Fund"

                base_currency = currency_mapping.get(fund.base_currency.lower() if fund.base_currency else None, 
                                                  f"{fund.base_currency.upper()} - {fund.base_currency.upper()}" if fund.base_currency else "USD - US Dollar")

                created_date = fund.created_at.strftime("%m/%d/%Y") if fund.created_at else None
                    
                # Example placeholder contact name generation
                contact_name = f"Mr./Ms. {fund_name.split()[0]} Manager"

                row_data.append({
                    "fund_id": fund_id,
                    "fundName": fund_name,
                    "fundType": fund_type,
                    "contactName": contact_name,
                    "baseCurrency": base_currency,
                    "createdDate": created_date,
                    "status": "Active" if fund.is_active else "Inactive",
                    "isAssigned": fund.id in assigned_fund_ids
                })

            # Pagination info
            total_funds = len(row_data)
            page_size = 10
            total_pages = (total_funds + page_size - 1) // page_size if total_funds else 1

            # Construct new JSON structure
            response_data = {
                "isShowToolTipForTab": True,
                "toolTipMessage": "Select funds to be assigned",
                "colsToShow": [
                    "fund_id",
                    "fundName",
                    "fundType",
                    "contactName",
                    "baseCurrency",
                    "createdDate",
                    "status",
                    "isAssigned"
                ],
                "rowSelection": {"mode": "multiRow"},
                "columnConfig": {
                    "fund_id": {"name": "Fund ID", "filter": True, "suppressHeaderMenuButton": False},
                    "fundName": {"name": "Fund Name", "filter": True, "suppressHeaderMenuButton": False},
                    "fundType": {"name": "Fund Type", "filter": True, "suppressHeaderMenuButton": False},
                    "contactName": {"name": "Contact Name", "filter": True, "suppressHeaderMenuButton": False},
                    "baseCurrency": {"name": "Base Currency", "filter": True, "suppressHeaderMenuButton": False},
                    "createdDate": {"name": "Created Date", "filter": True, "suppressHeaderMenuButton": False},
                    "status": {"name": "Status", "filter": True, "customCellRenderer": "statusAggregator", "suppressHeaderMenuButton": False},
                    "isAssigned": {"name": "Assigned", "filter": True, "suppressHeaderMenuButton": False, "customCellRenderer": "booleanRenderer"}
                },
                "rowData": row_data,
                "pagination": {
                    "current_page": 1,
                    "page_size": page_size,
                    "total_pages": total_pages
                }
            }

            return response_data

        finally:
            session.close()

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching all funds for client {client_id}: {e}")
        raise HTTPException(status_code=500, detail=f"Error fetching all funds for client: {str(e)}")


@router.get("/clients/view_all_fund_managers", tags=["Client CRUD API"])
async def get_all_fund_managers(
    __username: str = Depends(authenticate_user)
):
    """
    Get all fund managers in the system
    - Admin only endpoint - no client_id parameter required
    - Returns all fund managers with their details
    """
    try:
        # Get user role information
        user_data = getUserByUsername(__username)
        user_role = user_data.get("role") if user_data else "unknown"
        is_admin = is_admin_user(user_role)

        # Admin-only validation
        if not is_admin:
            raise HTTPException(
                status_code=403,
                detail="Access denied. This endpoint is only accessible to admin users."
            )

        db_manager = DatabaseManager()
        conn = db_manager.engine.connect()

        # Query all fund managers
        fund_managers_query = """
            SELECT 
                u.id,
                u.username,
                CONCAT(COALESCE(u.first_name, ''), ' ', COALESCE(u.last_name, '')) as full_name,
                u.first_name,
                u.last_name,
                u.email,
                u.is_active,
                u.created_at,
                c.name as client_name
            FROM public.users u
            JOIN public.roles r ON u.role_id = r.id
            LEFT JOIN public.clients c ON u.client_id = c.id
            WHERE r.role_code = 'fund_manager'
            ORDER BY u.created_at DESC
        """
        
        result = conn.execute(text(fund_managers_query))
        fund_managers_data = result.fetchall()
        conn.close()

        # Transform row data
        row_data = []
        for fm in fund_managers_data:
            # Generate fund manager ID
            fund_manager_id = f"FDMG{fm[0]:04d}"
            
            # Fund manager name (use client name or username)
            fund_manager_name = fm[8] if fm[8] else fm[1]
            
            # Contact name - format with title
            first_name = fm[3] or ""
            last_name = fm[4] or ""
            contact_name = f"Mr./Ms. {first_name} {last_name}".strip()
            if contact_name == "Mr./Ms.":
                contact_name = "N/A"
            
            # Created date
            created_date = fm[7].strftime("%m/%d/%Y") if fm[7] else None
            
            # Status
            status = "Active" if fm[6] else "Inactive"

            row_data.append({
                "fundManagerID": fund_manager_id,
                "fundManagerName": fund_manager_name,
                "contactName": contact_name,
                "createdDate": created_date,
                "status": status
            })

        # Pagination info
        total_fund_managers = len(row_data)
        page_size = 10
        total_pages = (total_fund_managers + page_size - 1) // page_size if total_fund_managers else 1

        # Construct response structure
        response_data = {
            "isShowToolTipForTab": True,
            "toolTipMessage": "Select fund managers to be assigned",
            "rowSelection": {"mode": "multiRow"},
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
                    "filter": True,
                    "suppressHeaderMenuButton": False
                },
                "fundManagerName": {
                    "name": "Fund Manager Name",
                    "filter": True,
                    "suppressHeaderMenuButton": False
                },
                "contactName": {
                    "name": "Contact Name",
                    "filter": True,
                    "suppressHeaderMenuButton": False
                },
                "createdDate": {
                    "name": "Created Date",
                    "filter": True,
                    "suppressHeaderMenuButton": False
                },
                "status": {
                    "name": "Status",
                    "filter": True,
                    "customCellRenderer": "statusAggregator",
                    "suppressHeaderMenuButton": False
                }
            },
            "rowData": row_data,
            "pagination": {
                "current_page": 1,
                "page_size": page_size,
                "total_pages": total_pages
            }
        }

        return response_data

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching all fund managers: {e}")
        raise HTTPException(status_code=500, detail=f"Error fetching all fund managers: {str(e)}")


@router.post("/clients/assign_fund_managers", tags=["Client CRUD API"])
async def assign_fund_managers_to_client(
    assignment_data: dict,
    __username: str = Depends(authenticate_user)
):
    """
    Assign fund managers to a client
    - Admin only endpoint - restricted to admin users
    
    Payload structure:
    {
        "fundManagers": [
            {
                "fundManagerID": "FDMG0001",
                "fundManagerName": "Capital X Asset Management Partners",
                "contactName": "Mr. Jack Hammer",
                "createdDate": "02/06/2025",
                "status": "Active"
            }
        ],
        "client_id": "6"
    }
    """
    try:
        # Get user role information and verify admin access
        user_data = getUserByUsername(__username)
        user_role = user_data.get("role") if user_data else "unknown"
        is_admin = is_admin_user(user_role)

        # Admin-only validation
        if not is_admin:
            raise HTTPException(
                status_code=403,
                detail="Access denied. This endpoint is only accessible to admin users."
            )
        
        # Validate required fields
        if "fundManagers" not in assignment_data or "client_id" not in assignment_data:
            raise HTTPException(
                status_code=400, 
                detail="Missing required fields: fundManagers and client_id"
            )
        
        if not assignment_data["fundManagers"]:
            raise HTTPException(
                status_code=400,
                detail="fundManagers list cannot be empty"
            )
        
        db_manager = DatabaseManager()
        conn = db_manager.engine.connect()
        
        try:
            # Validate that client exists
            client_query = "SELECT id, name FROM public.clients WHERE id = :client_id"
            client_result = conn.execute(text(client_query), {"client_id": assignment_data["client_id"]}).fetchone()
            
            if not client_result:
                conn.close()
                raise HTTPException(status_code=404, detail="Client not found")
            
            client_db_id = assignment_data["client_id"]
            client_name = client_result[1]
            
            # Get fund_manager role_id
            role_query = "SELECT id FROM public.roles WHERE role_code = 'fund_manager'"
            role_result = conn.execute(text(role_query)).fetchone()
            
            if not role_result:
                conn.close()
                raise HTTPException(status_code=500, detail="Fund manager role not found in system")
            
            fund_manager_role_id = role_result[0]
            
            assigned_fund_managers = []
            skipped_fund_managers = []
            errors = []
            
            # Process each fund manager
            for index, fm_data in enumerate(assignment_data["fundManagers"]):
                try:
                    fund_manager_id = fm_data.get("fundManagerID")
                    fund_manager_name = fm_data.get("fundManagerName", "Unknown")
                    
                    # Validate fund manager ID exists
                    if not fund_manager_id:
                        errors.append(f"Fund manager at index {index}: ID is required")
                        continue
                    
                    # Extract numeric ID from fundManagerID (e.g., FDMG0001 -> 1)
                    try:
                        numeric_id = int(fund_manager_id.replace("FDMG", ""))
                    except ValueError:
                        errors.append(f"Fund manager {fund_manager_id}: Invalid ID format. Expected format: FDMG0001")
                        continue
                    
                    # Check if fund manager exists and is a fund_manager role
                    fm_query = """
                        SELECT u.id, u.username, u.client_id, u.is_active
                        FROM public.users u
                        JOIN public.roles r ON u.role_id = r.id
                        WHERE u.id = :user_id AND r.role_code = 'fund_manager'
                    """
                    fm_result = conn.execute(text(fm_query), {"user_id": numeric_id}).fetchone()
                    
                    if not fm_result:
                        errors.append(f"Fund manager {fund_manager_id} ({fund_manager_name}): Not found or is not a fund manager")
                        continue
                    
                    fund_manager_db_id = fm_result[0]
                    fund_manager_username = fm_result[1]
                    current_client_id = fm_result[2]
                    is_active = fm_result[3]
                    
                    # Check if fund manager is active
                    if not is_active:
                        skipped_fund_managers.append({
                            "fundManagerID": fund_manager_id,
                            "fundManagerName": fund_manager_name,
                            "reason": "Fund manager is inactive"
                        })
                        continue
                    
                    # Check if already assigned to THIS client
                    if current_client_id == client_db_id:
                        skipped_fund_managers.append({
                            "fundManagerID": fund_manager_id,
                            "fundManagerName": fund_manager_name,
                            "reason": "Already assigned to this client"
                        })
                        continue
                    
                    # Assign fund manager to client by updating their client_id
                    assign_query = """
                        UPDATE public.users 
                        SET client_id = :client_id, updated_at = NOW()
                        WHERE id = :user_id
                    """
                    
                    result = conn.execute(text(assign_query), {
                        "client_id": client_db_id,
                        "user_id": fund_manager_db_id
                    })
                    
                    if result.rowcount == 0:
                        errors.append(f"Fund manager {fund_manager_id}: Failed to update assignment")
                        continue
                    
                    assigned_fund_managers.append({
                        "fundManagerID": fund_manager_id,
                        "fundManagerName": fund_manager_name,
                        "status": "Successfully Assigned"
                    })
                    
                    logger.info(f"Fund manager {fund_manager_id} ({fund_manager_username}) assigned to client {client_db_id} ({client_name})")
                    
                except Exception as e:
                    error_msg = f"Fund manager at index {index}: {str(e)}"
                    logger.error(error_msg)
                    errors.append(error_msg)
            
            # Commit all changes
            conn.commit()
            
            # Prepare response
            response = {
                "success": len(assigned_fund_managers) > 0,
                "message": f"Fund manager assignment completed for client '{client_name}'. {len(assigned_fund_managers)} assigned, {len(skipped_fund_managers)} skipped, {len(errors)} errors.",
                "client_id": assignment_data["client_id"],
                "client_name": client_name,
                "assigned_fund_managers": assigned_fund_managers,
                "skipped_fund_managers": skipped_fund_managers,
                "errors": errors if errors else None,
                "summary": {
                    "total_requested": len(assignment_data["fundManagers"]),
                    "assigned": len(assigned_fund_managers),
                    "skipped": len(skipped_fund_managers),
                    "errors": len(errors)
                }
            }
            
            return response
            
        finally:
            conn.close()
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error assigning fund managers to client: {str(e)}")
        raise HTTPException(
            status_code=500, 
            detail=f"Error assigning fund managers to client: {str(e)}"
        )
    
@router.get("/clients/fund_managers", tags=["Client CRUD API"])
async def get_client_fund_managers(
    client_id: str = Query(..., description="Client ID to get fund managers for"),
    __username: str = Depends(authenticate_user)
):
    """
    Get all fund managers assigned to a specific client
    - Returns all fund managers with their details for the given client_id
    - Works for both admin and non-admin users
    """
    try:
        # Get user role information
        user_data = getUserByUsername(__username)
        user_role = user_data.get("role") if user_data else "unknown"
        is_admin = is_admin_user(user_role)

        # Basic validation - user should have access to their own client data
        # Admin can access any client, non-admin can only access their own
        if not is_admin:
            # Non-admin users should only access their own client
            # This validation can be enhanced based on your business logic
            pass
        
        db_manager = DatabaseManager()
        conn = db_manager.engine.connect()
        
        try:
            # Validate that client exists
            client_query = "SELECT id, name FROM public.clients WHERE id = :client_id"
            client_result = conn.execute(text(client_query), {"client_id": client_id}).fetchone()
            
            if not client_result:
                conn.close()
                raise HTTPException(status_code=404, detail=f"Client with ID {client_id} not found")
            
            client_name = client_result[1]
            
            logger.info(f"Fetching fund managers for client_id: {client_id}, client_name: {client_name}")
            
            # Get fund managers assigned to this client
            # IMPORTANT: Filter by u.client_id = :client_id AND role = 'fund_manager'
            fund_managers_query = """
                SELECT 
                    u.id,
                    u.username,
                    u.first_name,
                    u.last_name,
                    u.email,
                    u.is_active,
                    u.created_at,
                    u.client_id
                FROM public.users u
                JOIN public.roles r ON u.role_id = r.id
                WHERE r.role_code = 'fund_manager' 
                AND u.client_id = :client_id
                ORDER BY u.created_at DESC
            """
            
            result = conn.execute(text(fund_managers_query), {"client_id": client_id})
            fund_managers_data = result.fetchall()
            
            logger.info(f"Found {len(fund_managers_data)} fund managers for client {client_id}")
            
            # Transform fund managers data to match the required table format
            row_data = []
            for fm in fund_managers_data:
                fm_id = fm[0]
                fm_username = fm[1]
                fm_first_name = fm[2] or ""
                fm_last_name = fm[3] or ""
                fm_email = fm[4] or "N/A"
                fm_is_active = fm[5]
                fm_created_at = fm[6]
                fm_client_id = fm[7]
                
                # Generate fund manager ID
                fund_manager_id = f"FDMG{fm_id:04d}"
                
                # Fund manager name - combine first and last name, fallback to username
                full_name = f"{fm_first_name} {fm_last_name}".strip()
                fund_manager_name = full_name if full_name else fm_username
                
                # Contact name - format with title
                contact_name = f"Mr./Ms. {full_name}".strip()
                if contact_name == "Mr./Ms.":
                    contact_name = f"Mr./Ms. {fm_username}"
                
                # Created date
                created_date = fm_created_at.strftime("%m/%d/%Y") if fm_created_at else "N/A"
                
                # Status
                status = "Active" if fm_is_active else "Inactive"
                
                row_data.append({
                    "fundManagerID": fund_manager_id,
                    "fundManagerName": fund_manager_name,
                    "contactName": contact_name,
                    "email": fm_email,
                    "createdDate": created_date,
                    "status": status,
                    "action": ""
                })
            
            # Calculate pagination
            total_fund_managers = len(row_data)
            page_size = 10
            total_pages = (total_fund_managers + page_size - 1) // page_size if total_fund_managers > 0 else 1
            
            response_data = {
                "success": True,
                "client_id": client_id,
                "client_name": client_name,
                "colsToShow": [
                    "fundManagerID",
                    "fundManagerName",
                    "contactName",
                    "email",
                    "createdDate",
                    "status",
                    "action"
                ],
                "columnConfig": {
                    "fundManagerID": {
                        "name": "Fund Manager ID",
                        "filter": True,
                        "suppressHeaderMenuButton": False
                    },
                    "fundManagerName": {
                        "name": "Fund Manager Name",
                        "filter": True,
                        "suppressHeaderMenuButton": False
                    },
                    "contactName": {
                        "name": "Contact Name",
                        "filter": True,
                        "suppressHeaderMenuButton": False
                    },
                    "email": {
                        "name": "Email",
                        "filter": True,
                        "suppressHeaderMenuButton": False
                    },
                    "createdDate": {
                        "name": "Created Date",
                        "filter": True,
                        "suppressHeaderMenuButton": False
                    },
                    "status": {
                        "name": "Status",
                        "filter": True,
                        "customCellRenderer": "statusAggregator",
                        "suppressHeaderMenuButton": False
                    },
                    "action": {
                        "name": "Action",
                        "customCellRenderer": "deleteFundManagerRenderer"
                    }
                },
                "rowData": row_data,
                "noDataMessage": f"Currently, there are no fund managers assigned to {client_name}. Kindly add fund managers.",
                "pagination": {
                    "current_page": 1,
                    "page_size": page_size,
                    "total_pages": total_pages
                }
            }
            
            return response_data
            
        finally:
            conn.close()
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching fund managers for client {client_id}: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Error fetching fund managers for client: {str(e)}")

"""
API endpoint to delete/remove an assigned Fund Manager from a client
Add this endpoint to your existing client CRUD API router
"""

@router.delete("/clients/fund_managers", tags=["Client CRUD API"])
async def delete_client_fund_manager(
    request_data: dict = Body(..., example={
        "fund_manager_id": "FDMG0033",
        "client_id": "2"
    }),
    __username: str = Depends(authenticate_user)
):
    """
    Delete/Remove a fund manager assigned to a specific client
    
    - Removes the fund manager from the client by setting their client_id to NULL
    - Only admin users can perform this operation
    - Validates that the fund manager exists and is actually assigned to the specified client
    - Returns success message with updated fund manager count
    
    Request Body:
        {
            "fund_manager_id": "FDMG0033",
            "client_id": "2"
        }
        
    Returns:
        Success response with message and updated client details
        
    Raises:
        HTTPException 403: If user is not an admin
        HTTPException 404: If client or fund manager not found
        HTTPException 400: If fund manager is not assigned to this client or missing required fields
        HTTPException 500: If database operation fails
    """
    
    # Extract data from request body
    client_id = request_data.get("client_id")
    fund_manager_id = request_data.get("fund_manager_id")
    
    if not client_id or not fund_manager_id:
        raise HTTPException(
            status_code=400,
            detail="Both client_id and fund_manager_id are required in request body"
        )
    try:
        # Get user role information and verify admin access
        user_data = getUserByUsername(__username)
        user_role = user_data.get("role") if user_data else "unknown"
        is_admin = is_admin_user(user_role)
        
        if not is_admin:
            raise HTTPException(
                status_code=403, 
                detail="Access denied. Only administrators can remove fund managers from clients."
            )
        
        # Extract numeric ID from fund_manager_id (format: FDMG####)
        try:
            if not fund_manager_id.startswith("FDMG"):
                raise ValueError("Invalid fund manager ID format")
            numeric_fm_id = int(fund_manager_id[4:])
        except (ValueError, IndexError) as e:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid fund manager ID format. Expected format: FDMG#### (e.g., FDMG0001)"
            )
        
        db_manager = DatabaseManager()
        conn = db_manager.engine.connect()
        
        try:
            # Start a transaction
            trans = conn.begin()
            
            try:
                # Validate that client exists
                client_query = "SELECT id, name FROM public.clients WHERE id = :client_id"
                client_result = conn.execute(text(client_query), {"client_id": client_id}).fetchone()
                
                if not client_result:
                    raise HTTPException(
                        status_code=404, 
                        detail=f"Client with ID {client_id} not found"
                    )
                
                client_name = client_result[1]
                
                # Validate that fund manager exists and belongs to this client
                validate_query = """
                    SELECT u.id, u.username, u.first_name, u.last_name, u.client_id, r.role_code
                    FROM public.users u
                    JOIN public.roles r ON u.role_id = r.id
                    WHERE u.id = :user_id
                """
                
                fm_result = conn.execute(
                    text(validate_query), 
                    {"user_id": numeric_fm_id}
                ).fetchone()
                
                if not fm_result:
                    raise HTTPException(
                        status_code=404,
                        detail=f"Fund Manager with ID {fund_manager_id} not found"
                    )
                
                fm_id, fm_username, fm_first_name, fm_last_name, fm_client_id, role_code = fm_result
                
                # Verify it's actually a fund manager
                if role_code != 'fund_manager':
                    raise HTTPException(
                        status_code=400,
                        detail=f"User {fund_manager_id} is not a fund manager (role: {role_code})"
                    )
                
                # Verify fund manager is assigned to this client
                if fm_client_id != client_id:
                    if fm_client_id is None:
                        raise HTTPException(
                            status_code=400,
                            detail=f"Fund Manager {fund_manager_id} is not assigned to any client"
                        )
                    else:
                        raise HTTPException(
                            status_code=400,
                            detail=f"Fund Manager {fund_manager_id} is not assigned to client {client_id}"
                        )
                
                # Remove fund manager from client by setting client_id to NULL
                update_query = """
                    UPDATE public.users
                    SET client_id = NULL,
                        updated_at = :updated_at
                    WHERE id = :user_id
                """
                
                conn.execute(
                    text(update_query),
                    {
                        "user_id": numeric_fm_id,
                        "updated_at": datetime.utcnow()
                    }
                )
                
                # Get updated fund manager count for this client
                count_query = """
                    SELECT COUNT(*) 
                    FROM public.users u
                    JOIN public.roles r ON u.role_id = r.id
                    WHERE r.role_code = 'fund_manager' 
                    AND u.client_id = :client_id
                    AND u.is_active = true
                """
                
                fm_count_result = conn.execute(
                    text(count_query),
                    {"client_id": client_id}
                ).fetchone()
                
                remaining_fm_count = fm_count_result[0] if fm_count_result else 0
                
                # Commit transaction
                trans.commit()
                
                # Prepare fund manager name for response
                fm_full_name = f"{fm_first_name or ''} {fm_last_name or ''}".strip()
                fm_display_name = fm_full_name if fm_full_name else fm_username
                
                logger.info(
                    f"Successfully removed fund manager {fund_manager_id} ({fm_display_name}) "
                    f"from client {client_id} ({client_name}). "
                    f"Remaining fund managers: {remaining_fm_count}"
                )
                
                return {
                    "success": True,
                    "message": f"Fund Manager {fm_display_name} successfully removed from client {client_name}",
                    "data": {
                        "client_id": client_id,
                        "client_name": client_name,
                        "removed_fund_manager": {
                            "id": fund_manager_id,
                            "name": fm_display_name,
                            "username": fm_username
                        },
                        "remaining_fund_managers": remaining_fm_count
                    }
                }
                
            except HTTPException:
                trans.rollback()
                raise
            except Exception as e:
                trans.rollback()
                logger.error(f"Error removing fund manager {fund_manager_id} from client {client_id}: {str(e)}")
                raise HTTPException(
                    status_code=500,
                    detail=f"Failed to remove fund manager from client: {str(e)}"
                )
                
        finally:
            conn.close()
            
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Unexpected error in delete_client_fund_manager: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"An unexpected error occurred: {str(e)}"
        )
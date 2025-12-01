"""
Accounts API
Provides endpoints for viewing and adding accounts (investors).
"""
import logging
from fastapi import APIRouter, HTTPException, Depends, Body, Query
from typing import Optional
from sqlalchemy import text
from datetime import datetime
from database_models import DatabaseManager
from rbac.utils.auth import getCurrentUser
from typing import List, Optional

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/accounts", tags=["Account API"])

async def get_active_funds() -> List[str]:
    """
    Helper function to get list of active fund names
    """
    try:
        db_manager = DatabaseManager()
        engine = db_manager.engine
        
        query = """
            SELECT name 
            FROM public.funds 
            WHERE is_active = true 
            ORDER BY name
        """
        
        with engine.connect() as conn:
            result = conn.execute(text(query))
            return [row[0] for row in result.fetchall()]
    except Exception as e:
        logger.error(f"Error fetching active funds: {e}")
        return []

async def authenticate_user(username: str = Depends(getCurrentUser)):
    return username

from database_models import Investor


@router.get("/add_account_form")
async def get_add_account_form(
    __username: str = Depends(authenticate_user)
):
    """
    Get the add account form structure with dynamic fund names
    """
    try:
        # Get active funds for the dropdown
        fund_options = await get_active_funds()
        
        # Build the form structure
        form_structure = {
            "sections": [
                {
                    "id": "account-details",
                    "title": "ACCOUNT DETAILS",
                    "fields": [
                        {
                            "id": "account-name",
                            "label": "Account Name",
                            "placeholder": "Ex. Acme Corp",
                            "type": "text",
                            "width": "50%",
                            "required": True
                        },
                        {
                            "id": "account-number",
                            "label": "Account Number (optional)",
                            "placeholder": "Ex. 981236",
                            "type": "text",
                            "width": "50%"
                        },
                        {
                            "id": "fund-name",
                            "label": "Fund Name",
                            "placeholder": "Select Fund",
                            "type": "select",
                            "width": "50%",
                            "options": fund_options
                        },
                        {
                            "id": "account-remark",
                            "label": "Account Remarks (optional)",
                            "placeholder": "Add remarks here",
                            "type": "text-area",
                            "width": "50%"
                        }
                    ],
                    "width": "100%"
                }
            ],
            "buttonText": "Create Account",
            "clickAction": {
                "type": "postData",
                "postAPIURL": "accounts/add_accounts",
                "actionAfterAPICall": {
                    "type": "navigation",
                    "to": "/frame",
                    "parameters": [
                        {
                            "key": "page",
                            "value": "AccountMaster"
                        }
                    ]
                }
            }
        }
        
        return form_structure
    except Exception as e:
        logger.error(f"Error generating add account form: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/view_all_accounts")
async def view_all_accounts(
    page: int = 1,
    page_size: int = 50,
    search: Optional[str] = None,
    status_filter: Optional[str] = None,
    __username: str = Depends(authenticate_user)
):
    """Return accounts in the frontend JSON format similar to view_all_accounts.json attachment, using direct SQL."""
    try:
        db_manager = DatabaseManager()
        engine = db_manager.engine

        # Build base SQL query
        sql_base = """
            SELECT 
                i.id,
                i.investor_name,
                i.account_name,
                i.account_number,
                i.contact_first_name,
                i.contact_last_name,
                i.created_at,
                i.is_active,
                (
                    SELECT string_agg(f.name, ', ')
                    FROM public.fund_investors fi 
                    JOIN public.funds f ON fi.fund_id = f.id
                    WHERE fi.investor_id = i.id
                ) as fund_names,
                (
                    SELECT string_agg(CAST(fi.is_active AS VARCHAR), ', ')
                    FROM public.fund_investors fi 
                    WHERE fi.investor_id = i.id
                ) as fund_active_status
            FROM public.investors i
            WHERE 1=1
        """
        
        # Build where clause and parameters
        params = {}
        if search:
            sql_base += """ 
                AND (
                    i.investor_name ILIKE :search 
                    OR i.account_name ILIKE :search 
                    OR i.account_number ILIKE :search
                    OR i.contact_first_name ILIKE :search
                    OR i.contact_last_name ILIKE :search
                )
            """
            params['search'] = f"%{search}%"

        if status_filter:
            sql_base += " AND i.is_active = :status"
            params['status'] = True if status_filter.lower() == "active" else False

        # Count total records for pagination
        count_sql = f"SELECT COUNT(*) FROM ({sql_base}) as count_query"
        with engine.connect() as conn:
            result = conn.execute(text(count_sql), params)
            total_count = result.scalar()

        # Add pagination
        sql = sql_base + " ORDER BY i.created_at DESC OFFSET :offset LIMIT :limit"
        params['offset'] = (page - 1) * page_size
        params['limit'] = page_size

        # Execute main query
        row_data = []
        with engine.connect() as conn:
            result = conn.execute(text(sql), params)
            for row in result:
                contact_name = f"{row.contact_first_name or ''} {row.contact_last_name or ''}".strip()
                created_str = row.created_at.strftime("%m/%d/%Y") if row.created_at else ""
                status_val = "Active" if row.is_active else "Inactive"

                # Get fund names
                fund_names = row.fund_names or ""
                
                row_data.append({
                    "accountId": row.id,  # Now an integer as requested
                    "accountName": row.investor_name or row.account_name or "",
                    "accountNumber": row.account_number or "",
                    "fund": fund_names,
                    "createdDate": created_str,
                    "status": status_val
                })

        total_pages = (total_count + page_size - 1) // page_size

        response = {
            "rowClickEnabled": True,
            "rowClickAction": {
                "type": "storeValues",
                "store": {
                    "key": "commonRightDrawer",
                    "value": {
                        "page": "detailedInfo",
                        "getAPIURL": "accounts/get_account_details",
                        "parameters": [
                            {
                                "name": "account_id",
                                "value": "",
                                "dynamicValue": {
                                    "enabled": True,
                                    "id": "accountId"
                                }
                            }
                        ],
                        "isOpen": True
                    }
                }
            },
            "colsToShow": [
                "accountId",
                "accountName", 
                "accountNumber",
                "fund",
                "createdDate",
                "status"
            ],
            "columnConfig": {
                "accountId": {"name": "Account ID", "filter": True, "suppressHeaderMenuButton": False},
                "accountName": {"name": "Account Name", "filter": True, "suppressHeaderMenuButton": False},
                "accountNumber": {"name": "Account Number", "filter": True, "cellStyle": {"textAlign": "right", "paddingRight": "14px"}, "headerClass": "ag-right-align-header", "suppressHeaderMenuButton": False},
                "fund": {"name": "Fund", "filter": True, "suppressHeaderMenuButton": False},
                "createdDate": {"name": "Created Date", "filter": True, "cellStyle": {"textAlign": "right", "paddingRight": "14px"}, "headerClass": "ag-right-align-header", "suppressHeaderMenuButton": False},
                "status": {"name": "Status", "filter": True, "customCellRenderer": "statusAggregator", "suppressHeaderMenuButton": False}
            },
            "rowData": row_data,
            "pagination": {
                "current_page": page,
                "page_size": page_size,
                "total_pages": total_pages
            }
        }

        return response

    except Exception as e:
        logger.error(f"Error fetching accounts for view_all_accounts (investors): {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/get_account_details")
async def get_account_details(
    account_id: str = Query(..., description="The ID of the account to retrieve"),
    __username: str = Depends(authenticate_user)
):
    """
    Get detailed information for a specific account.
    Returns account details in the frontend JSON format similar to account_details.json.
    """
    try:
        db_manager = DatabaseManager()
        engine = db_manager.engine

        # Query to get account details with fund information
        sql = text("""
            SELECT 
                i.id,
                i.investor_name,
                i.account_name,
                i.account_number,
                i.notes,
                i.is_active,
                i.created_at,
                i.contact_first_name,
                i.contact_last_name,
                (
                    SELECT string_agg(f.name, ', ')
                    FROM public.fund_investors fi 
                    JOIN public.funds f ON fi.fund_id = f.id
                    WHERE fi.investor_id = i.id
                ) as fund_names,
                (
                    SELECT string_agg(CAST(fi.is_active AS VARCHAR), ', ')
                    FROM public.fund_investors fi 
                    WHERE fi.investor_id = i.id
                ) as fund_active_status
            FROM public.investors i
            WHERE i.id = :account_id
        """)

        with engine.connect() as conn:
            result = conn.execute(sql, {"account_id": account_id}).fetchone()
            
            if not result:
                raise HTTPException(
                    status_code=404, 
                    detail=f"Account with ID {account_id} not found"
                )

        # Format the response according to the frontend requirements
        account_name = result.investor_name or result.account_name or ""
        created_date = result.created_at.strftime("%m/%d/%Y") if result.created_at else ""
        status = "Active" if result.is_active else "Inactive"
        remarks = result.notes if result.notes else "-"
        # Get fund names and their active status
        fund_names = result.fund_names if result.fund_names else ""
        fund_active_status = result.fund_active_status if hasattr(result, 'fund_active_status') else ""
        
        # Format fund names with inactive indicator if needed
        if fund_names:
            fund_names_list = fund_names.split(', ')
            fund_active_list = fund_active_status.split(', ') if fund_active_status else []
            
            formatted_fund_names = []
            for i, name in enumerate(fund_names_list):
                is_active = i < len(fund_active_list) and fund_active_list[i] == 'True'
                if not is_active:
                    name = f"{name} (inactive)"
                formatted_fund_names.append(name)
            
            fund_names = ", ".join(formatted_fund_names) if formatted_fund_names else "-"
        else:
            fund_names = "-"

        # Generate account ID format (e.g., ACNT0011)
        formatted_account_id = account_id

        response = {
            "title": "ACCOUNT DETAILS",
            "isEditable": True,
            "onEditClick": {
                "type": "navigation",
                "to": "/frame",
                "parameters": [
                    {
                        "key": "page",
                        "value": "EditAccountMaster"
                    },
                    {
                        "key": "account_id",
                        "value": account_id
                    }
                ]
            },
            "sections": [
                {
                    "fields": [
                        {
                            "label": "Account Name",
                            "value": account_name,
                            "sameLine": True
                        },
                        {
                            "label": "Account ID",
                            "value": formatted_account_id,
                            "sameLine": True
                        },
                        {
                            "label": "Fund Name",
                            "value": fund_names,
                            "sameLine": True
                        },
                        {
                            "label": "Account Remarks",
                            "value": remarks,
                            "sameLine": True
                        },
                        {
                            "label": "Status",
                            "value": status,
                            "type": "status-badge",
                            "sameLine": True
                        },
                        {
                            "label": "Created Date",
                            "value": created_date,
                            "sameLine": True
                        }
                    ]
                }
            ],
            "footer": {
                "fields": [
                    {
                        "type": "button",
                        "buttonText": f"Mark as {'Inactive' if result.is_active else 'Active'}?",
                        "buttonType": "text",
                        "buttonColor": "destructive" if result.is_active else "primary",
                        "onConfirmation": {
                            "title": f"Make Account {'Inactive' if result.is_active else 'Active'}?",
                            "description": f"Are you sure you want to mark account {account_name} as {'inactive' if result.is_active else 'active'}?",
                            "buttonText": f"Mark as {'Inactive' if result.is_active else 'Active'}",
                            "buttonColor": "destructive" if result.is_active else "primary",
                            "clickAction": {
                                "type": "patchData",
                                "patchAPIURL": "accounts/update_account_status",
                                "data": {
                                    "accountId": int(account_id),
                                    "active": not result.is_active
                                },
                                "actionAfterAPICall": {
                                    "type": "refreshModule",
                                    "moduleName": "AccountMastersTable"
                                }
                            }
                        }
                    }
                ]
            }
        }

        # Add additional contact information if available
        if result.contact_first_name or result.contact_last_name:
            contact_name = f"{result.contact_first_name or ''} {result.contact_last_name or ''}".strip()
            # Add contact section
            response["sections"].append({
                "title": "Contact Information",
                "fields": [
                    {
                        "label": "Contact Name",
                        "value": contact_name,
                        "sameLine": True
                    }
                ]
            })

        logger.info(f"Successfully fetched details for account {account_id}")
        return response

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching account details: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/add_accounts")
async def add_accounts(
    payload: dict = Body(...),
    __username: str = Depends(authenticate_user)
):
    """
    Add a new account (investor) with the provided details.
    Payload structure:
    {
        "account-name": "Apex Investors",
        "account-number": "981236",
        "fund-id": 123,
        "account-remark": "Remarks"
    }
    """
    try:
        db_manager = DatabaseManager()
        engine = db_manager.engine

        # Extract payload data
        account_name = payload.get("account-name")
        account_number = payload.get("account-number")
        fund_name = payload.get("fund-name")
        account_remark = payload.get("account-remark")

        # Validate required fields
        if not account_name:
            raise HTTPException(status_code=400, detail="account-name is required")
        if not account_number:
            raise HTTPException(status_code=400, detail="account-number is required")

        with engine.begin() as conn:
            # Check if account number already exists
            check_sql = text("""
                SELECT id FROM public.investors 
                WHERE account_number = :account_number
            """)
            existing = conn.execute(check_sql, {"account_number": account_number}).fetchone()
            
            if existing:
                raise HTTPException(
                    status_code=400, 
                    detail=f"Account number {account_number} already exists"
                )

            # Insert new investor (using 'notes' column for remarks)
            insert_sql = text("""
                INSERT INTO public.investors 
                (investor_name, account_name, account_number, notes, is_active, created_at, updated_at)
                VALUES (:investor_name, :account_name, :account_number, :notes, true, :created_at, :updated_at)
                RETURNING id
            """)
            
            result = conn.execute(insert_sql, {
                "investor_name": account_name,
                "account_name": account_name,
                "account_number": account_number,
                "notes": account_remark,
                "created_at": datetime.utcnow(),
                "updated_at": datetime.utcnow()
            })
            
            new_investor_id = result.fetchone()[0]

            # If fund name is provided, look up the fund and link investor to it
            if fund_name:
                # Check if fund exists and is active
                fund_sql = text("""
                    SELECT id FROM public.funds 
                    WHERE name = :fund_name AND is_active = true
                """)
                fund_result = conn.execute(fund_sql, {"fund_name": fund_name}).fetchone()
                
                if fund_result:
                    fund_id = fund_result[0]
                    # Link investor to fund
                    link_sql = text("""
                        INSERT INTO public.fund_investors 
                        (fund_id, investor_id, is_active, created_at, updated_at)
                        VALUES (:fund_id, :investor_id, true, :created_at, :updated_at)
                    """)
                    
                    conn.execute(link_sql, {
                        "fund_id": fund_id,
                        "investor_id": new_investor_id,
                        "created_at": datetime.utcnow(),
                        "updated_at": datetime.utcnow()
                    })
                else:
                    logger.warning(f"Fund with name '{fund_name}' not found or is inactive, investor created without fund linkage")

        logger.info(f"Successfully created account {account_name} with ID {new_investor_id}")
        
        response_data = {
            "success": True,
            "message": "Account created successfully",
            "data": {
                "account_id": new_investor_id,
                "account_name": account_name,
                "account_number": account_number,
                "notes": account_remark
            }
        }
        
        # Only add fund_id to response if a fund was linked
        if fund_id is not None:
            response_data["data"]["fund_id"] = fund_id
        
        return response_data

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error creating account: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    

@router.patch("/update_account_status")
async def update_account_status(
    payload: dict = Body(...),
    __username: str = Depends(authenticate_user)
):
    """
    Update account status (active/inactive).
    Payload structure:
    {
        "accountId": "ACNT0001",
        "active": true
    }
    """
    try:
        db_manager = DatabaseManager()
        engine = db_manager.engine

        # Extract payload data
        account_id = payload.get("accountId")
        is_active = payload.get("active")

        # Validate required fields
        if not account_id:
            raise HTTPException(status_code=400, detail="accountId is required")
        
        if is_active is None:
            raise HTTPException(status_code=400, detail="active field is required")

        # Validate active is boolean
        if not isinstance(is_active, bool):
            raise HTTPException(status_code=400, detail="active field must be a boolean (true/false)")

        with engine.begin() as conn:
            # Check if account exists
            check_sql = text("""
                SELECT id, investor_name, is_active 
                FROM public.investors 
                WHERE id = :account_id
            """)
            existing = conn.execute(check_sql, {"account_id": account_id}).fetchone()
            
            if not existing:
                raise HTTPException(
                    status_code=404, 
                    detail=f"Account with ID {account_id} not found"
                )

            # Check if status is already the same
            if existing.is_active == is_active:
                status_text = "active" if is_active else "inactive"
                return {
                    "success": True,
                    "message": f"Account is already {status_text}",
                    "data": {
                        "account_id": account_id,
                        "account_name": existing.investor_name,
                        "active": is_active
                    }
                }

            # Update account status
            update_sql = text("""
                UPDATE public.investors 
                SET is_active = :is_active,
                    updated_at = :updated_at
                WHERE id = :account_id
            """)
            
            conn.execute(update_sql, {
                "is_active": is_active,
                "updated_at": datetime.utcnow(),
                "account_id": account_id
            })

            # Also update fund_investors relationship if exists
            update_fund_sql = text("""
                UPDATE public.fund_investors 
                SET is_active = :is_active,
                    updated_at = :updated_at
                WHERE investor_id = :account_id
            """)
            
            conn.execute(update_fund_sql, {
                "is_active": is_active,
                "updated_at": datetime.utcnow(),
                "account_id": account_id
            })

        status_text = "activated" if is_active else "deactivated"
        logger.info(f"Successfully {status_text} account {account_id} ({existing.investor_name})")
        
        return {
            "success": True,
            "message": f"Account {status_text} successfully",
            "data": {
                "account_id": account_id,
                "account_name": existing.investor_name,
                "active": is_active,
                "updated_at": datetime.utcnow().isoformat()
            }
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error updating account status: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.put("/edit_account/{account_id}")
async def edit_account(
    account_id: str,
    payload: dict = Body(...),
    __username: str = Depends(authenticate_user)
):
    """
    Edit an existing account (investor) with the provided details.
    Payload structure:
    {
        "account-name": "Apex Investors",
        "account-number": "981236",
        "fund-name": "Great Hill Partners",
        "account-remark": "Updated remarks"
    }
    """
    try:
        db_manager = DatabaseManager()
        engine = db_manager.engine

        # Extract payload data
        account_name = payload.get("account-name")
        account_number = payload.get("account-number")
        fund_name = payload.get("fund-name")
        account_remark = payload.get("account-remark")

        # Validate at least one field is provided for update
        if not any([account_name, account_number, fund_name, account_remark is not None]):
            raise HTTPException(
                status_code=400, 
                detail="At least one field must be provided for update"
            )

        with engine.begin() as conn:
            # Check if account exists
            check_sql = text("""
                SELECT id, investor_name, account_number, notes 
                FROM public.investors 
                WHERE id = :account_id
            """)
            existing = conn.execute(check_sql, {"account_id": account_id}).fetchone()
            
            if not existing:
                raise HTTPException(
                    status_code=404, 
                    detail=f"Account with ID {account_id} not found"
                )

            # If account_number is being changed, check if new number already exists
            if account_number and account_number != existing.account_number:
                duplicate_check_sql = text("""
                    SELECT id FROM public.investors 
                    WHERE account_number = :account_number AND id != :account_id
                """)
                duplicate = conn.execute(duplicate_check_sql, {
                    "account_number": account_number,
                    "account_id": account_id
                }).fetchone()
                
                if duplicate:
                    raise HTTPException(
                        status_code=400, 
                        detail=f"Account number {account_number} already exists for another account"
                    )

            # Build dynamic update query based on provided fields
            update_fields = []
            update_params = {"account_id": account_id, "updated_at": datetime.utcnow()}
            
            if account_name is not None:
                update_fields.append("investor_name = :investor_name")
                update_fields.append("account_name = :account_name")
                update_params["investor_name"] = account_name
                update_params["account_name"] = account_name
            
            if account_number is not None:
                update_fields.append("account_number = :account_number")
                update_params["account_number"] = account_number
            
            if account_remark is not None:
                update_fields.append("notes = :notes")
                update_params["notes"] = account_remark
            
            update_fields.append("updated_at = :updated_at")
            
            # Execute update query
            if update_fields:
                update_sql = text(f"""
                    UPDATE public.investors 
                    SET {', '.join(update_fields)}
                    WHERE id = :account_id
                """)
                
                conn.execute(update_sql, update_params)

            # Handle fund association update
            if fund_name is not None:
                # First, deactivate all existing fund associations for this investor
                deactivate_sql = text("""
                    UPDATE public.fund_investors 
                    SET is_active = false, updated_at = :updated_at
                    WHERE investor_id = :investor_id
                """)
                conn.execute(deactivate_sql, {
                    "investor_id": account_id,
                    "updated_at": datetime.utcnow()
                })
                
                # If fund_name is not empty, create or activate the new association
                if fund_name:
                    # Find the fund
                    fund_sql = text("""
                        SELECT id FROM public.funds 
                        WHERE name = :fund_name AND is_active = true
                    """)
                    fund_result = conn.execute(fund_sql, {"fund_name": fund_name}).fetchone()
                    
                    if fund_result:
                        fund_id = fund_result[0]
                        
                        # Check if association already exists
                        existing_assoc_sql = text("""
                            SELECT id FROM public.fund_investors 
                            WHERE fund_id = :fund_id AND investor_id = :investor_id
                        """)
                        existing_assoc = conn.execute(existing_assoc_sql, {
                            "fund_id": fund_id,
                            "investor_id": account_id
                        }).fetchone()
                        
                        if existing_assoc:
                            # Reactivate existing association
                            reactivate_sql = text("""
                                UPDATE public.fund_investors 
                                SET is_active = true, updated_at = :updated_at
                                WHERE fund_id = :fund_id AND investor_id = :investor_id
                            """)
                            conn.execute(reactivate_sql, {
                                "fund_id": fund_id,
                                "investor_id": account_id,
                                "updated_at": datetime.utcnow()
                            })
                        else:
                            # Create new association
                            create_assoc_sql = text("""
                                INSERT INTO public.fund_investors 
                                (fund_id, investor_id, is_active, created_at, updated_at)
                                VALUES (:fund_id, :investor_id, true, :created_at, :updated_at)
                            """)
                            conn.execute(create_assoc_sql, {
                                "fund_id": fund_id,
                                "investor_id": account_id,
                                "created_at": datetime.utcnow(),
                                "updated_at": datetime.utcnow()
                            })
                    else:
                        logger.warning(f"Fund '{fund_name}' not found during account edit")

            # Fetch updated account details for response
            fetch_sql = text("""
                SELECT 
                    i.id,
                    i.investor_name,
                    i.account_number,
                    i.notes,
                    (
                        SELECT string_agg(f.name, ', ')
                        FROM public.fund_investors fi 
                        JOIN public.funds f ON fi.fund_id = f.id
                        WHERE fi.investor_id = i.id AND fi.is_active = true
                    ) as fund_names
                FROM public.investors i
                WHERE i.id = :account_id
            """)
            
            updated_account = conn.execute(fetch_sql, {"account_id": account_id}).fetchone()

        logger.info(f"Successfully updated account {account_id}")
        
        return {
            "success": True,
            "message": "Account updated successfully",
            "data": {
                "account_id": account_id,
                "account_name": updated_account.investor_name,
                "account_number": updated_account.account_number,
                "fund_name": updated_account.fund_names or "",
                "notes": updated_account.notes or ""
            }
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error updating account: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/get_account_for_edit")
async def get_account_for_edit(
    account_id: str = Query(..., description="The ID of the account to retrieve for editing"),
    __username: str = Depends(authenticate_user)
):
    """
    Get account information in the format needed for the edit form.
    Returns data matching the edit_account_form_details.json structure.
    """
    try:
        db_manager = DatabaseManager()
        engine = db_manager.engine

        # Query to get account details for editing
        sql = text("""
            SELECT 
                i.id,
                i.investor_name,
                i.account_number,
                i.notes,
                (
                    SELECT f.name
                    FROM public.fund_investors fi 
                    JOIN public.funds f ON fi.fund_id = f.id
                    WHERE fi.investor_id = i.id AND fi.is_active = true
                    LIMIT 1
                ) as fund_name
            FROM public.investors i
            WHERE i.id = :account_id
        """)

        with engine.connect() as conn:
            result = conn.execute(sql, {"account_id": account_id}).fetchone()
            
            if not result:
                raise HTTPException(
                    status_code=404, 
                    detail=f"Account with ID {account_id} not found"
                )

        # Format the account ID for display (e.g., ACNT0011)
        formatted_account_id = f"ACNT{int(account_id):04d}"
        
        # Get list of all funds for the dropdown
        funds_sql = text("""
            SELECT name FROM public.funds WHERE is_active = true ORDER BY name
        """)
        
        with engine.connect() as conn:
            funds_result = conn.execute(funds_sql)
            fund_options = [row[0] for row in funds_result]

        # Build the response according to the required format
        response = {
            "sections": [
                {
                    "id": "account-details",
                    "title": "ACCOUNT DETAILS",
                    "idToShow": formatted_account_id,
                    "fields": [
                        {
                            "id": "account-name",
                            "label": "Account Name",
                            "placeholder": "Ex. Acme Corp",
                            "defaultValue": result.investor_name or "",
                            "type": "text",
                            "width": "50%",
                            "required": True
                        },
                        {
                            "id": "account-number",
                            "label": "Account Number (optional)",
                            "defaultValue": result.account_number or "",
                            "placeholder": "Ex. 981236",
                            "type": "text",
                            "width": "50%"
                        },
                        {
                            "id": "fund-name",
                            "label": "Fund Name",
                            "placeholder": "Select Fund",
                            "defaultValue": result.fund_name or "",
                            "type": "select",
                            "width": "50%",
                            "options": fund_options
                        },
                        {
                            "id": "account-remark",
                            "label": "Account Remarks (optional)",
                            "defaultValue": result.notes or "",
                            "placeholder": "Add remarks here",
                            "type": "text-area",
                            "width": "50%"
                        }
                    ],
                    "width": "100%"
                }
            ],
            "buttonText": "Update Account",
            "onConfirmation": {
                "title": "Edit Account",
                "description": f"Are you sure you want to update {result.investor_name or 'this account'}?",
                "buttonText": "Update",
                "clickAction": {
                    "type": "putData",
                    "putAPIURL": f"accounts/edit_account/{account_id}",
                    "actionAfterAPICall": {
                        "type": "navigation",
                        "to": "/frame",
                        "parameters": [
                            {
                                "key": "page",
                                "value": "AccountMaster"
                            }
                        ]
                    }
                }
            }
        }

        logger.info(f"Successfully fetched account {account_id} for editing")
        return response

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching account for edit: {e}")
        raise HTTPException(status_code=500, detail=str(e))
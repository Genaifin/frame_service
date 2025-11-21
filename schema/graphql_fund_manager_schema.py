#!/usr/bin/env python3
"""
GraphQL Schema for Fund Manager Management - Clean Implementation with Authentication
Provides pure data responses without response formatting
Maintains consistency with REST API authentication system
"""

import strawberry
from typing import List, Optional
from strawberry.types import Info
from sqlalchemy.orm import joinedload
from sqlalchemy import or_
from database_models import FundManager, get_database_manager
from datetime import datetime
import logging

# Import authentication context
from .graphql_auth_context import require_authentication, require_role, get_current_user, is_authenticated

logger = logging.getLogger(__name__)

@strawberry.type
class FundManagerType:
    """GraphQL type for Fund Manager"""
    id: int
    fund_manager_name: str
    contact_title: Optional[str]
    contact_first_name: str
    contact_last_name: str
    contact_email: str
    contact_number: Optional[str]
    status: str
    created_at: str
    updated_at: str

@strawberry.input
class FundManagerCreateInput:
    """Input type for creating a fund manager"""
    fund_manager_name: str
    title: Optional[str] = None
    first_name: str
    last_name: str
    email: str
    contact_number: Optional[str] = None

@strawberry.input
class FundManagerUpdateInput:
    """Input type for updating a fund manager"""
    fund_manager_name: Optional[str] = None
    title: Optional[str] = None
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    email: Optional[str] = None
    contact_number: Optional[str] = None
    status: Optional[str] = None

@strawberry.input
class FundManagerStatusInput:
    """Input type for updating fund manager status"""
    fund_manager_id: int
    status: str

@strawberry.type
class FundManagerQuery:
    """GraphQL Query root for fund managers"""
    
    @strawberry.field
    def fund_managers(self, info: Info,
                     id: Optional[int] = None,
                     search: Optional[str] = None,
                     status_filter: Optional[str] = None,
                     limit: Optional[int] = 10,
                     offset: Optional[int] = 0) -> List[FundManagerType]:
        """Get fund managers with filtering and pagination - requires authentication"""
        # Require authentication
        require_authentication(info)
        
        db_manager = get_database_manager()
        session = db_manager.get_session()
        
        try:
            query = session.query(FundManager)
            
            # Single fund manager by ID
            if id:
                query = query.filter(FundManager.id == id)
            
            # Search functionality
            if search:
                query = query.filter(
                    or_(
                        FundManager.fund_manager_name.ilike(f"%{search}%"),
                        FundManager.contact_first_name.ilike(f"%{search}%"),
                        FundManager.contact_last_name.ilike(f"%{search}%"),
                        FundManager.contact_email.ilike(f"%{search}%")
                    )
                )
            
            # Status filter
            if status_filter == 'active':
                query = query.filter(FundManager.status == 'active')
            elif status_filter == 'inactive':
                query = query.filter(FundManager.status == 'inactive')
            
            # Pagination
            query = query.offset(offset).limit(limit)
            
            # Execute query
            fund_managers = query.all()
            
            # Convert to GraphQL types
            return [
                FundManagerType(
                    id=fm.id,
                    fund_manager_name=fm.fund_manager_name,
                    contact_title=fm.contact_title,
                    contact_first_name=fm.contact_first_name,
                    contact_last_name=fm.contact_last_name,
                    contact_email=fm.contact_email,
                    contact_number=fm.contact_number,
                    status=fm.status,
                    created_at=fm.created_at.isoformat() if fm.created_at else "",
                    updated_at=fm.updated_at.isoformat() if fm.updated_at else ""
                ) for fm in fund_managers
            ]
            
        except Exception as e:
            logger.error(f"GraphQL fund_managers query error: {e}")
            return []
            
        finally:
            session.close()

@strawberry.type
class FundManagerMutation:
    """GraphQL Mutation root for fund managers"""
    
    @strawberry.field
    def create_fund_manager(self, info: Info, input: FundManagerCreateInput) -> Optional[FundManagerType]:
        """Create a new fund manager - requires authentication"""
        # Require authentication
        require_authentication(info)
        db_manager = get_database_manager()
        session = db_manager.get_session()
        
        try:
            # Check if fund manager with same name already exists
            existing_fund_manager = session.query(FundManager).filter(
                FundManager.fund_manager_name == input.fund_manager_name
            ).first()
            if existing_fund_manager:
                raise ValueError("Fund Manager with this name already exists")
            
            # Check if fund manager with same email already exists
            existing_email_fund_manager = session.query(FundManager).filter(
                FundManager.contact_email == input.email
            ).first()
            if existing_email_fund_manager:
                raise ValueError("Fund Manager with this email already exists")
            
            # Create new fund manager
            new_fund_manager = FundManager(
                fund_manager_name=input.fund_manager_name,
                contact_title=input.title,
                contact_first_name=input.first_name,
                contact_last_name=input.last_name,
                contact_email=input.email,
                contact_number=input.contact_number,
                status='active'
            )
            
            session.add(new_fund_manager)
            session.flush()  # Flush to get the ID
            
            session.commit()
            
            # Convert to GraphQL type
            return FundManagerType(
                id=new_fund_manager.id,
                fund_manager_name=new_fund_manager.fund_manager_name,
                contact_title=new_fund_manager.contact_title,
                contact_first_name=new_fund_manager.contact_first_name,
                contact_last_name=new_fund_manager.contact_last_name,
                contact_email=new_fund_manager.contact_email,
                contact_number=new_fund_manager.contact_number,
                status=new_fund_manager.status,
                created_at=new_fund_manager.created_at.isoformat() if new_fund_manager.created_at else "",
                updated_at=new_fund_manager.updated_at.isoformat() if new_fund_manager.updated_at else ""
            )
            
        except Exception as e:
            session.rollback()
            logger.error(f"GraphQL create_fund_manager error: {e}")
            raise ValueError(str(e))
            
        finally:
            session.close()
    
    @strawberry.field
    def update_fund_manager(self, info: Info, fund_manager_id: int, input: FundManagerUpdateInput) -> Optional[FundManagerType]:
        """Update an existing fund manager - requires authentication"""
        # Require authentication
        require_authentication(info)
        db_manager = get_database_manager()
        session = db_manager.get_session()
        
        try:
            # Get fund manager
            fund_manager = session.query(FundManager).filter(FundManager.id == fund_manager_id).first()
            
            if not fund_manager:
                raise ValueError("Fund Manager not found")
            
            # Update fields if provided
            if input.fund_manager_name is not None:
                # Check if new name already exists for another fund manager
                existing_fund_manager = session.query(FundManager).filter(
                    FundManager.fund_manager_name == input.fund_manager_name,
                    FundManager.id != fund_manager_id
                ).first()
                if existing_fund_manager:
                    raise ValueError("Fund Manager with this name already exists")
                fund_manager.fund_manager_name = input.fund_manager_name
            
            if input.title is not None:
                fund_manager.contact_title = input.title
            
            if input.first_name is not None:
                fund_manager.contact_first_name = input.first_name
            
            if input.last_name is not None:
                fund_manager.contact_last_name = input.last_name
            
            if input.email is not None:
                # Check if new email already exists for another fund manager
                existing_email_fund_manager = session.query(FundManager).filter(
                    FundManager.contact_email == input.email,
                    FundManager.id != fund_manager_id
                ).first()
                if existing_email_fund_manager:
                    raise ValueError("Fund Manager with this email already exists")
                fund_manager.contact_email = input.email
            
            if input.contact_number is not None:
                fund_manager.contact_number = input.contact_number
            
            if input.status is not None:
                if input.status not in ['active', 'inactive']:
                    raise ValueError("Status must be 'active' or 'inactive'")
                fund_manager.status = input.status
            
            fund_manager.updated_at = datetime.utcnow()
            
            session.commit()
            
            # Refresh fund manager
            session.refresh(fund_manager)
            
            # Convert to GraphQL type
            return FundManagerType(
                id=fund_manager.id,
                fund_manager_name=fund_manager.fund_manager_name,
                contact_title=fund_manager.contact_title,
                contact_first_name=fund_manager.contact_first_name,
                contact_last_name=fund_manager.contact_last_name,
                contact_email=fund_manager.contact_email,
                contact_number=fund_manager.contact_number,
                status=fund_manager.status,
                created_at=fund_manager.created_at.isoformat() if fund_manager.created_at else "",
                updated_at=fund_manager.updated_at.isoformat() if fund_manager.updated_at else ""
            )
            
        except Exception as e:
            session.rollback()
            logger.error(f"GraphQL update_fund_manager error: {e}")
            raise ValueError(str(e))
            
        finally:
            session.close()
    
    @strawberry.field
    def toggle_fund_manager_status(self, info: Info, fund_manager_id: int) -> Optional[FundManagerType]:
        """Toggle fund manager active/inactive status - requires authentication"""
        # Require authentication
        require_authentication(info)
        db_manager = get_database_manager()
        session = db_manager.get_session()
        
        try:
            fund_manager = session.query(FundManager).filter(FundManager.id == fund_manager_id).first()
            
            if not fund_manager:
                raise ValueError("Fund Manager not found")
            
            # Toggle status
            if fund_manager.status == 'active':
                fund_manager.status = 'inactive'
            else:
                fund_manager.status = 'active'
            
            fund_manager.updated_at = datetime.utcnow()
            
            session.commit()
            
            # Refresh fund manager
            session.refresh(fund_manager)
            
            # Convert to GraphQL type
            return FundManagerType(
                id=fund_manager.id,
                fund_manager_name=fund_manager.fund_manager_name,
                contact_title=fund_manager.contact_title,
                contact_first_name=fund_manager.contact_first_name,
                contact_last_name=fund_manager.contact_last_name,
                contact_email=fund_manager.contact_email,
                contact_number=fund_manager.contact_number,
                status=fund_manager.status,
                created_at=fund_manager.created_at.isoformat() if fund_manager.created_at else "",
                updated_at=fund_manager.updated_at.isoformat() if fund_manager.updated_at else ""
            )
            
        except Exception as e:
            session.rollback()
            logger.error(f"GraphQL toggle_fund_manager_status error: {e}")
            raise ValueError(str(e))
            
        finally:
            session.close()
    
    @strawberry.field
    def delete_fund_manager(self, info: Info, fund_manager_id: int) -> bool:
        """Delete a fund manager (soft delete by setting status to inactive) - requires authentication"""
        # Require authentication
        require_authentication(info)
        db_manager = get_database_manager()
        session = db_manager.get_session()
        
        try:
            fund_manager = session.query(FundManager).filter(FundManager.id == fund_manager_id).first()
            if not fund_manager:
                raise ValueError("Fund Manager not found")
            
            # Soft delete by setting status to inactive
            fund_manager.status = 'inactive'
            fund_manager.updated_at = datetime.utcnow()
            
            session.commit()
            return True
            
        except Exception as e:
            session.rollback()
            logger.error(f"GraphQL delete_fund_manager error: {e}")
            raise ValueError(str(e))
            
        finally:
            session.close()

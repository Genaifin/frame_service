#!/usr/bin/env python3
"""
GraphQL Schema for Client Management - Clean Implementation with Authentication
Provides pure data responses without response formatting
Maintains consistency with REST API authentication system
"""

import strawberry
from typing import List, Optional
from strawberry.types import Info
from sqlalchemy.orm import joinedload
from sqlalchemy import or_, text
from database_models import Client, User, get_database_manager
from datetime import datetime
import logging

# Import authentication context
from .graphql_auth_context import require_authentication, require_role, get_current_user, is_authenticated

logger = logging.getLogger(__name__)

@strawberry.type
class ClientType:
    """GraphQL type for Client"""
    id: int
    name: str
    code: str
    description: Optional[str]
    type: Optional[str]
    contact_title: Optional[str]
    contact_first_name: Optional[str]
    contact_last_name: Optional[str]
    contact_email: Optional[str]
    contact_number: Optional[str]
    admin_title: Optional[str]
    admin_first_name: Optional[str]
    admin_last_name: Optional[str]
    admin_email: Optional[str]
    admin_job_title: Optional[str]
    is_active: bool
    created_at: str
    updated_at: str

@strawberry.type
class ClientUserType:
    """GraphQL type for User (simplified for client context)"""
    id: int
    username: str
    display_name: str
    first_name: str
    last_name: str
    email: Optional[str]
    is_active: bool

@strawberry.type
class ClientDetailType:
    """GraphQL type for detailed client information"""
    id: int
    name: str
    code: str
    description: Optional[str]
    type: Optional[str]
    contact_title: Optional[str]
    contact_first_name: Optional[str]
    contact_last_name: Optional[str]
    contact_email: Optional[str]
    contact_number: Optional[str]
    admin_title: Optional[str]
    admin_first_name: Optional[str]
    admin_last_name: Optional[str]
    admin_email: Optional[str]
    admin_job_title: Optional[str]
    is_active: bool
    created_at: str
    updated_at: str
    users: List[ClientUserType]
    user_count: int

@strawberry.input
class ClientCreateInput:
    """Input type for creating a client"""
    name: str
    code: Optional[str] = None
    description: Optional[str] = None
    type: Optional[str] = "individual"
    contact_title: Optional[str] = None
    contact_first_name: Optional[str] = None
    contact_last_name: Optional[str] = None
    contact_email: Optional[str] = None
    contact_number: Optional[str] = None
    admin_title: Optional[str] = None
    admin_first_name: Optional[str] = None
    admin_last_name: Optional[str] = None
    admin_email: Optional[str] = None
    admin_job_title: Optional[str] = None
    is_active: Optional[bool] = True

@strawberry.input
class ClientUpdateInput:
    """Input type for updating a client"""
    name: Optional[str] = None
    code: Optional[str] = None
    description: Optional[str] = None
    type: Optional[str] = None
    contact_title: Optional[str] = None
    contact_first_name: Optional[str] = None
    contact_last_name: Optional[str] = None
    contact_email: Optional[str] = None
    contact_number: Optional[str] = None
    admin_title: Optional[str] = None
    admin_first_name: Optional[str] = None
    admin_last_name: Optional[str] = None
    admin_email: Optional[str] = None
    admin_job_title: Optional[str] = None
    is_active: Optional[bool] = None

@strawberry.input
class FundAssignmentInput:
    """Input type for assigning funds to client"""
    client_id: int
    fund_ids: List[int]

@strawberry.type
class ClientQuery:
    """GraphQL Query root for clients"""
    
    @strawberry.field
    def clients(self, info: Info,
                id: Optional[int] = None,
                search: Optional[str] = None,
                status_filter: Optional[str] = None,
                limit: Optional[int] = 10,
                offset: Optional[int] = 0) -> List[ClientType]:
        """Get clients with filtering and pagination - requires authentication"""
        # Require authentication
        require_authentication(info)
        
        db_manager = get_database_manager()
        session = db_manager.get_session()
        
        try:
            query = session.query(Client)
            
            # Single client by ID
            if id:
                query = query.filter(Client.id == id)
            
            # Search functionality
            if search:
                query = query.filter(
                    or_(
                        Client.name.ilike(f"%{search}%"),
                        Client.code.ilike(f"%{search}%"),
                        Client.description.ilike(f"%{search}%")
                    )
                )
            
            # Status filter
            if status_filter == 'active':
                query = query.filter(Client.is_active == True)
            elif status_filter == 'inactive':
                query = query.filter(Client.is_active == False)
            
            # Exclude "all_clients" entry
            query = query.filter(Client.code != 'all_clients')
            
            # Pagination
            query = query.offset(offset).limit(limit)
            
            # Execute query
            clients = query.all()
            
            # Convert to GraphQL types
            return [
                ClientType(
                    id=client.id,
                    name=client.name,
                    code=client.code,
                    description=client.description,
                    type=client.type,
                    contact_title=client.contact_title,
                    contact_first_name=client.contact_first_name,
                    contact_last_name=client.contact_last_name,
                    contact_email=client.contact_email,
                    contact_number=client.contact_number,
                    admin_title=client.admin_title,
                    admin_first_name=client.admin_first_name,
                    admin_last_name=client.admin_last_name,
                    admin_email=client.admin_email,
                    admin_job_title=client.admin_job_title,
                    is_active=client.is_active,
                    created_at=client.created_at.isoformat() if client.created_at else "",
                    updated_at=client.updated_at.isoformat() if client.updated_at else ""
                ) for client in clients
            ]
            
        except Exception as e:
            logger.error(f"GraphQL clients query error: {e}")
            return []
            
        finally:
            session.close()
    
    @strawberry.field
    def client_details(self, info: Info, client_id: int) -> Optional[ClientDetailType]:
        """Get detailed client information including users - requires authentication"""
        # Require authentication
        require_authentication(info)
        
        db_manager = get_database_manager()
        session = db_manager.get_session()
        
        try:
            # Get client
            client = session.query(Client).filter(Client.id == client_id).first()
            
            if not client:
                return None
            
            # Get users with this client
            users_with_client = session.query(User).filter(
                User.client_id == client.id,
                User.is_active == True
            ).all()
            
            # Convert users to GraphQL types
            users = [
                ClientUserType(
                    id=user.id,
                    username=user.username,
                    display_name=user.display_name,
                    first_name=user.first_name,
                    last_name=user.last_name,
                    email=user.email,
                    is_active=user.is_active
                ) for user in users_with_client
            ]
            
            return ClientDetailType(
                id=client.id,
                name=client.name,
                code=client.code,
                description=client.description,
                type=client.type,
                contact_title=client.contact_title,
                contact_first_name=client.contact_first_name,
                contact_last_name=client.contact_last_name,
                contact_email=client.contact_email,
                contact_number=client.contact_number,
                admin_title=client.admin_title,
                admin_first_name=client.admin_first_name,
                admin_last_name=client.admin_last_name,
                admin_email=client.admin_email,
                admin_job_title=client.admin_job_title,
                is_active=client.is_active,
                created_at=client.created_at.isoformat() if client.created_at else "",
                updated_at=client.updated_at.isoformat() if client.updated_at else "",
                users=users,
                user_count=len(users)
            )
            
        except Exception as e:
            logger.error(f"GraphQL client_details query error: {e}")
            return None
            
        finally:
            session.close()

@strawberry.type
class ClientMutation:
    """GraphQL Mutation root for clients"""
    
    @strawberry.field
    def create_client(self, info: Info, input: ClientCreateInput) -> Optional[ClientType]:
        """Create a new client - requires authentication"""
        # Require authentication
        require_authentication(info)
        db_manager = get_database_manager()
        session = db_manager.get_session()
        
        try:
            # Generate client code if not provided
            if not input.code:
                client_code = input.name.replace(" ", "").lower()
            else:
                client_code = input.code
            
            # Check if client code already exists
            existing_client = session.query(Client).filter(Client.code == client_code).first()
            if existing_client:
                raise ValueError("Client code already exists")
            
            # Create new client
            new_client = Client(
                name=input.name,
                code=client_code,
                description=input.description,
                type=input.type,
                contact_title=input.contact_title,
                contact_first_name=input.contact_first_name,
                contact_last_name=input.contact_last_name,
                contact_email=input.contact_email,
                contact_number=input.contact_number,
                admin_title=input.admin_title,
                admin_first_name=input.admin_first_name,
                admin_last_name=input.admin_last_name,
                admin_email=input.admin_email,
                admin_job_title=input.admin_job_title,
                is_active=input.is_active
            )
            
            session.add(new_client)
            session.flush()  # Flush to get the ID
            
            session.commit()
            
            # Convert to GraphQL type
            return ClientType(
                id=new_client.id,
                name=new_client.name,
                code=new_client.code,
                description=new_client.description,
                type=new_client.type,
                contact_title=new_client.contact_title,
                contact_first_name=new_client.contact_first_name,
                contact_last_name=new_client.contact_last_name,
                contact_email=new_client.contact_email,
                contact_number=new_client.contact_number,
                admin_title=new_client.admin_title,
                admin_first_name=new_client.admin_first_name,
                admin_last_name=new_client.admin_last_name,
                admin_email=new_client.admin_email,
                admin_job_title=new_client.admin_job_title,
                is_active=new_client.is_active,
                created_at=new_client.created_at.isoformat() if new_client.created_at else "",
                updated_at=new_client.updated_at.isoformat() if new_client.updated_at else ""
            )
            
        except Exception as e:
            session.rollback()
            logger.error(f"GraphQL create_client error: {e}")
            raise ValueError(str(e))
            
        finally:
            session.close()
    
    @strawberry.field
    def update_client(self, info: Info, client_id: int, input: ClientUpdateInput) -> Optional[ClientType]:
        """Update an existing client - requires authentication"""
        # Require authentication
        require_authentication(info)
        db_manager = get_database_manager()
        session = db_manager.get_session()
        
        try:
            # Get client
            client = session.query(Client).filter(Client.id == client_id).first()
            
            if not client:
                raise ValueError("Client not found")
            
            # Update fields if provided
            if input.name is not None:
                client.name = input.name
            if input.code is not None:
                # Check if new code already exists for another client
                existing_client = session.query(Client).filter(
                    Client.code == input.code,
                    Client.id != client_id
                ).first()
                if existing_client:
                    raise ValueError("Client code already exists")
                client.code = input.code
            if input.description is not None:
                client.description = input.description
            if input.type is not None:
                client.type = input.type
            if input.contact_title is not None:
                client.contact_title = input.contact_title
            if input.contact_first_name is not None:
                client.contact_first_name = input.contact_first_name
            if input.contact_last_name is not None:
                client.contact_last_name = input.contact_last_name
            if input.contact_email is not None:
                client.contact_email = input.contact_email
            if input.contact_number is not None:
                client.contact_number = input.contact_number
            if input.admin_title is not None:
                client.admin_title = input.admin_title
            if input.admin_first_name is not None:
                client.admin_first_name = input.admin_first_name
            if input.admin_last_name is not None:
                client.admin_last_name = input.admin_last_name
            if input.admin_email is not None:
                client.admin_email = input.admin_email
            if input.admin_job_title is not None:
                client.admin_job_title = input.admin_job_title
            if input.is_active is not None:
                client.is_active = input.is_active
            
            client.updated_at = datetime.utcnow()
            
            session.commit()
            
            # Refresh client
            session.refresh(client)
            
            # Convert to GraphQL type
            return ClientType(
                id=client.id,
                name=client.name,
                code=client.code,
                description=client.description,
                type=client.type,
                contact_title=client.contact_title,
                contact_first_name=client.contact_first_name,
                contact_last_name=client.contact_last_name,
                contact_email=client.contact_email,
                contact_number=client.contact_number,
                admin_title=client.admin_title,
                admin_first_name=client.admin_first_name,
                admin_last_name=client.admin_last_name,
                admin_email=client.admin_email,
                admin_job_title=client.admin_job_title,
                is_active=client.is_active,
                created_at=client.created_at.isoformat() if client.created_at else "",
                updated_at=client.updated_at.isoformat() if client.updated_at else ""
            )
            
        except Exception as e:
            session.rollback()
            logger.error(f"GraphQL update_client error: {e}")
            raise ValueError(str(e))
            
        finally:
            session.close()
    
    @strawberry.field
    def delete_client(self, info: Info, client_id: int) -> bool:
        """Delete a client (soft delete) - requires authentication"""
        # Require authentication
        require_authentication(info)
        db_manager = get_database_manager()
        session = db_manager.get_session()
        
        try:
            client = session.query(Client).filter(Client.id == client_id).first()
            if not client:
                raise ValueError("Client not found")
            
            # Soft delete by setting is_active to False
            client.is_active = False
            client.updated_at = datetime.utcnow()
            
            session.commit()
            return True
            
        except Exception as e:
            session.rollback()
            logger.error(f"GraphQL delete_client error: {e}")
            raise ValueError(str(e))
            
        finally:
            session.close()
    
    @strawberry.field
    def toggle_client_status(self, info: Info, client_id: int) -> Optional[ClientType]:
        """Toggle client active/inactive status - requires authentication"""
        # Require authentication
        require_authentication(info)
        db_manager = get_database_manager()
        session = db_manager.get_session()
        
        try:
            client = session.query(Client).filter(Client.id == client_id).first()
            
            if not client:
                raise ValueError("Client not found")
            
            # Toggle status
            client.is_active = not client.is_active
            client.updated_at = datetime.utcnow()
            
            session.commit()
            
            # Refresh client
            session.refresh(client)
            
            # Convert to GraphQL type
            return ClientType(
                id=client.id,
                name=client.name,
                code=client.code,
                description=client.description,
                type=client.type,
                contact_title=client.contact_title,
                contact_first_name=client.contact_first_name,
                contact_last_name=client.contact_last_name,
                contact_email=client.contact_email,
                contact_number=client.contact_number,
                admin_title=client.admin_title,
                admin_first_name=client.admin_first_name,
                admin_last_name=client.admin_last_name,
                admin_email=client.admin_email,
                admin_job_title=client.admin_job_title,
                is_active=client.is_active,
                created_at=client.created_at.isoformat() if client.created_at else "",
                updated_at=client.updated_at.isoformat() if client.updated_at else ""
            )
            
        except Exception as e:
            session.rollback()
            logger.error(f"GraphQL toggle_client_status error: {e}")
            raise ValueError(str(e))
            
        finally:
            session.close()

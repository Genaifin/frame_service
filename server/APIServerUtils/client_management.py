#!/usr/bin/env python3
"""
Client Management API Utilities
Provides CRUD operations for clients based on the dashboard requirements
"""

from fastapi import HTTPException, status
from fastapi.responses import JSONResponse
from typing import List, Dict, Any, Optional
from datetime import datetime
import logging
import sys
from pathlib import Path

# Ensure project root is on sys.path for Docker/runtime compatibility
_project_root = Path(__file__).resolve().parents[2]
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

# Import database models
from database_models import get_database_manager, User, Client

logger = logging.getLogger(__name__)

class ClientManagementService:
    """Service class for client management operations"""
    
    def __init__(self):
        self.db_manager = get_database_manager()
    
    async def get_all_clients(
        self, 
        search: Optional[str] = None,
        status_filter: Optional[str] = None,
        page: int = 1,
        page_size: int = 10
    ) -> Dict[str, Any]:
        """Get all clients with pagination, search, and filtering"""
        try:
            # Test database connection first
            if not self.db_manager.engine:
                raise Exception("Database engine not initialized")
            
            session = self.db_manager.get_session()
            
            # Build base query
            query = session.query(Client)
            
            # Apply search filter
            if search:
                search_term = f"%{search}%"
                query = query.filter(
                    (Client.name.ilike(search_term)) |
                    (Client.code.ilike(search_term)) |
                    (Client.description.ilike(search_term))
                )
            
            # Apply status filter
            if status_filter and status_filter.lower() != "all":
                if status_filter.lower() == "active":
                    query = query.filter(Client.is_active == True)
                elif status_filter.lower() == "inactive":
                    query = query.filter(Client.is_active == False)
            
            # Get total count for pagination
            total_count = query.count()
            
            # Apply pagination
            offset = (page - 1) * page_size
            clients = query.offset(offset).limit(page_size).all()
            
            # Get client details with user count
            client_list = []
            for client in clients:
                # Get user count for this client
                user_count = session.query(User).filter(
                    User.client_id == client.id,
                    User.is_active == True
                ).count()
                
                client_data = {
                    'id': client.id,
                    'client_name': client.name,
                    'client_code': client.code,
                    'description': client.description,
                    'status': 'Active' if client.is_active else 'Inactive',
                    'user_count': user_count,
                    'created_at': client.created_at.strftime('%m/%d/%Y') if client.created_at else None,
                    'updated_at': client.updated_at.strftime('%m/%d/%Y') if client.updated_at else None
                }
                client_list.append(client_data)
            
            # Calculate pagination info
            total_pages = (total_count + page_size - 1) // page_size
            start_record = offset + 1
            end_record = min(offset + page_size, total_count)
            
            return {
                'success': True,
                'data': client_list,
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
            logger.error(f"Error getting clients: {e}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to retrieve clients: {str(e)}"
            )
        finally:
            session.close()
    
    async def get_client_by_id(self, client_id: int) -> Dict[str, Any]:
        """Get a specific client by ID with full details"""
        try:
            session = self.db_manager.get_session()
            
            client = session.query(Client).filter(Client.id == client_id).first()
            if not client:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail="Client not found"
                )
            
            # Get users associated with this client
            users_query = session.query(User).filter(
                User.client_id == client.id,
                User.is_active == True
            )
            
            users = []
            for user in users_query.all():
                users.append({
                    'id': user.id,
                    'username': user.username,
                    'display_name': user.display_name,
                    'email': user.email
                })
            
            # Remove duplicates (in case user has multiple roles for same client)
            unique_users = []
            seen_user_ids = set()
            for user in users:
                if user['id'] not in seen_user_ids:
                    unique_users.append(user)
                    seen_user_ids.add(user['id'])
            
            client_data = {
                'id': client.id,
                'client_name': client.name,
                'client_code': client.code,
                'description': client.description,
                'is_active': client.is_active,
                'users': unique_users,
                'user_count': len(unique_users),
                'created_at': client.created_at.isoformat() if client.created_at else None,
                'updated_at': client.updated_at.isoformat() if client.updated_at else None
            }
            
            return {
                'success': True,
                'data': client_data
            }
            
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error getting client {client_id}: {e}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to retrieve client: {str(e)}"
            )
        finally:
            session.close()
    
    async def create_client(self, client_data: Dict[str, Any]) -> Dict[str, Any]:
        """Create a new client with optional permissions"""
        session = None
        try:
            session = self.db_manager.get_session()
            
            # Validate required fields
            required_fields = ['client_name', 'client_code']
            for field in required_fields:
                if field not in client_data or not client_data[field]:
                    raise HTTPException(
                        status_code=status.HTTP_400_BAD_REQUEST,
                        detail=f"Missing required field: {field}"
                    )
            
            # Check if client_name already exists
            existing_name = session.query(Client).filter(Client.name == client_data['client_name']).first()
            if existing_name:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="Client name already exists"
                )
            
            # Check if client_code already exists
            existing_code = session.query(Client).filter(Client.code == client_data['client_code']).first()
            if existing_code:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="Client code already exists"
                )
            
            # Start transaction
            with session.begin():
                # Create new client
                new_client = Client(
                    name=client_data['client_name'],
                    code=client_data['client_code'],
                    description=client_data.get('description'),
                    is_active=client_data.get('is_active', True)
                )
                
                session.add(new_client)
                session.flush()  # Get the new client ID
                
                # Handle products and permissions if provided
                if 'products' in client_data and 'data' in client_data['products']:
                    from database_models import Module, RoleOrClientBasedModuleLevelPermission
                    
                    # Get all modules to map names to IDs
                    modules = {m.module_name: m for m in session.query(Module).all()}
                    
                    # Process product permissions
                    for product_name, product_data in client_data['products']['data'].items():
                        if product_data.get('isActive', False):
                            # Create permission for the main product
                            if product_name in modules:
                                permission = RoleOrClientBasedModuleLevelPermission(
                                    client_id=new_client.id,
                                    module_id=modules[product_name].id,
                                    client_has_permission=True,
                                    is_active=True
                                )
                                session.add(permission)
                            
                            # Process child modules (e.g., Dashboard, File Manager)
                            if 'children' in product_data:
                                for child_name, child_data in product_data['children'].items():
                                    if child_name in modules and child_data.get('isActive', False):
                                        permission = RoleOrClientBasedModuleLevelPermission(
                                            client_id=new_client.id,
                                            module_id=modules[child_name].id,
                                            client_has_permission=True,
                                            is_active=True
                                        )
                                        session.add(permission)
                
                # Handle master permissions if provided
                if 'masters' in client_data and 'data' in client_data['masters']:
                    from database_models import Master, RoleOrClientBasedModuleLevelPermission
                    
                    # Get all masters to map names to IDs
                    masters = {m.name: m for m in session.query(Master).all()}
                    
                    for master_name, master_data in client_data['masters']['data'].items():
                        if master_data.get('isActive', False) and master_name in masters:
                            # Create permission for the master
                            permission = RoleOrClientBasedModuleLevelPermission(
                                client_id=new_client.id,
                                master_id=masters[master_name].id,
                                client_has_permission=True,
                                is_active=True
                            )
                            session.add(permission)
                            
                            # Process child masters if any
                            if 'children' in master_data:
                                for child_name, child_data in master_data['children'].items():
                                    if child_name in masters and child_data.get('isActive', False):
                                        permission = RoleOrClientBasedModuleLevelPermission(
                                            client_id=new_client.id,
                                            master_id=masters[child_name].id,
                                            client_has_permission=True,
                                            is_active=True
                                        )
                                        session.add(permission)
                
                session.commit()
            
            # Create dedicated schema for the client
            schema_name = None
            try:
                schema_name = self.db_manager.create_client_schema(new_client.name)
                logger.info(f"Created schema '{schema_name}' for client '{new_client.name}'")
                schema_creation_success = True
                schema_message = f"Schema '{schema_name}' created successfully"
            except Exception as schema_error:
                logger.warning(f"Failed to create schema for client '{new_client.name}': {schema_error}")
                schema_creation_success = False
                schema_message = f"Client created but schema creation failed: {str(schema_error)}"
            
            return {
                'success': True,
                'message': 'Client created successfully',
                'data': {
                    'id': new_client.id,
                    'client_name': new_client.name,
                    'client_code': new_client.code,
                    'description': new_client.description,
                    'status': 'Active' if new_client.is_active else 'Inactive',
                    'schema_created': schema_creation_success,
                    'schema_message': schema_message,
                    'schema_name': schema_name if schema_creation_success else None
                }
            }
            
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error creating client: {e}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to create client: {str(e)}"
            )
        finally:
            session.close()
    
    async def update_client(self, client_id: int, client_data: Dict[str, Any]) -> Dict[str, Any]:
        """Update an existing client"""
        try:
            session = self.db_manager.get_session()
            
            # Get existing client
            client = session.query(Client).filter(Client.id == client_id).first()
            if not client:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail="Client not found"
                )
            
            # Check if client_name is being changed and if it already exists
            if 'client_name' in client_data and client_data['client_name'] != client.name:
                existing_name = session.query(Client).filter(Client.name == client_data['client_name']).first()
                if existing_name:
                    raise HTTPException(
                        status_code=status.HTTP_400_BAD_REQUEST,
                        detail="Client name already exists"
                    )
            
            # Check if client_code is being changed and if it already exists
            if 'client_code' in client_data and client_data['client_code'] != client.code:
                existing_code = session.query(Client).filter(Client.code == client_data['client_code']).first()
                if existing_code:
                    raise HTTPException(
                        status_code=status.HTTP_400_BAD_REQUEST,
                        detail="Client code already exists"
                    )
            
            # Update client fields
            if 'client_name' in client_data:
                client.name = client_data['client_name']
            if 'client_code' in client_data:
                client.code = client_data['client_code']
            if 'description' in client_data:
                client.description = client_data['description']
            if 'is_active' in client_data:
                client.is_active = client_data['is_active']
            
            # Update timestamp
            client.updated_at = datetime.utcnow()
            
            session.commit()
            
            return {
                'success': True,
                'message': 'Client updated successfully',
                'data': {
                    'id': client.id,
                    'client_name': client.name,
                    'client_code': client.code,
                    'description': client.description,
                    'status': 'Active' if client.is_active else 'Inactive'
                }
            }
            
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error updating client {client_id}: {e}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to update client: {str(e)}"
            )
        finally:
            session.close()
    
    async def delete_client(self, client_id: int, hard_delete: bool = True) -> Dict[str, Any]:
        """
        Delete a client
        
        Args:
            client_id: ID of the client to delete
            hard_delete: If True, permanently delete the record and schema (default). If False, soft delete (set is_active=False)
        """
        try:
            session = self.db_manager.get_session()
            
            client = session.query(Client).filter(Client.id == client_id).first()
            if not client:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail="Client not found"
                )
            
            # Check if client has active users
            active_user_count = session.query(User).filter(
                User.client_id == client_id,
                User.is_active == True
            ).count()
            
            if active_user_count > 0:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"Cannot delete client. There are {active_user_count} active users assigned to this client. Please reassign or deactivate users first."
                )
            
            if hard_delete:
                # Hard delete - permanently remove the record
                try:
                    # Store client name for schema deletion
                    client_name = client.name
                    
                    # Delete the client record
                    session.delete(client)
                    session.commit()
                    
                    # Drop the client schema if it exists
                    try:
                        self.db_manager.drop_client_schema(client_name)
                        schema_message = f"Schema for '{client_name}' dropped successfully"
                    except Exception as schema_error:
                        logger.warning(f"Failed to drop schema for client '{client_name}': {schema_error}")
                        schema_message = f"Client deleted but schema cleanup failed: {str(schema_error)}"
                    
                    return {
                        'success': True,
                        'message': f'Client permanently deleted successfully. {schema_message}',
                        'delete_type': 'hard_delete'
                    }
                    
                except Exception as e:
                    session.rollback()
                    raise e
                    
            else:
                # Soft delete - set is_active to False
                client.is_active = False
                client.updated_at = datetime.utcnow()
                
                session.commit()
                
                return {
                    'success': True,
                    'message': 'Client deactivated successfully (soft delete)',
                    'delete_type': 'soft_delete'
                }
            
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error deleting client {client_id}: {e}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to delete client: {str(e)}"
            )
        finally:
            session.close()
    
    async def bulk_update_clients(self, client_ids: List[int], updates: Dict[str, Any]) -> Dict[str, Any]:
        """Bulk update multiple clients"""
        try:
            session = self.db_manager.get_session()
            
            updated_count = 0
            for client_id in client_ids:
                client = session.query(Client).filter(Client.id == client_id).first()
                if client:
                    # Apply updates
                    for field, value in updates.items():
                        if hasattr(client, field):
                            setattr(client, field, value)
                    
                    client.updated_at = datetime.utcnow()
                    updated_count += 1
            
            session.commit()
            
            return {
                'success': True,
                'message': f'Successfully updated {updated_count} clients',
                'updated_count': updated_count
            }
            
        except Exception as e:
            logger.error(f"Error bulk updating clients: {e}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to bulk update clients: {str(e)}"
            )
        finally:
            session.close()

# Create service instance
client_service = ClientManagementService()

# API Response functions
async def get_clients_response(
    search: Optional[str] = None,
    status_filter: Optional[str] = None,
    page: int = 1,
    page_size: int = 10
) -> JSONResponse:
    """Get all clients with pagination and filtering"""
    result = await client_service.get_all_clients(search, status_filter, page, page_size)
    return JSONResponse(content=result)

async def get_client_response(client_id: int) -> JSONResponse:
    """Get a specific client by ID"""
    result = await client_service.get_client_by_id(client_id)
    return JSONResponse(content=result)

async def create_client_response(client_data: Dict[str, Any]) -> JSONResponse:
    """Create a new client"""
    result = await client_service.create_client(client_data)
    return JSONResponse(content=result, status_code=201)

async def update_client_response(client_id: int, client_data: Dict[str, Any]) -> JSONResponse:
    """Update an existing client"""
    result = await client_service.update_client(client_id, client_data)
    return JSONResponse(content=result)

async def delete_client_response(client_id: int, hard_delete: bool = True) -> JSONResponse:
    """Delete a client"""
    result = await client_service.delete_client(client_id, hard_delete)
    return JSONResponse(content=result)

async def bulk_update_clients_response(client_ids: List[int], updates: Dict[str, Any]) -> JSONResponse:
    """Bulk update multiple clients"""
    result = await client_service.bulk_update_clients(client_ids, updates)
    return JSONResponse(content=result)

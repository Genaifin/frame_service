#!/usr/bin/env python3
"""
GraphQL Schema for Role Management
Provides GraphQL endpoints for role operations with authentication
"""

import strawberry
from typing import List, Optional
from strawberry.types import Info
from sqlalchemy.orm import joinedload
from sqlalchemy import func
from database_models import Role, User, Module, Permission, get_database_manager
from models.permission_models import RoleOrClientBasedModuleLevelPermission
from datetime import datetime
import logging

# Import authentication context
from .graphql_auth_context import require_authentication, require_role, get_current_user, is_authenticated

logger = logging.getLogger(__name__)

@strawberry.type
class RoleType:
    """GraphQL type for Role"""
    id: int
    role_name: str
    role_code: str
    description: Optional[str]
    is_active: bool
    created_at: str
    updated_at: str

@strawberry.type
class RoleUserType:
    """GraphQL type for User (simplified for role context)"""
    id: int
    username: str
    display_name: str
    first_name: str
    last_name: str
    email: Optional[str]
    is_active: bool

@strawberry.type
class PermissionType:
    """GraphQL type for Permission"""
    id: int
    permission_code: str
    permission_name: str
    description: Optional[str]

@strawberry.type
class ModuleType:
    """GraphQL type for Module"""
    id: int
    module_name: str
    module_code: str
    description: Optional[str]

@strawberry.type
class RoleModulePermissionType:
    """GraphQL type for Role-Module-Permission relationship"""
    id: int
    role_id: Optional[int]
    client_id: Optional[int]
    module_id: int
    master_id: Optional[int]
    permission_id: int
    is_active: bool
    module: ModuleType
    permission: PermissionType

@strawberry.type
class RoleDetailType:
    """GraphQL type for detailed role information"""
    id: int
    role_name: str
    role_code: str
    description: Optional[str]
    is_active: bool
    created_at: str
    updated_at: str
    users: List[RoleUserType]
    permissions: List[RoleModulePermissionType]
    user_count: int
    module_permissions: strawberry.scalars.JSON

@strawberry.type
class RoleSummaryType:
    """GraphQL type for role summary/aggregated data"""
    role_id: int
    role_name: str
    role_code: str
    total_users: int
    products: str
    status: str
    is_active: bool
    created_at: str

@strawberry.input
class RoleCreateInput:
    """Input type for creating a role"""
    role_name: str
    role_code: str
    description: Optional[str] = None
    is_active: bool = True
    permissions: Optional[List[strawberry.scalars.JSON]] = None

@strawberry.input
class RoleUpdateInput:
    """Input type for updating a role"""
    role_name: Optional[str] = None
    role_code: Optional[str] = None
    description: Optional[str] = None
    is_active: Optional[bool] = None
    permissions: Optional[List[strawberry.scalars.JSON]] = None

@strawberry.input
class RoleInactivationInput:
    """Input type for inactivating a role with user reassignment"""
    role_id: int
    assign_same: bool
    role: Optional[str] = None
    users: Optional[List[strawberry.scalars.JSON]] = None

@strawberry.type
class RoleQuery:
    """GraphQL Query root for roles"""
    
    @strawberry.field
    def roles(self, info: Info,
              id: Optional[int] = None,
              search: Optional[str] = None,
              status_filter: Optional[str] = None,
              limit: Optional[int] = 10,
              offset: Optional[int] = 0) -> List[RoleType]:
        """Get roles with filtering and pagination - requires authentication"""
        # Require authentication
        require_authentication(info)
        
        db_manager = get_database_manager()
        session = db_manager.get_session()
        
        try:
            query = session.query(Role)
            
            # Single role by ID
            if id:
                query = query.filter(Role.id == id)
            
            # Search functionality
            if search:
                query = query.filter(
                    Role.role_name.ilike(f"%{search}%") |
                    Role.role_code.ilike(f"%{search}%") |
                    Role.description.ilike(f"%{search}%")
                )
            
            # Status filter
            if status_filter == 'active':
                query = query.filter(Role.is_active == True)
            elif status_filter == 'inactive':
                query = query.filter(Role.is_active == False)
            
            # Pagination
            query = query.offset(offset).limit(limit)
            
            # Execute query
            roles = query.all()
            
            # Convert to GraphQL types
            return [
                RoleType(
                    id=role.id,
                    role_name=role.role_name,
                    role_code=role.role_code,
                    description=role.description,
                    is_active=role.is_active,
                    created_at=role.created_at.isoformat() if role.created_at else "",
                    updated_at=role.updated_at.isoformat() if role.updated_at else ""
                ) for role in roles
            ]
            
        except Exception as e:
            logger.error(f"GraphQL roles query error: {e}")
            return []
            
        finally:
            session.close()
    
    @strawberry.field
    def role_details(self, info: Info, role_id: int) -> Optional[RoleDetailType]:
        """Get detailed role information including users and permissions - requires authentication"""
        # Require authentication
        require_authentication(info)
        
        session = info.context["db"]
        
        try:
            # Get role with users and their clients
            role = session.query(Role).options(
                joinedload(Role.users).joinedload(User.client)
            ).filter(Role.id == role_id).first()
            
            if not role:
                raise Exception(f"Role with ID {role_id} not found")
                
            # Get role permissions with module and permission details (only role-based, not client-based)
            role_permissions = session.query(RoleOrClientBasedModuleLevelPermission).options(
                joinedload(RoleOrClientBasedModuleLevelPermission.module),
                joinedload(RoleOrClientBasedModuleLevelPermission.permission)
            ).filter(
                RoleOrClientBasedModuleLevelPermission.role_id == role_id,
                RoleOrClientBasedModuleLevelPermission.client_id.is_(None),  # Only role-based permissions
                RoleOrClientBasedModuleLevelPermission.is_active == True
            ).all()
            
            # Convert to GraphQL types
            users = [
                RoleUserType(
                    id=user.id,
                    username=user.username,
                    display_name=user.display_name or f"{user.first_name} {user.last_name}".strip(),
                    first_name=user.first_name or "",
                    last_name=user.last_name or "",
                    email=user.email,
                    is_active=user.is_active
                )
                for user in role.users if user.is_active
            ]
            
            permissions = [
                RoleModulePermissionType(
                    id=rp.id,
                    role_id=rp.role_id,
                    client_id=rp.client_id,
                    module_id=rp.module_id,
                    master_id=rp.master_id,
                    permission_id=rp.permission_id,
                    is_active=rp.is_active,
                    module=ModuleType(
                        id=rp.module.id,
                        module_name=rp.module.module_name,
                        module_code=rp.module.module_code,
                        description=rp.module.description
                    ),
                    permission=PermissionType(
                        id=rp.permission.id,
                        permission_name=rp.permission.permission_name,
                        permission_code=rp.permission.permission_code,
                        description=rp.permission.description
                    )
                )
                for rp in role_permissions
            ]
            
            # Get module permissions as a dictionary for the UI
            module_permissions = {}
            for rp in role_permissions:
                if rp.module.module_code not in module_permissions:
                    module_permissions[rp.module.module_code] = {}
                module_permissions[rp.module.module_code][rp.permission.permission_code] = True
            
            return RoleDetailType(
                id=role.id,
                role_name=role.role_name,
                role_code=role.role_code,
                description=role.description,
                is_active=role.is_active,
                created_at=role.created_at.isoformat(),
                updated_at=role.updated_at.isoformat(),
                users=users,
                permissions=permissions,
                user_count=len(users),
                module_permissions=module_permissions
            )
            
        except Exception as e:
            logger.error(f"Error getting role details: {e}")
            raise Exception(f"Failed to get role details: {str(e)}")
        finally:
            session.close()
    
    @strawberry.field
    def roles_summary(self, info: Info,
                      page: int = 1, 
                      page_size: int = 10) -> List[RoleSummaryType]:
        """Get aggregated roles data with user counts - requires authentication"""
        # Require authentication
        require_authentication(info)
        
        db_manager = get_database_manager()
        session = db_manager.get_session()
        
        try:
            # Query to get role data with user counts
            base_query = session.query(
                Role.id.label('role_id'),
                Role.role_name.label('role'),
                Role.role_code.label('role_code'),
                Role.is_active.label('role_is_active'),
                Role.created_at.label('created_at'),
                func.count(User.id).label('total_users')
            ).outerjoin(User, Role.id == User.role_id)\
             .group_by(Role.id, Role.role_name, Role.role_code, Role.is_active, Role.created_at)\
             .order_by(Role.role_name)
            
            # Apply pagination
            offset = (page - 1) * page_size
            role_stats = base_query.offset(offset).limit(page_size).all()
            
            # Convert to GraphQL types
            roles = []
            for role_stat in role_stats:
                total_users = role_stat.total_users or 0
                status = "Active" if role_stat.role_is_active else "Inactive"
                
                roles.append(RoleSummaryType(
                    role_id=role_stat.role_id,
                    role_name=role_stat.role,
                    role_code=role_stat.role_code,
                    total_users=total_users,
                    products="Frame, NAV Validus",
                    status=status,
                    is_active=role_stat.role_is_active,
                    created_at=role_stat.created_at.isoformat() if role_stat.created_at else ""
                ))
            
            return roles
            
        except Exception as e:
            logger.error(f"GraphQL roles_summary query error: {e}")
            return []
            
        finally:
            session.close()

@strawberry.type
class RoleMutation:
    """GraphQL Mutation root for roles"""
    
    @strawberry.field
    def create_role(self, info: Info, input: RoleCreateInput) -> Optional[RoleType]:
        """Create a new role with optional permissions - requires authentication"""
        # Require authentication
        require_authentication(info)
        
        db_manager = get_database_manager()
        session = db_manager.get_session()
        
        try:
            # Validate required fields
            if not input.role_name or len(input.role_name.strip()) < 2:
                raise ValueError("Role name must be at least 2 characters long")
            
            if not input.role_code or len(input.role_code.strip()) < 2:
                raise ValueError("Role code must be at least 2 characters long")
            
            # Check if role name already exists
            existing_role_name = session.query(Role).filter(
                Role.role_name == input.role_name.strip()
            ).first()
            
            if existing_role_name:
                raise ValueError("Role name already exists")
            
            # Check if role code already exists
            existing_role_code = session.query(Role).filter(
                Role.role_code == input.role_code.strip()
            ).first()
            
            if existing_role_code:
                raise ValueError("Role code already exists")
            
            # Create new role
            new_role = Role(
                role_name=input.role_name.strip(),
                role_code=input.role_code.strip(),
                description=input.description.strip() if input.description else None,
                is_active=input.is_active
            )
            
            session.add(new_role)
            session.flush()  # Flush to get the ID
            
            # Handle permissions if provided
            if input.permissions:
                RoleMutation._create_role_permissions(session, new_role.id, input.permissions)
            
            # Commit the transaction
            session.commit()
            
            # Convert to GraphQL type
            return RoleType(
                id=new_role.id,
                role_name=new_role.role_name,
                role_code=new_role.role_code,
                description=new_role.description,
                is_active=new_role.is_active,
                created_at=new_role.created_at.isoformat() if new_role.created_at else "",
                updated_at=new_role.updated_at.isoformat() if new_role.updated_at else ""
            )
            
        except Exception as e:
            session.rollback()
            logger.error(f"GraphQL create_role error: {e}")
            raise ValueError(str(e))
            
        finally:
            session.close()
    
    @strawberry.field
    def update_role(self, info: Info, role_id: int, input: RoleUpdateInput) -> Optional[RoleType]:
        """Update an existing role with optional permissions - requires authentication"""
        # Require authentication
        require_authentication(info)
        
        db_manager = get_database_manager()
        session = db_manager.get_session()
        
        try:
            # Get role
            role = session.query(Role).filter(Role.id == role_id).first()
            
            if not role:
                raise ValueError("Role not found")
            
            # Update fields if provided
            if input.role_name is not None:
                if len(input.role_name.strip()) < 2:
                    raise ValueError("Role name must be at least 2 characters long")
                
                # Check if role name already exists for another role
                existing_role_name = session.query(Role).filter(
                    Role.role_name == input.role_name.strip(),
                    Role.id != role_id
                ).first()
                
                if existing_role_name:
                    raise ValueError("Role name already exists")
                
                role.role_name = input.role_name.strip()
            
            if input.role_code is not None:
                if len(input.role_code.strip()) < 2:
                    raise ValueError("Role code must be at least 2 characters long")
                
                # Check if role code already exists for another role
                existing_role_code = session.query(Role).filter(
                    Role.role_code == input.role_code.strip(),
                    Role.id != role_id
                ).first()
                
                if existing_role_code:
                    raise ValueError("Role code already exists")
                
                role.role_code = input.role_code.strip()
            
            if input.description is not None:
                role.description = input.description.strip() if input.description else None
            
            if input.is_active is not None:
                role.is_active = input.is_active
            
            role.updated_at = datetime.utcnow()
            
            # Handle permissions if provided
            if input.permissions is not None:
                # Remove existing role-based permissions (not client permissions)
                session.query(RoleOrClientBasedModuleLevelPermission).filter(
                    RoleOrClientBasedModuleLevelPermission.role_id == role_id,
                    RoleOrClientBasedModuleLevelPermission.client_id.is_(None)  # Only role-based permissions
                ).delete()
                
                # Add new permissions
                if input.permissions:
                    RoleMutation._create_role_permissions(session, role_id, input.permissions)
            
            session.commit()
            
            # Refresh role
            session.refresh(role)
            
            # Convert to GraphQL type
            return RoleType(
                id=role.id,
                role_name=role.role_name,
                role_code=role.role_code,
                description=role.description,
                is_active=role.is_active,
                created_at=role.created_at.isoformat() if role.created_at else "",
                updated_at=role.updated_at.isoformat() if role.updated_at else ""
            )
            
        except Exception as e:
            session.rollback()
            logger.error(f"GraphQL update_role error: {e}")
            raise ValueError(str(e))
            
        finally:
            session.close()
    
    @strawberry.field
    def delete_role(self, info: Info, role_id: int, hard_delete: bool = False) -> bool:
        """Delete a role (soft delete by default, hard delete optional) - requires authentication"""
        # Require authentication
        require_authentication(info)
        
        db_manager = get_database_manager()
        session = db_manager.get_session()
        
        try:
            role = session.query(Role).filter(Role.id == role_id).first()
            if not role:
                raise ValueError("Role not found")
            
            # Check if role has active users assigned
            active_user_count = session.query(User).filter(
                User.role_id == role_id,
                User.is_active == True
            ).count()
            
            if active_user_count > 0:
                raise ValueError(f"Cannot delete role. There are {active_user_count} active users with this role. Please reassign users first.")
            
            if hard_delete:
                # Hard delete - permanently remove the record
                session.delete(role)
            else:
                # Soft delete - set is_active to False
                role.is_active = False
                role.updated_at = datetime.utcnow()
            
            session.commit()
            return True
            
        except Exception as e:
            session.rollback()
            logger.error(f"GraphQL delete_role error: {e}")
            raise ValueError(str(e))
            
        finally:
            session.close()
    
    @strawberry.field
    def activate_role(self, info: Info, role_id: int) -> Optional[RoleType]:
        """Activate a role - requires authentication"""
        # Require authentication
        require_authentication(info)
        
        db_manager = get_database_manager()
        session = db_manager.get_session()
        
        try:
            role = session.query(Role).filter(Role.id == role_id).first()
            
            if not role:
                raise ValueError("Role not found")
            
            # Activate role
            role.is_active = True
            role.updated_at = datetime.utcnow()
            
            session.commit()
            
            # Refresh role
            session.refresh(role)
            
            # Convert to GraphQL type
            return RoleType(
                id=role.id,
                role_name=role.role_name,
                role_code=role.role_code,
                description=role.description,
                is_active=role.is_active,
                created_at=role.created_at.isoformat() if role.created_at else "",
                updated_at=role.updated_at.isoformat() if role.updated_at else ""
            )
            
        except Exception as e:
            session.rollback()
            logger.error(f"GraphQL activate_role error: {e}")
            raise ValueError(str(e))
            
        finally:
            session.close()
    
    @strawberry.field
    def inactivate_role_with_reassignment(self, info: Info, input: RoleInactivationInput) -> bool:
        """Inactivate a role and reassign users to new roles - requires authentication"""
        # Require authentication
        require_authentication(info)
        
        db_manager = get_database_manager()
        session = db_manager.get_session()
        
        try:
            # Get role
            role = session.query(Role).filter(Role.id == input.role_id).first()
            if not role:
                raise ValueError("Role not found")
            
            # Get users with this role
            users_with_role = session.query(User).filter(
                User.role_id == input.role_id,
                User.is_active == True
            ).all()
            
            if input.assign_same:
                # Assign same role to all users
                if not input.role:
                    raise ValueError("Role code is required when assign_same is true")
                
                # Get new role
                new_role = session.query(Role).filter(Role.role_code == input.role).first()
                if not new_role:
                    raise ValueError("New role not found")
                
                # Update all users
                for user in users_with_role:
                    user.role_id = new_role.id
                    user.updated_at = datetime.utcnow()
            else:
                # Assign different roles to each user
                if not input.users:
                    raise ValueError("Users list is required when assign_same is false")
                
                # Create mapping of user names to new roles
                user_role_mapping = {}
                for user_assignment in input.users:
                    user_name = user_assignment.get('nameOfUsers')
                    role_code = user_assignment.get('role')
                    if user_name and role_code:
                        user_role_mapping[user_name] = role_code
                
                # Update users
                for user in users_with_role:
                    user_display_name = f"{user.first_name} {user.last_name}"
                    if user_display_name in user_role_mapping:
                        role_code = user_role_mapping[user_display_name]
                        new_role = session.query(Role).filter(Role.role_code == role_code).first()
                        if new_role:
                            user.role_id = new_role.id
                            user.updated_at = datetime.utcnow()
            
            # Inactivate the role
            role.is_active = False
            role.updated_at = datetime.utcnow()
            
            session.commit()
            return True
            
        except Exception as e:
            session.rollback()
            logger.error(f"GraphQL inactivate_role_with_reassignment error: {e}")
            raise ValueError(str(e))
            
        finally:
            session.close()
    
    @staticmethod
    def _create_role_permissions(session, role_id: int, permissions: List[dict]):
        """Helper method to create role permissions for modules"""
        try:
            for perm in permissions:
                module_code = perm.get('module_code')
                permission_codes = perm.get('permissions', [])
                
                # Get module
                module = session.query(Module).filter(
                    Module.module_code == module_code,
                    Module.is_active == True
                ).first()
                
                if not module:
                    logger.warning(f"Module not found: {module_code}")
                    continue
                    
                # Get permissions
                perms = session.query(Permission).filter(
                    Permission.permission_code.in_(permission_codes),
                    Permission.is_active == True
                ).all()
                
                # Create role-module-permission relationships
                for permission in perms:
                    # Check if relationship already exists
                    existing = session.query(RoleOrClientBasedModuleLevelPermission).filter(
                        RoleOrClientBasedModuleLevelPermission.role_id == role_id,
                        RoleOrClientBasedModuleLevelPermission.module_id == module.id,
                        RoleOrClientBasedModuleLevelPermission.permission_id == permission.id,
                        RoleOrClientBasedModuleLevelPermission.client_id.is_(None),  # Only role-based permissions
                        RoleOrClientBasedModuleLevelPermission.is_active == True
                    ).first()
                    
                    if not existing:
                        rmp = RoleOrClientBasedModuleLevelPermission(
                            role_id=role_id,
                            module_id=module.id,
                            permission_id=permission.id,
                            client_id=None,  # Role-based permission
                            is_active=True
                        )
                        session.add(rmp)
                        
            session.commit()
            
        except Exception as e:
            session.rollback()
            logger.error(f"Error creating role permissions: {e}")
            raise

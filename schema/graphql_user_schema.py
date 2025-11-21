#!/usr/bin/env python3
"""
GraphQL Schema for User Management - Clean Implementation with Authentication
Provides pure data responses without response formatting
Maintains consistency with REST API authentication system
"""

import strawberry
from typing import List, Optional
from strawberry.types import Info
from sqlalchemy.orm import joinedload
from sqlalchemy import or_
from database_models import User, Role, Client, get_database_manager
from datetime import datetime
import logging

# Import authentication context
from .graphql_auth_context import require_authentication, require_role, get_current_user, is_authenticated

logger = logging.getLogger(__name__)

@strawberry.type
class UserRoleType:
    """GraphQL type for Role"""
    id: int
    role_name: str
    role_code: str
    description: Optional[str]
    is_active: bool
    created_at: str
    updated_at: str

@strawberry.type
class UserClientType:
    """GraphQL type for Client"""
    id: int
    name: str
    code: str
    description: Optional[str]
    type: Optional[str]
    is_active: bool
    created_at: str
    updated_at: str

@strawberry.type
class UserType:
    """GraphQL type for User"""
    id: int
    username: str
    email: Optional[str]
    display_name: str
    first_name: str
    last_name: str
    temp_password: bool
    is_active: bool
    created_at: str
    updated_at: str
    role: Optional[UserRoleType]
    client: Optional[UserClientType]

@strawberry.input
class UserCreateInput:
    """Input type for creating a user"""
    first_name: str
    last_name: str
    email: str
    job_title: str
    role_id: int
    client_id: Optional[int] = None

@strawberry.input
class UserUpdateInput:
    """Input type for updating a user"""
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    email: Optional[str] = None
    job_title: Optional[str] = None
    role_id: Optional[int] = None
    client_id: Optional[int] = None
    is_active: Optional[bool] = None

@strawberry.input
class BulkUserCreateInput:
    """Input type for bulk user creation"""
    forms: strawberry.scalars.JSON

@strawberry.type
class UserQuery:
    """GraphQL Query root for users"""
    
    @strawberry.field
    def users(self, info: Info,
              id: Optional[int] = None,
              search: Optional[str] = None,
              status_filter: Optional[str] = None,
              limit: Optional[int] = 10,
              offset: Optional[int] = 0) -> List[UserType]:
        """Get users with filtering and pagination - requires authentication"""
        # Require authentication
        require_authentication(info)
        
        db_manager = get_database_manager()
        session = db_manager.get_session()
        
        try:
            query = session.query(User)
            
            # Single user by ID
            if id:
                query = query.filter(User.id == id)
            
            # Search functionality
            if search:
                query = query.filter(
                    or_(
                        User.first_name.ilike(f"%{search}%"),
                        User.last_name.ilike(f"%{search}%"),
                        User.email.ilike(f"%{search}%"),
                        User.username.ilike(f"%{search}%")
                    )
                )
            
            # Status filter
            if status_filter == 'active':
                query = query.filter(User.is_active == True)
            elif status_filter == 'inactive':
                query = query.filter(User.is_active == False)
            
            # Pagination
            query = query.offset(offset).limit(limit)
            
            # Execute query with relationships
            users = query.options(
                joinedload(User.role),
                joinedload(User.client)
            ).all()
            
            # Convert to GraphQL types
            return [
                UserType(
                    id=user.id,
                    username=user.username,
                    email=user.email,
                    display_name=user.display_name,
                    first_name=user.first_name,
                    last_name=user.last_name,
                    temp_password=user.temp_password,
                    is_active=user.is_active,
                    created_at=user.created_at.isoformat() if user.created_at else "",
                    updated_at=user.updated_at.isoformat() if user.updated_at else "",
                    role=UserRoleType(
                        id=user.role.id,
                        role_name=user.role.role_name,
                        role_code=user.role.role_code,
                        description=user.role.description,
                        is_active=user.role.is_active,
                        created_at=user.role.created_at.isoformat() if user.role.created_at else "",
                        updated_at=user.role.updated_at.isoformat() if user.role.updated_at else ""
                    ) if user.role else None,
                    client=UserClientType(
                        id=user.client.id,
                        name=user.client.name,
                        code=user.client.code,
                        description=user.client.description,
                        type=user.client.type,
                        is_active=user.client.is_active,
                        created_at=user.client.created_at.isoformat() if user.client.created_at else "",
                        updated_at=user.client.updated_at.isoformat() if user.client.updated_at else ""
                    ) if user.client else None
                ) for user in users
            ]
            
        except Exception as e:
            logger.error(f"GraphQL users query error: {e}")
            return []
            
        finally:
            session.close()
    
    @strawberry.field
    def user_edit_form(self, info: Info, user_id: int) -> Optional[UserType]:
        """Get user data for edit form - requires authentication"""
        # Require authentication
        require_authentication(info)
        db_manager = get_database_manager()
        session = db_manager.get_session()
        
        try:
            # Get user with relationships
            user = session.query(User).options(
                joinedload(User.role),
                joinedload(User.client)
            ).filter(User.id == user_id).first()
            
            if not user:
                logger.warning(f"User with ID {user_id} not found")
                return None
            
            return UserType(
                id=user.id,
                username=user.username,
                email=user.email,
                display_name=user.display_name,
                first_name=user.first_name,
                last_name=user.last_name,
                temp_password=user.temp_password,
                is_active=user.is_active,
                created_at=user.created_at.isoformat() if user.created_at else "",
                updated_at=user.updated_at.isoformat() if user.updated_at else "",
                role=UserRoleType(
                    id=user.role.id,
                    role_name=user.role.role_name,
                    role_code=user.role.role_code,
                    description=user.role.description,
                    is_active=user.role.is_active,
                    created_at=user.role.created_at.isoformat() if user.role.created_at else "",
                    updated_at=user.role.updated_at.isoformat() if user.role.updated_at else ""
                ) if user.role else None,
                client=UserClientType(
                    id=user.client.id,
                    name=user.client.name,
                    code=user.client.code,
                    description=user.client.description,
                    type=user.client.type,
                    is_active=user.client.is_active,
                    created_at=user.client.created_at.isoformat() if user.client.created_at else "",
                    updated_at=user.client.updated_at.isoformat() if user.client.updated_at else ""
                ) if user.client else None
            )
            
        except Exception as e:
            logger.error(f"GraphQL user_edit_form query error for user_id {user_id}: {e}")
            import traceback
            logger.error(f"Traceback: {traceback.format_exc()}")
            return None
            
        finally:
            session.close()

@strawberry.type
class UserMutation:
    """GraphQL Mutation root for users"""
    
    @strawberry.field
    def create_user(self, info: Info, input: UserCreateInput) -> Optional[UserType]:
        """Create a new user - requires authentication"""
        # Require authentication
        require_authentication(info)
        db_manager = get_database_manager()
        session = db_manager.get_session()
        
        try:
            # Check if email already exists
            existing_email = session.query(User).filter(User.email == input.email).first()
            if existing_email:
                raise ValueError("Email already exists")
            
            # Verify role exists
            role = session.query(Role).filter(Role.id == input.role_id).first()
            if not role:
                raise ValueError("Invalid role ID")
            
            # Generate unique username
            username = UserMutation._generate_unique_username(input.first_name, session)
            
            # Generate temporary password
            temp_password = UserMutation._generate_temporary_password()
            password_hash = UserMutation._get_password_hash(temp_password)
            
            # Create new user
            new_user = User(
                username=username,
                email=input.email,
                display_name=f"{input.first_name} {input.last_name}",
                first_name=input.first_name,
                last_name=input.last_name,
                password_hash=password_hash,
                temp_password=True,
                role_id=input.role_id,
                client_id=input.client_id,
                is_active=True
            )
            
            session.add(new_user)
            session.flush()  # Flush to get the ID
            
            # Get the created user with relationships
            created_user = session.query(User).options(
                joinedload(User.role),
                joinedload(User.client)
            ).filter(User.id == new_user.id).first()
            
            session.commit()
            
            # Convert to GraphQL type
            return UserType(
                id=created_user.id,
                username=created_user.username,
                email=created_user.email,
                display_name=created_user.display_name,
                first_name=created_user.first_name,
                last_name=created_user.last_name,
                temp_password=created_user.temp_password,
                is_active=created_user.is_active,
                created_at=created_user.created_at.isoformat() if created_user.created_at else "",
                updated_at=created_user.updated_at.isoformat() if created_user.updated_at else "",
                role=UserRoleType(
                    id=created_user.role.id,
                    role_name=created_user.role.role_name,
                    role_code=created_user.role.role_code,
                    description=created_user.role.description,
                    is_active=created_user.role.is_active,
                    created_at=created_user.role.created_at.isoformat() if created_user.role.created_at else "",
                    updated_at=created_user.role.updated_at.isoformat() if created_user.role.updated_at else ""
                ) if created_user.role else None,
                client=UserClientType(
                    id=created_user.client.id,
                    name=created_user.client.name,
                    code=created_user.client.code,
                    description=created_user.client.description,
                    type=created_user.client.type,
                    is_active=created_user.client.is_active,
                    created_at=created_user.client.created_at.isoformat() if created_user.client.created_at else "",
                    updated_at=created_user.client.updated_at.isoformat() if created_user.client.updated_at else ""
                ) if created_user.client else None
            )
            
        except Exception as e:
            session.rollback()
            logger.error(f"GraphQL create_user error: {e}")
            raise ValueError(str(e))
            
        finally:
            session.close()
    
    @strawberry.field
    def update_user(self, info: Info, user_id: int, input: UserUpdateInput) -> Optional[UserType]:
        """Update an existing user - requires authentication"""
        # Require authentication
        require_authentication(info)
        db_manager = get_database_manager()
        session = db_manager.get_session()
        
        try:
            # Get user
            user = session.query(User).options(
                joinedload(User.role),
                joinedload(User.client)
            ).filter(User.id == user_id).first()
            
            if not user:
                raise ValueError("User not found")
            
            # Update fields if provided
            if input.first_name is not None:
                user.first_name = input.first_name
            if input.last_name is not None:
                user.last_name = input.last_name
            if input.email is not None:
                # Check if email already exists for another user
                existing_email = session.query(User).filter(
                    User.email == input.email,
                    User.id != user_id
                ).first()
                if existing_email:
                    raise ValueError("Email already exists")
                user.email = input.email
            
            # Update display name if first or last name changed
            if input.first_name is not None or input.last_name is not None:
                user.display_name = f"{user.first_name} {user.last_name}"
            
            if input.role_id is not None:
                # Verify role exists
                role = session.query(Role).filter(Role.id == input.role_id).first()
                if not role:
                    raise ValueError("Invalid role ID")
                user.role_id = input.role_id
            
            if input.client_id is not None:
                user.client_id = input.client_id
            
            if input.is_active is not None:
                user.is_active = input.is_active
            
            user.updated_at = datetime.utcnow()
            
            session.commit()
            
            # Refresh user with relationships
            session.refresh(user)
            user = session.query(User).options(
                joinedload(User.role),
                joinedload(User.client)
            ).filter(User.id == user_id).first()
            
            # Convert to GraphQL type
            return UserType(
                id=user.id,
                username=user.username,
                email=user.email,
                display_name=user.display_name,
                first_name=user.first_name,
                last_name=user.last_name,
                temp_password=user.temp_password,
                is_active=user.is_active,
                created_at=user.created_at.isoformat() if user.created_at else "",
                updated_at=user.updated_at.isoformat() if user.updated_at else "",
                role=UserRoleType(
                    id=user.role.id,
                    role_name=user.role.role_name,
                    role_code=user.role.role_code,
                    description=user.role.description,
                    is_active=user.role.is_active,
                    created_at=user.role.created_at.isoformat() if user.role.created_at else "",
                    updated_at=user.role.updated_at.isoformat() if user.role.updated_at else ""
                ) if user.role else None,
                client=UserClientType(
                    id=user.client.id,
                    name=user.client.name,
                    code=user.client.code,
                    description=user.client.description,
                    type=user.client.type,
                    is_active=user.client.is_active,
                    created_at=user.client.created_at.isoformat() if user.client.created_at else "",
                    updated_at=user.client.updated_at.isoformat() if user.client.updated_at else ""
                ) if user.client else None
            )
            
        except Exception as e:
            session.rollback()
            logger.error(f"GraphQL update_user error: {e}")
            raise ValueError(str(e))
            
        finally:
            session.close()
    
    @strawberry.field
    def delete_user(self, info: Info, user_id: int) -> bool:
        """Delete a user (soft delete) - requires authentication"""
        # Require authentication
        require_authentication(info)
        db_manager = get_database_manager()
        session = db_manager.get_session()
        
        try:
            user = session.query(User).filter(User.id == user_id).first()
            if not user:
                raise ValueError("User not found")
            
            # Soft delete by setting is_active to False
            user.is_active = False
            user.updated_at = datetime.utcnow()
            
            session.commit()
            return True
            
        except Exception as e:
            session.rollback()
            logger.error(f"GraphQL delete_user error: {e}")
            raise ValueError(str(e))
            
        finally:
            session.close()
    
    @strawberry.field
    def toggle_user_status(self, info: Info, user_id: int) -> Optional[UserType]:
        """Toggle user active/inactive status - requires authentication"""
        # Require authentication
        require_authentication(info)
        db_manager = get_database_manager()
        session = db_manager.get_session()
        
        try:
            user = session.query(User).options(
                joinedload(User.role),
                joinedload(User.client)
            ).filter(User.id == user_id).first()
            
            if not user:
                raise ValueError("User not found")
            
            # Toggle status
            user.is_active = not user.is_active
            user.updated_at = datetime.utcnow()
            
            session.commit()
            
            # Refresh user with relationships
            session.refresh(user)
            user = session.query(User).options(
                joinedload(User.role),
                joinedload(User.client)
            ).filter(User.id == user_id).first()
            
            # Convert to GraphQL type
            return UserType(
                id=user.id,
                username=user.username,
                email=user.email,
                display_name=user.display_name,
                first_name=user.first_name,
                last_name=user.last_name,
                temp_password=user.temp_password,
                is_active=user.is_active,
                created_at=user.created_at.isoformat() if user.created_at else "",
                updated_at=user.updated_at.isoformat() if user.updated_at else "",
                role=UserRoleType(
                    id=user.role.id,
                    role_name=user.role.role_name,
                    role_code=user.role.role_code,
                    description=user.role.description,
                    is_active=user.role.is_active,
                    created_at=user.role.created_at.isoformat() if user.role.created_at else "",
                    updated_at=user.role.updated_at.isoformat() if user.role.updated_at else ""
                ) if user.role else None,
                client=UserClientType(
                    id=user.client.id,
                    name=user.client.name,
                    code=user.client.code,
                    description=user.client.description,
                    type=user.client.type,
                    is_active=user.client.is_active,
                    created_at=user.client.created_at.isoformat() if user.client.created_at else "",
                    updated_at=user.client.updated_at.isoformat() if user.client.updated_at else ""
                ) if user.client else None
            )
            
        except Exception as e:
            session.rollback()
            logger.error(f"GraphQL toggle_user_status error: {e}")
            raise ValueError(str(e))
            
        finally:
            session.close()
    
    @strawberry.field
    def create_users_bulk(self, info: Info, input: BulkUserCreateInput) -> List[UserType]:
        """Create multiple users from forms - requires authentication"""
        # Require authentication
        require_authentication(info)
        db_manager = get_database_manager()
        session = db_manager.get_session()
        
        created_users = []
        
        try:
            # Process each form in the input
            forms_data = input.forms
            if not isinstance(forms_data, dict):
                raise ValueError("Invalid input format")
            
            for form_key, user_data in forms_data.items():
                try:
                    # Validate required fields
                    if not all(key in user_data for key in ['first_name', 'last_name', 'email', 'job_title', 'role_id']):
                        continue
                    
                    # Generate unique username
                    username = UserMutation._generate_unique_username(user_data['first_name'], session)
                    
                    # Check if email already exists
                    existing_email = session.query(User).filter(User.email == user_data['email']).first()
                    if existing_email:
                        continue
                    
                    # Verify role exists
                    role = session.query(Role).filter(Role.id == user_data['role_id']).first()
                    if not role:
                        continue
                    
                    # Generate temporary password
                    temp_password = UserMutation._generate_temporary_password()
                    password_hash = UserMutation._get_password_hash(temp_password)
                    
                    # Create new user
                    new_user = User(
                        username=username,
                        email=user_data['email'],
                        display_name=f"{user_data['first_name']} {user_data['last_name']}",
                        first_name=user_data['first_name'],
                        last_name=user_data['last_name'],
                        password_hash=password_hash,
                        temp_password=True,
                        role_id=user_data['role_id'],
                        client_id=None,
                        is_active=True
                    )
                    
                    session.add(new_user)
                    session.flush()  # Flush to get the ID
                    
                    # Get the created user with relationships
                    created_user = session.query(User).options(
                        joinedload(User.role),
                        joinedload(User.client)
                    ).filter(User.id == new_user.id).first()
                    
                    # Convert to GraphQL type
                    user_type = UserType(
                        id=created_user.id,
                        username=created_user.username,
                        email=created_user.email,
                        display_name=created_user.display_name,
                        first_name=created_user.first_name,
                        last_name=created_user.last_name,
                        temp_password=created_user.temp_password,
                        is_active=created_user.is_active,
                        created_at=created_user.created_at.isoformat() if created_user.created_at else "",
                        updated_at=created_user.updated_at.isoformat() if created_user.updated_at else "",
                        role=UserRoleType(
                            id=created_user.role.id,
                            role_name=created_user.role.role_name,
                            role_code=created_user.role.role_code,
                            description=created_user.role.description,
                            is_active=created_user.role.is_active,
                            created_at=created_user.role.created_at.isoformat() if created_user.role.created_at else "",
                            updated_at=created_user.role.updated_at.isoformat() if created_user.role.updated_at else ""
                        ) if created_user.role else None,
                        client=UserClientType(
                            id=created_user.client.id,
                            name=created_user.client.name,
                            code=created_user.client.code,
                            description=created_user.client.description,
                            type=created_user.client.type,
                            is_active=created_user.client.is_active,
                            created_at=created_user.client.created_at.isoformat() if created_user.client.created_at else "",
                            updated_at=created_user.client.updated_at.isoformat() if created_user.client.updated_at else ""
                        ) if created_user.client else None
                    )
                    
                    created_users.append(user_type)
                    
                except Exception as e:
                    logger.error(f"Error creating user from form {form_key}: {e}")
                    continue
            
            # Commit all successful creations
            session.commit()
            return created_users
            
        except Exception as e:
            session.rollback()
            logger.error(f"GraphQL bulk user creation error: {e}")
            raise ValueError(str(e))
            
        finally:
            session.close()
    
    @staticmethod
    def _generate_unique_username(first_name: str, session) -> str:
        """Generate a unique username based on first name"""
        import random
        import re
        
        # Clean and normalize first name
        base_username = re.sub(r'[^a-zA-Z]', '', first_name.lower().strip())
        if not base_username:
            base_username = "user"
        
        max_attempts = 10
        
        # Try with 4-digit random suffix
        for attempt in range(max_attempts):
            random_suffix = random.randint(1000, 9999)
            username = f"{base_username}{random_suffix}"
            
            # Check if username exists
            existing = session.query(User).filter(User.username == username).first()
            if not existing:
                return username
        
        # Fallback with timestamp if all attempts fail
        import time
        timestamp_suffix = int(time.time()) % 10000
        return f"{base_username}{timestamp_suffix}"
    
    @staticmethod
    def _generate_temporary_password() -> str:
        """Generate a secure temporary password"""
        import secrets
        import string
        
        # Define character sets
        uppercase = string.ascii_uppercase
        lowercase = string.ascii_lowercase
        digits = string.digits
        special_chars = "!@#$%"
        
        # Ensure at least one character from each set
        password = [
            secrets.choice(uppercase),
            secrets.choice(lowercase),
            secrets.choice(digits),
            secrets.choice(special_chars)
        ]
        
        # Fill remaining 4 characters from all sets
        all_chars = uppercase + lowercase + digits + special_chars
        for _ in range(4):
            password.append(secrets.choice(all_chars))
        
        # Shuffle the password list to randomize positions
        secrets.SystemRandom().shuffle(password)
        
        return ''.join(password)
    
    @staticmethod
    def _get_password_hash(password: str) -> str:
        """Hash the password"""
        from rbac.utils.auth import getPasswordHash
        return getPasswordHash(password)

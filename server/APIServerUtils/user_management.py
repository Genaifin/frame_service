#!/usr/bin/env python3
"""
User Management API Utilities
Provides CRUD operations for users based on the dashboard requirements
"""

import logging
import sys
import json

from fastapi import HTTPException, status
from fastapi.responses import JSONResponse
from typing import List, Dict, Any, Optional
from datetime import datetime

from pathlib import Path
from pathlib import Path

# Ensure project root is on sys.path for Docker/runtime compatibility
_project_root = Path(__file__).resolve().parents[2]
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

# Import database models
from database_models import get_database_manager, User, Client, Role, Module, Permission
from models.permission_models import RoleOrClientBasedModuleLevelPermission
from sqlalchemy import desc

logger = logging.getLogger(__name__)

class UserManagementService:
    """Service class for user management operations"""
    
    def __init__(self):
        self.db_manager = get_database_manager()
    
    def _generate_unique_username(self, first_name: str, session) -> str:
        """Generate a unique username based on first name with collision detection"""
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
    
    def _generate_temporary_password(self) -> str:
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
    
    async def get_all_users(
        self, 
        search: Optional[str] = None,
        status_filter: Optional[str] = None,
        page: int = 1,
        page_size: int = 10
    ) -> Dict[str, Any]:
        """Get all users with pagination, search, and filtering"""
        try:
            session = self.db_manager.get_session()

            query = session.query(
                User.id,
                User.username,
                User.email,
                User.display_name,
                User.job_title,
                User.is_active,
                User.updated_at,
                User.created_at,
                User.client_id,
                Role.role_name.label("role_name"),
                Role.role_code.label("role_code"),
            ).join(Role, User.role_id == Role.id)
            
            # Apply search filter
            if search:
                search_term = f"%{search}%"
                query = query.filter(
                    (User.username.ilike(search_term)) |
                    (User.display_name.ilike(search_term)) |
                    (User.email.ilike(search_term))
                )
            
            # Apply status filter
            if status_filter and status_filter.lower() != "all":
                if status_filter.lower() == "active":
                    query = query.filter(User.is_active == True)
                elif status_filter.lower() == "inactive":
                    query = query.filter(User.is_active == False)
            
            # Get total count for pagination
            total_count = query.count()
            
            # Apply sorting by created_at in descending order (newest first) and pagination
            offset = (page - 1) * page_size
            users = query.order_by(desc(User.created_at)).offset(offset).limit(page_size).all()
            
            # Get user details with client and role information
            user_list = []
            for user in users:
                # Get user's client information directly from User.client_id
                clients = []
                roles = []
                if user.client_id:
                    client = session.query(Client).filter(Client.id == user.client_id).first()
                    if client:
                        clients.append(client.name)
                if user.role_name:
                    roles.append(user.role_name)
                
                user_data = {
                    'id': user.id,
                    'username': user.username,
                    'display_name': user.display_name,
                    'email': user.email,
                    'job_title': user.job_title or "",
                    'clients': clients,
                    'roles': roles,
                    'role_str': user.role_name if user.role_name else 'Unknown',
                    'role': user.role_code if user.role_code else 'unknown',
                    'status': 'Active' if user.is_active else 'Inactive',
                    'created_at': user.created_at.strftime('%m/%d/%Y') if user.created_at else None,
                    'updated_at': user.updated_at.strftime('%m/%d/%Y') if user.updated_at else None
                }
                user_list.append(user_data)
            
            # Load template and return organization format

            
            # Load template
            template_path = Path(__file__).parent.parent.parent / "frontendUtils" / "renders" / "organization_view_all_users.json"
            with open(template_path, 'r') as f:
                response = json.load(f)
            
            # Generate users row data
            users_row_data = []
            for user in user_list:
                user_row = {
                    "userID": user['id'],
                    "name": user['display_name'],
                    "jobTitle": user['job_title'],
                    "emailID": user['email'],
                    "role": user['role_str'],
                    "createdDate": user['created_at'],
                    "status": user['status']
                }
                users_row_data.append(user_row)
            
            # Update template with dynamic data
            response["rowData"] = users_row_data
            response["pagination"]["current_page"] = page
            response["pagination"]["page_size"] = page_size
            response["pagination"]["total_pages"] = (total_count + page_size - 1) // page_size
            
            return response
            
        except Exception as e:
            logger.error(f"Error getting users: {e}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to retrieve users: {str(e)}"
            )
        finally:
            session.close()

    async def get_user_by_id(self, user_id: int) -> Dict[str, Any]:
        """Get a specific user by ID with full details in organization format"""
        try:
            session = self.db_manager.get_session()
            
            from sqlalchemy.orm import joinedload
            user = session.query(User).options(joinedload(User.role)).filter(User.id == user_id).first()
            if not user:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail="User not found"
                )
            
            # Load the template from JSON file
            template_path = Path(__file__).parent.parent.parent / "frontendUtils" / "renders" / "organization_user_details.json"
            with open(template_path, 'r') as f:
                response = json.load(f)
            
            # Format created date to MM/DD/YYYY
            created_date = user.created_at.strftime('%m/%d/%Y') if user.created_at else None
            
            # Use actual database user ID instead of formatted version
            actual_user_id = user.id
            
            # Update the onEditClick user_id parameter with actual user ID
            response["onEditClick"]["parameters"][1]["value"] = actual_user_id
            
            # Update the sections fields with dynamic values
            fields = response["sections"][0]["fields"]
            for field in fields:
                if field["label"] == "First Name":
                    field["value"] = user.first_name or ""
                elif field["label"] == "Last Name":
                    field["value"] = user.last_name or ""
                elif field["label"] == "User ID":
                    field["value"] = actual_user_id
                elif field["label"] == "Email ID":
                    field["value"] = user.email or ""
                elif field["label"] == "Job Title":
                    field["value"] = user.job_title or ""
                elif field["label"] == "Role":
                    field["value"] = user.role.role_name if user.role else "Unknown"
                elif field["label"] == "Status":
                    field["value"] = "Active" if user.is_active else "Inactive"
                elif field["label"] == "Created Date":
                    field["value"] = created_date or ""
            
            # Make footer dynamic based on user status
            footer_field = response["footer"]["fields"][0]
            
            # Dynamic button text
            footer_field["buttonText"] = "Mark as Inactive?" if user.is_active else "Mark as Active?"
            
            # Dynamic button color
            footer_field["buttonColor"] = "destructive" if user.is_active else None
            
            # Dynamic confirmation dialog
            confirmation = footer_field["onConfirmation"]
            confirmation["title"] = f"Make User {'Inactive' if user.is_active else 'Active'}?"
            confirmation["description"] = f"Are you sure you want to mark {user.display_name} as {'inactive' if user.is_active else 'active'}?"
            confirmation["buttonText"] = f"Mark as {'Inactive' if user.is_active else 'Active'}"
            confirmation["buttonColor"] = "destructive" if user.is_active else None
            
            # Update click action data with actual user ID and correct active status
            confirmation["clickAction"]["data"]["user_id"] = actual_user_id
            confirmation["clickAction"]["data"]["active"] = not user.is_active
            
            return response
            
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error getting user {user_id}: {e}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to retrieve user: {str(e)}"
            )
        finally:
            session.close()
    
    async def create_user(self, user_data: Dict[str, Any], authenticated_username: str = None) -> Dict[str, Any]:
        """Create a new user with first_name/last_name format"""
        try:
            session = self.db_manager.get_session()
            
            # Get client_id from authenticated user
            if authenticated_username:
                auth_user = session.query(User).filter(User.username == authenticated_username).first()
                if not auth_user:
                    raise HTTPException(
                        status_code=status.HTTP_401_UNAUTHORIZED,
                        detail="Authenticated user not found"
                    )
                if not auth_user.client_id:
                    raise HTTPException(
                        status_code=status.HTTP_400_BAD_REQUEST,
                        detail="Authenticated user has no associated client"
                    )
                client_id = auth_user.client_id
            else:
                # Fallback to request client_id if no authenticated username provided
                client_id = user_data.get('client_id')
                if not client_id:
                    raise HTTPException(
                        status_code=status.HTTP_400_BAD_REQUEST,
                        detail="Missing client_id"
                    )
            
            # Validate required fields for new format (removed client_id since it comes from token)
            required_fields = ['first_name', 'last_name', 'email', 'job_title', 'role_name', 'password']
            for field in required_fields:
                if field not in user_data or not user_data[field]:
                    raise HTTPException(
                        status_code=status.HTTP_400_BAD_REQUEST,
                        detail=f"Missing required field: {field}"
                    )
            
            # Check if email already exists
            existing_email = session.query(User).filter(User.email == user_data['email']).first()
            if existing_email:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="Email already exists"
                )
            
            # Validate role_name exists (case-insensitive)
            role_name = user_data['role_name'].lower().strip()
            role = session.query(Role).filter(Role.role_name.ilike(role_name)).first()
            if not role:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"Invalid role_name: '{user_data['role_name']}'. Role does not exist."
                )
            
            # Validate client_id exists
            client = session.query(Client).filter(Client.id == client_id).first()
            if not client:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"Invalid client_id: '{client_id}'. Client does not exist."
                )
            
            # Generate username
            username = self._generate_unique_username(user_data['first_name'], session)
            
            # Use provided password
            password = user_data['password']
            
            # Generate display name
            display_name = f"{user_data['first_name']} {user_data['last_name']}"
            
            # Hash the password
            from rbac.utils.auth import getPasswordHash
            password_hash = getPasswordHash(password)
            
            # Create new user - password is provided by user, not temporary
            new_user = User(
                username=username,
                display_name=display_name,
                first_name=user_data['first_name'],
                last_name=user_data['last_name'],
                job_title=user_data['job_title'],
                email=user_data['email'],
                password_hash=password_hash,
                temp_password=False,
                role_id=role.id,
                client_id=client_id,
                is_active=user_data.get('is_active', True)
            )
            
            session.add(new_user)
            session.commit()
            session.refresh(new_user)
            
            return {
                'success': True,
                'message': 'User created successfully',
                'data': {
                    'id': new_user.id,
                    'username': new_user.username,
                    'display_name': new_user.display_name,
                    'first_name': new_user.first_name,
                    'last_name': new_user.last_name,
                    'email': new_user.email,
                    'role': new_user.role.role_code if new_user.role else 'unknown',
                    'status': 'Active' if new_user.is_active else 'Inactive',
                    'temp_password': new_user.temp_password
                },
                'credentials': {
                    'username': username,
                    'password': password
                }
            }
            
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error creating user: {e}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to create user: {str(e)}"
            )
        finally:
            session.close()
    
    
    async def bulk_create_users(self, users_data: Dict[str, Dict[str, Any]], authenticated_username: str = None) -> Dict[str, Any]:
        """Create multiple users (bulk creation)"""
        results = {}
        credentials = {}
        successful_count = 0
        failed_count = 0
        
        for form_id, user_data in users_data.items():
            try:
                # Create individual user using existing create_user method
                result = await self.create_user(user_data, authenticated_username)
                
                if result.get('success'):
                    results[form_id] = {
                        'success': True,
                        'message': 'User created successfully',
                        'data': result['data']
                    }
                    credentials[form_id] = result['credentials']
                    successful_count += 1
                else:
                    results[form_id] = {
                        'success': False,
                        'message': result.get('message', 'User creation failed'),
                        'error': 'Unknown error'
                    }
                    failed_count += 1
                    
            except Exception as e:
                results[form_id] = {
                    'success': False,
                    'message': f'Failed to create user: {str(e)}',
                    'error': str(e)
                }
                failed_count += 1
        
        total_users = len(users_data)
        overall_success = failed_count == 0
        
        return {
            'success': overall_success,
            'message': f'Bulk user creation completed. {successful_count}/{total_users} users created successfully.',
            'total_users': total_users,
            'successful_creations': successful_count,
            'failed_creations': failed_count,
            'results': results,
            'credentials': credentials
        }
    
    async def update_user(self, user_id: int, user_data: Dict[str, Any]) -> Dict[str, Any]:
        """Update an existing user with new format (first_name/last_name and role validation)"""
        try:
            session = self.db_manager.get_session()
            
            # Get existing user
            user = session.query(User).filter(User.id == user_id).first()
            if not user:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail="User not found"
                )
            
            # Check if email is being changed and if it already exists
            if 'email' in user_data and user_data['email'] and user_data['email'] != user.email:
                existing_email = session.query(User).filter(User.email == user_data['email']).first()
                if existing_email:
                    raise HTTPException(
                        status_code=status.HTTP_400_BAD_REQUEST,
                        detail="Email already exists"
                    )
            
            # Validate role if provided
            if 'role_name' in user_data and user_data['role_name']:
                role = session.query(Role).filter(Role.role_code == user_data['role_name'].lower()).first()
                if not role:
                    raise HTTPException(
                        status_code=status.HTTP_400_BAD_REQUEST,
                        detail=f"Invalid role: '{user_data['role_name']}'. Role does not exist."
                    )
                user.role_id = role.id
            
            # Update user fields
            if 'first_name' in user_data and user_data['first_name']:
                user.first_name = user_data['first_name']
            
            if 'last_name' in user_data and user_data['last_name']:
                user.last_name = user_data['last_name']
            
            # Update display_name if first_name or last_name changed
            if ('first_name' in user_data and user_data['first_name']) or ('last_name' in user_data and user_data['last_name']):
                user.display_name = f"{user.first_name} {user.last_name}"
            
            if 'email' in user_data and user_data['email']:
                user.email = user_data['email']
            
            if 'job_title' in user_data and user_data['job_title']:
                user.job_title = user_data['job_title']
            
            if 'client_id' in user_data:
                user.client_id = user_data['client_id']
            
            if 'is_active' in user_data:
                user.is_active = user_data['is_active']
            
            # Handle password update
            if 'password' in user_data and user_data['password']:
                from rbac.utils.auth import getPasswordHash
                user.password_hash = getPasswordHash(user_data['password'])
                user.temp_password = False
            
            # Update timestamp
            user.updated_at = datetime.utcnow()
            
            session.commit()
            
            return {
                'success': True,
                'message': 'User updated successfully',
                'data': {
                    'id': user.id,
                    'username': user.username,
                    'display_name': user.display_name,
                    'first_name': user.first_name,
                    'last_name': user.last_name,
                    'email': user.email,
                    'role': user.role.role_code if user.role else 'unknown',
                    'status': 'Active' if user.is_active else 'Inactive',
                    'temp_password': user.temp_password
                }
            }
            
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error updating user {user_id}: {e}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to update user: {str(e)}"
            )
        finally:
            session.close()
    
    async def delete_user(self, user_id: int) -> Dict[str, Any]:
        """Delete a user (soft delete by setting is_active to False)"""
        try:
            session = self.db_manager.get_session()
            
            user = session.query(User).filter(User.id == user_id).first()
            if not user:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail="User not found"
                )
            
            # Soft delete - set is_active to False
            user.is_active = False
            user.updated_at = datetime.utcnow()
            
            # Remove client assignment
            user.client_id = None
            
            session.commit()
            
            return {
                'success': True,
                'message': 'User deleted successfully'
            }
            
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error deleting user {user_id}: {e}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to delete user: {str(e)}"
            )
        finally:
            session.close()
    
    async def toggle_user_status(self, user_id: int, is_active: bool) -> Dict[str, Any]:
        """Toggle user active/inactive status"""
        try:
            session = self.db_manager.get_session()
            
            user = session.query(User).filter(User.id == user_id).first()
            if not user:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail="User not found"
                )
            
            # Update user status
            user.is_active = is_active
            user.updated_at = datetime.utcnow()
            
            session.commit()
            
            return {
                'success': True,
                'message': f'User {"activated" if is_active else "deactivated"} successfully',
                'data': {
                    'id': user.id,
                    'username': user.username,
                    'display_name': user.display_name,
                    'email': user.email,
                    'status': 'Active' if user.is_active else 'Inactive'
                }
            }
            
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error toggling user status {user_id}: {e}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to update user status: {str(e)}"
            )
        finally:
            session.close()

    async def inactivate_role_with_user_reassignment(self, role_data: Dict[str, Any]) -> Dict[str, Any]:
        """Inactivate a role and reassign users to new roles"""
        try:
            session = self.db_manager.get_session()
            
            # Extract role_id from the request
            role_id_str = role_data.get('role_id', '')
            if not role_id_str:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="role_id is required"
                )
            
            # Handle both ROLE0001 format and raw ID format
            if role_id_str.startswith('ROLE'):
                role_id = int(role_id_str.replace('ROLE', ''))
            else:
                role_id = int(role_id_str)
            
            # Get the role to inactivate
            role_to_inactivate = session.query(Role).filter(Role.id == role_id).first()
            if not role_to_inactivate:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail="Role not found"
                )
            
            # Get all active users with this role
            users_to_reassign = session.query(User).filter(
                User.role_id == role_id,
                User.is_active == True
            ).all()
            
            if not users_to_reassign:
                # No users to reassign, just inactivate the role
                role_to_inactivate.is_active = False
                role_to_inactivate.updated_at = datetime.utcnow()
                session.commit()
                
                return {
                    'success': True,
                    'message': f'Role "{role_to_inactivate.role_name}" inactivated successfully. No users were assigned to this role.',
                    'data': {
                        'role_id': role_id,
                        'role_name': role_to_inactivate.role_name,
                        'users_reassigned': 0,
                        'role_status': 'Inactive'
                    }
                }
            
            assign_same = role_data.get('assignSame', False)
            reassignment_results = []
            
            if assign_same:
                # Assign same role to all users
                role_name = role_data.get('role', '')
                if not role_name:
                    raise HTTPException(
                        status_code=status.HTTP_400_BAD_REQUEST,
                        detail="role is required when assignSame is true"
                    )
                
                # Convert role name to snake_case format (e.g., "Admin" -> "admin", "System Admin" -> "system_admin")
                new_role_code = role_name.lower().replace(' ', '_').replace('-', '_')
                
                # Find the new role
                new_role = session.query(Role).filter(Role.role_code == new_role_code).first()
                if not new_role:
                    raise HTTPException(
                        status_code=status.HTTP_400_BAD_REQUEST,
                        detail=f"Role '{new_role_code}' not found"
                    )
                
                # Reassign all users to the new role
                for user in users_to_reassign:
                    user.role_id = new_role.id
                    user.updated_at = datetime.utcnow()
                    reassignment_results.append({
                        'user_id': user.id,
                        'username': user.username,
                        'display_name': user.display_name,
                        'old_role': role_to_inactivate.role_name,
                        'new_role': new_role.role_name
                    })
                
            else:
                # Assign different roles to each user
                users_data = role_data.get('users', [])
                if not users_data:
                    raise HTTPException(
                        status_code=status.HTTP_400_BAD_REQUEST,
                        detail="users array is required when assignSame is false"
                    )
                
                # Create a mapping of user names to new roles
                user_role_mapping = {}
                for user_data in users_data:
                    user_name = user_data.get('nameOfUsers', '')
                    role_name = user_data.get('role', '')
                    if user_name and role_name:
                        # Convert role name to snake_case format (e.g., "Admin" -> "admin", "System Admin" -> "system_admin")
                        new_role_code = role_name.lower().replace(' ', '_').replace('-', '_')
                        user_role_mapping[user_name] = new_role_code
                
                # Reassign users based on the mapping
                for user in users_to_reassign:
                    user_display_name = user.display_name
                    if user_display_name in user_role_mapping:
                        new_role_code = user_role_mapping[user_display_name]
                        new_role = session.query(Role).filter(Role.role_code == new_role_code).first()
                        
                        if new_role:
                            user.role_id = new_role.id
                            user.updated_at = datetime.utcnow()
                            reassignment_results.append({
                                'user_id': user.id,
                                'username': user.username,
                                'display_name': user.display_name,
                                'old_role': role_to_inactivate.role_name,
                                'new_role': new_role.role_name
                            })
                        else:
                            reassignment_results.append({
                                'user_id': user.id,
                                'username': user.username,
                                'display_name': user.display_name,
                                'old_role': role_to_inactivate.role_name,
                                'new_role': None,
                                'error': f"Role '{new_role_code}' not found"
                            })
                    else:
                        reassignment_results.append({
                            'user_id': user.id,
                            'username': user.username,
                            'display_name': user.display_name,
                            'old_role': role_to_inactivate.role_name,
                            'new_role': None,
                            'error': f"No new role specified for user '{user_display_name}'"
                        })
            
            # Inactivate the role
            role_to_inactivate.is_active = False
            role_to_inactivate.updated_at = datetime.utcnow()
            
            session.commit()
            
            return {
                'success': True,
                'message': f'Role "{role_to_inactivate.role_name}" inactivated successfully. {len(reassignment_results)} users reassigned.',
                'data': {
                    'role_id': role_id,
                    'role_name': role_to_inactivate.role_name,
                    'users_reassigned': len(reassignment_results),
                    'role_status': 'Inactive',
                    'reassignment_results': reassignment_results
                }
            }
            
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error inactivating role with user reassignment: {e}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to inactivate role: {str(e)}"
            )
        finally:
            session.close()

    async def activate_role(self, role_id: int) -> Dict[str, Any]:
        """Activate a role by setting is_active to True"""
        try:
            session = self.db_manager.get_session()
            
            # Get the role to activate
            role = session.query(Role).filter(Role.id == role_id).first()
            if not role:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail="Role not found"
                )
            
            # Check if role is already active
            if role.is_active:
                return {
                    'success': True,
                    'message': f'Role "{role.role_name}" is already active',
                    'data': {
                        'role_id': role_id,
                        'role_name': role.role_name,
                        'role_status': 'Active'
                    }
                }
            
            # Activate the role
            role.is_active = True
            role.updated_at = datetime.utcnow()
            
            session.commit()
            
            return {
                'success': True,
                'message': f'Role "{role.role_name}" activated successfully',
                'data': {
                    'role_id': role_id,
                    'role_name': role.role_name,
                    'role_status': 'Active'
                }
            }
            
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error activating role {role_id}: {e}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to activate role: {str(e)}"
            )
        finally:
            session.close()

    async def get_role_details(self, role_id: int, edit_mode: bool = False) -> Dict[str, Any]:
        """Get detailed role information including permissions and users"""
        try:
            session = self.db_manager.get_session()
            
            # Get the role
            role = session.query(Role).filter(Role.id == role_id).first()
            if not role:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail="Role not found"
                )
            
            if edit_mode:
                # Return edit form format
                return await self._get_role_edit_form(role, session)
            else:
                # Return detail view format
                return await self._get_role_detail_view(role, session)
                
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error getting role details {role_id}: {e}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to retrieve role details: {str(e)}"
            )
        finally:
            session.close()

    async def _get_role_detail_view(self, role: Role, session) -> Dict[str, Any]:
        """Get role details in detail view format"""
        # Load the template
        template_path = Path(__file__).parent.parent.parent / "frontendUtils" / "renders" / "organization_roles_detail_response.json"
        with open(template_path, 'r') as f:
            response = json.load(f)
        
        # Get users with this role
        users_with_role = session.query(User).filter(
            User.role_id == role.id,
            User.is_active == True
        ).all()
        
        user_names = [user.display_name for user in users_with_role]
        
        # Get role permissions for base-level modules (Frame, NAV Validus)
        role_permissions = session.query(
            Module.module_name,
            Permission.permission_code,
            RoleOrClientBasedModuleLevelPermission.is_active
        ).join(RoleOrClientBasedModuleLevelPermission, Module.id == RoleOrClientBasedModuleLevelPermission.module_id)\
         .join(Permission, RoleOrClientBasedModuleLevelPermission.permission_id == Permission.id)\
         .filter(RoleOrClientBasedModuleLevelPermission.role_id == role.id)\
         .filter(RoleOrClientBasedModuleLevelPermission.client_id.is_(None))\
         .filter(Module.module_name.in_(['Frame', 'NAV Validus']))\
         .all()
        
        # Group permissions by module
        module_permissions = {}
        for module_name, permission_code, is_active in role_permissions:
            if is_active:
                if module_name not in module_permissions:
                    module_permissions[module_name] = {}
                module_permissions[module_name][permission_code] = True
        
        # Format created date
        created_date = role.created_at.strftime('%m/%d/%Y') if role.created_at else "N/A"
        
        # Update template with dynamic data
        # Update onEditClick parameter
        response["onEditClick"]["parameters"][0]["value"] = role.id
        
        # Update sections fields
        fields = response["sections"][0]["fields"]
        for field in fields:
            if field["label"] == "Role Name":
                field["value"] = role.role_name
            elif field["label"] == "Description":
                field["value"] = role.description or "No description provided"
            elif field["label"] == "Role ID":
                field["value"] = role.id
            elif field["label"] == "Status":
                field["value"] = "Active" if role.is_active else "Inactive"
            elif field["label"] == "Created Date":
                field["value"] = created_date
        
        # Update footer with dynamic logic based on role.is_active status
        footer_field = response["footer"]["fields"][0]
        
        # Dynamic button text
        footer_field["buttonText"] = "Mark as Inactive?" if role.is_active else "Mark as Active?"
        
        # Dynamic button color
        footer_field["buttonColor"] = "destructive" if role.is_active else None
        
        # Dynamic confirmation dialog
        confirmation = footer_field["onConfirmation"]
        confirmation["title"] = f"Make Role {'Inactive' if role.is_active else 'Active'}?"
        confirmation["description"] = f"Are you sure you want to mark {role.role_name} as {'inactive' if role.is_active else 'active'}?"
        confirmation["buttonText"] = f"Mark as {'Inactive' if role.is_active else 'Active'}"
        confirmation["buttonColor"] = "destructive" if role.is_active else None
        
        # Update makeInActive field based on role status
        response["makeInActive"] = role.is_active
        
        # Update click action for role status toggle using existing endpoints
        if role.is_active:
            # Get available roles for reassignment (exclude current role)
            available_roles = session.query(Role).filter(
                Role.is_active == True,
                Role.id != role.id  # Exclude current role
            ).order_by(Role.role_name).all()
            
            role_options = [role_item.role_name for role_item in available_roles]
            
            # Use existing inactivate/reassignment endpoint for active roles
            # Add role selection field to confirmation dialog
            confirmation["clickAction"] = {
                "type": "patchData",
                "patchAPIURL": "api/roles/inactivate/reassignment",
                "data": {
                    "role_id": str(role.id),
                    "assignSame": True,
                    "role": role_options[0] if role_options else "Admin"  # Use first available role or fallback
                },
                "actionAfterAPICall": {
                    "type": "refreshModule",
                    "moduleName": "RolesTable"
                }
            }
            
            # Add role selection field to confirmation dialog if multiple roles available
            if len(role_options) > 1:
                confirmation["fields"] = [
                    {
                        "id": "reassign_role",
                        "label": "Reassign users to role",
                        "type": "select",
                        "options": role_options,
                        "required": True,
                        "defaultValue": role_options[0]
                    }
                ]
        else:
            # Use existing activate endpoint for inactive roles
            confirmation["clickAction"] = {
                "type": "patchData",
                "patchAPIURL": f"api/roles/activate?role_id={role.id}",
                "data": {},
                "actionAfterAPICall": {
                    "type": "refreshModule",
                    "moduleName": "RolesTable"
                }
            }
        
        # Update users list
        response["users"] = user_names
        
        # Update permissions for base-level modules
        for permission in response["permissions"]:
            module_name = permission["module"]
            if module_name in module_permissions:
                perms = module_permissions[module_name]
                # Add permission booleans to base-level modules
                permission["create"] = perms.get('create', False)
                permission["view"] = perms.get('view', False)
                permission["update"] = perms.get('update', False)
                permission["delete"] = perms.get('delete', False)
            else:
                # If no permissions found, set all to False
                permission["create"] = False
                permission["view"] = False
                permission["update"] = False
                permission["delete"] = False
        
        return response

    async def _get_role_edit_form(self, role: Role, session) -> Dict[str, Any]:
        """Get role details in edit form format"""
        # Load the edit form template
        template_path = Path(__file__).parent.parent.parent / "frontendUtils" / "renders" / "user_role_edit_response.json"
        with open(template_path, 'r') as f:
            response = json.load(f)
        
        # Update role name in the response
        response["role_name"] = role.role_name
        
        # Build permissions structure with submodules
        permissions = await self._build_role_permissions_structure(role, session)
        response["permissions"] = permissions
        
        return response

    async def _build_role_permissions_structure(self, role: Role, session) -> List[Dict[str, Any]]:
        """Build the nested permissions structure for role edit form using database hierarchy"""
        permissions = []
        
        # Get role's module permissions from database
        role_permissions = session.query(RoleOrClientBasedModuleLevelPermission).filter(
            RoleOrClientBasedModuleLevelPermission.role_id == role.id,
            RoleOrClientBasedModuleLevelPermission.client_id.is_(None),  # Only role-based permissions
            RoleOrClientBasedModuleLevelPermission.is_active == True
        ).all()
        
        # Create a mapping of module_id -> permissions for quick lookup
        module_permissions_map = {}
        for rmp in role_permissions:
            module_id = rmp.module_id
            permission_code = rmp.permission.permission_code
            
            if module_id not in module_permissions_map:
                module_permissions_map[module_id] = {}
            module_permissions_map[module_id][permission_code] = True
        
        # Get all root modules (level 0) ordered by sort_order
        root_modules = session.query(Module).filter(
            Module.level == 0,
            Module.is_active == True
        ).order_by(Module.sort_order).all()
        
        # Build the nested structure for each root module
        for root_module in root_modules:
            # Get root module permissions
            root_perms = module_permissions_map.get(root_module.id, {})
            root_module_item = {
                "module": root_module.module_name,
                "_children": await self._build_module_children(root_module.id, module_permissions_map, session, root_perms)
            }
            permissions.append(root_module_item)
        
        return permissions
    
    async def _build_module_children(self, parent_id: int, module_permissions_map: Dict[int, Dict[str, bool]], session, parent_permissions: Dict[str, bool] = None) -> List[Dict[str, Any]]:
        """Recursively build children modules structure with permission inheritance"""
        children = []
        
        # Get direct children of this module
        child_modules = session.query(Module).filter(
            Module.parent_id == parent_id,
            Module.is_active == True
        ).order_by(Module.sort_order).all()
        
        for child_module in child_modules:
            # Get permissions for this module
            module_perms = module_permissions_map.get(child_module.id, {})
            
            # Inherit permissions from parent if not explicitly set
            inherited_perms = {}
            if parent_permissions:
                for perm_type in ['create', 'read', 'update', 'delete']:
                    # Use explicit permission if exists, otherwise inherit from parent
                    inherited_perms[perm_type] = module_perms.get(perm_type, parent_permissions.get(perm_type, False))
            else:
                inherited_perms = module_perms
            
            # If this module has children, build them recursively
            if child_module.level < 2:  # Only go up to level 2 (sub-submodules)
                child_item = {
                    "module": child_module.module_name,
                    "_children": await self._build_module_children(child_module.id, module_permissions_map, session, inherited_perms)
                }
            else:
                # This is a leaf module (level 2), add CRUD permissions
                child_item = {
                    "module": child_module.module_name,
                    "create": inherited_perms.get("create", False),
                    "view": inherited_perms.get("read", False),
                    "update": inherited_perms.get("update", False),
                    "delete": inherited_perms.get("delete", False)
                }
            
            children.append(child_item)
        
        return children

    async def bulk_update_users(self, user_ids: List[int], updates: Dict[str, Any]) -> Dict[str, Any]:
        """Bulk update multiple users"""
        try:
            session = self.db_manager.get_session()
            
            updated_count = 0
            for user_id in user_ids:
                user = session.query(User).filter(User.id == user_id).first()
                if user:
                    # Apply updates
                    for field, value in updates.items():
                        if hasattr(user, field):
                            setattr(user, field, value)
                    
                    user.updated_at = datetime.utcnow()
                    updated_count += 1
            
            session.commit()
            
            return {
                'success': True,
                'message': f'Successfully updated {updated_count} users',
                'updated_count': updated_count
            }
            
        except Exception as e:
            logger.error(f"Error bulk updating users: {e}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to bulk update users: {str(e)}"
            )
        finally:
            session.close()
    
    async def get_available_clients(self) -> Dict[str, Any]:
        """Get all available clients for user assignment, sorted by creation date (newest first)"""
        try:
            session = self.db_manager.get_session()
            
            clients = (session.query(Client)
                     .filter(Client.is_active == True)
                     .order_by(desc(Client.created_at))
                     .all())
            
            client_list = [
                {
                    'id': client.id,
                    'name': client.name,
                    'code': client.code,
                    'description': client.description
                }
                for client in clients
            ]
            
            return {
                'success': True,
                'data': client_list
            }
            
        except Exception as e:
            logger.error(f"Error getting clients: {e}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to retrieve clients: {str(e)}"
            )
        finally:
            session.close()
    
    async def get_available_roles(self) -> Dict[str, Any]:
        """Get all available roles for user assignment, sorted by creation date (newest first)"""
        try:
            session = self.db_manager.get_session()
            
            roles = (session.query(Role)
                    .filter(Role.is_active == True)
                    .order_by(desc(Role.created_at))
                    .all())
            
            role_list = [
                {
                    'id': role.id,
                    'name': role.role_name,
                    'code': role.role_code,
                    'description': role.description
                }
                for role in roles
            ]
            
            return {
                'success': True,
                'data': role_list
            }
            
        except Exception as e:
            logger.error(f"Error getting roles: {e}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to retrieve roles: {str(e)}"
            )
        finally:
            session.close()
    
    async def get_role_by_id(self, role_id: int) -> Dict[str, Any]:
        """Get a specific role by ID with full details"""
        try:
            session = self.db_manager.get_session()
            
            role = session.query(Role).filter(Role.id == role_id).first()
            if not role:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail="Role not found"
                )
            
            # Get user count for this role
            user_count = session.query(User).filter(
                User.role_id == role_id,
                User.is_active == True
            ).count()
            
            role_data = {
                'id': role.id,
                'role_name': role.role_name,
                'role_code': role.role_code,
                'description': role.description,
                'is_active': role.is_active,
                'user_count': user_count,
                'created_at': role.created_at.isoformat() if role.created_at else None,
                'updated_at': role.updated_at.isoformat() if role.updated_at else None
            }
            
            return {
                'success': True,
                'data': role_data
            }
            
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error getting role {role_id}: {e}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to retrieve role: {str(e)}"
            )
        finally:
            session.close()
    
    def _process_module_permissions_recursive(self, session, role_id: int, permission_data: Dict[str, Any], 
                                            created_permissions: List[Dict], ignored_modules: List[str], 
                                            parent_module_id: int = None, level: int = 0):
        """Recursively process module permissions including child hierarchies"""
        module_name = permission_data.get('module')
        if not module_name:
            return
        
        # Find or create the module
        module = session.query(Module).filter(Module.module_name == module_name).first()
        if not module:
            # Create new module if it doesn't exist
            module_code = module_name.lower().replace(' ', '_')
            module = Module(
                module_name=module_name,
                module_code=module_code,
                description=f"Module for {module_name}",
                parent_id=parent_module_id,
                level=level,
                is_active=True
            )
            session.add(module)
            session.flush()  # Flush to get the ID without committing
            session.refresh(module)
        
        # Process each permission type (create, read, update, delete)
        permission_types = ['create', 'read', 'update', 'delete']
        
        for perm_type in permission_types:
            if permission_data.get(perm_type) is True:
                # Find or create the permission
                permission = session.query(Permission).filter(
                    Permission.permission_code == perm_type
                ).first()
                
                if not permission:
                    # Create new permission if it doesn't exist
                    permission = Permission(
                        permission_name=perm_type.title(),
                        permission_code=perm_type,
                        description=f"{perm_type.title()} permission",
                        is_active=True
                    )
                    session.add(permission)
                    session.flush()  # Flush to get the ID without committing
                    session.refresh(permission)
                
                # Check if this permission already exists for this role-module combination
                existing_permission = session.query(RoleOrClientBasedModuleLevelPermission).filter(
                    RoleOrClientBasedModuleLevelPermission.role_id == role_id,
                    RoleOrClientBasedModuleLevelPermission.client_id.is_(None),  # Only role-based permissions
                    RoleOrClientBasedModuleLevelPermission.module_id == module.id,
                    RoleOrClientBasedModuleLevelPermission.permission_id == permission.id
                ).first()
                
                if not existing_permission:
                    # Create role-module-permission mapping
                    role_module_permission = RoleOrClientBasedModuleLevelPermission(
                        role_id=role_id,
                        module_id=module.id,
                        master_id=None,  # Explicitly set to None for module permissions
                        permission_id=permission.id,
                        is_active=True
                    )
                    session.add(role_module_permission)
                    
                    created_permissions.append({
                        'module': module_name,
                        'permission': perm_type,
                        'module_id': module.id,
                        'permission_id': permission.id,
                        'level': level,
                        'parent_id': parent_module_id
                    })
        
        # Process child modules recursively
        children = permission_data.get('children', [])
        if children:
            for child_permission_data in children:
                self._process_module_permissions_recursive(
                    session, role_id, child_permission_data, created_permissions, 
                    ignored_modules, module.id, level + 1
                )
    
    async def create_role(self, role_data: Dict[str, Any]) -> Dict[str, Any]:
        """Create a new role with optional permissions"""
        try:
            session = self.db_manager.get_session()
            
            # Validate required fields
            if 'role_name' not in role_data or not role_data['role_name']:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="Missing required field: role_name"
                )
            
            # Auto-generate role_code from role_name if not provided
            if 'role_code' not in role_data or not role_data['role_code']:
                role_data['role_code'] = role_data['role_name'].lower().replace(' ', '_').replace('-', '_')
            
            # Check if role_name already exists
            existing_name = session.query(Role).filter(Role.role_name == role_data['role_name']).first()
            if existing_name:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="Role name already exists"
                )
            
            # Check if role_code already exists
            existing_code = session.query(Role).filter(Role.role_code == role_data['role_code']).first()
            if existing_code:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="Role code already exists"
                )
            
            # Create new role
            new_role = Role(
                role_name=role_data['role_name'],
                role_code=role_data['role_code'],
                description=role_data.get('description'),
                is_active=role_data.get('is_active', True)
            )
            
            session.add(new_role)
            session.commit()
            session.refresh(new_role)
            
            # Process permissions if provided
            created_permissions = []
            ignored_modules = []
            
            if role_data.get('permissions'):
                permissions = role_data['permissions']
                
                # Process permissions recursively (including child hierarchies)
                for permission_data in permissions:
                    self._process_module_permissions_recursive(
                        session, new_role.id, permission_data, created_permissions, ignored_modules
                    )
                
                session.commit()
            
            response_data = {
                'id': new_role.id,
                'role_name': new_role.role_name,
                'role_code': new_role.role_code,
                'description': new_role.description,
                'status': 'Active' if new_role.is_active else 'Inactive'
            }
            
            # Add permission information to response if permissions were processed
            if created_permissions:
                response_data['created_permissions'] = created_permissions
                response_data['ignored_modules'] = ignored_modules
            
            return {
                'success': True,
                'message': 'Role created successfully',
                'data': response_data
            }
            
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error creating role: {e}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to create role: {str(e)}"
            )
        finally:
            session.close()
    
    async def update_role(self, role_id: int, role_data: Dict[str, Any]) -> Dict[str, Any]:
        """Update an existing role with optional permissions"""
        try:
            session = self.db_manager.get_session()
            
            # Get existing role
            role = session.query(Role).filter(Role.id == role_id).first()
            if not role:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail="Role not found"
                )
            
            # Check if role_name is being changed and if it already exists
            if 'role_name' in role_data and role_data['role_name'] != role.role_name:
                existing_name = session.query(Role).filter(Role.role_name == role_data['role_name']).first()
                if existing_name:
                    raise HTTPException(
                        status_code=status.HTTP_400_BAD_REQUEST,
                        detail="Role name already exists"
                    )
            
            # Check if role_code is being changed and if it already exists
            if 'role_code' in role_data and role_data['role_code'] != role.role_code:
                existing_code = session.query(Role).filter(Role.role_code == role_data['role_code']).first()
                if existing_code:
                    raise HTTPException(
                        status_code=status.HTTP_400_BAD_REQUEST,
                        detail="Role code already exists"
                    )
            
            # Update role fields
            if 'role_name' in role_data:
                role.role_name = role_data['role_name']
            if 'role_code' in role_data:
                role.role_code = role_data['role_code']
            if 'description' in role_data:
                role.description = role_data['description']
            if 'is_active' in role_data:
                role.is_active = role_data['is_active']
            
            # Update timestamp
            role.updated_at = datetime.utcnow()
            
            # Process permissions if provided
            created_permissions = []
            ignored_modules = []
            
            if role_data.get('permissions'):
                permissions = role_data['permissions']
                
                # Clear existing role-based permissions (not client permissions)
                session.query(RoleOrClientBasedModuleLevelPermission).filter(
                    RoleOrClientBasedModuleLevelPermission.role_id == role.id,
                    RoleOrClientBasedModuleLevelPermission.client_id.is_(None)  # Only role-based permissions
                ).delete()
                
                # Process permissions recursively (including child hierarchies)
                for permission_data in permissions:
                    self._process_module_permissions_recursive(
                        session, role.id, permission_data, created_permissions, ignored_modules
                    )
                
                session.commit()
            
            response_data = {
                'id': role.id,
                'role_name': role.role_name,
                'role_code': role.role_code,
                'description': role.description,
                'status': 'Active' if role.is_active else 'Inactive'
            }
            
            # Add permission information to response if permissions were processed
            if created_permissions:
                response_data['created_permissions'] = created_permissions
                response_data['ignored_modules'] = ignored_modules
            
            return {
                'success': True,
                'message': 'Role updated successfully',
                'data': response_data
            }
            
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error updating role {role_id}: {e}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to update role: {str(e)}"
            )
        finally:
            session.close()
    
    async def delete_role(self, role_id: int, hard_delete: bool = False) -> Dict[str, Any]:
        """Delete a role (soft delete by default, hard delete optional)"""
        try:
            session = self.db_manager.get_session()
            
            role = session.query(Role).filter(Role.id == role_id).first()
            if not role:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail="Role not found"
                )
            
            # Check if role has active users assigned
            active_user_count = session.query(User).filter(
                User.role_id == role_id,
                User.is_active == True
            ).count()
            
            if active_user_count > 0:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"Cannot delete role. There are {active_user_count} active users with this role. Please reassign users first."
                )
            
            if hard_delete:
                # Hard delete - permanently remove the record
                session.delete(role)
                session.commit()
                
                return {
                    'success': True,
                    'message': 'Role permanently deleted successfully',
                    'delete_type': 'hard_delete'
                }
            else:
                # Soft delete - set is_active to False
                role.is_active = False
                role.updated_at = datetime.utcnow()
                
                session.commit()
                
                return {
                    'success': True,
                    'message': 'Role deactivated successfully (soft delete)',
                    'delete_type': 'soft_delete'
                }
            
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error deleting role {role_id}: {e}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to delete role: {str(e)}"
            )
        finally:
            session.close()
    
    async def get_roles_aggregated_data(
        self, 
        page: int = 1, 
        page_size: int = 10
    ) -> Dict[str, Any]:
        """Get aggregated roles data with user counts and status information"""
        try:
            session = self.db_manager.get_session()
            
            # Query to get role data with user counts
            from sqlalchemy import func
            
            # Build base query for counting total records
            base_query = session.query(
                Role.id.label('role_id'),
                Role.role_name.label('role'),
                Role.is_active.label('role_is_active'),
                func.count(User.id).label('total_users')
            ).outerjoin(User, Role.id == User.role_id)\
             .group_by(Role.id, Role.role_name, Role.is_active)\
             .order_by(Role.role_name)
            
            # Get total count for pagination
            total_count = base_query.count()
            
            # Apply pagination
            offset = (page - 1) * page_size
            role_stats = base_query.offset(offset).limit(page_size).all()
            
            # Load the template
            template_path = Path(__file__).parent.parent.parent / "frontendUtils" / "renders" / "organization_template_roles.json"
            with open(template_path, 'r') as f:
                response = json.load(f)
            
            # Generate row data
            row_data = []
            for role_stat in role_stats:
                total_users = role_stat.total_users or 0
                status = "Active" if role_stat.role_is_active else "Inactive"
                
                role_data = {
                    "roleID": role_stat.role_id,
                    "role": role_stat.role,
                    "totalUsers": str(total_users),
                    "products": "Frame, NAV Validus",
                    "status": status
                }
                row_data.append(role_data)
            
            # Just update the rowData in the loaded JSON template
            response["rowData"] = row_data
            
            return response
            
        except Exception as e:
            logger.error(f"Error getting aggregated roles data: {e}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to retrieve aggregated roles data: {str(e)}"
            )
        finally:
            session.close()
    
# Create service instance
user_service = UserManagementService()

# API Response functions
async def get_users_response(
    search: Optional[str] = None,
    status_filter: Optional[str] = None,
    page: int = 1,
    page_size: int = 10
) -> JSONResponse:
    """Get all users with pagination and filtering"""
    result = await user_service.get_all_users(search, status_filter, page, page_size)
    return JSONResponse(content=result)

async def get_user_response(user_id: int) -> JSONResponse:
    """Get a specific user by ID"""
    result = await user_service.get_user_by_id(user_id)
    return JSONResponse(content=result)

async def get_user_add_form_response() -> JSONResponse:
    """Get user add form template with dynamic role options"""
    try:
        # Load the add form template
        import os
        base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        template_path = os.path.join(base_dir, 'frontendUtils', 'renders', 'user_add_form_response.json')
        
        with open(template_path, 'r', encoding='utf-8') as f:
            template = json.load(f)
        
        # Get all active roles from database for dynamic options
        db_manager = get_database_manager()
        session = db_manager.get_session()
        
        try:
            roles = session.query(Role).filter(Role.is_active == True).order_by(Role.role_name).all()
            role_options = [role.role_name for role in roles]
        finally:
            session.close()
        
        # Update template with dynamic role options
        if template.get('sections'):
            for section in template['sections']:
                if section.get('template') and section['template'].get('fields'):
                    for field in section['template']['fields']:
                        # Update role field with dynamic options
                        if field.get('id') == 'role_name' and field.get('type') == 'select':
                            field['options'] = role_options
        
        return JSONResponse(content=template)
        
    except Exception as e:
        logger.error(f"Error getting user add form: {e}")
        return JSONResponse(content={"error": str(e)}, status_code=500)


async def get_user_edit_form_response(user_id: int) -> JSONResponse:
    """Get user data in edit form format"""
    try:
        # Get user data
        user_result = await user_service.get_user_by_id(user_id)
        
        # Load the edit form template
        import os
        base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        template_path = os.path.join(base_dir, 'frontendUtils', 'renders', 'user_details_response_for_edit_form.json')
        
        with open(template_path, 'r', encoding='utf-8') as f:
            template = json.load(f)
        
        # Get all active roles from database for dynamic options
        db_manager = get_database_manager()
        session = db_manager.get_session()
        
        try:
            roles = session.query(Role).filter(Role.is_active == True).order_by(Role.role_name).all()
            role_options = [role.role_name for role in roles]
        finally:
            session.close()
        
        # Extract user data from the response
        user_sections = user_result.get('sections', [])
        
        # Find user details from sections
        user_details = {}
        for section in user_sections:
            for field in section.get('fields', []):
                field_label = field.get('label', '')
                field_value = field.get('value', '')
                user_details[field_label] = field_value
        
        # Debug: Print extracted user details
        logger.info(f"Extracted user details: {user_details}")
        
        # Update template with actual user data and dynamic role options
        if template.get('sections'):
            for section in template['sections']:
                if section.get('template') and section['template'].get('fields'):
                    for field in section['template']['fields']:
                        field_label = field.get('label', '')
                        field_id = field.get('id', '')
                        
                        # Skip password field - keep it empty for security
                        if field_id == 'password':
                            field['defaultValue'] = ""
                        elif field_label in user_details:
                            field['defaultValue'] = user_details[field_label]
                        
                        # Update role field with dynamic options
                        if field.get('id') == 'role_name' and field.get('type') == 'select':
                            field['options'] = role_options
        
        # Update onConfirmation with real user name
        if template.get('onConfirmation'):
            first_name = user_details.get('First Name', '')
            last_name = user_details.get('Last Name', '')
            user_name = f"{first_name} {last_name}".strip() or 'User'
            template['onConfirmation']['description'] = f"Are you sure you want to update {user_name}?"
        
        return JSONResponse(content=template)
        
    except Exception as e:
        logger.error(f"Error getting user edit form: {e}")
        return JSONResponse(content={"error": str(e)}, status_code=500)


async def bulk_create_users_response(users_data: Dict[str, Dict[str, Any]], authenticated_username: str = None) -> JSONResponse:
    """Create multiple users (bulk creation)"""
    result = await user_service.bulk_create_users(users_data, authenticated_username)
    status_code = 201 if result['success'] else 207  # 207 Multi-Status for partial success
    return JSONResponse(content=result, status_code=status_code)

async def update_user_response(user_id: int, user_data: Dict[str, Any]) -> JSONResponse:
    """Update an existing user"""
    result = await user_service.update_user(user_id, user_data)
    return JSONResponse(content=result)

async def delete_user_response(user_id: int) -> JSONResponse:
    """Delete a user"""
    result = await user_service.delete_user(user_id)
    return JSONResponse(content=result)

async def bulk_update_users_response(user_ids: List[int], updates: Dict[str, Any]) -> JSONResponse:
    """Bulk update multiple users"""
    result = await user_service.bulk_update_users(user_ids, updates)
    return JSONResponse(content=result)

async def toggle_user_status_response(user_id: int, is_active: bool) -> JSONResponse:
    """Toggle user active/inactive status"""
    result = await user_service.toggle_user_status(user_id, is_active)
    return JSONResponse(content=result)

async def get_organization_details_response() -> JSONResponse:
    """Get organization details from JSON template"""
    try:
        template_path = Path("frontendUtils/renders/organization_details.json")
        with open(template_path, 'r') as file:
            response = json.load(file)
        return JSONResponse(content=response)
    except Exception as e:
        return JSONResponse(
            content={"error": f"Failed to load organization details: {str(e)}"}, 
            status_code=500
        )

async def inactivate_role_with_user_reassignment_response(role_data: Dict[str, Any]) -> JSONResponse:
    """Inactivate role and reassign users to new roles"""
    result = await user_service.inactivate_role_with_user_reassignment(role_data)
    return JSONResponse(content=result)

async def activate_role_response(role_id: int) -> JSONResponse:
    """Activate a role"""
    result = await user_service.activate_role(role_id)
    return JSONResponse(content=result)

async def get_role_details_response(role_id: int, edit_mode: bool = False) -> JSONResponse:
    """Get detailed role information"""
    result = await user_service.get_role_details(role_id, edit_mode)
    return JSONResponse(content=result)

async def get_clients_response() -> JSONResponse:
    """Get all available clients"""
    result = await user_service.get_available_clients()
    return JSONResponse(content=result)

async def get_roles_response() -> JSONResponse:
    """Get all available roles"""
    result = await user_service.get_available_roles()
    return JSONResponse(content=result)

async def get_role_response(role_id: int) -> JSONResponse:
    """Get a specific role by ID"""
    result = await user_service.get_role_by_id(role_id)
    return JSONResponse(content=result)

async def create_role_response(role_data: Dict[str, Any]) -> JSONResponse:
    """Create a new role"""
    result = await user_service.create_role(role_data)
    return JSONResponse(content=result, status_code=201)

async def update_role_response(role_id: int, role_data: Dict[str, Any]) -> JSONResponse:
    """Update an existing role"""
    result = await user_service.update_role(role_id, role_data)
    return JSONResponse(content=result)

async def delete_role_response(role_id: int, hard_delete: bool = False) -> JSONResponse:
    """Delete a role"""
    result = await user_service.delete_role(role_id, hard_delete)
    return JSONResponse(content=result)

async def get_roles_aggregated_response(
    page: int = 1,
    page_size: int = 10
) -> JSONResponse:
    """Get aggregated roles data with user counts and status"""
    result = await user_service.get_roles_aggregated_data(page, page_size)
    return JSONResponse(content=result)



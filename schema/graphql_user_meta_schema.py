#!/usr/bin/env python3
"""
Dedicated GraphQL Schema for User Metadata
Provides a focused endpoint for retrieving user roles and permissions
"""

import strawberry
from typing import List, Optional
from strawberry.types import Info
import logging

# Import authentication context
from .graphql_auth_context import require_authentication, get_current_user

logger = logging.getLogger(__name__)

@strawberry.type
class PermissionsType:
    """GraphQL type for Permissions"""
    products: strawberry.scalars.JSON
    masters: strawberry.scalars.JSON

@strawberry.type
class UserMetaType:
    """GraphQL type for User Metadata including roles and permissions"""
    username: str
    display_name: str = strawberry.field(name="displayName")
    role_str: str = strawberry.field(name="roleStr")
    role: str
    email: str
    role_id: Optional[int] = strawberry.field(name="roleId")
    client_id: Optional[int] = strawberry.field(name="clientId")
    role_name: str = strawberry.field(name="roleName")
    client_name: str = strawberry.field(name="clientName")
    first_name: str = strawberry.field(name="firstName")
    last_name: str = strawberry.field(name="lastName")
    job_title: str = strawberry.field(name="jobTitle")
    permissions: PermissionsType
    feature_access: strawberry.scalars.JSON = strawberry.field(name="featureAccess")

@strawberry.type
class UserMetaQuery:
    """Dedicated GraphQL Query for User Metadata"""
    
    @strawberry.field
    def user_meta(self, info: Info) -> Optional[UserMetaType]:
        """Get current user metadata including roles and permissions - requires authentication"""
        # Require authentication
        require_authentication(info)
        
        try:
            # Get current user from context
            user_data = get_current_user(info)
            if not user_data:
                logger.warning("No user data found in context")
                return None
            
            username = user_data.get('username')
            if not username:
                logger.warning("Username not found in user data")
                return None
            
            # Get user preferences including roles and permissions
            from rbac.utils.frontend import getUserPreferences
            preferences = getUserPreferences(username)
            
            # Create permissions object
            permissions_data = preferences.get('permissions', {})
            permissions = PermissionsType(
                products=permissions_data.get('products', {}),
                masters=permissions_data.get('masters', {})
            )
            
            # Get feature access (hierarchical structure)
            feature_access = preferences.get('feature-access', [])
            
            # Return user metadata
            return UserMetaType(
                username=preferences.get('username', ''),
                display_name=preferences.get('displayName', ''),
                role_str=preferences.get('roleStr', ''),
                role=preferences.get('role', ''),
                email=preferences.get('email', ''),
                role_id=preferences.get('role_id'),
                client_id=preferences.get('client_id'),
                role_name=preferences.get('role_name', ''),
                client_name=preferences.get('client_name', ''),
                first_name=preferences.get('first_name', ''),
                last_name=preferences.get('last_name', ''),
                job_title=preferences.get('job_title', ''),
                permissions=permissions,
                feature_access=feature_access
            )
            
        except Exception as e:
            logger.error(f"GraphQL user_meta query error: {e}", exc_info=True)
            return None

# Create the dedicated schema for user metadata
user_meta_schema = strawberry.Schema(query=UserMetaQuery)


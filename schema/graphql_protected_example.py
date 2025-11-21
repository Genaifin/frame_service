"""
Example of how to protect GraphQL resolvers with authentication
"""
import strawberry
from strawberry.types import Info
from typing import List, Optional
from .graphql_auth_context import require_authentication, require_role

@strawberry.type
class ProtectedUser:
    id: int
    username: str
    display_name: str
    role: str

class ProtectedUserQuery:
    """Example protected GraphQL queries"""
    
    @strawberry.field
    def me(self, info: Info) -> Optional[ProtectedUser]:
        """Get current user info - requires authentication"""
        try:
            user = require_authentication(info)
            return ProtectedUser(
                id=user.get("id", 0),
                username=user.get("username", ""),
                display_name=user.get("displayName", ""),
                role=user.get("role", "")
            )
        except Exception as e:
            raise Exception(f"Authentication required: {str(e)}")
    
    @strawberry.field
    def admin_only_data(self, info: Info) -> str:
        """Admin-only data - requires admin role"""
        try:
            user = require_authentication(info)
            if user.get("role") != "admin":
                raise Exception("Admin role required")
            return "This is admin-only data"
        except Exception as e:
            raise Exception(f"Access denied: {str(e)}")

class ProtectedUserMutation:
    """Example protected GraphQL mutations"""
    
    @strawberry.mutation
    def update_profile(self, info: Info, display_name: str) -> bool:
        """Update user profile - requires authentication"""
        try:
            user = require_authentication(info)
            # Here you would update the user's profile
            # For now, just return success
            return True
        except Exception as e:
            raise Exception(f"Authentication required: {str(e)}")
    
    @strawberry.mutation
    def delete_user(self, info: Info, user_id: int) -> bool:
        """Delete user - requires admin role"""
        try:
            user = require_authentication(info)
            if user.get("role") != "admin":
                raise Exception("Admin role required")
            # Here you would delete the user
            return True
        except Exception as e:
            raise Exception(f"Access denied: {str(e)}")

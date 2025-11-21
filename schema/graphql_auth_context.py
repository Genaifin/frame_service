"""
GraphQL Authentication Context for Strawberry
Maintains consistency with REST API authentication system
"""
from typing import Optional, Dict, Any
from strawberry.types import Info
from rbac.utils.auth import verifyToken
from rbac.utils.frontend import getUserByUsername
import logging

logger = logging.getLogger(__name__)

class GraphQLAuthContext:
    """Authentication context for GraphQL operations - consistent with REST API"""
    
    def __init__(self, request=None):
        self.request = request
        self.user = None
        self.is_authenticated = False
        self.token = None
        self.username = None
        self.role = None
        
        if request:
            self._extract_and_verify_token()
    
    def _extract_and_verify_token(self):
        """Extract and verify JWT token from request headers - same as REST API"""
        try:
            # Extract Authorization header (same format as REST API)
            auth_header = self.request.headers.get("Authorization", "")
            
            if not auth_header.startswith("Bearer "):
                logger.debug("No Bearer token found in Authorization header")
                return
            
            # Extract token (same as REST API)
            self.token = auth_header[7:]  # Remove "Bearer " prefix
            
            # Verify token using same function as REST API
            payload = verifyToken(self.token)
            if not payload:
                logger.debug("Invalid or expired token")
                return
            
            # Get username from token (same as REST API)
            username = payload.get("sub")
            if not username:
                logger.debug("No username in token payload")
                return
            
            # Get user details from database (same as REST API)
            user_data = getUserByUsername(username)
            if not user_data:
                logger.debug(f"User not found: {username}")
                return
            
            # Set authentication context (same structure as REST API)
            self.user = user_data
            self.username = username
            self.role = user_data.get("role")
            self.is_authenticated = True
            
            logger.debug(f"User authenticated: {username} with role: {self.role}")
            
        except Exception as e:
            logger.error(f"Authentication error: {e}")
            self.is_authenticated = False
    
    def require_auth(self) -> Dict[str, Any]:
        """Require authentication - same error format as REST API"""
        if not self.is_authenticated or not self.user:
            raise Exception("Could not validate credentials")
        return self.user
    
    def get_user_role(self) -> Optional[str]:
        """Get user role if authenticated"""
        if self.is_authenticated and self.user:
            return self.user.get("role")
        return None
    
    def has_role(self, required_role: str) -> bool:
        """Check if user has specific role"""
        user_role = self.get_user_role()
        return user_role == required_role
    
    def get_username(self) -> Optional[str]:
        """Get username if authenticated"""
        return self.username

def get_auth_context(info: Info) -> GraphQLAuthContext:
    """Get authentication context from GraphQL info"""
    request = info.context.get("request")
    return GraphQLAuthContext(request)

def require_authentication(info: Info) -> Dict[str, Any]:
    """Require authentication for GraphQL operations - same as REST API"""
    context = get_auth_context(info)
    return context.require_auth()

def require_role(role: str):
    """Decorator to require specific role"""
    def decorator(func):
        def wrapper(info: Info, *args, **kwargs):
            context = get_auth_context(info)
            user = context.require_auth()
            
            if not context.has_role(role):
                raise Exception(f"Role '{role}' required")
            
            return func(info, *args, **kwargs)
        return wrapper
    return decorator

def get_current_user(info: Info) -> Optional[Dict[str, Any]]:
    """Get current user if authenticated, None otherwise - same as REST API"""
    context = get_auth_context(info)
    if context.is_authenticated:
        return context.user
    return None

def is_authenticated(info: Info) -> bool:
    """Check if user is authenticated"""
    context = get_auth_context(info)
    return context.is_authenticated

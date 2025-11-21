#!/usr/bin/env python3
"""
GraphQL Authentication Mutations
Provides login, logout, and token management directly in GraphQL
"""

import strawberry
from typing import Optional
from strawberry.types import Info
from rbac.utils.auth import authenticateUser, createAccessToken, verifyToken, getPasswordHash
from rbac.utils.frontend import getUserByUsername
from datetime import timedelta
import logging

logger = logging.getLogger(__name__)

@strawberry.type
class LoginResponse:
    """Response type for login mutation"""
    success: bool
    message: str
    access_token: Optional[str] = None
    token_type: Optional[str] = None
    expires_in: Optional[int] = None
    username: Optional[str] = None
    display_name: Optional[str] = None
    role: Optional[str] = None

@strawberry.type
class LogoutResponse:
    """Response type for logout mutation"""
    success: bool
    message: str
    username: Optional[str] = None

@strawberry.type
class TokenRefreshResponse:
    """Response type for token refresh mutation"""
    success: bool
    message: str
    access_token: Optional[str] = None
    token_type: Optional[str] = None
    expires_in: Optional[int] = None

@strawberry.input
class LoginInput:
    """Input type for login mutation"""
    username: str
    password: str

@strawberry.input
class PasswordChangeInput:
    """Input type for password change mutation"""
    current_password: str
    new_password: str

@strawberry.type
class AuthMutation:
    """GraphQL Mutations for authentication"""
    
    @strawberry.mutation
    def login(self, info: Info, input: LoginInput) -> LoginResponse:
        """Login with username and password - returns JWT token"""
        logger.info(f"GraphQL login attempt for username: {input.username}")
        
        try:
            logger.info("Starting user authentication...")
            # Authenticate user using same logic as REST API
            user = authenticateUser(input.username, input.password)
            logger.info(f"Authentication result: {user is not None}")
            
            if not user:
                logger.warning(f"Login failed for username: {input.username}")
                return LoginResponse(
                    success=False,
                    message="Invalid username or password"
                )
            
            logger.info(f"User authenticated successfully: {user.get('username')}")
            
            # Create access token (same as REST API)
            logger.info("Creating access token...")
            access_token_expires = timedelta(minutes=30)
            access_token = createAccessToken(
                data={"sub": user["username"]}, 
                expiresDelta=access_token_expires
            )
            logger.info("Access token created successfully")
            
            response = LoginResponse(
                success=True,
                message="Login successful",
                access_token=access_token,
                token_type="bearer",
                expires_in=30 * 60,  # 30 minutes in seconds
                username=user["username"],
                display_name=user["displayName"],
                role=user["role"]
            )
            
            logger.info(f"Login successful for user: {user['username']}")
            return response
            
        except Exception as e:
            logger.error(f"GraphQL login error: {e}", exc_info=True)
            return LoginResponse(
                success=False,
                message=f"Login failed: {str(e)}"
            )
    
    @strawberry.mutation
    def logout(self, info: Info) -> LogoutResponse:
        """Logout current user - requires authentication"""
        try:
            # Get current user from token
            from .graphql_auth_context import get_auth_context
            context = get_auth_context(info)
            
            if not context.is_authenticated:
                return LogoutResponse(
                    success=False,
                    message="Not authenticated"
                )
            
            username = context.get_username()
            
            # In a real implementation, you might want to:
            # - Add token to blacklist
            # - Clear server-side session
            # - Log logout event
            
            return LogoutResponse(
                success=True,
                message="Logout successful",
                username=username
            )
            
        except Exception as e:
            logger.error(f"GraphQL logout error: {e}")
            return LogoutResponse(
                success=False,
                message=f"Logout failed: {str(e)}"
            )
    
    @strawberry.mutation
    def refresh_token(self, info: Info) -> TokenRefreshResponse:
        """Refresh current JWT token - requires authentication"""
        try:
            # Get current user from token
            from .graphql_auth_context import get_auth_context
            context = get_auth_context(info)
            
            if not context.is_authenticated:
                return TokenRefreshResponse(
                    success=False,
                    message="Not authenticated"
                )
            
            # Create new token with extended expiry
            access_token_expires = timedelta(minutes=30)
            new_access_token = createAccessToken(
                data={"sub": context.username}, 
                expiresDelta=access_token_expires
            )
            
            return TokenRefreshResponse(
                success=True,
                message="Token refreshed successfully",
                access_token=new_access_token,
                token_type="bearer",
                expires_in=30 * 60  # 30 minutes in seconds
            )
            
        except Exception as e:
            logger.error(f"GraphQL token refresh error: {e}")
            return TokenRefreshResponse(
                success=False,
                message=f"Token refresh failed: {str(e)}"
            )
    
    @strawberry.mutation
    def change_password(self, info: Info, input: PasswordChangeInput) -> LoginResponse:
        """Change user password - requires authentication"""
        try:
            # Get current user from token
            from .graphql_auth_context import get_auth_context
            context = get_auth_context(info)
            
            if not context.is_authenticated:
                return LoginResponse(
                    success=False,
                    message="Not authenticated"
                )
            
            # Get user from database
            user_data = getUserByUsername(context.username)
            if not user_data:
                return LoginResponse(
                    success=False,
                    message="User not found"
                )
            
            # Verify current password
            from rbac.utils.auth import verifyPassword
            if not verifyPassword(input.current_password, user_data["password"]):
                return LoginResponse(
                    success=False,
                    message="Current password is incorrect"
                )
            
            # Update password in database
            from database_models import get_database_manager, User
            db_manager = get_database_manager()
            session = db_manager.get_session()
            
            try:
                user = session.query(User).filter(User.username == context.username).first()
                if user:
                    user.password_hash = getPasswordHash(input.new_password)
                    user.temp_password = False  # Clear temp password flag
                    session.commit()
                    
                    return LoginResponse(
                        success=True,
                        message="Password changed successfully",
                        username=user_data["username"],
                        display_name=user_data["displayName"],
                        role=user_data["role"]
                    )
                else:
                    return LoginResponse(
                        success=False,
                        message="User not found in database"
                    )
            finally:
                session.close()
            
        except Exception as e:
            logger.error(f"GraphQL change password error: {e}")
            return LoginResponse(
                success=False,
                message=f"Password change failed: {str(e)}"
            )

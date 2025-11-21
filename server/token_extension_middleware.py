"""
Token Extension Middleware
Automatically extends token expiry when APIs are used within 30 minutes of expiry
"""

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response
from fastapi import HTTPException, status
from typing import Dict, Optional
import time
import logging

# Import authentication utilities
from rbac.utils.auth import extendTokenExpiry, isTokenExpiringSoon, verifyToken, getTokenExpiryTime
from rbac.utils.frontend import getUserByUsername

logger = logging.getLogger(__name__)

class TokenExtensionMiddleware(BaseHTTPMiddleware):
    """
    Middleware that automatically extends token expiry when APIs are used
    within 24 hours of token expiry (for 48-hour tokens)
    """
    
    def __init__(self, app, extensionThresholdMinutes: int = 1440):  # 24 hours before expiry
        super().__init__(app)
        self.extensionThresholdMinutes = extensionThresholdMinutes
        # Track last API usage per user to avoid excessive extensions
        self.lastExtensionTime: Dict[str, float] = {}
        self.minExtensionInterval = 3600  # 1 hour minimum between extensions for 48-hour tokens
        
    async def dispatch(self, request: Request, call_next):
        # Skip token extension for certain paths
        skipPaths = [
            "/login", 
            "/logout", 
            "/health", 
            "/docs", 
            "/openapi.json",
            "/favicon.ico"
        ]
        
        if any(request.url.path.startswith(path) for path in skipPaths):
            return await call_next(request)
        
        # Extract token from Authorization header
        token = self._extractToken(request)
        
        if token:
            try:
                # Verify token and get user info
                payload = verifyToken(token)
                if payload:
                    username = payload.get("sub")
                    currentTime = time.time()
                    
                    # Check if token is expiring soon
                    if isTokenExpiringSoon(token, self.extensionThresholdMinutes):
                        # Check if we've extended recently for this user
                        lastExtension = self.lastExtensionTime.get(username, 0)
                        
                        if currentTime - lastExtension >= self.minExtensionInterval:
                            # Extend the token by 48 hours (2880 minutes) for activity-based renewal
                            extendedToken = extendTokenExpiry(token, 2880)  # Extend by 48 hours
                            
                            if extendedToken:
                                # Update the last extension time
                                self.lastExtensionTime[username] = currentTime
                                
                                logger.info(f"Token extended for user: {username}")
                                
                                # Process the request
                                response = await call_next(request)
                                
                                # Add the new token to response headers
                                response.headers["X-New-Token"] = extendedToken
                                response.headers["X-Token-Extended"] = "true"
                                
                                return response
                            else:
                                logger.warning(f"Failed to extend token for user: {username}")
                        else:
                            logger.debug(f"Token extension skipped for user {username} - too recent")
                
            except Exception as e:
                logger.error(f"Error in token extension middleware: {e}")
        
        # If no token or extension not needed, proceed normally
        return await call_next(request)
    
    def _extractToken(self, request: Request) -> Optional[str]:
        """Extract JWT token from Authorization header"""
        authHeader = request.headers.get("Authorization")
        if authHeader and authHeader.startswith("Bearer "):
            return authHeader[7:]  # Remove "Bearer " prefix
        return None
    
    def _shouldExtendToken(self, token: str, username: str) -> bool:
        """Determine if token should be extended based on expiry and usage patterns"""
        try:
            # Check if token is expiring within threshold
            if not isTokenExpiringSoon(token, self.extensionThresholdMinutes):
                return False
            
            # Check if we've extended recently
            currentTime = time.time()
            lastExtension = self.lastExtensionTime.get(username, 0)
            
            return currentTime - lastExtension >= self.minExtensionInterval
            
        except Exception as e:
            logger.error(f"Error checking token extension eligibility: {e}")
            return False


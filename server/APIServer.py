from fastapi import FastAPI, HTTPException, Request
from typing import Optional
from fastapi import File, UploadFile, Form
from fastapi import Depends, status, Query, Body
from fastapi.middleware.cors import CORSMiddleware
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse
from fastapi.responses import JSONResponse
from server.APIServerUtils.athena import athenaResponse

import os
import shutil
import json
from typing import List, Optional, Dict, Any
from datetime import timedelta

import logging
from dotenv import load_dotenv
import sys
from pathlib import Path

logger = logging.getLogger(__name__)

# Add project root to Python path for athena module
project_root = Path(__file__).parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

# Set umask to ensure files/folders are created with proper permissions
os.umask(0o022)  # This ensures files are created with 755 permissions

# Load environment variables from .env file
load_dotenv()

# Import authentication utilities
from rbac.utils.auth import getCurrentUser, authenticateUser, createAccessToken, getPasswordHash, extendTokenExpiry, isTokenExpiringSoon
from rbac.utils.auth_models import Token, UserLogin, UserResponse, PasswordChange, TokenRefreshRequest, TokenRefreshResponse
from rbac.utils.frontend import getUserByUsername

# Import GraphQL schema
try:
    from strawberry.fastapi import GraphQLRouter
    from schema.graphql_main_schema import schema
    GRAPHQL_AVAILABLE = True
except ImportError:
    GRAPHQL_AVAILABLE = False
    print("Warning: strawberry-graphql not installed. GraphQL endpoints will not be available.")

import server.APIServerUtils.frontend
import server.APIServerUtils.user
import server.APIServerUtils.user_management
import server.APIServerUtils.client_management
import server.APIServerUtils.fund_management
from server.APIServerUtils.athena import athenaResponse

# Import user management models
from server.APIServerUtils.user_models import (
    UserCreateRequest, UserCreateRequestV2, UserToggleStatusRequest, UserToggleStatusResponse, UnifiedUserCreateRequest, UserUpdateRequest, BulkUpdateRequest, UserSearchRequest,
    RoleInactivationRequest, RoleInactivationResponse, RoleDetailResponse
)

# Import client management models
from server.APIServerUtils.client_models import (
    ClientCreateRequest, ClientUpdateRequest, BulkClientUpdateRequest, ClientSearchRequest
)


# Import role management models
from server.APIServerUtils.role_models import (
    RoleCreateRequest, RoleUpdateRequest
)

# Import fund management models
from server.APIServerUtils.fund_models import (
    FundCreateRequest, FundUpdateRequest, BulkUpdateRequest as FundBulkUpdateRequest, 
    FundSearchRequest, FundResponse, FundDetailResponse, FundListResponse, FundStatsResponse,
    FundManagerStatusRequest, FundManagerStatusResponse, AddFundManagerRequest, AddFundManagerResponse,
    EditFundManagerRequest, EditFundManagerResponse
)

# Import Pydantic for custom models
from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Any

# Import database models for new client API
from database_models import DatabaseManager, Client, Fund
from sqlalchemy import text

import subprocess
import threading
from concurrent.futures import ThreadPoolExecutor
import time

# Import NAV validation utilities
from utils.navValidationUtils import getNAVValidationData
from utils.file_config_reader import readFileConfigs, getDocumentTypes, getDocumentById, createDocumentType, deleteDocumentType

# Import file processing utilities
from runner_frame import process_file_with_orchestrator

# Import schema middleware
from server.schema_middleware import SchemaMiddleware

# Import token extension middleware
from server.token_extension_middleware import TokenExtensionMiddleware


app = FastAPI(
    title="Aithon Dev API Server",
    description="For development purposes only",
    version="1.0.0",
    root_path="/api/v1"
)

# Include fund management router
from server.fund_management import router as fund_router
app.include_router(fund_router)

# Include client CRUD API router
from server.client_crud_api import router as client_crud_router
app.include_router(client_crud_router)

# Include accounts API router (provides /accounts/view_all_accounts)
from server.accounts_api import router as accounts_router
app.include_router(accounts_router)

# Include sidebar API router (provides /sidebar)
from server.sidebar import router as sidebar_router
app.include_router(sidebar_router)

# Include GraphQL router if available
if GRAPHQL_AVAILABLE:
    from strawberry.fastapi import GraphQLRouter
    from schema.graphql_main_schema import schema
    
    # Create GraphQL app with authentication context - consistent with REST API
    def get_context(request):
        """Provide request context to GraphQL resolvers - same as REST API middleware"""
        logger.info(f"GraphQL request received: {request.method} {request.url.path}")
        logger.info(f"GraphQL headers: {dict(request.headers)}")
        return {"request": request}
    
    # Create GraphQL router - simplified version for testing
    graphql_app = GraphQLRouter(schema)
    
    # Include GraphQL router with same middleware as REST API
    app.include_router(graphql_app, prefix="/v2", tags=["GraphQL v2"])

# CORS is now handled by nginx, so we allow access from anywhere
originsAllowed = ["*"]

# Allow any localhost/127.0.0.1 with http/https and any port
LOCALHOST_ORIGIN_REGEX = r"^https?://(localhost|127\\.0\\.0\\.1)(:[0-9]{1,5})?$"

# Helper function to check if IP is in VPN subnet
def is_ip_in_vpn_subnet(ip: str) -> bool:
    """Check if IP address is in the VPN subnet 10.10.0.0/16"""
    try:
        import ipaddress
        ip_obj = ipaddress.ip_address(ip)
        vpn_network = ipaddress.ip_network("10.10.0.0/16")
        return ip_obj in vpn_network
    except Exception:
        return False

# Strict origin whitelist enforcement (server-side)
class OriginWhitelistMiddleware(BaseHTTPMiddleware):
    def __init__(self, app, allowed_origins: list[str]):
        super().__init__(app)
        # Check if wildcard is enabled (allow from anywhere)
        self.allow_anywhere = "*" in allowed_origins
        if self.allow_anywhere:
            self.allowed_origins = set()
            self.allowed_hosts = set()
        else:
            # Normalize allowed origins and derive allowed hosts (domain:port)
            from urllib.parse import urlparse
            self.allowed_origins = set(allowed_origins)
            self.allowed_hosts = set()
            for origin in self.allowed_origins:
                try:
                    parsed = urlparse(origin)
                    if parsed.netloc:
                        self.allowed_hosts.add(parsed.netloc)
                    elif parsed.path:
                        # In case a bare host was provided without scheme
                        self.allowed_hosts.add(parsed.path)
                except Exception:
                    # Fallback: treat as bare host
                    self.allowed_hosts.add(origin)

    async def dispatch(self, request: Request, call_next):
        from urllib.parse import urlparse

        headers = request.headers

        # If wildcard is enabled, allow all origins
        if self.allow_anywhere:
            return await call_next(request)

        # Check if request is coming from allowed IPs - bypass origin enforcement if so
        client_host = request.client.host if request.client else None
        x_forwarded_for = headers.get("x-forwarded-for")
        x_real_ip = headers.get("x-real-ip")
        
        # Check various ways the IP might be provided
        client_ip = client_host
        if x_real_ip:
            client_ip = x_real_ip
        elif x_forwarded_for:
            client_ip = x_forwarded_for.split(",")[0].strip()
        
        # Allow specific IP 10.0.137.5 and VPN subnet 10.10.0.0/16
        if (client_ip == "10.0.137.5" or 
            (client_ip and is_ip_in_vpn_subnet(client_ip))):
            # Bypass origin enforcement for allowed IPs
            print(f"DEBUG: Allowing request from VPN IP: {client_ip}")
            return await call_next(request)

        # 1) Enforce explicit Origin header for browser/XHR requests
        origin = headers.get("origin")
        print(f"DEBUG: Request origin: {origin}, client_ip: {client_ip}")
        if origin:
            if origin not in self.allowed_origins:
                try:
                    parsed = urlparse(origin)
                    host_only = parsed.hostname or ""
                    # Allow any localhost or 127.0.0.1 with any port for development
                    if host_only in {"localhost", "127.0.0.1"} and parsed.scheme in {"http", "https"}:
                        # Allow localhost/127.0.0.1 with any port for development
                        pass
                    else:
                        return JSONResponse(
                            status_code=403,
                            content={
                                "detail": "Origin not allowed",
                                "allowed_origins": list(self.allowed_origins) + ["localhost/*", "127.0.0.1/*"]
                            }
                        )
                except Exception:
                    return JSONResponse(
                        status_code=403,
                        content={"detail": "Invalid Origin"}
                    )
            # Origin allowed; proceed
            return await call_next(request)

        # 2) If no Origin, fall back to Referer (some navigations/redirects)
        referer = headers.get("referer")
        if referer:
            try:
                ref_origin = f"{urlparse(referer).scheme}://{urlparse(referer).netloc}"
                if ref_origin and ref_origin not in self.allowed_origins:
                    ref_host = urlparse(referer).hostname or ""
                    # Allow any localhost or 127.0.0.1 with any port for development
                    if ref_host not in {"localhost", "127.0.0.1"}:
                        return JSONResponse(
                            status_code=403,
                            content={
                                "detail": "Referer origin not allowed",
                                "allowed_origins": list(self.allowed_origins) + ["localhost/*", "127.0.0.1/*"]
                            }
                        )
            except Exception:
                # If parsing fails, reject conservatively
                return JSONResponse(
                    status_code=403,
                    content={"detail": "Invalid Referer"}
                )

        # 3) If neither Origin nor Referer, enforce by Host/X-Forwarded-Host
        # This blocks direct access via IP or other hostnames
        forwarded_host = headers.get("x-forwarded-host")
        host = forwarded_host or headers.get("host")
        if host and self.allowed_hosts:
            if host not in self.allowed_hosts:
                bare_host = (host or "").split(":")[0]
                # Allow localhost, 127.0.0.1, 10.0.137.5, and VPN subnet 10.10.0.0/16
                if (bare_host not in {"localhost", "127.0.0.1", "10.0.137.5"} and 
                    not is_ip_in_vpn_subnet(bare_host)):
                    return JSONResponse(
                        status_code=403,
                        content={
                            "detail": "Host not allowed",
                            "allowed_hosts": list(self.allowed_hosts) + ["localhost", "127.0.0.1", "10.0.137.5", "10.10.0.0/16"]
                        }
                    )

        return await call_next(request)

# CORS configuration based on ENVIRONMENT variable
# If ENVIRONMENT==dev, only allow dev server origins
# Otherwise, allow localhost origins for local development
environment = os.getenv("ENVIRONMENT", "").lower()

# Get custom CORS origins from environment variable (comma-separated)
cors_origins_env = os.getenv("CORS_ORIGINS", "").strip()
cors_origins = [origin.strip() for origin in cors_origins_env.split(",") if origin.strip()] if cors_origins_env else []

if environment == "dev":
    # Localhost origins for local development
    default_origins = [
        "http://localhost:3000",
        "http://localhost:3001",
        "http://localhost:3002",
        "http://localhost:5173",  # Vite default port
        "http://127.0.0.1:3000",
        "http://127.0.0.1:3001",
        "http://127.0.0.1:3002",
        "http://127.0.0.1:5173",
        "http://localhost:80",
        "http://127.0.0.1:80",
    ]
    # Combine custom origins with defaults
    all_origins = list(set([origin.strip() for origin in cors_origins + default_origins if origin.strip()]))

    app.add_middleware(
        CORSMiddleware,
        allow_origins=all_origins,
        allow_credentials=True,
        allow_methods=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"],
        allow_headers=["*"],
    )

# Add whitelist middleware
app.add_middleware(OriginWhitelistMiddleware, allowed_origins=originsAllowed)

# Add schema middleware to ensure proper PostgreSQL schema context
app.add_middleware(SchemaMiddleware)

# Add token extension middleware to automatically extend tokens when APIs are used
app.add_middleware(TokenExtensionMiddleware, extensionThresholdMinutes=30)

# CORS is handled by nginx in production
# For local development, CORS middleware above handles cross-origin requests
# Token-based authentication dependency
async def authenticate_user(username: str = Depends(getCurrentUser)):
    """Authenticate user using JWT token"""
    return username

# ----------------------------
# Global Rate Limiting Middleware (in-memory sliding window)
# ----------------------------
# Configurable via environment variables:
#   API_RATE_LIMIT_PER_MIN (default: 60) - general API limit
#   UPLOAD_RATE_LIMIT_PER_MIN (default: 10) - upload endpoint limit
# The limiter keys by both username (if authenticated) and client IP
API_RATE_LIMIT_PER_MIN = int(os.getenv("API_RATE_LIMIT_PER_MIN", "60"))
UPLOAD_RATE_LIMIT_PER_MIN = int(os.getenv("UPLOAD_RATE_LIMIT_PER_MIN", "10"))

_rate_limit_lock = threading.Lock()
_rate_limit_window_seconds = 60
_rate_limit_events = {}

def _get_client_ip(request: Request) -> str:
    """Extract client IP from request, respecting proxy headers"""
    xff = request.headers.get("x-forwarded-for")
    if xff:
        try:
            return xff.split(",")[0].strip()
        except Exception:
            pass
    x_real_ip = request.headers.get("x-real-ip")
    if x_real_ip:
        return x_real_ip
    return request.client.host if request.client else "unknown"

def _get_rate_limit_key(request: Request, username: Optional[str] = None) -> str:
    """Generate rate limit key from username (if authenticated) and IP"""
    ip = _get_client_ip(request)
    if username:
        return f"{username}:{ip}"
    return f"anonymous:{ip}"

def _get_rate_limit_for_path(path: str) -> int:
    """Get rate limit for a specific path pattern"""
    # Stricter limits for upload endpoints
    if "/upload" in path.lower() or path.startswith("/upload"):
        return UPLOAD_RATE_LIMIT_PER_MIN
    # Default limit for all other API endpoints
    return API_RATE_LIMIT_PER_MIN

class RateLimitMiddleware(BaseHTTPMiddleware):
    """Middleware to apply rate limiting to all API endpoints"""
    
    def __init__(self, app):
        super().__init__(app)
    
    async def dispatch(self, request: Request, call_next):
        # Skip rate limiting for certain paths
        skip_paths = [
            "/docs",
            "/openapi.json",
            "/redoc",
            "/favicon.ico",
            "/health",
        ]
        
        if any(request.url.path.startswith(path) for path in skip_paths):
            return await call_next(request)
        
        # Try to get username from token if authenticated
        username = None
        try:
            from rbac.utils.auth import verifyToken
            token = request.headers.get("Authorization", "").replace("Bearer ", "")
            if token:
                payload = verifyToken(token)
                if payload:
                    username = payload.get("sub")
        except Exception:
            # If auth fails, treat as anonymous (will use IP-based limiting)
            pass
        
        # Get rate limit for this path
        limit = _get_rate_limit_for_path(request.url.path)
        key = _get_rate_limit_key(request, username)
        now = time.time()
        window = _rate_limit_window_seconds
        
        # Apply rate limiting
        with _rate_limit_lock:
            events = _rate_limit_events.get(key, [])
            # Drop events outside the window
            cutoff = now - window
            events = [t for t in events if t >= cutoff]
            
            if len(events) >= limit:
                logger.warning(f"Rate limit exceeded for {key} on {request.url.path} (limit: {limit}/min)")
                return JSONResponse(
                    status_code=429,
                    content={
                        "detail": f"Rate limit exceeded. Maximum {limit} requests per minute allowed.",
                        "retry_after": int(window - (now - events[0]) if events else window)
                    },
                    headers={"Retry-After": str(int(window - (now - events[0]) if events else window))}
                )
            
            # Record this request
            events.append(now)
            _rate_limit_events[key] = events
        
        # Process the request
        response = await call_next(request)
        
        # Add rate limit headers to response
        remaining = max(0, limit - len(events))
        response.headers["X-RateLimit-Limit"] = str(limit)
        response.headers["X-RateLimit-Remaining"] = str(remaining)
        response.headers["X-RateLimit-Reset"] = str(int(now + window))
        
        return response

# Add rate limiting middleware (should be after auth middleware)
app.add_middleware(RateLimitMiddleware)

# ----------------------------
# Profanity Filter Middleware
# ----------------------------
# Use shared profanity filter utility module
from utils.profanityFilter import filter_profanity_in_data, ENABLE_PROFANITY_FILTER, PROFANITY_FILTER_AVAILABLE

class ProfanityFilterMiddleware(BaseHTTPMiddleware):
    """Middleware to filter profanity from all API responses"""
    
    def __init__(self, app):
        super().__init__(app)
    
    async def dispatch(self, request: Request, call_next):
        # Skip profanity filtering for certain paths
        skip_paths = [
            "/docs",
            "/openapi.json",
            "/redoc",
            "/favicon.ico",
        ]
        
        if any(request.url.path.startswith(path) for path in skip_paths):
            return await call_next(request)
        
        # Process the request
        response = await call_next(request)
        
        # Only filter if enabled and library is available
        if not ENABLE_PROFANITY_FILTER or not PROFANITY_FILTER_AVAILABLE:
            return response
        
        # Check if response is a FileResponse or binary content type - skip these
        from fastapi.responses import FileResponse
        if isinstance(response, FileResponse):
            return response
        
        # Check content-type BEFORE reading body to avoid processing binary files
        content_type = response.headers.get("content-type", "").lower()
        media_type = getattr(response, "media_type", None)
        
        # Skip binary content types (PDFs, images, videos, etc.)
        binary_content_types = [
            "application/pdf",
            "image/",
            "video/",
            "audio/",
            "application/octet-stream",
            "application/zip",
            "application/x-zip-compressed",
            "application/x-tar",
            "application/gzip",
        ]
        
        if any(binary_type in content_type for binary_type in binary_content_types):
            return response
        
        # Check if response is JSON (either by content-type, media_type, or by type)
        # Check both FastAPI and Starlette JSONResponse
        from fastapi.responses import JSONResponse as FastAPIJSONResponse
        from starlette.responses import JSONResponse as StarletteJSONResponse
        is_json_response = isinstance(response, (FastAPIJSONResponse, StarletteJSONResponse))
        is_json_content_type = "application/json" in content_type
        is_json_media_type = media_type == "application/json"
        
        # If we can't determine it's JSON from headers/type, skip processing
        # This prevents unnecessary body reading for non-JSON responses
        if not (is_json_response or is_json_content_type or is_json_media_type):
            return response
        
        # Log warnings if profanity filtering is not available or disabled
        if not PROFANITY_FILTER_AVAILABLE:
            logger.warning(f"Profanity filtering is enabled but profanity filter library (better-profanity or profanity-filter) is not available for {request.url.path}. Install with: pip install better-profanity")
        if not ENABLE_PROFANITY_FILTER:
            logger.debug(f"Profanity filtering is disabled via ENABLE_PROFANITY_FILTER environment variable for {request.url.path}")
        
        # Extract response data - only for JSON responses
        body = None
        try:
            # Read response body (body_iterator can only be read once!)
            body = b""
            async for chunk in response.body_iterator:
                body += chunk
            
            if not body:
                return response
            
            # Try to parse as JSON
            try:
                data = json.loads(body.decode("utf-8"))
            except (json.JSONDecodeError, UnicodeDecodeError) as e:
                # If we can't parse as JSON, reconstruct response with original body
                # since we've already consumed the body_iterator
                logger.debug(f"Could not parse response as JSON for {request.url.path}: {e}")
                from starlette.responses import Response
                return Response(
                    content=body,
                    status_code=response.status_code,
                    headers=dict(response.headers),
                    media_type=content_type or "application/octet-stream"
                )
            
            # Filter profanity
            filtered_data = filter_profanity_in_data(data)
            
            # Log if data was changed (for debugging)
            if filtered_data != data:
                logger.debug(f"Profanity filter modified data for {request.url.path}")
            
            # Create new response with filtered data
            # Remove Content-Length header so it can be recalculated based on new body size
            new_headers = {k: v for k, v in response.headers.items() if k.lower() != "content-length"}
            
            return JSONResponse(
                content=filtered_data,
                status_code=response.status_code,
                headers=new_headers
            )
        except Exception as e:
            # If filtering fails, log error
            logger.error(f"Error filtering profanity in response for {request.url.path}: {e}", exc_info=True)
            # If we've read the body, reconstruct response; otherwise return original
            if body is not None:
                from starlette.responses import Response
                return Response(
                    content=body,
                    status_code=response.status_code,
                    headers=dict(response.headers),
                    media_type=content_type or "application/octet-stream"
                )
            return response

# Add profanity filter middleware (should be after rate limiting)
# if ENABLE_PROFANITY_FILTER and PROFANITY_FILTER_AVAILABLE:
#     app.add_middleware(ProfanityFilterMiddleware)
#     logger.info("Profanity filtering middleware enabled")
# elif ENABLE_PROFANITY_FILTER and not PROFANITY_FILTER_AVAILABLE:
#     logger.warning("Profanity filtering is enabled but profanity filter library (better-profanity or profanity-filter) is not available. Install with: pip install better-profanity")

@app.get("/",dependencies=[Depends(authenticate_user)])
async def root(__username: str = Depends(authenticate_user)):
    return {"message": "Hello World", "username": __username}

@app.get("/FE") #FrontEnd
async def getFrontEndResponse(request: Request, *, __username: str = Depends(authenticate_user)):
    params={
        'query':dict(request.query_params),
        'username':__username
    }
    myResponse=await server.APIServerUtils.frontend.getResponse(params)
    return myResponse

@app.get("/userMeta") #FrontEnd
async def getUserMetaResponse(request: Request, *, __username: str = Depends(authenticate_user)):
    myResponse=await server.APIServerUtils.user.getUserMetaResponse(__username)
    return myResponse

@app.get("/health", tags=["Health"])
def health_check():
    return JSONResponse(content={"status": "ok"})

@app.get("/test-profanity-filter", tags=["Testing"])
async def test_profanity_filter():
    """Test endpoint to verify profanity filtering is working"""
    test_data = {
        "message": "This is a test with badword in it",
        "clean_message": "This is a clean message",
        "nested": {
            "another_badword": "text with badword",
            "clean": "clean text"
        },
        "array": ["badword", "clean", "another badword"]
    }
    return JSONResponse(content=test_data)

@app.post("/graphql-test", tags=["GraphQL Debug"])
async def graphql_test(request: Request):
    """Test endpoint to debug GraphQL issues"""
    try:
        logger.info("GraphQL test endpoint called")
        body = await request.body()
        logger.info(f"Request body: {body.decode()}")
        logger.info(f"Request headers: {dict(request.headers)}")
        
        return JSONResponse(content={
            "status": "success",
            "message": "GraphQL test endpoint working",
            "body_received": len(body),
            "headers_count": len(request.headers)
        })
    except Exception as e:
        logger.error(f"GraphQL test endpoint error: {e}", exc_info=True)
        return JSONResponse(content={"status": "error", "message": str(e)}, status_code=500)

@app.post('/login', description="Login and get access token", status_code=status.HTTP_200_OK, tags=["authentication"])
async def login(userCredentials: UserLogin):
    """Login with username and password to get JWT token"""
    user = authenticateUser(userCredentials.username, userCredentials.password)
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Bearer"},
        )
    
    # Create access token
    accessTokenExpires = timedelta(days=1)
    accessToken = createAccessToken(
        data={"sub": user["username"]}, expiresDelta=accessTokenExpires
    )
    
    return Token(
        accessToken=accessToken,
        tokenType="bearer",
        expiresIn=24 * 60 * 60,  # 1 day in seconds (86400)
        username=user["username"],
        displayName=user["displayName"],
        role=user["role"]
    )

@app.post('/logout', description="Logout user", status_code=status.HTTP_200_OK, tags=["authentication"])
async def logout(username: str = Depends(authenticate_user)):
    """Logout user (client should discard token)"""
    return {"message": "Successfully logged out", "username": username}

@app.get('/auth', description="Validate current token", status_code=status.HTTP_200_OK, tags=["authentication"])
async def validateToken(username: str = Depends(authenticate_user)):
    """Validate current JWT token"""
    user = getUserByUsername(username)
    return {
        "valid": True,
        "username": username,
        "displayName": user["displayName"],
        "role": user["role"]
    }

@app.post('/refresh-token', description="Refresh access token", status_code=status.HTTP_200_OK, tags=["authentication"])
async def refreshToken(refreshRequest: TokenRefreshRequest):
    """Refresh an existing JWT token by extending its expiry time"""
    try:
        # Verify the current token
        from rbac.utils.auth import verifyToken
        payload = verifyToken(refreshRequest.accessToken)
        
        if not payload:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid or expired token",
                headers={"WWW-Authenticate": "Bearer"},
            )
        
        username = payload.get("sub")
        if not username:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid token payload",
                headers={"WWW-Authenticate": "Bearer"},
            )
        
        # Extend the token by 30 minutes
        extendedToken = extendTokenExpiry(refreshRequest.accessToken, 30)
        
        if not extendedToken:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to extend token",
            )
        
        # Get user information
        user = getUserByUsername(username)
        
        return TokenRefreshResponse(
            accessToken=extendedToken,
            tokenType="bearer",
            expiresIn=30 * 60,  # 30 minutes in seconds
            message="Token refreshed successfully"
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error refreshing token: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Internal server error during token refresh",
        )

@app.post('/change-password', description="Change user password", status_code=status.HTTP_200_OK, tags=["authentication"])
async def changePassword(passwordData: PasswordChange, username: str = Depends(authenticate_user)):
    """Change user password"""
    from rbac.utils.auth import verifyPassword, getPasswordHash
    from rbac.utils.frontend import getUserByUsername
    # Ensure project root is on sys.path for Docker/runtime compatibility
    import sys
    from pathlib import Path
    _project_root = Path(__file__).resolve().parents[1]
    if str(_project_root) not in sys.path:
        sys.path.insert(0, str(_project_root))
    from database_models import DatabaseManager
    import json
    from pathlib import Path
    
    try:
        # Try database password change first
        db_manager = DatabaseManager()
        user = db_manager.get_user_by_username(username)
        
        if user:
            # Verify current password
            storedPassword = user.password_hash
            if storedPassword.startswith('$2b$'):
                # Hashed password
                if not verifyPassword(passwordData.currentPassword, storedPassword):
                    raise HTTPException(status_code=400, detail="Current password is incorrect")
            else:
                # Plain text password (legacy)
                import secrets
                if not secrets.compare_digest(passwordData.currentPassword, storedPassword):
                    raise HTTPException(status_code=400, detail="Current password is incorrect")
            
            # Hash new password
            newHashedPassword = getPasswordHash(passwordData.newPassword)
            
            # Update user in database
            session = db_manager.get_session()
            try:
                user.password_hash = newHashedPassword
                session.commit()
                return {"message": "Password changed successfully", "username": username}
            except Exception as e:
                session.rollback()
                print(f"Error updating password in database: {e}")
                # Fallback to JSON file update
            finally:
                session.close()
        
        # Fallback to JSON file password change
        user = getUserByUsername(username)
        if not user:
            raise HTTPException(status_code=404, detail="User not found")
        
        # Verify current password
        storedPassword = user.get('password', '')
        if storedPassword.startswith('$2b$'):
            # Hashed password
            if not verifyPassword(passwordData.currentPassword, storedPassword):
                raise HTTPException(status_code=400, detail="Current password is incorrect")
        else:
            # Plain text password (legacy)
            import secrets
            if not secrets.compare_digest(passwordData.currentPassword, storedPassword):
                raise HTTPException(status_code=400, detail="Current password is incorrect")
        
        # Hash new password
        newHashedPassword = getPasswordHash(passwordData.newPassword)
        
        # Update user in users.json
        users = getUserByUsername()
        for userData in users.values():
            if userData['username'] == username:
                userData['password'] = newHashedPassword
                break
        
        # Save updated users
        current_file = Path(__file__).resolve()
        users_json_path = current_file.parent.parent / "rbac" / "configs" / "users.json"
        
        with open(users_json_path, 'w') as f:
            json.dump(list(users.values()), f, indent=4)
        
        return {"message": "Password changed successfully", "username": username}
        
    except HTTPException:
        raise
    except Exception as e:
        print(f"Error in changePassword: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@app.get('/profile', description="Get user profile", status_code=status.HTTP_200_OK, tags=["authentication"])
async def getUserProfile(username: str = Depends(authenticate_user)):
    """Get current user profile information"""
    user = getUserByUsername(username)
    return UserResponse(
        username=user["username"],
        displayName=user["displayName"],
        roleStr=user["roleStr"],
        role=user["role"],
        email=user["email"],
        role_id=user["role_id"],
        client_id=user["client_id"],
        role_name=user["role_name"],
        client_name=user["client_name"],
        first_name=user["first_name"],
        last_name=user["last_name"],
        job_title=user["job_title"]
    )
    
# Cache for genie responses to avoid repeated LLM calls
genie_response_cache = {}
CACHE_TTL_SECONDS = 300  # 5 minutes cache

def _get_cached_genie_response(question: str, fund_name: str = None) -> Optional[Dict]:
    """Get cached response for genie question"""
    cache_key = f"{question.lower().strip()}|{fund_name or 'no_fund'}"
    cached_data = genie_response_cache.get(cache_key)
    
    if cached_data:
        import time
        if time.time() - cached_data['timestamp'] < CACHE_TTL_SECONDS:
            logger.info(f"Returning cached response for question: '{question}'")
            return cached_data['response']
        else:
            # Remove expired cache entry
            del genie_response_cache[cache_key]
    
    return None

def _cache_genie_response(question: str, response: Dict, fund_name: str = None):
    """Cache genie response"""
    import time
    cache_key = f"{question.lower().strip()}|{fund_name or 'no_fund'}"
    genie_response_cache[cache_key] = {
        'response': response,
        'timestamp': time.time()
    }
    logger.info(f"Cached response for question: '{question}'")

@app.post("/genie", description="Genie endpoint that returns answers to NAV validation questions", tags=["genie"])
async def genie_response(question: str = Query(None), context: Optional[Dict[str, Any]] = Body(default=None), *, __username: str = Depends(authenticate_user)):
    """Returns answers to NAV validation questions - OPTIMIZED VERSION"""
    # If no specific question is asked, return the default response
    if not question:
        return {
            "message": "Your wish is my command! 🧞‍♂️",
            "username": __username,
            "available_questions": [
                "Which category of NAV validations have failed?",
                "Can you give the impact of override of NAV on each category on NAV?",
                "Which failed NAV validations assigned are not yet actioned?",
                "Which failed NAV validations assigned are greater than 5 business days?",
                "For which all securities prices are missing?",
                "For which all securities prices are unchanged?",
                "Show the benchmark & NAV returns % comparison period over period line chart for past 3 months",
                "Can you give me the bar chart for all the expenses in February",
            ]
        }
    
    # Extract fund_name from context if provided
    fund_name = None
    if context and isinstance(context, dict):
        fund_name = context.get('context', {}).get('sourceA')
    
    # OPTIMIZATION 1: Check cache first to avoid expensive LLM calls
    cached_response = _get_cached_genie_response(question, fund_name)
    if cached_response:
        return cached_response
    
    # OPTIMIZATION 2: Check for hardcoded patterns first (faster than LLM)
    question_lower = question.lower()
    
    # Quick pattern matching for common questions (avoid LLM call)
    if any(pattern in question_lower for pattern in ["category", "failed"]) and "nav" in question_lower:
        validation_data = getNAVValidationData()
        failed_categories = [cat["category"] for cat in validation_data["failed_categories"]]
        answer = ", ".join(failed_categories)
        response = {
            "question": question,
            "answer": answer,
            "username": __username,
            "details": {
                "failed_categories": failed_categories,
                "total_failed": validation_data["total_failed"],
                "data_source": "system"
            }
        }
        _cache_genie_response(question, response, fund_name)
        return response
    
    # STEP 1: Try athenaResponse for complex questions (with caching)
    try:
        params = {
            'query': {
                'question': question,
                'fund_name': fund_name
            }
        }
        athena_result = await athenaResponse(params)
        
        # STEP 2: Check if athenaResponse succeeded (no error and has meaningful content)
        if athena_result and "error" not in athena_result:
            # Check if response has meaningful content
            text = athena_result.get("text", "")
            modules = athena_result.get("modules", [])
            
            # If we have meaningful content, cache and return it immediately
            if text and text != "No results found for your question." and text.strip():
                _cache_genie_response(question, athena_result, fund_name)
                return athena_result
        
        # If athenaResponse failed or returned no meaningful content, fall back to hardcoded logic
        logger.info(f"Athena response insufficient for question: '{question}', falling back to hardcoded logic")
    except Exception as e:
        # If athenaResponse throws an exception, fall back to hardcoded logic
        logger.warning(f"Athena response failed with error: {e}, falling back to hardcoded logic")
    
    # STEP 3 & 4: Fallback to hardcoded question matching logic
    question_lower = question.lower()
    
    # OPTIMIZATION 3: More early return patterns for common questions
    if "impact" in question_lower and "override" in question_lower and "nav" in question_lower:
        validation_data = getNAVValidationData()
        override_impacts = []
        for cat in validation_data["failed_categories"]:
            override_impacts.append(f"{cat['category']}: {cat['override_impact']}")
        
        response = {
            "question": question,
            "answer": f"Override impacts: {'; '.join(override_impacts)}. Total impact: {validation_data['total_override_impact']}",
            "username": __username,
            "details": {
                "override_impacts": validation_data["failed_categories"],
                "total_override_impact": validation_data["total_override_impact"],
                "data_source": "system"
            }
        }
        _cache_genie_response(question, response, fund_name)
        return response
    
    if "unactioned" in question_lower or "not yet actioned" in question_lower:
        # Quick response for unactioned validations
        response = {
            "question": question,
            "answer": "Currently checking unactioned NAV validations...",
            "username": __username,
            "details": {
                "unactioned_count": 12,  # This would come from actual data
                "data_source": "system"
            }
        }
        _cache_genie_response(question, response, fund_name)
        return response
    
    if "greater than 5" in question_lower and "business days" in question_lower:
        # Quick response for overdue validations
        response = {
            "question": question,
            "answer": "Currently checking overdue NAV validations (>5 business days)...",
            "username": __username,
            "details": {
                "overdue_count": 8,  # This would come from actual data
                "data_source": "system"
            }
        }
        _cache_genie_response(question, response, fund_name)
        return response
    
    if "missing" in question_lower and "prices" in question_lower:
        # Quick response for missing prices
        response = {
            "question": question,
            "answer": "Currently checking securities with missing prices...",
            "username": __username,
            "details": {
                "missing_prices_count": 5,  # This would come from actual data
                "data_source": "system"
            }
        }
        _cache_genie_response(question, response, fund_name)
        return response
    
    if "unchanged" in question_lower and "prices" in question_lower:
        # Quick response for unchanged prices
        response = {
            "question": question,
            "answer": "Currently checking securities with unchanged prices...",
            "username": __username,
            "details": {
                "unchanged_prices_count": 3,  # This would come from actual data
                "data_source": "system"
            }
        }
        _cache_genie_response(question, response, fund_name)
        return response
    
    # Question 1: Which category of NAV validations have failed?
    if "category" in question_lower and "failed" in question_lower:
        validation_data = getNAVValidationData()
        failed_categories = [cat["category"] for cat in validation_data["failed_categories"]]
        answer = ", ".join(failed_categories)
        return {
            "question": question,
            "answer": answer,
            "username": __username,
            "details": {
                "failed_categories": failed_categories,
                "total_failed": validation_data["total_failed"],
                "data_source": "system"
            }
        }
    
    # Question 2: Can you give the impact of override of NAV on each category on NAV?
    elif "impact" in question_lower and "override" in question_lower:
        validation_data = getNAVValidationData()
        override_impacts = {}
        total_impact = 0.0
        for cat in validation_data["failed_categories"]:
            impact_str = cat["override_impact"]
            override_impacts[cat["category"]] = impact_str
            # Convert percentage to float for calculation
            impact_value = float(impact_str.replace("%", ""))
            total_impact += impact_value
        
        # Build answer string
        impact_answers = []
        for category, impact in override_impacts.items():
            impact_answers.append(f"{category} override impact {impact}")
        answer = " | ".join(impact_answers)
        return {
            "question": question,
            "answer": answer,
            "username": __username,
            "details": {
                "override_impacts": override_impacts,
                "total_impact": f"{total_impact:.5f}%",
                "data_source": "system"
            }
        }
    
    # Question 3: Which failed NAV validations assigned are not yet actioned?
    elif "not yet actioned" in question_lower or "not actioned" in question_lower:
        validation_data = getNAVValidationData()
        unactioned_categories = [
            cat["category"] for cat in validation_data["failed_categories"]
            if cat["status"] == "unactioned"
        ]
        answer = ", ".join(unactioned_categories) if unactioned_categories else "None"
        return {
            "question": question,
            "answer": answer,
            "username": __username,
            "details": {
                "unactioned_validations": unactioned_categories,
                "count": len(unactioned_categories),
                "data_source": "system"
            }
        }
    
    # Question 4: Which failed NAV validations assigned are greater than 5 business days?
    elif "greater than 5 business days" in question_lower or "5 business days" in question_lower:
        validation_data = getNAVValidationData()
        overdue_categories = [
            cat["category"] for cat in validation_data["failed_categories"]
            if cat["days_overdue"] > 5
        ]
        answer = " & ".join(overdue_categories) if overdue_categories else "None"
        return {
            "question": question,
            "answer": answer,
            "username": __username,
            "details": {
                "overdue_validations": overdue_categories,
                "count": len(overdue_categories),
                "days_overdue": ">5 business days",
                "data_source": "system"
            }
        }
    
    # Question 5: For which all securities prices are missing?
    elif "securities prices are missing" in question_lower or "missing prices" in question_lower:
        validation_data = getNAVValidationData()
        missing_price_securities = []
        for cat in validation_data["failed_categories"]:
            if cat["category"] == "Missing Price":
                missing_price_securities = cat["securities"]
                break
        answer = " & ".join(missing_price_securities) if missing_price_securities else "None"
        return {
            "question": question,
            "answer": answer,
            "username": __username,
            "details": {
                "securities_with_missing_prices": missing_price_securities,
                "count": len(missing_price_securities),
                "validation_type": "Missing Price",
                "data_source": "system"
            }
        }
    
    # Question 6: For which all securities prices are unchanged?
    elif "securities prices are unchanged" in question_lower or "unchanged prices" in question_lower:
        validation_data = getNAVValidationData()
        unchanged_price_securities = []
        for cat in validation_data["failed_categories"]:
            if cat["category"] == "Unchanged Price":
                unchanged_price_securities = cat["securities"]
                break
        answer = " & ".join(unchanged_price_securities) if unchanged_price_securities else "None"
        return {
            "question": question,
            "answer": answer,
            "username": __username,
            "details": {
                "securities_with_unchanged_prices": unchanged_price_securities,
                "count": len(unchanged_price_securities),
                "validation_type": "Unchanged Price",
                "data_source": "system"
            }
        }
    
    # Question 16: Show the benchmark & NAV returns % comparison period over period line chart for past 3 months
    elif "benchmark" in question_lower and "nav returns" in question_lower and "chart" in question_lower and "line" in question_lower:
        from utils.navValidationUtils import generate_chart_response
        try:
            # Use the generic chart generation system
            # This uses data models and configurations to generate any chart
            response_data = generate_chart_response(
                configuration_name="nav_benchmark_returns_comparison",
                fund_name="NexBridge",
                source_name="Bluefield")
            return {
                "response": {
                    "question": question,
                    "text": "Here's the NAV vs Benchmark returns % comparison chart for the past 3 months:",
                    "username": __username,
                    "modules": response_data["modules"],
                    "metadata": response_data["metadata"],
                    "details": {
                        "configuration_used": response_data["metadata"]["configuration"],
                        "data_sources": response_data["metadata"]["source_count"],
                        "chart_type": response_data["metadata"]["chart_type"]
                    }
                },
            }
        except Exception as e:
            return {
                "question": question,
                "answer": f"Error generating chart data: {str(e)}",
                "username": __username,
                "error": str(e)
            }
    
    # Question 17: Can you give the bar chart for legal fees for prior 6 months
    elif ("legal expense" in question_lower or "legal fees" in question_lower) and ("trend" in question_lower or "chart" in question_lower) and ("six months" in question_lower or "6 months" in question_lower):
        from utils.navValidationUtils import generate_chart_response
        try:
            # Generate legal expense trend chart using configuration-based approach
            chart_result = generate_chart_response(configuration_name="legal_expense_trend")
            # Format response for genie
            return {
                "response": {
                    "question": question,
                    "text": "Here's the Legal Expense trend chart for 6 months (Jan-Jun):",
                    "username": __username,
                    "modules": chart_result["modules"],
                    "metadata": chart_result["metadata"],
                    "details": {
                        "configuration_used": chart_result["metadata"]["configuration"],
                        "data_sources": chart_result["metadata"]["source_count"],
                        "chart_type": chart_result["metadata"]["chart_type"]
                    }
                },
            }
        except Exception as e:
            return {
                "question": question,
                "answer": f"Error generating legal expense trend chart: {str(e)}",
                "username": __username,
                "error": str(e)
            }
    else:
        # If we reached here, no hardcoded pattern matched
        # Return athena_result if it exists (even if it wasn't perfect), otherwise return generic help
        if 'athena_result' in locals() and athena_result:
            return athena_result
        
        # Final fallback: generic help message
        return {
            "message": f"Can you please provide a more specific question?",
            "username": __username,
            "suggestions": [
                "Try asking about NAV validation categories",
                "Ask about override impacts",
                "Inquire about unactioned validations",
                "Check for overdue validations (>5 business days)",
                "Ask about securities with missing prices",
                "Ask about securities with unchanged prices"
            ],
            "example_questions": [
                "Which category of NAV validations have failed?",
                "Can you give the impact of override of NAV on each category on NAV?",
                "Which failed NAV validations assigned are not yet actioned?",
                "Which failed NAV validations assigned are greater than 5 business days?",
                "For which all securities prices are missing?",
                "For which all securities prices are unchanged?",
                "Show the benchmark & NAV returns % comparison period over period line chart for past 3 months",
                "Can you give me the bar chart for all the expenses in February"
            ]
        }


@app.post("/api/genie/cache/clear", description="Clear genie response cache", tags=["genie"])
async def clear_genie_cache(*, __username: str = Depends(authenticate_user)):
    """Clear all genie response cache entries"""
    try:
        global genie_response_cache
        cache_size = len(genie_response_cache)
        genie_response_cache.clear()
        return {
            "message": f"Cleared {cache_size} cached genie responses",
            "cache_size_before": cache_size,
            "cache_size_after": 0
        }
    except Exception as e:
        logger.error(f"Error clearing genie cache: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Error clearing genie cache: {str(e)}"
        )

@app.get("/api/genie/cache/info", description="Get genie cache information", tags=["genie"])
async def get_genie_cache_info(*, __username: str = Depends(authenticate_user)):
    """Get information about the current genie cache state"""
    try:
        global genie_response_cache
        import time
        current_time = time.time()
        
        # Calculate cache statistics
        total_entries = len(genie_response_cache)
        expired_entries = 0
        valid_entries = 0
        
        for cache_key, cache_data in genie_response_cache.items():
            if current_time - cache_data['timestamp'] >= CACHE_TTL_SECONDS:
                expired_entries += 1
            else:
                valid_entries += 1
        
        return {
            "total_entries": total_entries,
            "valid_entries": valid_entries,
            "expired_entries": expired_entries,
            "cache_ttl_seconds": CACHE_TTL_SECONDS,
            "cache_keys": list(genie_response_cache.keys())[:10]  # Show first 10 keys
        }
    except Exception as e:
        logger.error(f"Error getting genie cache info: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Error getting genie cache info: {str(e)}"
        )
async def clear_validation_cache(*, __username: str = Depends(authenticate_user)):
    """
    Clear all validation cache entries to force fresh data retrieval
    Use this when database data has been updated and you need fresh validation results
    """
    try:
        from frontendUtils.renders.validus.singleFundCompare import clearValidationCache
        result = clearValidationCache()
        return result
    except Exception as e:
        logger.error(f"Error clearing validation cache: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Error clearing validation cache: {str(e)}"
        )

@app.post("/api/validation/cache/invalidate", description="Invalidate specific validation cache entries", tags=["validation"])
async def invalidate_validation_cache(
    fund_name: str = None,
    source: str = None,
    date: str = None,
    *, __username: str = Depends(authenticate_user)
):
    """
    Invalidate specific validation cache entries based on fund, source, or date
    Use this for targeted cache invalidation when specific data is updated
    """
    try:
        from frontendUtils.renders.validus.singleFundCompare import invalidateValidationCache
        result = invalidateValidationCache(fund_name, source, date)
        return result
    except Exception as e:
        logger.error(f"Error invalidating validation cache: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Error invalidating validation cache: {str(e)}"
        )

# Upload file API for dashboard file uploads and details 
@app.post("/upload_files", description="Upload files to frameDemo/l0 folder", tags=["file-upload"])
async def upload_files(
    file: UploadFile = File(...),
    folder: str = Form("l0"),
    storage_type: str = Form("local"), 
    source: str = Form("api"),
    file_classification: str = Form(""),
    *, __username: str = Depends(authenticate_user)
):
    try:
        from storage import STORAGE
        from utils.unclassified import getFileHash, getISO8601FromPDFDate
        import filetype
        from PyPDF2 import PdfReader
        
        myStorageConfig = {
            'defaultFileStorage': 'onPrem',
        }
        client = 'frameDemo'
        myStorage = STORAGE(client, myStorageConfig)
        
        target_dir = myStorage.getLayerNFolder('l0')
        os.makedirs(target_dir, exist_ok=True)
        
        if file.filename is None:
            raise HTTPException(status_code=400, detail="Filename is required")
        file_path = os.path.join(target_dir, file.filename)
      
              
        with open(file_path, "wb") as buffer:
            shutil.copyfileobj(file.file, buffer)
        
        myDataOp = {
            "dataTypeToSaveAs": "statusUpdate",
            "opParams": {
                "layerName": "l2",
                "trackerName": "processedFiles", 
                "operation": "replaceOrAppendByKey",
                "key": [file.filename]
            }
        }
        myStorage.doDataOperation(myDataOp)
        
        myHash = getFileHash(file_path)
        
        # Always process the file for l1 layer if not already processed
        if myHash not in myStorage.getAllLayerNFiles('l1'):
            fileKind = filetype.guess(file_path)
            
            # Handle cases where filetype can't determine the type
            if fileKind is None:
                file_extension = os.path.splitext(file.filename)[1].lower()
                if file_extension == '.txt':
                    mime_type = 'text/plain'
                elif file_extension == '.csv':
                    mime_type = 'text/csv'
                elif file_extension == '.json':
                    mime_type = 'application/json'
                elif file_extension == '.xlsx':
                    mime_type = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
                elif file_extension == '.xls':
                    mime_type = 'application/vnd.ms-excel'
                else:
                    mime_type = 'application/octet-stream'
            else:
                mime_type = fileKind.mime
            
            myMetaData = {
                'typeName': mime_type,
                'fileHash': myHash,
                'fileOriginalName': file.filename,
                'fileOriginalPath': file_path,
                "typeSpecificParams": {}
            }
            
            if myMetaData['typeName'] == 'application/pdf':
                try:
                    reader = PdfReader(file_path)
                    myMetaData['typeSpecificParams']['numPages'] = len(reader.pages)
                    myMetaData['typeSpecificParams']['EXIF'] = {}
                    if reader.metadata is not None:
                        for key, value in reader.metadata.items():
                            if key in ['/CreationDate', '/ModDate']:
                                myMetaData['typeSpecificParams']['EXIF'][key] = getISO8601FromPDFDate(value)
                            else:
                                myMetaData['typeSpecificParams']['EXIF'][key] = value
                except Exception as e:
                    myMetaData['typeSpecificParams']['error'] = f"Could not read PDF metadata: {str(e)}"
            
            hash_dir = myStorage.getDir('l1', [myHash])
            
            myMetaDataOp = {
                "dataTypeToSaveAs": "JSONDump",
                "opParams": {
                    "layerName": "l1",
                    "folderArray": [myHash],
                    "operation": "replace"
                },
                "data": myMetaData,
                "key": "fileMetaData"
            }
            myStorage.doDataOperation(myMetaDataOp)
            
            # Copy file to l1 layer with proper extension
            file_extension = os.path.splitext(file.filename)[1] or '.pdf'
            raw_file_path = os.path.join(hash_dir, f'rawFile{file_extension}')
            shutil.copy2(file_path, raw_file_path)
        
        # Always update ldummy layer regardless of whether file was already in l1
        try:
            existing_ldummy = myStorage.getJSONDump('ldummy', '', 'allFileMeta')
            if not isinstance(existing_ldummy, dict):
                existing_ldummy = {}
        except:
            existing_ldummy = {}
        
        # Determine file type based on filename and MIME type (AI classification will be updated later after processing)
        filename_lower = file.filename.lower()
        if "statement" in filename_lower:
            file_type = "Fund Statement"
        elif "capcall" in filename_lower or "capital_call" in filename_lower:
            file_type = "Capital Call Notice" 
        elif "distribution" in filename_lower:
            file_type = "Distribution Notice"
        elif "factsheet" in filename_lower:
            file_type = "Fund Fact Sheet"
        elif "ppm" in filename_lower:
            file_type = "Private Placement Memorandum"
        elif "k1" in filename_lower:
            file_type = "K1"
        elif file.filename.lower().endswith('.pdf'):
            file_type = "PDF Document"
        elif file.filename.lower().endswith('.txt'):
            file_type = "Text Document"
        elif file.filename.lower().endswith(('.csv', '.xlsx', '.xls')):
            file_type = "Spreadsheet"
        elif file.filename.lower().endswith('.json'):
            file_type = "JSON Document"
        else:
            file_type = "Document"
        
        existing_ldummy[file.filename] = {
            "fileHash": myHash,
            "fileType": file_type,
            "status": "Processed",
            "fileName": file.filename
        }
        
        myLdummyOp = {
            "dataTypeToSaveAs": "JSONDump",
            "opParams": {
                "layerName": "ldummy",
                "folderArray": [],
                "operation": "replace"
            },
            "data": existing_ldummy,
            "key": "allFileMeta"
        }
        myStorage.doDataOperation(myLdummyOp)
        
        # Start file processing in background thread     
        def run_processor():
            try:
                return process_file_with_orchestrator(file_path, file.filename)
            except Exception as e:
                print(f"Background processing failed for {file.filename}: {e}")
        
        # Submit to background thread without waiting for result
        executor = ThreadPoolExecutor(max_workers=1)
        executor.submit(run_processor)
        
        return JSONResponse(content={
            "response": True,
            "filename": file.filename,
            "message": f"Successfully uploaded {file.filename}",
            "file_size": file.size,
            "file_path": file_path,
            "file_hash": myHash,
            "username": __username
        })
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Upload failed: {str(e)}")

@app.post("/start_processing", description="Start automatic file processing session", tags=["processing"])
async def start_processing(__username: str = Depends(authenticate_user)):
    """
    Start automatic processing of queued files.
    Processes until queue is empty.
    """
    return JSONResponse(content={
            "success": True,
            "message": "Processing started successfully",
            "status": "processing",
            "username": __username
        })

    # try:
    #     import sys
    #     import os
    #     from pathlib import Path
        
    #     # Get the parent directory (validusBoxes)
    #     current_dir = Path(__file__).parent.parent
    #     runner_file = current_dir / "runner_frame.py"
        
    #     if not runner_file.exists():
    #         raise HTTPException(status_code=500, detail=f"Runner file not found at: {runner_file}")
        
    #     # Add to Python path
    #     if str(current_dir) not in sys.path:
    #         sys.path.insert(0, str(current_dir))
        
    #     try:
    #         # Import the module
    #         import runner_frame
    #         from importlib import reload
    #         reload(runner_frame)  # Force reload in case of changes
            
    #         # Check if function exists
    #         if not hasattr(runner_frame, 'run_processing_session'):
    #             raise HTTPException(status_code=500, detail="run_processing_session function not found in runner_frame")
            
    #         # Run the processing session in a background thread
    #         def run_processing():
    #             try:
    #                 # Change to the runner_frame directory to ensure relative paths work
    #                 original_cwd = os.getcwd()
    #                 os.chdir(current_dir)
                    
    #                 try:
    #                     result = runner_frame.run_processing_session()
    #                     return result
    #                 finally:
    #                     # Always restore original working directory
    #                     os.chdir(original_cwd)
                        
    #             except Exception as e:
    #                 print(f"Error in run_processing_session: {e}")
    #                 return {"status": "error", "message": str(e)}
            
    #         # Start processing in background
    #         executor = ThreadPoolExecutor(max_workers=1)
    #         future = executor.submit(run_processing)
            
    #         return JSONResponse(content={
    #             "success": True,
    #             "message": "Processing started successfully",
    #             "status": "processing",
    #             "username": __username
    #         })
            
    #     except ImportError as e:
    #         raise HTTPException(status_code=500, detail=f"Failed to import runner_frame: {e}")
            
    # except Exception as e:
    #     raise HTTPException(status_code=500, detail=f"Processing failed: {str(e)}")


@app.post("/process_existing_files", description="Process all existing files in l0 directory and add them to ldummy layer", tags=["file-processing"])
async def process_existing_files(*, __username: str = Depends(authenticate_user)):
    """Process all existing files in l0 directory and add them to ldummy layer"""
    try:
        from storage import STORAGE
        from utils.unclassified import getFileHash, getISO8601FromPDFDate
        import filetype
        from PyPDF2 import PdfReader
        
        myStorageConfig = {
            'defaultFileStorage': 'onPrem',
        }
        client = 'frameDemo'
        myStorage = STORAGE(client, myStorageConfig)
        
        # Get all files in l0 directory
        l0_files = myStorage.getAllLayerNFiles('l0')
        
        # Get existing ldummy data
        try:
            existing_ldummy = myStorage.getJSONDump('ldummy', '', 'allFileMeta')
            if not isinstance(existing_ldummy, dict):
                existing_ldummy = {}
        except:
            existing_ldummy = {}
        
        processed_count = 0
        skipped_count = 0
        
        for filename in l0_files:
            file_path = myStorage.getLocalFilePath('l0', filename)
            myHash = getFileHash(file_path)            

            # Add to ldummy with initial status (will be updated after processing)
            existing_ldummy[filename] = {
                "fileHash": myHash,
                "fileType": "Pending",
                "status": "Pending",
                "fileName": filename
            }

            # Check if file is already in l1 (processed)
            if myHash in myStorage.getAllLayerNFiles('l1'):
                skipped_count += 1
                continue

            
            # Process the file
            try:
                fileKind = filetype.guess(file_path)
                
                # Handle cases where filetype can't determine the type
                if fileKind is None:
                    file_extension = os.path.splitext(filename)[1].lower()
                    if file_extension == '.txt':
                        mime_type = 'text/plain'
                    elif file_extension == '.csv':
                        mime_type = 'text/csv'
                    elif file_extension == '.json':
                        mime_type = 'application/json'
                    elif file_extension == '.xlsx':
                        mime_type = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
                    elif file_extension == '.xls':
                        mime_type = 'application/vnd.ms-excel'
                    else:
                        mime_type = 'application/octet-stream'
                else:
                    mime_type = fileKind.mime
                
                myMetaData = {
                    'typeName': mime_type,
                    'fileHash': myHash,
                    'fileOriginalName': filename,
                    'fileOriginalPath': file_path,
                    'typeSpecificParams': {}
                }
                
                if myMetaData['typeName'] == 'application/pdf':
                    try:
                        reader = PdfReader(file_path)
                        myMetaData['typeSpecificParams']['numPages'] = len(reader.pages)
                        myMetaData['typeSpecificParams']['EXIF'] = {}
                        if reader.metadata is not None:
                            for key, value in reader.metadata.items():
                                if key in ['/CreationDate', '/ModDate']:
                                    myMetaData['typeSpecificParams']['EXIF'][key] = getISO8601FromPDFDate(value)
                                else:
                                    myMetaData['typeSpecificParams']['EXIF'][key] = value
                    except Exception as e:
                        myMetaData['typeSpecificParams']['error'] = f"Could not read PDF metadata: {str(e)}"
                
                # Create l1 directory and save metadata
                hash_dir = myStorage.getDir('l1', [myHash])
                
                myMetaDataOp = {
                    "dataTypeToSaveAs": "JSONDump",
                    "opParams": {
                        "layerName": "l1",
                        "folderArray": [myHash],
                        "operation": "replace"
                    },
                    "data": myMetaData,
                    "key": "fileMetaData"
                }
                myStorage.doDataOperation(myMetaDataOp)
                
                # Copy file to l1 layer with proper extension
                file_extension = os.path.splitext(filename)[1] or '.pdf'
                raw_file_path = os.path.join(hash_dir, f'rawFile{file_extension}')
                shutil.copy2(file_path, raw_file_path)
                
                # Determine file type based on filename
                filename_lower = filename.lower()
                if "statement" in filename_lower:
                    file_type = "Fund Statement"
                elif "capcall" in filename_lower or "capital_call" in filename_lower:
                    file_type = "Capital Call Notice" 
                elif "distribution" in filename_lower:
                    file_type = "Distribution Notice"
                elif "factsheet" in filename_lower:
                    file_type = "Fund Fact Sheet"
                elif "ppm" in filename_lower:
                    file_type = "Private Placement Memorandum"
                elif "k1" in filename_lower:
                    file_type = "K1"
                elif filename.lower().endswith('.pdf'):
                    file_type = "PDF Document"
                elif filename.lower().endswith('.txt'):
                    file_type = "Text Document"
                elif filename.lower().endswith(('.csv', '.xlsx', '.xls')):
                    file_type = "Spreadsheet"
                elif filename.lower().endswith('.json'):
                    file_type = "JSON Document"
                else:
                    file_type = "Document"
                
                # Add to ldummy
                existing_ldummy[filename] = {
                    "fileHash": myHash,
                    "fileType": file_type,
                    "status": "Processed",
                    "fileName": filename
                }
                
                processed_count += 1
                
            except Exception as e:
                # Add to ldummy with error status
                existing_ldummy[filename] = {
                    "fileHash": myHash,
                    "fileType": "Error",
                    "status": "Failed",
                    "fileName": filename,
                    "error": str(e)
                }
        
        # Save updated ldummy data
        if processed_count > 0:
            myLdummyOp = {
                "dataTypeToSaveAs": "JSONDump",
                "opParams": {
                    "layerName": "ldummy",
                    "folderArray": [],
                    "operation": "replace"
                },
                "data": existing_ldummy,
                "key": "allFileMeta"
            }
            myStorage.doDataOperation(myLdummyOp)
        
        return JSONResponse(content={
            "response": True,
            "message": f"Processed {processed_count} files, skipped {skipped_count} files",
            "processed_count": processed_count,
            "skipped_count": skipped_count,
            "total_files_in_ldummy": len(existing_ldummy),
            "username": __username
        })
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Upload failed: {str(e)}")


from utils.file_config_reader import readFileConfigs

@app.get("/get-file-config", description="Get file configuration settings", tags=["file-config"])
async def getFileConfig(*, __username: str = Depends(authenticate_user)):
    """
    Get file configuration settings from utils/fileConfigs folder.
    Returns JSON schemas for document processing and classification.
    """
    try:
        # Use the utility function to read file configurations
        config_data = readFileConfigs()
        
        # Return the complete response
        return JSONResponse(content=config_data)
        
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(
            status_code=500, 
            detail=f"Failed to retrieve file configuration: {str(e)}"
        )


@app.get("/get-document-types/{document_type_id}", description="Get information about a specific document type by ID", tags=["file-config"])
async def getDocumentByIdRoute(
    document_type_id: str,
    *, __username: str = Depends(authenticate_user)
):
    """
    Get information about a specific document type by its ID.
    """
    try:
        # Use the utility function to get document by ID
        document_data = getDocumentById(document_type_id)
        
        # Check if document was found
        if "success" in document_data and not document_data["success"]:
            raise HTTPException(status_code=404, detail=document_data["error"])
        
        # Return the response
        return JSONResponse(content=document_data)
        
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(
            status_code=500, 
            detail=f"Failed to retrieve document: {str(e)}"
        )
        
@app.get("/get-document-types", description="Get basic document type information", tags=["file-config"])
async def getDocumentTypesRoute(
    document_type_id: str = None,
    *, __username: str = Depends(authenticate_user)
):
    """
    Get basic information about available document types.
    If document_type_id is provided, returns information about that specific document.
    Otherwise, returns information about all available document types.
    """
    try:
        # Use the utility function to get document types
        document_types_data = getDocumentTypes(document_type_id)
        
        # Return the response
        return JSONResponse(content=document_types_data)
        
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(
            status_code=500, 
            detail=f"Failed to retrieve document types: {str(e)}"
        )

@app.post("/create-document-type", description="Create a new document type", tags=["file-config"])
async def createDocumentTypeRoute(
    document_data: dict,
    *, __username: str = Depends(authenticate_user)
):
    """
    Create a new document type with the provided schema and metadata.
    
    Expected payload structure:
    {
        "document_type": "string",
        "document_description": "string", 
        "schema_blob": {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "object",
            "properties": {
                // JSON schema properties
            }
        },
        "is_active": true (optional, defaults to true)
    }
    """
    try:
        # Validate required fields
        required_fields = ["document_type", "document_description", "schema_blob"]
        for field in required_fields:
            if field not in document_data:
                raise HTTPException(
                    status_code=400, 
                    detail=f"Missing required field: {field}"
                )
        
        # Validate document_type is a string
        if not isinstance(document_data["document_type"], str):
            raise HTTPException(
                status_code=400,
                detail="document_type must be a string"
            )
        
        # Validate document_description is a string
        if not isinstance(document_data["document_description"], str):
            raise HTTPException(
                status_code=400,
                detail="document_description must be a string"
            )
        
        # Validate schema_blob is a dictionary
        if not isinstance(document_data["schema_blob"], dict):
            raise HTTPException(
                status_code=400,
                detail="schema_blob must be a valid JSON schema object"
            )
        
        # Use the utility function to create the document type
        result = createDocumentType(document_data)
        
        # Check if creation was successful
        if not result["success"]:
            raise HTTPException(status_code=400, detail=result["error"])
        
        # Return the response
        return JSONResponse(
            content=result,
            status_code=201
        )
        
    except HTTPException:
        # Re-raise HTTP exceptions as-is
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500, 
            detail=f"Failed to create document type: {str(e)}"
        )

@app.delete("/delete-document-type/{document_type_id}", description="Delete a document type by ID", tags=["file-config"])
async def deleteDocumentTypeRoute(
    document_type_id: str,
    *, __username: str = Depends(authenticate_user)
):
    """
    Delete a document type by its ID.
    
    This will permanently remove the document type configuration file.
    Use with caution as this action cannot be undone.
    """
    try:
        # Validate document_type_id is a valid integer
        try:
            int(document_type_id)
        except ValueError:
            raise HTTPException(
                status_code=400,
                detail="document_type_id must be a valid integer"
            )
        
        # Use the utility function to delete the document type
        result = deleteDocumentType(document_type_id)
        
        # Check if deletion was successful
        if not result["success"]:
            raise HTTPException(status_code=404, detail=result["error"])
        
        # Return the response
        return JSONResponse(
            content=result,
            status_code=200
        )
        
    except HTTPException:
        # Re-raise HTTP exceptions as-is
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500, 
            detail=f"Failed to delete document type: {str(e)}"
        )

# ============================================================================
# USER MANAGEMENT API ENDPOINTS
# ============================================================================


@app.get("/api/users/clients", description="Get all available clients", tags=["user-management"])
async def get_available_clients(
    *, __username: str = Depends(authenticate_user)
):
    """
    Get all available clients for user assignment.
    
    Returns a list of clients that can be assigned to users.
    """
    return await server.APIServerUtils.user_management.get_clients_response()

@app.get("/api/roles", description="Get all available roles", tags=["role-management"])
async def get_available_roles(
    *, __username: str = Depends(authenticate_user)
):
    """
    Get all available roles for user assignment.
    
    Returns a list of roles that can be assigned to users.
    """
    return await server.APIServerUtils.user_management.get_roles_response()

@app.get("/api/roles/summary", description="Get aggregated roles data with user statistics", tags=["role-management"])
async def get_roles_aggregated(
    page: int = 1,
    page_size: int = 10,
    *, __username: str = Depends(authenticate_user)
):
    """
    Get aggregated roles data including:
    - roleID: Role ID (database ID)
    - role: Role name
    - totalUsers: Total number of users with this role (as string)
    - products: Products assigned to role (Frame, NAV Validus)
    - status: Role status (Active/Inactive based on roles.is_active)
    
    Parameters:
    - page: Page number for pagination (default: 1)
    - page_size: Number of records per page (default: 10)
    
    Returns the complete frontend template with data populated in rowData.
    """
    return await server.APIServerUtils.user_management.get_roles_aggregated_response(page, page_size)

@app.patch("/api/roles/inactivate/reassignment", description="Inactivate role and reassign users", tags=["role-management"])
async def inactivate_role_with_user_reassignment(
    role_data: RoleInactivationRequest,
    *, __username: str = Depends(authenticate_user)
):
    """
    Inactivate a role and reassign all its users to new roles.
    
    Supports two scenarios:
    
    **Scenario 1: Assign same role to all users**
    ```json
    {
      "role_id": "ROLE0001",
      "assignSame": true,
      "role": "Manager"
    }
    ```
    
    **Scenario 2: Assign different roles to each user**
    ```json
    {
      "role_id": "ROLE0001", 
      "assignSame": false,
      "users": [
        {
          "nameOfUsers": "Sam Johnson",
          "role": "Admin"
        },
        {
          "nameOfUsers": "Mike Little", 
          "role": "Manager"
        }
      ]
    }
    ```
    
    - **role_id**: Role ID to inactivate (supports both "ROLE0001" format and raw ID)
    - **assignSame**: Boolean - true for same role assignment, false for different roles
    - **role**: New role code (required when assignSame=true)
    - **users**: Array of user assignments (required when assignSame=false)
    """
    return await server.APIServerUtils.user_management.inactivate_role_with_user_reassignment_response(role_data.dict())

@app.get("/api/roles/details", description="Get detailed role information", tags=["role-management"])
async def get_role_details(
    role_id: int = Query(..., description="Role ID to get details for"),
    edit_mode: bool = Query(False, description="Return response in edit form format"),
    *, __username: str = Depends(authenticate_user)
):
    """
    Get detailed information about a specific role including permissions and users.
    
    Returns role details in the exact format required by the frontend, including:
    - Role basic information (name, description, ID, status, created date)
    - Associated users with their details
    - Module permissions with CRUD access levels
    - Complete frontend template structure
    
    - **role_id**: Query parameter - The ID of the role to retrieve details for
    - **edit_mode**: Boolean parameter - If true, returns edit form format; if false, returns detail view format
    
    Returns a comprehensive role object with all related data for frontend consumption.
    """
    return await server.APIServerUtils.user_management.get_role_details_response(role_id, edit_mode)

@app.post("/api/roles", description="Create a new role with optional permissions", tags=["role-management"])
async def create_role(
    role_data: RoleCreateRequest,
    *, __username: str = Depends(authenticate_user)
):
    """
    Create a new role with optional permissions for hierarchical modules.
    
    Required fields:
    - **role_name**: Role name (2-50 characters)
    
    Optional fields:
    - **role_code**: Unique role code (2-50 characters) - auto-generated from role_name if not provided
    - **description**: Role description (max 255 characters)
    - **is_active**: Whether role is active (default: true)
    - **permissions**: List of module permissions (supports hierarchical _children modules)
    
    The role_code is automatically generated from role_name by converting to lowercase and replacing spaces/hyphens with underscores.
    For example: "Test Role 1" -> "test_role_1"
    
    The permissions field supports hierarchical module structures with nested _children modules.
    Each module can have create, read, update, delete permissions, and child modules are processed recursively.
    
    Example with hierarchical permissions:
    {
        "role_name": "Auditor",
        "description": "Auditor role",
        "permissions": [
            {
                "module": "Frame",
                "create": true,
                "read": true,
                "update": false,
                "delete": false,
                "_children": [
                    {
                        "module": "Document Processing",
                        "create": false,
                        "read": true,
                        "update": false,
                        "delete": false
                    },
                    {
                        "module": "User Management",
                        "create": true,
                        "read": true,
                        "update": true,
                        "delete": false
                    }
                ]
            },
            {
                "module": "NAV Validus",
                "create": true,
                "read": true,
                "update": false,
                "delete": false
            }
        ]
    }
    """
    role_dict = role_data.dict()
    return await server.APIServerUtils.user_management.create_role_response(role_dict)

@app.put("/api/roles", description="Update an existing role with optional permissions", tags=["role-management"])
async def update_role(
    role_id: int = Query(..., description="The ID of the role to update"),
    role_data: RoleUpdateRequest = Body(...),
    *, __username: str = Depends(authenticate_user)
):
    """
    Update an existing role's information with optional permissions for hierarchical modules.
    
    - **role_id**: The ID of the role to update (query parameter)
    
    All fields are optional for updates:
    - **role_name**: Role name (2-50 characters)
    - **description**: Role description (max 255 characters)
    - **is_active**: Whether role is active
    - **permissions**: List of module permissions (supports hierarchical _children modules)
    
    **Note**: role_code is NOT editable and will be ignored if provided.
    
    The permissions field supports hierarchical module structures with nested _children modules.
    Each module can have create, read, update, delete permissions, and child modules are processed recursively.
    
    Example with hierarchical permissions:
    {
        "role_name": "Reviewer",
        "permissions": [
            {
                "module": "Frame",
                "create": true,
                "read": true,
                "update": false,
                "delete": false,
                "_children": [
                    {
                        "module": "Document Processing",
                        "create": false,
                        "read": true,
                        "update": false,
                        "delete": false
                    },
                    {
                        "module": "User Management",
                        "create": true,
                        "read": true,
                        "update": true,
                        "delete": false
                    }
                ]
            },
            {
                "module": "NAV Validus",
                "create": true,
                "read": true,
                "update": false,
                "delete": false
            }
        ]
    }
    """
    update_data = {k: v for k, v in role_data.dict().items() if v is not None}
    return await server.APIServerUtils.user_management.update_role_response(role_id, update_data)

@app.delete("/api/roles", description="Delete a role (soft delete by default)", tags=["role-management"])
async def delete_role(
    role_id: int = Query(..., description="The ID of the role to delete"),
    hard_delete: bool = Query(False, description="If true, permanently delete the role (default: false)"),
    *, __username: str = Depends(authenticate_user)
):
    """
    Delete a role by setting their status to inactive (soft delete) or permanently remove (hard delete).
    
    - **role_id**: The ID of the role to delete (query parameter)
    - **hard_delete**: If true, permanently delete the role (default: false) (query parameter)
    
    Note: Cannot delete roles that have active users assigned.
    """
    return await server.APIServerUtils.user_management.delete_role_response(role_id, hard_delete)

@app.patch("/api/roles/activate", description="Activate a role", tags=["role-management"])
async def activate_role(
    role_id: int = Query(..., description="The ID of the role to activate"),
    *, __username: str = Depends(authenticate_user)
):
    """
    Activate a role by setting its status to active.
    
    - **role_id**: The ID of the role to activate (query parameter)
    
    Returns the updated role information with active status.
    """
    return await server.APIServerUtils.user_management.activate_role_response(role_id)

@app.get("/api/users/org_details", description="Get organization details", tags=["user-management"])
async def get_organization_details(
    *, __username: str = Depends(authenticate_user)
):
    """
    Get organization details from organization_details.json template.
    
    Returns the organization details JSON template.
    """
    return await server.APIServerUtils.user_management.get_organization_details_response()

@app.get("/api/users", description="Get users with optional filtering", tags=["user-management"])
async def get_users(
    request: Request,
    user_id: Optional[int] = Query(None, description="Get a specific user by ID"),
    search: Optional[str] = Query(None, description="Search users by name, email, or username"),
    status_filter: Optional[str] = Query(None, description="Filter by status: active, inactive, or all"),
    page: int = Query(1, description="Page number for pagination"),
    page_size: int = Query(10, description="Number of users per page"),
    edit_mode: bool = Query(False, description="Return response in edit form format"),
    add_form: bool = Query(False, description="Return response in add form format"),
    *, __username: str = Depends(authenticate_user)
):
    """
    Get users with optional filtering and pagination.
    
    - **user_id**: Optional. Get a specific user by ID (required for edit_mode)
    - **search**: Optional. Search users by name, email, or username
    - **status_filter**: Optional. Filter by status: active, inactive, or all
    - **page**: Page number for pagination (default: 1)
    - **page_size**: Number of users per page (default: 10)
    - **edit_mode**: Return response in edit form format (requires user_id, default: false)
    - **add_form**: Return response in add form format (does not require user_id, default: false)
    
    Note: When add_form=true, the API returns a form template for creating new users and ignores user_id.
    When edit_mode=true, user_id is required to get the specific user's data for editing.
    """
    # Debug logging
    logger.info(f"get_users called with: user_id={user_id}, edit_mode={edit_mode}, add_form={add_form}")
    logger.info(f"Request URL: {request.url}")
    logger.info(f"Query params: {dict(request.query_params)}")
    logger.info(f"edit_mode type: {type(edit_mode)}, value: {edit_mode}")
    logger.info(f"add_form type: {type(add_form)}, value: {add_form}")
    
    # Handle add_form first - it doesn't require user_id since we're adding a new user
    if add_form:
        logger.info("Taking add_form path - returning add form template")
        return await server.APIServerUtils.user_management.get_user_add_form_response()
    
    if user_id:
        # If user_id is provided, get specific user
        logger.info(f"user_id provided: {user_id}, edit_mode: {edit_mode}")
        if edit_mode:
            logger.info("Taking edit_mode path")
            return await server.APIServerUtils.user_management.get_user_edit_form_response(user_id)
        else:
            logger.info("Taking default user details path")
            return await server.APIServerUtils.user_management.get_user_response(user_id)
    else:
        # Otherwise, get all users with pagination and filtering
        logger.info(f"No user_id provided, returning users list")
        return await server.APIServerUtils.user_management.get_users_response(search, status_filter, page, page_size)

@app.post("/api/users", description="Create user(s)", tags=["user-management"])
async def create_user(
    request: UnifiedUserCreateRequest,
    *, __username: str = Depends(authenticate_user)
):
    """
    Create single or multiple users with the specified details.
    
    For single user:
    ```json
    {
        "form_1": {
            "first_name": "Jane",
            "last_name": "Doe", 
            "email": "jane.doe@mail.com",
            "job_title": "Sales Head",
            "role_name": "admin",
            "password": "SecurePassword123"
        }
    }
    ```
    
    For multiple users:
    ```json
    {
        "form_1": {
            "first_name": "Jane",
            "last_name": "Doe", 
            "email": "jane.doe@mail.com",
            "job_title": "Sales Head",
            "role_name": "admin",
            "password": "SecurePassword123"
        },
        "form_2": {
            "first_name": "John",
            "last_name": "Smith",
            "email": "john@example.com",
            "job_title": "Developer",
            "role_name": "dev",
            "password": "AnotherPassword456"
        }
    }
    ```
    
    Required fields for each user:
    - **first_name**: User's first name (1-50 characters)
    - **last_name**: User's last name (1-50 characters)
    - **email**: User's email address
    - **job_title**: User's job title
    - **role_name**: User's role name (case-insensitive, e.g., "admin", "user", "dev", "manager")
    - **password**: User's password (minimum 6 characters)
    
    Optional fields:
    - **is_active**: Boolean (defaults to true)
    
    Note: Username will be auto-generated based on first name.
    Note: client_id will be automatically set from the authenticated user's client.
    """
    # Convert Pydantic model to dict format for the service
    validated_users = {}
    request_dict = request.dict()
    
    for form_id, user_data in request_dict.items():
        if user_data is not None:  # Skip None values (optional forms)
            validated_users[form_id] = user_data
    
    return await server.APIServerUtils.user_management.bulk_create_users_response(validated_users, __username)


@app.put("/api/users", description="Update an existing user", tags=["user-management"])
async def update_user(
    user_id: int = Query(..., description="The ID of the user to update"),
    request: UserUpdateRequest = Body(...),
    *, __username: str = Depends(authenticate_user)
):
    """
    Update an existing user's information.
    
    - **user_id**: The ID of the user to update (query parameter)
    
    All fields are optional - only provided fields will be updated:
    - **first_name**: User's first name (1-50 characters)
    - **last_name**: User's last name (1-50 characters)
    - **email**: User's email address (must be unique)
    - **job_title**: User's job title
    - **role_name**: User's role code (e.g., 'admin', 'user', 'manager', 'analyst')
    - **client_id**: Client ID to assign user to
    - **is_active**: Whether user is active
    - **password**: User's new password (minimum 6 characters)
    
    Note: If first_name or last_name is updated, display_name will be automatically updated.
    If password is updated, temp_password flag will be set to False.
    """
    # Convert the Pydantic model to dict and filter out None values
    update_data = {k: v for k, v in request.dict().items() if v is not None}
    
    return await server.APIServerUtils.user_management.update_user_response(user_id, update_data)

@app.patch("/api/users/status", description="Toggle user active/inactive status", tags=["user-management"], response_model=UserToggleStatusResponse)
async def toggle_user_status(
    request: UserToggleStatusRequest,
    *, __username: str = Depends(authenticate_user)
):
    """
    Toggle a user's active/inactive status.
    
    Request body:
    ```json
    {
        "user_id": 1,
        "active": true
    }
    ```
    
    - **user_id**: The ID of the user to update (integer)
    - **active**: Boolean value - true to activate, false to deactivate
    
    Returns the updated user information with the new status.
    """
    return await server.APIServerUtils.user_management.toggle_user_status_response(request.user_id, request.active)

@app.delete("/api/users", description="Delete a user (soft delete)", tags=["user-management"])
async def delete_user(
    user_id: int = Query(..., description="The ID of the user to delete"),
    *, __username: str = Depends(authenticate_user)
):
    """
    Delete a user by setting their status to inactive.
    
    - **user_id**: The ID of the user to delete (query parameter)
    
    This is a soft delete - the user record remains in the database
    but is marked as inactive.
    """
    return await server.APIServerUtils.user_management.delete_user_response(user_id)

@app.post("/api/users/bulk-update", description="Bulk update multiple users", tags=["user-management"])
async def bulk_update_users(
    bulk_data: BulkUpdateRequest,
    *, __username: str = Depends(authenticate_user)
):
    """
    Bulk update multiple users with the same changes.
    
    - **user_ids**: List of user IDs to update
    - **updates**: Dictionary of fields to update for all users
    
    Example updates:
    - {"is_active": false} - Deactivate multiple users
    - {"role": "user"} - Change role for multiple users
    """
    return await server.APIServerUtils.user_management.bulk_update_users_response(
        bulk_data.user_ids,
        bulk_data.updates
    )

@app.post("/api/users/reset-password", description="Reset user password", tags=["user-management"])
async def reset_user_password(
    user_id: int = Query(..., description="The ID of the user whose password to reset"),
    new_password: str = Body(..., description="The new password (minimum 6 characters)"),
    *, __username: str = Depends(authenticate_user)
):
    """
    Reset a user's password.
    
    - **user_id**: The ID of the user whose password to reset (query parameter)
    - **new_password**: The new password (minimum 6 characters) (request body)
    """
    if len(new_password) < 6:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Password must be at least 6 characters long"
        )
    
    # Hash the new password
    from rbac.utils.auth import getPasswordHash
    password_hash = getPasswordHash(new_password)
    
    # Update the user's password
    update_data = {"password_hash": password_hash}
    
    return await server.APIServerUtils.user_management.update_user_response(user_id, update_data)

@app.post("/api/users/toggle-status", description="Toggle user active status", tags=["user-management"])
async def toggle_user_status(
    user_id: int = Query(..., description="The ID of the user whose status to toggle"),
    *, __username: str = Depends(authenticate_user)
):
    """
    Toggle a user's active status (activate/deactivate).
    
    - **user_id**: The ID of the user whose status to toggle (query parameter)
    """
    # First get the current user to see their status
    user_detail = await server.APIServerUtils.user_management.user_service.get_user_by_id(user_id)
    # Since get_user_by_id now returns the response directly, we need to find the status in the sections
    current_status = None
    for section in user_detail.get('sections', []):
        for field in section.get('fields', []):
            if field.get('label') == 'Status':
                current_status = field.get('value') == 'Active'
                break
        if current_status is not None:
            break
    
    if current_status is None:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Could not determine current user status"
        )
    
    # Toggle the status
    new_status = not current_status
    update_data = {"is_active": new_status}
    
    return await server.APIServerUtils.user_management.update_user_response(user_id, update_data)



# ============================================================================
# Fund Manager API Endpoint
# ============================================================================

@app.get("/funds/managers", tags=["Fund Manager API"])
async def get_fund_managers(
    page: int = 1,
    page_size: int = 10,
    search: Optional[str] = None,
    status_filter: Optional[str] = None,
    *, __username: str = Depends(authenticate_user)
):
    """
    Get all fund managers with pagination and filtering
    Returns data in the format expected by fund_managers_template.json
    """
    return await server.APIServerUtils.fund_management.get_fund_managers_response(page, page_size, search, status_filter)


@app.get("/funds/managers/details", tags=["Fund Manager API"])
async def get_fund_manager_details(
    fund_manager_id: int = Query(..., description="Fund Manager ID"),
    edit_mode: bool = Query(False, description="Return response in edit form format"),
    *, __username: str = Depends(authenticate_user)
):
    """
    Get detailed information for a specific fund manager
    
    Returns fund manager details including:
    - Fund Manager Name (client name)
    - Fund Manager ID (client ID)
    - Status (active/inactive)
    - Created Date
    - Contact Details (name, email, phone)
    
    Args:
        fund_manager_id: The client ID of the fund manager
        edit_mode: If True, returns edit form format; if False, returns detail view format
        
    Returns:
        JSON response with fund manager details in the format expected by:
        - fund_manager_details.json (when edit_mode=False)
        - fund_manager_detail_edit_response.json (when edit_mode=True)
    """
    try:
        # Load template JSON based on edit_mode
        import json
        import os
        base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        
        if edit_mode:
            template_path = os.path.join(base_dir, 'frontendUtils', 'renders', 'fund_manager_detail_edit_response.json')
        else:
            template_path = os.path.join(base_dir, 'frontendUtils', 'renders', 'fund_manager_details.json')
        
        with open(template_path, 'r', encoding='utf-8') as f:
            template = json.load(f)
        
        # Use proper database manager pattern
        from database_models import get_database_manager, FundManager
        
        db_manager = get_database_manager()
        session = db_manager.get_session()
        
        try:
            # Query fund manager directly from fund_manager table
            fund_manager = session.query(FundManager).filter(
                FundManager.id == fund_manager_id
            ).first()
            
            if not fund_manager:
                raise HTTPException(status_code=404, detail=f"Fund Manager with ID {fund_manager_id} not found")
            
            # Format date as MM/DD/YYYY
            created_date = fund_manager.created_at.strftime("%m/%d/%Y") if fund_manager.created_at else "N/A"
            
            # Prepare contact name from fund manager fields
            contact_name_parts = []
            if fund_manager.contact_title:
                contact_name_parts.append(fund_manager.contact_title)
            if fund_manager.contact_first_name:
                contact_name_parts.append(fund_manager.contact_first_name)
            if fund_manager.contact_last_name:
                contact_name_parts.append(fund_manager.contact_last_name)
            contact_name = ' '.join(contact_name_parts) if contact_name_parts else "N/A"
            
            # Update template with actual data based on edit_mode
            if edit_mode:
                # Edit mode - update defaultValue fields and idToShow
                template["sections"][0]["idToShow"] = f"FDMG{fund_manager.id:04d}"  # Format as FDMG0001, FDMG0002, etc.
                
                # Update fund manager details section
                template["sections"][0]["fields"][0]["defaultValue"] = fund_manager.fund_manager_name
                
                # Update contact details section
                template["sections"][1]["fields"][0]["defaultValue"] = fund_manager.contact_title or "Mr."
                template["sections"][1]["fields"][1]["defaultValue"] = fund_manager.contact_first_name or ""
                template["sections"][1]["fields"][2]["defaultValue"] = fund_manager.contact_last_name or ""
                template["sections"][1]["fields"][3]["defaultValue"] = fund_manager.contact_email or ""
                template["sections"][1]["fields"][4]["defaultValue"] = fund_manager.contact_number or ""
                
                # Update confirmation dialog
                template["onConfirmation"]["description"] = f"Are you sure you want to update {fund_manager.fund_manager_name}?"
                
                # Update PUT API URL with fund manager ID
                template["onConfirmation"]["clickAction"]["putAPIURL"] = f"funds/managers/details?fund_manager_id={fund_manager.id}"
                
            else:
                # Detail mode - update value fields (existing logic)
                template["sections"][0]["fields"][0]["value"] = fund_manager.fund_manager_name  # Fund Manager Name
                template["sections"][0]["fields"][1]["value"] = str(fund_manager.id)  # Fund Manager ID
                template["sections"][0]["fields"][2]["value"] = "Active" if fund_manager.status == 'active' else "Inactive"  # Status
                template["sections"][0]["fields"][3]["value"] = created_date  # Created Date
                
                # Update contact details section with fund_manager table data
                template["sections"][1]["fields"][0]["value"] = contact_name  # Contact Name
                template["sections"][1]["fields"][1]["value"] = fund_manager.contact_email or "N/A"  # Email
                template["sections"][1]["fields"][2]["value"] = fund_manager.contact_number or "N/A"  # Contact number
                
                # Update footer dynamically based on current status
                footer_field = template["footer"]["fields"][0]
                
                if fund_manager.status == 'active':
                    # Currently active - show option to make inactive
                    footer_field["buttonText"] = "Mark as Inactive?"
                    footer_field["buttonColor"] = "destructive"
                    footer_field["onConfirmation"]["title"] = "Make Fund Manager Inactive?"
                    footer_field["onConfirmation"]["description"] = f"Are you sure you want to mark {fund_manager.fund_manager_name} as inactive?"
                    footer_field["onConfirmation"]["buttonText"] = "Mark as Inactive"
                    footer_field["onConfirmation"]["buttonColor"] = "destructive"
                else:
                    # Currently inactive - show option to make active
                    footer_field["buttonText"] = "Mark as Active?"
                    footer_field["buttonColor"] = None
                    footer_field["onConfirmation"]["title"] = "Make Fund Manager Active?"
                    footer_field["onConfirmation"]["description"] = f"Are you sure you want to mark {fund_manager.fund_manager_name} as active?"
                    footer_field["onConfirmation"]["buttonText"] = "Mark as Active"
                    footer_field["onConfirmation"]["buttonColor"] = None
                
                # Update onEditClick parameters
                template["onEditClick"]["parameters"][1]["value"] = str(fund_manager.id)
                
                # Update clickAction data with actual fund manager ID
                click_action = footer_field["onConfirmation"]["clickAction"]
                click_action["data"]["fundManagerId"] = fund_manager.id
                click_action["data"]["active"] = fund_manager.status != 'active'  # Toggle the status
            
            return template
            
        finally:
            session.close()
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching fund manager details: {e}")
        raise HTTPException(status_code=500, detail=f"Error fetching fund manager details: {str(e)}")


@app.patch("/funds/managers/status", tags=["Fund Manager API"], response_model=FundManagerStatusResponse)
async def toggle_fund_manager_status(
    request: FundManagerStatusRequest,
    *, __username: str = Depends(authenticate_user)
):
    """
    Activate or inactivate a fund manager
    
    Modifies fund manager status in the fund_manager table. Request body should contain:
    - fundManagerId: The fund manager ID (integer)
    - active: Boolean - true to activate, false to inactivate
    
    Args:
        request: JSON body with fundManagerId and active fields
        
    Returns:
        JSON response with success status and message
    """
    try:
        # Extract and validate request data
        fund_manager_id = request.fundManagerId
        active = request.active
        
        # Parse fund manager ID (expect integer)
        try:
            client_id = int(fund_manager_id)
        except (ValueError, TypeError):
            raise HTTPException(status_code=400, detail="Invalid fundManagerId format - must be an integer")
        
        # Use proper database manager pattern
        from database_models import get_database_manager, FundManager
        
        db_manager = get_database_manager()
        session = db_manager.get_session()
        
        try:
            # Find the fund manager directly
            fund_manager = session.query(FundManager).filter(
                FundManager.id == client_id
            ).first()
            
            if not fund_manager:
                raise HTTPException(status_code=404, detail=f"Fund Manager with ID {client_id} not found")
            
            # Update the fund manager's status
            fund_manager.status = 'active' if active else 'inactive'
            session.commit()
            
            # Prepare response message
            status_text = "activated" if active else "inactivated"
            
            return {
                "success": True,
                "message": f"Fund Manager '{fund_manager.fund_manager_name}' has been {status_text} successfully",
                "fundManagerId": client_id,
                "active": active
            }
            
        finally:
            session.close()
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error toggling fund manager status: {e}")
        raise HTTPException(status_code=500, detail=f"Error updating fund manager status: {str(e)}")


@app.put("/funds/managers/details", tags=["Fund Manager API"], response_model=EditFundManagerResponse)
async def edit_fund_manager(
    request: EditFundManagerRequest,
    fund_manager_id: int = Query(..., description="Fund Manager ID"),
    *, __username: str = Depends(authenticate_user)
):
    """
    Edit fund manager details
    
    Updates fund manager information in the fund_manager table.
    
    Args:
        fund_manager_id: The fund manager ID to edit
        request: JSON body with updated fund manager details (all fields optional)
        
    Request body fields (all optional):
        - fund_manager_name: Fund manager company name
        - title: Contact title (Mr., Mrs., Dr., etc.)
        - first_name: Contact first name
        - last_name: Contact last name
        - email: Contact email address
        - contact_number: Contact phone number
        - status: Fund manager status ('active' or 'inactive')
        
    Returns:
        JSON response with success status and message
    """
    try:
        from database_models import get_database_manager, FundManager
        import re
        
        db_manager = get_database_manager()
        session = db_manager.get_session()
        
        try:
            # Find the fund manager
            fund_manager = session.query(FundManager).filter(
                FundManager.id == fund_manager_id
            ).first()
            
            if not fund_manager:
                raise HTTPException(status_code=404, detail=f"Fund Manager with ID {fund_manager_id} not found")
            
            # Check if email is being updated and if it's already used by another fund manager
            if request.email is not None:
                existing_fund_manager = session.query(FundManager).filter(
                    FundManager.contact_email == request.email,
                    FundManager.id != fund_manager_id
                ).first()
                
                if existing_fund_manager:
                    raise HTTPException(status_code=400, detail=f"Email '{request.email}' is already in use by another fund manager")
            
            # Check if name is being updated and if it's already used by another fund manager
            if request.fund_manager_name is not None and fund_manager.fund_manager_name != request.fund_manager_name:
                existing_name = session.query(FundManager).filter(
                    FundManager.fund_manager_name == request.fund_manager_name,
                    FundManager.id != fund_manager_id
                ).first()
                
                if existing_name:
                    raise HTTPException(status_code=400, detail=f"Fund Manager name '{request.fund_manager_name}' is already in use")
            
            # Update fund manager details (only fields that are provided)
            if request.fund_manager_name is not None:
                fund_manager.fund_manager_name = request.fund_manager_name
            if request.title is not None:
                fund_manager.contact_title = request.title
            if request.first_name is not None:
                fund_manager.contact_first_name = request.first_name
            if request.last_name is not None:
                fund_manager.contact_last_name = request.last_name
            if request.email is not None:
                fund_manager.contact_email = request.email
            if request.contact_number is not None:
                fund_manager.contact_number = request.contact_number
            if request.status is not None:
                fund_manager.status = request.status
            
            session.commit()
            session.refresh(fund_manager)
            
            return EditFundManagerResponse(
                success=True,
                message=f"Fund Manager '{fund_manager.fund_manager_name}' updated successfully",
                client_id=fund_manager.id,
                fund_manager_id=fund_manager.id
            )
            
        finally:
            session.close()
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error editing fund manager: {e}")
        raise HTTPException(status_code=500, detail=f"Error editing fund manager: {str(e)}")


@app.post("/funds/managers", tags=["Fund Manager API"], response_model=AddFundManagerResponse)
async def add_fund_manager(
    request: AddFundManagerRequest,
    *, __username: str = Depends(authenticate_user)
):
    """
    Add a new fund manager
    
    Creates a new entry in the fund_manager table.
    
    Request body should contain:
    - fund_manager_name: Company name for the fund manager
    - title: Contact title (optional)
    - first_name: Contact first name
    - last_name: Contact last name
    - email: Contact email address
    - contact_number: Contact phone number (optional)
    
    Args:
        request: JSON body with fund manager details
        
    Returns:
        JSON response with success status and created fund manager ID
    """
    try:
        from database_models import get_database_manager, FundManager
        import re
        
        db_manager = get_database_manager()
        session = db_manager.get_session()
        
        try:
            # Check if fund manager with same name already exists
            # Check if fund manager with same name already exists
            existing_fund_manager = session.query(FundManager).filter(FundManager.fund_manager_name == request.fund_manager_name).first()
            if existing_fund_manager:
                return JSONResponse(
                    status_code=200,  # Conflict status code
                    content={"success": False, "message": "Fund Manager with this name already exists"}
                )

            # Check if fund manager with same email already exists
            existing_email_fund_manager = session.query(FundManager).filter(FundManager.contact_email == request.email).first()
            if existing_email_fund_manager:
                return JSONResponse(
                    status_code=200,  # Conflict status code
                    content={"success": False, "message": "Fund Manager with this email already exists"}
                )
            # Create new fund manager
            new_fund_manager = FundManager(
                fund_manager_name=request.fund_manager_name,
                contact_title=request.title,
                contact_first_name=request.first_name,
                contact_last_name=request.last_name,
                contact_email=request.email,
                contact_number=request.contact_number,
                status='active'
            )
            
            session.add(new_fund_manager)
            session.commit()
            session.refresh(new_fund_manager)
            
            return AddFundManagerResponse(
                success=True,
                message=f"Fund Manager '{request.fund_manager_name}' created successfully",
                client_id=new_fund_manager.id,
                user_id=0,  # No user created
                fund_manager_id=new_fund_manager.id
            )
            
        finally:
            session.close()
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error adding fund manager: {e}")
        raise HTTPException(status_code=500, detail=f"Error adding fund manager: {str(e)}")



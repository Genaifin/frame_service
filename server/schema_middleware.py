#!/usr/bin/env python3
"""
FastAPI Middleware for PostgreSQL Schema Management
"""

import os
import logging
from typing import Callable
from fastapi import Request, Response
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.types import ASGIApp
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

logger = logging.getLogger(__name__)

class SchemaMiddleware(BaseHTTPMiddleware):
    """Middleware to ensure proper PostgreSQL schema context"""
    
    def __init__(self, app: ASGIApp):
        super().__init__(app)
        self.default_schema = os.getenv('DB_SCHEMA', 'public')
    
    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        """Process request and ensure schema context"""
        # Add schema information to request state
        request.state.default_schema = self.default_schema
        
        # Log schema context for debugging
        logger.debug(f"Request {request.method} {request.url.path} using schema: {self.default_schema}")
        
        # Process the request
        response = await call_next(request)
        
        return response

def get_schema_context(request: Request) -> str:
    """Get the current schema context from request state"""
    return getattr(request.state, 'default_schema', os.getenv('DB_SCHEMA', 'public'))

def ensure_schema_in_request(request: Request, schema_name: str = None) -> str:
    """Ensure a specific schema is set for the request"""
    schema = schema_name or get_schema_context(request)
    request.state.current_schema = schema
    return schema

#!/usr/bin/env python3
"""
Main GraphQL Schema - Clean Implementation with Authentication
Combines all GraphQL operations with pure data responses
Maintains consistency with REST API authentication system
"""

import strawberry
from typing import List, Optional
from strawberry.types import Info
import logging

logger = logging.getLogger(__name__)

# Import authentication context
from .graphql_auth_context import (
    get_auth_context, 
    require_authentication, 
    require_role, 
    get_current_user, 
    is_authenticated
)

# Import all schema components
from .graphql_user_schema import UserQuery, UserMutation
from .graphql_client_schema import ClientQuery, ClientMutation
from .graphql_role_schema import RoleQuery, RoleMutation
from .graphql_fund_manager_schema import FundManagerQuery, FundManagerMutation
from .graphql_fund_schema import FundQuery, FundMutation
from .graphql_investor_schema import InvestorQuery, InvestorMutation
from .graphql_document_schema import DocumentQuery, DocumentMutation
from .graphql_document_configuration_schema import DocumentConfigurationQuery, DocumentConfigurationMutation
from .graphql_auth_mutations import AuthMutation
from .graphql_table_schema import TableSchemaQuery, TableSchemaMutation
from .graphql_validation_schema import ValidationQuery, ValidationMutation
from .graphql_report_ingestion_schema import ReportIngestionQuery
from .graphql_rules_schema import RuleQuery, RuleMutation

# Authentication types for GraphQL
@strawberry.type
class AuthUser:
    """Authenticated user information - consistent with REST API"""
    username: str
    display_name: str
    role: str
    is_active: bool

@strawberry.type
class AuthStatus:
    """Authentication status response"""
    is_authenticated: bool
    user: Optional[AuthUser] = None
    message: Optional[str] = None

# Create unified query class with authentication
@strawberry.type
class Query(UserQuery, ClientQuery, RoleQuery, FundManagerQuery, FundQuery, InvestorQuery, DocumentQuery, DocumentConfigurationQuery, TableSchemaQuery, ValidationQuery, ReportIngestionQuery, RuleQuery):
    """Unified GraphQL Query root with authentication"""
    
    @strawberry.field
    def auth_status(self, info: Info) -> AuthStatus:
        """Get current authentication status - same as REST /auth endpoint"""
        logger.info("GraphQL auth_status query called")
        try:
            if is_authenticated(info):
                logger.info("User is authenticated")
                user = get_current_user(info)
                if user:
                    logger.info(f"User data retrieved: {user.get('username')}")
                    return AuthStatus(
                        is_authenticated=True,
                        user=AuthUser(
                            username=user.get("username", ""),
                            display_name=user.get("displayName", ""),
                            role=user.get("role", ""),
                            is_active=user.get("is_active", True)
                        ),
                        message="Authentication successful"
                    )
            
            logger.info("User is not authenticated")
            return AuthStatus(
                is_authenticated=False,
                user=None,
                message="Not authenticated"
            )
        except Exception as e:
            logger.error(f"GraphQL auth_status error: {e}", exc_info=True)
            return AuthStatus(
                is_authenticated=False,
                user=None,
                message=f"Authentication error: {str(e)}"
            )
    
    @strawberry.field
    def me(self, info: Info) -> Optional[AuthUser]:
        """Get current user information - requires authentication"""
        try:
            user = require_authentication(info)
            return AuthUser(
                username=user.get("username", ""),
                display_name=user.get("displayName", ""),
                role=user.get("role", ""),
                is_active=user.get("is_active", True)
            )
        except Exception as e:
            raise Exception(f"Authentication required: {str(e)}")

# Create unified mutation class with authentication
@strawberry.type
class Mutation(UserMutation, ClientMutation, RoleMutation, FundManagerMutation, FundMutation, InvestorMutation, DocumentMutation, DocumentConfigurationMutation, AuthMutation, TableSchemaMutation, ValidationMutation, RuleMutation):
    """Unified GraphQL Mutation root with authentication"""
    pass

# Create the main schema
schema = strawberry.Schema(query=Query, mutation=Mutation)

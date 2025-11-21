"""
GraphQL Schema Package

This package contains all GraphQL schema definitions for the ValidusBoxes API.
Each module represents a different domain (users, clients, roles, fund managers).
"""

from .graphql_main_schema import schema

__all__ = ['schema']

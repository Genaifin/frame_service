#!/usr/bin/env python3
"""
PostgreSQL Schema Management Utilities
"""

import os
import logging
from typing import Optional, List
from sqlalchemy import text
from sqlalchemy.orm import Session
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

logger = logging.getLogger(__name__)

class SchemaManager:
    """Manages PostgreSQL schemas and ensures proper schema usage"""
    
    def __init__(self, session: Session):
        self.session = session
        self.default_schema = os.getenv('DB_SCHEMA', 'public')
    
    def ensure_schema_exists(self, schema_name: str) -> bool:
        """Ensure a schema exists, create if it doesn't"""
        try:
            # Check if schema exists
            result = self.session.execute(
                text("SELECT schema_name FROM information_schema.schemata WHERE schema_name = :schema"),
                {"schema": schema_name}
            ).fetchone()
            
            if not result:
                # Create schema
                self.session.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema_name}"))
                self.session.commit()
                logger.info(f"Created schema: {schema_name}")
                return True
            return True
        except Exception as e:
            logger.error(f"Failed to ensure schema {schema_name}: {e}")
            self.session.rollback()
            return False
    
    def set_search_path(self, schema_name: str = None) -> bool:
        """Set the search path for the current session"""
        try:
            schema = schema_name or self.default_schema
            self.session.execute(text(f"SET search_path TO {schema}, public"))
            logger.debug(f"Set search path to: {schema}, public")
            return True
        except Exception as e:
            logger.error(f"Failed to set search path: {e}")
            return False
    
    def get_current_schema(self) -> str:
        """Get the current schema from the session"""
        try:
            result = self.session.execute(text("SHOW search_path")).fetchone()
            if result:
                # Extract the first schema from search_path
                search_path = result[0]
                schemas = [s.strip() for s in search_path.split(',')]
                return schemas[0].strip('"') if schemas else self.default_schema
            return self.default_schema
        except Exception as e:
            logger.error(f"Failed to get current schema: {e}")
            return self.default_schema
    
    def list_schemas(self) -> List[str]:
        """List all available schemas"""
        try:
            result = self.session.execute(
                text("SELECT schema_name FROM information_schema.schemata WHERE schema_name NOT IN ('information_schema', 'pg_catalog', 'pg_toast')")
            ).fetchall()
            return [row[0] for row in result]
        except Exception as e:
            logger.error(f"Failed to list schemas: {e}")
            return []
    
    def validate_table_in_schema(self, table_name: str, schema_name: str = None) -> bool:
        """Validate that a table exists in the specified schema"""
        try:
            schema = schema_name or self.default_schema
            result = self.session.execute(
                text("""
                    SELECT table_name 
                    FROM information_schema.tables 
                    WHERE table_schema = :schema AND table_name = :table
                """),
                {"schema": schema, "table": table_name}
            ).fetchone()
            return result is not None
        except Exception as e:
            logger.error(f"Failed to validate table {table_name} in schema {schema_name}: {e}")
            return False
    
    def get_table_schema(self, table_name: str) -> Optional[str]:
        """Get the schema for a specific table"""
        try:
            result = self.session.execute(
                text("""
                    SELECT table_schema 
                    FROM information_schema.tables 
                    WHERE table_name = :table
                """),
                {"table": table_name}
            ).fetchone()
            return result[0] if result else None
        except Exception as e:
            logger.error(f"Failed to get schema for table {table_name}: {e}")
            return None

def ensure_schema_context(session: Session, schema_name: str = None) -> SchemaManager:
    """Helper function to ensure schema context is set"""
    schema_manager = SchemaManager(session)
    schema_manager.set_search_path(schema_name)
    return schema_manager

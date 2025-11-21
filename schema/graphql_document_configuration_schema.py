#!/usr/bin/env python3
"""
GraphQL Schema for Document Configuration Management
Provides GraphQL endpoints for document configuration operations with authentication
"""

import strawberry
from typing import List, Optional
from strawberry.types import Info
from sqlalchemy import desc
from database_models import DocumentConfiguration, get_database_manager
import logging

# Import authentication context
from .graphql_auth_context import require_authentication, get_current_user

logger = logging.getLogger(__name__)

@strawberry.type
class DocumentConfigurationType:
    """GraphQL type for DocumentConfiguration"""
    id: int = strawberry.field(name="id")
    name: str = strawberry.field(name="name")
    description: Optional[str] = strawberry.field(name="description", default=None)
    sla: Optional[int] = strawberry.field(name="sla", default=None)
    fields: Optional[strawberry.scalars.JSON] = strawberry.field(name="fields", default=None)

@strawberry.input
class DocumentConfigurationCreateInput:
    """Input type for creating a document configuration"""
    name: str = strawberry.field(name="name")
    description: Optional[str] = strawberry.field(name="description", default=None)
    sla: Optional[int] = strawberry.field(name="sla", default=None)
    fields: Optional[strawberry.scalars.JSON] = strawberry.field(name="fields", default=None)

@strawberry.input
class DocumentConfigurationUpdateInput:
    """Input type for updating a document configuration"""
    name: Optional[str] = strawberry.field(name="name", default=None)
    description: Optional[str] = strawberry.field(name="description", default=None)
    sla: Optional[int] = strawberry.field(name="sla", default=None)
    fields: Optional[strawberry.scalars.JSON] = strawberry.field(name="fields", default=None)

@strawberry.input
class DocumentConfigurationFilterInput:
    """Input type for filtering document configurations"""
    name: Optional[str] = strawberry.field(name="name", default=None)
    sla: Optional[int] = strawberry.field(name="sla", default=None)

@strawberry.type
class DocumentConfigurationResponse:
    """Response type for document configuration mutations"""
    success: bool = strawberry.field(name="success")
    message: str = strawberry.field(name="message")
    documentConfiguration: Optional[DocumentConfigurationType] = strawberry.field(name="documentConfiguration", default=None)

@strawberry.type
class DocumentConfigurationListResponse:
    """Response type for document configuration list queries"""
    success: bool = strawberry.field(name="success")
    message: str = strawberry.field(name="message")
    documentConfigurations: List[DocumentConfigurationType] = strawberry.field(name="documentConfigurations")
    total: int = strawberry.field(name="total")

@strawberry.type
class DocumentConfigurationQuery:
    """GraphQL queries for document configurations"""
    
    @strawberry.field
    def documentConfigurations(
        self,
        info: Info,
        filter: Optional[DocumentConfigurationFilterInput] = None,
        limit: Optional[int] = 100,
        offset: Optional[int] = 0
    ) -> DocumentConfigurationListResponse:
        """Get all document configurations with optional filtering - requires authentication"""
        try:
            # Require authentication
            user = require_authentication(info)
            logger.info(f"User {user.get('username')} querying document configurations")
            
            db_manager = get_database_manager()
            session = db_manager.get_session()
            
            try:
                # Build query
                query = session.query(DocumentConfiguration)
                
                # Apply filters if provided
                if filter:
                    if filter.name:
                        query = query.filter(DocumentConfiguration.name.ilike(f"%{filter.name}%"))
                    if filter.sla is not None:
                        query = query.filter(DocumentConfiguration.sla == filter.sla)
                
                # Get total count
                total = query.count()
                
                # Apply pagination and ordering
                configs = query.order_by(DocumentConfiguration.name).limit(limit).offset(offset).all()
                
                # Convert to GraphQL types
                config_list = []
                for config in configs:
                    config_list.append(DocumentConfigurationType(
                        id=config.id,
                        name=config.name,
                        description=config.description,
                        sla=config.sla,
                        fields=config.fields
                    ))
                
                return DocumentConfigurationListResponse(
                    success=True,
                    message=f"Retrieved {len(config_list)} document configurations",
                    documentConfigurations=config_list,
                    total=total
                )
                
            finally:
                session.close()
                
        except Exception as e:
            logger.error(f"Error querying document configurations: {e}", exc_info=True)
            return DocumentConfigurationListResponse(
                success=False,
                message=f"Error querying document configurations: {str(e)}",
                documentConfigurations=[],
                total=0
            )
    
    @strawberry.field
    def documentConfiguration(self, info: Info, id: int) -> DocumentConfigurationResponse:
        """Get a specific document configuration by ID - requires authentication"""
        try:
            # Require authentication
            user = require_authentication(info)
            logger.info(f"User {user.get('username')} querying document configuration {id}")
            
            db_manager = get_database_manager()
            session = db_manager.get_session()
            
            try:
                config = session.query(DocumentConfiguration).filter(
                    DocumentConfiguration.id == id
                ).first()
                
                if not config:
                    return DocumentConfigurationResponse(
                        success=False,
                        message=f"Document configuration with ID {id} not found",
                        documentConfiguration=None
                    )
                
                config_type = DocumentConfigurationType(
                    id=config.id,
                    name=config.name,
                    description=config.description,
                    sla=config.sla,
                    fields=config.fields
                )
                
                return DocumentConfigurationResponse(
                    success=True,
                    message="Document configuration retrieved successfully",
                    documentConfiguration=config_type
                )
                
            finally:
                session.close()
                
        except Exception as e:
            logger.error(f"Error querying document configuration: {e}", exc_info=True)
            return DocumentConfigurationResponse(
                success=False,
                message=f"Error querying document configuration: {str(e)}",
                documentConfiguration=None
            )
    
    @strawberry.field
    def documentConfigurationByName(
        self,
        info: Info,
        name: str
    ) -> DocumentConfigurationResponse:
        """Get a document configuration by name - requires authentication"""
        try:
            # Require authentication
            user = require_authentication(info)
            logger.info(f"User {user.get('username')} querying document configuration by name: {name}")
            
            db_manager = get_database_manager()
            session = db_manager.get_session()
            
            try:
                config = session.query(DocumentConfiguration).filter(
                    DocumentConfiguration.name == name
                ).first()
                
                if not config:
                    return DocumentConfigurationResponse(
                        success=False,
                        message=f"Document configuration with name '{name}' not found",
                        documentConfiguration=None
                    )
                
                config_type = DocumentConfigurationType(
                    id=config.id,
                    name=config.name,
                    description=config.description,
                    sla=config.sla,
                    fields=config.fields
                )
                
                return DocumentConfigurationResponse(
                    success=True,
                    message="Document configuration retrieved successfully",
                    documentConfiguration=config_type
                )
                
            finally:
                session.close()
                
        except Exception as e:
            logger.error(f"Error querying document configuration by name: {e}", exc_info=True)
            return DocumentConfigurationResponse(
                success=False,
                message=f"Error querying document configuration: {str(e)}",
                documentConfiguration=None
            )

@strawberry.type
class DocumentConfigurationMutation:
    """GraphQL mutations for document configurations"""
    
    @strawberry.mutation
    def createDocumentConfiguration(
        self,
        info: Info,
        input: DocumentConfigurationCreateInput
    ) -> DocumentConfigurationResponse:
        """Create a new document configuration - requires authentication"""
        try:
            # Require authentication
            user = require_authentication(info)
            logger.info(f"User {user.get('username')} creating document configuration: {input.name}")
            
            db_manager = get_database_manager()
            session = db_manager.get_session()
            
            try:
                # Check if configuration with same name already exists
                existing = session.query(DocumentConfiguration).filter(
                    DocumentConfiguration.name == input.name
                ).first()
                
                if existing:
                    return DocumentConfigurationResponse(
                        success=False,
                        message=f"Document configuration with name '{input.name}' already exists",
                        documentConfiguration=None
                    )
                
                # Create new document configuration
                new_config = DocumentConfiguration(
                    name=input.name,
                    description=input.description,
                    sla=input.sla,
                    fields=input.fields
                )
                
                session.add(new_config)
                session.commit()
                session.refresh(new_config)
                
                config_type = DocumentConfigurationType(
                    id=new_config.id,
                    name=new_config.name,
                    description=new_config.description,
                    sla=new_config.sla,
                    fields=new_config.fields
                )
                
                return DocumentConfigurationResponse(
                    success=True,
                    message=f"Document configuration '{input.name}' created successfully",
                    documentConfiguration=config_type
                )
                
            except Exception as e:
                session.rollback()
                logger.error(f"Database error creating document configuration: {e}", exc_info=True)
                return DocumentConfigurationResponse(
                    success=False,
                    message=f"Database error: {str(e)}",
                    documentConfiguration=None
                )
            finally:
                session.close()
                
        except Exception as e:
            logger.error(f"Error creating document configuration: {e}", exc_info=True)
            return DocumentConfigurationResponse(
                success=False,
                message=f"Error creating document configuration: {str(e)}",
                documentConfiguration=None
            )
    
    @strawberry.mutation
    def updateDocumentConfiguration(
        self,
        info: Info,
        id: int,
        input: DocumentConfigurationUpdateInput
    ) -> DocumentConfigurationResponse:
        """Update an existing document configuration - requires authentication"""
        try:
            # Require authentication
            user = require_authentication(info)
            logger.info(f"User {user.get('username')} updating document configuration {id}")
            
            db_manager = get_database_manager()
            session = db_manager.get_session()
            
            try:
                config = session.query(DocumentConfiguration).filter(
                    DocumentConfiguration.id == id
                ).first()
                
                if not config:
                    return DocumentConfigurationResponse(
                        success=False,
                        message=f"Document configuration with ID {id} not found",
                        documentConfiguration=None
                    )
                
                # Check if name is being changed and if new name already exists
                if input.name is not None and input.name != config.name:
                    existing = session.query(DocumentConfiguration).filter(
                        DocumentConfiguration.name == input.name,
                        DocumentConfiguration.id != id
                    ).first()
                    
                    if existing:
                        return DocumentConfigurationResponse(
                            success=False,
                            message=f"Document configuration with name '{input.name}' already exists",
                            documentConfiguration=None
                        )
                
                # Update fields
                if input.name is not None:
                    config.name = input.name
                if input.description is not None:
                    config.description = input.description
                if input.sla is not None:
                    config.sla = input.sla
                if input.fields is not None:
                    config.fields = input.fields
                
                session.commit()
                session.refresh(config)
                
                config_type = DocumentConfigurationType(
                    id=config.id,
                    name=config.name,
                    description=config.description,
                    sla=config.sla,
                    fields=config.fields
                )
                
                return DocumentConfigurationResponse(
                    success=True,
                    message=f"Document configuration {id} updated successfully",
                    documentConfiguration=config_type
                )
                
            except Exception as e:
                session.rollback()
                logger.error(f"Database error updating document configuration: {e}", exc_info=True)
                return DocumentConfigurationResponse(
                    success=False,
                    message=f"Database error: {str(e)}",
                    documentConfiguration=None
                )
            finally:
                session.close()
                
        except Exception as e:
            logger.error(f"Error updating document configuration: {e}", exc_info=True)
            return DocumentConfigurationResponse(
                success=False,
                message=f"Error updating document configuration: {str(e)}",
                documentConfiguration=None
            )
    
    @strawberry.mutation
    def deleteDocumentConfiguration(
        self,
        info: Info,
        id: int
    ) -> DocumentConfigurationResponse:
        """Delete a document configuration - requires authentication"""
        try:
            # Require authentication
            user = require_authentication(info)
            logger.info(f"User {user.get('username')} deleting document configuration {id}")
            
            db_manager = get_database_manager()
            session = db_manager.get_session()
            
            try:
                config = session.query(DocumentConfiguration).filter(
                    DocumentConfiguration.id == id
                ).first()
                
                if not config:
                    return DocumentConfigurationResponse(
                        success=False,
                        message=f"Document configuration with ID {id} not found",
                        documentConfiguration=None
                    )
                
                # Delete the configuration
                session.delete(config)
                session.commit()
                
                return DocumentConfigurationResponse(
                    success=True,
                    message=f"Document configuration {id} deleted successfully",
                    documentConfiguration=None
                )
                
            except Exception as e:
                session.rollback()
                logger.error(f"Database error deleting document configuration: {e}", exc_info=True)
                return DocumentConfigurationResponse(
                    success=False,
                    message=f"Database error: {str(e)}",
                    documentConfiguration=None
                )
            finally:
                session.close()
                
        except Exception as e:
            logger.error(f"Error deleting document configuration: {e}", exc_info=True)
            return DocumentConfigurationResponse(
                success=False,
                message=f"Error deleting document configuration: {str(e)}",
                documentConfiguration=None
            )


#!/usr/bin/env python3
"""
GraphQL Schema for Document Management
Provides GraphQL endpoints for document operations with authentication
"""

import strawberry
from typing import List, Optional
from strawberry.types import Info
from sqlalchemy.orm import joinedload
from sqlalchemy import or_, desc
from database_models import Document, Fund, get_database_manager
from datetime import datetime
import logging

# Import authentication context
from .graphql_auth_context import require_authentication, require_role, get_current_user, is_authenticated

logger = logging.getLogger(__name__)

@strawberry.type
class DocumentType:
    """GraphQL type for Document"""
    id: int = strawberry.field(name="id")
    doc_id: Optional[str] = strawberry.field(name="docId", default=None)
    name: str = strawberry.field(name="name")
    type: Optional[str] = strawberry.field(name="type", default=None)
    path: str = strawberry.field(name="path")
    size: Optional[int] = strawberry.field(name="size", default=None)
    status: str = strawberry.field(name="status")
    fund_id: Optional[int] = strawberry.field(name="fundId", default=None)
    account_id: Optional[int] = strawberry.field(name="accountId", default=None)
    client_id: Optional[int] = strawberry.field(name="clientId", default=None)
    upload_date: str = strawberry.field(name="uploadDate")
    replay: bool = strawberry.field(name="replay")
    created_by: Optional[str] = strawberry.field(name="createdBy", default=None)
    metadata: Optional[strawberry.scalars.JSON] = strawberry.field(name="metadata", default=None)
    is_active: bool = strawberry.field(name="isActive")
    created_at: str = strawberry.field(name="createdAt")
    updated_at: str = strawberry.field(name="updatedAt")

@strawberry.type
class FundType:
    """GraphQL type for Fund (simplified for document context)"""
    id: int
    name: str
    code: str
    description: Optional[str] = None
    is_active: bool

@strawberry.input
class DocumentCreateInput:
    """Input type for creating a document"""
    name: str = strawberry.field(name="name")
    type: Optional[str] = strawberry.field(name="type", default=None)
    path: str = strawberry.field(name="path")
    size: Optional[int] = strawberry.field(name="size", default=None)
    status: str = strawberry.field(name="status", default="pending")
    fund_id: Optional[int] = strawberry.field(name="fundId", default=None)
    account_id: Optional[int] = strawberry.field(name="accountId", default=None)
    client_id: Optional[int] = strawberry.field(name="clientId", default=None)
    replay: bool = strawberry.field(name="replay", default=False)
    created_by: Optional[str] = strawberry.field(name="createdBy", default=None)
    metadata: Optional[strawberry.scalars.JSON] = strawberry.field(name="metadata", default=None)
    doc_id: Optional[str] = strawberry.field(name="docId", default=None)

@strawberry.input
class DocumentUpdateInput:
    """Input type for updating a document"""
    name: Optional[str] = strawberry.field(name="name", default=None)
    type: Optional[str] = strawberry.field(name="type", default=None)
    path: Optional[str] = strawberry.field(name="path", default=None)
    size: Optional[int] = strawberry.field(name="size", default=None)
    status: Optional[str] = strawberry.field(name="status", default=None)
    fund_id: Optional[int] = strawberry.field(name="fundId", default=None)
    account_id: Optional[int] = strawberry.field(name="accountId", default=None)
    client_id: Optional[int] = strawberry.field(name="clientId", default=None)
    replay: Optional[bool] = strawberry.field(name="replay", default=None)
    created_by: Optional[str] = strawberry.field(name="createdBy", default=None)
    metadata: Optional[strawberry.scalars.JSON] = strawberry.field(name="metadata", default=None)
    is_active: Optional[bool] = strawberry.field(name="isActive", default=None)
    doc_id: Optional[str] = strawberry.field(name="docId", default=None)

@strawberry.input
class DocumentFilterInput:
    """Input type for filtering documents"""
    name: Optional[str] = strawberry.field(name="name", default=None)
    type: Optional[str] = strawberry.field(name="type", default=None)
    status: Optional[str] = strawberry.field(name="status", default=None)
    fund_id: Optional[int] = strawberry.field(name="fundId", default=None)
    account_id: Optional[int] = strawberry.field(name="accountId", default=None)
    client_id: Optional[int] = strawberry.field(name="clientId", default=None)
    replay: Optional[bool] = strawberry.field(name="replay", default=None)
    created_by: Optional[str] = strawberry.field(name="createdBy", default=None)
    is_active: Optional[bool] = strawberry.field(name="isActive", default=None)
    doc_id: Optional[str] = strawberry.field(name="docId", default=None)

@strawberry.type
class DocumentResponse:
    """Response type for document mutations"""
    success: bool = strawberry.field(name="success")
    message: str = strawberry.field(name="message")
    document: Optional[DocumentType] = strawberry.field(name="document", default=None)

@strawberry.type
class DocumentListResponse:
    """Response type for document list queries"""
    success: bool = strawberry.field(name="success")
    message: str = strawberry.field(name="message")
    documents: List[DocumentType] = strawberry.field(name="documents")
    total: int = strawberry.field(name="total")

@strawberry.type
class DocumentQuery:
    """GraphQL queries for documents"""
    
    @strawberry.field
    def documents(
        self,
        info: Info,
        filter: Optional[DocumentFilterInput] = None,
        limit: Optional[int] = 100,
        offset: Optional[int] = 0
    ) -> DocumentListResponse:
        """Get all documents with optional filtering - requires authentication"""
        try:
            # Require authentication
            user = require_authentication(info)
            logger.info(f"User {user.get('username')} querying documents")
            
            db_manager = get_database_manager()
            session = db_manager.get_session()
            
            try:
                # Build query
                query = session.query(Document)
                
                # Apply filters if provided
                if filter:
                    if filter.name:
                        query = query.filter(Document.name.ilike(f"%{filter.name}%"))
                    if filter.type:
                        query = query.filter(Document.type == filter.type)
                    if filter.status:
                        query = query.filter(Document.status == filter.status)
                    if filter.fund_id is not None:
                        query = query.filter(Document.fund_id == filter.fund_id)
                    if filter.account_id is not None:
                        query = query.filter(Document.account_id == filter.account_id)
                    if filter.client_id is not None:
                        query = query.filter(Document.client_id == filter.client_id)
                    if filter.replay is not None:
                        query = query.filter(Document.replay == filter.replay)
                    if filter.created_by:
                        query = query.filter(Document.created_by == filter.created_by)
                    if filter.is_active is not None:
                        query = query.filter(Document.is_active == filter.is_active)
                    else:
                        query = query.filter(Document.is_active == True)
                else:
                    query = query.filter(Document.is_active == True)
                
                # Get total count
                total = query.count()
                
                # Apply pagination and ordering
                documents = query.order_by(desc(Document.created_at)).limit(limit).offset(offset).all()
                
                # Convert to GraphQL types
                document_list = []
                for doc in documents:
                    document_list.append(DocumentType(
                        id=doc.id,
                        doc_id=str(doc.doc_id) if doc.doc_id else None,
                        name=doc.name,
                        type=doc.type,
                        path=doc.path,
                        size=doc.size,
                        status=doc.status,
                        fund_id=doc.fund_id,
                        account_id=doc.account_id,
                        client_id=doc.client_id,
                        upload_date=doc.upload_date.isoformat() if doc.upload_date else "",
                        replay=doc.replay,
                        created_by=doc.created_by,
                        metadata=doc.document_metadata,
                        is_active=doc.is_active,
                        created_at=doc.created_at.isoformat() if doc.created_at else "",
                        updated_at=doc.updated_at.isoformat() if doc.updated_at else ""
                    ))
                
                return DocumentListResponse(
                    success=True,
                    message=f"Retrieved {len(document_list)} documents",
                    documents=document_list,
                    total=total
                )
                
            finally:
                session.close()
                
        except Exception as e:
            logger.error(f"Error querying documents: {e}", exc_info=True)
            return DocumentListResponse(
                success=False,
                message=f"Error querying documents: {str(e)}",
                documents=[],
                total=0
            )
    
    @strawberry.field
    def document(self, info: Info, document_id: int) -> DocumentResponse:
        """Get a specific document by ID - requires authentication"""
        try:
            # Require authentication
            user = require_authentication(info)
            logger.info(f"User {user.get('username')} querying document {document_id}")
            
            db_manager = get_database_manager()
            session = db_manager.get_session()
            
            try:
                document = session.query(Document).filter(
                    Document.id == document_id,
                    Document.is_active == True
                ).first()
                
                if not document:
                    return DocumentResponse(
                        success=False,
                        message=f"Document with ID {document_id} not found",
                        document=None
                    )
                
                document_type = DocumentType(
                    id=document.id,
                    doc_id=str(document.doc_id) if document.doc_id else None,
                    name=document.name,
                    type=document.type,
                    path=document.path,
                    size=document.size,
                    status=document.status,
                    fund_id=document.fund_id,
                    account_id=document.account_id,
                    client_id=document.client_id,
                    upload_date=document.upload_date.isoformat() if document.upload_date else "",
                    replay=document.replay,
                    created_by=document.created_by,
                    metadata=document.document_metadata,
                    is_active=document.is_active,
                    created_at=document.created_at.isoformat() if document.created_at else "",
                    updated_at=document.updated_at.isoformat() if document.updated_at else ""
                )
                
                return DocumentResponse(
                    success=True,
                    message="Document retrieved successfully",
                    document=document_type
                )
                
            finally:
                session.close()
                
        except Exception as e:
            logger.error(f"Error querying document: {e}", exc_info=True)
            return DocumentResponse(
                success=False,
                message=f"Error querying document: {str(e)}",
                document=None
            )
    
    @strawberry.field
    def documents_by_fund(
        self,
        info: Info,
        fund_id: int,
        limit: Optional[int] = 100,
        offset: Optional[int] = 0
    ) -> DocumentListResponse:
        """Get all documents for a specific fund - requires authentication"""
        try:
            # Require authentication
            user = require_authentication(info)
            logger.info(f"User {user.get('username')} querying documents for fund {fund_id}")
            
            db_manager = get_database_manager()
            session = db_manager.get_session()
            
            try:
                # Build query
                query = session.query(Document).filter(
                    Document.fund_id == fund_id,
                    Document.is_active == True
                )
                
                # Get total count
                total = query.count()
                
                # Apply pagination and ordering
                documents = query.order_by(desc(Document.created_at)).limit(limit).offset(offset).all()
                
                # Convert to GraphQL types
                document_list = []
                for doc in documents:
                    document_list.append(DocumentType(
                        id=doc.id,
                        doc_id=str(doc.doc_id) if doc.doc_id else None,
                        name=doc.name,
                        type=doc.type,
                        path=doc.path,
                        size=doc.size,
                        status=doc.status,
                        fund_id=doc.fund_id,
                        account_id=doc.account_id,
                        client_id=doc.client_id,
                        upload_date=doc.upload_date.isoformat() if doc.upload_date else "",
                        replay=doc.replay,
                        created_by=doc.created_by,
                        metadata=doc.document_metadata,
                        is_active=doc.is_active,
                        created_at=doc.created_at.isoformat() if doc.created_at else "",
                        updated_at=doc.updated_at.isoformat() if doc.updated_at else ""
                    ))
                
                return DocumentListResponse(
                    success=True,
                    message=f"Retrieved {len(document_list)} documents for fund {fund_id}",
                    documents=document_list,
                    total=total
                )
                
            finally:
                session.close()
                
        except Exception as e:
            logger.error(f"Error querying documents by fund: {e}", exc_info=True)
            return DocumentListResponse(
                success=False,
                message=f"Error querying documents by fund: {str(e)}",
                documents=[],
                total=0
            )

@strawberry.type
class DocumentMutation:
    """GraphQL mutations for documents"""
    
    @strawberry.mutation
    def create_document(
        self,
        info: Info,
        input: DocumentCreateInput
    ) -> DocumentResponse:
        """Create a new document - requires authentication"""
        try:
            # Require authentication
            user = require_authentication(info)
            logger.info(f"User {user.get('username')} creating document: {input.name}")
            
            db_manager = get_database_manager()
            session = db_manager.get_session()
            
            try:
                # Set created_by if not provided
                if not input.created_by:
                    input.created_by = user.get('username')
                
                # Create new document
                new_document = Document(
                    name=input.name,
                    type=input.type,
                    path=input.path,
                    size=input.size,
                    status=input.status,
                    fund_id=input.fund_id,
                    account_id=input.account_id,
                    client_id=input.client_id,
                    replay=input.replay,
                    created_by=input.created_by,
                    document_metadata=input.metadata,
                    doc_id=input.doc_id
                )
                
                session.add(new_document)
                session.commit()
                session.refresh(new_document)
                
                document_type = DocumentType(
                    id=new_document.id,
                    doc_id=str(new_document.doc_id) if new_document.doc_id else None,
                    name=new_document.name,
                    type=new_document.type,
                    path=new_document.path,
                    size=new_document.size,
                    status=new_document.status,
                    fund_id=new_document.fund_id,
                    account_id=new_document.account_id,
                    client_id=new_document.client_id,
                    upload_date=new_document.upload_date.isoformat() if new_document.upload_date else "",
                    replay=new_document.replay,
                    created_by=new_document.created_by,
                    metadata=new_document.document_metadata,
                    is_active=new_document.is_active,
                    created_at=new_document.created_at.isoformat() if new_document.created_at else "",
                    updated_at=new_document.updated_at.isoformat() if new_document.updated_at else ""
                )
                
                return DocumentResponse(
                    success=True,
                    message=f"Document '{input.name}' created successfully",
                    document=document_type
                )
                
            except Exception as e:
                session.rollback()
                logger.error(f"Database error creating document: {e}", exc_info=True)
                return DocumentResponse(
                    success=False,
                    message=f"Database error: {str(e)}",
                    document=None
                )
            finally:
                session.close()
                
        except Exception as e:
            logger.error(f"Error creating document: {e}", exc_info=True)
            return DocumentResponse(
                success=False,
                message=f"Error creating document: {str(e)}",
                document=None
            )
    
    @strawberry.mutation
    def update_document(
        self,
        info: Info,
        document_id: int,
        input: DocumentUpdateInput
    ) -> DocumentResponse:
        """Update an existing document - requires authentication"""
        try:
            # Require authentication
            user = require_authentication(info)
            logger.info(f"User {user.get('username')} updating document {document_id}")
            
            db_manager = get_database_manager()
            session = db_manager.get_session()
            
            try:
                document = session.query(Document).filter(
                    Document.id == document_id,
                    Document.is_active == True
                ).first()
                
                if not document:
                    return DocumentResponse(
                        success=False,
                        message=f"Document with ID {document_id} not found",
                        document=None
                    )
                
                # Update fields
                if input.name is not None:
                    document.name = input.name
                if input.type is not None:
                    document.type = input.type
                if input.path is not None:
                    document.path = input.path
                if input.size is not None:
                    document.size = input.size
                if input.status is not None:
                    document.status = input.status
                if input.fund_id is not None:
                    document.fund_id = input.fund_id
                if input.account_id is not None:
                    document.account_id = input.account_id
                if input.client_id is not None:
                    document.client_id = input.client_id
                if input.replay is not None:
                    document.replay = input.replay
                if input.created_by is not None:
                    document.created_by = input.created_by
                if input.metadata is not None:
                    document.document_metadata = input.metadata
                if input.is_active is not None:
                    document.is_active = input.is_active
                if input.doc_id is not None:
                    document.doc_id = input.doc_id
                
                session.commit()
                session.refresh(document)
                
                document_type = DocumentType(
                    id=document.id,
                    doc_id=str(document.doc_id) if document.doc_id else None,
                    name=document.name,
                    type=document.type,
                    path=document.path,
                    size=document.size,
                    status=document.status,
                    fund_id=document.fund_id,
                    account_id=document.account_id,
                    client_id=document.client_id,
                    upload_date=document.upload_date.isoformat() if document.upload_date else "",
                    replay=document.replay,
                    created_by=document.created_by,
                    metadata=document.document_metadata,
                    is_active=document.is_active,
                    created_at=document.created_at.isoformat() if document.created_at else "",
                    updated_at=document.updated_at.isoformat() if document.updated_at else ""
                )
                
                return DocumentResponse(
                    success=True,
                    message=f"Document {document_id} updated successfully",
                    document=document_type
                )
                
            except Exception as e:
                session.rollback()
                logger.error(f"Database error updating document: {e}", exc_info=True)
                return DocumentResponse(
                    success=False,
                    message=f"Database error: {str(e)}",
                    document=None
                )
            finally:
                session.close()
                
        except Exception as e:
            logger.error(f"Error updating document: {e}", exc_info=True)
            return DocumentResponse(
                success=False,
                message=f"Error updating document: {str(e)}",
                document=None
            )
    
    @strawberry.mutation
    def delete_document(
        self,
        info: Info,
        document_id: int
    ) -> DocumentResponse:
        """Soft delete a document (set is_active to False) - requires authentication"""
        try:
            # Require authentication
            user = require_authentication(info)
            logger.info(f"User {user.get('username')} deleting document {document_id}")
            
            db_manager = get_database_manager()
            session = db_manager.get_session()
            
            try:
                document = session.query(Document).filter(
                    Document.id == document_id,
                    Document.is_active == True
                ).first()
                
                if not document:
                    return DocumentResponse(
                        success=False,
                        message=f"Document with ID {document_id} not found",
                        document=None
                    )
                
                # Soft delete
                document.is_active = False
                session.commit()
                
                return DocumentResponse(
                    success=True,
                    message=f"Document {document_id} deleted successfully",
                    document=None
                )
                
            except Exception as e:
                session.rollback()
                logger.error(f"Database error deleting document: {e}", exc_info=True)
                return DocumentResponse(
                    success=False,
                    message=f"Database error: {str(e)}",
                    document=None
                )
            finally:
                session.close()
                
        except Exception as e:
            logger.error(f"Error deleting document: {e}", exc_info=True)
            return DocumentResponse(
                success=False,
                message=f"Error deleting document: {str(e)}",
                document=None
            )


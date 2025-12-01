#!/usr/bin/env python3
"""
GraphQL Schema for Document Management
Provides GraphQL endpoints for document operations with authentication
"""

import strawberry
from typing import List, Optional
from strawberry.types import Info
from sqlalchemy.orm import joinedload
from sqlalchemy.orm.attributes import flag_modified
from sqlalchemy import or_, desc
from database_models import Document, Fund, Investor, FundInvestor, Client, get_database_manager, CapitalCallsExtraction, DistributionsExtraction, StatementsExtraction
from datetime import datetime
import logging

# Import authentication context
from .graphql_auth_context import require_authentication, require_role, get_current_user, is_authenticated

logger = logging.getLogger(__name__)

@strawberry.type
class AssignedAccountType:
    """GraphQL type for assigned account information"""
    status: Optional[str] = strawberry.field(name="status", default=None)
    account_id: Optional[str] = strawberry.field(name="accountId", default=None)
    account_name: Optional[str] = strawberry.field(name="accountName", default=None)
    investor_name: Optional[str] = strawberry.field(name="investorName", default=None)
    fund: Optional[str] = strawberry.field(name="fund", default=None)
    client_id: Optional[int] = strawberry.field(name="clientId", default=None)
    client_name: Optional[str] = strawberry.field(name="clientName", default=None)

def _getAssignedAccountsFromMetadata(metadata):
    """Helper function to extract assigned accounts from document metadata"""
    if not metadata or not isinstance(metadata, dict):
        return []
    
    assigned_accounts = metadata.get('assigned_accounts', [])
    if not isinstance(assigned_accounts, list):
        return []
    
    result = []
    for account in assigned_accounts:
        if isinstance(account, dict):
            result.append(AssignedAccountType(
                status=account.get('status'),
                account_id=account.get('account_id'),
                account_name=account.get('account_name'),
                investor_name=account.get('investor_name'),
                fund=account.get('fund'),
                client_id=account.get('client_id'),
                client_name=account.get('client_name')
            ))
    return result

def _getExtractedData(session, document_id: int, document_type: Optional[str]):
    """Helper function to get extracted data from appropriate extraction table"""
    if not document_type:
        logger.debug(f"No document type provided for document {document_id}")
        return None
    
    extracted_data = []
    
    try:
        # Normalize document type for comparison
        doc_type_normalized = document_type.lower().replace(' ', '').replace('_', '')
        logger.debug(f"Fetching extracted data for document {document_id}, type: {document_type} (normalized: {doc_type_normalized})")
        
        # Map document type to extraction table
        if doc_type_normalized in ['capitalcall', 'capitalcalls', 'capcall', 'capcalls']:
            logger.debug(f"Querying CapitalCallsExtraction for document {document_id}")
            extractions = session.query(CapitalCallsExtraction).filter(
                CapitalCallsExtraction.doc_id == document_id
            ).all()
            logger.debug(f"Found {len(extractions)} capital call extraction records")
            
            for extraction in extractions:
                data = {
                    'id': extraction.id,
                    'Investor': extraction.Investor,
                    'Account': extraction.Account,
                    'InvestorRefID': extraction.InvestorRefID,
                    'AccountRefID': extraction.AccountRefID,
                    'Security': extraction.Security,
                    'TransactionDate': extraction.TransactionDate.isoformat() if extraction.TransactionDate else None,
                    'Currency': extraction.Currency,
                    'Distribution': float(extraction.Distribution) if extraction.Distribution else None,
                    'DeemedCapitalCall': float(extraction.DeemedCapitalCall) if extraction.DeemedCapitalCall else None,
                    'IncomeDistribution': float(extraction.IncomeDistribution) if extraction.IncomeDistribution else None,
                    'IncomeReinvested': float(extraction.IncomeReinvested) if extraction.IncomeReinvested else None,
                    'RecallableSell': float(extraction.RecallableSell) if extraction.RecallableSell else None,
                    'ReturnOfCapital': float(extraction.ReturnOfCapital) if extraction.ReturnOfCapital else None,
                    'DistributionOutsideCommitment': float(extraction.DistributionOutsideCommitment) if extraction.DistributionOutsideCommitment else None,
                    'CapitalCall': float(extraction.CapitalCall) if extraction.CapitalCall else None,
                    'CapitalCallOutsideCommitment': float(extraction.CapitalCallOutsideCommitment) if extraction.CapitalCallOutsideCommitment else None,
                    'NetCashFlowQC': float(extraction.NetCashFlowQC) if extraction.NetCashFlowQC else None,
                    'TransferIn': float(extraction.TransferIn) if extraction.TransferIn else None,
                    'TransferOut': float(extraction.TransferOut) if extraction.TransferOut else None,
                    'Quantity': float(extraction.Quantity) if extraction.Quantity else None,
                    'Price': float(extraction.Price) if extraction.Price else None,
                    'CommittedCapital': float(extraction.CommittedCapital) if extraction.CommittedCapital else None,
                    'RemainingCommittedCapital': float(extraction.RemainingCommittedCapital) if extraction.RemainingCommittedCapital else None,
                    'ContributionsToDate': float(extraction.ContributionsToDate) if extraction.ContributionsToDate else None,
                    'DistributionsToDate': float(extraction.DistributionsToDate) if extraction.DistributionsToDate else None,
                    'ReturnOfCapitalToDate': float(extraction.ReturnOfCapitalToDate) if extraction.ReturnOfCapitalToDate else None,
                    'DeemedGPContribution': float(extraction.DeemedGPContribution) if extraction.DeemedGPContribution else None,
                    'Investments': float(extraction.Investments) if extraction.Investments else None,
                }
                extracted_data.append(data)
        
        elif doc_type_normalized in ['distribution', 'distributions', 'distributionnotice', 'distributionnotices']:
            logger.debug(f"Querying DistributionsExtraction for document {document_id}")
            extractions = session.query(DistributionsExtraction).filter(
                DistributionsExtraction.doc_id == document_id
            ).all()
            logger.debug(f"Found {len(extractions)} distribution extraction records")
            
            for extraction in extractions:
                data = {
                    'id': extraction.id,
                    'Investor': extraction.Investor,
                    'Account': extraction.Account,
                    'InvestorRefID': extraction.InvestorRefID,
                    'AccountRefID': extraction.AccountRefID,
                    'Security': extraction.Security,
                    'TransactionDate': extraction.TransactionDate.isoformat() if extraction.TransactionDate else None,
                    'Currency': extraction.Currency,
                    'Distribution': float(extraction.Distribution) if extraction.Distribution else None,
                    'DeemeedCapitalCall': float(extraction.DeemeedCapitalCall) if extraction.DeemeedCapitalCall else None,
                    'IncomeDistribution': float(extraction.IncomeDistribution) if extraction.IncomeDistribution else None,
                    'IncomeReinvested': float(extraction.IncomeReinvested) if extraction.IncomeReinvested else None,
                    'RecallableSell': float(extraction.RecallableSell) if extraction.RecallableSell else None,
                    'ReturnOfCapital': float(extraction.ReturnOfCapital) if extraction.ReturnOfCapital else None,
                    'DistributionOutsideCommitment': float(extraction.DistributionOutsideCommitment) if extraction.DistributionOutsideCommitment else None,
                    'CapitalCall': float(extraction.CapitalCall) if extraction.CapitalCall else None,
                    'CapitalCallOutsideCommitment': float(extraction.CapitalCallOutsideCommitment) if extraction.CapitalCallOutsideCommitment else None,
                    'NetCashFlowQC': float(extraction.NetCashFlowQC) if extraction.NetCashFlowQC else None,
                    'TransferIn': float(extraction.TransferIn) if extraction.TransferIn else None,
                    'TransferOut': float(extraction.TransferOut) if extraction.TransferOut else None,
                    'Quantity': float(extraction.Quantity) if extraction.Quantity else None,
                    'Price': float(extraction.Price) if extraction.Price else None,
                    'CommittedCapital': float(extraction.CommittedCapital) if extraction.CommittedCapital else None,
                    'RemainingCommittedCapital': float(extraction.RemainingCommittedCapital) if extraction.RemainingCommittedCapital else None,
                    'ContributionsToDate': float(extraction.ContributionsToDate) if extraction.ContributionsToDate else None,
                    'DistributionsToDate': float(extraction.DistributionsToDate) if extraction.DistributionsToDate else None,
                    'ReturnOfCapitalToDate': float(extraction.ReturnOfCapitalToDate) if extraction.ReturnOfCapitalToDate else None,
                    'Carry': float(extraction.Carry) if extraction.Carry else None,
                    'Clawback': float(extraction.Clawback) if extraction.Clawback else None,
                }
                extracted_data.append(data)
        
        elif doc_type_normalized in ['statement', 'statements', 'fundstatement', 'fundstatements']:
            logger.debug(f"Querying StatementsExtraction for document {document_id}")
            extractions = session.query(StatementsExtraction).filter(
                StatementsExtraction.doc_id == document_id
            ).all()
            logger.debug(f"Found {len(extractions)} statement extraction records")
            
            for extraction in extractions:
                data = {
                    'id': extraction.id,
                    'Investor': extraction.Investor,
                    'Account': extraction.Account,
                    'InvestorRefID': extraction.InvestorRefID,
                    'AccountRefID': extraction.AccountRefID,
                    'Security': extraction.Security,
                    'PeriodBeginningDT': extraction.PeriodBeginningDT.isoformat() if extraction.PeriodBeginningDT else None,
                    'PeriodEndingDT': extraction.PeriodEndingDT.isoformat() if extraction.PeriodEndingDT else None,
                    'Currency': extraction.Currency,
                    'NetOpeningCapital': float(extraction.NetOpeningCapital) if extraction.NetOpeningCapital else None,
                    'Contributions': float(extraction.Contributions) if extraction.Contributions else None,
                    'ContributionOutsideCommitment': float(extraction.ContributionOutsideCommitment) if extraction.ContributionOutsideCommitment else None,
                    'Withdrawals': float(extraction.Withdrawals) if extraction.Withdrawals else None,
                    'ReturnOfCapital': float(extraction.ReturnOfCapital) if extraction.ReturnOfCapital else None,
                    'NetCapitalActivity': float(extraction.NetCapitalActivity) if extraction.NetCapitalActivity else None,
                    'TransfersIn': float(extraction.TransfersIn) if extraction.TransfersIn else None,
                    'TransfersOut': float(extraction.TransfersOut) if extraction.TransfersOut else None,
                    'NetTransfers': float(extraction.NetTransfers) if extraction.NetTransfers else None,
                    'IncomeDistribution': float(extraction.IncomeDistribution) if extraction.IncomeDistribution else None,
                    'RealizedGainLoss': float(extraction.RealizedGainLoss) if extraction.RealizedGainLoss else None,
                    'UnrealizedGainLoss': float(extraction.UnrealizedGainLoss) if extraction.UnrealizedGainLoss else None,
                    'NetGainLoss': float(extraction.NetGainLoss) if extraction.NetGainLoss else None,
                    'InvestmentIncome': float(extraction.InvestmentIncome) if extraction.InvestmentIncome else None,
                    'OtherIncomeLoss': float(extraction.OtherIncomeLoss) if extraction.OtherIncomeLoss else None,
                    'ManagementFee': float(extraction.ManagementFee) if extraction.ManagementFee else None,
                    'OtherExpenses': float(extraction.OtherExpenses) if extraction.OtherExpenses else None,
                    'CarriedInterest': float(extraction.CarriedInterest) if extraction.CarriedInterest else None,
                    'OtherAdjustments': float(extraction.OtherAdjustments) if extraction.OtherAdjustments else None,
                }
                extracted_data.append(data)
        else:
            logger.warning(f"Unknown document type '{document_type}' (normalized: '{doc_type_normalized}') for document {document_id}. No extraction table matched.")
        
        if extracted_data:
            logger.info(f"Successfully retrieved {len(extracted_data)} extraction records for document {document_id}")
            return extracted_data
        else:
            logger.debug(f"No extraction data found for document {document_id} with type {document_type}")
            return None
    
    except Exception as e:
        logger.error(f"Error fetching extracted data for document {document_id}: {e}", exc_info=True)
        return None

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
    assigned_accounts: Optional[List[AssignedAccountType]] = strawberry.field(name="assignedAccounts", default=None)
    extracted_data: Optional[List[strawberry.scalars.JSON]] = strawberry.field(name="extractedData", default=None)

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
    assign_account_ids: Optional[List[str]] = strawberry.field(name="assignAccountIds", default=None)

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
                    assigned_accounts = _getAssignedAccountsFromMetadata(doc.document_metadata)
                    extracted_data = _getExtractedData(session, doc.id, doc.type)
                    document_list.append(DocumentType(
                        id=doc.id,
                        doc_id=str(doc.doc_id) if doc.doc_id else None,
                        name=doc.name,
                        type=doc.type,
                        path=doc.path,
                        size=doc.size,
                        status="Delivered" if doc.status == "completed" else doc.status,
                        fund_id=doc.fund_id,
                        account_id=doc.account_id,
                        client_id=doc.client_id,
                        upload_date=doc.upload_date.isoformat() if doc.upload_date else "",
                        replay=doc.replay,
                        created_by=doc.created_by,
                        metadata=doc.document_metadata,
                        is_active=doc.is_active,
                        created_at=doc.created_at.isoformat() if doc.created_at else "",
                        updated_at=doc.updated_at.isoformat() if doc.updated_at else "",
                        assigned_accounts=assigned_accounts if assigned_accounts else None,
                        extracted_data=extracted_data
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
                
                assigned_accounts = _getAssignedAccountsFromMetadata(document.document_metadata)
                extracted_data = _getExtractedData(session, document.id, document.type)
                
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
                    updated_at=document.updated_at.isoformat() if document.updated_at else "",
                    assigned_accounts=assigned_accounts if assigned_accounts else None,
                    extracted_data=extracted_data
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
                    assigned_accounts = _getAssignedAccountsFromMetadata(doc.document_metadata)
                    extracted_data = _getExtractedData(session, doc.id, doc.type)
                    document_list.append(DocumentType(
                        id=doc.id,
                        doc_id=str(doc.doc_id) if doc.doc_id else None,
                        name=doc.name,
                        type=doc.type,
                        path=doc.path,
                        size=doc.size,
                        status="Delivered" if doc.status == "completed" else doc.status,
                        fund_id=doc.fund_id,
                        account_id=doc.account_id,
                        client_id=doc.client_id,
                        upload_date=doc.upload_date.isoformat() if doc.upload_date else "",
                        replay=doc.replay,
                        created_by=doc.created_by,
                        metadata=doc.document_metadata,
                        is_active=doc.is_active,
                        created_at=doc.created_at.isoformat() if doc.created_at else "",
                        updated_at=doc.updated_at.isoformat() if doc.updated_at else "",
                        assigned_accounts=assigned_accounts if assigned_accounts else None,
                        extracted_data=extracted_data
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
                
                assigned_accounts = _getAssignedAccountsFromMetadata(new_document.document_metadata)
                extracted_data = _getExtractedData(session, new_document.id, new_document.type)
                
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
                    updated_at=new_document.updated_at.isoformat() if new_document.updated_at else "",
                    assigned_accounts=assigned_accounts if assigned_accounts else None,
                    extracted_data=extracted_data
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
                    logger.info(f"Updating document name to: {input.name}")
                if input.type is not None:
                    old_type = document.type
                    document.type = input.type
                    logger.info(f"Updating document type from '{old_type}' to '{input.type}'")
                if input.path is not None:
                    document.path = input.path
                if input.size is not None:
                    document.size = input.size
                if input.status is not None:
                    old_status = document.status
                    document.status = input.status
                    logger.info(f"Updating document status from '{old_status}' to '{input.status}'")
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
                # Handle metadata update - merge instead of replace to preserve assigned_accounts
                if input.metadata is not None:
                    # Merge with existing metadata to preserve assigned_accounts
                    if document.document_metadata is None:
                        document.document_metadata = {}
                    # Create a copy to avoid modifying the input
                    existing_metadata = document.document_metadata.copy() if isinstance(document.document_metadata, dict) else {}
                    # Merge input metadata with existing (input takes precedence for non-assigned_accounts fields)
                    if isinstance(input.metadata, dict):
                        merged_metadata = {**existing_metadata, **input.metadata}
                        # Preserve assigned_accounts from existing if not in input
                        if 'assigned_accounts' in existing_metadata and 'assigned_accounts' not in input.metadata:
                            merged_metadata['assigned_accounts'] = existing_metadata['assigned_accounts']
                        document.document_metadata = merged_metadata
                    else:
                        document.document_metadata = input.metadata
                
                if input.is_active is not None:
                    document.is_active = input.is_active
                if input.doc_id is not None:
                    document.doc_id = input.doc_id
                
                # Handle assignAccountIds if provided
                if input.assign_account_ids is not None:
                    # Get existing assigned accounts from metadata (use current state after metadata merge)
                    current_metadata = document.document_metadata or {}
                    if not isinstance(current_metadata, dict):
                        current_metadata = {}
                        document.document_metadata = current_metadata
                    
                    existing_assigned_accounts = current_metadata.get('assigned_accounts', [])
                    if not isinstance(existing_assigned_accounts, list):
                        existing_assigned_accounts = []
                    
                    # Track existing account IDs to avoid duplicates (convert to string for consistent comparison)
                    existing_account_ids = {str(acc.get('account_id')) for acc in existing_assigned_accounts if isinstance(acc, dict)}
                    
                    # Validate and fetch investors for each account_id
                    invalid_account_ids = []
                    new_assigned_accounts = []
                    
                    for account_id in input.assign_account_ids:
                        # Convert to string for comparison
                        account_id_str = str(account_id)
                        
                        # Skip if already assigned
                        if account_id_str in existing_account_ids:
                            logger.info(f"Account {account_id_str} already assigned, skipping")
                            continue
                        
                        # Fetch investor by account_number (ensure string comparison)
                        investor = session.query(Investor).filter(
                            Investor.account_number == account_id_str
                        ).first()
                        
                        if not investor:
                            logger.warning(f"Investor with account_number '{account_id_str}' not found")
                            invalid_account_ids.append(account_id_str)
                            continue
                        
                        logger.info(f"Processing account {account_id_str} for investor {investor.id}")
                        
                        # Get fund information from FundInvestor (get first active fund)
                        fund_info = None
                        client_id = None
                        client_name = None
                        
                        fund_investor = session.query(FundInvestor).options(
                            joinedload(FundInvestor.fund).joinedload(Fund.clients)
                        ).filter(
                            FundInvestor.investor_id == investor.id,
                            FundInvestor.is_active == True
                        ).first()
                        
                        if fund_investor and fund_investor.fund:
                            fund_info = fund_investor.fund.name
                            # Get client information from fund's clients relationship (get first client)
                            if fund_investor.fund.clients:
                                first_client = fund_investor.fund.clients[0]
                                client_id = first_client.id
                                client_name = first_client.name
                        
                        # Create assigned account entry
                        assigned_account = {
                            'status': investor.status,
                            'account_id': investor.account_number,
                            'account_name': investor.account_name,
                            'investor_name': investor.investor_name,
                            'fund': fund_info,
                            'client_id': client_id,
                            'client_name': client_name
                        }
                        
                        new_assigned_accounts.append(assigned_account)
                        logger.info(f"Successfully added account {account_id_str} to assigned accounts")
                    
                    # Return error if any account IDs are invalid
                    if invalid_account_ids:
                        logger.error(f"Invalid account IDs found: {invalid_account_ids}")
                        return DocumentResponse(
                            success=False,
                            message=f"Invalid account IDs: {', '.join(invalid_account_ids)}. These accounts do not exist in the investor table.",
                            document=None
                        )
                    
                    # Log summary
                    logger.info(f"Processing {len(input.assign_account_ids)} account IDs. Found {len(new_assigned_accounts)} new accounts to add.")
                    
                    # Merge new accounts with existing ones (avoiding duplicates)
                    existing_assigned_accounts.extend(new_assigned_accounts)
                    logger.info(f"Total assigned accounts after merge: {len(existing_assigned_accounts)}")
                    
                    # Update metadata with assigned accounts (ensure metadata dict exists)
                    if document.document_metadata is None:
                        document.document_metadata = {}
                    elif not isinstance(document.document_metadata, dict):
                        document.document_metadata = {}
                    
                    document.document_metadata['assigned_accounts'] = existing_assigned_accounts
                    # Mark the JSON column as modified so SQLAlchemy detects the change
                    flag_modified(document, 'document_metadata')
                    logger.info(f"Updated document metadata with {len(existing_assigned_accounts)} assigned accounts: {[acc.get('account_id') for acc in existing_assigned_accounts]}")
                
                session.flush()  # Flush before commit to ensure changes are written
                session.commit()
                session.refresh(document)
                
                # Debug: Log the metadata structure after refresh
                logger.info(f"Document metadata after refresh: {document.document_metadata}")
                if document.document_metadata and isinstance(document.document_metadata, dict):
                    assigned_in_metadata = document.document_metadata.get('assigned_accounts', [])
                    logger.info(f"Assigned accounts in metadata: {len(assigned_in_metadata) if isinstance(assigned_in_metadata, list) else 0} accounts")
                
                # Extract assigned accounts from metadata (ensure we get the latest)
                assigned_accounts = _getAssignedAccountsFromMetadata(document.document_metadata)
                logger.info(f"Extracted {len(assigned_accounts)} assigned accounts from metadata for response")
                
                # Get extracted data
                extracted_data = _getExtractedData(session, document.id, document.type)
                
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
                    updated_at=document.updated_at.isoformat() if document.updated_at else "",
                    assigned_accounts=assigned_accounts if assigned_accounts else None,
                    extracted_data=extracted_data
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

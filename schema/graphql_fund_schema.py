#!/usr/bin/env python3
"""
GraphQL Schema for Fund Management - Clean Implementation with Authentication
Provides pure data responses without response formatting
Maintains consistency with REST API authentication system
"""

import strawberry
from typing import List, Optional
from strawberry.types import Info
from sqlalchemy import or_, desc
from database_models import Fund, Client, get_database_manager, client_funds
from datetime import datetime, date
import logging

# Import authentication context
from .graphql_auth_context import require_authentication

logger = logging.getLogger(__name__)

@strawberry.type
class FundType:
    """GraphQL type for Fund"""
    id: int
    name: str
    code: str
    description: Optional[str]
    type: Optional[str]
    fund_manager: Optional[str]
    base_currency: Optional[str]
    fund_admin: Optional[strawberry.scalars.JSON]
    shadow: Optional[strawberry.scalars.JSON]
    contact_person: Optional[str]
    contact_email: Optional[str]
    contact_number: Optional[str]
    sector: Optional[str]
    geography: Optional[str]
    strategy: Optional[strawberry.scalars.JSON]
    market_cap: Optional[str]
    benchmark: Optional[strawberry.scalars.JSON]
    fund_metadata: Optional[strawberry.scalars.JSON]
    stage: Optional[str]
    inception_date: Optional[str]
    investment_start_date: Optional[str]
    commitment_subscription: Optional[float]
    is_active: bool
    created_at: str
    updated_at: str

@strawberry.input
class FundCreateInput:
    """Input type for creating a fund"""
    name: str
    code: Optional[str] = None
    description: Optional[str] = None
    type: Optional[str] = None
    fund_manager: Optional[str] = None
    base_currency: Optional[str] = None
    fund_admin: Optional[strawberry.scalars.JSON] = None
    shadow: Optional[strawberry.scalars.JSON] = None
    contact_person: Optional[str] = None
    contact_email: Optional[str] = None
    contact_number: Optional[str] = None
    sector: Optional[str] = None
    geography: Optional[str] = None
    strategy: Optional[strawberry.scalars.JSON] = None
    market_cap: Optional[str] = None
    benchmark: Optional[strawberry.scalars.JSON] = None
    fund_metadata: Optional[strawberry.scalars.JSON] = None
    stage: Optional[str] = None
    inception_date: Optional[str] = None  # ISO format date string
    investment_start_date: Optional[str] = None  # ISO format date string
    commitment_subscription: Optional[float] = None
    is_active: Optional[bool] = True

@strawberry.type
class FundQuery:
    """GraphQL Query root for funds"""
    
    @strawberry.field
    def funds(self, info: Info,
              id: Optional[int] = None,
              search: Optional[str] = None,
              status_filter: Optional[str] = None,
              limit: Optional[int] = 10,
              offset: Optional[int] = 0) -> List[FundType]:
        """Get funds with filtering and pagination from public.funds table - requires authentication"""
        # Require authentication
        require_authentication(info)
        
        db_manager = get_database_manager()
        session = db_manager.get_session()
        
        try:
            query = session.query(Fund)
            
            # Single fund by ID
            if id:
                query = query.filter(Fund.id == id)
            
            # Search functionality
            if search:
                query = query.filter(
                    or_(
                        Fund.name.ilike(f"%{search}%"),
                        Fund.code.ilike(f"%{search}%"),
                        Fund.description.ilike(f"%{search}%"),
                        Fund.fund_manager.ilike(f"%{search}%")
                    )
                )
            
            # Status filter
            if status_filter == 'active':
                query = query.filter(Fund.is_active == True)
            elif status_filter == 'inactive':
                query = query.filter(Fund.is_active == False)
            
            # Apply sorting by created_at in descending order (newest first) and pagination
            query = query.order_by(desc(Fund.created_at)).offset(offset).limit(limit)
            
            # Execute query
            funds = query.all()
            
            # Convert to GraphQL types
            return [
                FundType(
                    id=fund.id,
                    name=fund.name,
                    code=fund.code,
                    description=fund.description,
                    type=fund.type,
                    fund_manager=fund.fund_manager,
                    base_currency=fund.base_currency,
                    fund_admin=fund.fund_admin,
                    shadow=fund.shadow,
                    contact_person=fund.contact_person,
                    contact_email=fund.contact_email,
                    contact_number=fund.contact_number,
                    sector=fund.sector,
                    geography=fund.geography,
                    strategy=fund.strategy,
                    market_cap=fund.market_cap,
                    benchmark=fund.benchmark,
                    fund_metadata=fund.fund_metadata,
                    stage=fund.stage,
                    inception_date=fund.inception_date.isoformat() if fund.inception_date else None,
                    investment_start_date=fund.investment_start_date.isoformat() if fund.investment_start_date else None,
                    commitment_subscription=float(fund.commitment_subscription) if fund.commitment_subscription else None,
                    is_active=fund.is_active,
                    created_at=fund.created_at.isoformat() if fund.created_at else "",
                    updated_at=fund.updated_at.isoformat() if fund.updated_at else ""
                ) for fund in funds
            ]
            
        except Exception as e:
            logger.error(f"GraphQL funds query error: {e}")
            return []
            
        finally:
            session.close()
    
    @strawberry.field
    def fundsByClient(self, info: Info,
                      client_id: int,
                      search: Optional[str] = None,
                      status_filter: Optional[str] = None,
                      limit: Optional[int] = 10,
                      offset: Optional[int] = 0) -> List[FundType]:
        """Get funds associated with a specific client from client_funds table - requires authentication"""
        # Require authentication
        require_authentication(info)
        
        db_manager = get_database_manager()
        session = db_manager.get_session()
        
        try:
            # Verify client exists
            client = session.query(Client).filter(Client.id == client_id).first()
            if not client:
                logger.warning(f"Client with id {client_id} not found")
                return []
            
            # Query funds through the client_funds association table
            query = session.query(Fund).join(
                client_funds, Fund.id == client_funds.c.fund_id
            ).filter(
                client_funds.c.client_id == client_id
            )
            
            # Search functionality
            if search:
                query = query.filter(
                    or_(
                        Fund.name.ilike(f"%{search}%"),
                        Fund.code.ilike(f"%{search}%"),
                        Fund.description.ilike(f"%{search}%"),
                        Fund.fund_manager.ilike(f"%{search}%")
                    )
                )
            
            # Status filter
            if status_filter == 'active':
                query = query.filter(Fund.is_active == True)
            elif status_filter == 'inactive':
                query = query.filter(Fund.is_active == False)
            
            # Pagination
            query = query.offset(offset).limit(limit)
            
            # Execute query
            funds = query.all()
            
            # Convert to GraphQL types
            return [
                FundType(
                    id=fund.id,
                    name=fund.name,
                    code=fund.code,
                    description=fund.description,
                    type=fund.type,
                    fund_manager=fund.fund_manager,
                    base_currency=fund.base_currency,
                    fund_admin=fund.fund_admin,
                    shadow=fund.shadow,
                    contact_person=fund.contact_person,
                    contact_email=fund.contact_email,
                    contact_number=fund.contact_number,
                    sector=fund.sector,
                    geography=fund.geography,
                    strategy=fund.strategy,
                    market_cap=fund.market_cap,
                    benchmark=fund.benchmark,
                    fund_metadata=fund.fund_metadata,
                    stage=fund.stage,
                    inception_date=fund.inception_date.isoformat() if fund.inception_date else None,
                    investment_start_date=fund.investment_start_date.isoformat() if fund.investment_start_date else None,
                    commitment_subscription=float(fund.commitment_subscription) if fund.commitment_subscription else None,
                    is_active=fund.is_active,
                    created_at=fund.created_at.isoformat() if fund.created_at else "",
                    updated_at=fund.updated_at.isoformat() if fund.updated_at else ""
                ) for fund in funds
            ]
            
        except Exception as e:
            logger.error(f"GraphQL fundsByClient query error: {e}")
            return []
            
        finally:
            session.close()

@strawberry.type
class FundMutation:
    """GraphQL Mutation root for funds"""
    
    @strawberry.field
    def createFund(self, info: Info, input: FundCreateInput) -> Optional[FundType]:
        """Create a new fund - requires authentication"""
        # Require authentication
        require_authentication(info)
        
        db_manager = get_database_manager()
        session = db_manager.get_session()
        
        try:
            # Generate fund code if not provided
            if not input.code:
                fund_code = input.name.replace(" ", "").lower()
            else:
                fund_code = input.code
            
            # Check if fund code already exists
            existing_fund = session.query(Fund).filter(Fund.code == fund_code).first()
            if existing_fund:
                raise ValueError(f"Fund with code '{fund_code}' already exists")
            
            # Check if fund name already exists
            existing_fund_name = session.query(Fund).filter(Fund.name == input.name).first()
            if existing_fund_name:
                raise ValueError(f"Fund with name '{input.name}' already exists")
            
            # Parse date strings if provided
            inception_date_obj = None
            if input.inception_date:
                try:
                    # Try parsing as date string first (YYYY-MM-DD)
                    if len(input.inception_date) == 10:
                        inception_date_obj = datetime.strptime(input.inception_date, "%Y-%m-%d").date()
                    else:
                        # Try parsing as datetime string
                        inception_date_obj = datetime.fromisoformat(input.inception_date.replace('Z', '+00:00')).date()
                except ValueError:
                    raise ValueError(f"Invalid inception_date format. Expected ISO format (YYYY-MM-DD)")
            
            investment_start_date_obj = None
            if input.investment_start_date:
                try:
                    # Try parsing as date string first (YYYY-MM-DD)
                    if len(input.investment_start_date) == 10:
                        investment_start_date_obj = datetime.strptime(input.investment_start_date, "%Y-%m-%d").date()
                    else:
                        # Try parsing as datetime string
                        investment_start_date_obj = datetime.fromisoformat(input.investment_start_date.replace('Z', '+00:00')).date()
                except ValueError:
                    raise ValueError(f"Invalid investment_start_date format. Expected ISO format (YYYY-MM-DD)")
            
            # Create new fund
            new_fund = Fund(
                name=input.name,
                code=fund_code,
                description=input.description,
                type=input.type,
                fund_manager=input.fund_manager,
                base_currency=input.base_currency,
                fund_admin=input.fund_admin,
                shadow=input.shadow,
                contact_person=input.contact_person,
                contact_email=input.contact_email,
                contact_number=input.contact_number,
                sector=input.sector,
                geography=input.geography,
                strategy=input.strategy,
                market_cap=input.market_cap,
                benchmark=input.benchmark,
                fund_metadata=input.fund_metadata,
                stage=input.stage,
                inception_date=inception_date_obj,
                investment_start_date=investment_start_date_obj,
                commitment_subscription=input.commitment_subscription,
                is_active=input.is_active if input.is_active is not None else True
            )
            
            session.add(new_fund)
            session.flush()  # Flush to get the ID
            
            session.commit()
            
            # Convert to GraphQL type
            return FundType(
                id=new_fund.id,
                name=new_fund.name,
                code=new_fund.code,
                description=new_fund.description,
                type=new_fund.type,
                fund_manager=new_fund.fund_manager,
                base_currency=new_fund.base_currency,
                fund_admin=new_fund.fund_admin,
                shadow=new_fund.shadow,
                contact_person=new_fund.contact_person,
                contact_email=new_fund.contact_email,
                contact_number=new_fund.contact_number,
                sector=new_fund.sector,
                geography=new_fund.geography,
                strategy=new_fund.strategy,
                market_cap=new_fund.market_cap,
                benchmark=new_fund.benchmark,
                fund_metadata=new_fund.fund_metadata,
                stage=new_fund.stage,
                inception_date=new_fund.inception_date.isoformat() if new_fund.inception_date else None,
                investment_start_date=new_fund.investment_start_date.isoformat() if new_fund.investment_start_date else None,
                commitment_subscription=float(new_fund.commitment_subscription) if new_fund.commitment_subscription else None,
                is_active=new_fund.is_active,
                created_at=new_fund.created_at.isoformat() if new_fund.created_at else "",
                updated_at=new_fund.updated_at.isoformat() if new_fund.updated_at else ""
            )
            
        except ValueError as e:
            logger.error(f"GraphQL createFund validation error: {e}")
            session.rollback()
            raise Exception(str(e))
        except Exception as e:
            logger.error(f"GraphQL createFund error: {e}")
            session.rollback()
            raise Exception(f"Failed to create fund: {str(e)}")
            
        finally:
            session.close()


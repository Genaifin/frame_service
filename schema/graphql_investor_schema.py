#!/usr/bin/env python3
"""
GraphQL Schema for Investor Management
Provides GraphQL endpoints for investor operations with authentication
"""

import strawberry
from typing import List, Optional
from strawberry.types import Info
from sqlalchemy.orm import joinedload
from sqlalchemy import or_
from database_models import Investor, FundInvestor, Fund, get_database_manager
from datetime import datetime
import logging

# Import authentication context
from .graphql_auth_context import require_authentication, require_role, get_current_user, is_authenticated

logger = logging.getLogger(__name__)

@strawberry.type
class InvestorType:
    """GraphQL type for Investor"""
    id: int
    investor_name: str
    account_name: str
    account_number: str
    contact_title: Optional[str] = None
    contact_first_name: Optional[str] = None
    contact_last_name: Optional[str] = None
    contact_email: Optional[str] = None
    contact_number: Optional[str] = None
    address_line1: Optional[str] = None
    address_line2: Optional[str] = None
    city: Optional[str] = None
    state: Optional[str] = None
    postal_code: Optional[str] = None
    country: Optional[str] = None
    investor_type: Optional[str] = None
    tax_id: Optional[str] = None
    kyc_status: Optional[str] = None
    risk_profile: Optional[str] = None
    is_active: bool
    notes: Optional[str] = None
    investor_metadata: Optional[strawberry.scalars.JSON] = None
    created_at: str
    updated_at: str

@strawberry.type
class FundInvestorType:
    """GraphQL type for Fund-Investor mapping"""
    id: int
    fund_id: int
    investor_id: int
    investment_amount: Optional[float] = None
    investment_date: Optional[str] = None
    investment_type: Optional[str] = None
    units_held: Optional[float] = None
    unit_price: Optional[float] = None
    is_active: bool
    notes: Optional[str] = None
    investor_metadata: Optional[strawberry.scalars.JSON] = None
    created_at: str
    updated_at: str

@strawberry.type
class FundType:
    """GraphQL type for Fund (simplified for investor context)"""
    id: int
    name: str
    code: str
    description: Optional[str] = None
    type: Optional[str] = None
    is_active: bool

@strawberry.input
class InvestorCreateInput:
    """Input type for creating an investor"""
    investor_name: str
    account_name: str
    account_number: str
    contact_title: Optional[str] = None
    contact_first_name: Optional[str] = None
    contact_last_name: Optional[str] = None
    contact_email: Optional[str] = None
    contact_number: Optional[str] = None
    address_line1: Optional[str] = None
    address_line2: Optional[str] = None
    city: Optional[str] = None
    state: Optional[str] = None
    postal_code: Optional[str] = None
    country: Optional[str] = None
    investor_type: Optional[str] = None
    tax_id: Optional[str] = None
    kyc_status: Optional[str] = None
    risk_profile: Optional[str] = None
    notes: Optional[str] = None
    investor_metadata: Optional[strawberry.scalars.JSON] = None

@strawberry.input
class InvestorUpdateInput:
    """Input type for updating an investor"""
    investor_name: Optional[str] = None
    account_name: Optional[str] = None
    account_number: Optional[str] = None
    contact_title: Optional[str] = None
    contact_first_name: Optional[str] = None
    contact_last_name: Optional[str] = None
    contact_email: Optional[str] = None
    contact_number: Optional[str] = None
    address_line1: Optional[str] = None
    address_line2: Optional[str] = None
    city: Optional[str] = None
    state: Optional[str] = None
    postal_code: Optional[str] = None
    country: Optional[str] = None
    investor_type: Optional[str] = None
    tax_id: Optional[str] = None
    kyc_status: Optional[str] = None
    risk_profile: Optional[str] = None
    notes: Optional[str] = None
    investor_metadata: Optional[strawberry.scalars.JSON] = None

@strawberry.input
class FundInvestorCreateInput:
    """Input type for creating fund-investor mapping"""
    fund_id: int
    investor_id: int
    investment_amount: Optional[float] = None
    investment_date: Optional[str] = None
    investment_type: Optional[str] = None
    units_held: Optional[float] = None
    unit_price: Optional[float] = None
    notes: Optional[str] = None
    investor_metadata: Optional[strawberry.scalars.JSON] = None

@strawberry.input
class FundInvestorUpdateInput:
    """Input type for updating fund-investor mapping"""
    investment_amount: Optional[float] = None
    investment_date: Optional[str] = None
    investment_type: Optional[str] = None
    units_held: Optional[float] = None
    unit_price: Optional[float] = None
    notes: Optional[str] = None
    investor_metadata: Optional[strawberry.scalars.JSON] = None

@strawberry.type
class InvestorQuery:
    """GraphQL Query root for investors"""
    
    @strawberry.field
    def investors(self, info: Info,
                  id: Optional[int] = None,
                  search: Optional[str] = None,
                  status_filter: Optional[str] = None,
                  investor_type: Optional[str] = None,
                  kyc_status: Optional[str] = None,
                  limit: Optional[int] = 50,
                  offset: Optional[int] = 0) -> List[InvestorType]:
        """Get investors with filtering and pagination - requires authentication"""
        # Require authentication
        require_authentication(info)
        
        db_manager = get_database_manager()
        session = db_manager.get_session()
        
        try:
            query = session.query(Investor)
            
            # Single investor by ID
            if id:
                query = query.filter(Investor.id == id)
            
            # Search functionality
            if search:
                query = query.filter(
                    or_(
                        Investor.investor_name.ilike(f"%{search}%"),
                        Investor.account_name.ilike(f"%{search}%"),
                        Investor.account_number.ilike(f"%{search}%"),
                        Investor.contact_email.ilike(f"%{search}%"),
                        Investor.contact_first_name.ilike(f"%{search}%"),
                        Investor.contact_last_name.ilike(f"%{search}%")
                    )
                )
            
            # Status filter
            if status_filter == 'active':
                query = query.filter(Investor.is_active == True)
            elif status_filter == 'inactive':
                query = query.filter(Investor.is_active == False)
            
            # Investor type filter
            if investor_type:
                query = query.filter(Investor.investor_type == investor_type)
            
            # KYC status filter
            if kyc_status:
                query = query.filter(Investor.kyc_status == kyc_status)
            
            # Pagination
            query = query.offset(offset).limit(limit)
            
            # Execute query
            investors = query.all()
            
            # Convert to GraphQL types
            return [
                InvestorType(
                    id=investor.id,
                    investor_name=investor.investor_name,
                    account_name=investor.account_name,
                    account_number=investor.account_number,
                    contact_title=investor.contact_title,
                    contact_first_name=investor.contact_first_name,
                    contact_last_name=investor.contact_last_name,
                    contact_email=investor.contact_email,
                    contact_number=investor.contact_number,
                    address_line1=investor.address_line1,
                    address_line2=investor.address_line2,
                    city=investor.city,
                    state=investor.state,
                    postal_code=investor.postal_code,
                    country=investor.country,
                    investor_type=investor.investor_type,
                    tax_id=investor.tax_id,
                    kyc_status=investor.kyc_status,
                    risk_profile=investor.risk_profile,
                    is_active=investor.is_active,
                    notes=investor.notes,
                    metadata=investor.metadata,
                    created_at=investor.created_at.isoformat() if investor.created_at else "",
                    updated_at=investor.updated_at.isoformat() if investor.updated_at else ""
                ) for investor in investors
            ]
            
        except Exception as e:
            logger.error(f"GraphQL investors query error: {e}")
            return []
            
        finally:
            session.close()
    
    @strawberry.field
    def investor_edit_form(self, info: Info, investor_id: int) -> Optional[InvestorType]:
        """Get investor data for edit form - requires authentication"""
        # Require authentication
        require_authentication(info)
        
        db_manager = get_database_manager()
        session = db_manager.get_session()
        
        try:
            investor = session.query(Investor).filter(Investor.id == investor_id).first()
            
            if not investor:
                return None
            
            return InvestorType(
                id=investor.id,
                investor_name=investor.investor_name,
                account_name=investor.account_name,
                account_number=investor.account_number,
                contact_title=investor.contact_title,
                contact_first_name=investor.contact_first_name,
                contact_last_name=investor.contact_last_name,
                contact_email=investor.contact_email,
                contact_number=investor.contact_number,
                address_line1=investor.address_line1,
                address_line2=investor.address_line2,
                city=investor.city,
                state=investor.state,
                postal_code=investor.postal_code,
                country=investor.country,
                investor_type=investor.investor_type,
                tax_id=investor.tax_id,
                kyc_status=investor.kyc_status,
                risk_profile=investor.risk_profile,
                is_active=investor.is_active,
                notes=investor.notes,
                metadata=investor.metadata,
                created_at=investor.created_at.isoformat() if investor.created_at else "",
                updated_at=investor.updated_at.isoformat() if investor.updated_at else ""
            )
            
        except Exception as e:
            logger.error(f"GraphQL investor_edit_form error: {e}")
            return None
            
        finally:
            session.close()
    
    @strawberry.field
    def fund_investors(self, info: Info,
                       fund_id: Optional[int] = None,
                       investor_id: Optional[int] = None,
                       limit: Optional[int] = 50,
                       offset: Optional[int] = 0) -> List[FundInvestorType]:
        """Get fund-investor mappings - requires authentication"""
        # Require authentication
        require_authentication(info)
        
        db_manager = get_database_manager()
        session = db_manager.get_session()
        
        try:
            query = session.query(FundInvestor)
            
            # Filter by fund ID
            if fund_id:
                query = query.filter(FundInvestor.fund_id == fund_id)
            
            # Filter by investor ID
            if investor_id:
                query = query.filter(FundInvestor.investor_id == investor_id)
            
            # Pagination
            query = query.offset(offset).limit(limit)
            
            # Execute query
            fund_investors = query.all()
            
            # Convert to GraphQL types
            return [
                FundInvestorType(
                    id=fi.id,
                    fund_id=fi.fund_id,
                    investor_id=fi.investor_id,
                    investment_amount=float(fi.investment_amount) if fi.investment_amount else None,
                    investment_date=fi.investment_date.isoformat() if fi.investment_date else None,
                    investment_type=fi.investment_type,
                    units_held=float(fi.units_held) if fi.units_held else None,
                    unit_price=float(fi.unit_price) if fi.unit_price else None,
                    is_active=fi.is_active,
                    notes=fi.notes,
                    metadata=fi.metadata,
                    created_at=fi.created_at.isoformat() if fi.created_at else "",
                    updated_at=fi.updated_at.isoformat() if fi.updated_at else ""
                ) for fi in fund_investors
            ]
            
        except Exception as e:
            logger.error(f"GraphQL fund_investors query error: {e}")
            return []
            
        finally:
            session.close()

@strawberry.type
class InvestorMutation:
    """GraphQL Mutation root for investors"""
    
    @strawberry.field
    def create_investor(self, info: Info, input: InvestorCreateInput) -> Optional[InvestorType]:
        """Create a new investor - requires authentication"""
        # Require authentication
        require_authentication(info)
        
        db_manager = get_database_manager()
        session = db_manager.get_session()
        
        try:
            # Check if account number already exists
            existing_account = session.query(Investor).filter(Investor.account_number == input.account_number).first()
            if existing_account:
                raise ValueError("Account number already exists")
            
            # Create new investor
            new_investor = Investor(
                investor_name=input.investor_name,
                account_name=input.account_name,
                account_number=input.account_number,
                contact_title=input.contact_title,
                contact_first_name=input.contact_first_name,
                contact_last_name=input.contact_last_name,
                contact_email=input.contact_email,
                contact_number=input.contact_number,
                address_line1=input.address_line1,
                address_line2=input.address_line2,
                city=input.city,
                state=input.state,
                postal_code=input.postal_code,
                country=input.country,
                investor_type=input.investor_type,
                tax_id=input.tax_id,
                kyc_status=input.kyc_status or 'pending',
                risk_profile=input.risk_profile,
                notes=input.notes,
                metadata=input.metadata,
                is_active=True
            )
            
            session.add(new_investor)
            session.commit()
            
            # Get the created investor
            created_investor = session.query(Investor).filter(Investor.id == new_investor.id).first()
            
            return InvestorType(
                id=created_investor.id,
                investor_name=created_investor.investor_name,
                account_name=created_investor.account_name,
                account_number=created_investor.account_number,
                contact_title=created_investor.contact_title,
                contact_first_name=created_investor.contact_first_name,
                contact_last_name=created_investor.contact_last_name,
                contact_email=created_investor.contact_email,
                contact_number=created_investor.contact_number,
                address_line1=created_investor.address_line1,
                address_line2=created_investor.address_line2,
                city=created_investor.city,
                state=created_investor.state,
                postal_code=created_investor.postal_code,
                country=created_investor.country,
                investor_type=created_investor.investor_type,
                tax_id=created_investor.tax_id,
                kyc_status=created_investor.kyc_status,
                risk_profile=created_investor.risk_profile,
                is_active=created_investor.is_active,
                notes=created_investor.notes,
                metadata=created_investor.metadata,
                created_at=created_investor.created_at.isoformat() if created_investor.created_at else "",
                updated_at=created_investor.updated_at.isoformat() if created_investor.updated_at else ""
            )
            
        except Exception as e:
            logger.error(f"GraphQL create_investor error: {e}")
            session.rollback()
            raise Exception(f"Failed to create investor: {str(e)}")
            
        finally:
            session.close()
    
    @strawberry.field
    def update_investor(self, info: Info, investor_id: int, input: InvestorUpdateInput) -> Optional[InvestorType]:
        """Update an existing investor - requires authentication"""
        # Require authentication
        require_authentication(info)
        
        db_manager = get_database_manager()
        session = db_manager.get_session()
        
        try:
            investor = session.query(Investor).filter(Investor.id == investor_id).first()
            
            if not investor:
                raise ValueError("Investor not found")
            
            # Check if account number already exists (if being updated)
            if input.account_number and input.account_number != investor.account_number:
                existing_account = session.query(Investor).filter(Investor.account_number == input.account_number).first()
                if existing_account:
                    raise ValueError("Account number already exists")
            
            # Update fields
            if input.investor_name is not None:
                investor.investor_name = input.investor_name
            if input.account_name is not None:
                investor.account_name = input.account_name
            if input.account_number is not None:
                investor.account_number = input.account_number
            if input.contact_title is not None:
                investor.contact_title = input.contact_title
            if input.contact_first_name is not None:
                investor.contact_first_name = input.contact_first_name
            if input.contact_last_name is not None:
                investor.contact_last_name = input.contact_last_name
            if input.contact_email is not None:
                investor.contact_email = input.contact_email
            if input.contact_number is not None:
                investor.contact_number = input.contact_number
            if input.address_line1 is not None:
                investor.address_line1 = input.address_line1
            if input.address_line2 is not None:
                investor.address_line2 = input.address_line2
            if input.city is not None:
                investor.city = input.city
            if input.state is not None:
                investor.state = input.state
            if input.postal_code is not None:
                investor.postal_code = input.postal_code
            if input.country is not None:
                investor.country = input.country
            if input.investor_type is not None:
                investor.investor_type = input.investor_type
            if input.tax_id is not None:
                investor.tax_id = input.tax_id
            if input.kyc_status is not None:
                investor.kyc_status = input.kyc_status
            if input.risk_profile is not None:
                investor.risk_profile = input.risk_profile
            if input.notes is not None:
                investor.notes = input.notes
            if input.metadata is not None:
                investor.metadata = input.metadata
            
            session.commit()
            
            return InvestorType(
                id=investor.id,
                investor_name=investor.investor_name,
                account_name=investor.account_name,
                account_number=investor.account_number,
                contact_title=investor.contact_title,
                contact_first_name=investor.contact_first_name,
                contact_last_name=investor.contact_last_name,
                contact_email=investor.contact_email,
                contact_number=investor.contact_number,
                address_line1=investor.address_line1,
                address_line2=investor.address_line2,
                city=investor.city,
                state=investor.state,
                postal_code=investor.postal_code,
                country=investor.country,
                investor_type=investor.investor_type,
                tax_id=investor.tax_id,
                kyc_status=investor.kyc_status,
                risk_profile=investor.risk_profile,
                is_active=investor.is_active,
                notes=investor.notes,
                metadata=investor.metadata,
                created_at=investor.created_at.isoformat() if investor.created_at else "",
                updated_at=investor.updated_at.isoformat() if investor.updated_at else ""
            )
            
        except Exception as e:
            logger.error(f"GraphQL update_investor error: {e}")
            session.rollback()
            raise Exception(f"Failed to update investor: {str(e)}")
            
        finally:
            session.close()
    
    @strawberry.field
    def toggle_investor_status(self, info: Info, investor_id: int) -> Optional[InvestorType]:
        """Toggle investor active/inactive status - requires authentication"""
        # Require authentication
        require_authentication(info)
        
        db_manager = get_database_manager()
        session = db_manager.get_session()
        
        try:
            investor = session.query(Investor).filter(Investor.id == investor_id).first()
            
            if not investor:
                raise ValueError("Investor not found")
            
            # Toggle status
            investor.is_active = not investor.is_active
            session.commit()
            
            return InvestorType(
                id=investor.id,
                investor_name=investor.investor_name,
                account_name=investor.account_name,
                account_number=investor.account_number,
                contact_title=investor.contact_title,
                contact_first_name=investor.contact_first_name,
                contact_last_name=investor.contact_last_name,
                contact_email=investor.contact_email,
                contact_number=investor.contact_number,
                address_line1=investor.address_line1,
                address_line2=investor.address_line2,
                city=investor.city,
                state=investor.state,
                postal_code=investor.postal_code,
                country=investor.country,
                investor_type=investor.investor_type,
                tax_id=investor.tax_id,
                kyc_status=investor.kyc_status,
                risk_profile=investor.risk_profile,
                is_active=investor.is_active,
                notes=investor.notes,
                metadata=investor.metadata,
                created_at=investor.created_at.isoformat() if investor.created_at else "",
                updated_at=investor.updated_at.isoformat() if investor.updated_at else ""
            )
            
        except Exception as e:
            logger.error(f"GraphQL toggle_investor_status error: {e}")
            session.rollback()
            raise Exception(f"Failed to toggle investor status: {str(e)}")
            
        finally:
            session.close()
    
    @strawberry.field
    def create_fund_investor(self, info: Info, input: FundInvestorCreateInput) -> Optional[FundInvestorType]:
        """Create fund-investor mapping - requires authentication"""
        # Require authentication
        require_authentication(info)
        
        db_manager = get_database_manager()
        session = db_manager.get_session()
        
        try:
            # Check if fund exists
            fund = session.query(Fund).filter(Fund.id == input.fund_id).first()
            if not fund:
                raise ValueError("Fund not found")
            
            # Check if investor exists
            investor = session.query(Investor).filter(Investor.id == input.investor_id).first()
            if not investor:
                raise ValueError("Investor not found")
            
            # Check if mapping already exists
            existing_mapping = session.query(FundInvestor).filter(
                FundInvestor.fund_id == input.fund_id,
                FundInvestor.investor_id == input.investor_id
            ).first()
            if existing_mapping:
                raise ValueError("Fund-investor mapping already exists")
            
            # Create new mapping
            new_mapping = FundInvestor(
                fund_id=input.fund_id,
                investor_id=input.investor_id,
                investment_amount=input.investment_amount,
                investment_date=datetime.strptime(input.investment_date, '%Y-%m-%d').date() if input.investment_date else None,
                investment_type=input.investment_type,
                units_held=input.units_held,
                unit_price=input.unit_price,
                notes=input.notes,
                metadata=input.metadata,
                is_active=True
            )
            
            session.add(new_mapping)
            session.commit()
            
            # Get the created mapping
            created_mapping = session.query(FundInvestor).filter(FundInvestor.id == new_mapping.id).first()
            
            return FundInvestorType(
                id=created_mapping.id,
                fund_id=created_mapping.fund_id,
                investor_id=created_mapping.investor_id,
                investment_amount=float(created_mapping.investment_amount) if created_mapping.investment_amount else None,
                investment_date=created_mapping.investment_date.isoformat() if created_mapping.investment_date else None,
                investment_type=created_mapping.investment_type,
                units_held=float(created_mapping.units_held) if created_mapping.units_held else None,
                unit_price=float(created_mapping.unit_price) if created_mapping.unit_price else None,
                is_active=created_mapping.is_active,
                notes=created_mapping.notes,
                metadata=created_mapping.metadata,
                created_at=created_mapping.created_at.isoformat() if created_mapping.created_at else "",
                updated_at=created_mapping.updated_at.isoformat() if created_mapping.updated_at else ""
            )
            
        except Exception as e:
            logger.error(f"GraphQL create_fund_investor error: {e}")
            session.rollback()
            raise Exception(f"Failed to create fund-investor mapping: {str(e)}")
            
        finally:
            session.close()
    
    @strawberry.field
    def update_fund_investor(self, info: Info, mapping_id: int, input: FundInvestorUpdateInput) -> Optional[FundInvestorType]:
        """Update fund-investor mapping - requires authentication"""
        # Require authentication
        require_authentication(info)
        
        db_manager = get_database_manager()
        session = db_manager.get_session()
        
        try:
            mapping = session.query(FundInvestor).filter(FundInvestor.id == mapping_id).first()
            
            if not mapping:
                raise ValueError("Fund-investor mapping not found")
            
            # Update fields
            if input.investment_amount is not None:
                mapping.investment_amount = input.investment_amount
            if input.investment_date is not None:
                mapping.investment_date = datetime.strptime(input.investment_date, '%Y-%m-%d').date()
            if input.investment_type is not None:
                mapping.investment_type = input.investment_type
            if input.units_held is not None:
                mapping.units_held = input.units_held
            if input.unit_price is not None:
                mapping.unit_price = input.unit_price
            if input.notes is not None:
                mapping.notes = input.notes
            if input.metadata is not None:
                mapping.metadata = input.metadata
            
            session.commit()
            
            return FundInvestorType(
                id=mapping.id,
                fund_id=mapping.fund_id,
                investor_id=mapping.investor_id,
                investment_amount=float(mapping.investment_amount) if mapping.investment_amount else None,
                investment_date=mapping.investment_date.isoformat() if mapping.investment_date else None,
                investment_type=mapping.investment_type,
                units_held=float(mapping.units_held) if mapping.units_held else None,
                unit_price=float(mapping.unit_price) if mapping.unit_price else None,
                is_active=mapping.is_active,
                notes=mapping.notes,
                metadata=mapping.metadata,
                created_at=mapping.created_at.isoformat() if mapping.created_at else "",
                updated_at=mapping.updated_at.isoformat() if mapping.updated_at else ""
            )
            
        except Exception as e:
            logger.error(f"GraphQL update_fund_investor error: {e}")
            session.rollback()
            raise Exception(f"Failed to update fund-investor mapping: {str(e)}")
            
        finally:
            session.close()
    
    @strawberry.field
    def toggle_fund_investor_status(self, info: Info, mapping_id: int) -> Optional[FundInvestorType]:
        """Toggle fund-investor mapping status - requires authentication"""
        # Require authentication
        require_authentication(info)
        
        db_manager = get_database_manager()
        session = db_manager.get_session()
        
        try:
            mapping = session.query(FundInvestor).filter(FundInvestor.id == mapping_id).first()
            
            if not mapping:
                raise ValueError("Fund-investor mapping not found")
            
            # Toggle status
            mapping.is_active = not mapping.is_active
            session.commit()
            
            return FundInvestorType(
                id=mapping.id,
                fund_id=mapping.fund_id,
                investor_id=mapping.investor_id,
                investment_amount=float(mapping.investment_amount) if mapping.investment_amount else None,
                investment_date=mapping.investment_date.isoformat() if mapping.investment_date else None,
                investment_type=mapping.investment_type,
                units_held=float(mapping.units_held) if mapping.units_held else None,
                unit_price=float(mapping.unit_price) if mapping.unit_price else None,
                is_active=mapping.is_active,
                notes=mapping.notes,
                metadata=mapping.metadata,
                created_at=mapping.created_at.isoformat() if mapping.created_at else "",
                updated_at=mapping.updated_at.isoformat() if mapping.updated_at else ""
            )
            
        except Exception as e:
            logger.error(f"GraphQL toggle_fund_investor_status error: {e}")
            session.rollback()
            raise Exception(f"Failed to toggle fund-investor mapping status: {str(e)}")
            
        finally:
            session.close()

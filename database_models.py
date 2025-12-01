#!/usr/bin/env python3
"""
SQLAlchemy ORM Models for User, Client, and Role Management System
"""

from sqlalchemy import create_engine, Column, Integer, BigInteger, String, Boolean, DateTime, ForeignKey, UniqueConstraint, Text, text, CheckConstraint, Numeric, Date, Table, JSON
from sqlalchemy.orm import foreign, remote, relationship
from sqlalchemy.dialects.postgresql import BIT, UUID
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, declarative_mixin
from sqlalchemy.sql import func
from datetime import datetime
from typing import Optional, List, Dict, Any, TYPE_CHECKING, TypeVar, Type, Any, ClassVar, Union
import os
import sys
import logging
import uuid
from urllib.parse import quote_plus
from dotenv import load_dotenv

# Create base class for declarative models
Base = declarative_base()

# For type hints with circular imports
if TYPE_CHECKING:
    from models.permission_models import RoleOrClientBasedModuleLevelPermission, Master

# Type variable for forward references
_T = TypeVar('_T', bound='Base')

# This will be set after all models are defined
RoleOrClientBasedModuleLevelPermission: Any = None  # Will be set after class definitions

# Try to import psycopg2, fallback to pg8000 if not available
try:
    import psycopg2
    PSYCOPG2_AVAILABLE = True
except ImportError:
    PSYCOPG2_AVAILABLE = False
    print("Warning: psycopg2 not available, will use pg8000 for PostgreSQL connections")

# Import schema management utilities
from utils.schema_manager import SchemaManager, ensure_schema_context

# Import permission models after all base models are defined
from models.permission_models import RoleOrClientBasedModuleLevelPermission as PermissionModel, Master

# Set the global reference after all models are defined
RoleOrClientBasedModuleLevelPermission = PermissionModel

# Export Master for convenience
__all__ = ['Master', 'RoleOrClientBasedModuleLevelPermission', 'FileQueue', 'DatabaseManager']

# Load environment variables
load_dotenv()
print("DB_HOST from .env:", os.getenv("DB_HOST"))

# Setup logging
logger = logging.getLogger(__name__)

class User(Base):
    """User model for authentication and user management"""
    __tablename__ = 'users'
    __table_args__ = {'schema': os.getenv('DB_SCHEMA', 'public')}
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    username = Column(String(50), unique=True, nullable=False, index=True)
    email = Column(String(100), unique=True, nullable=True, index=True)
    display_name = Column(String(100), nullable=False)
    first_name = Column(String(50), nullable=False)
    last_name = Column(String(50), nullable=False)
    job_title = Column(String(100), nullable=True)
    password_hash = Column(String(255), nullable=False)
    temp_password = Column(Boolean, default=True, nullable=False)
    role_id = Column(Integer, ForeignKey(f'{os.getenv("DB_SCHEMA", "public")}.roles.id'), nullable=False, index=True)
    client_id = Column(Integer, ForeignKey(f'{os.getenv("DB_SCHEMA", "public")}.clients.id', ondelete='SET NULL'), nullable=True, index=True)
    is_active = Column(Boolean, default=True, index=True)
    created_at = Column(DateTime, default=func.now())
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now())
    
    # Relationships
    role = relationship("Role", back_populates="users")
    client = relationship("Client", back_populates="users")
    
    def __repr__(self):
        return f"<User(username='{self.username}', email='{self.email}', display_name='{self.display_name}', role_id={self.role_id})>"
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert user to dictionary"""
        return {
            'id': self.id,
            'username': self.username,
            'email': self.email,
            'display_name': self.display_name,
            'first_name': self.first_name,
            'last_name': self.last_name,
            'job_title': self.job_title,
            'temp_password': self.temp_password,
            'role_id': self.role_id,
            'role': self.role.to_dict() if self.role else None,
            'client_id': self.client_id,
            'client': self.client.to_dict() if self.client else None,
            'is_active': self.is_active,
            'created_at': self.created_at.isoformat() if self.created_at else None,
            'updated_at': self.updated_at.isoformat() if self.updated_at else None
        }

class Client(Base):
    """Client model for client organizations"""
    __tablename__ = 'clients'
    __table_args__ = {'schema': os.getenv('DB_SCHEMA', 'public')}
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(100), unique=True, nullable=False, index=True)
    code = Column(String(50), unique=True, nullable=False, index=True)
    description = Column(Text)
    type = Column(String(200), nullable=True, index=True)  # e.g., Institutional Investor, Service Provider, Fund Manager, etc.
    
    # Contact Details
    contact_title = Column(String(20), nullable=True)  # Mr., Ms., Dr., etc.
    contact_first_name = Column(String(100), nullable=True)
    contact_last_name = Column(String(100), nullable=True)
    contact_email = Column(String(100), nullable=True, index=True)
    contact_number = Column(String(30), nullable=True)
    
    # Admin Details  
    admin_title = Column(String(20), nullable=True)  # Mr., Ms., Dr., etc.
    admin_first_name = Column(String(100), nullable=True)
    admin_last_name = Column(String(100), nullable=True)
    admin_email = Column(String(100), nullable=True, index=True)
    admin_job_title = Column(String(100), nullable=True)  # Administrator, Manager, etc.
    
    is_active = Column(Boolean, default=True, index=True)
    created_at = Column(DateTime, default=func.now())
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now())
    
    # Relationships
    users = relationship("User", back_populates="client")
    funds = relationship("Fund", secondary=f"{os.getenv('DB_SCHEMA', 'public')}.client_funds", back_populates="clients")
    products = relationship("Product", secondary=f"{os.getenv('DB_SCHEMA', 'public')}.client_products", back_populates="clients")
    client_permissions = relationship(
        "RoleOrClientBasedModuleLevelPermission",
        primaryjoin="and_(Client.id==foreign(remote(RoleOrClientBasedModuleLevelPermission.client_id)), "
                    "RoleOrClientBasedModuleLevelPermission.role_id==None)",
        viewonly=True,
        lazy="selectin"
    )
    
    def __repr__(self):
        return f"<Client(name='{self.name}', code='{self.code}')>"
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert client to dictionary"""
        return {
            'id': self.id,
            'client_name': self.name,  # Map to API field name
            'client_code': self.code,  # Map to API field name
            'description': self.description,
            'client_type': self.type,  # Map to API field name
            
            # Contact Details
            'contact_title': self.contact_title,
            'contact_first_name': self.contact_first_name,
            'contact_last_name': self.contact_last_name,
            'contact_email': self.contact_email,
            'contact_number': self.contact_number,
            
            # Admin Details
            'admin_title': self.admin_title,
            'admin_first_name': self.admin_first_name,
            'admin_last_name': self.admin_last_name,
            'admin_email': self.admin_email,
            'admin_job_title': self.admin_job_title,
            
            'is_active': self.is_active,
            'created_at': self.created_at.isoformat() if self.created_at else None,
            'updated_at': self.updated_at.isoformat() if self.updated_at else None
        }

class Fund(Base):
    """Fund model representing investment funds"""
    __tablename__ = 'funds'
    __table_args__ = {'schema': os.getenv('DB_SCHEMA', 'public')}
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(150), unique=True, nullable=False, index=True)
    code = Column(String(80), unique=True, nullable=False, index=True)
    description = Column(Text)
    type = Column(String(50))
    fund_manager = Column(Text)
    base_currency = Column(String(10))
    fund_admin = Column(JSON)
    shadow = Column(JSON)
    contact_person = Column(String(100))
    contact_email = Column(String(100))
    contact_number = Column(String(30))
    sector = Column(String(100))
    geography = Column(String(100))
    strategy = Column(JSON)
    market_cap = Column(String(50))
    benchmark = Column(JSON)
    fund_metadata = Column('metadata', JSON)
    
    # Lifecycle fields
    stage = Column(String(50), nullable=True, index=True)  # Fund stage (Launch, Active, Winding Down, etc.)
    inception_date = Column(Date, nullable=True)  # When the fund was established
    investment_start_date = Column(Date, nullable=True)  # When investments began
    commitment_subscription = Column(Numeric(18, 2), nullable=True)  # Commitment/subscription amount
    
    is_active = Column(Boolean, default=True, index=True)
    created_at = Column(DateTime, default=func.now())
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now())
    
    # Relationships
    clients = relationship("Client", secondary=f"{os.getenv('DB_SCHEMA', 'public')}.client_funds", back_populates="funds")
    investors = relationship("FundInvestor", back_populates="fund", cascade="all, delete-orphan")
    
    def __repr__(self):
        return f"<Fund(name='{self.name}', code='{self.code}')>"
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'id': self.id,
            'fund_name': self.name,  # Map to API field name
            'fund_code': self.code,  # Map to API field name
            'description': self.description,
            'type': self.type,
            'fund_manager': self.fund_manager,
            'base_currency': self.base_currency,
            'fund_admin': self.fund_admin,
            'shadow': self.shadow,
            'contact_person': self.contact_person,
            'contact_email': self.contact_email,
            'contact_number': self.contact_number,
            'sector': self.sector,
            'geography': self.geography,
            'strategy': self.strategy,
            'market_cap': self.market_cap,
            'benchmark': self.benchmark,
            'metadata': self.fund_metadata,
            'stage': self.stage,
            'inception_date': self.inception_date.isoformat() if self.inception_date else None,
            'investment_start_date': self.investment_start_date.isoformat() if self.investment_start_date else None,
            'commitment_subscription': float(self.commitment_subscription) if self.commitment_subscription else None,
            'is_active': self.is_active,
            'created_at': self.created_at.isoformat() if self.created_at else None,
            'updated_at': self.updated_at.isoformat() if self.updated_at else None
        }

# Association table between clients and funds
client_funds = Table(
    'client_funds',
    Base.metadata,
    Column('client_id', Integer, ForeignKey(f'{os.getenv("DB_SCHEMA", "public")}.clients.id', ondelete='CASCADE'), primary_key=True),
    Column('fund_id', Integer, ForeignKey(f'{os.getenv("DB_SCHEMA", "public")}.funds.id', ondelete='CASCADE'), primary_key=True),
    Column('created_at', DateTime, default=func.now()),
    Column('updated_at', DateTime, default=func.now(), onupdate=func.now()),
    schema=os.getenv('DB_SCHEMA', 'public')
)

class Product(Base):
    """Product model representing platform products like Frame, Validus, etc."""
    __tablename__ = 'products'
    __table_args__ = {'schema': os.getenv('DB_SCHEMA', 'public')}

    id = Column(Integer, primary_key=True, autoincrement=True)
    product_name = Column(String(100), unique=True, nullable=False, index=True)
    product_code = Column(String(50), unique=True, nullable=False, index=True)
    description = Column(Text)
    is_active = Column(Boolean, default=True, index=True)
    created_at = Column(DateTime, default=func.now())
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now())

    # Relationships
    clients = relationship("Client", secondary=f"{os.getenv('DB_SCHEMA', 'public')}.client_products", back_populates="products")

    def __repr__(self):
        return f"<Product(product_name='{self.product_name}', product_code='{self.product_code}')>"

    def to_dict(self) -> Dict[str, Any]:
        return {
            'id': self.id,
            'product_name': self.product_name,
            'product_code': self.product_code,
            'description': self.description,
            'is_active': self.is_active,
            'created_at': self.created_at.isoformat() if self.created_at else None,
            'updated_at': self.updated_at.isoformat() if self.updated_at else None
        }

# Association table between clients and products
client_products = Table(
    'client_products',
    Base.metadata,
    Column('client_id', Integer, ForeignKey(f'{os.getenv("DB_SCHEMA", "public")}.clients.id', ondelete='CASCADE'), primary_key=True),
    Column('product_id', Integer, ForeignKey(f'{os.getenv("DB_SCHEMA", "public")}.products.id', ondelete='CASCADE'), primary_key=True),
    Column('created_at', DateTime, default=func.now()),
    Column('updated_at', DateTime, default=func.now(), onupdate=func.now()),
    schema=os.getenv('DB_SCHEMA', 'public')
)

class Role(Base):
    """Role model for user roles"""
    __tablename__ = 'roles'
    __table_args__ = {'schema': os.getenv('DB_SCHEMA', 'public')}
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    role_name = Column(String(50), unique=True, nullable=False, index=True)
    role_code = Column(String(50), unique=True, nullable=False, index=True)
    description = Column(Text)
    is_active = Column(Boolean, default=True, index=True)
    created_at = Column(DateTime, default=func.now())
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now())
    
    # Relationships
    users = relationship("User", back_populates="role")
    role_permissions = relationship(
        "RoleOrClientBasedModuleLevelPermission",
        primaryjoin="and_(Role.id==foreign(remote(RoleOrClientBasedModuleLevelPermission.role_id)), "
                    "RoleOrClientBasedModuleLevelPermission.client_id==None)",
        viewonly=True,
        lazy="selectin"
    )
    
    def __repr__(self):
        return f"<Role(role_name='{self.role_name}', role_code='{self.role_code}')>"
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert role to dictionary"""
        return {
            'id': self.id,
            'role_name': self.role_name,
            'role_code': self.role_code,
            'description': self.description,
            'is_active': self.is_active,
            'created_at': self.created_at.isoformat() if self.created_at else None,
            'updated_at': self.updated_at.isoformat() if self.updated_at else None
        }

class Module(Base):
    """Module model for system modules with hierarchical support"""
    __tablename__ = 'modules'
    __table_args__ = {'schema': os.getenv('DB_SCHEMA', 'public')}
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    module_name = Column(String(50), unique=True, nullable=False, index=True)
    module_code = Column(String(50), unique=True, nullable=False, index=True)
    description = Column(Text)
    parent_id = Column(Integer, ForeignKey(f'{os.getenv("DB_SCHEMA", "public")}.modules.id', ondelete='CASCADE'), nullable=True, index=True)
    level = Column(Integer, nullable=False, default=0, index=True)
    sort_order = Column(Integer, nullable=False, default=1, index=True)
    is_active = Column(Boolean, default=True, index=True)
    created_at = Column(DateTime, default=func.now())
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now())
    
    # Relationships
    permissions = relationship(
        "RoleOrClientBasedModuleLevelPermission",
        primaryjoin="and_(Module.id==foreign(remote(RoleOrClientBasedModuleLevelPermission.module_id)), "
                    "RoleOrClientBasedModuleLevelPermission.master_id==None)",
        viewonly=True,
        lazy="selectin"
    )
    
    # Hierarchical relationships
    parent = relationship("Module", remote_side=[id], backref="children")
    
    def __repr__(self):
        return f"<Module(module_name='{self.module_name}', module_code='{self.module_code}')>"
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert module to dictionary"""
        return {
            'id': self.id,
            'module_name': self.module_name,
            'module_code': self.module_code,
            'description': self.description,
            'parent_id': self.parent_id,
            'level': self.level,
            'sort_order': self.sort_order,
            'is_active': self.is_active,
            'created_at': self.created_at.isoformat() if self.created_at else None,
            'updated_at': self.updated_at.isoformat() if self.updated_at else None
        }

class Permission(Base):
    """Permission model for available permissions"""
    __tablename__ = 'permissions'
    __table_args__ = {'schema': os.getenv('DB_SCHEMA', 'public')}
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    permission_name = Column(String(50), nullable=False)
    permission_code = Column(String(50), unique=True, nullable=False, index=True)
    description = Column(Text)
    is_active = Column(Boolean, default=True, index=True)
    created_at = Column(DateTime, default=func.now())
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now())
    
    # Relationships
    role_permissions = relationship(
        "RoleOrClientBasedModuleLevelPermission",
        primaryjoin="Permission.id==foreign(remote(RoleOrClientBasedModuleLevelPermission.permission_id))",
        viewonly=True,
        lazy="selectin"
    )
    
    def __repr__(self):
        return f"<Permission(permission_name='{self.permission_name}', permission_code='{self.permission_code}')>"
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert permission to dictionary"""
        return {
            'id': self.id,
            'permission_name': self.permission_name,
            'permission_code': self.permission_code,
            'description': self.description,
            'is_active': self.is_active,
            'created_at': self.created_at.isoformat() if self.created_at else None,
            'updated_at': self.updated_at.isoformat() if self.updated_at else None
        }


class KpiLibrary(Base):
    """KPI Library model for validation parameters"""
    __tablename__ = 'kpi_library'
    __table_args__ = {'schema': 'nexbridge'}
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    kpi_code = Column(String(100), unique=True, nullable=False, index=True)
    kpi_name = Column(String(200), nullable=False)
    kpi_type = Column(String(50), nullable=False, index=True)  # 'NAV_VALIDATION' or 'RATIO_VALIDATION'
    category = Column(String(100), index=True)  # e.g., 'Financial', 'Liquidity', 'Concentration'
    description = Column(Text)
    
    # Common validation fields
    source_type = Column(String(50), nullable=False)  # 'SINGLE_SOURCE' or 'DUAL_SOURCE'
    precision_type = Column(String(50), nullable=False)  # 'PERCENTAGE' or 'ABSOLUTE'
    
    # Ratio-specific fields (mandatory for RATIO_VALIDATION)
    numerator_field = Column(String(200))
    denominator_field = Column(String(200))
    numerator_description = Column(Text)
    denominator_description = Column(Text)
    
    # Metadata
    is_active = Column(Boolean, default=True, index=True)
    created_at = Column(DateTime, default=func.now())
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now())
    created_by = Column(String(100))
    updated_by = Column(String(100))  # Track who last updated
    
    # Relationships
    thresholds = relationship("KpiThreshold", back_populates="kpi", cascade="all, delete-orphan")
    
    # Constraints
    __table_args__ = (
        CheckConstraint("kpi_type IN ('NAV_VALIDATION', 'RATIO_VALIDATION')", name='chk_kpi_type'),
        CheckConstraint("source_type IN ('SINGLE_SOURCE', 'DUAL_SOURCE')", name='chk_source_type'),
        CheckConstraint("precision_type IN ('PERCENTAGE', 'ABSOLUTE')", name='chk_precision_type'),
        # Requirement #2: Make numerator/denominator mandatory for ratio validations
        CheckConstraint(
            "kpi_type != 'RATIO_VALIDATION' OR (numerator_field IS NOT NULL AND denominator_field IS NOT NULL)",
            name='chk_ratio_fields_required'
        ),
        # NEW: Compound unique constraint for (kpi_code, kpi_name, kpi_type, category, source_type)
        UniqueConstraint('kpi_code', 'kpi_name', 'kpi_type', 'category', 'source_type', name='uq_kpi_combination'),
        {'schema': 'nexbridge'}
    )
    
    def __repr__(self):
        return f"<KpiLibrary(kpi_code='{self.kpi_code}', kpi_name='{self.kpi_name}', kpi_type='{self.kpi_type}')>"
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert KPI to dictionary"""
        return {
            'id': self.id,
            'kpi_code': self.kpi_code,
            'kpi_name': self.kpi_name,
            'kpi_type': self.kpi_type,
            'category': self.category,
            'description': self.description,
            'source_type': self.source_type,
            'precision_type': self.precision_type,
            'numerator_field': self.numerator_field,
            'denominator_field': self.denominator_field,
            'numerator_description': self.numerator_description,
            'denominator_description': self.denominator_description,
            'is_active': self.is_active,
            'created_at': self.created_at.isoformat() if self.created_at else None,
            'updated_at': self.updated_at.isoformat() if self.updated_at else None,
            'created_by': self.created_by,
            'updated_by': self.updated_by  # Added updated_by tracking
        }

class KpiThreshold(Base):
    """KPI Threshold values for validation parameters"""
    __tablename__ = 'kpi_thresholds'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    kpi_id = Column(Integer, ForeignKey('nexbridge.kpi_library.id', ondelete='CASCADE'), nullable=False, index=True)
    fund_id = Column(String(100), index=True)  # Optional: fund-specific thresholds
    threshold_value = Column(Numeric(18, 6), nullable=False)
    
    # Metadata
    is_active = Column(Boolean, default=True, index=True)
    created_at = Column(DateTime, default=func.now())
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now())
    created_by = Column(String(100))
    updated_by = Column(String(100))  # Track who last updated
    
    # Relationships
    kpi = relationship("KpiLibrary", back_populates="thresholds")
    
    # Unique constraint to prevent duplicate thresholds for same KPI/fund combination
    __table_args__ = (
        # UniqueConstraint('kpi_id', 'fund_id', 'is_active', name='uq_kpi_fund_active_threshold'),
        UniqueConstraint('kpi_id', 'fund_id', name='uq_kpi_fund_threshold'),
        {'schema': 'nexbridge'}
    )
    
    def __repr__(self):
        return f"<KpiThreshold(kpi_id={self.kpi_id}, fund_id='{self.fund_id}', threshold_value={self.threshold_value})>"
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert threshold to dictionary"""
        return {
            'id': self.id,
            'kpi_id': self.kpi_id,
            'fund_id': self.fund_id,
            'threshold_value': float(self.threshold_value) if self.threshold_value else None,
            'is_active': self.is_active,
            'created_at': self.created_at.isoformat() if self.created_at else None,
            'updated_at': self.updated_at.isoformat() if self.updated_at else None,
            'created_by': self.created_by,
            'updated_by': self.updated_by,  # Added updated_by tracking
            'kpi': self.kpi.to_dict() if self.kpi else None
        }

class Calendar(Base):
    """Calendar model for fund publishing schedules and document frequencies"""
    __tablename__ = 'calendars'
    __table_args__ = {'schema': os.getenv('DB_SCHEMA', 'public')}
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    fund_id = Column(Integer, ForeignKey(f'{os.getenv("DB_SCHEMA", "public")}.funds.id', ondelete='CASCADE'), nullable=False, index=True)
    frequency = Column(String(50), nullable=False, index=True)  # monthly, quarterly, etc.
    delay = Column(Integer, nullable=False)  # delay in days
    documents = Column(JSON, nullable=True)  # JSON array of document configurations (optional)
    created_by = Column(String(255), nullable=True)
    updated_by = Column(String(255), nullable=True)
    created_at = Column(DateTime, default=func.now())
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now())
    is_active = Column(Boolean, default=True, index=True)
    
    # Relationships
    fund = relationship("Fund", backref="calendars")
    
    # Constraints
    __table_args__ = (
        CheckConstraint("frequency IN ('daily', 'weekly', 'monthly', 'quarterly', 'annually')", name='chk_frequency'),
        CheckConstraint("delay >= 0", name='chk_delay_positive'),
        {'schema': os.getenv('DB_SCHEMA', 'public')}
    )
    
    def __repr__(self):
        return f"<Calendar(id={self.id}, fund_id={self.fund_id}, frequency='{self.frequency}')>"
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert calendar to dictionary"""
        return {
            'id': self.id,
            'fund_id': self.fund_id,
            'frequency': self.frequency,
            'delay': self.delay,
            'documents': self.documents,
            'created_by': self.created_by,
            'updated_by': self.updated_by,
            'created_at': self.created_at.isoformat() if self.created_at else None,
            'updated_at': self.updated_at.isoformat() if self.updated_at else None,
            'is_active': self.is_active
        }

class DataSource(Base):
    """Data source model for public schema with comprehensive source information"""
    __tablename__ = 'data_sources'
    __table_args__ = {'schema': os.getenv('DB_SCHEMA', 'public')}
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    fund_id = Column(Integer, ForeignKey(f'{os.getenv("DB_SCHEMA", "public")}.funds.id', ondelete='CASCADE'), nullable=True, index=True)
    name = Column(String(255), nullable=False, index=True)  # Unique per fund for active records only (partial index on fund_id, name WHERE is_active)
    source = Column(String(50), nullable=False, index=True)  # Email, S3 Bucket, Portal, API, SFTP
    holiday_calendar = Column(String(20), nullable=False, index=True)  # US, Europe
    source_details = Column(JSON, nullable=True)  # JSON for flexible source-specific configuration
    document_for = Column(JSON, nullable=True)  # JSON field for document associations
    additional_details = Column(Text, nullable=True)  # Long text for additional information
    is_active = Column(Boolean, default=True, index=True)
    created_at = Column(DateTime, default=func.now())
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now())
    created_by = Column(String(255), nullable=False)
    updated_by = Column(String(255), nullable=True)
    
    # Constraints
    __table_args__ = (
        CheckConstraint("source IN ('Email', 'S3 Bucket', 'Portal', 'API', 'SFTP')", name='chk_source_type'),
        CheckConstraint("holiday_calendar IN ('US', 'Europe')", name='chk_holiday_calendar'),
        {'schema': os.getenv('DB_SCHEMA', 'public')}
    )
    
    def __repr__(self):
        return f"<DataSource(id={self.id}, name='{self.name}', source='{self.source}')>"
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert data source to dictionary"""
        return {
            'id': self.id,
            'fund_id': self.fund_id,
            'name': self.name,
            'source': self.source,
            'holiday_calendar': self.holiday_calendar,
            'source_details': self.source_details,
            'document_for': self.document_for,
            'additional_details': self.additional_details,
            'is_active': self.is_active,
            'created_at': self.created_at.isoformat() if self.created_at else None,
            'updated_at': self.updated_at.isoformat() if self.updated_at else None,
            'created_by': self.created_by,
            'updated_by': self.updated_by
        }

class Source(Base):
    """Source model for data sources (nexbridge schema - legacy)"""
    __tablename__ = 'source'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(255), unique=True, nullable=False, index=True)
    created_at = Column(DateTime, default=func.now())
    created_by = Column(String(255), nullable=False)
    
    # Relationships
    nav_packs = relationship("NavPack", back_populates="source", cascade="all, delete-orphan")
    
    __table_args__ = {'schema': 'nexbridge'}
    
    def __repr__(self):
        return f"<Source(id={self.id}, name='{self.name}')>"
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert source to dictionary"""
        return {
            'id': self.id,
            'name': self.name,
            'created_at': self.created_at.isoformat() if self.created_at else None,
            'created_by': self.created_by
        }

class NavPack(Base):
    """Nav Pack model for logical grouping of fund/source/date"""
    __tablename__ = 'nav_pack'
    
    navpack_id = Column(Integer, primary_key=True, autoincrement=True)
    fund_id = Column(Integer, nullable=False, index=True)
    source_id = Column(Integer, ForeignKey('nexbridge.source.id'), nullable=False, index=True)
    file_date = Column(Date, nullable=False, index=True)
    
    # Relationships
    source = relationship("Source", back_populates="nav_packs")
    versions = relationship("NavPackVersion", back_populates="nav_pack", cascade="all, delete-orphan")
    
    __table_args__ = (
        UniqueConstraint('fund_id', 'source_id', 'file_date', name='uq_fund_source_date'),
        {'schema': 'nexbridge'}
    )
    
    def __repr__(self):
        return f"<NavPack(navpack_id={self.navpack_id}, fund_id={self.fund_id}, source_id={self.source_id}, file_date={self.file_date})>"
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert nav pack to dictionary"""
        return {
            'navpack_id': self.navpack_id,
            'fund_id': self.fund_id,
            'source_id': self.source_id,
            'file_date': self.file_date.isoformat() if self.file_date else None,
            'source': self.source.to_dict() if self.source else None
        }

class NavPackVersion(Base):
    """Nav Pack Version model for file and override management"""
    __tablename__ = 'navpack_version'
    
    navpack_version_id = Column(Integer, primary_key=True, autoincrement=True)
    navpack_id = Column(Integer, ForeignKey('nexbridge.nav_pack.navpack_id'), nullable=False, index=True)
    version = Column(Integer, nullable=False, index=True)
    file_name = Column(String(255), nullable=False)
    uploaded_by = Column(String(255), nullable=False)
    uploaded_on = Column(DateTime, default=func.now())
    override_on = Column(DateTime)
    override_by = Column(String(255))
    base_version = Column(Integer, ForeignKey('nexbridge.navpack_version.navpack_version_id'))
    
    # Relationships
    nav_pack = relationship("NavPack", back_populates="versions")
    trial_balance_entries = relationship("TrialBalance", back_populates="navpack_version", cascade="all, delete-orphan")
    portfolio_valuations = relationship("PortfolioValuation", back_populates="navpack_version", cascade="all, delete-orphan")
    dividends = relationship("Dividend", back_populates="navpack_version", cascade="all, delete-orphan")
    base_version_ref = relationship("NavPackVersion", remote_side="NavPackVersion.navpack_version_id")
    
    __table_args__ = (
        UniqueConstraint('navpack_id', 'version', name='uq_navpack_version'),
        CheckConstraint('version > 0', name='chk_positive_version'),
        {'schema': 'nexbridge'}
    )
    
    def __repr__(self):
        return f"<NavPackVersion(navpack_version_id={self.navpack_version_id}, navpack_id={self.navpack_id}, version={self.version}, file_name='{self.file_name}')>"
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert nav pack version to dictionary"""
        return {
            'navpack_version_id': self.navpack_version_id,
            'navpack_id': self.navpack_id,
            'version': self.version,
            'file_name': self.file_name,
            'uploaded_by': self.uploaded_by,
            'uploaded_on': self.uploaded_on.isoformat() if self.uploaded_on else None,
            'override_on': self.override_on.isoformat() if self.override_on else None,
            'override_by': self.override_by,
            'base_version': self.base_version,
            'nav_pack': self.nav_pack.to_dict() if self.nav_pack else None
        }

class TrialBalance(Base):
    """Trial Balance model for pure data storage"""
    __tablename__ = 'trial_balance'
    
    row_id = Column(Integer, primary_key=True, autoincrement=True)
    type = Column(String(255), nullable=False, index=True)
    category = Column(String(255), nullable=True, index=True)  # Made nullable
    accounting_head = Column(String(255), nullable=True, index=True)  # Made nullable
    financial_account = Column(String(255), nullable=False, index=True)
    ending_balance = Column(Numeric(15, 2), nullable=True)  # Made nullable
    extra_data = Column(Text, nullable=True)  # JSON data for additional information
    navpack_version_id = Column(Integer, ForeignKey('nexbridge.navpack_version.navpack_version_id'), nullable=False, index=True)
    
    # Relationships
    navpack_version = relationship("NavPackVersion", back_populates="trial_balance_entries")
    
    __table_args__ = {'schema': 'nexbridge'}
    
    def __repr__(self):
        return f"<TrialBalance(row_id={self.row_id}, type='{self.type}', category='{self.category}', accounting_head='{self.accounting_head}', ending_balance={self.ending_balance})>"
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert trial balance to dictionary"""
        return {
            'row_id': self.row_id,
            'type': self.type,
            'category': self.category,
            'accounting_head': self.accounting_head,
            'financial_account': self.financial_account,
            'ending_balance': float(self.ending_balance) if self.ending_balance else None,
            'navpack_version_id': self.navpack_version_id
        }

class PortfolioValuation(Base):
    """Portfolio Valuation by Instrument model"""
    __tablename__ = 'portfolio_valuation'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    inv_type = Column(String(255), nullable=False, index=True)
    inv_id = Column(String(255), nullable=False, index=True)
    end_qty = Column(Numeric(18, 6), nullable=False)
    end_local_market_price = Column(Numeric(18, 6), nullable=True)  # Made nullable
    end_local_mv = Column(Numeric(18, 2), nullable=True)  # Made nullable
    end_book_mv = Column(Numeric(18, 2), nullable=True)  # End Book Market Value
    navpack_version_id = Column(Integer, ForeignKey('nexbridge.navpack_version.navpack_version_id'), nullable=False, index=True)
    extra_data = Column(Text, nullable=True)  # JSON data for additional information (Inv Desc)
    
    # Relationships
    navpack_version = relationship("NavPackVersion", back_populates="portfolio_valuations")
    
    __table_args__ = {'schema': 'nexbridge'}
    
    def __repr__(self):
        return f"<PortfolioValuation(id={self.id}, inv_type='{self.inv_type}', inv_id='{self.inv_id}', end_local_mv={self.end_local_mv})>"
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert portfolio valuation to dictionary"""
        return {
            'id': self.id,
            'inv_type': self.inv_type,
            'inv_id': self.inv_id,
            'end_qty': float(self.end_qty) if self.end_qty else None,
            'end_local_market_price': float(self.end_local_market_price) if self.end_local_market_price else None,
            'end_local_mv': float(self.end_local_mv) if self.end_local_mv else None,
            'end_book_mv': float(self.end_book_mv) if self.end_book_mv else None,
            'navpack_version_id': self.navpack_version_id
        }

class Dividend(Base):
    """Dividend model for dividend payments"""
    __tablename__ = 'dividend'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    security_id = Column(String(255), nullable=False, index=True)
    security_name = Column(String(255), nullable=False, index=True)
    amount = Column(Numeric(18, 2), nullable=False)
    navpack_version_id = Column(Integer, ForeignKey('nexbridge.navpack_version.navpack_version_id'), nullable=False, index=True)
    extra_data = Column(Text, nullable=True)  # JSON data for future use
    
    # Relationships
    navpack_version = relationship("NavPackVersion", back_populates="dividends")
    
    __table_args__ = {'schema': 'nexbridge'}
    
    def __repr__(self):
        return f"<Dividend(id={self.id}, security_id='{self.security_id}', security_name='{self.security_name}', amount={self.amount})>"
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert dividend to dictionary"""
        return {
            'id': self.id,
            'security_id': self.security_id,
            'security_name': self.security_name,
            'amount': float(self.amount) if self.amount else None,
            'navpack_version_id': self.navpack_version_id
        }

class FundManager(Base):
    """Fund Manager model for managing fund manager entities"""
    __tablename__ = 'fund_manager'
    __table_args__ = {'schema': os.getenv('DB_SCHEMA', 'public')}

    id = Column(Integer, primary_key=True, autoincrement=True)
    fund_manager_name = Column(String(255), unique=True, nullable=False, index=True)
    contact_title = Column(String(100), nullable=True)
    contact_first_name = Column(String(100), nullable=False)
    contact_last_name = Column(String(100), nullable=False)
    contact_email = Column(String(255), unique=True, nullable=False, index=True)
    contact_number = Column(String(50), nullable=True)
    status = Column(String(20), nullable=False, default='active')
    created_at = Column(DateTime, nullable=True, server_default=func.current_timestamp())
    updated_at = Column(DateTime, nullable=True, server_default=func.current_timestamp(), onupdate=func.current_timestamp())

    def to_dict(self):
        """Convert FundManager instance to dictionary"""
        return {
            'id': self.id,
            'fund_manager_name': self.fund_manager_name,
            'contact_title': self.contact_title,
            'contact_first_name': self.contact_first_name,
            'contact_last_name': self.contact_last_name,
            'contact_email': self.contact_email,
            'contact_number': self.contact_number,
            'status': self.status,
            'created_at': self.created_at.isoformat() if self.created_at else None,
            'updated_at': self.updated_at.isoformat() if self.updated_at else None
        }

class Benchmark(Base):
    """Benchmark data model for storing market indices and benchmark values"""
    __tablename__ = 'benchmarks'
    __table_args__ = {'schema': 'public'}
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    benchmark = Column(String(100), nullable=False, comment='Name of the benchmark (e.g., S&P 500 Index)')
    date = Column(Date, nullable=False, comment='Date of the benchmark value')
    value = Column(Numeric(15, 4), nullable=False, comment='Benchmark value on the given date')
    extra_data = Column(JSON, nullable=True, comment='Additional metadata about the benchmark')
    created_at = Column(DateTime, default=func.now(), nullable=False)
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now(), nullable=False)
    
    # Create unique constraint on benchmark name and date
    __table_args__ = (
        UniqueConstraint('benchmark', 'date', name='uq_benchmark_date'),
        {'schema': 'public'}
    )
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert Benchmark instance to dictionary"""
        return {
            'id': self.id,
            'benchmark': self.benchmark,
            'date': self.date.isoformat() if self.date else None,
            'value': float(self.value) if self.value else None,
            'extra_data': self.extra_data,
            'created_at': self.created_at.isoformat() if self.created_at else None,
            'updated_at': self.updated_at.isoformat() if self.updated_at else None
        }

class FileQueue(Base):
    """File Queue model for managing file processing queue in FIFO order"""
    __tablename__ = 'file_queue'
    __table_args__ = {'schema': 'public'}
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    filename = Column(String(255), nullable=False, index=True, comment='Name of the file')
    file_path = Column(String(500), nullable=False, comment='Full path to the file')
    file_hash = Column(String(64), nullable=True, index=True, comment='Hash of the file for deduplication')
    folder = Column(String(50), nullable=False, server_default='l0', comment='Target folder (e.g., l0, l1)')
    storage_type = Column(String(50), nullable=False, server_default='local', comment='Storage type (local, s3, etc.)')
    source = Column(String(50), nullable=False, server_default='api', comment='Source of upload (api, sftp, etc.)')
    file_classification = Column(String(100), nullable=True, comment='File classification or type')
    username = Column(String(50), nullable=False, index=True, comment='Username who uploaded the file')
    status = Column(String(20), nullable=False, server_default='pending', index=True, comment='Status: pending, processing, completed, failed')
    error_message = Column(Text, nullable=True, comment='Error message if processing failed')
    created_at = Column(DateTime, nullable=False, server_default=text('now()'), index=True, comment='When the file was added to queue')
    updated_at = Column(DateTime, nullable=False, server_default=text('now()'), comment='When the record was last updated')
    started_at = Column(DateTime, nullable=True, comment='When processing started')
    completed_at = Column(DateTime, nullable=True, comment='When processing completed')
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert FileQueue instance to dictionary"""
        return {
            'id': self.id,
            'filename': self.filename,
            'file_path': self.file_path,
            'file_hash': self.file_hash,
            'folder': self.folder,
            'storage_type': self.storage_type,
            'source': self.source,
            'file_classification': self.file_classification,
            'username': self.username,
            'status': self.status,
            'created_at': self.created_at.isoformat() if self.created_at else None,
            'updated_at': self.updated_at.isoformat() if self.updated_at else None,
            'started_at': self.started_at.isoformat() if self.started_at else None,
            'completed_at': self.completed_at.isoformat() if self.completed_at else None,
            'error_message': self.error_message
        }

class DatabaseManager:
    """Database manager using SQLAlchemy ORM with singleton pattern"""
    
    _instance = None
    _engine = None
    _session_local = None
    _db_type = None
    _initialized = False
    
    def __new__(cls, db_type: str = None, connection_params: Optional[Dict[str, Any]] = None):
        """Singleton pattern to reuse the same database connection"""
        if cls._instance is None:
            cls._instance = super(DatabaseManager, cls).__new__(cls)
        return cls._instance
    
    def __init__(self, db_type: str = None, connection_params: Optional[Dict[str, Any]] = None):
        """
        Initialize database manager with automatic fallback (singleton)
        
        Args:
            db_type: Type of database ('postgresql' or 'sqlite') - auto-detected if None
            connection_params: Database connection parameters
        """
        # Only initialize once
        if self._initialized:
            return
            
        # Try to determine database type from environment
        env_db_type = os.getenv('DB_TYPE', 'postgresql').lower()
        self.db_type = (db_type or env_db_type).lower()
        
        self.connection_params = connection_params or self._get_default_connection_params()
        self.engine = None
        self.SessionLocal = None
        self._create_engine_with_fallback()
        
        # Mark as initialized
        DatabaseManager._initialized = True
    
    @classmethod
    def reset_singleton(cls):
        """Reset the singleton instance (useful for testing or reconnection)"""
        cls._instance = None
        cls._engine = None
        cls._session_local = None
        cls._db_type = None
        cls._initialized = False
    
    def _get_default_connection_params(self) -> Dict[str, Any]:
        """Get connection parameters from environment variables (no defaults)"""
        if self.db_type == "postgresql":
            required_vars = ['DB_HOST', 'DB_PORT', 'DB_NAME', 'DB_USER', 'DB_PASSWORD']
            params = {}
            for var in required_vars:
                value = os.getenv(var)
                if value is None:
                    raise ValueError(f"Environment variable '{var}' is required for PostgreSQL connection.")
                # Allow empty password for passwordless authentication
                if var == 'DB_PASSWORD' and value == "":
                    params[var.lower().replace('db_', '')] = ""
                elif value == "" and var != 'DB_PASSWORD':
                    raise ValueError(f"Environment variable '{var}' is required for PostgreSQL connection and must not be empty.")
                else:
                    params[var.lower().replace('db_', '')] = value
            return params
        elif self.db_type == "sqlite":
            db_path = os.getenv('DB_PATH')
            if not db_path:
                raise ValueError("Environment variable 'DB_PATH' is required for SQLite connection and must not be empty.")
            return {'database': db_path}
        else:
            raise ValueError(f"Unsupported database type: {self.db_type}")
    
    def _create_engine_with_fallback(self):
        """Create SQLAlchemy engine with fallback to SQLite"""
        try:
            self._create_engine()
            # Test the connection
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            # Only log if this is the first initialization
            if not DatabaseManager._initialized:
                print(f"Successfully connected to {self.db_type} database")
        except Exception as e:
            if not DatabaseManager._initialized:
                print(f"Failed to connect to {self.db_type} database: {e}")
            if self.db_type == "postgresql":
                if not DatabaseManager._initialized:
                    print("Falling back to SQLite database...")
                self.db_type = "sqlite"
                self.connection_params = self._get_default_connection_params()
                try:
                    self._create_engine()
                    if not DatabaseManager._initialized:
                        print("Successfully connected to SQLite database")
                except Exception as sqlite_error:
                    if not DatabaseManager._initialized:
                        print(f"Failed to connect to SQLite database: {sqlite_error}")
                    raise RuntimeError("Unable to connect to any database")
            else:
                raise e

    def _create_engine(self):
        """Create SQLAlchemy engine with proper URL encoding for special characters"""
        if self.db_type == "postgresql":
            # Always get params from environment to ensure latest values
            user = os.getenv('DB_USER')
            password = os.getenv('DB_PASSWORD')
            host = os.getenv('DB_HOST')
            port = os.getenv('DB_PORT')
            database = os.getenv('DB_NAME')
            
            # Check that all required parameters are present (password can be empty for passwordless auth)
            required_params = [user, host, port, database]
            if not all(param is not None for param in required_params):
                raise ValueError("All PostgreSQL connection parameters (except password) must be set in .env")
            
            # Handle empty password properly - ensure it's an empty string, not None
            password = password or ""
            
            # Get schema from environment or default to 'validus'
            schema = os.getenv('DB_SCHEMA', 'validus')
            
            # Use pg8000 if psycopg2 is not available, otherwise use psycopg2
            if PSYCOPG2_AVAILABLE:
                # For empty passwords, create connection without password in URL to avoid psycopg2 issues
                if password == "":
                    connection_string = f"postgresql+psycopg2://{user}@{host}:{port}/{database}?options=-csearch_path%3D{schema},public"
                else:
                    connection_string = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}?options=-csearch_path%3D{schema},public"
            else:
                # Use pg8000 as fallback
                if password == "":
                    connection_string = f"postgresql+pg8000://{user}@{host}:{port}/{database}?options=-csearch_path%3D{schema},public"
                else:
                    connection_string = f"postgresql+pg8000://{user}:{password}@{host}:{port}/{database}?options=-csearch_path%3D{schema},public"
        elif self.db_type == "sqlite":
            db_path = os.getenv('DB_PATH', 'validus_boxes.db')
            connection_string = f"sqlite:///{db_path}"
        else:
            raise ValueError(f"Unsupported database type: {self.db_type}")

        # Configure engine with better connection handling
        engine_kwargs = {
            'echo': False,
            'pool_size': 5,
            'max_overflow': 10,
            'pool_pre_ping': True,  # Test connections before use
            'pool_recycle': 3600,   # Recycle connections every hour
            'connect_args': {
                'connect_timeout': 10,
                'options': f'-c search_path={schema},public',
                'application_name': 'validus_boxes'
            }
        }
        
        self.engine = create_engine(connection_string, **engine_kwargs)
        self.SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=self.engine)
    
    def create_schema(self, schema_name: str = "public"):
        """Create a schema if it doesn't exist"""
        with self.engine.connect() as connection:
            if self.db_type == "postgresql":
                # Sanitize schema name to be safe for SQL
                safe_schema_name = self._sanitize_schema_name(schema_name)
                connection.execute(text(f"CREATE SCHEMA IF NOT EXISTS {safe_schema_name}"))
                logger.info(f"Created schema: {safe_schema_name}")
            # For SQLite, schemas are not supported, so we skip this
            
    def create_client_schema(self, client_name: str) -> str:
        """Create a dedicated schema for a client"""
        # Convert client name to lowercase and sanitize
        schema_name = self._sanitize_schema_name(client_name.lower())
        
        if self.db_type == "postgresql":
            try:
                with self.engine.connect() as connection:
                    connection.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema_name}"))
                    logger.info(f"Created client schema: {schema_name}")
                return schema_name
            except Exception as e:
                logger.error(f"Failed to create schema {schema_name}: {e}")
                raise e
        else:
            # For SQLite, we can't create schemas, but we return the schema name
            # for consistency (tables can use prefixes instead)
            logger.info(f"SQLite detected: Schema concept simulated with table prefixes for {schema_name}")
            return schema_name
    
    def drop_client_schema(self, client_name: str) -> str:
        """
        Drop a client schema (PostgreSQL only)
        
        Args:
            client_name: The client name to generate schema name from
            
        Returns:
            The dropped schema name
            
        Raises:
            RuntimeError: If schema drop fails
        """
        # Convert client name to lowercase and sanitize
        schema_name = self._sanitize_schema_name(client_name.lower())
        
        if self.db_type == "postgresql":
            try:
                with self.engine.connect() as connection:
                    # Drop schema cascade to remove all objects within it
                    connection.execute(text(f"DROP SCHEMA IF EXISTS {schema_name} CASCADE"))
                    logger.info(f"Dropped client schema: {schema_name}")
                return schema_name
            except Exception as e:
                logger.error(f"Failed to drop schema {schema_name}: {e}")
                raise RuntimeError(f"Failed to drop schema '{schema_name}': {str(e)}")
        else:
            # For SQLite, schemas don't exist, so this is a no-op
            logger.info(f"SQLite detected: No schema to drop for {schema_name}")
            return schema_name
    
    def _sanitize_schema_name(self, name: str) -> str:
        """Sanitize schema name to be database-safe"""
        import re
        # Remove special characters and replace spaces/hyphens with underscores
        sanitized = re.sub(r'[^a-zA-Z0-9_]', '_', name.strip())
        # Ensure it starts with a letter or underscore
        if sanitized and not sanitized[0].isalpha() and sanitized[0] != '_':
            sanitized = f"client_{sanitized}"
        # Ensure it's not empty
        if not sanitized:
            sanitized = "client_default"
        return sanitized.lower()
    
    def create_tables(self):
        """Create all tables"""
        # First create the schema
        self.create_schema()
        # Then create all tables
        Base.metadata.create_all(bind=self.engine)
    
    def create_nexbridge_tables(self):
        """Create nexbridge schema and KPI tables"""
        # First create the nexbridge schema
        self.create_schema('nexbridge')
        # Then create KPI tables specifically
        KpiLibrary.__table__.create(bind=self.engine, checkfirst=True)
        KpiThreshold.__table__.create(bind=self.engine, checkfirst=True)
    
    def get_session(self):
        """Get database session with proper schema context"""
        session = self.SessionLocal()
        if self.db_type == "postgresql":
            # Use SchemaManager to ensure proper schema context
            schema_manager = SchemaManager(session)
            schema_manager.set_search_path()
        return session
    
    def get_session_with_schema(self, schema_name: str = None):
        """Get database session with specific schema context"""
        session = self.SessionLocal()
        if self.db_type == "postgresql" and schema_name:
            # Set search path for this session
            session.execute(text(f"SET search_path TO {schema_name}, public"))          
        return session
    
    def get_user_by_username(self, username: str) -> Optional[User]:
        """Get user by username"""
        session = self.get_session()
        try:
            from sqlalchemy.orm import joinedload
            return session.query(User).options(joinedload(User.role), joinedload(User.client)).filter(User.username == username, User.is_active == True).first()
        finally:
            session.close()
    
    def get_user_by_email(self, email: str) -> Optional[User]:
        """Get user by email address"""
        session = self.get_session()
        try:
            from sqlalchemy.orm import joinedload
            return session.query(User).options(joinedload(User.role), joinedload(User.client)).filter(User.email == email, User.is_active == True).first()
        finally:
            session.close()
    
    def get_user_permissions(self, username: str, module_code: str = None) -> Dict[str, Any]:
        """
        Get user permissions using ORM
        
        Args:
            username: Username to check
            module_code: Optional module code to filter by
            
        Returns:
            Dictionary with user permissions
        """
        session = self.get_session()
        try:
            # Get user with relationships
            user = session.query(User).filter(
                User.username == username, 
                User.is_active == True
            ).first()
            
            if not user:
                return {"success": False, "error": "User not found"}
            
            # Get user's role (direct relationship)
            if not user.role:
                return {"success": False, "error": "User has no assigned role"}
            
            # Initialize permissions dictionary
            all_permissions = {}
            
            # Get role-module-permission relationships (only role-based, not client-based)
            role_permissions = session.query(RoleOrClientBasedModuleLevelPermission).filter(
                RoleOrClientBasedModuleLevelPermission.role_id == user.role_id,
                RoleOrClientBasedModuleLevelPermission.client_id.is_(None),  # Only role-based permissions
                RoleOrClientBasedModuleLevelPermission.is_active == True
            ).all()
            
            for role_perm in role_permissions:
                # Get module and permission details
                module = session.query(Module).filter(
                    Module.id == role_perm.module_id,
                    Module.is_active == True
                ).first()
                
                permission = session.query(Permission).filter(
                    Permission.id == role_perm.permission_id,
                    Permission.is_active == True
                ).first()
                
                if module and permission:
                    if module.module_code not in all_permissions:
                        all_permissions[module.module_code] = {
                            "name": module.module_name,
                            "permissions": []
                        }
                    
                    all_permissions[module.module_code]["permissions"].append({
                        "code": permission.permission_code,
                        "name": permission.permission_name
                    })
            
            # Filter by module if specified
            if module_code:
                if module_code in all_permissions:
                    permissions = all_permissions[module_code]["permissions"]
                else:
                    permissions = []
                
                return {
                    "success": True,
                    "username": username,
                    "permissions": permissions
                }
            
            return {
                "success": True,
                "username": username,
                "permissions": all_permissions
            }
                
        except Exception as e:
            return {"success": False, "error": str(e)}
        finally:
            session.close()
    
    def create_user(self, username: str, display_name: str, password_hash: str, role_code: str, email: str = None) -> Optional[User]:
        """Create a new user"""
        session = self.get_session()
        try:
            # Get role by code
            role = session.query(Role).filter(Role.role_code == role_code).first()
            if not role:
                print(f"Role with code '{role_code}' not found")
                return None
            
            user = User(
                username=username,
                email=email,
                display_name=display_name,
                password_hash=password_hash,
                role_id=role.id
            )
            session.add(user)
            session.commit()
            session.refresh(user)
            return user
        except Exception as e:
            print(f"Error creating user: {e}")
            return None
        finally:
            session.close()
    
    def assign_user_to_client(self, username: str, client_code: str, role_code: str) -> bool:
        """Assign user to client with specific role"""
        session = self.get_session()
        try:
            # Get user
            user = session.query(User).filter(User.username == username).first()
            if not user:
                return False
            
            # Get client
            client = session.query(Client).filter(Client.client_code == client_code).first()
            if not client:
                return False
            
            # Get role
            role = session.query(Role).filter(Role.role_code == role_code).first()
            if not role:
                return False
            
            # Update user's client_id directly
            user.client_id = client.id
            session.commit()
            return True
            
        except Exception as e:
            print(f"Error assigning user to client: {e}")
            return False
        finally:
            session.close()
    
    def get_all_users(self) -> List[User]:
        """Get all active users"""
        session = self.get_session()
        try:
            from sqlalchemy.orm import joinedload
            return session.query(User).options(joinedload(User.role)).filter(User.is_active == True).all()
        finally:
            session.close()
    
    def get_user_summary(self, username: str) -> Dict[str, Any]:
        """Get user summary with clients and roles"""
        session = self.get_session()
        try:
            from sqlalchemy.orm import joinedload
            user = session.query(User).options(joinedload(User.role)).filter(
                User.username == username, 
                User.is_active == True
            ).first()
            
            if not user:
                return {"success": False, "error": "User not found"}
            
            # Get user's client and role (direct relationships)
            clients = []
            roles = []
            
            if user.client:
                clients.append(user.client.name)
            if user.role:
                roles.append(user.role.role_name)
            
            return {
                "success": True,
                "user": user.to_dict(),
                "assigned_clients": list(set(clients)),
                "assigned_roles": list(set(roles))
            }
            
        except Exception as e:
            return {"success": False, "error": str(e)}
        finally:
            session.close()
    
    def get_user_roles(self, username: str) -> List[str]:
        """Get all roles for a user from database"""
        session = self.get_session()
        try:
            from sqlalchemy.orm import joinedload
            user = session.query(User).options(joinedload(User.role)).filter(
                User.username == username, 
                User.is_active == True
            ).first()
            
            if not user:
                return []
            
            # Get user's role (direct relationship)
            roles = []
            if user.role and user.role.is_active:
                roles.append(user.role.role_code)
            
            return list(set(roles))
            
        except Exception as e:
            print(f"Error getting user roles: {e}")
            return []
        finally:
            session.close()
    
    def get_users_by_role(self, role_code: str) -> List[str]:
        """Get all users that have a specific role"""
        session = self.get_session()
        try:
            # Get users with the specified role (direct relationship)
            from sqlalchemy.orm import joinedload
            users = session.query(User).options(joinedload(User.role)).join(Role).filter(
                Role.role_code == role_code,
                Role.is_active == True,
                User.is_active == True
            ).all()
            
            usernames = [user.username for user in users]
            
            return list(set(usernames))
            
        except Exception as e:
            print(f"Error getting users by role: {e}")
            return []
        finally:
            session.close()
    
    def get_all_roles(self) -> List[Dict[str, Any]]:
        """Get all active roles from database"""
        session = self.get_session()
        try:
            roles = session.query(Role).filter(Role.is_active == True).all()
            return [role.to_dict() for role in roles]
        except Exception as e:
            print(f"Error getting all roles: {e}")
            return []
        finally:
            session.close()
    
    def get_all_user_groups(self) -> Dict[str, Any]:
        """Get all user groups in the format expected by userGroups.json"""
        session = self.get_session()
        try:
            roles = session.query(Role).filter(Role.is_active == True).all()
            groups = {}
            
            for role in roles:
                role_code = role.role_code
                role_name = role.role_name
                description = role.description or f'Users with {role_name} role'
                
                # Get users for this role
                users = self.get_users_by_role(role_code)
                
                groups[role_code] = {
                    'description': description,
                    'users': users
                }
            
            return groups
            
        except Exception as e:
            print(f"Error getting all user groups: {e}")
            return {}
        finally:
            session.close()
    
    # KPI Management Methods (following user/client patterns)
    def get_kpi_by_code(self, kpi_code: str) -> Optional[KpiLibrary]:
        """Get KPI by code"""
        session = self.get_session()
        try:
            from sqlalchemy.orm import joinedload
            return session.query(KpiLibrary).options(joinedload(KpiLibrary.thresholds)).filter(
                KpiLibrary.kpi_code == kpi_code, 
                KpiLibrary.is_active == True
            ).first()
        finally:
            session.close()
    
    def get_kpi_by_id(self, kpi_id: int) -> Optional[KpiLibrary]:
        """Get KPI by ID"""
        session = self.get_session()
        try:
            from sqlalchemy.orm import joinedload
            return session.query(KpiLibrary).options(joinedload(KpiLibrary.thresholds)).filter(
                KpiLibrary.id == kpi_id, 
                KpiLibrary.is_active == True
            ).first()
        finally:
            session.close()
    
    def get_all_kpis(self, kpi_type: str = None, category: str = None) -> List[KpiLibrary]:
        """Get all active KPIs with optional filtering"""
        session = self.get_session()
        try:
            from sqlalchemy.orm import joinedload
            query = session.query(KpiLibrary).options(joinedload(KpiLibrary.thresholds)).filter(
                KpiLibrary.is_active == True
            )
            
            if kpi_type:
                query = query.filter(KpiLibrary.kpi_type == kpi_type)
            if category:
                query = query.filter(KpiLibrary.category == category)
                
            return query.all()
        finally:
            session.close()
    
    def create_kpi(self, kpi_data: Dict[str, Any]) -> Optional[KpiLibrary]:
        """Create a new KPI"""
        session = self.get_session()
        try:
            kpi = KpiLibrary(**kpi_data)
            session.add(kpi)
            session.commit()
            session.refresh(kpi)
            return kpi
        except Exception as e:
            session.rollback()
            logger.error(f"Error creating KPI: {e}")
            return None
        finally:
            session.close()
    
    def update_kpi(self, kpi_id: int, kpi_data: Dict[str, Any]) -> Optional[KpiLibrary]:
        """Update an existing KPI"""
        session = self.get_session()
        try:
            kpi = session.query(KpiLibrary).filter(
                KpiLibrary.id == kpi_id,
                KpiLibrary.is_active == True
            ).first()
            
            if not kpi:
                return None
            
            for key, value in kpi_data.items():
                if hasattr(kpi, key):
                    setattr(kpi, key, value)
            
            session.commit()
            session.refresh(kpi)
            return kpi
        except Exception as e:
            session.rollback()
            logger.error(f"Error updating KPI: {e}")
            return None
        finally:
            session.close()
    
    def delete_kpi(self, kpi_id: int) -> bool:
        """Soft delete a KPI"""
        session = self.get_session()
        try:
            kpi = session.query(KpiLibrary).filter(
                KpiLibrary.id == kpi_id,
                KpiLibrary.is_active == True
            ).first()
            
            if not kpi:
                return False
            
            kpi.is_active = False
            session.commit()
            return True
        except Exception as e:
            session.rollback()
            logger.error(f"Error deleting KPI: {e}")
            return False
        finally:
            session.close()
    
    def get_kpi_threshold(self, kpi_id: int, fund_id: str = None) -> Optional[KpiThreshold]:
        """Get threshold for a KPI (fund-specific or default)"""
        session = self.get_session()
        try:
            # First try to find fund-specific threshold
            if fund_id:
                threshold = session.query(KpiThreshold).filter(
                    KpiThreshold.kpi_id == kpi_id,
                    KpiThreshold.fund_id == fund_id,
                    KpiThreshold.is_active == True
                ).first()
                if threshold:
                    return threshold
            
            # Fall back to default threshold (fund_id is None)
            return session.query(KpiThreshold).filter(
                KpiThreshold.kpi_id == kpi_id,
                KpiThreshold.fund_id.is_(None),
                KpiThreshold.is_active == True
            ).first()
        finally:
            session.close()
    
    def create_kpi_threshold(self, threshold_data: Dict[str, Any]) -> Optional[KpiThreshold]:
        """Create a new KPI threshold"""
        session = self.get_session()
        try:
            threshold = KpiThreshold(**threshold_data)
            session.add(threshold)
            session.commit()
            session.refresh(threshold)
            return threshold
        except Exception as e:
            session.rollback()
            logger.error(f"Error creating KPI threshold: {e}")
            return None
        finally:
            session.close()
    
    def update_kpi_threshold(self, threshold_id: int, threshold_data: Dict[str, Any]) -> Optional[KpiThreshold]:
        """Update an existing KPI threshold"""
        session = self.get_session()
        try:
            threshold = session.query(KpiThreshold).filter(
                KpiThreshold.id == threshold_id,
                KpiThreshold.is_active == True
            ).first()
            
            if not threshold:
                return None
            
            for key, value in threshold_data.items():
                if hasattr(threshold, key):
                    setattr(threshold, key, value)
            
            session.commit()
            session.refresh(threshold)
            return threshold
        except Exception as e:
            session.rollback()
            logger.error(f"Error updating KPI threshold: {e}")
            return None
        finally:
            session.close()
    
    def delete_kpi_threshold(self, threshold_id: int) -> bool:
        """Delete a KPI threshold"""
        session = self.get_session()
        try:
            threshold = session.query(KpiThreshold).filter(
                KpiThreshold.id == threshold_id,
                KpiThreshold.is_active == True
            ).first()
            
            if not threshold:
                return False
            
            threshold.is_active = False
            session.commit()
            return True
        except Exception as e:
            session.rollback()
            logger.error(f"Error deleting KPI threshold: {e}")
            return False
        finally:
            session.close()
    
    def get_kpi_categories(self) -> List[str]:
        """Get all unique KPI categories"""
        session = self.get_session()
        try:
            categories = session.query(KpiLibrary.category).filter(
                KpiLibrary.is_active == True,
                KpiLibrary.category.isnot(None)
            ).distinct().all()
            return [cat[0] for cat in categories if cat[0]]
        finally:
            session.close()
    
    # DataSource Management Methods
    def get_data_source_by_id(self, source_id: int) -> Optional[DataSource]:
        """Get data source by ID"""
        session = self.get_session()
        try:
            return session.query(DataSource).filter(
                DataSource.id == source_id,
                DataSource.is_active == True
            ).first()
        finally:
            session.close()
    
    def get_data_source_by_name(self, name: str) -> Optional[DataSource]:
        """Get data source by name"""
        session = self.get_session()
        try:
            return session.query(DataSource).filter(
                DataSource.name == name,
                DataSource.is_active == True
            ).first()
        finally:
            session.close()
    
    def get_all_data_sources(self, fund_id: int = None, source_type: str = None, holiday_calendar: str = None) -> List[DataSource]:
        """Get all active data sources with optional filtering"""
        session = self.get_session()
        try:
            query = session.query(DataSource).filter(DataSource.is_active == True)
            
            if fund_id:
                query = query.filter(DataSource.fund_id == fund_id)
            
            if source_type:
                query = query.filter(DataSource.source == source_type)
            if holiday_calendar:
                query = query.filter(DataSource.holiday_calendar == holiday_calendar)
                
            return query.all()
        finally:
            session.close()
    
    def create_data_source(self, source_data: Dict[str, Any]) -> Optional[DataSource]:
        """Create a new data source"""
        session = self.get_session()
        try:
            data_source = DataSource(**source_data)
            session.add(data_source)
            session.commit()
            session.refresh(data_source)
            return data_source
        except Exception as e:
            session.rollback()
            logger.error(f"Error creating data source: {e}")
            return None
        finally:
            session.close()
    
    def update_data_source(self, source_id: int, source_data: Dict[str, Any]) -> Optional[DataSource]:
        """Update an existing data source"""
        session = self.get_session()
        try:
            data_source = session.query(DataSource).filter(
                DataSource.id == source_id,
                DataSource.is_active == True
            ).first()
            
            if not data_source:
                return None
            
            for key, value in source_data.items():
                if hasattr(data_source, key):
                    setattr(data_source, key, value)
            
            session.commit()
            session.refresh(data_source)
            return data_source
        except Exception as e:
            session.rollback()
            logger.error(f"Error updating data source: {e}")
            return None
        finally:
            session.close()
    
    def delete_data_source(self, source_id: int) -> bool:
        """Soft delete a data source"""
        session = self.get_session()
        try:
            data_source = session.query(DataSource).filter(
                DataSource.id == source_id,
                DataSource.is_active == True
            ).first()
            
            if not data_source:
                return False
            
            data_source.is_active = False
            session.commit()
            return True
        except Exception as e:
            session.rollback()
            logger.error(f"Error deleting data source: {e}")
            return False
        finally:
            session.close()
    
    def get_data_source_types(self) -> List[str]:
        """Get all unique data source types"""
        session = self.get_session()
        try:
            source_types = session.query(DataSource.source).filter(
                DataSource.is_active == True,
                DataSource.source.isnot(None)
            ).distinct().all()
            return [st[0] for st in source_types if st[0]]
        finally:
            session.close()
    
    def get_holiday_calendars(self) -> List[str]:
        """Get all unique holiday calendars"""
        session = self.get_session()
        try:
            calendars = session.query(DataSource.holiday_calendar).filter(
                DataSource.is_active == True,
                DataSource.holiday_calendar.isnot(None)
            ).distinct().all()
            return [cal[0] for cal in calendars if cal[0]]
        finally:
            session.close()
    
    # Calendar Management Methods
    def get_calendar_by_fund_id(self, fund_id: int) -> Optional[Calendar]:
        """Get calendar by fund ID"""
        session = self.get_session()
        try:
            return session.query(Calendar).filter(
                Calendar.fund_id == fund_id,
                Calendar.is_active == True
            ).first()
        finally:
            session.close()
    
    def get_calendar_by_id(self, calendar_id: int) -> Optional[Calendar]:
        """Get calendar by ID"""
        session = self.get_session()
        try:
            return session.query(Calendar).filter(
                Calendar.id == calendar_id,
                Calendar.is_active == True
            ).first()
        finally:
            session.close()
    
    def create_calendar(self, calendar_data: Dict[str, Any]) -> Optional[Calendar]:
        """Create a new calendar"""
        session = self.get_session()
        try:
            calendar = Calendar(**calendar_data)
            session.add(calendar)
            session.commit()
            session.refresh(calendar)
            return calendar
        except Exception as e:
            session.rollback()
            logger.error(f"Error creating calendar: {e}")
            return None
        finally:
            session.close()
    
    def update_calendar(self, calendar_id: int, calendar_data: Dict[str, Any]) -> Optional[Calendar]:
        """Update an existing calendar"""
        session = self.get_session()
        try:
            calendar = session.query(Calendar).filter(
                Calendar.id == calendar_id,
                Calendar.is_active == True
            ).first()
            
            if not calendar:
                return None
            
            for key, value in calendar_data.items():
                if hasattr(calendar, key):
                    setattr(calendar, key, value)
            
            session.commit()
            session.refresh(calendar)
            return calendar
        except Exception as e:
            session.rollback()
            logger.error(f"Error updating calendar: {e}")
            return None
        finally:
            session.close()
    
    def delete_calendar(self, calendar_id: int) -> bool:
        """Soft delete a calendar"""
        session = self.get_session()
        try:
            calendar = session.query(Calendar).filter(
                Calendar.id == calendar_id,
                Calendar.is_active == True
            ).first()
            
            if not calendar:
                return False
            
            calendar.is_active = False
            session.commit()
            return True
        except Exception as e:
            session.rollback()
            logger.error(f"Error deleting calendar: {e}")
            return False
        finally:
            session.close()

# Example usage
# Helper functions for easier database access
def get_database_manager() -> DatabaseManager:
    """
    Get a DatabaseManager instance (singleton pattern ensures reuse)
    
    Returns:
        DatabaseManager: Shared database manager instance
    """
    return DatabaseManager()

def get_database_session():
    """
    Get a database session with proper schema context
    
    Returns:
        SQLAlchemy session object
    """
    db_manager = get_database_manager()
    return db_manager.get_session()

def get_database_session_with_schema(schema_name: str = None):
    """
    Get a database session with specific schema context
    
    Args:
        schema_name: Name of the schema to use
        
    Returns:
        SQLAlchemy session object
    """
    db_manager = get_database_manager()
    return db_manager.get_session_with_schema(schema_name)

class Document(Base):
    """Document model for tracking processed documents"""
    __tablename__ = 'documents'
    __table_args__ = {'schema': os.getenv('DB_SCHEMA', 'public')}
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    doc_id = Column(UUID(as_uuid=True), unique=True, nullable=False, index=True, 
                   default=uuid.uuid4,
                   comment='Auto-generated unique identifier for the document')
    name = Column(String(255), nullable=False, index=True)
    type = Column(String(50), nullable=True, index=True)  # CapitalCall, Distribution, Statement, etc.
    path = Column(String(500), nullable=False)
    size = Column(Integer, nullable=True)  # Size in bytes
    status = Column(String(50), nullable=False, default='pending', index=True)  # pending, processing, completed, failed, validated, rejected
    fund_id = Column(Integer, ForeignKey(f'{os.getenv("DB_SCHEMA", "public")}.funds.id', ondelete='SET NULL'), nullable=True, index=True)
    account_id = Column(Integer, nullable=True, index=True)
    client_id = Column(Integer, ForeignKey(f'{os.getenv("DB_SCHEMA", "public")}.clients.id', ondelete='SET NULL'), nullable=True, index=True)
    upload_date = Column(DateTime, default=func.now(), nullable=False)
    replay = Column(Boolean, nullable=False, default=False)  # Flag for replay processing
    created_by = Column(String(100), nullable=True)  # Username who created/uploaded the document
    document_metadata = Column('metadata', JSON, nullable=True)  # Additional flexible metadata
    is_active = Column(Boolean, default=True, index=True)
    created_at = Column(DateTime, default=func.now(), nullable=False)
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now(), nullable=False)
    
    # Relationships
    fund = relationship("Fund", backref="documents")
    client = relationship("Client", backref="documents")
    capital_calls_extractions = relationship("CapitalCallsExtraction", back_populates="document")
    distributions_extractions = relationship("DistributionsExtraction", back_populates="document")
    statements_extractions = relationship("StatementsExtraction", back_populates="document")
    
    # Constraints
    __table_args__ = (
        CheckConstraint("status IN ('pending', 'processing', 'completed', 'failed', 'validated', 'rejected')", name='chk_document_status'),
        {'schema': os.getenv('DB_SCHEMA', 'public')}
    )
    
    def __repr__(self):
        return f"<Document(name='{self.name}', type='{self.type}', status='{self.status}')>"
        
    def __init__(self, **kwargs):
        # Call the parent class's __init__ method
        super().__init__(**kwargs)
        # Ensure doc_id is set if not provided
        if self.doc_id is None:
            self.doc_id = uuid.uuid4()
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert document to dictionary"""
        return {
            'id': self.id,
            'doc_id': str(self.doc_id) if self.doc_id else None,
            'name': self.name,
            'type': self.type,
            'path': self.path,
            'size': self.size,
            'status': self.status,
            'fund_id': self.fund_id,
            'account_id': self.account_id,
            'client_id': self.client_id,
            'upload_date': self.upload_date.isoformat() if self.upload_date else None,
            'replay': self.replay,
            'created_by': self.created_by,
            'metadata': self.document_metadata,
            'is_active': self.is_active,
            'created_at': self.created_at.isoformat() if self.created_at else None,
            'updated_at': self.updated_at.isoformat() if self.updated_at else None
        }

class DocumentConfiguration(Base):
    """Document Configuration model for storing document type schemas and configurations"""
    __tablename__ = 'document_configuration'
    __table_args__ = {'schema': os.getenv('DB_SCHEMA', 'public')}
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(255), nullable=False, index=True)
    description = Column(Text, nullable=True)
    sla = Column(Integer, nullable=True)  # SLA in days (e.g., 0, 1, 2, etc.)
    fields = Column(JSON, nullable=True)  # JSON schema blob
    is_deleted = Column(Boolean, default=False, nullable=False, index=True)  # Soft delete flag
    deleted_at = Column(DateTime, nullable=True)  # When the record was soft deleted
    
    def __repr__(self):
        return f"<DocumentConfiguration(name='{self.name}', sla={self.sla})>"
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert document configuration to dictionary"""
        return {
            'id': self.id,
            'name': self.name,
            'description': self.description,
            'sla': self.sla,
            'fields': self.fields,
            'is_deleted': self.is_deleted,
            'deleted_at': self.deleted_at.isoformat() if self.deleted_at else None
        }

class Investor(Base):
    """Investor model representing fund investors"""
    __tablename__ = 'investors'
    __table_args__ = {'schema': os.getenv('DB_SCHEMA', 'public')}
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    investor_name = Column(String(200), nullable=False, index=True)
    account_name = Column(String(200), nullable=False)
    account_number = Column(String(100), nullable=False, unique=True, index=True)
    
    # Contact details
    contact_title = Column(String(20))  # Mr., Mrs., Dr., etc.
    contact_first_name = Column(String(100))
    contact_last_name = Column(String(100))
    contact_email = Column(String(150), index=True)
    contact_number = Column(String(30))
    
    # Address information
    address_line1 = Column(String(200))
    address_line2 = Column(String(200))
    city = Column(String(100))
    state = Column(String(100))
    postal_code = Column(String(20))
    country = Column(String(100))
    
    # Additional details
    investor_type = Column(String(50))  # Individual, Corporate, Institutional, etc.
    tax_id = Column(String(50))
    kyc_status = Column(String(20), default='pending')  # pending, verified, rejected
    risk_profile = Column(String(20))  # conservative, moderate, aggressive
    sector = Column(Text)  # JSON array: Energy, Materials, Manufacturing, etc.
    status = Column(String(50))  # invested, exited, etc.
    
    # Status and metadata
    is_active = Column(Boolean, default=True, nullable=False, index=True)
    notes = Column(Text)
    investor_metadata = Column(JSON)
    
    # Timestamps
    created_at = Column(DateTime, default=func.now(), nullable=False)
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now(), nullable=False)
    
    # Relationships
    fund_investments = relationship("FundInvestor", back_populates="investor", cascade="all, delete-orphan")
    
    def to_dict(self):
        """Convert to dictionary"""
        return {
            'id': self.id,
            'investor_name': self.investor_name,
            'account_name': self.account_name,
            'account_number': self.account_number,
            'contact_title': self.contact_title,
            'contact_first_name': self.contact_first_name,
            'contact_last_name': self.contact_last_name,
            'contact_email': self.contact_email,
            'contact_number': self.contact_number,
            'address_line1': self.address_line1,
            'address_line2': self.address_line2,
            'city': self.city,
            'state': self.state,
            'postal_code': self.postal_code,
            'country': self.country,
            'investor_type': self.investor_type,
            'tax_id': self.tax_id,
            'kyc_status': self.kyc_status,
            'risk_profile': self.risk_profile,
            'is_active': self.is_active,
            'notes': self.notes,
            'investor_metadata': self.investor_metadata,
            'created_at': self.created_at.isoformat() if self.created_at else None,
            'updated_at': self.updated_at.isoformat() if self.updated_at else None
        }

class FundInvestor(Base):
    """Mapping table between funds and investors"""
    __tablename__ = 'fund_investors'
    __table_args__ = {'schema': os.getenv('DB_SCHEMA', 'public')}
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    fund_id = Column(Integer, ForeignKey(f'{os.getenv("DB_SCHEMA", "public")}.funds.id', ondelete='CASCADE'), nullable=False, index=True)
    investor_id = Column(Integer, ForeignKey(f'{os.getenv("DB_SCHEMA", "public")}.investors.id', ondelete='CASCADE'), nullable=False, index=True)
    
    # Investment details
    investment_amount = Column(Numeric(15, 2))  # Total investment amount
    investment_date = Column(Date)  # When the investment was made
    investment_type = Column(String(50))  # Initial, Additional, Redemption, etc.
    units_held = Column(Numeric(15, 6))  # Number of units/shares held
    unit_price = Column(Numeric(10, 4))  # Price per unit at investment
    
    # Status and metadata
    is_active = Column(Boolean, default=True, nullable=False, index=True)
    notes = Column(Text)
    fund_investor_metadata = Column(JSON)
    
    # Timestamps
    created_at = Column(DateTime, default=func.now(), nullable=False)
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now(), nullable=False)
    
    # Relationships
    fund = relationship("Fund", back_populates="investors")
    investor = relationship("Investor", back_populates="fund_investments")
    
    # Unique constraint to prevent duplicate fund-investor pairs
    __table_args__ = (
        UniqueConstraint('fund_id', 'investor_id', name='unique_fund_investor'),
        {'schema': os.getenv('DB_SCHEMA', 'public')}
    )
    
    def to_dict(self):
        """Convert to dictionary"""
        return {
            'id': self.id,
            'fund_id': self.fund_id,
            'investor_id': self.investor_id,
            'investment_amount': float(self.investment_amount) if self.investment_amount else None,
            'investment_date': self.investment_date.isoformat() if self.investment_date else None,
            'investment_type': self.investment_type,
            'units_held': float(self.units_held) if self.units_held else None,
            'unit_price': float(self.unit_price) if self.unit_price else None,
            'is_active': self.is_active,
            'notes': self.notes,
            'fund_investor_metadata': self.fund_investor_metadata,
            'created_at': self.created_at.isoformat() if self.created_at else None,
            'updated_at': self.updated_at.isoformat() if self.updated_at else None
        }

class CapitalCallsExtraction(Base):
    """Capital Calls extraction table for storing extracted data from capital call documents"""
    __tablename__ = 'capital_calls'
    __table_args__ = {'schema': os.getenv('DB_SCHEMA', 'public')}
    
    # Primary key and foreign keys
    id = Column(Integer, primary_key=True, autoincrement=True)
    doc_id = Column(Integer, ForeignKey(f'{os.getenv("DB_SCHEMA", "public")}.documents.id'), nullable=False, index=True)
    schema_id = Column(Integer, nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=True)
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=True)
    
    # Core identification fields
    Investor = Column(Text, nullable=True)
    Account = Column(Text, nullable=True)
    InvestorRefID = Column(Text, nullable=True)
    AccountRefID = Column(Text, nullable=True)
    Security = Column(Text, nullable=True)
    
    # Transaction details
    TransactionDate = Column(Date, nullable=True)
    Currency = Column(String(10), nullable=True)
    
    # Financial fields (decimal precision for monetary values)
    Distribution = Column(Numeric(precision=18, scale=2), nullable=True)
    DeemedCapitalCall = Column(Numeric(precision=18, scale=2), nullable=True)
    IncomeDistribution = Column(Numeric(precision=18, scale=2), nullable=True)
    IncomeReinvested = Column(Numeric(precision=18, scale=2), nullable=True)
    RecallableSell = Column(Numeric(precision=18, scale=2), nullable=True)
    ReturnOfCapital = Column(Numeric(precision=18, scale=2), nullable=True)
    DistributionOutsideCommitment = Column(Numeric(precision=18, scale=2), nullable=True)
    CapitalCall = Column(Numeric(precision=18, scale=2), nullable=True)
    CapitalCallOutsideCommitment = Column(Numeric(precision=18, scale=2), nullable=True)
    NetCashFlowQC = Column(Numeric(precision=18, scale=2), nullable=True)
    TransferIn = Column(Numeric(precision=18, scale=2), nullable=True)
    TransferOut = Column(Numeric(precision=18, scale=2), nullable=True)
    
    # Security transaction details
    Quantity = Column(Numeric(precision=18, scale=4), nullable=True)
    Price = Column(Numeric(precision=18, scale=2), nullable=True)
    
    # Capital commitment tracking
    CommittedCapital = Column(Numeric(precision=18, scale=2), nullable=True)
    RemainingCommittedCapital = Column(Numeric(precision=18, scale=2), nullable=True)
    ContributionsToDate = Column(Numeric(precision=18, scale=2), nullable=True)
    DistributionsToDate = Column(Numeric(precision=18, scale=2), nullable=True)
    ReturnOfCapitalToDate = Column(Numeric(precision=18, scale=2), nullable=True)
    
    # Additional financial fields
    DeemedGPContribution = Column(Numeric(precision=18, scale=2), nullable=True)
    Investments = Column(Numeric(precision=18, scale=2), nullable=True)
    ManagementFeeInsideCommitment = Column(Numeric(precision=18, scale=2), nullable=True)
    ManagementFeeOutsideCommitment = Column(Numeric(precision=18, scale=2), nullable=True)
    PartnershipExpenses = Column(Numeric(precision=18, scale=2), nullable=True)
    PartnershipExpensesAccountingAdminIT = Column(Numeric(precision=18, scale=2), nullable=True)
    PartnershipExpensesAuditTax = Column(Numeric(precision=18, scale=2), nullable=True)
    PartnershipExpensesBankFees = Column(Numeric(precision=18, scale=2), nullable=True)
    PartnershipExpensesCustodyFees = Column(Numeric(precision=18, scale=2), nullable=True)
    PartnershipExpensesDueDiligence = Column(Numeric(precision=18, scale=2), nullable=True)
    PartnershipExpensesLegal = Column(Numeric(precision=18, scale=2), nullable=True)
    PartnershipExpensesOrganizationCosts = Column(Numeric(precision=18, scale=2), nullable=True)
    PartnershipExpensesTravelEntertainment = Column(Numeric(precision=18, scale=2), nullable=True)
    PartnershipExpensesOther = Column(Numeric(precision=18, scale=2), nullable=True)
    PlacementAgentFees = Column(Numeric(precision=18, scale=2), nullable=True)
    SubsequentCloseInterest = Column(Numeric(precision=18, scale=2), nullable=True)
    WorkingCapital = Column(Numeric(precision=18, scale=2), nullable=True)
    
    # Confidence scores and verbatim text (JSON fields)
    ConfidenceScore = Column(Text, nullable=True)  # JSON string
    VerbatimText = Column(Text, nullable=True)  # JSON string
    
    # Common extraction metadata
    additional_columns = Column(Text, nullable=True)
    document_name = Column(String(2000), nullable=True)
    extraction = Column(String(255), nullable=False)
    file_hex_hash = Column(String(255), nullable=True)
    total = Column(Integer, nullable=True)
    found = Column(Integer, nullable=True)
    missing = Column(Integer, nullable=True)
    
    # Relationships
    document = relationship("Document", back_populates="capital_calls_extractions")
    
    def to_dict(self):
        """Convert to dictionary"""
        return {
            'id': self.id,
            'doc_id': self.doc_id,
            'schema_id': self.schema_id,
            'created_at': self.created_at.isoformat() if self.created_at else None,
            'updated_at': self.updated_at.isoformat() if self.updated_at else None,
            'Investor': self.Investor,
            'Account': self.Account,
            'InvestorRefID': self.InvestorRefID,
            'AccountRefID': self.AccountRefID,
            'Security': self.Security,
            'TransactionDate': self.TransactionDate.isoformat() if self.TransactionDate else None,
            'Currency': self.Currency,
            'Distribution': float(self.Distribution) if self.Distribution else None,
            'DeemedCapitalCall': float(self.DeemedCapitalCall) if self.DeemedCapitalCall else None,
            'IncomeDistribution': float(self.IncomeDistribution) if self.IncomeDistribution else None,
            'IncomeReinvested': float(self.IncomeReinvested) if self.IncomeReinvested else None,
            'RecallableSell': float(self.RecallableSell) if self.RecallableSell else None,
            'ReturnOfCapital': float(self.ReturnOfCapital) if self.ReturnOfCapital else None,
            'DistributionOutsideCommitment': float(self.DistributionOutsideCommitment) if self.DistributionOutsideCommitment else None,
            'CapitalCall': float(self.CapitalCall) if self.CapitalCall else None,
            'CapitalCallOutsideCommitment': float(self.CapitalCallOutsideCommitment) if self.CapitalCallOutsideCommitment else None,
            'NetCashFlowQC': float(self.NetCashFlowQC) if self.NetCashFlowQC else None,
            'TransferIn': float(self.TransferIn) if self.TransferIn else None,
            'TransferOut': float(self.TransferOut) if self.TransferOut else None,
            'Quantity': float(self.Quantity) if self.Quantity else None,
            'Price': float(self.Price) if self.Price else None,
            'CommittedCapital': float(self.CommittedCapital) if self.CommittedCapital else None,
            'RemainingCommittedCapital': float(self.RemainingCommittedCapital) if self.RemainingCommittedCapital else None,
            'ContributionsToDate': float(self.ContributionsToDate) if self.ContributionsToDate else None,
            'DistributionsToDate': float(self.DistributionsToDate) if self.DistributionsToDate else None,
            'ReturnOfCapitalToDate': float(self.ReturnOfCapitalToDate) if self.ReturnOfCapitalToDate else None,
            'DeemedGPContribution': float(self.DeemedGPContribution) if self.DeemedGPContribution else None,
            'Investments': float(self.Investments) if self.Investments else None,
            'ManagementFeeInsideCommitment': float(self.ManagementFeeInsideCommitment) if self.ManagementFeeInsideCommitment else None,
            'ManagementFeeOutsideCommitment': float(self.ManagementFeeOutsideCommitment) if self.ManagementFeeOutsideCommitment else None,
            'PartnershipExpenses': float(self.PartnershipExpenses) if self.PartnershipExpenses else None,
            'PartnershipExpensesAccountingAdminIT': float(self.PartnershipExpensesAccountingAdminIT) if self.PartnershipExpensesAccountingAdminIT else None,
            'PartnershipExpensesAuditTax': float(self.PartnershipExpensesAuditTax) if self.PartnershipExpensesAuditTax else None,
            'PartnershipExpensesBankFees': float(self.PartnershipExpensesBankFees) if self.PartnershipExpensesBankFees else None,
            'PartnershipExpensesCustodyFees': float(self.PartnershipExpensesCustodyFees) if self.PartnershipExpensesCustodyFees else None,
            'PartnershipExpensesDueDiligence': float(self.PartnershipExpensesDueDiligence) if self.PartnershipExpensesDueDiligence else None,
            'PartnershipExpensesLegal': float(self.PartnershipExpensesLegal) if self.PartnershipExpensesLegal else None,
            'PartnershipExpensesOrganizationCosts': float(self.PartnershipExpensesOrganizationCosts) if self.PartnershipExpensesOrganizationCosts else None,
            'PartnershipExpensesTravelEntertainment': float(self.PartnershipExpensesTravelEntertainment) if self.PartnershipExpensesTravelEntertainment else None,
            'PartnershipExpensesOther': float(self.PartnershipExpensesOther) if self.PartnershipExpensesOther else None,
            'PlacementAgentFees': float(self.PlacementAgentFees) if self.PlacementAgentFees else None,
            'SubsequentCloseInterest': float(self.SubsequentCloseInterest) if self.SubsequentCloseInterest else None,
            'WorkingCapital': float(self.WorkingCapital) if self.WorkingCapital else None,
            'ConfidenceScore': self.ConfidenceScore,
            'VerbatimText': self.VerbatimText,
            'additional_columns': self.additional_columns,
            'document_name': self.document_name,
            'extraction': self.extraction,
            'file_hex_hash': self.file_hex_hash,
            'total': self.total,
            'found': self.found,
            'missing': self.missing
        }

class DistributionsExtraction(Base):
    """Distributions extraction table for storing extracted data from distribution documents"""
    __tablename__ = 'distributions'
    __table_args__ = {'schema': os.getenv('DB_SCHEMA', 'public')}
    
    # Primary key and foreign keys
    id = Column(Integer, primary_key=True, autoincrement=True)
    doc_id = Column(Integer, ForeignKey(f'{os.getenv("DB_SCHEMA", "public")}.documents.id'), nullable=False, index=True)
    schema_id = Column(Integer, nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=True)
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=True)
    
    # Core identification fields
    Investor = Column(Text, nullable=True)
    Account = Column(Text, nullable=True)
    InvestorRefID = Column(Text, nullable=True)
    AccountRefID = Column(Text, nullable=True)
    Security = Column(Text, nullable=True)
    
    # Transaction details
    TransactionDate = Column(Date, nullable=True)
    Currency = Column(String(10), nullable=True)
    
    # Financial fields (decimal precision for monetary values)
    Distribution = Column(Numeric(precision=18, scale=2), nullable=True)
    DeemeedCapitalCall = Column(Numeric(precision=18, scale=2), nullable=True)  # Note: keeping original typo from schema
    IncomeDistribution = Column(Numeric(precision=18, scale=2), nullable=True)
    IncomeReinvested = Column(Numeric(precision=18, scale=2), nullable=True)
    RecallableSell = Column(Numeric(precision=18, scale=2), nullable=True)
    ReturnOfCapital = Column(Numeric(precision=18, scale=2), nullable=True)
    DistributionOutsideCommitment = Column(Numeric(precision=18, scale=2), nullable=True)
    CapitalCall = Column(Numeric(precision=18, scale=2), nullable=True)
    CapitalCallOutsideCommitment = Column(Numeric(precision=18, scale=2), nullable=True)
    NetCashFlowQC = Column(Numeric(precision=18, scale=2), nullable=True)
    TransferIn = Column(Numeric(precision=18, scale=2), nullable=True)
    TransferOut = Column(Numeric(precision=18, scale=2), nullable=True)
    
    # Security transaction details
    Quantity = Column(Numeric(precision=18, scale=4), nullable=True)
    Price = Column(Numeric(precision=18, scale=2), nullable=True)
    
    # Capital commitment tracking
    CommittedCapital = Column(Numeric(precision=18, scale=2), nullable=True)
    RemainingCommittedCapital = Column(Numeric(precision=18, scale=2), nullable=True)
    ContributionsToDate = Column(Numeric(precision=18, scale=2), nullable=True)
    DistributionsToDate = Column(Numeric(precision=18, scale=2), nullable=True)
    ReturnOfCapitalToDate = Column(Numeric(precision=18, scale=2), nullable=True)
    
    # Additional distribution fields
    Carry = Column(Numeric(precision=18, scale=2), nullable=True)
    Clawback = Column(Numeric(precision=18, scale=2), nullable=True)
    RealizedGainCash = Column(Numeric(precision=18, scale=2), nullable=True)
    RealizedGainStock = Column(Numeric(precision=18, scale=2), nullable=True)
    RealizedLossCash = Column(Numeric(precision=18, scale=2), nullable=True)
    RealizedLossStock = Column(Numeric(precision=18, scale=2), nullable=True)
    ReturnOfCapitalManagementFees = Column(Numeric(precision=18, scale=2), nullable=True)
    ReturnOfCapitalPartnershipExpenses = Column(Numeric(precision=18, scale=2), nullable=True)
    ReturnOfCapitalStock = Column(Numeric(precision=18, scale=2), nullable=True)
    TemporaryReturnOfCapitalManagementFees = Column(Numeric(precision=18, scale=2), nullable=True)
    SubsequentCloseInterest = Column(Numeric(precision=18, scale=2), nullable=True)
    Other = Column(Numeric(precision=18, scale=2), nullable=True)

    # Confidence scores and verbatim text (JSON fields)
    ConfidenceScore = Column(Text, nullable=True)  # JSON string
    VerbatimText = Column(Text, nullable=True)  # JSON string
    
    # Common extraction metadata
    additional_columns = Column(Text, nullable=True)
    document_name = Column(String(2000), nullable=True)
    extraction = Column(String(255), nullable=False)
    file_hex_hash = Column(String(255), nullable=True)
    total = Column(Integer, nullable=True)
    found = Column(Integer, nullable=True)
    missing = Column(Integer, nullable=True)
    
    # Relationships
    document = relationship("Document", back_populates="distributions_extractions")
    
    def to_dict(self):
        """Convert to dictionary"""
        return {
            'id': self.id,
            'doc_id': self.doc_id,
            'schema_id': self.schema_id,
            'created_at': self.created_at.isoformat() if self.created_at else None,
            'updated_at': self.updated_at.isoformat() if self.updated_at else None,
            'Investor': self.Investor,
            'Account': self.Account,
            'InvestorRefID': self.InvestorRefID,
            'AccountRefID': self.AccountRefID,
            'Security': self.Security,
            'TransactionDate': self.TransactionDate.isoformat() if self.TransactionDate else None,
            'Currency': self.Currency,
            'Distribution': float(self.Distribution) if self.Distribution else None,
            'DeemeedCapitalCall': float(self.DeemeedCapitalCall) if self.DeemeedCapitalCall else None,
            'IncomeDistribution': float(self.IncomeDistribution) if self.IncomeDistribution else None,
            'IncomeReinvested': float(self.IncomeReinvested) if self.IncomeReinvested else None,
            'RecallableSell': float(self.RecallableSell) if self.RecallableSell else None,
            'ReturnOfCapital': float(self.ReturnOfCapital) if self.ReturnOfCapital else None,
            'DistributionOutsideCommitment': float(self.DistributionOutsideCommitment) if self.DistributionOutsideCommitment else None,
            'CapitalCall': float(self.CapitalCall) if self.CapitalCall else None,
            'CapitalCallOutsideCommitment': float(self.CapitalCallOutsideCommitment) if self.CapitalCallOutsideCommitment else None,
            'NetCashFlowQC': float(self.NetCashFlowQC) if self.NetCashFlowQC else None,
            'TransferIn': float(self.TransferIn) if self.TransferIn else None,
            'TransferOut': float(self.TransferOut) if self.TransferOut else None,
            'Quantity': float(self.Quantity) if self.Quantity else None,
            'Price': float(self.Price) if self.Price else None,
            'CommittedCapital': float(self.CommittedCapital) if self.CommittedCapital else None,
            'RemainingCommittedCapital': float(self.RemainingCommittedCapital) if self.RemainingCommittedCapital else None,
            'ContributionsToDate': float(self.ContributionsToDate) if self.ContributionsToDate else None,
            'DistributionsToDate': float(self.DistributionsToDate) if self.DistributionsToDate else None,
            'ReturnOfCapitalToDate': float(self.ReturnOfCapitalToDate) if self.ReturnOfCapitalToDate else None,
            'Carry': float(self.Carry) if self.Carry else None,
            'Clawback': float(self.Clawback) if self.Clawback else None,
            'RealizedGainCash': float(self.RealizedGainCash) if self.RealizedGainCash else None,
            'RealizedGainStock': float(self.RealizedGainStock) if self.RealizedGainStock else None,
            'RealizedLossCash': float(self.RealizedLossCash) if self.RealizedLossCash else None,
            'RealizedLossStock': float(self.RealizedLossStock) if self.RealizedLossStock else None,
            'ReturnOfCapitalManagementFees': float(self.ReturnOfCapitalManagementFees) if self.ReturnOfCapitalManagementFees else None,
            'ReturnOfCapitalPartnershipExpenses': float(self.ReturnOfCapitalPartnershipExpenses) if self.ReturnOfCapitalPartnershipExpenses else None,
            'ReturnOfCapitalStock': float(self.ReturnOfCapitalStock) if self.ReturnOfCapitalStock else None,
            'TemporaryReturnOfCapitalManagementFees': float(self.TemporaryReturnOfCapitalManagementFees) if self.TemporaryReturnOfCapitalManagementFees else None,
            'SubsequentCloseInterest': float(self.SubsequentCloseInterest) if self.SubsequentCloseInterest else None,
            'Other': float(self.Other) if self.Other else None,
            'ConfidenceScore': self.ConfidenceScore,
            'VerbatimText': self.VerbatimText,
            'additional_columns': self.additional_columns,
            'document_name': self.document_name,
            'extraction': self.extraction,
            'file_hex_hash': self.file_hex_hash,
            'total': self.total,
            'found': self.found,
            'missing': self.missing
        }

class StatementsExtraction(Base):
    """Statements extraction table for storing extracted data from statement documents"""
    __tablename__ = 'statements'
    __table_args__ = {'schema': os.getenv('DB_SCHEMA', 'public')}
    
    # Primary key and foreign keys
    id = Column(Integer, primary_key=True, autoincrement=True)
    doc_id = Column(Integer, ForeignKey(f'{os.getenv("DB_SCHEMA", "public")}.documents.id'), nullable=False, index=True)
    schema_id = Column(Integer, nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=True)
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=True)
    
    # Core identification fields
    Investor = Column(Text, nullable=True)
    Account = Column(Text, nullable=True)
    InvestorRefID = Column(Text, nullable=True)
    AccountRefID = Column(Text, nullable=True)
    Security = Column(Text, nullable=True)
    
    # Period details
    PeriodBeginningDT = Column(Date, nullable=True)
    PeriodEndingDT = Column(Date, nullable=True)
    Currency = Column(String(10), nullable=True)
    
    # Capital and contribution fields (decimal precision for monetary values)
    NetOpeningCapital = Column(Numeric(precision=18, scale=2), nullable=True)
    Contributions = Column(Numeric(precision=18, scale=2), nullable=True)
    ContributionOutsideCommitment = Column(Numeric(precision=18, scale=2), nullable=True)
    Withdrawals = Column(Numeric(precision=18, scale=2), nullable=True)
    ReturnOfCapital = Column(Numeric(precision=18, scale=2), nullable=True)
    NetCapitalActivity = Column(Numeric(precision=18, scale=2), nullable=True)
    
    # Transfer fields
    TransfersIn = Column(Numeric(precision=18, scale=2), nullable=True)
    TransfersOut = Column(Numeric(precision=18, scale=2), nullable=True)
    NetTransfers = Column(Numeric(precision=18, scale=2), nullable=True)
    
    # Income and performance fields
    IncomeDistribution = Column(Numeric(precision=18, scale=2), nullable=True)
    RealizedGainLoss = Column(Numeric(precision=18, scale=2), nullable=True)
    UnrealizedGainLoss = Column(Numeric(precision=18, scale=2), nullable=True)
    NetGainLoss = Column(Numeric(precision=18, scale=2), nullable=True)
    InvestmentIncome = Column(Numeric(precision=18, scale=2), nullable=True)
    OtherIncomeLoss = Column(Numeric(precision=18, scale=2), nullable=True)
    
    # Fee and expense fields
    ManagementFee = Column(Numeric(precision=18, scale=2), nullable=True)
    OtherExpenses = Column(Numeric(precision=18, scale=2), nullable=True)
    CarriedInterest = Column(Numeric(precision=18, scale=2), nullable=True)
    OtherAdjustments = Column(Numeric(precision=18, scale=2), nullable=True)
    
    # Closing capital and security details
    NetClosingCapital = Column(Numeric(precision=18, scale=2), nullable=True)
    Quantity = Column(Numeric(precision=18, scale=4), nullable=True)
    Price = Column(Numeric(precision=18, scale=2), nullable=True)
    
    # Performance metrics
    MTDPerformance = Column(Numeric(precision=18, scale=2), nullable=True)
    QTDPerformance = Column(Numeric(precision=18, scale=2), nullable=True)
    YTDPerformance = Column(Numeric(precision=18, scale=2), nullable=True)
    IRR = Column(Numeric(precision=18, scale=2), nullable=True)
    
    # Commitment tracking
    Commitment = Column(Numeric(precision=18, scale=2), nullable=True)
    ContributionsToDate = Column(Numeric(precision=18, scale=2), nullable=True)
    UnfundedCommitment = Column(Numeric(precision=18, scale=2), nullable=True)
    UnfundedCommitmentAdj = Column(Numeric(precision=18, scale=2), nullable=True)
    DistributionsSubjectToRecall = Column(Numeric(precision=18, scale=2), nullable=True)
    TotalDistributions = Column(Numeric(precision=18, scale=2), nullable=True)
    
    # Boolean fields
    RecallableDistribution = Column(Boolean, nullable=True)
    CommitmentOnlyStatement = Column(Boolean, nullable=True)
    
    # Confidence scores and verbatim text (JSON fields)
    ConfidenceScore = Column(Text, nullable=True)  # JSON string
    VerbatimText = Column(Text, nullable=True)  # JSON string
    
    # Common extraction metadata
    additional_columns = Column(Text, nullable=True)
    document_name = Column(String(2000), nullable=True)
    extraction = Column(String(255), nullable=False)
    file_hex_hash = Column(String(255), nullable=True)
    total = Column(Integer, nullable=True)
    found = Column(Integer, nullable=True)
    missing = Column(Integer, nullable=True)
    
    # Relationships
    document = relationship("Document", back_populates="statements_extractions")
    
    def to_dict(self):
        """Convert to dictionary"""
        return {
            'id': self.id,
            'doc_id': self.doc_id,
            'schema_id': self.schema_id,
            'created_at': self.created_at.isoformat() if self.created_at else None,
            'updated_at': self.updated_at.isoformat() if self.updated_at else None,
            'Investor': self.Investor,
            'Account': self.Account,
            'InvestorRefID': self.InvestorRefID,
            'AccountRefID': self.AccountRefID,
            'Security': self.Security,
            'PeriodBeginningDT': self.PeriodBeginningDT.isoformat() if self.PeriodBeginningDT else None,
            'PeriodEndingDT': self.PeriodEndingDT.isoformat() if self.PeriodEndingDT else None,
            'Currency': self.Currency,
            'NetOpeningCapital': float(self.NetOpeningCapital) if self.NetOpeningCapital else None,
            'Contributions': float(self.Contributions) if self.Contributions else None,
            'ContributionOutsideCommitment': float(self.ContributionOutsideCommitment) if self.ContributionOutsideCommitment else None,
            'Withdrawals': float(self.Withdrawals) if self.Withdrawals else None,
            'ReturnOfCapital': float(self.ReturnOfCapital) if self.ReturnOfCapital else None,
            'NetCapitalActivity': float(self.NetCapitalActivity) if self.NetCapitalActivity else None,
            'TransfersIn': float(self.TransfersIn) if self.TransfersIn else None,
            'TransfersOut': float(self.TransfersOut) if self.TransfersOut else None,
            'NetTransfers': float(self.NetTransfers) if self.NetTransfers else None,
            'IncomeDistribution': float(self.IncomeDistribution) if self.IncomeDistribution else None,
            'RealizedGainLoss': float(self.RealizedGainLoss) if self.RealizedGainLoss else None,
            'UnrealizedGainLoss': float(self.UnrealizedGainLoss) if self.UnrealizedGainLoss else None,
            'NetGainLoss': float(self.NetGainLoss) if self.NetGainLoss else None,
            'InvestmentIncome': float(self.InvestmentIncome) if self.InvestmentIncome else None,
            'OtherIncomeLoss': float(self.OtherIncomeLoss) if self.OtherIncomeLoss else None,
            'ManagementFee': float(self.ManagementFee) if self.ManagementFee else None,
            'OtherExpenses': float(self.OtherExpenses) if self.OtherExpenses else None,
            'CarriedInterest': float(self.CarriedInterest) if self.CarriedInterest else None,
            'OtherAdjustments': float(self.OtherAdjustments) if self.OtherAdjustments else None,
            'NetClosingCapital': float(self.NetClosingCapital) if self.NetClosingCapital else None,
            'Quantity': float(self.Quantity) if self.Quantity else None,
            'Price': float(self.Price) if self.Price else None,
            'MTDPerformance': float(self.MTDPerformance) if self.MTDPerformance else None,
            'QTDPerformance': float(self.QTDPerformance) if self.QTDPerformance else None,
            'YTDPerformance': float(self.YTDPerformance) if self.YTDPerformance else None,
            'IRR': float(self.IRR) if self.IRR else None,
            'Commitment': float(self.Commitment) if self.Commitment else None,
            'ContributionsToDate': float(self.ContributionsToDate) if self.ContributionsToDate else None,
            'UnfundedCommitment': float(self.UnfundedCommitment) if self.UnfundedCommitment else None,
            'UnfundedCommitmentAdj': float(self.UnfundedCommitmentAdj) if self.UnfundedCommitmentAdj else None,
            'DistributionsSubjectToRecall': float(self.DistributionsSubjectToRecall) if self.DistributionsSubjectToRecall else None,
            'TotalDistributions': float(self.TotalDistributions) if self.TotalDistributions else None,
            'RecallableDistribution': self.RecallableDistribution,
            'CommitmentOnlyStatement': self.CommitmentOnlyStatement,
            'ConfidenceScore': self.ConfidenceScore,
            'VerbatimText': self.VerbatimText,
            'additional_columns': self.additional_columns,
            'document_name': self.document_name,
            'extraction': self.extraction,
            'file_hex_hash': self.file_hex_hash,
            'total': self.total,
            'found': self.found,
            'missing': self.missing
        }

class SubproductMaster(Base):
    """Subproduct Master table for managing subproducts"""
    __tablename__ = 'tbl_subproduct_master'
    __table_args__ = {'schema': 'validus'}
    
    intsubproductid = Column(Integer, primary_key=True, autoincrement=True)
    vcsubproductname = Column(String(250), nullable=False)
    vcdescription = Column(String(500))
    isactive = Column(BIT(1), default=text("B'1'"), nullable=False)
    intcreatedby = Column(Integer)
    dtcreatedat = Column(DateTime, default=func.now(), nullable=False)
    intupdatedby = Column(Integer)
    dtupdatedat = Column(DateTime, onupdate=func.now())
    
    # Relationships
    subproduct_details = relationship("SubproductDetails", back_populates="subproduct", cascade="all, delete-orphan")
    validations = relationship("ValidationMaster", back_populates="subproduct", cascade="all, delete-orphan")
    ratios = relationship("RatioMaster", back_populates="subproduct", cascade="all, delete-orphan")
    
    def to_dict(self):
        """Convert to dictionary"""
        return {
            'intsubproductid': self.intsubproductid,
            'vcsubproductname': self.vcsubproductname,
            'vcdescription': self.vcdescription,
            'isactive': self.isactive,
            'intcreatedby': self.intcreatedby,
            'dtcreatedat': self.dtcreatedat.isoformat() if self.dtcreatedat else None,
            'intupdatedby': self.intupdatedby,
            'dtupdatedat': self.dtupdatedat.isoformat() if self.dtupdatedat else None
        }

class SubproductDetails(Base):
    """Subproduct Details table for managing subproduct type configurations"""
    __tablename__ = 'tbl_subproduct_details'
    __table_args__ = {'schema': 'validus'}
    
    intsubproductdetailid = Column(Integer, primary_key=True, autoincrement=True)
    intsubproductid = Column(Integer, ForeignKey(f'validus.tbl_subproduct_master.intsubproductid'), nullable=False, index=True)
    vcvalidustype = Column(String(250))  # Validation or Ratio
    vctype = Column(String(250))
    vcsubtype = Column(String(250))
    vcdescription = Column(String(500))
    isactive = Column(BIT(1), default=text("B'1'"), nullable=False)
    intcreatedby = Column(Integer)
    dtcreatedat = Column(DateTime, default=func.now(), nullable=False)
    intupdatedby = Column(Integer)
    dtupdatedat = Column(DateTime, onupdate=func.now())
    
    # Relationships
    subproduct = relationship("SubproductMaster", back_populates="subproduct_details")
    
    def to_dict(self):
        """Convert to dictionary"""
        return {
            'intsubproductdetailid': self.intsubproductdetailid,
            'intsubproductid': self.intsubproductid,
            'vcvalidustype': self.vcvalidustype,
            'vctype': self.vctype,
            'vcsubtype': self.vcsubtype,
            'vcdescription': self.vcdescription,
            'isactive': self.isactive,
            'intcreatedby': self.intcreatedby,
            'dtcreatedat': self.dtcreatedat.isoformat() if self.dtcreatedat else None,
            'intupdatedby': self.intupdatedby,
            'dtupdatedat': self.dtupdatedat.isoformat() if self.dtupdatedat else None
        }

class ValidationMaster(Base):
    """Validation Master table for managing validation rules"""
    __tablename__ = 'tbl_validation_master'
    __table_args__ = {'schema': 'validus'}
    
    intvalidationmasterid = Column(Integer, primary_key=True, autoincrement=True)
    intsubproductid = Column(Integer, ForeignKey(f'validus.tbl_subproduct_master.intsubproductid'), nullable=False, index=True)
    vcsourcetype = Column(String(250))  # Single or Dual
    vctype = Column(String(250))
    vcsubtype = Column(String(250))
    issubtype_subtotal = Column(BIT(1))
    vcvalidationname = Column(String(250))
    isvalidation_subtotal = Column(BIT(1))
    vcdescription = Column(String(500))
    intthreshold = Column(Numeric(12, 4))
    vcthresholdtype = Column(String(100))
    vcthreshold_abs_range = Column(String(20))
    intthresholdmin = Column(Numeric(30, 6))
    intthresholdmax = Column(Numeric(30, 6))
    intprecision = Column(Numeric(12, 10))
    isactive = Column(BIT(1), default=text("B'1'"), nullable=False)
    intcreatedby = Column(Integer)
    dtcreatedat = Column(DateTime, default=func.now(), nullable=False)
    intupdatedby = Column(Integer)
    dtupdatedat = Column(DateTime, onupdate=func.now())
    
    # Relationships
    subproduct = relationship("SubproductMaster", back_populates="validations")
    details = relationship("ValidationDetails", back_populates="master", cascade="all, delete-orphan")
    configurations = relationship("ValidationConfiguration", back_populates="validation", cascade="all, delete-orphan")
    
    def to_dict(self):
        """Convert to dictionary"""
        return {
            'intvalidationmasterid': self.intvalidationmasterid,
            'intsubproductid': self.intsubproductid,
            'vcsourcetype': self.vcsourcetype,
            'vctype': self.vctype,
            'vcsubtype': self.vcsubtype,
            'issubtype_subtotal': self.issubtype_subtotal,
            'vcvalidationname': self.vcvalidationname,
            'isvalidation_subtotal': self.isvalidation_subtotal,
            'vcdescription': self.vcdescription,
            'intthreshold': float(self.intthreshold) if self.intthreshold else None,
            'vcthresholdtype': self.vcthresholdtype,
            'vcthreshold_abs_range': self.vcthreshold_abs_range,
            'intthresholdmin': float(self.intthresholdmin) if self.intthresholdmin else None,
            'intthresholdmax': float(self.intthresholdmax) if self.intthresholdmax else None,
            'intprecision': float(self.intprecision) if self.intprecision else None,
            'isactive': self.isactive,
            'intcreatedby': self.intcreatedby,
            'dtcreatedat': self.dtcreatedat.isoformat() if self.dtcreatedat else None,
            'intupdatedby': self.intupdatedby,
            'dtupdatedat': self.dtupdatedat.isoformat() if self.dtupdatedat else None
        }

class ValidationDetails(Base):
    """Validation Details table for managing validation data model specifications"""
    __tablename__ = 'tbl_validation_details'
    __table_args__ = {'schema': 'validus'}
    
    intvalidationdetailid = Column(Integer, primary_key=True, autoincrement=True)
    intvalidationmasterid = Column(Integer, ForeignKey(f'validus.tbl_validation_master.intvalidationmasterid'), nullable=False, index=True)
    intdatamodelid = Column(Text, nullable=False, index=True)  # Changed from Integer to Text to support ranged validations
    intgroup_attributeid = Column(Integer, ForeignKey(f'validus.tbl_data_model_details.intdatamodeldetailid'), nullable=True, index=True)
    intassettypeid = Column(Integer, ForeignKey(f'validus.tbl_data_model_details.intdatamodeldetailid'), nullable=True, index=True)
    intcalc_attributeid = Column(Integer, ForeignKey(f'validus.tbl_data_model_details.intdatamodeldetailid'), nullable=True, index=True)
    vcaggregationtype = Column(String(20))  # sum/avg/max/min/etc
    vcfilter = Column(Text)
    vcfiltertype = Column(String(1))
    vcformula = Column(Text)
    intcreatedby = Column(Integer)
    dtcreatedat = Column(DateTime, default=func.now(), nullable=False)
    intupdatedby = Column(Integer)
    dtupdatedat = Column(DateTime, onupdate=func.now())
    
    # Relationships
    master = relationship("ValidationMaster", back_populates="details")
    # Note: datamodel relationship removed because intdatamodelid is now Text (can contain comma-separated IDs for ranged validations)
    # If you need to access data models, parse intdatamodelid and query DataModelMaster separately
    group_attribute_detail = relationship("DataModelDetails", foreign_keys=[intgroup_attributeid])
    assettype_detail = relationship("DataModelDetails", foreign_keys=[intassettypeid])
    calc_attribute_detail = relationship("DataModelDetails", foreign_keys=[intcalc_attributeid])
    
    def to_dict(self):
        """Convert to dictionary"""
        return {
            'intvalidationdetailid': self.intvalidationdetailid,
            'intvalidationmasterid': self.intvalidationmasterid,
            'intdatamodelid': self.intdatamodelid,
            'intgroup_attributeid': self.intgroup_attributeid,
            'intassettypeid': self.intassettypeid,
            'intcalc_attributeid': self.intcalc_attributeid,
            'vcaggregationtype': self.vcaggregationtype,
            'vcfilter': self.vcfilter,
            'vcfiltertype': self.vcfiltertype,
            'vcformula': self.vcformula,
            'intcreatedby': self.intcreatedby,
            'dtcreatedat': self.dtcreatedat.isoformat() if self.dtcreatedat else None,
            'intupdatedby': self.intupdatedby,
            'dtupdatedat': self.dtupdatedat.isoformat() if self.dtupdatedat else None
        }

class RatioMaster(Base):
    """Ratio Master table for managing ratio rules"""
    __tablename__ = 'tbl_ratio_master'
    __table_args__ = {'schema': 'validus'}
    
    intratiomasterid = Column(Integer, primary_key=True, autoincrement=True)
    intsubproductid = Column(Integer, ForeignKey(f'validus.tbl_subproduct_master.intsubproductid'), nullable=False, index=True)
    vcsourcetype = Column(String(250))  # Single or Dual
    vctype = Column(String(250))
    vcrationame = Column(String(250))
    isratio_subtotal = Column(BIT(1))
    vcdescription = Column(String(500))
    intthreshold = Column(Numeric(12, 4))
    vcthresholdtype = Column(String(100))
    vcthreshold_abs_range = Column(String(20))
    intthresholdmin = Column(Numeric(30, 6))
    intthresholdmax = Column(Numeric(30, 6))
    intprecision = Column(Numeric(12, 10))
    isactive = Column(BIT(1), default=text("B'1'"), nullable=False)
    intcreatedby = Column(Integer)
    dtcreatedat = Column(DateTime, default=func.now(), nullable=False)
    intupdatedby = Column(Integer)
    dtupdatedat = Column(DateTime, onupdate=func.now())
    
    # Relationships
    subproduct = relationship("SubproductMaster", back_populates="ratios")
    details = relationship("RatioDetails", back_populates="master", cascade="all, delete-orphan")
    configurations = relationship("RatioConfiguration", back_populates="ratio", cascade="all, delete-orphan")
    
    def to_dict(self):
        """Convert to dictionary"""
        return {
            'intratiomasterid': self.intratiomasterid,
            'intsubproductid': self.intsubproductid,
            'vcsourcetype': self.vcsourcetype,
            'vctype': self.vctype,
            'vcrationame': self.vcrationame,
            'isratio_subtotal': self.isratio_subtotal,
            'vcdescription': self.vcdescription,
            'intthreshold': float(self.intthreshold) if self.intthreshold else None,
            'vcthresholdtype': self.vcthresholdtype,
            'vcthreshold_abs_range': self.vcthreshold_abs_range,
            'intthresholdmin': float(self.intthresholdmin) if self.intthresholdmin else None,
            'intthresholdmax': float(self.intthresholdmax) if self.intthresholdmax else None,
            'intprecision': float(self.intprecision) if self.intprecision else None,
            'isactive': self.isactive,
            'intcreatedby': self.intcreatedby,
            'dtcreatedat': self.dtcreatedat.isoformat() if self.dtcreatedat else None,
            'intupdatedby': self.intupdatedby,
            'dtupdatedat': self.dtupdatedat.isoformat() if self.dtupdatedat else None
        }

class RatioDetails(Base):
    """Ratio Details table for managing ratio data model specifications"""
    __tablename__ = 'tbl_ratio_details'
    __table_args__ = {'schema': 'validus'}
    
    intratiodetailid = Column(Integer, primary_key=True, autoincrement=True)
    intratiomasterid = Column(Integer, ForeignKey(f'validus.tbl_ratio_master.intratiomasterid'), nullable=False, index=True)
    intdatamodelid = Column(Text, nullable=False, index=True)  # Changed from Integer to Text to support ranged ratios
    vcfilter = Column(Text)
    vcfiltertype = Column(String(1))
    vcnumerator = Column(Text)
    vcdenominator = Column(Text)
    vcformula = Column(Text)
    intcreatedby = Column(Integer)
    dtcreatedat = Column(DateTime, default=func.now(), nullable=False)
    intupdatedby = Column(Integer)
    dtupdatedat = Column(DateTime, onupdate=func.now())
    
    # Relationships
    master = relationship("RatioMaster", back_populates="details")
    # Note: datamodel relationship removed because intdatamodelid is now Text (can contain comma-separated IDs for ranged ratios)
    # If you need to access data models, parse intdatamodelid and query DataModelMaster separately
    
    def to_dict(self):
        """Convert to dictionary"""
        return {
            'intratiodetailid': self.intratiodetailid,
            'intratiomasterid': self.intratiomasterid,
            'intdatamodelid': self.intdatamodelid,
            'vcfilter': self.vcfilter,
            'vcfiltertype': self.vcfiltertype,
            'vcnumerator': self.vcnumerator,
            'vcdenominator': self.vcdenominator,
            'vcformula': self.vcformula,
            'intcreatedby': self.intcreatedby,
            'dtcreatedat': self.dtcreatedat.isoformat() if self.dtcreatedat else None,
            'intupdatedby': self.intupdatedby,
            'dtupdatedat': self.dtupdatedat.isoformat() if self.dtupdatedat else None
        }


class DataModelMaster(Base):
    """Data Model Master table for managing data model definitions"""
    __tablename__ = 'tbl_data_model_master'
    __table_args__ = {'schema': 'validus'}
    
    intdatamodelid = Column(Integer, primary_key=True, autoincrement=True)
    vcmodelname = Column(String(250))
    vcdescription = Column(String(500))
    vcmodelid = Column(String(100))
    vccategory = Column(String(100))
    vcsource = Column(String(100))
    vctablename = Column(String(250))
    isactive = Column(Boolean, default=True, nullable=False)
    intcreatedby = Column(Integer)
    dtcreatedat = Column(DateTime, default=func.now(), nullable=False)
    intupdatedby = Column(Integer)
    dtupdatedat = Column(DateTime, onupdate=func.now())
    
    # Relationships
    details = relationship("DataModelDetails", back_populates="master", cascade="all, delete-orphan")
    
    def to_dict(self):
        """Convert to dictionary"""
        return {
            'intdatamodelid': self.intdatamodelid,
            'vcmodelname': self.vcmodelname,
            'vcdescription': self.vcdescription,
            'vcmodelid': self.vcmodelid,
            'vccategory': self.vccategory,
            'vcsource': self.vcsource,
            'vctablename': self.vctablename,
            'isactive': self.isactive if self.isactive is not None else True,
            'intcreatedby': self.intcreatedby,
            'dtcreatedat': self.dtcreatedat.isoformat() if self.dtcreatedat else None,
            'intupdatedby': self.intupdatedby,
            'dtupdatedat': self.dtupdatedat.isoformat() if self.dtupdatedat else None
        }

class DataModelDetails(Base):
    """Data Model Details table for managing column definitions"""
    __tablename__ = 'tbl_data_model_details'
    __table_args__ = {'schema': 'validus'}
    
    intdatamodeldetailid = Column(Integer, primary_key=True, autoincrement=True)
    intdatamodelid = Column(Integer, ForeignKey(f'validus.tbl_data_model_master.intdatamodelid'), nullable=False, index=True)
    vcfieldname = Column(String(250))
    vcfielddescription = Column(String(500))
    vcdatatype = Column(String(100))
    intlength = Column(Integer)
    intprecision = Column(Integer)
    intscale = Column(Integer)
    vcdateformat = Column(String(100))
    vcdmcolumnname = Column(String(250))
    vcdefaultvalue = Column(String(255))
    ismandatory = Column(Boolean)
    intdisplayorder = Column(Integer)
    intcreatedby = Column(Integer)
    dtcreatedat = Column(DateTime, default=func.now(), nullable=False)
    intupdatedby = Column(Integer)
    dtupdatedat = Column(DateTime, onupdate=func.now())
    
    # Relationships
    master = relationship("DataModelMaster", back_populates="details")
    
    def to_dict(self):
        """Convert to dictionary"""
        return {
            'intdatamodeldetailid': self.intdatamodeldetailid,
            'intdatamodelid': self.intdatamodelid,
            'vcfieldname': self.vcfieldname,
            'vcfielddescription': self.vcfielddescription,
            'vcdatatype': self.vcdatatype,
            'intlength': self.intlength,
            'intprecision': self.intprecision,
            'intscale': self.intscale,
            'vcdateformat': self.vcdateformat,
            'vcdmcolumnname': self.vcdmcolumnname,
            'vcdefaultvalue': self.vcdefaultvalue,
            'ismandatory': self.ismandatory,
            'intdisplayorder': self.intdisplayorder,
            'intcreatedby': self.intcreatedby,
            'dtcreatedat': self.dtcreatedat.isoformat() if self.dtcreatedat else None,
            'intupdatedby': self.intupdatedby,
            'dtupdatedat': self.dtupdatedat.isoformat() if self.dtupdatedat else None
        }

class DataLoadInstance(Base):
    """Data Load Instance table for tracking data loads"""
    __tablename__ = 'tbl_data_load_instance'
    __table_args__ = {'schema': 'validus'}
    
    intdataloadinstanceid = Column(Integer, primary_key=True, autoincrement=True)
    intclientid = Column(Integer, nullable=True, index=True)
    intfundid = Column(Integer, nullable=True, index=True)
    vccurrency = Column(String(10), nullable=True)
    intdatamodelid = Column(Integer, ForeignKey(f'validus.tbl_data_model_master.intdatamodelid'), nullable=True, index=True)
    dtdataasof = Column(Date, nullable=True)
    vcdatadate = Column(String(250), nullable=True)
    vcdatasourcetype = Column(String(100), nullable=True)
    vcdatasourcename = Column(String(100), nullable=True)
    vcloadtype = Column(String(100), default='Manual', nullable=True)
    vcloadstatus = Column(String(100), nullable=True)
    vcdataloaddescription = Column(String(500), nullable=True)
    intloadedby = Column(Integer, nullable=True)
    dtloadedat = Column(DateTime, default=func.now(), nullable=True)
    
    # Relationships
    data_model = relationship("DataModelMaster", foreign_keys=[intdatamodelid])
    
    def to_dict(self):
        """Convert to dictionary"""
        return {
            'intdataloadinstanceid': self.intdataloadinstanceid,
            'intclientid': self.intclientid,
            'intfundid': self.intfundid,
            'vccurrency': self.vccurrency,
            'intdatamodelid': self.intdatamodelid,
            'dtdataasof': self.dtdataasof.isoformat() if self.dtdataasof else None,
            'vcdatadate': self.vcdatadate,
            'vcdatasourcetype': self.vcdatasourcetype,
            'vcdatasourcename': self.vcdatasourcename,
            'vcloadtype': self.vcloadtype,
            'vcloadstatus': self.vcloadstatus,
            'vcdataloaddescription': self.vcdataloaddescription,
            'intloadedby': self.intloadedby,
            'dtloadedat': self.dtloadedat.isoformat() if self.dtloadedat else None
        }

class ValidationConfiguration(Base):
    """Validation Configuration table for managing validation configuration"""
    __tablename__ = 'tbl_validation_configuration'
    __table_args__ = {'schema': 'validus'}
    
    intvalidationconfigurationid = Column(Integer, primary_key=True, autoincrement=True)
    intclientid = Column(Integer, nullable=True, index=True)
    intfundid = Column(Integer, nullable=True, index=True)
    intvalidationmasterid = Column(Integer, ForeignKey(f'validus.tbl_validation_master.intvalidationmasterid'), nullable=False, index=True)
    isactive = Column(Boolean, default=False, nullable=False)
    vccondition = Column(String(100))
    intthreshold = Column(Numeric(12, 4))
    vcthresholdtype = Column(String(100))
    vcthreshold_abs_range = Column(String(20))  # 'Absolute' or 'Range'
    intthresholdmin = Column(Numeric(30, 6))  # Minimum threshold for range
    intthresholdmax = Column(Numeric(30, 6))  # Maximum threshold for range
    intprecision = Column(Numeric(12, 10))
    intcreatedby = Column(Integer)
    dtcreatedat = Column(DateTime, default=func.now(), nullable=False)
    intupdatedby = Column(Integer)
    dtupdatedat = Column(DateTime, onupdate=func.now())
    
    # Relationships
    validation = relationship("ValidationMaster", back_populates="configurations")
    
    def to_dict(self):
        """Convert to dictionary"""
        return {
            'intvalidationconfigurationid': self.intvalidationconfigurationid,
            'intclientid': self.intclientid,
            'intfundid': self.intfundid,
            'intvalidationmasterid': self.intvalidationmasterid,
            'isactive': self.isactive,
            'vccondition': self.vccondition,
            'intthreshold': float(self.intthreshold) if self.intthreshold else None,
            'vcthresholdtype': self.vcthresholdtype,
            'vcthreshold_abs_range': self.vcthreshold_abs_range,
            'intthresholdmin': float(self.intthresholdmin) if self.intthresholdmin else None,
            'intthresholdmax': float(self.intthresholdmax) if self.intthresholdmax else None,
            'intprecision': float(self.intprecision) if self.intprecision else None,
            'intcreatedby': self.intcreatedby,
            'dtcreatedat': self.dtcreatedat.isoformat() if self.dtcreatedat else None,
            'intupdatedby': self.intupdatedby,
            'dtupdatedat': self.dtupdatedat.isoformat() if self.dtupdatedat else None
        }


class RatioConfiguration(Base):
    """Ratio Configuration table for managing ratio configuration"""
    __tablename__ = 'tbl_ratio_configuration'
    __table_args__ = {'schema': 'validus'}
    
    intratioconfigurationid = Column(Integer, primary_key=True, autoincrement=True)
    intclientid = Column(Integer, nullable=True, index=True)
    intfundid = Column(Integer, nullable=True, index=True)
    intratiomasterid = Column(Integer, ForeignKey(f'validus.tbl_ratio_master.intratiomasterid'), nullable=False, index=True)
    isactive = Column(Boolean, default=False, nullable=False)
    vccondition = Column(String(100))
    intthreshold = Column(Numeric(12, 4))
    vcthresholdtype = Column(String(100))
    vcthreshold_abs_range = Column(String(20))  # 'Absolute' or 'Range'
    intthresholdmin = Column(Numeric(30, 6))  # Minimum threshold for range
    intthresholdmax = Column(Numeric(30, 6))  # Maximum threshold for range
    intprecision = Column(Numeric(12, 10))
    intcreatedby = Column(Integer)
    dtcreatedat = Column(DateTime, default=func.now(), nullable=False)
    intupdatedby = Column(Integer)
    dtupdatedat = Column(DateTime, onupdate=func.now())
    
    # Relationships
    ratio = relationship("RatioMaster", back_populates="configurations")
    
    def to_dict(self):
        """Convert to dictionary"""
        return {
            'intratioconfigurationid': self.intratioconfigurationid,
            'intclientid': self.intclientid,
            'intfundid': self.intfundid,
            'intratiomasterid': self.intratiomasterid,
            'isactive': self.isactive,
            'vccondition': self.vccondition,
            'intthreshold': float(self.intthreshold) if self.intthreshold else None,
            'vcthresholdtype': self.vcthresholdtype,
            'vcthreshold_abs_range': self.vcthreshold_abs_range,
            'intthresholdmin': float(self.intthresholdmin) if self.intthresholdmin else None,
            'intthresholdmax': float(self.intthresholdmax) if self.intthresholdmax else None,
            'intprecision': float(self.intprecision) if self.intprecision else None,
            'intcreatedby': self.intcreatedby,
            'dtcreatedat': self.dtcreatedat.isoformat() if self.dtcreatedat else None,
            'intupdatedby': self.intupdatedby,
            'dtupdatedat': self.dtupdatedat.isoformat() if self.dtupdatedat else None
        }
    
class ProcessInstance(Base):
    """Process Instance table for tracking validation/ratio process runs"""
    __tablename__ = 'tbl_process_instance'
    __table_args__ = {'schema': 'validus'}
    
    intprocessinstanceid = Column(BigInteger, primary_key=True, autoincrement=True)
    intclientid = Column(Integer, nullable=True, index=True)
    intfundid = Column(Integer, nullable=True, index=True)
    vccurrency = Column(String(10), nullable=True)
    vcvalidustype = Column(String(100), nullable=True)
    vcsourcetype = Column(String(50), nullable=True)
    vcsource_a = Column(String(250), nullable=True)
    vcsource_b = Column(String(250), nullable=True)
    dtdate_a = Column(Date, nullable=True)
    dtdate_b = Column(Date, nullable=True)
    dtprocesstime_start = Column(DateTime, default=func.now(), nullable=True)
    dtprocesstime_end = Column(DateTime, nullable=True)
    vcprocessstats = Column(String(50), nullable=True)
    vcstatusdescription = Column(String(250), nullable=True)
    intuserid = Column(Integer, nullable=True)
    
    # Relationships
    details = relationship("ProcessInstanceDetails", back_populates="process_instance")
    
    def to_dict(self):
        """Convert to dictionary"""
        return {
            'intprocessinstanceid': self.intprocessinstanceid,
            'intclientid': self.intclientid,
            'intfundid': self.intfundid,
            'vccurrency': self.vccurrency,
            'vcvalidustype': self.vcvalidustype,
            'vcsourcetype': self.vcsourcetype,
            'vcsource_a': self.vcsource_a,
            'vcsource_b': self.vcsource_b,
            'dtdate_a': self.dtdate_a.isoformat() if self.dtdate_a else None,
            'dtdate_b': self.dtdate_b.isoformat() if self.dtdate_b else None,
            'dtprocesstime_start': self.dtprocesstime_start.isoformat() if self.dtprocesstime_start else None,
            'dtprocesstime_end': self.dtprocesstime_end.isoformat() if self.dtprocesstime_end else None,
            'vcprocessstats': self.vcprocessstats,
            'vcstatusdescription': self.vcstatusdescription,
            'intuserid': self.intuserid
        }
    
class ProcessInstanceDetails(Base):
    """Process Instance Details table for tracking individual data load instances in a process"""
    __tablename__ = 'tbl_process_instance_details'
    __table_args__ = {'schema': 'validus'}
    
    intprocessinstancedetailid = Column(BigInteger, primary_key=True, autoincrement=True)
    intprocessinstanceid = Column(BigInteger, ForeignKey(f'validus.tbl_process_instance.intprocessinstanceid'), nullable=True, index=True)
    intdataloadinstanceid = Column(BigInteger, nullable=True, index=True)
    dtprocesstime = Column(DateTime, default=func.now(), nullable=True)
    
    # Relationships
    process_instance = relationship("ProcessInstance", back_populates="details")
    
    def to_dict(self):
        """Convert to dictionary"""
        return {
            'intprocessinstancedetailid': self.intprocessinstancedetailid,
            'intprocessinstanceid': self.intprocessinstanceid,
            'intdataloadinstanceid': self.intdataloadinstanceid,
            'dtprocesstime': self.dtprocesstime.isoformat() if self.dtprocesstime else None
        }


# Factory functions for dynamic schema models
# Cache for created models to avoid recreating them
_validation_result_model_cache = {}
_ratio_result_model_cache = {}

def create_validation_result_model(schema_name: str = 'validus'):
    """
    Factory function to create ValidationResult model with specified schema.
    
    Args:
        schema_name: The schema name where the table exists (e.g., 'nexbridge', 'validus')
    
    Returns:
        A SQLAlchemy model class for ValidationResult
    """
    # Check cache first
    if schema_name in _validation_result_model_cache:
        return _validation_result_model_cache[schema_name]
    
    # Create unique class name based on schema to avoid replacement warnings
    class_name = f"ValidationResult_{schema_name.replace('.', '_').replace('-', '_')}"
    
    class ValidationResult(Base):
        """Validation Result table - can exist in any schema with same structure"""
        __tablename__ = 'tbl_validation_result'
        __table_args__ = {'schema': schema_name, 'extend_existing': True}
        
        intvalidationresultid = Column(BigInteger, primary_key=True, autoincrement=True)
        intprocessinstanceid = Column(BigInteger, ForeignKey(f'validus.tbl_process_instance.intprocessinstanceid'), nullable=True, index=True)
        intdatamodelid = Column(Integer, ForeignKey(f'validus.tbl_data_model_master.intdatamodelid'), nullable=True, index=True)
        intvalidationconfigurationid = Column(Integer, ForeignKey(f'validus.tbl_validation_configuration.intvalidationconfigurationid'), nullable=True, index=True)
        intdmrecid = Column(BigInteger, nullable=True)
        vcside = Column(String(1), nullable=True)
        intsideuniqueid = Column(BigInteger, nullable=True)
        intmatchid = Column(BigInteger, nullable=True)
        intformulaoutput = Column(Numeric(32, 6), nullable=True)
        vcformulaoutput = Column(Text, nullable=True)
        vcstatus = Column(String(100), nullable=True)
        vcaction = Column(String(100), nullable=True)
        intactionuserid = Column(Integer, nullable=True)
        dtactiontime = Column(DateTime, nullable=True)
        intassignedtouserid = Column(Integer, nullable=True)
        vcassignedstatus = Column(String(100), nullable=True)
        intnewvalue = Column(Numeric(32, 6), nullable=True)
        vccomment = Column(String(500), nullable=True)
        isactive = Column(Boolean, default=True, nullable=True)
        
        # Relationships - Note: These reference tables in 'validus' schema
        process_instance = relationship("ProcessInstance", foreign_keys=[intprocessinstanceid])
        data_model = relationship("DataModelMaster", foreign_keys=[intdatamodelid])
        validation_configuration = relationship("ValidationConfiguration", foreign_keys=[intvalidationconfigurationid])
        
        def to_dict(self):
            """Convert to dictionary"""
            return {
                'intvalidationresultid': self.intvalidationresultid,
                'intprocessinstanceid': self.intprocessinstanceid,
                'intdatamodelid': self.intdatamodelid,
                'intvalidationconfigurationid': self.intvalidationconfigurationid,
                'intdmrecid': self.intdmrecid,
                'vcside': self.vcside,
                'intsideuniqueid': self.intsideuniqueid,
                'intmatchid': self.intmatchid,
                'intformulaoutput': float(self.intformulaoutput) if self.intformulaoutput is not None else None,
                'vcformulaoutput': self.vcformulaoutput,
                'vcstatus': self.vcstatus,
                'vcaction': self.vcaction,
                'intactionuserid': self.intactionuserid,
                'dtactiontime': self.dtactiontime.isoformat() if self.dtactiontime else None,
                'intassignedtouserid': self.intassignedtouserid,
                'vcassignedstatus': self.vcassignedstatus,
                'intnewvalue': float(self.intnewvalue) if self.intnewvalue is not None else None,
                'vccomment': self.vccomment,
                'isactive': self.isactive if self.isactive is not None else True
            }
    
    # Set the schema name as a class attribute for reference
    ValidationResult.__schema_name__ = schema_name
    # Rename the class to avoid replacement warnings
    ValidationResult.__name__ = class_name
    ValidationResult.__qualname__ = class_name
    
    # Cache the model for reuse
    _validation_result_model_cache[schema_name] = ValidationResult
    return ValidationResult


def create_ratio_result_model(schema_name: str = 'validus'):
    """
    Factory function to create RatioResult model with specified schema.
    
    Args:
        schema_name: The schema name where the table exists (e.g., 'nexbridge', 'validus')
    
    Returns:
        A SQLAlchemy model class for RatioResult
    """
    # Check cache first
    if schema_name in _ratio_result_model_cache:
        return _ratio_result_model_cache[schema_name]
    
    # Create unique class name based on schema to avoid replacement warnings
    class_name = f"RatioResult_{schema_name.replace('.', '_').replace('-', '_')}"
    
    class RatioResult(Base):
        """Ratio Result table - can exist in any schema with same structure"""
        __tablename__ = 'tbl_ratio_result'
        __table_args__ = {'schema': schema_name, 'extend_existing': True}
        
        intratioresultid = Column(BigInteger, primary_key=True, autoincrement=True)
        intprocessinstanceid = Column(BigInteger, ForeignKey(f'validus.tbl_process_instance.intprocessinstanceid'), nullable=True, index=True)
        intdatamodelid = Column(Integer, ForeignKey(f'validus.tbl_data_model_master.intdatamodelid'), nullable=True, index=True)
        intratioconfigurationid = Column(Integer, ForeignKey(f'validus.tbl_ratio_configuration.intratioconfigurationid'), nullable=True, index=True)
        vcside = Column(String(1), nullable=True)  # A or B
        intsideuniqueid = Column(BigInteger, nullable=True)
        intmatchid = Column(BigInteger, nullable=True)
        intnumeratoroutput = Column(Numeric(32, 6), nullable=True)
        intdenominatoroutput = Column(Numeric(32, 6), nullable=True)
        intformulaoutput = Column(Numeric(32, 6), nullable=True)
        vcformulaoutput = Column(Text, nullable=True)
        vcstatus = Column(String(100), nullable=True)  # Passed / Failed etc
        vcaction = Column(String(100), nullable=True)  # No change/ Override / Assign etc
        intactionuserid = Column(Integer, nullable=True)
        dtactiontime = Column(DateTime, nullable=True)
        vccomment = Column(String(500), nullable=True)
        isactive = Column(Boolean, default=True, nullable=True)
        
        # Relationships - Note: These reference tables in 'validus' schema
        process_instance = relationship("ProcessInstance", foreign_keys=[intprocessinstanceid])
        data_model = relationship("DataModelMaster", foreign_keys=[intdatamodelid])
        ratio_configuration = relationship("RatioConfiguration", foreign_keys=[intratioconfigurationid])
        
        def to_dict(self):
            """Convert to dictionary"""
            return {
                'intratioresultid': self.intratioresultid,
                'intprocessinstanceid': self.intprocessinstanceid,
                'intdatamodelid': self.intdatamodelid,
                'intratioconfigurationid': self.intratioconfigurationid,
                'vcside': self.vcside,
                'intsideuniqueid': self.intsideuniqueid,
                'intmatchid': self.intmatchid,
                'intnumeratoroutput': float(self.intnumeratoroutput) if self.intnumeratoroutput is not None else None,
                'intdenominatoroutput': float(self.intdenominatoroutput) if self.intdenominatoroutput is not None else None,
                'intformulaoutput': float(self.intformulaoutput) if self.intformulaoutput is not None else None,
                'vcformulaoutput': self.vcformulaoutput,
                'vcstatus': self.vcstatus,
                'vcaction': self.vcaction,
                'intactionuserid': self.intactionuserid,
                'dtactiontime': self.dtactiontime.isoformat() if self.dtactiontime else None,
                'vccomment': self.vccomment,
                'isactive': self.isactive if self.isactive is not None else True
            }
    
    # Set the schema name as a class attribute for reference
    RatioResult.__schema_name__ = schema_name
    # Rename the class to avoid replacement warnings
    RatioResult.__name__ = class_name
    RatioResult.__qualname__ = class_name
    
    # Cache the model for reuse
    _ratio_result_model_cache[schema_name] = RatioResult
    return RatioResult


# Note: Do not create default instances here to avoid SQLAlchemy warnings
# Always use the factory functions create_validation_result_model() and create_ratio_result_model()
# with the desired schema name when you need to use these models

if __name__ == "__main__":
    # Initialize database manager
    db_manager = DatabaseManager()
    
    # Create tables
    db_manager.create_tables()
    
    # Example: Get user permissions
    result = db_manager.get_user_permissions("zeeshan")
    print(result)
    
    # Example: Check specific permission
    has_permission = db_manager.check_user_permission("zeeshan", "frame", "read")
    print(f"Zeeshan has frame.read permission: {has_permission}")
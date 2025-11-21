from sqlalchemy import Column, Integer, String, Boolean, DateTime, ForeignKey, Text, CheckConstraint, UniqueConstraint, JSON
from sqlalchemy.orm import relationship, foreign, remote
from sqlalchemy.sql import func, text
import os
import sys
import sqlalchemy as sa
from typing import TYPE_CHECKING, Any, Optional, List, Dict, Type, TypeVar, Union

# Import Base from database_models to avoid circular imports
from database_models import Base

# For type hints
if TYPE_CHECKING:
    from database_models import Client, Role, Module, Permission

class RoleOrClientBasedModuleLevelPermission(Base):
    """
    Role or Client based Module/Master Level Permissions
    
    This table stores permissions that can be assigned to either:
    - A role (role_id) or a client (client_id)
    - For either a module (module_id) or a master (master_id)
    """
    __tablename__ = 'role_or_client_based_module_level_permissions'
    __table_args__ = (
        # Either role_id or client_id must be set (but not both)
        CheckConstraint(
            """
            (role_id IS NOT NULL AND client_id IS NULL AND client_has_permission IS NULL) OR 
            (role_id IS NULL AND client_id IS NOT NULL AND permission_id IS NULL)
            """,
            name='chk_role_or_client'
        ),
        # Either module_id or master_id must be set (but not both)
        CheckConstraint(
            '(module_id IS NOT NULL AND master_id IS NULL) OR (module_id IS NULL AND master_id IS NOT NULL)',
            name='chk_module_or_master'
        ),
        # client_has_permission must be set when client_id is set
        CheckConstraint(
            """
            (client_id IS NULL AND client_has_permission IS NULL) OR 
            (client_id IS NOT NULL AND client_has_permission IS NOT NULL)
            """,
            name='chk_client_permission'
        ),
        # Unique constraint to prevent duplicates for role-based permissions
        # This will be implemented as a partial index in the migration
        # to handle the condition where permission_id is not null
        
        # Unique constraint for client-based permissions
        # This will be implemented as a partial index in the migration
        # to handle the condition where client_id is not null
        {'schema': os.getenv('DB_SCHEMA', 'public')}
    )
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    
    # Role or Client (one must be set)
    role_id = Column(
        Integer, 
        ForeignKey(f'{os.getenv("DB_SCHEMA", "public")}.roles.id', ondelete='CASCADE'), 
        nullable=True, 
        index=True
    )
    client_id = Column(
        Integer, 
        ForeignKey(f'{os.getenv("DB_SCHEMA", "public")}.clients.id', ondelete='CASCADE'), 
        nullable=True, 
        index=True
    )
    
    # Module or Master (one must be set)
    module_id = Column(
        Integer, 
        ForeignKey(f'{os.getenv("DB_SCHEMA", "public")}.modules.id', ondelete='CASCADE'), 
        nullable=True, 
        index=True
    )
    master_id = Column(
        Integer, 
        ForeignKey(f'{os.getenv("DB_SCHEMA", "public")}.masters.id', ondelete='CASCADE'), 
        nullable=True, 
        index=True
    )
    
    # Permission (only used for role-based permissions)
    permission_id = Column(
        Integer, 
        ForeignKey(f'{os.getenv("DB_SCHEMA", "public")}.permissions.id', ondelete='CASCADE'), 
        nullable=True,  # Nullable for client-based permissions
        index=True
    )
    
    # Client permission flag (only used for client-based permissions)
    client_has_permission = Column(
        Boolean,
        nullable=True,  # Nullable for role-based permissions
        index=True,
        comment='Boolean flag indicating if the client has permission (only used when client_id is set)'
    )
    
    # Status
    is_active = Column(Boolean, default=True, index=True)
    created_at = Column(DateTime, default=func.now())
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now())
    
    # Relationships with string-based class names to avoid circular imports
    role = relationship("Role", back_populates="role_permissions")
    client = relationship("Client", back_populates="client_permissions")
    module = relationship("Module", back_populates="permissions")
    master = relationship("Master", back_populates="permissions")
    permission = relationship("Permission", back_populates="role_permissions", viewonly=True)
    
    def __repr__(self):
        return (f"<RoleOrClientBasedModuleLevelPermission(id={self.id}, "
                f"role_id={self.role_id}, client_id={self.client_id}, "
                f"module_id={self.module_id}, master_id={self.master_id}, "
                f"permission_id={self.permission_id})>")


class Master(Base):
    """Master model for master-level permissions"""
    __tablename__ = 'masters'
    __table_args__ = {'schema': os.getenv('DB_SCHEMA', 'public')}
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(100), nullable=False)
    code = Column(String(50), unique=True, nullable=False, index=True)
    description = Column(Text, nullable=True)
    is_active = Column(Boolean, default=True, index=True)
    created_at = Column(DateTime, default=func.now())
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now())
    
    # Relationships
    permissions = relationship(
        "RoleOrClientBasedModuleLevelPermission",
        primaryjoin="and_(Master.id==foreign(remote(RoleOrClientBasedModuleLevelPermission.master_id)))",
        back_populates="master",
        viewonly=True,
        lazy="selectin"
    )
    
    def __repr__(self):
        return f"<Master(id={self.id}, name='{self.name}', code='{self.code}')>"

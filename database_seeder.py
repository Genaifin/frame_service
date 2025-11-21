#!/usr/bin/env python3
"""
Database Data Seeding Script
This script handles data seeding for the ValidusBoxes system

Handles the following data seeding:
1. User Management: users, clients, roles, modules, permissions, user_client_roles, role_module_permissions
"""

import os
import json
import bcrypt
from database_models import (
    User, Client, Role, Module, Permission, 
    RoleOrClientBasedModuleLevelPermission,
    DatabaseManager, FundManager
)
from typing import Dict, Any
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DatabaseSeeder:
    """Database seeding for ValidusBoxes system data"""
    
    def __init__(self, db_manager: DatabaseManager = None):
        self.db_manager = db_manager or DatabaseManager()
    
    def _hash_password(self, password: str) -> str:
        """Hash a password using bcrypt"""
        # Generate salt and hash the password
        salt = bcrypt.gensalt()
        hashed = bcrypt.hashpw(password.encode('utf-8'), salt)
        return hashed.decode('utf-8')
    
    def insert_initial_data(self):
        """Insert initial data for modules, permissions, roles, and clients"""
        session = self.db_manager.get_session()
        try:
            # Insert modules with hierarchical structure
            logger.info("Inserting modules with hierarchy...")
            
            # Define hierarchical module structure
            modules_hierarchy = [
                # Root modules (level 0)
                {'module_name': 'Frame', 'module_code': 'frame', 'description': 'Frame module for document processing', 'level': 0, 'sort_order': 1},
                {'module_name': 'NAV Validus', 'module_code': 'nav_validus', 'description': 'NAV Validus module for NAV processing', 'level': 0, 'sort_order': 2},
                {'module_name': 'Resolve', 'module_code': 'resolve', 'description': 'Resolve module for conflict resolution', 'level': 0, 'sort_order': 3},
                {'module_name': 'Kube', 'module_code': 'kube', 'description': 'Kube module for container management', 'level': 0, 'sort_order': 4},
                {'module_name': 'AIMS', 'module_code': 'aims', 'description': 'AIMS module for asset management', 'level': 0, 'sort_order': 5},
            ]
            
            # Handle cleanup of old 'Validus' module first
            old_validus = session.query(Module).filter(Module.module_name == 'Validus', Module.module_code == 'validus').first()
            if old_validus:
                # Check if NAV Validus already exists
                nav_validus_exists = session.query(Module).filter(Module.module_name == 'NAV Validus').first()
                if nav_validus_exists:
                    # Delete the old Validus module
                    session.delete(old_validus)
                    logger.info("Deleted duplicate 'Validus' module (keeping 'NAV Validus')")
                else:
                    # Rename Validus to NAV Validus
                    old_validus.module_name = 'NAV Validus'
                    old_validus.module_code = 'nav_validus'
                    old_validus.description = 'NAV Validus module for NAV processing'
                    logger.info("Renamed 'Validus' to 'NAV Validus'")
            
            # Insert root modules first
            root_modules_map = {}
            for module_data in modules_hierarchy:
                existing = session.query(Module).filter(Module.module_code == module_data['module_code']).first()
                if not existing:
                    module = Module(**module_data)
                    session.add(module)
                    session.flush()  # Flush to get the ID
                    root_modules_map[module_data['module_code']] = module.id
                    logger.info(f"Added root module: {module_data['module_name']}")
                else:
                    # Update existing module with hierarchy fields
                    existing.level = module_data['level']
                    existing.sort_order = module_data['sort_order']
                    root_modules_map[module_data['module_code']] = existing.id
                    logger.info(f"Updated existing module: {module_data['module_name']}")
            
            # Define submodules (level 1)
            submodules_data = [
                {'module_name': 'Dashboard', 'module_code': 'dashboard', 'description': 'Dashboard submodule', 'parent_code': 'frame', 'level': 1, 'sort_order': 1},
                {'module_name': 'File Manager', 'module_code': 'file_manager', 'description': 'File Manager submodule', 'parent_code': 'frame', 'level': 1, 'sort_order': 2},
                {'module_name': 'Single Fund', 'module_code': 'single_fund', 'description': 'Single Fund submodule', 'parent_code': 'nav_validus', 'level': 1, 'sort_order': 1},
                {'module_name': 'Multi Fund', 'module_code': 'multi_fund', 'description': 'Multi Fund submodule', 'parent_code': 'nav_validus', 'level': 1, 'sort_order': 2},
            ]
            
            # Insert submodules
            submodules_map = {}
            for submodule_data in submodules_data:
                existing = session.query(Module).filter(Module.module_code == submodule_data['module_code']).first()
                if not existing:
                    parent_id = root_modules_map.get(submodule_data['parent_code'])
                    if parent_id:
                        submodule = Module(
                            module_name=submodule_data['module_name'],
                            module_code=submodule_data['module_code'],
                            description=submodule_data['description'],
                            parent_id=parent_id,
                            level=submodule_data['level'],
                            sort_order=submodule_data['sort_order']
                        )
                        session.add(submodule)
                        session.flush()  # Flush to get the ID
                        submodules_map[submodule_data['module_name']] = submodule.id
                        logger.info(f"Added submodule: {submodule_data['module_name']}")
                else:
                    # Update existing submodule
                    parent_id = root_modules_map.get(submodule_data['parent_code'])
                    if parent_id:
                        existing.parent_id = parent_id
                        existing.level = submodule_data['level']
                        existing.sort_order = submodule_data['sort_order']
                        submodules_map[submodule_data['module_name']] = existing.id
                        logger.info(f"Updated existing submodule: {submodule_data['module_name']}")
            
            # Define sub-submodules (level 2)
            subsubmodules_data = [
                {'module_name': 'Statuswise Dashboard', 'module_code': 'statuswise_dashboard', 'description': 'Statuswise Dashboard sub-submodule', 'parent_name': 'Dashboard', 'level': 2, 'sort_order': 1},
                {'module_name': 'Completeness Dashboard', 'module_code': 'completeness_dashboard', 'description': 'Completeness Dashboard sub-submodule', 'parent_name': 'Dashboard', 'level': 2, 'sort_order': 2},
                {'module_name': 'File Info', 'module_code': 'file_info', 'description': 'File Info sub-submodule', 'parent_name': 'File Manager', 'level': 2, 'sort_order': 1},
                {'module_name': 'NAV Validations', 'module_code': 'nav_validations', 'description': 'NAV Validations sub-submodule', 'parent_name': 'Single Fund', 'level': 2, 'sort_order': 1},
                {'module_name': 'Ratio Validations', 'module_code': 'ratio_validations', 'description': 'Ratio Validations sub-submodule', 'parent_name': 'Single Fund', 'level': 2, 'sort_order': 2},
                {'module_name': 'Validations', 'module_code': 'validations', 'description': 'Validations sub-submodule', 'parent_name': 'Multi Fund', 'level': 2, 'sort_order': 1},
            ]
            
            # Insert sub-submodules
            for subsubmodule_data in subsubmodules_data:
                existing = session.query(Module).filter(Module.module_code == subsubmodule_data['module_code']).first()
                if not existing:
                    parent_id = submodules_map.get(subsubmodule_data['parent_name'])
                    if parent_id:
                        subsubmodule = Module(
                            module_name=subsubmodule_data['module_name'],
                            module_code=subsubmodule_data['module_code'],
                            description=subsubmodule_data['description'],
                            parent_id=parent_id,
                            level=subsubmodule_data['level'],
                            sort_order=subsubmodule_data['sort_order']
                        )
                        session.add(subsubmodule)
                        logger.info(f"Added sub-submodule: {subsubmodule_data['module_name']}")
                else:
                    # Update existing sub-submodule
                    parent_id = submodules_map.get(subsubmodule_data['parent_name'])
                    if parent_id:
                        existing.parent_id = parent_id
                        existing.level = subsubmodule_data['level']
                        existing.sort_order = subsubmodule_data['sort_order']
                        logger.info(f"Updated existing sub-submodule: {subsubmodule_data['module_name']}")
            
            session.commit()
            logger.info("Modules with hierarchy inserted successfully!")
            
            # Insert permissions
            logger.info("Inserting permissions...")
            permissions_data = [
                {'permission_name': 'Create', 'permission_code': 'create', 'description': 'Create new records'},
                {'permission_name': 'Read', 'permission_code': 'read', 'description': 'Read/view records'},
                {'permission_name': 'Update', 'permission_code': 'update', 'description': 'Update existing records'},
                {'permission_name': 'Delete', 'permission_code': 'delete', 'description': 'Delete records'}
            ]
            
            for permission_data in permissions_data:
                existing = session.query(Permission).filter(Permission.permission_code == permission_data['permission_code']).first()
                if not existing:
                    permission = Permission(**permission_data)
                    session.add(permission)
            
            # Insert roles
            logger.info("Inserting roles...")
            roles_data = [
                {'role_name': 'Admin', 'role_code': 'admin', 'description': 'Administrator with full system access'},
                {'role_name': 'Manager', 'role_code': 'manager', 'description': 'Manager with elevated permissions'},
                {'role_name': 'Controller', 'role_code': 'controller', 'description': 'Controller role for oversight'},
                {'role_name': 'Auditor', 'role_code': 'auditor', 'description': 'Auditor role for compliance'},
                {'role_name': 'Developer', 'role_code': 'dev', 'description': 'Developer with technical access'},
                {'role_name': 'User', 'role_code': 'user', 'description': 'Regular user with standard access'},
                {'role_name': 'Client POC', 'role_code': 'client_poc_user', 'description': 'Client point of contact'},
                {'role_name': 'Fund Manager', 'role_code': 'fund_manager', 'description': 'Fund Manager with fund management access'},
                {'role_name': 'Analyst', 'role_code': 'analyst', 'description': 'Analyst with data analysis and reporting access'}
            ]
            
            for role_data in roles_data:
                existing = session.query(Role).filter(Role.role_code == role_data['role_code']).first()
                if not existing:
                    role = Role(**role_data)
                    session.add(role)
                    logger.info(f"Added role: {role_data['role_code']}")
                else:
                    logger.info(f"Role {role_data['role_code']} already exists, skipping...")
            
            session.commit()
            logger.info("Roles inserted successfully!")
            
            # Insert clients
            logger.info("Inserting clients...")
            clients_data = [
                {'name': 'All Clients', 'code': 'all_clients', 'description': 'Access to all clients'},
                {'name': 'NexBridge', 'code': 'nexbridge', 'description': 'NexBridge'},
                {'name': 'StoneWell', 'code': 'stonewell', 'description': 'StoneWell'},
                {'name': 'Skulptor', 'code': 'skulptor', 'description': 'Skulptor'},
                {'name': 'AIMS', 'code': 'aims', 'description': 'AIMS'}
            ]
            
            for client_data in clients_data:
                existing = session.query(Client).filter(Client.code == client_data['code']).first()
                if not existing:
                    client = Client(**client_data)
                    session.add(client)
            
            session.commit()
            logger.info("Initial data inserted successfully!")
            
        except Exception as e:
            session.rollback()
            logger.error(f"Error inserting initial data: {e}")
            raise
        finally:
            session.close()
    
    def insert_users(self):
        """Insert users from the existing JSON structure"""
        session = self.db_manager.get_session()
        try:
            logger.info("Inserting users...")
            # Load users from JSON config
            base_dir = os.path.dirname(os.path.abspath(__file__))
            users_json_path = os.path.join(base_dir, 'rbac', 'configs', 'users.json')

            with open(users_json_path, 'r', encoding='utf-8') as users_file:
                users_json = json.load(users_file)
            

            # Normalize and map JSON fields to database fields
            users_data = []
            for user_rec in users_json:
                raw_role_code = (user_rec.get('role') or '').lower()
                # Normalize role codes to match roles table
                if raw_role_code in ('admin_level_0', 'admin_level_1', 'admin'):
                    role_code = 'admin'
                elif raw_role_code in ('dev', 'developer'):
                    role_code = 'dev'
                elif raw_role_code in ('manager',):
                    role_code = 'manager'
                elif raw_role_code in ('controller',):
                    role_code = 'controller'
                elif raw_role_code in ('auditor',):
                    role_code = 'auditor'
                elif raw_role_code in ('client_poc_user', 'client-poc', 'poc'):
                    role_code = 'client_poc_user'
                else:
                    role_code = 'user'

                users_data.append({
                    'username': user_rec.get('username'),
                    'display_name': user_rec.get('displayName'),
                    'first_name': user_rec.get('firstName'),
                    'last_name': user_rec.get('lastName'),
                    'password_hash': user_rec.get('password'),
                    'role_str': user_rec.get('roleStr'),
                    'role': role_code
                })
            
            for user_data in users_data:
                existing = session.query(User).filter(User.username == user_data['username']).first()
                if not existing:
                    # Create user with role_id instead of role_str/role
                    user_dict = user_data.copy()
                    role_code = user_dict.pop('role', None)  # Remove 'role' from dict
                    user_dict.pop('role_str', None)  # Remove 'role_str' from dict
                    
                    # Get role by role_code and add role_id
                    if role_code:
                        role = session.query(Role).filter(Role.role_code == role_code).first()
                        if role:
                            user_dict['role_id'] = role.id
                        else:
                            logger.warning(f"Role '{role_code}' not found for user {user_data['username']}, skipping")
                            continue
                    else:
                        logger.warning(f"No role specified for user {user_data['username']}, skipping")
                        continue
                    
                    # Assign default client_id (most users go to all_clients)
                    all_clients_client = session.query(Client).filter(Client.code == 'all_clients').first()
                    if all_clients_client:
                        user_dict['client_id'] = all_clients_client.id
                    
                    # Ensure first_name and last_name are set (they should be in JSON now)
                    if not user_dict.get('first_name'):
                        user_dict['first_name'] = user_dict.get('username', 'User').capitalize()
                    if not user_dict.get('last_name'):
                        user_dict['last_name'] = "User"
                    
                    # Set default job_title if not provided
                    if not user_dict.get('job_title'):
                        user_dict['job_title'] = 'Staff'
                    
                    # Set temp_password to False for seeded users (they have permanent passwords)
                    user_dict['temp_password'] = False
                    
                    user = User(**user_dict)
                    session.add(user)
            
            session.commit()
            logger.info("Users inserted successfully!")
            
        except Exception as e:
            session.rollback()
            logger.error(f"Error inserting users: {e}")
            raise
        finally:
            session.close()
    
    def update_users_with_emails(self):
        """Update existing users with email addresses based on username patterns"""
        logger.info("Updating existing users with email addresses...")
        
        session = self.db_manager.get_session()
        try:
            # Get all users without email addresses
            users_without_email = session.query(User).filter(User.email.is_(None)).all()
            
            for user in users_without_email:
                # Generate email based on username with special-case overrides
                if user.username in ('aim_admin', 'aims_admin'):
                    email = 'ops@aimsb2b.com'
                elif user.username == 'rutvik':
                    email = 'sales@aimsb2b.com'
                else:
                    email = f"{user.username}@aithonsolutions.com"
                    user.email = email
                    logger.info(f"Updated user {user.username} with email: {email}")
            
            session.commit()
            logger.info(f"Updated {len(users_without_email)} users with email addresses")
            
        except Exception as e:
            session.rollback()
            logger.error(f"Error updating users with emails: {e}")
            raise
        finally:
            session.close()
    
    def setup_user_client_assignments(self):
        """Setup user-client assignments - assign client_id directly to users based on their roles"""
        session = self.db_manager.get_session()
        try:
            logger.info("Setting up user-client assignments...")
            
            # Get all users
            all_users = session.query(User).filter(User.is_active == True).all()
            
            # Get the 'all_clients' client for admin users
            all_clients_client = session.query(Client).filter(Client.code == 'all_clients').first()
            
            # Get roles
            admin_role = session.query(Role).filter(Role.role_code == 'admin').first()
            user_role = session.query(Role).filter(Role.role_code == 'user').first()
            developer_role = session.query(Role).filter(Role.role_code == 'dev').first()
            manager_role = session.query(Role).filter(Role.role_code == 'manager').first()
            controller_role = session.query(Role).filter(Role.role_code == 'controller').first()
            auditor_role = session.query(Role).filter(Role.role_code == 'auditor').first()
            client_poc_role = session.query(Role).filter(Role.role_code == 'client_poc_user').first()
            
            # Setup client assignments based on user roles
            for user in all_users:
                # Skip if user already has a client assigned
                if user.client_id:
                    logger.info(f"User {user.username} already has client assigned, skipping...")
                    continue
                
                # Assign client based on user role
                if user.role and user.role.role_code == 'admin':
                    # Admin users get access to all_clients
                    if all_clients_client and admin_role:
                        user.client_id = all_clients_client.id
                        logger.info(f"Assigned admin user {user.username} to all_clients")
                
                elif user.role and user.role.role_code == 'developer':
                    # Dev users get access to all_clients
                    if all_clients_client and developer_role:
                        user.client_id = all_clients_client.id
                        logger.info(f"Assigned dev user {user.username} to all_clients")
                
                elif user.role and user.role.role_code == 'user':
                    # Regular users get access to all_clients
                    if all_clients_client and user_role:
                        user.client_id = all_clients_client.id
                        logger.info(f"Assigned user {user.username} to all_clients")

                elif user.role and user.role.role_code == 'client_poc_user':
                    # Client POC users get access to all_clients
                    if all_clients_client and client_poc_role:
                        user.client_id = all_clients_client.id
                        logger.info(f"Assigned client_poc user {user.username} to all_clients")
                
                elif user.role and user.role.role_code == 'manager':
                    # Manager users get access to AIMS client
                    aims_client = session.query(Client).filter(Client.code == 'aims').first()
                    if aims_client and manager_role:
                        user.client_id = aims_client.id
                        logger.info(f"Assigned manager user {user.username} to AIMS client")
                
                elif user.role and user.role.role_code == 'controller':
                    # Controller users get access to NexBridge client
                    nexbridge_client = session.query(Client).filter(Client.code == 'nexbridge').first()
                    if nexbridge_client and controller_role:
                        user.client_id = nexbridge_client.id
                        logger.info(f"Assigned controller user {user.username} to NexBridge client")
                
                elif user.role and user.role.role_code == 'auditor':
                    # Auditor users get access to NexBridge client
                    nexbridge_client = session.query(Client).filter(Client.code == 'nexbridge').first()
                    if nexbridge_client and auditor_role:
                        user.client_id = nexbridge_client.id
                        logger.info(f"Assigned auditor user {user.username} to NexBridge client")
                
                else:
                    # Default fallback - assign to all_clients
                    if all_clients_client and user_role:
                        user.client_id = all_clients_client.id
                        logger.info(f"Assigned user {user.username} (role: {user.role.role_code if user.role else 'None'}) to all_clients as fallback")
            
            session.commit()
            logger.info("User-client assignments setup successfully!")
            
        except Exception as e:
            session.rollback()
            logger.error(f"Error setting up user-client assignments: {e}")
            raise
        finally:
            session.close()
    
    def setup_role_module_permissions(self):
        """Setup role-module-permission relationships"""
        session = self.db_manager.get_session()
        try:
            logger.info("Setting up role-module-permission relationships...")
            
            # Get roles
            admin_role = session.query(Role).filter(Role.role_code == 'admin').first()
            user_role = session.query(Role).filter(Role.role_code == 'user').first()
            developer_role = session.query(Role).filter(Role.role_code == 'dev').first()
            manager_role = session.query(Role).filter(Role.role_code == 'manager').first()
            controller_role = session.query(Role).filter(Role.role_code == 'controller').first()
            auditor_role = session.query(Role).filter(Role.role_code == 'auditor').first()
            client_poc_role = session.query(Role).filter(Role.role_code == 'client_poc_user').first()
            
            # Get modules
            frame_module = session.query(Module).filter(Module.module_code == 'frame').first()
            validus_module = session.query(Module).filter(Module.module_code == 'nav_validus').first()
            aims_module = session.query(Module).filter(Module.module_code == 'aims').first()
            
            # Get permissions
            read_permission = session.query(Permission).filter(Permission.permission_code == 'read').first()
            create_permission = session.query(Permission).filter(Permission.permission_code == 'create').first()
            update_permission = session.query(Permission).filter(Permission.permission_code == 'update').first()
            delete_permission = session.query(Permission).filter(Permission.permission_code == 'delete').first()
            
            if not all([admin_role, user_role, developer_role, manager_role, controller_role, auditor_role]):
                logger.error("One or more roles not found!")
                return
            
            if not all([frame_module, validus_module, aims_module]):
                logger.error("One or more modules not found!")
                return
            
            if not all([read_permission, create_permission, update_permission, delete_permission]):
                logger.error("One or more permissions not found!")
                return
            
            # Clear existing role-module-permission relationships
            session.query(RoleOrClientBasedModuleLevelPermission).filter(
                RoleOrClientBasedModuleLevelPermission.client_id.is_(None)
            ).delete()
            
            modules = [frame_module, validus_module, aims_module]
            
            # Admin role: Full access to all modules
            for module in modules:
                for permission in [read_permission, create_permission, update_permission, delete_permission]:
                    rmp = RoleOrClientBasedModuleLevelPermission(
                        role_id=admin_role.id,
                        module_id=module.id,
                        permission_id=permission.id,
                        client_id=None  # Role-based permission
                    )
                    session.add(rmp)
            
            # Developer role: Full access to all modules
            for module in modules:
                for permission in [read_permission, create_permission, update_permission, delete_permission]:
                    rmp = RoleOrClientBasedModuleLevelPermission(
                        role_id=developer_role.id,
                        module_id=module.id,
                        permission_id=permission.id,
                        client_id=None  # Role-based permission
                    )
                    session.add(rmp)
            
            # Manager role: Read and write access to all modules
            for module in modules:
                for permission in [read_permission, create_permission, update_permission]:
                    rmp = RoleOrClientBasedModuleLevelPermission(
                        role_id=manager_role.id,
                        module_id=module.id,
                        permission_id=permission.id,
                        client_id=None  # Role-based permission
                    )
                    session.add(rmp)
            
            # Controller role: Read and write access to all modules
            for module in modules:
                for permission in [read_permission, create_permission, update_permission]:
                    rmp = RoleOrClientBasedModuleLevelPermission(
                        role_id=controller_role.id,
                        module_id=module.id,
                        permission_id=permission.id,
                        client_id=None  # Role-based permission
                    )
                    session.add(rmp)
            
            # Auditor role: Read-only access to all modules
            for module in modules:
                rmp = RoleOrClientBasedModuleLevelPermission(
                    role_id=auditor_role.id,
                    module_id=module.id,
                    permission_id=read_permission.id,
                    client_id=None  # Role-based permission
                )
                session.add(rmp)
            
            # User role: Read access to all modules
            for module in modules:
                rmp = RoleOrClientBasedModuleLevelPermission(
                    role_id=user_role.id,
                    module_id=module.id,
                    permission_id=read_permission.id,
                    client_id=None  # Role-based permission
                )
                session.add(rmp)

            # Client POC role: Read access to all modules
            if client_poc_role:
                for module in modules:
                    rmp = RoleOrClientBasedModuleLevelPermission(
                        role_id=client_poc_role.id,
                        module_id=module.id,
                        permission_id=read_permission.id,
                        client_id=None  # Role-based permission
                    )
                session.add(rmp)
            
            session.commit()
            logger.info("Role-module-permission relationships set up successfully!")
            
        except Exception as e:
            session.rollback()
            logger.error(f"Error setting up role-module-permission relationships: {e}")
            raise
        finally:
            session.close()
    
    def get_migration_summary(self) -> Dict[str, Any]:
        """Get migration summary statistics"""
        session = self.db_manager.get_session()
        try:
            summary = {}
            
            # Count records in each table
            summary['users'] = session.query(User).count()
            summary['clients'] = session.query(Client).count()
            summary['roles'] = session.query(Role).count()
            summary['modules'] = session.query(Module).count()
            summary['permissions'] = session.query(Permission).count()
            summary['role_module_permissions'] = session.query(RoleOrClientBasedModuleLevelPermission).filter(
                RoleOrClientBasedModuleLevelPermission.client_id.is_(None)
            ).count()
            
            # Add fund managers
            try:
                summary['fund_managers'] = session.query(FundManager).count()
            except Exception as fm_error:
                logger.warning(f"Could not count FundManager table: {fm_error}")
                summary['fund_managers'] = 0            
                       
            return summary
            
        except Exception as e:
            logger.error(f"Error getting migration summary: {e}")
            return {}
        finally:
            session.close()

    def add_secure_user(self, username: str, email: str, display_name: str, 
                       plain_password: str, role_code: str, first_name: str = None, last_name: str = None):
        """
        Securely add a single user with automatic password hashing
        
        Args:
            username: Unique username
            email: User email address
            display_name: Display name for the user
            plain_password: Plain text password (will be hashed automatically)
            role_code: Role code (admin, user, client_poc_user, etc.)
            first_name: User's first name (optional, will be derived from display_name if not provided)
            last_name: User's last name (optional, will be derived from display_name if not provided)
        
        Returns:
            bool: True if user was added successfully, False otherwise
        """
        session = self.db_manager.get_session()
        try:
            # Check if user already exists
            existing = session.query(User).filter(User.username == username).first()
            if existing:
                logger.info(f"User {username} already exists, skipping...")
                return False
            
            # Check if email already exists
            existing_email = session.query(User).filter(User.email == email).first()
            if existing_email:
                logger.info(f"Email {email} already exists for user {existing_email.username}, skipping user {username}...")
                return False
            
            # Get role by role_code
            role = session.query(Role).filter(Role.role_code == role_code).first()
            if not role:
                logger.warning(f"Role '{role_code}' not found for user {username}, skipping")
                return False
            
            # Hash the password
            hashed_password = self._hash_password(plain_password)
            
            # Handle first_name and last_name derivation
            if not first_name or not last_name:
                # Derive from display_name if not provided
                name_parts = display_name.strip().split()
                
                if len(name_parts) >= 2:
                    # Check if it looks like an initial (single letter followed by period)
                    last_part = name_parts[-1]
                    if len(last_part) <= 2 and last_part.endswith('.'):
                        # Names like "Roshi D." or "K.B" - use entire display_name as first_name
                        derived_first = display_name.strip()
                        derived_last = "User"
                    else:
                        # Normal case: "John Doe" or "John Michael Doe"
                        derived_first = name_parts[0]
                        derived_last = ' '.join(name_parts[1:])
                elif len(name_parts) == 1:
                    # Single name: use as first_name
                    derived_first = name_parts[0]
                    derived_last = "User"
                else:
                    # Empty display_name: use username
                    derived_first = username.capitalize()
                    derived_last = "User"
                
                # Use provided values or derived values
                final_first_name = first_name or derived_first
                final_last_name = last_name or derived_last
            else:
                final_first_name = first_name
                final_last_name = last_name
            
            # Get default client (all_clients)
            all_clients_client = session.query(Client).filter(Client.code == 'all_clients').first()
            
            # Create user with hashed password and new fields
            user = User(
                username=username,
                email=email,
                display_name=display_name,
                first_name=final_first_name,
                last_name=final_last_name,
                job_title='Staff',  # Default job title for migrated users
                password_hash=hashed_password,
                temp_password=False,  # Existing users have permanent passwords
                role_id=role.id,
                client_id=all_clients_client.id if all_clients_client else None
            )
            session.add(user)
            session.commit()
            
            logger.info(f"Added user: {username} with email: {email} (password securely hashed)")
            return True
            
        except Exception as e:
            session.rollback()
            logger.error(f"Error adding user {username}: {e}")
            return False
        finally:
            session.close()
    
    def setup_new_user_client_assignments(self):
        """Setup user-client assignments for newly added users"""
        session = self.db_manager.get_session()
        try:
            logger.info("Setting up client assignments for new users...")
            
            # Get the new users
            new_usernames = ['rutvik', 'rutvik_dev', 'aims_admin', 'client_poc_user']
            
            # Get clients and roles
            all_clients_client = session.query(Client).filter(Client.code == 'all_clients').first()
            aims_client = session.query(Client).filter(Client.code == 'aims').first()
            
            admin_role = session.query(Role).filter(Role.role_code == 'admin').first()
            user_role = session.query(Role).filter(Role.role_code == 'user').first()
            client_poc_role = session.query(Role).filter(Role.role_code == 'client_poc_user').first()
            
            for username in new_usernames:
                user = session.query(User).filter(User.username == username).first()
                if not user:
                    logger.warning(f"User {username} not found, skipping client assignment")
                    continue
                
                # Skip if user already has a client assigned
                if user.client_id:
                    logger.info(f"User {username} already has client assigned, skipping...")
                    continue
                
                # Assign client based on user role
                if user.role and user.role.role_code == 'admin':
                    if all_clients_client and admin_role:
                        user.client_id = all_clients_client.id
                        logger.info(f"Assigned admin user {username} to all_clients")
                
                elif user.role and user.role.role_code == 'user':
                    if all_clients_client and user_role:
                        user.client_id = all_clients_client.id
                        logger.info(f"Assigned user {username} to all_clients")
                
                elif user.role and user.role.role_code == 'client_poc_user':
                    if all_clients_client and client_poc_role:
                        user.client_id = all_clients_client.id
                        logger.info(f"Assigned client_poc user {username} to all_clients")
            
            session.commit()
            logger.info("Client assignments setup for new users completed!")
            
        except Exception as e:
            session.rollback()
            logger.error(f"Error setting up client assignments for new users: {e}")
            raise
        finally:
            session.close()
    
    def insert_master_data(self):
        """Insert master data into the masters table with conflict handling"""
        from database_models import Master
        from sqlalchemy.dialects.postgresql import insert
        from sqlalchemy import text, func
        
        session = self.db_manager.get_session()
        try:
            logger.info("Upserting master data...")
            
            # Master records to insert/update
            master_records = [
                {'name': 'Client Master', 'code': 'CLIENT_MASTER', 'description': 'Client management'},
                {'name': 'Fund Master', 'code': 'FUND_MASTER', 'description': 'Fund management'},
                {'name': 'Fund Manager Master', 'code': 'FUND_MANAGER_MASTER', 'description': 'Fund manager management'},
                {'name': 'Account Master', 'code': 'ACCOUNT_MASTER', 'description': 'Account management'},
                {'name': 'Process Configuration', 'code': 'PROCESS_CONFIGURATION', 'description': 'Process configuration'}
            ]
            
            # Get table object for the Master model
            table = Master.__table__
            
            # Build the insert statement with ON CONFLICT DO UPDATE
            stmt = insert(table).values(master_records)
            
            # On conflict, update the name and description if they're different
            update_dict = {
                'name': text('EXCLUDED.name'),
                'description': text('EXCLUDED.description'),
                'updated_at': func.now()
            }
            
            # Execute the upsert
            stmt = stmt.on_conflict_do_update(
                index_elements=['code'],  # This should match the unique constraint
                set_=update_dict
            )
            
            # Execute the statement
            result = session.execute(stmt)
            session.commit()
            
            # Log the number of records processed
            logger.info(f"Upserted master records. {len(master_records)} records processed.")
            
            # Return the number of records processed
            return len(master_records)
            
        except Exception as e:
            session.rollback()
            logger.error(f"Error inserting master data: {e}")
            raise
        finally:
            session.close()
    
    def insert_fund_managers(self):
        """Insert fund manager data into the fund_manager table"""
        session = self.db_manager.get_session()
        try:
            logger.info("Inserting fund managers...")
            
            # Check if fund managers already exist
            existing_count = session.query(FundManager).count()
            if existing_count > 0:
                logger.info(f"Found {existing_count} existing fund managers. Skipping seed.")
                return
            
            # Sample fund manager data with realistic investment company names
            fund_managers_data = [
                {
                    "fund_manager_name": "Blackstone Capital Partners",
                    "contact_title": "Managing Director",
                    "contact_first_name": "Sarah",
                    "contact_last_name": "Johnson",
                    "contact_email": "sarah.johnson@blackstone.com",
                    "contact_number": "+1-212-583-5000",
                    "status": "active"
                },
                {
                    "fund_manager_name": "KKR & Co. Inc.",
                    "contact_title": "Principal",
                    "contact_first_name": "Michael",
                    "contact_last_name": "Chen",
                    "contact_email": "michael.chen@kkr.com",
                    "contact_number": "+1-212-750-8300",
                    "status": "active"
                },
                {
                    "fund_manager_name": "Apollo Global Management",
                    "contact_title": "Senior Vice President",
                    "contact_first_name": "Emily",
                    "contact_last_name": "Rodriguez",
                    "contact_email": "emily.rodriguez@apollo.com",
                    "contact_number": "+1-212-822-0500",
                    "status": "active"
                },
                {
                    "fund_manager_name": "Carlyle Group",
                    "contact_title": "Investment Director",
                    "contact_first_name": "David",
                    "contact_last_name": "Thompson",
                    "contact_email": "david.thompson@carlyle.com",
                    "contact_number": "+1-202-729-5626",
                    "status": "active"
                },
                {
                    "fund_manager_name": "TPG Capital",
                    "contact_title": "Partner",
                    "contact_first_name": "Lisa",
                    "contact_last_name": "Wang",
                    "contact_email": "lisa.wang@tpg.com",
                    "contact_number": "+1-415-743-1500",
                    "status": "active"
                },
                {
                    "fund_manager_name": "Bain Capital",
                    "contact_title": "Managing Partner",
                    "contact_first_name": "Robert",
                    "contact_last_name": "Anderson",
                    "contact_email": "robert.anderson@baincapital.com",
                    "contact_number": "+1-617-572-2000",
                    "status": "active"
                },
                {
                    "fund_manager_name": "Warburg Pincus",
                    "contact_title": "Principal",
                    "contact_first_name": "Jennifer",
                    "contact_last_name": "Martinez",
                    "contact_email": "jennifer.martinez@warburgpincus.com",
                    "contact_number": "+1-212-878-0600",
                    "status": "active"
                },
                {
                    "fund_manager_name": "General Atlantic",
                    "contact_title": "Managing Director",
                    "contact_first_name": "James",
                    "contact_last_name": "Wilson",
                    "contact_email": "james.wilson@generalatlantic.com",
                    "contact_number": "+1-203-629-8658",
                    "status": "active"
                },
                {
                    "fund_manager_name": "Silver Lake Partners",
                    "contact_title": "Senior Managing Director",
                    "contact_first_name": "Amanda",
                    "contact_last_name": "Taylor",
                    "contact_email": "amanda.taylor@silverlake.com",
                    "contact_number": "+1-650-331-7000",
                    "status": "active"
                },
                {
                    "fund_manager_name": "Hellman & Friedman",
                    "contact_title": "Partner",
                    "contact_first_name": "Christopher",
                    "contact_last_name": "Brown",
                    "contact_email": "christopher.brown@hf.com",
                    "contact_number": "+1-415-788-5111",
                    "status": "active"
                },
                {
                    "fund_manager_name": "Francisco Partners",
                    "contact_title": "Managing Partner",
                    "contact_first_name": "Maria",
                    "contact_last_name": "Garcia",
                    "contact_email": "maria.garcia@franciscopartners.com",
                    "contact_number": "+1-415-293-2000",
                    "status": "active"
                },
                {
                    "fund_manager_name": "Thoma Bravo",
                    "contact_title": "Principal",
                    "contact_first_name": "Kevin",
                    "contact_last_name": "Lee",
                    "contact_email": "kevin.lee@thomabravo.com",
                    "contact_number": "+1-312-777-4440",
                    "status": "active"
                },
                {
                    "fund_manager_name": "Insight Partners",
                    "contact_title": "Managing Director",
                    "contact_first_name": "Rachel",
                    "contact_last_name": "Davis",
                    "contact_email": "rachel.davis@insightpartners.com",
                    "contact_number": "+1-212-230-9200",
                    "status": "active"
                },
                {
                    "fund_manager_name": "Summit Partners",
                    "contact_title": "Principal",
                    "contact_first_name": "Daniel",
                    "contact_last_name": "Miller",
                    "contact_email": "daniel.miller@summitpartners.com",
                    "contact_number": "+1-617-824-1000",
                    "status": "active"
                },
                {
                    "fund_manager_name": "Vista Equity Partners",
                    "contact_title": "Senior Managing Director",
                    "contact_first_name": "Nicole",
                    "contact_last_name": "White",
                    "contact_email": "nicole.white@vistaequitypartners.com",
                    "contact_number": "+1-415-222-9700",
                    "status": "active"
                },
                {
                    "fund_manager_name": "Permira Advisers",
                    "contact_title": "Partner",
                    "contact_first_name": "Andrew",
                    "contact_last_name": "Clark",
                    "contact_email": "andrew.clark@permira.com",
                    "contact_number": "+44-20-7632-1000",
                    "status": "active"
                },
                {
                    "fund_manager_name": "EQT Partners",
                    "contact_title": "Investment Director",
                    "contact_first_name": "Sophie",
                    "contact_last_name": "Anderson",
                    "contact_email": "sophie.anderson@eqtpartners.com",
                    "contact_number": "+46-8-506-55-000",
                    "status": "active"
                },
                {
                    "fund_manager_name": "CVC Capital Partners",
                    "contact_title": "Managing Partner",
                    "contact_first_name": "Thomas",
                    "contact_last_name": "Schmidt",
                    "contact_email": "thomas.schmidt@cvc.com",
                    "contact_number": "+44-20-7420-4200",
                    "status": "active"
                },
                {
                    "fund_manager_name": "Advent International",
                    "contact_title": "Principal",
                    "contact_first_name": "Isabella",
                    "contact_last_name": "Rossi",
                    "contact_email": "isabella.rossi@adventinternational.com",
                    "contact_number": "+1-617-951-9400",
                    "status": "active"
                },
                {
                    "fund_manager_name": "Cinven",
                    "contact_title": "Partner",
                    "contact_first_name": "Oliver",
                    "contact_last_name": "Williams",
                    "contact_email": "oliver.williams@cinven.com",
                    "contact_number": "+44-20-7661-3333",
                    "status": "active"
                },
                # Add some inactive fund managers
                {
                    "fund_manager_name": "Legacy Capital Management",
                    "contact_title": "Former Managing Director",
                    "contact_first_name": "Richard",
                    "contact_last_name": "Foster",
                    "contact_email": "richard.foster@legacycapital.com",
                    "contact_number": "+1-212-555-0100",
                    "status": "inactive"
                },
                {
                    "fund_manager_name": "Old Bridge Investments",
                    "contact_title": "Former Partner",
                    "contact_first_name": "Patricia",
                    "contact_last_name": "Moore",
                    "contact_email": "patricia.moore@oldbridge.com",
                    "contact_number": "+1-415-555-0200",
                    "status": "inactive"
                }
            ]
            
            # Insert fund managers
            created_count = 0
            for fm_data in fund_managers_data:
                fund_manager = FundManager(**fm_data)
                session.add(fund_manager)
                created_count += 1
            
            # Commit the transaction
            session.commit()
            logger.info(f"✅ Successfully seeded {created_count} fund managers!")
            
            # Display summary
            logger.info("\n📊 Fund Manager Summary:")
            active_count = session.query(FundManager).filter(FundManager.status == 'active').count()
            inactive_count = session.query(FundManager).filter(FundManager.status == 'inactive').count()
            logger.info(f"   • Active: {active_count}")
            logger.info(f"   • Inactive: {inactive_count}")
            logger.info(f"   • Total: {active_count + inactive_count}")
            
        except Exception as e:
            session.rollback()
            logger.error(f"Error seeding fund managers: {str(e)}")
            raise
            
        finally:
            session.close()
    
    def insert_fund_manager_users(self):
        """Insert fund manager users"""
        session = self.db_manager.get_session()
        try:
            logger.info("Inserting fund manager users...")
            
            # Get fund manager role
            fund_manager_role = session.query(Role).filter(Role.role_code == 'fund_manager').first()
            if not fund_manager_role:
                logger.error("Fund Manager role not found, skipping fund manager users")
                return
            
            # Get all_clients client
            all_clients_client = session.query(Client).filter(Client.code == 'all_clients').first()
            if not all_clients_client:
                logger.error("All Clients client not found, skipping fund manager users")
                return
            
            # Fund manager users data
            fund_manager_users = [
                {
                    'username': 'fundmanager1',
                    'display_name': 'John Smith',
                    'first_name': 'John',
                    'last_name': 'Smith',
                    'job_title': 'Fund Manager',
                    'email': 'john.smith@fundmanager.com',
                    'password_hash': self._hash_password('fundmanager123'),
                    'temp_password': False,
                    'role_id': fund_manager_role.id,
                    'client_id': all_clients_client.id,
                    'is_active': True
                },
                {
                    'username': 'fundmanager2',
                    'display_name': 'Sarah Johnson',
                    'first_name': 'Sarah',
                    'last_name': 'Johnson',
                    'job_title': 'Fund Manager',
                    'email': 'sarah.johnson@fundmanager.com',
                    'password_hash': self._hash_password('fundmanager123'),
                    'temp_password': False,
                    'role_id': fund_manager_role.id,
                    'client_id': all_clients_client.id,
                    'is_active': True
                },
                {
                    'username': 'fundmanager3',
                    'display_name': 'Michael Brown',
                    'first_name': 'Michael',
                    'last_name': 'Brown',
                    'job_title': 'Fund Manager',
                    'email': 'michael.brown@fundmanager.com',
                    'password_hash': self._hash_password('fundmanager123'),
                    'temp_password': False,
                    'role_id': fund_manager_role.id,
                    'client_id': all_clients_client.id,
                    'is_active': True
                }
            ]
            
            for user_data in fund_manager_users:
                existing = session.query(User).filter(User.username == user_data['username']).first()
                if not existing:
                    user = User(**user_data)
                    session.add(user)
                    logger.info(f"Added fund manager user: {user_data['username']}")
                else:
                    logger.info(f"Fund manager user {user_data['username']} already exists, skipping...")
            
            session.commit()
            logger.info("Fund manager users inserted successfully!")
            
        except Exception as e:
            session.rollback()
            logger.error(f"Error inserting fund manager users: {e}")
            raise
        finally:
            session.close()
    
    def add_users_from_table(self):
        """
        Add users from the provided Excel table with secure password hashing.
        
        Users to add:
        - sales@aimsb2b.com (Demo User, role: user)
        - client_user (Client User, role: user)  
        - ops@aimsb2b.com (AIMS Ops User, role: admin)
        - client_poc_user (Client POC User, role: client_poc_user)
        """
        logger.info("Adding users from provided table...")
        
        # User data from the table with secure password handling
        users_to_add = [
            {
                'username': 'rutvik',
                'email': 'sales@aimsb2b.com',
                'display_name': 'Demo User',
                'first_name': 'Demo',
                'last_name': 'User',
                'password': 'hwbM8JyDt8fzShqh',
                'role_code': 'user'
            },
            {
                'username': 'client_user', 
                'email': 'client_user@aithonsolutions.com',
                'display_name': 'Client User',
                'first_name': 'Client',
                'last_name': 'User',
                'password': 'hwbM8JyDt8fzShqh',
                'role_code': 'user'
            },
            {
                'username': 'aims_admin',
                'email': 'ops@aimsb2b.com', 
                'display_name': 'AIMS Ops User',
                'first_name': 'AIMS',
                'last_name': 'Ops User',
                'password': 'aim-aithon@2025',
                'role_code': 'admin'
            },
            {
                'username': 'client_poc_user',
                'email': 'client_poc_user',
                'display_name': 'Client POC User',
                'first_name': 'Client',
                'last_name': 'POC User',
                'password': 'client@aims2025',
                'role_code': 'client_poc_user'
            }
        ]
        
        success_count = 0
        for user_data in users_to_add:
            try:
                # Use the existing secure user addition method
                success = self.add_secure_user(
                    username=user_data['username'],
                    email=user_data['email'], 
                    display_name=user_data['display_name'],
                    plain_password=user_data['password'],
                    role_code=user_data['role_code'],
                    first_name=user_data.get('first_name'),
                    last_name=user_data.get('last_name')
                )
                if success:
                    success_count += 1
                    logger.info(f"✅ Successfully added user: {user_data['username']}")
                else:
                    logger.warning(f"⚠️ User {user_data['username']} may already exist or had an issue")
                    
            except Exception as e:
                logger.error(f"❌ Error adding user {user_data['username']}: {e}")
        
        logger.info(f"User addition complete. Successfully added {success_count}/{len(users_to_add)} users.")
        
        # Clear sensitive data from memory
        for user_data in users_to_add:
            user_data['password'] = '[REDACTED]'
            
        return success_count == len(users_to_add)
    
    def setup_benchmark_data(self):
        """Setup benchmark data including S&P 500 Index values"""
        logger.info("Setting up benchmark data...")
        
        try:
            from database_models import Benchmark
            from datetime import datetime
            
            session = self.db_manager.get_session()
            
            # S&P 500 Index data
            sp500_data = [
                {'date': '2023-12-31', 'value': 4769.83},
                {'date': '2024-01-31', 'value': 4845.65},
                {'date': '2024-02-29', 'value': 5096.27},
                {'date': '2024-03-31', 'value': 5254.35},
                {'date': '2024-04-30', 'value': 5035.69},
                {'date': '2024-05-31', 'value': 5277.51},
                {'date': '2024-06-30', 'value': 5460.48},
                {'date': '2024-07-31', 'value': 5522.30},
            ]
            
            # Insert S&P 500 data
            for data_point in sp500_data:
                # Check if benchmark already exists
                existing_benchmark = session.query(Benchmark).filter(
                    Benchmark.benchmark == 'S&P 500 Index',
                    Benchmark.date == datetime.strptime(data_point['date'], '%Y-%m-%d').date()
                ).first()
                
                if not existing_benchmark:
                    benchmark = Benchmark(
                        benchmark='S&P 500 Index',
                        date=datetime.strptime(data_point['date'], '%Y-%m-%d').date(),
                        value=data_point['value'],
                        extra_data={'source': 'Market data', 'currency': 'USD'}
                    )
                    session.add(benchmark)
                    logger.info(f"Added S&P 500 Index data for {data_point['date']}: {data_point['value']}")
                else:
                    logger.info(f"S&P 500 Index data for {data_point['date']} already exists")
            
            session.commit()
            logger.info("Benchmark data setup completed successfully")
            
        except Exception as e:
            logger.error(f"Error setting up benchmark data: {e}")
            session.rollback()
            raise
        finally:
            session.close()
    
    def run_seeding(self):
        """Run complete database seeding"""
        logger.info("Starting database seeding...")
        
        try:
            # Step 1: Insert initial data
            self.insert_initial_data()
            
            # Step 2: Insert users
            self.insert_users()
            
            # Step 2.5: Add users from table
            self.add_users_from_table()
            
            # Step 2.6: Insert fund manager users
            self.insert_fund_manager_users()
            
            # Step 3: Update existing users with email addresses
            self.update_users_with_emails()
            
            # Step 4: Setup user-client assignments
            self.setup_user_client_assignments()
            
            # Step 5: Setup role-module-permission relationships
            self.setup_role_module_permissions()
            
            # Step 8: Insert master data
            self.insert_master_data()
            
            # Step 9: Insert fund managers
            self.insert_fund_managers()
            
            # Step 10: Get summary
            summary = self.get_migration_summary()
            
            logger.info("Database seeding completed successfully!")
            logger.info("Seeding Summary:")
            for table, count in summary.items():
                logger.info(f"  {table}: {count} records")
            
            return True
            
        except Exception as e:
            logger.error(f"Database seeding failed: {e}")
            return False

def main():
    """Main function to run database seeding"""
    # Initialize database manager
    db_manager = DatabaseManager()
    
    # Create seeder instance
    seeder = DatabaseSeeder(db_manager)
    
    # Run seeding
    success = seeder.run_seeding()
    
    if success:
        print("Database seeding completed successfully!")
    else:
        print("Database seeding failed!")
        exit(1)

if __name__ == "__main__":
    main()

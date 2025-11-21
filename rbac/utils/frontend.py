from typing import Dict, Any, Optional

"""Utilities for frontend user data backed by database.

This module needs to import `DatabaseManager` from `database_models.py` which
resides at the project root. We make the import robust across environments
(local, Docker, package installs) by trying multiple strategies.
"""

# Import DatabaseManager for database operations
import sys
import os
# Add the root directory to Python path for Docker compatibility
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
from database_models import DatabaseManager, Module, Master, Permission
from models.permission_models import RoleOrClientBasedModuleLevelPermission

# Initialize database manager
db_manager = DatabaseManager()

def getUsersByUsername() -> Dict[str, Any]:
    """Get all users from database"""
    try:
        users = db_manager.get_all_users()
        result = {}
        for user in users:
            result[user.username] = {
                'username': user.username,
                'displayName': user.display_name,
                'roleStr': user.role.role_name if user.role else 'Unknown',
                'role': user.role.role_code if user.role else 'unknown',
                'password': user.password_hash,  # Keep password_hash for backward compatibility
                'is_active': user.is_active
            }
        return result
    except Exception as e:
        print(f"Error getting users from database: {e}")
        # No fallback - return empty dict if database fails
        return {}

def getUserByUsername(username: str) -> Optional[Dict[str, Any]]:
    """Get user by username from database"""
    try:
        user = db_manager.get_user_by_username(username)
        if user:
            return {
                'username': user.username,
                'displayName': user.display_name,
                'roleStr': user.role.role_name if user.role else 'Unknown',
                'role': user.role.role_code if user.role else 'unknown',
                'email': user.email,
                'role_id': user.role_id,
                'client_id': user.client_id,
                'role_name': user.role.role_name if user.role else 'Unknown',
                'client_name': user.client.name if user.client else None,
                'first_name': user.first_name,
                'last_name': user.last_name,
                'job_title': user.job_title,
                'password': user.password_hash,  # Keep password_hash for backward compatibility
                'is_active': user.is_active
            }
        return None
    except Exception as e:
        print(f"Error getting user from database: {e}")
        # No fallback - return None if database fails
        return None


def getUserProfileSectionData(username:str):
    user=getUserByUsername(username)
    if user is None:
        # Return default profile data if user not found
        return {
            "profileImg": '',  # Placeholder for profile image URL
            "profileName": "Unknown User",
            "profileDesignation": "Unknown Role"
        }
    return {
        "profileImg": '',  # Placeholder for profile image URL
        "profileName": user['displayName'],
        "profileDesignation": user['roleStr']
    }

def _getClientPermissions(client_id: Optional[int]) -> Dict[str, Any]:
    """
    Get permissions (products/modules and masters) for a client.
    Returns only permissions that exist for the client (not all available ones).
    Returns a dictionary with 'products' and 'masters' keys.
    """
    if not client_id:
        return {
            "products": {},
            "masters": {}
        }
    
    try:
        session = db_manager.get_session()
        try:
            # Get module permissions for this client (only ones that exist)
            module_permissions_query = session.query(
                RoleOrClientBasedModuleLevelPermission.module_id,
                RoleOrClientBasedModuleLevelPermission.client_has_permission,
                Module.module_name,
                Module.parent_id
            ).join(
                Module,
                Module.id == RoleOrClientBasedModuleLevelPermission.module_id
            ).filter(
                RoleOrClientBasedModuleLevelPermission.client_id == client_id,
                RoleOrClientBasedModuleLevelPermission.is_active == True,
                RoleOrClientBasedModuleLevelPermission.module_id.isnot(None),
                Module.is_active == True
            ).all()
            
            # Get all modules to build parent-child mapping (only for modules with permissions)
            module_ids_with_permissions = {perm.module_id for perm in module_permissions_query}
            
            if module_ids_with_permissions:
                # Get module details for modules that have permissions
                modules_with_perms = session.query(
                    Module.id,
                    Module.module_name,
                    Module.parent_id
                ).filter(
                    Module.id.in_(module_ids_with_permissions),
                    Module.is_active == True
                ).all()
                
                # Build module map for quick lookup
                module_map = {}
                for m in modules_with_perms:
                    module_map[m.id] = {
                        'name': m.module_name,
                        'parent_id': m.parent_id
                    }
                
                # Also get parent modules if they're not in the permission list
                parent_ids_needed = {m.parent_id for m in modules_with_perms if m.parent_id and m.parent_id not in module_ids_with_permissions}
                if parent_ids_needed:
                    parent_modules = session.query(
                        Module.id,
                        Module.module_name,
                        Module.parent_id
                    ).filter(
                        Module.id.in_(parent_ids_needed),
                        Module.is_active == True
                    ).all()
                    for m in parent_modules:
                        module_map[m.id] = {
                            'name': m.module_name,
                            'parent_id': m.parent_id
                        }
                
                # Create a map of module_id -> enabled status
                module_permission_map = {
                    perm.module_id: perm.client_has_permission 
                    for perm in module_permissions_query
                }
                
                # Build products structure - only include modules with permissions
                products_dict = {}
                
                # Process modules to build the structure
                for perm in module_permissions_query:
                    module_id = perm.module_id
                    has_permission = perm.client_has_permission
                    module_name = perm.module_name
                    parent_id = perm.parent_id
                    
                    if parent_id is None:
                        # This is a parent module (product like "Frame", "NAV Validus")
                        if module_name not in products_dict:
                            products_dict[module_name] = {
                                "enabled": has_permission or False,
                                "modules": {}
                            }
                    else:
                        # This is a child module
                        parent_info = module_map.get(parent_id)
                        if parent_info:
                            parent_name = parent_info['name']
                            # Ensure parent exists in products_dict
                            if parent_name not in products_dict:
                                # Check if parent has permission record
                                parent_has_perm = module_permission_map.get(parent_id, False)
                                products_dict[parent_name] = {
                                    "enabled": parent_has_perm,
                                    "modules": {}
                                }
                            # Add child module
                            products_dict[parent_name]["modules"][module_name] = {
                                "enabled": has_permission or False
                            }
            else:
                products_dict = {}
            
            # Get master permissions for this client (only ones that exist)
            master_permissions_query = session.query(
                RoleOrClientBasedModuleLevelPermission.master_id,
                RoleOrClientBasedModuleLevelPermission.client_has_permission,
                Master.name
            ).join(
                Master,
                Master.id == RoleOrClientBasedModuleLevelPermission.master_id
            ).filter(
                RoleOrClientBasedModuleLevelPermission.client_id == client_id,
                RoleOrClientBasedModuleLevelPermission.is_active == True,
                RoleOrClientBasedModuleLevelPermission.master_id.isnot(None),
                Master.is_active == True
            ).all()
            
            # Build masters dict - only include masters with permission records
            masters_dict = {}
            for perm in master_permissions_query:
                master_name = perm.name
                has_permission = perm.client_has_permission
                masters_dict[master_name] = {
                    "enabled": has_permission or False
                }
            
            return {
                "products": products_dict,
                "masters": masters_dict
            }
            
        finally:
            session.close()
            
    except Exception as e:
        print(f"Error getting client permissions: {e}")
        import traceback
        traceback.print_exc()
        return {
            "products": {},
            "masters": {}
        }


def _getRoleFeatureAccess(role_id: Optional[int]) -> list:
    """
    Get feature-access structure for a role based on role_id.
    Returns a hierarchical list of modules with permissions (create, view, update, delete).
    """
    if not role_id:
        return []
    
    try:
        session = db_manager.get_session()
        try:
            # Get all role-based module permissions
            role_permissions = session.query(
                RoleOrClientBasedModuleLevelPermission.module_id,
                Permission.permission_code,
                Module.module_name,
                Module.parent_id,
                Module.id
            ).join(
                Permission,
                Permission.id == RoleOrClientBasedModuleLevelPermission.permission_id
            ).join(
                Module,
                Module.id == RoleOrClientBasedModuleLevelPermission.module_id
            ).filter(
                RoleOrClientBasedModuleLevelPermission.role_id == role_id,
                RoleOrClientBasedModuleLevelPermission.client_id.is_(None),
                RoleOrClientBasedModuleLevelPermission.is_active == True,
                RoleOrClientBasedModuleLevelPermission.module_id.isnot(None),
                Module.is_active == True,
                Permission.is_active == True
            ).all()
            
            # Get ALL active modules to build complete hierarchy
            all_modules = session.query(
                Module.id,
                Module.module_name,
                Module.parent_id
            ).filter(
                Module.is_active == True
            ).all()
            
            # Build complete module map
            module_map = {}  # module_id -> {module_name, parent_id}
            module_permissions_map = {}  # module_id -> {module_name, parent_id, permissions: {create, view, update, delete}}
            
            # First, add all modules to the map
            for module in all_modules:
                module_map[module.id] = {
                    'name': module.module_name,
                    'parent_id': module.parent_id
                }
                # Initialize with empty permissions
                module_permissions_map[module.id] = {
                    'name': module.module_name,
                    'parent_id': module.parent_id,
                    'permissions': {
                        'create': False,
                        'view': False,
                        'update': False,
                        'delete': False
                    }
                }
            
            # Now apply permissions from role_permissions
            for perm in role_permissions:
                module_id = perm.module_id
                permission_code = perm.permission_code
                
                # Map permission codes (read -> view)
                if permission_code == 'create':
                    module_permissions_map[module_id]['permissions']['create'] = True
                elif permission_code == 'read':
                    module_permissions_map[module_id]['permissions']['view'] = True
                elif permission_code == 'update':
                    module_permissions_map[module_id]['permissions']['update'] = True
                elif permission_code == 'delete':
                    module_permissions_map[module_id]['permissions']['delete'] = True
            
            # Build hierarchical structure
            # Level 1: Top-level modules (parent_id is None)
            # Level 2: Modules with parent_id pointing to level 1
            # Level 3: Modules with parent_id pointing to level 2
            
            # Group modules by level - store module_id with module_info
            top_level_modules = {}  # module_id -> module_info
            second_level_modules = {}  # parent_id -> [(module_id, module_info)]
            third_level_modules = {}  # parent_id -> [(module_id, module_info)]
            
            for module_id, module_info in module_permissions_map.items():
                parent_id = module_info['parent_id']
                
                if parent_id is None:
                    # Top level
                    top_level_modules[module_id] = module_info
                else:
                    # Check if parent is top level
                    parent_info = module_map.get(parent_id)
                    if parent_info and parent_info['parent_id'] is None:
                        # Second level
                        if parent_id not in second_level_modules:
                            second_level_modules[parent_id] = []
                        second_level_modules[parent_id].append((module_id, module_info))
                    else:
                        # Third level
                        if parent_id not in third_level_modules:
                            third_level_modules[parent_id] = []
                        third_level_modules[parent_id].append((module_id, module_info))
            
            # Build the feature-access array
            feature_access = []
            
            # Sort top-level modules by name for consistent ordering
            sorted_top_modules = sorted(top_level_modules.items(), key=lambda x: x[1]['name'])
            
            # Process top-level modules
            for top_module_id, top_module_info in sorted_top_modules:
                top_module_entry = {
                    "module": top_module_info['name'],
                    "_children": []
                }
                
                # Add second-level children (sort by name for consistency)
                second_level_children = sorted(
                    second_level_modules.get(top_module_id, []),
                    key=lambda x: x[1]['name']
                )
                for second_child_id, second_child_info in second_level_children:
                    second_child_entry = {
                        "module": second_child_info['name'],
                        "_children": []
                    }
                    
                    # Get third-level children for this second-level module (sort by name)
                    third_level_children = sorted(
                        third_level_modules.get(second_child_id, []),
                        key=lambda x: x[1]['name']
                    )
                    
                    for third_child_id, third_child_info in third_level_children:
                        third_child_entry = {
                            "module": third_child_info['name'],
                            "create": third_child_info['permissions']['create'],
                            "view": third_child_info['permissions']['view'],
                            "update": third_child_info['permissions']['update'],
                            "delete": third_child_info['permissions']['delete']
                        }
                        second_child_entry["_children"].append(third_child_entry)
                    
                    # If no third-level children, check if second-level has permissions
                    if not second_child_entry["_children"]:
                        perms = second_child_info['permissions']
                        if any(perms.values()):
                            # Add as leaf with permissions
                            second_child_entry = {
                                "module": second_child_info['name'],
                                "create": perms['create'],
                                "view": perms['view'],
                                "update": perms['update'],
                                "delete": perms['delete']
                            }
                            top_module_entry["_children"].append(second_child_entry)
                        else:
                            # Add as container without permissions
                            top_module_entry["_children"].append(second_child_entry)
                    else:
                        top_module_entry["_children"].append(second_child_entry)
                
                feature_access.append(top_module_entry)
            
            return feature_access
            
        finally:
            session.close()
            
    except Exception as e:
        print(f"Error getting role feature access: {e}")
        import traceback
        traceback.print_exc()
        return []


def getUserPreferences(username:str):
    user = getUserByUsername(username)
    if user is None:
        # Return default preferences if user not found
        return {
            "username": username,
            "displayName": "Unknown User",
            "roleStr": "Unknown Role",
            "role": "unknown",
            "email": "",
            "role_id": "",
            "client_id": "",
            "role_name": "",
            "client_name": "",
            "first_name": "",
            "last_name": "",
            "job_title": "",
            "permissions": {
                "products": {},
                "masters": {}
            },
            "feature-access": []
        }
    
    # Get client permissions
    client_id = user.get('client_id')
    permissions = _getClientPermissions(client_id)
    
    # Get feature-access based on role_id
    role_id = user.get('role_id')
    feature_access = _getRoleFeatureAccess(role_id)
    
    return {
        "username": user['username'],
        "displayName": user['displayName'],
        "roleStr": user['roleStr'],
        "role": user['role'],
        "email": user.get('email', ''),
        "role_id": user.get('role_id', ''),
        "client_id": user.get('client_id', ''),
        "role_name": user.get('role_name', ''),
        "client_name": user.get('client_name', ''),
        "first_name": user.get('first_name', ''),
        "last_name": user.get('last_name', ''),
        "job_title": user.get('job_title', ''),
        "permissions": permissions,
        "feature-access": feature_access
    }



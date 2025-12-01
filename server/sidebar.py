"""
Sidebar API
Provides endpoints for sidebar navigation data.
"""
import logging
from fastapi import APIRouter, Depends
from fastapi.responses import JSONResponse
from rbac.utils.auth import getCurrentUser
from rbac.utils.frontend import getUserByUsername
from database_models import DatabaseManager

logger = logging.getLogger(__name__)

router = APIRouter(tags=["Navigation"])

async def authenticate_user(username: str = Depends(getCurrentUser)):
    return username

def is_admin_user(user_role: str) -> bool:
    """Check if user is an admin"""
    if not user_role:
        return False
    return 'admin' in user_role.lower()

@router.get("/user-modules")
def get_user_modules(*, __username: str = Depends(authenticate_user)):
    """Get modules that the authenticated user has permissions for"""
    try:
        db_manager = DatabaseManager()
        result = db_manager.get_user_permissions(__username)
        
        if not result.get("success", False):
            return JSONResponse(
                content={"error": result.get("error", "Failed to get user permissions")}, 
                status_code=400
            )
        
        return JSONResponse(content=result)
        
    except Exception as e:
        logger.error(f"Error getting user modules for {__username}: {e}")
        return JSONResponse(
            content={"error": "Internal server error"}, 
            status_code=500
        )

@router.get("/sidebar")
def get_sidebar_data(*, __username: str = Depends(authenticate_user)):
    """Get sidebar navigation data filtered by user permissions"""
    try:
        # Get user information to check admin status
        user = getUserByUsername(__username)
        if not user:
            logger.warning(f"User {__username} not found")
            return JSONResponse(content={"topSection": [], "bottomSection": []})
        
        user_role = user.get('role', '')
        is_admin = is_admin_user(user_role)
        
        # If user is admin, return admin sidebar configuration
        if is_admin:
            admin_sidebar_data = {
                "topSection": [
                    {
                    "label": "FRAME",
                    "icon": "frame-menu",
                    "children": [
                        {
                        "label": "Dashboard",
                        "to": "/frame?page=FrameDashboard"
                        },
                        {
                        "label": "Completeness",
                        "to": "/frame/completeness-dashboard"
                        },
                        {
                        "label": "File Manager",
                        "to": "/frame/file-manager"
                        }
                    ]
                    },
                    {
                    "label": "KUBE",
                    "icon": "vector"
                    },
                    {
                    "label": "VALIDUS",
                    "icon": "validus",
                    "children": [
                        {
                        "label": "NAV",
                        "children": [
                            {
                            "label": "Single Fund",
                            "to": "/validus?page=singleFundCompare"
                            },
                            # {
                            # "label": "Multi Fund",
                            # "to": "/validus?page=MultiFund"
                            # }
                        ]
                        },
                        {
                        "label": "Private Credit",
                        "isDisabled": True,
                        "children": []
                        }
                    ]
                    },
                    {
                    "label": "RESOLVE",
                    "icon": "resolve",
                    "to": "/resolve"
                    },
                    {
                    "label": "",
                    "icon": "plus",
                    "to": "/plus"
                    }
                ],
                "bottomSection": [
                    {
                    "label": "SETUP",
                    "icon": "settings",
                    "children": [
                        {
                        "label": "Common Masters",
                        "children": [
                            {
                            "label": "Client",
                            "to": "/frame?page=ClientMastersForAdmin"
                            },
                            {
                            "label": "Fund Managers",
                            "to": "/frame?page=FundManagerMaster"
                            },
                            {
                            "label": "Fund",
                            "to": "/frame?page=FundMastersForAdmin"
                            },
                            {
                            "label": "Account",
                            "to": "/frame?page=AccountMaster"
                            },
                            {
                            "label": "Source",
                            "to": "/frame/source"
                            }
                        ]
                        },
                        {
                        "label": "Frame Masters",
                        "children": [
                            {
                            "label": "Masters",
                            "children": [
                                {
                                "label": "Account Linking Master",
                                "to": "/frame/account-linking-master"
                                }
                            ]
                            },
                            {
                            "label": "Configuration",
                            "children": [
                                {
                                "label": "Document Configuration",
                                "to": "/setup/document-configuration"
                                },
                                {
                                "label": "Process Configuration",
                                "to": "/setup/process-configuration"
                                },
                                {
                                "label": "ETL",
                                "to": "/setup/etl"
                                }
                            ]
                            },
                            {
                            "label": "Rules",
                            "children": [
                                {
                                "label": "Classification / Ignore Rules",
                                "to": "/setup/rules/classification-or-ignore"
                                },
                                {
                                "label": "Password Rules",
                                "to": "/setup/rules/password"
                                },
                                {
                                "label": "ETL Rules",
                                "to": "/setup/rules/etl"
                                }
                            ]
                            }
                        ]
                        },
                        {
                        "label": "Validus Masters",
                        "children": [
                            {
                            "label": "Master",
                            "children": [
                                {
                                "label": "Validations",
                                "to": "/validus/validation-master"
                                },
                                {
                                "label": "Ratios",
                                "to": "/validus/ratios-master"
                                },
                                {
                                "label": "Benchmarks",
                                "to": "/validus/benchmarks-master"
                                },
                                {
                                "label": "Data Models",
                                "to": "/validus/data-models-master"
                                }
                            ]
                            },
                            {
                            "label": "Configuration",
                            "children": [
                                {
                                "label": "Threshold & Configurations",
                                "to": "/validus/configurations"
                                }
                            ]
                            }
                        ]
                        },
                        {
                        "label": "Organization",
                        "to": "/frame?page=OrganizationForAdmin"
                        }
                    ]
                    },
                    {
                    "label": " ",
                    "icon": "user",
                    "children": [
                        {
                        "type": "divider",
                        "items": [
                            {
                            "label": "Logout",
                            "to": ""
                            }
                        ]
                        }
                    ]
                    }
                ]
                }

            return JSONResponse(content=admin_sidebar_data)
        
        # For non-admin users, get permissions and filter sidebar
        db_manager = DatabaseManager()
        permissions_result = db_manager.get_user_permissions(__username)
        
        if not permissions_result.get("success", False):
            logger.warning(f"Failed to get permissions for user {__username}: {permissions_result.get('error')}")
            # Return empty sidebar if permissions can't be retrieved
            return JSONResponse(content={"topSection": [], "bottomSection": []})
        
        user_permissions = permissions_result.get("permissions", {})
        
        # Define the complete sidebar structure for non-admin users
        full_sidebar_data = {
                             "topSection": [
                                 {
                                 "label": "FRAME",
                                 "icon": "frame-menu",
                                 "module_code": "frame",
                                 "children": [
                                     { "label": "Dashboard", "to": "/frame?page=FrameDashboard" },
                                     {
                                     "label": "Completeness",
                                     "to": "/frame/completeness-dashboard"
                                     },
                                     { "label": "File Manager", "to": "/frame/file-manager" }
                                 ]
                                 },
                                 {
                                 "label": "KUBE",
                                 "icon": "vector",
                                 "module_code": "kube"
                                 },
                                 {
                                 "label": "VALIDUS",
                                 "icon": "validus",
                                 "module_code": "nav_validus",
                                 "children": [
                                     {
                                     "label": "NAV",
                                     "children": [
                                         { "label": "Single Fund", "to": "/validus?page=singleFundCompare" },
                                        #  { "label": "Multi Fund", "to": "/validus?page=MultiFund" }
                                     ]
                                     },
                                     {
                                     "label": "Private Credit",
                                     "isDisabled": True,
                                     "children": []
                                     }
                                 ]
                                 },
                                 {
                                 "label": "RESOLVE",
                                 "icon": "resolve",
                                 "module_code": "resolve",
                                 "to": "/resolve"
                                 },
                                 {
                                 "label": "",
                                 "icon": "plus",
                                 "module_code": "aims",
                                 "to": "/plus"
                                 }
                             ],
                            "bottomSection": [
                                {
                                "label": "SETUP",
                                "icon": "settings",
                                "children": [
                                    {
                                    "label": "Common Masters",
                                    "children": [
                                        { "label": "Client", "to": "/frame?page=ClientMasters" },
                                        {
                                        "label": "Fund Managers",
                                        "to": "/frame?page=FundManagerMaster"
                                        },
                                        { "label": "Fund", "to": "/frame?page=FundMasters" },
                                        { "label": "Account", "to": "/frame?page=AccountMaster" }
                                    ]
                                    },
                                    {
                                    "label": "Frame Masters",
                                    "children": [
                                        {
                                        "label": "Configuration",
                                        "children": [
                                            {
                                            "label": "Document Configuration",
                                            "to": "/setup/document-configuration"
                                            },
                                            {
                                            "label": "Process Configuration",
                                            "to": "/setup/process-configuration"
                                            }
                                        ]
                                        }
                                    ]
                                    },
                                    {
                                    "label": "Validus Masters",
                                    "children": [
                                        {
                                        "label": "Configuration",
                                        "children": [
                                            {
                                            "label": "Threshold & Configurations",
                                            "to": "/validus/configurations"
                                            }
                                        ]
                                        }
                                    ]
                                    },
                                    { "label": "Organization", "to": "/frame?page=Organization" }
                                ]
                                },
                                {
                                "label": " ",
                                "icon": "user",
                                "children": [
                                    {
                                    "type": "divider",
                                    "items": [{ "label": "Logout", "to": "" }]
                                    }
                                ]
                                }
                            ]
                            }
        
        # Filter sidebar based on user permissions
        filtered_sidebar = _filter_sidebar_by_permissions(full_sidebar_data, user_permissions)
        
        # Remove module_code from the response
        cleaned_sidebar = _remove_module_codes(filtered_sidebar)
        
        return JSONResponse(content=cleaned_sidebar)
        
    except Exception as e:
        logger.error(f"Error getting sidebar data for {__username}: {e}")
        # Return empty sidebar on error
        return JSONResponse(content={"topSection": [], "bottomSection": []})

def _filter_sidebar_by_permissions(sidebar_data: dict, user_permissions: dict) -> dict:
    """Filter sidebar sections based on user permissions"""
    filtered_data = {
        "topSection": [],
        "bottomSection": []
    }
    
    # Filter top section
    for item in sidebar_data.get("topSection", []):
        if _has_module_permission(item, user_permissions):
            filtered_data["topSection"].append(item)
    
    # Filter bottom section (setup and user sections are always shown)
    filtered_data["bottomSection"] = sidebar_data.get("bottomSection", [])
    
    return filtered_data

def _has_module_permission(item: dict, user_permissions: dict) -> bool:
    """Check if user has permission for a module"""
    module_code = item.get("module_code")
    
    # If no module_code specified, show the item (for backward compatibility)
    if not module_code:
        return True
    
    # Check if user has any permissions for this module
    if module_code in user_permissions:
        permissions = user_permissions[module_code].get("permissions", [])
        # User has permission if they have any permission for this module
        return len(permissions) > 0
    
    # If module not found in permissions, don't show it
    return False

def _remove_module_codes(data: dict) -> dict:
    """Recursively remove module_code fields from sidebar data"""
    if isinstance(data, dict):
        cleaned_data = {}
        for key, value in data.items():
            if key != "module_code":
                cleaned_data[key] = _remove_module_codes(value)
        return cleaned_data
    elif isinstance(data, list):
        return [_remove_module_codes(item) for item in data]
    else:
        return data
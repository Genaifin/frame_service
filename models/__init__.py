# Import models to make them available when importing from models package
from .permission_models import RoleOrClientBasedModuleLevelPermission, Master

# This makes the models available when importing from the models package
__all__ = ['RoleOrClientBasedModuleLevelPermission', 'Master']

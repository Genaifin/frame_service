import json
import os
import sys
from rbac.utils.userGroups import getAllUserFromRawUsers

# Add the root directory to Python path for database access
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__)))))
from database_models import DatabaseManager

def userHasFundReadPerm(user, fundUniqueId):
    """
    Check if user has permission to access a fund
    Currently allows all users - permission checking can be re-enabled later
    """
    # For now, allow all users access to all funds
    return True

def _checkJsonWallsPermission(user, fundUniqueId):
    """Fallback to JSON-based walls system"""
    myWalls=getWallsByFund(fundUniqueId)
    if myWalls=={}:
        return False
    
    allUsersWithAccess=getAllUserFromRawUsers(myWalls['userGroups'])
    return user in allUsersWithAccess

def getWallsByFund(fundUniqueId):
    allWalls = getWalls()
    if fundUniqueId not in allWalls['byFund']:
        return {}
    return allWalls['byFund'][fundUniqueId]
def getWalls():
    # Get the directory of the current file and construct path relative to it
    current_dir = os.path.dirname(os.path.abspath(__file__))
    walls_path = os.path.join(current_dir, '..', 'walls.json')
    with open(walls_path,'r') as f:
        allWalls=json.load(f)
    return allWalls
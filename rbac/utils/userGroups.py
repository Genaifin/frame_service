import json
import os
from pathlib import Path

def getAllUserGroups():
    #TODO : Use a database or a more secure storage mechanism
    # Get absolute path to userGroups.json file
    current_file = Path(__file__).resolve()
    user_groups_json_path = current_file.parent.parent / "configs" / "userGroups.json"
    
    with open(user_groups_json_path, 'r') as f:
        allgroups=json.load(f)
    return allgroups

def getAllUsersWithAccessToGroup(groupName):
    allgroups = getAllUserGroups()
    if groupName not in allgroups:
        return []
    myUsersRaw=allgroups[groupName]['users']
    myUsers=getAllUserFromRawUsers(myUsersRaw)
    return myUsers

def getAllUserFromRawUsers(rawUsers):
    myUsers = []
    for user in rawUsers:
        if user[:7]=="group::":
            groupUsers=getAllUsersWithAccessToGroup(user[7:])
            myUsers+= groupUsers
        else:
            myUsers.append(user)
    myUsers=list(sorted(set(myUsers)))
    return myUsers

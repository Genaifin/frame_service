from storage import STORAGE
from clients.validusDemo.compliance.utils.checks import userHasFundReadPerm

fundsConfig={
    'NexBridge':{
        'fundDisplayName':'NexBridge',
        'fundCurrency':'USD',
        'fundUniqueId':'NexBridge',
        'fundSources':['Bluefield'],        
    },
    'ASOF':{
        'fundDisplayName':'Altura Strategic Opportunities',
        'fundCurrency':'USD',
        'fundUniqueId':'ASOF',
        'fundSources':['Harborview','ClearLedger'],        
    },
    'Stonewell':{
        'fundDisplayName':'Stonewell Diversified', # keeping short for display purposes
        'fundCurrency':'USD',
        'fundUniqueId':'Stonewell',
        'fundSources':['StratusGA','VeridexAS'],        
    },
}
sourceConfig={
   'Bluefield':{
        'sourceType':'Fund_Admin_1',
        'sourceDisplayName':'Bluefield',
        'sourceUniqueId':'Bluefield',
    },
    'Harborview':{
        'sourceType':'Fund_Admin_1',
        'sourceDisplayName':'Harborview',
        'sourceUniqueId':'Harborview',
    },
    'ClearLedger':{
        'sourceType':'Fund_Admin_2',
        'sourceDisplayName':'ClearLedger',
        'sourceUniqueId':'ClearLedger',
    },
   'StratusGA':{
        'sourceType':'Fund_Admin_1',
        'sourceDisplayName':'StratusGA',
        'sourceUniqueId':'StratusGA',
    },
    'VeridexAS':{
        'sourceType':'Fund_Admin_2',
        'sourceDisplayName':'VeridexAS',
        'sourceUniqueId':'VeridexAS',
    },
    'Source_1_Label':{
        'sourceType':'Fund_Admin_1',
        'sourceDisplayName':'Fund Admin 1',
        'sourceUniqueId':'source_1_label',
    },
    'Admin_Label':{
        'sourceType':'Fund_Admin_1',
        'sourceDisplayName':'Admin',
        'sourceUniqueId':'admin_label',
    },
    'Source_2_Label':{
        'sourceType':'Fund_Admin_2',
        'sourceDisplayName':'Fund Admin 2',
        'sourceUniqueId':'source_2_label',
    },
    'Shadow_Label':{
        'sourceType':'Fund_Admin_2',
        'sourceDisplayName':'Shadow',
        'sourceUniqueId':'shadow_label',
    }
}  

def fundsUserHasReadPermsFor(username:str):
    funds=[]

    for fundId,config in fundsConfig.items():
        if userHasFundReadPerm(username,fundId):
            funds.append(fundId)
    return funds

def getParamSelector(params:dict):
    myStorage=_getStorage()
    allDates=getAllDatesForFunds(myStorage)
    fundsUserHasPerms=fundsUserHasReadPermsFor(params['username'])  
    filteredFundConfig = {k: v for k, v in fundsConfig.items() if k in fundsUserHasPerms}
    newDates={}
    for fundName,values in allDates.items():
        if fundName in fundsUserHasPerms:
            newDates[filteredFundConfig[fundName]['fundDisplayName']]=values


    myFilters=[]
    myFilters.append({
        'type':'group',
        'key':'fund_meta',
        'fields':[    
            {
                'type':'dropdown',
                'key':'fundName',
                'label':'Fund Name',
                'options':[it['fundDisplayName'] for it in filteredFundConfig.values()],
                'defaultValue' : [it['fundDisplayName'] for it in filteredFundConfig.values()][0] if filteredFundConfig else None
            },
            {
                'type':'dropdown',
                'key':'ccy',
                'label':'Currency',
                'dependsOn':['fundName'],
                'dynamicOptions':{filteredFundConfig[fundId]['fundDisplayName']:filteredFundConfig[fundId]['fundCurrency'] for fundId in filteredFundConfig},
                'autoSelectSingleOption':True,
                'alwaysDisabled':True
            }
        ],
        'visibilityConditions':[__visibilityCondition_AlwaysVisible()]
    })
    myFilters.append({
        "autoSelectSingleOption": True,
        "dependsOn": [
            "fundName"
        ],
        "dynamicOptions": {
                "Altura Strategic Opportunities": [
                    "Dual Source"
                ],
                "NexBridge": [
                    "Single Source"
                ],
                "Stonewell Diversified": [
                    "Dual Source"
                ]
        },
        "type": "dropdown",
        "key": "comparison_source",
        "label": "Comparison Source",
        "visibilityConditions": [
            {
                "keys": [
                    "fundName"
                ],
                "type": "dependsOn"
            }
        ]
    })
  
    myFilters.append({
        'type':'group',
        'key':'source_group_1',
        'fields':[    
            {
                "autoSelectSingleOption": True,
                "key": "source_1",
                "label": "Source Type",
                "type": "dropdown",
                "dependsOn": ["fundName"],
                "dynamicOptions": {
                    "Altura Strategic Opportunities": [
                        "Admin"
                    ],
                    "NexBridge": [
                        "Admin"
                    ],
                    "Stonewell Diversified": [
                        "Fund Admin 1"
                    ]
                },
            },
            {
                "autoSelectSingleOption": True,
                'type':'dropdown',
                'key':'sourceA',
                'label':'Select',
                'dependsOn':['fundName'],
                "dynamicOptions": {
                    "Altura Strategic Opportunities": [
                        "Harborview"
                    ],
                    "NexBridge": [
                        "Bluefield"
                    ],
                    "Stonewell Diversified": [
                        "StratusGA"
                    ]
                },
                
            }
        ],
        'visibilityConditions':[_visibilityConditionDependsOn(['comparison_source'])]
    })
    myFilters.append({
        'type':'group',
        'key':'source_group_2',
        'fields':[    
            {
                "autoSelectSingleOption": True,
                "key": "source_2",
                "label": "Source Type",
                "type": "dropdown",
                "dependsOn": ["fundName"],
                "dynamicOptions": {
                    "Altura Strategic Opportunities": [
                        "Shadow"
                    ],
                    "Stonewell Diversified": [
                        "Fund Admin 2"
                    ]
                },
                },
            {
                "autoSelectSingleOption": True,
                "key": "sourceB",
                "label": "Select",
                "type": "dropdown",
                "dynamicOptions": {
                        "Altura Strategic Opportunities": [
                            "ClearLedger"
                        ],
                        "Stonewell Diversified": [
                            "VeridexAS"
                        ]
                    },
                "dependsOn": ["fundName"],             
            }
        ],
        'visibilityConditions':[_visibilityConditionConditions({'comparison_source':'Dual Source'})]
    })


    myFilters.append({
        "type": "dropdown",
        "key": "dateA",
        "label": "Start Month",
        "dependsOn":["fundName"],
        'dynamicOptions':newDates,
        "visibilityConditions": [_visibilityConditionDependsOn(['comparison_source'])],
        'linkedField':{
            'target':'dateB',
            'direction':"next"
        },
        'defaultValue': next(iter(newDates.values()))[0] if newDates and next(iter(newDates.values()), []) else ''
    })
    
    myFilters.append({
        "type": "dropdown",
        "key": "dateB",
        "label": "End Month",
        "dependsOn":["fundName"],
        'dynamicOptions':newDates,
        "visibilityConditions": [_visibilityConditionConditions({'comparison_source':'Single Source'})],
        'linkedField':{
            'target':'dateA',
            'direction':"previous"
        },
        # 'defaultValue': (
        #     next(iter(newDates.values()))[1]
        #     if newDates and len(next(iter(newDates.values()), [])) > 1
        #     else (next(iter(newDates.values()), [])[0] if newDates and next(iter(newDates.values()), []) else '')
        # )
    })
    return {
        'filters':myFilters
    }

def getAllDatesForFunds(aStorage):
    from utils.dateUtil import convertDateToFormat
    
    myTB=aStorage.getFilteredTableAsDF('l1','trail_balance',{})
    myStruct=myTB.groupby('fundName')['processDate'].unique().to_dict()

    for fundName,values in myStruct.items():
        # Convert all dates to m-d-Y format
        formattedDates = []
        for date in values:
            try:
                formattedDate = convertDateToFormat(date, 'MM-DD-YYYY')
                formattedDates.append(formattedDate)
            except Exception:
                # If conversion fails, keep original date
                formattedDates.append(date)
        myStruct[fundName] = sorted(formattedDates)
    return myStruct

def __visibilityCondition_AlwaysVisible():
    return {    
        'type':'alwaysVisible'
    }

def _visibilityConditionDependsOn(keys:list):
    return {
        'type':'dependsOn',
        'keys':keys
    }

def _visibilityConditionDependsOnSingleSource(keys:list):
    return {
        'type':'conditions',
        'conditions': [{'key':"comparison_source",'value':keys }]
    }

def _visibilityConditionConditions(map):
    return {
        'type':'conditions',
        'conditions':[{'key':k,'value':v} for k,v in map.items()]
    }

def _getClient():
    return 'validusDemo' # check for perms here?

def _getStorage():
    myStorageConfig={
        'defaultFileStorage':'onPrem',
    }
    client=_getClient() 
    return STORAGE(client,myStorageConfig)



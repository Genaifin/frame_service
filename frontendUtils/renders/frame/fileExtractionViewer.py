from frontendUtils.renders.utils.nestedTable import getNestedTableFromRenderStructure
from fastapi import HTTPException
from storage import STORAGE
from utils.statusSync import sync_file_statuses
from utils.profanityFilter import filter_profanity_in_data

def getAllFilesTabs(params:dict):
    myRender={
        'tabProperties':{
            "type": "secondary",
            "subTabKey": "SLAWise"
        },
        'tableProperties':getAllFilesTable(params)
    }
    return myRender

def getAllFilesTable(params:dict):
    myStorage=_getStorage()
    l2State=myStorage.getState('l2')
    myRows=[]

    # Sync real-time status from queue before loading data
    try:
        sync_file_statuses()
    except Exception as e:
        print(f"Warning: Status sync failed: {e}")

    allMeta=myStorage.getJSONDump('ldummy',[],'allFileMeta')

    for fileName, fileData in allMeta.items():
        if isinstance(fileData, dict):
            # Generate complete row data structure with all required fields
            fileHash = fileData.get("fileHash", "unknown")
            fileType = fileData.get("fileType", "Document")
            status = fileData.get("status", "Unknown")
            
            # Generate fileID from fileHash (last 6 characters)
            fileID = fileHash[-6:] if len(fileHash) > 6 else fileHash
            
            # Generate sample data for demonstration (in real implementation, these would come from actual data)
            # Create variation in SLAWise values for demonstration
            slaWiseOptions = ["Within SLA", "SLA", "SLA Breached", "Uncategorised"]
            slaWiseIndex = hash(fileName) % len(slaWiseOptions)
            
            enhancedFileData = {
                "fileHash": fileHash,
                "fileType": fileType,
                "fileID": fileID,
                "fileName": fileName,
                "receivedDate": "21-04-2025",  # This should come from actual upload date
                "status": status,
                "statusDate": "21-04-2025",  # This should come from actual status date
                "stage": "Via Automation",
                "source": "SFTP",
                "age": "1 Day",
                "SLAType": "SLA",
                "fund": "BlackRock's Fundamental Equities (FE)",
                "accountName": "Finance Bank National Association",
                "clientName": "High Growth Partners",
                "extractor": "-",
                "SLAWise": slaWiseOptions[slaWiseIndex],  # Vary SLAWise values for demonstration
                "clickAction": {
                    "type": "navigation",
                    "to": "/frame",
                    "parameters": [
                        {
                            "key": "fileHash",
                            "value": "",
                            "dynamicValue": {
                                "enabled": True,
                                "id": "fileHash"
                            }
                        },
                        {
                            "key": "page",
                            "value": "FileViewer"
                        }
                    ]
                }
            }
            myRows.append(enhancedFileData)
    
    myTableProperties={
        'tableType':"rowGrouping",
        "moduleType": "nestedTable",
        "rowGroupings": ["fileType"],
        "colsToShow": [
            "fileID",
            "fileName",
            "receivedDate",
            "status",
            "statusDate",
            "stage",
            "source",
            "age",
            "SLAType",
            "fund",
            "accountName",
            "clientName",
            "extractor"
        ],
        "autoGroupColumnDef": {
            "headerName": "File Type"
        },
        "autoSizeColumns": False,
        "groupDisplayType": "groupRows",
        "columnConfig": {
            "fileID": {
                "name": "File ID",
                "minWidth": 180
            },
            "fileName": {
                "customCellRenderer": "mask",
                "name": "File Name",
                "minWidth": 180
            },
            "receivedDate": {
                "name": "Received Date",
                "minWidth": 180
            },
            "status": {
                "customCellRenderer": "labelToNumberAggregator",
                "name": "Status",
                "minWidth": 180
            },
            "statusDate": {
                "name": "Status Date",
                "minWidth": 180
            },
            "stage": {
                "name": "Stage",
                "minWidth": 180
            },
            "source": {
                "name": "Source",
                "minWidth": 180
            },
            "age": {
                "name": "Age",
                "minWidth": 180
            },
            "SLAType": {
                "name": "SLA Type",
                "minWidth": 180
            },
            "fund": {
                "name": "Fund",
                "filter": "agTextColumnFilter",
                "minWidth": 180
            },
            "accountName": {
                "name": "Account Name",
                "filter": "agTextColumnFilter",
                "minWidth": 180
            },
            "clientName": {
                "name": "Client Name",
                "filter": "agTextColumnFilter",
                "minWidth": 180
            },
            "extractor": {
                "name": "Extractor",
                "filter": "agTextColumnFilter",
                "minWidth": 180
            }
        },
        "rowClickEnabled": True,
        "rowData": myRows,
    }

    return getNestedTableFromRenderStructure(myTableProperties)

def getGetAllExtractedDataFor1File(params:dict):
    myStorage=_getStorage()
    allHashes=myStorage.getAllLayerNFiles('l1')
    l2State=myStorage.getState('l2')

    myQuery=params['query']

    fileHash=myQuery.get('fileHash',None)
    if fileHash is None:
        raise HTTPException(status_code=400, detail="fileHash is required")
    
    if fileHash not in allHashes:
        raise HTTPException(status_code=400, detail="fileHash is invalid")
        
    myRender=myStorage.getJSONDump('l1',fileHash,'forFrontend')

    # Filter profanity from the response data
    filteredRender = filter_profanity_in_data(myRender)

    return filteredRender

def _getClient():
    return 'frameDemo' # check for perms here?

def _getStorage():
    myStorageConfig={
        'defaultFileStorage':'onPrem',
    }
    client=_getClient() 
    return STORAGE(client,myStorageConfig)

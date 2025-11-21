from storage import STORAGE
from fastapi.responses import FileResponse
from fastapi import HTTPException
import json

def getPdfData(params:dict):
    myStorage=_getStorage()
    myQuery=params['query']
    if 'pdfHash' not in myQuery:
        raise HTTPException(status_code=400, detail="pdfHash is required")
    
    myLocalPath=myStorage.getLocalFilePath('l1',[myQuery['pdfHash'],'rawFile.pdf'])

    if not myStorage.fileExists(myLocalPath):
        raise HTTPException(status_code=400, detail="pdfHash is invalid")
    
    return FileResponse(myLocalPath,
        headers={
            "Content-Disposition": 'inline"'
        })

def getJsonData(params:dict):
    myStorage=_getStorage()
    myQuery=params['query']
    if 'jsonHash' not in myQuery:
        raise HTTPException(status_code=400, detail="jsonHash is required")
    
    myLocalPath=myStorage.getLocalFilePath('l1',[myQuery['jsonHash'],'forFrontend.json'])

    if not myStorage.fileExists(myLocalPath):
        raise HTTPException(status_code=400, detail="jsonHash is invalid")
    
    return FileResponse(myLocalPath,
        headers={
            "Content-Disposition": 'inline"',
            "Content-Type": "application/json"
        })

def _getClient():
    return 'frameDemo' # check for perms here?

def _getStorage():
    myStorageConfig={
        'defaultFileStorage':'onPrem',
    }
    client=_getClient() 
    return STORAGE(client,myStorageConfig)
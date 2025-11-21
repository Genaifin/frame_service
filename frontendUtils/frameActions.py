
from frontendUtils.renders.frame.frameDummyActions import uploadFile,clearState
from frontendUtils.renders.frame.fileGetters import getPdfData, getJsonData
from fastapi import HTTPException

async def takeFrameAction(params:dict):
    querySubType=params['query'].get('_subType',None)
    globalFuncMap=await fileGetterFuncs()

    if querySubType is None:
        raise HTTPException(status_code=400, detail="_subType is required")
    
    if querySubType not in globalFuncMap:
        raise HTTPException(status_code=400, detail=f"_subType {querySubType} is invalid")
    
    myFuncMap=globalFuncMap[querySubType]
    
    fileType=params['query'].get('fileType',None)
    if fileType is None:
        raise HTTPException(status_code=400, detail="fileType is required")

    if fileType not in myFuncMap:
        raise HTTPException(status_code=400, detail=f"fileType {fileType} is invalid")
    
    myFunc=myFuncMap[fileType]
    return myFunc(params)

async def fileGetterFuncs():
    return {
        'getFile':{
            'pdf':getPdfData,
            'json':getJsonData,
        },
        'uploadFile':{
            'pdf':uploadFile,
        },
        'clearState':{
            'pdf':clearState,
        },
    }
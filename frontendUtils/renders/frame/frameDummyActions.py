from fastapi.responses import JSONResponse
from fastapi import HTTPException
from storage import STORAGE


def uploadFile(params:dict):
    myQuery=params['query']
    pdfName=myQuery.get('pdfName',None)
    if pdfName is None:
        raise HTTPException(status_code=400, detail="pdfName is required")
    myStorage=_getStorage()

    allPDFs=myStorage.getAllLayerNFiles('l0') 

    #todo: need to add more data to calculate this one 
    myDataOp={
        "dataTypeToSaveAs":"statusUpdate",
        "opParams":{
            "layerName":"l2",
            "trackerName":"processedFiles",
            "operation":"replaceOrAppendByKey",
            "key":[pdfName]
        }
    }
    
    myStorage.doDataOperation(myDataOp)

    myResponse= {
            "filename": pdfName,
            "message": f"successfully uploaded {pdfName}",
        }
    return JSONResponse(content=myResponse)

def clearState(params:dict):
    myStorage=_getStorage()
    myStorage.clearState('l2')

    myResponse= {
            "message": f"successfully cleared state",
        }
    return JSONResponse(content=myResponse)
    
def _getClient():
    return 'frameDemo' # check for perms here?

def _getStorage():
    myStorageConfig={
        'defaultFileStorage':'onPrem',
    }
    client=_getClient() 
    return STORAGE(client,myStorageConfig)
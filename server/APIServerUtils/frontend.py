from fastapi import HTTPException
from frontendUtils.frameActions import takeFrameAction
from .athena import athenaResponse

from fastapi.responses import JSONResponse

async def getResponse(params:dict):
    queryType=params['query'].get('_type',None)
    if queryType is None:
        raise HTTPException(status_code=400, detail="_type is required")
    
    if queryType == 'frameAction':
        return await takeFrameAction(params)
    
    if queryType == 'athena':
        return await athenaResponse(params) 
    
    raise HTTPException(status_code=400, detail=f"queryType {queryType} is invalid")

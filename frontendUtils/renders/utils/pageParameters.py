from frontendUtils.renders.validus.paramSelector import getParamSelector
from fastapi import HTTPException
nameToFunc={
    'validus_singleFund':getParamSelector
}

def getParameters(params:dict):
    """
    This function is used to get the parameters for the page.
    """
    querySubType=params['query'].get('paramName',None)
    if querySubType is None:
        raise HTTPException(status_code=400, detail="paramName is required")
    
    if querySubType not in nameToFunc:
        raise HTTPException(status_code=400, detail=f"paramName {querySubType} is invalid")
    
    myFunc=nameToFunc[querySubType]

    myResponse=myFunc(params)
    return myResponse   

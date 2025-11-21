import frontendUtils.renders.config.singleFundPages as allPagesConfig
from fastapi import HTTPException
def convertToFrontEndConfig(aPageConfig): # mostly unsed now
    myConfig=[]

    currentY=0
    currentId=0
    for row in aPageConfig:
        currentX=0
        maxY=0
        for myModule in row:
            myModuleConfig,currentX,currentId=myModule.getFrontendConfig(currentX,currentY,currentId)
            maxY=max(maxY,myModule.h)
            myConfig.append(myModuleConfig)

        if currentX>12:
            raise Exception('width for row %s %.2f'%(str([ aModule.metricName for aModule in row]),currentX))

        currentY+=maxY
    return myConfig

def convertNAVParamsToFrontEndConfig(aNAVParams,username:str):
    myConfig=[]
    for aNAVParam in aNAVParams:
        myConfig.append(aNAVParam.getFrontendParams(username))
    return myConfig

def getPageConfig(params:dict):
    aPageName=params['query'].get('pageName',None)
    if aPageName is None:
        raise HTTPException(status_code=400, detail="pageName is required")

    if aPageName not in allPagesConfig.pages:
        raise HTTPException(status_code=400, detail=f"Dont know how to get page config for {aPageName}")

    myPageClass=allPagesConfig.pages[aPageName]
    myPageConfig=myPageClass.getFrontendConfig(params['username'])
    
    return myPageConfig
    
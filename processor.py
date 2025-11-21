from storage import STORAGE
from utils.jsonConversionHelper import getLambdaFromString

def processBox(aUBI:str,aClient:str,aSLABox:dict,aTaskConfig:dict,aRunDate:str,aStorage:STORAGE):
    myBox=getLambdaFromString(aClient,aUBI)()
    myConfig=aSLABox['extraConfig']
    myConfig['client']=aClient
    myConfig['taskConfig']=aTaskConfig
    myConfig['runDate']=aRunDate
    myBox.setConfig(myConfig,aStorage)
    myResult=myBox.process()
    if myResult['status']=='success':
        for myDataOp in myResult['dataOps']:
            aStorage.doDataOperation(myDataOp)

    if myResult['status'] in ['success','skipped']:
        boxesPostProcess=aSLABox.get('boxesPostProcess',[])
        if len(boxesPostProcess)>0:
            for boxPostProcess in boxesPostProcess:
                processBox(boxPostProcess['UBI'],aClient,boxPostProcess,aTaskConfig,aRunDate,aStorage)
        else:
            return 0


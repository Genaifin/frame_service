from file import XLSX
from storage import STORAGE
from utils.dateUtil import fileNameMatches
import copy

class L0ProcessorNAV():
    # def __init__(self):
    #     self.aFund = kwargs.get('aFund')
    #     self.aFileType = kwargs.get('aFileType')
    #     self.aDate = kwargs.get('aDate')

    def setConfig(self, aConfig: dict,storage: STORAGE):
        self.config = aConfig
        self.client = aConfig.get('client')
        self.taskConfig = aConfig.get('taskConfig')
        self.L0File = aConfig.get('L0File')
        self.runDate = aConfig.get('runDate')

        self.storage=storage

    def getUniqueRunID(self):
        return f"L0Box___{self.client}___{self.taskName}___{self.L0File}___{self.runDate}"
    
    def process(self):
        startingState=self.storage.getState('l0')
        fileMetadata=self.taskConfig['L0Files'][self.L0File]

        myFile=self.getFileMeetingExpectations(fileMetadata)

        if myFile is None:
            return {'status':'error', 'message':f"ERROR: Havent Recieved Expected File {self.L0File}"}
        else:
            if myFile.getFilename() in startingState.get('processedFiles',[]):
                print(f"Skipping File {myFile.getFilename()} because it has already been processed")
                return {'status':'skipped'}
            print(f"Processing File {myFile.getFilename()}")

            myDataOps=myFile.getDataOperations(fileMetadata['actionsBySheets'],{
                'processDate':self.runDate,
                'fundName':self.taskConfig['fundUniqueId']
            })
            myDataOps.append(
                {
                    "dataTypeToSaveAs":"statusUpdate",
                    "opParams":{
                        "layerName":"l0",
                        "trackerName":"processedFiles",
                        "operation":"replaceOrAppendByKey",
                        "key":[myFile.getFilename()]
                    }
                }
            )
            return {'status':'success', 'dataOps':copy.deepcopy(myDataOps)}
    
    def getFileMeetingExpectations(self,fileMetadata: dict):
        allFiles=self.storage.getAllLayerNFiles('l0')
        for file in allFiles:
            if fileNameMatches(file,fileMetadata['customFileIdentifier'],self.runDate,fileMetadata['dateInNameFormat']):
                if fileMetadata['fileFormat']=='XLSX':
                    return XLSX(self.storage,self.storage.getLocalFilePath('l0',file))
        return None

        

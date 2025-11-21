import os
import json
import pandas as pd
from pathlib import Path
from utils.jsonConversionHelper import getLambdaFromString
import utils.logger as _logger
import pickle
import shutil
from PyPDF2 import PdfWriter
logger=_logger.getLogger('dev')

class STORAGE():
    def __init__(self,aClient: str,aStorageConfig: dict):
        self.client=aClient
        self.storageConfig=aStorageConfig

    def fileExists(self,aLocalFilePath: str):
        return os.path.exists(aLocalFilePath)
    
    def getAllLayerNFiles(self,aLayerName: str,subFolders: list=[]):
        self.onlySupportedForOnPrem()
        files=os.listdir(self.getDir(aLayerName,subFolders))
        if '.ipynb_checkpoints' in files:
            files.remove('.ipynb_checkpoints')
        return files
    
    def getDir(self,aLayerName: str,subFolders: list=[]):
        self.onlySupportedForOnPrem()
        folderPath=os.path.join(self.getLayerNFolder(aLayerName),*subFolders)
        if not os.path.exists(folderPath):
            os.makedirs(folderPath, exist_ok=True)
        return folderPath
    
    def _handleTabularOperation(self,aDataOperation: dict):
        myOpParams=aDataOperation['opParams']

        if myOpParams['operation']=='replaceOrAppendByKey':
            myDF=pd.DataFrame(json.loads(aDataOperation['rows']))
            myFilePath=self.getLocalFilePath(myOpParams['layerName'],myOpParams['tableName'])+'.csv'
            if os.path.exists(myFilePath):
                logger.info('%s already exists, appending %d rows to it' %(myOpParams['tableName'],len(myDF)))
                myExistingDF=self.getFullTableAsDF(myOpParams['layerName'],myOpParams['tableName'])
                assert myExistingDF.dtypes.equals(myDF.dtypes), 'dont know how to append unmatched schemas %s %s' %(str(myExistingDF.dtypes),str(myDF.dtypes))
                
                startingLen=len(myExistingDF)
                myExistingDF=myExistingDF.merge(myDF[myOpParams['key']],on=myOpParams['key'], how='left', indicator=True).query('_merge == "left_only"').drop('_merge', axis=1)
                endLen=len(myExistingDF)
                if startingLen!=endLen:
                    logger.info('removed %d rows in %s on key = %s'%(startingLen-endLen,myOpParams['tableName'],str(myOpParams['key'])))

                assert myExistingDF.dtypes.equals(myDF.dtypes), 'schema changed? %s %s' %(str(myExistingDF.dtypes),str(myDF.dtypes))

                myExistingDF=pd.concat([myExistingDF,myDF],ignore_index=True)
                myExistingDF.to_csv(myFilePath,index=False)
            else:
                os.makedirs(self.getLayerNFolder(myOpParams['layerName']), exist_ok=True)
                myDF.to_csv(myFilePath,index=False)
        elif myOpParams['operation']=='deleteByKey':
            myFilePath=self.getLocalFilePath(myOpParams['layerName'],myOpParams['tableName'])+'.csv'
            if os.path.exists(myFilePath):
                myExistingDF=self.getFullTableAsDF(myOpParams['layerName'],myOpParams['tableName'])
                startingLen=len(myExistingDF)
                mask = pd.Series([True] * len(myExistingDF))
                for col, val in aDataOperation['key'].items():
                    mask &= myExistingDF[col] == val
                myExistingDF=myExistingDF[~mask]
                endLen=len(myExistingDF)
                if startingLen!=endLen:
                    logger.info('deleted %d rows in %s on key = %s'%(startingLen-endLen,myOpParams['tableName'],str(aDataOperation['key'])))

                if len(myExistingDF)>0:
                    myExistingDF.to_csv(myFilePath,index=False)
                else:
                    os.remove(myFilePath) # controversial
            else:
                logger.info('no table %s found in layer %s'%(myOpParams['tableName'],myOpParams['layerName']))
        else:
            raise Exception(f"Only replaceOrAppendByKey and deleteByKey are supported for now not {myOpParams['operation']}")

    def _handleStatusUpdateOperation(self,aDataOperation: dict):
        myOpParams=aDataOperation['opParams']
        
        if myOpParams['operation']=='replaceOrAppendByKey':
            currentStatus=self.getState(myOpParams['layerName'])
            currentTracker=currentStatus.get(myOpParams['trackerName'],[])
            currentTracker=currentTracker+myOpParams['key']
            currentTracker=list(set(currentTracker))
            currentStatus[myOpParams['trackerName']]=currentTracker
            self.updateState(myOpParams['layerName'],currentStatus)
        elif myOpParams['operation']=='replaceOrAppendKeyValue':
            currentStatus=self.getState(myOpParams['layerName'])
            if myOpParams['trackerName'] not in currentStatus:
                currentStatus[myOpParams['trackerName']]={}

            currentStatus[myOpParams['trackerName']][myOpParams['key']]=myOpParams['value']
            self.updateState(myOpParams['layerName'],currentStatus)
        else:
            raise Exception(f"Only replaceOrAppendByKey and replaceOrAppendKeyValue are supported for now not {myOpParams['operation']}")

    def _handleJSONDumpOperation(self,aDataOperation: dict):
        myOpParams=aDataOperation['opParams']
        myFolderArray=myOpParams['folderArray']
        myFilePath=self.getLocalFilePath(myOpParams['layerName'],myFolderArray+[aDataOperation['key']+'.json'])
        self.getDir(myOpParams['layerName'],myFolderArray) # this makes sure that the folder exists
        if myOpParams['operation']=='replace':
            with open(myFilePath,'w') as f:
                json.dump(aDataOperation['data'],f,indent=2)
        else:
            raise Exception(f"Only replace is supported for now not {myOpParams['operation']}")

    def _handleVariableDumpOperation(self,aDataOperation: dict):
        myOpParams=aDataOperation['opParams']
        myFolderArray=myOpParams['folderArray']
        myFilePath=self.getLocalFilePath(myOpParams['layerName'],myFolderArray+[aDataOperation['key']])
        self.getDir(myOpParams['layerName'],myFolderArray) # this makes sure that the folder exists
        with open(myFilePath+'.pkl', 'wb') as f:
            pickle.dump(aDataOperation['data'], f)

    def _handleTextDumpOperation(self,aDataOperation: dict):
        myOpParams=aDataOperation['opParams']
        myFolderArray=myOpParams['folderArray']
        myFilePath=self.getLocalFilePath(myOpParams['layerName'],myFolderArray+[aDataOperation['key']])
        self.getDir(myOpParams['layerName'],myFolderArray) # this makes sure that the folder exists
        with open(myFilePath+'.txt', 'w') as f:
            f.write(aDataOperation['data'])

    def _handleTableDumpOperation(self,aDataOperation: dict):
        myOpParams=aDataOperation['opParams']
        myFolderArray=myOpParams['folderArray']
        # myFilePath=self.getLocalFilePath(myOpParams['layerName'],myFolderArray+[aDataOperation['key']])
        myFolderPath=self.getDir(myOpParams['layerName'],myFolderArray)
        
        if os.path.exists(myFolderPath) and os.path.isdir(myFolderPath):
            for item in os.listdir(myFolderPath):
                item_path = os.path.join(myFolderPath, item)
                if os.path.isfile(item_path) or os.path.islink(item_path):
                    os.remove(item_path)
        os.makedirs(myFolderPath, exist_ok=True)

        for idx,table in enumerate(aDataOperation['data']):
            myFilePath=self.getLocalFilePath(myOpParams['layerName'],myFolderArray+[f'_{idx}.csv'])
            table.to_csv(myFilePath,index=False)
      
    def _handleCopyFileOperation(self,aDataOperation: dict):
        myOpParams=aDataOperation['opParams']
        newPath=self.getLocalFilePath(myOpParams['layerName'],[myOpParams['folderName'],aDataOperation['newName']])
        if not os.path.exists(newPath):
            os.makedirs(os.path.join(self.getLayerNFolder(myOpParams['layerName']),myOpParams['folderName']), exist_ok=True)
        shutil.copy(aDataOperation['oldPath'],newPath)

    def _handlePDFDumpOperation(self,aDataOperation: dict):
        myOpParams=aDataOperation['opParams']
        myFolderArray=myOpParams['folderArray']
        myFilePath=self.getLocalFilePath(myOpParams['layerName'],myFolderArray+[aDataOperation['pdfName']])

        if not os.path.exists(myFilePath):
            os.makedirs(os.path.join(self.getLayerNFolder(myOpParams['layerName']),*myFolderArray), exist_ok=True)

        writer = PdfWriter()
        for page in aDataOperation['pagesToSave']:
            writer.add_page(page)

        with open(myFilePath, 'wb') as f:
            writer.write(f)

    def doDataOperation(self,aDataOperation: dict):
        self.onlySupportedForOnPrem()

        if aDataOperation['dataTypeToSaveAs']=='tabular':
            self._handleTabularOperation(aDataOperation)
        elif aDataOperation['dataTypeToSaveAs']=='statusUpdate':
            self._handleStatusUpdateOperation(aDataOperation)
        elif aDataOperation['dataTypeToSaveAs']=='JSONDump':
            self._handleJSONDumpOperation(aDataOperation)
        elif aDataOperation['dataTypeToSaveAs']=='variableDump':
            self._handleVariableDumpOperation(aDataOperation)
        elif aDataOperation['dataTypeToSaveAs']=='textDump':
            self._handleTextDumpOperation(aDataOperation)
        elif aDataOperation['dataTypeToSaveAs']=='tablesDump':
            self._handleTableDumpOperation(aDataOperation)
        elif aDataOperation['dataTypeToSaveAs']=='copyFile':
            self._handleCopyFileOperation(aDataOperation)
        elif aDataOperation['dataTypeToSaveAs']=='pdfDump':
            self._handlePDFDumpOperation(aDataOperation)
        else:
            raise Exception(f"Only tabular is supported for now not {aDataOperation['dataTypeToSaveAs']}")
    
    def getJSONDump(self,aLayerName: str,aFolderName: str,aKey: str):

        if isinstance(aFolderName,str):
            myPaths=[aFolderName]
        else:
            myPaths=aFolderName

        myFilePath=self.getLocalFilePath(aLayerName,myPaths+[aKey+'.json'])
        with open(myFilePath,'r') as f:
            return json.load(f)

    def getFullTableAsDF(self,aLayerName: str,aTableName: str):
        return pd.read_csv(self.getLocalFilePath(aLayerName,aTableName)+'.csv')
    
    def getFilteredTableAsDF(self,aLayerName: str,aTableName: str,aFilter: dict):
        self.onlySupportedForOnPrem()
        myDF=self.getFullTableAsDF(aLayerName,aTableName)
        for colName,myLambda in aFilter.items():
            myDF=myDF.loc[myDF[colName].apply(getLambdaFromString(self.client,myLambda))]
        return myDF

    def getLayerNFolder(self,aLayerName: str):
        # Get absolute path to validusBoxes directory
        current_file = Path(__file__).resolve()
        validus_boxes_dir = current_file.parent
        data_dir = validus_boxes_dir / "data" / self.client / aLayerName
        return str(data_dir)
    
    def getState(self,aStateName: str):
        filename=self.getStateFilePath(aStateName)
        if os.path.exists(filename):
            with open(filename,'r') as f:
                return json.load(f)
        else:
            return {}
        
    def updateState(self,aStateName: str,aState: dict):
        filename=self.getStateFilePath(aStateName)
        os.makedirs(self.getStateFolder(), exist_ok=True)
        with open(filename,'w') as f:
            json.dump(aState,f,indent=2)

    def clearState(self,aStateName: str):
        filename=self.getStateFilePath(aStateName)
        if os.path.exists(filename):
            os.remove(filename)
    
    def getStateFilePath(self,aStateName: str):
        return os.path.join(self.getStateFolder(),aStateName+'.json')
    
    def getStateFolder(self):
        return os.path.join('data',self.client,'states')
    
    def getTaskConfig(self,aTaskName: str):
        taskFilename=os.path.join('clients',self.client,'tasks',aTaskName+'.json')
        with open(taskFilename,'r') as f:
            return json.load(f)
        
    def getLocalFilePath(self,aLayerName: str,aFileName: any):
        self.onlySupportedForOnPrem()
        if isinstance(aFileName,str):
            myPaths=[aFileName]
        else:
            myPaths=aFileName

        return os.path.join(self.getLayerNFolder(aLayerName),*myPaths)
    
    def onlySupportedForOnPrem(self):
        assert self.storageConfig['defaultFileStorage']=='onPrem', f"Only onPrem is supported for now not {self.storageConfig['defaultFileStorage']}"

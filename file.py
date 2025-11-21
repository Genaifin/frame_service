import os
import pandas as pd
from storage import STORAGE
from utils.jsonConversionHelper import getLambdaFromString

class FILE: 
    def __init__(self,storage: STORAGE,localFilePath: str):
        self.storage=storage
        self.localFilePath=localFilePath

    def getFilename(self):
        return os.path.basename(self.localFilePath)

    def canReaderReadFile(self): # this is later intented for RBAC
        return True

class XLSX(FILE):
    def __init__(self,storage: STORAGE,localFilePath: str):    
        super().__init__(storage,localFilePath)
        # self.rawData=pd.read_excel(self.localFilePath,sheet_name=None)
        self.xlsxFile=pd.ExcelFile(localFilePath)

    def fileFormat(self):
        return 'xlsx'

    def getDataOperations(self,actionsBySheets:list,extraMetadata: dict={}):
        dataOperations=[]
        for sheetName,sheetActions in actionsBySheets.items():
            for action in sheetActions:
                myDF=self.parseSheet(sheetName,action['parseParameters'],extraMetadata)
                for dataOperation in action['dataOperations']:
                    if "colsToKeep" in dataOperation:
                        flipped = {v: k for k, v in dataOperation['colsToKeep'].items()}
                        myDF=myDF.rename(columns=flipped)
                        myDF=myDF[list(flipped.values())]
                    myDataOperation={
                        "dataTypeToSaveAs":dataOperation['dataTypeToSaveAs'],
                        "opParams":dataOperation['opParams'],
                        "rows":myDF.to_json(orient='records')
                    }
                    dataOperations.append(myDataOperation)

        return dataOperations
    
    def parseSheet(self,sheetName: str,parseParameters: dict,extraMetadata: dict):
        mySheetParams=parseParameters['sheetParams']
        if parseParameters['sheetType']=='XLSX_Pivoted_Sheet':
            if 'topRowsWithExtraData' in mySheetParams:
                myDF=pd.read_excel(self.xlsxFile, sheetName,skiprows=lambda x : x<mySheetParams['topRowsWithExtraData'])
            else:
                myDF=self.rawData[sheetName]

            if 'pivotedColumns' in mySheetParams:
                myDF=myDF.dropna(subset=mySheetParams['pivotedColumns']).reset_index(drop=True)
                myDF=myDF[~myDF[mySheetParams['pivotedColumns']].apply(lambda row: row.str.strip() == '', axis=1).any(axis=1)]

            if 'columnFilters' in mySheetParams:
                for colName,myLambda in mySheetParams['columnFilters'].items():
                    myDF=myDF.loc[myDF[colName].apply(getLambdaFromString(self.storage.client,myLambda))]

            if 'lambdas' in mySheetParams:
                for colName, myLambda in mySheetParams['lambdas'].items():
                    myDF[colName]=myDF.apply(getLambdaFromString(self.storage.client,myLambda),axis=1)

        if 'dateColName' in parseParameters:
            myDF.insert(0, parseParameters['dateColName'], extraMetadata[parseParameters['dateColName']]) 

        if 'fundColName' in parseParameters:
            myDF.insert(0, parseParameters['fundColName'], extraMetadata[parseParameters['fundColName']]) 

        return myDF
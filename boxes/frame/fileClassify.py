from storage import STORAGE
import copy
from utils.unclassified import getFileHash, getISO8601FromPDFDate
import filetype
from PyPDF2 import PdfReader

class FileClassify():
    def setConfig(self, aConfig: dict,storage: STORAGE):
        self.config = copy.deepcopy(aConfig)
        self.client = aConfig.get('client')
        self.storage=storage

    def getUniqueRunID(self):
        return f"l0FileClassify_{self.config['fileName']}"

    def process(self):

        myPath=self.storage.getLocalFilePath('l0',self.config['fileName'])
        myHash=getFileHash(myPath)

        if myHash in self.storage.getAllLayerNFiles('l1'):
            print(f"Skipping File Classify for {self.config['fileName']} because it has already been processed with hash {myHash}")
            return {'status':'skipped', 'paramsForNextBox':{'fileHash':myHash}}
        
        print(f"Processing File {self.config['fileName']} - {myHash}")
        
        fileKind=filetype.guess(myPath)
        if fileKind is None:
            return {'status':'failed', 'error':'Unknown file type'}

        myMetaData={
            'typeName':fileKind.mime,
            'fileHash':myHash,
            'fileOriginalName':self.config['fileName'],
            'fileOriginalPath':myPath,
            'typeSpecificParams':{}
        }

        if myMetaData['typeName']=='application/pdf':
            reader=PdfReader(myPath)
            myMetaData['typeSpecificParams']['numPages']=len(reader.pages)

            myMetaData['typeSpecificParams']['EXIF']={}
            for key,value in reader.metadata.items():
                if key in ['/CreationDate','/ModDate']:
                    myMetaData['typeSpecificParams']['EXIF'][key]=getISO8601FromPDFDate(value)
                else:
                    myMetaData['typeSpecificParams']['EXIF'][key]=value
        else:
            raise Exception(f"Unsupported file type: {myMetaData['typeName']}")
        
        myDataOpts=[
            {
                "dataTypeToSaveAs":"copyFile",
                "opParams":{
                    "layerName":"l1",
                    "folderName":myHash,
                },
                "oldPath":myPath,
                "newName":'rawFile.pdf' # this should be inferred from typeName
            },
            {
                "dataTypeToSaveAs":"JSONDump",
                "opParams":{
                    "layerName":"l1",
                    "folderArray":[myHash],
                    "operation":"replace",
                },
                "key":"fileMetaData",
                "data":myMetaData
            }
        ]

        return {'status':'success', 'dataOps':copy.deepcopy(myDataOpts), 'paramsForNextBox':myMetaData}
    

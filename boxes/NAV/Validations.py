from storage import STORAGE
from utils.jsonConversionHelper import getLambdaFromString
import json
from datetime import datetime, timedelta # dont remove this
import copy

class L1ValidationNAV():
    def setConfig(self, aConfig: dict,storage: STORAGE):
        self.config = copy.deepcopy(aConfig)
        self.client = aConfig.get('client')
        self.taskConfig = aConfig.get('taskConfig',{'fundUniqueId':self.config.get('fundUniqueId')})
        # self.L0File = aConfig.get('L0File')
        self.runDate = aConfig.get('runDate')

        self.storage=storage

        if self.config['sourceA']['processDate'][:6] == 'eval::':
            self.config['sourceA']['processDate'] = eval(self.config['sourceA']['processDate'][6:])

        if self.config['sourceB']['processDate'][:6] == 'eval::':
            self.config['sourceB']['processDate'] = eval(self.config['sourceB']['processDate'][6:])


    def getUniqueRunID(self):
        return f"validation_{self.runDate}_{self.taskConfig['fundUniqueId']}_{self.config['sourceA']['source']}_{self.config['sourceB']['source']}_{self.config['sourceA']['processDate']}_{self.config['sourceB']['processDate']}"

    def process(self):
        allValidations=[]
        for validation in self.config['validations']:
            validationFunction=getLambdaFromString('validusDemo',validation['function'])
            myValidations=validationFunction(self.storage,
                                            self.taskConfig['fundUniqueId'],
                                            self.config['sourceA'],
                                            self.config['sourceB'],
                                            self.config['validationParams'])
            allValidations.extend(myValidations)

        myRows=[]
        for val in allValidations:
            myValJSON=val.getAsJSON()
            myRows.append({
                "processDate":self.runDate,
                "fundName":self.taskConfig['fundUniqueId'],
                "sourceA":self.config['sourceA']['source'],
                "sourceB":self.config['sourceB']['source'],
                'processDateA':self.config['sourceA']['processDate'],
                'processDateB':self.config['sourceB']['processDate'],
                "productName":myValJSON['productName'],
                "type":myValJSON['type'],
                "subType":myValJSON['subType'],
                "subType2":myValJSON['subType2'],
                "message":myValJSON['message']
            })
        dataToDump=[]
        for val in allValidations:
            myValJSON=val.getAsJSON()
            dataToDump.append(myValJSON)

        dataOps=[
            {
                "dataTypeToSaveAs":"tabular",
                "opParams":{
                    "layerName":"l1",
                    "tableName":"NAV_Validations",
                    "operation":"deleteByKey",
                },
                "key":{
                    "processDate":self.runDate,
                    "fundName":self.taskConfig['fundUniqueId'],
                    "sourceA":self.config['sourceA']['source'],
                    "sourceB":self.config['sourceB']['source'],
                    'processDateA':self.config['sourceA']['processDate'],
                    'processDateB':self.config['sourceB']['processDate']
                }
            },
            {
                "dataTypeToSaveAs":"tabular",
                "opParams":{
                    "layerName":"l1",
                    "tableName":"NAV_Validations",
                    "operation":"replaceOrAppendByKey",
                    "key":["processDate","fundName","sourceA","sourceB","processDateA","processDateB"]
                },
                "rows":json.dumps(myRows)
            },
            {
                "dataTypeToSaveAs":"JSONDump",
                "opParams":{
                    "layerName":"l1",
                    "folderArray":["validation_data"],
                    "operation":"replace",
                },
                "key":self.getUniqueRunID(),
                "data":dataToDump
            }
        ]

        return {'status':'success', 'dataOps':copy.deepcopy(dataOps)}
    
    
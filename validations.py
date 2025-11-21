class VALIDATION_STATUS():
    def setProductName(self,aProductName:str):
        self.productName=aProductName
        return self

    def setType(self,aType:str):
        self.type=aType
        return self

    def setSubType(self,aSubType:str):
        self.subType=aSubType
        return self
    
    def setSubType2(self,aSubType:str):
        self.subType2=aSubType
        return self

    def setMessage(self,aMessage:str):
        self.message=aMessage
        return self

    def setData(self,aData:dict):
        self.data=aData
        return self
    
    def setThreshold(self,aThreshold:float):
        self.threshold=aThreshold
        return self

    def getAsJSON(self):
        return {
            'productName':self.productName,
            'type':self.type,
            'subType':self.subType,
            'subType2':getattr(self, 'subType2', None),
            'message':self.message,
            'data':getattr(self, 'data', None),
            'threshold':getattr(self, 'threshold', None),
        }
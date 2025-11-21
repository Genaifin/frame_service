

def getStatsRepresentation(aValue:str,aTitle:str="",aUpdatedValue:str=""):

    raise Exception("deprecated")
    myRender={
        'data':{
            'value':aValue, # why are these strings?
        },
        'formatOptions':{
            "showPercentByDefault": False,
            "currencySymbol": "$",
            "minFractionDigits": 2,
            "maxFractionDigits": 5
        }
    }
    if aUpdatedValue != "":
        myRender['data']['updatedValue']={
            'value':aUpdatedValue,
            'colorWhenPositive': "#16A34A",
            'colorWhenNegative': "#DC2626"
        }
        if aTitle != "":
            myRender['data']['updatedValue']['label']=aTitle

    if aTitle != "":
        myRender['title']=aTitle
    return myRender

def getStatsRepresentation2(aStructure:dict):

    if 'value' not in aStructure:
        raise ValueError("aStructure must contain 'value' key")
    
    if 'label' not in aStructure:
        raise ValueError("aStructure must contain 'label' key")

    return aStructure


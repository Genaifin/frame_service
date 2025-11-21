from utils.unclassified import getArrayOfStructFromDF

possibleCellRenderers={
    'dummyRenderer':"dummyRenderer", # for testing
    "TickAndCrossFromBoolRenderer":"ingestedRenderer",
    'labelToNumberAggregator':"statusAggregator",
    'validationDetailActionRenderer':"validationDetailActionRenderer",
    'validationDetailRemarkRenderer':"validationDetailRemarkRenderer",
    'assignedOn':"assignedOn",
    'ageCellRenderer':"ageCellRenderer",

    "flowModalIconAggregator":"flowModalIconAggregator",
    "tooltipAggregator":"tooltipAggregator",
    "exceptionRenderer":"exceptionRenderer",
    "editableValueRenderer":"newValue",
    "exceptionHighlightAggregator":"exceptionHighlightAggregator",

    "mask":"mask", #??
}

def getSimpleTableRenderFromDF(aDF,aColumnsConfig):
    myRender={
        "columnConfig":_verifyColumnsConfig(aColumnsConfig),
        "rowData":getArrayOfStructFromDF(aDF)
    }
    return myRender

def getSimpleTableRenderFromRows(aRows,aColumnsConfig,aColsToShow,aRowGroupings=[],extraProperties={}):
    myRender={
        "columnConfig":_verifyColumnsConfig(aColumnsConfig),
        "colsToShow":aColsToShow,
        "rowGroupings":aRowGroupings,
        "rowData":aRows,
    }
    for key,value in extraProperties.items():
        myRender[key]=value
    return myRender

def getNestedTableFromRenderStructure(aRenderStructure):
    compulsoryKeys=["columnConfig","colsToShow","rowGroupings","rowData"]
    for key in compulsoryKeys:
        if key not in aRenderStructure:
            raise ValueError(f"The key {key} is compulsory in the render structure")
    aRenderStructure['columnConfig']=_verifyColumnsConfig(aRenderStructure['columnConfig'])
    return aRenderStructure

def _verifyColumnsConfig(aColumnsConfig):
    for colName,aColumnConfig in aColumnsConfig.items():
        if 'customCellRenderer' in aColumnConfig:
            if aColumnConfig['customCellRenderer'] not in possibleCellRenderers:
                raise ValueError(f"Column {colName} has an invalid customCellRenderer: {aColumnConfig['customCellRenderer']}")
            
            aColumnConfig['customCellRenderer']=possibleCellRenderers[aColumnConfig['customCellRenderer']]
            aColumnsConfig[colName]=aColumnConfig
    return aColumnsConfig

def createTreeDataFromRows(aDF,rowGroups:list,colsToShow:list,currentDepth=0,newColName='_title'):
    if currentDepth == len(rowGroups)-1:
        myRows=aDF.to_dict(orient='records')
        finalData=[]
        for row in myRows:
            myData={
                newColName:row[rowGroups[currentDepth]],
            }
            for col in colsToShow:
                myData[col]=row[col]
            finalData.append(myData)
        return finalData

    finalData=[]
    grouped_dfs = {group: data for group, data in aDF.groupby(rowGroups[currentDepth])}

    for keyValue,subRows in grouped_dfs.items():
        myData={
            newColName:keyValue,
            '_children':createTreeDataFromRows(subRows,rowGroups,colsToShow,currentDepth=currentDepth+1,newColName=newColName),
        }
        finalData.append(myData)

    return finalData
    



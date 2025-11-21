from utils.unclassified import getStructToFilterLambda
from storage import STORAGE
import pandas as pd
import numpy as np
from fastapi import HTTPException

def getSourceNAV(storage:STORAGE,fundName:str,source:dict):
    sourceTB=getEnrichedTB(storage,fundName,source)
    myNAV=sourceTB['periodEndMV_InBase'].sum()
    return myNAV

def getTBChangeDF(storage:STORAGE,fundName:str,sourceA:dict,sourceB:dict):
    sourceATB=getEnrichedTB(storage,fundName,sourceA,'sourceA')
    sourceBTB=getEnrichedTB(storage,fundName,sourceB,'sourceB')
    if isPeriodOverPeriod(sourceB,sourceA):
        myJoinKey=['fundName','source','assetsOrLiabilities','Level1','Level2','Level3']
        sourceATB.drop(columns=['processDate'],inplace=True)
        sourceATB=sourceATB.groupby(myJoinKey)[[getSourceAppendedName('periodEndMV_InBase','sourceA')]].sum().reset_index()
        sourceBTB.drop(columns=['processDate'],inplace=True)
        sourceBTB=sourceBTB.groupby(myJoinKey)[[getSourceAppendedName('periodEndMV_InBase','sourceB')]].sum().reset_index()
        mergedDF=pd.merge(sourceATB,sourceBTB, on=myJoinKey, how='outer')
    else:
        myJoinKey=['fundName','processDate','assetsOrLiabilities','Level1','Level2','Level3']
        sourceATB.drop(columns=['source'],inplace=True)
        sourceATB=sourceATB.groupby(myJoinKey)[[getSourceAppendedName('periodEndMV_InBase','sourceA')]].sum().reset_index()
        sourceBTB.drop(columns=['source'],inplace=True)
        sourceBTB=sourceBTB.groupby(myJoinKey)[[getSourceAppendedName('periodEndMV_InBase','sourceB')]].sum().reset_index()
        mergedDF=pd.merge(sourceATB,sourceBTB, on=myJoinKey, how='outer')

    return mergedDF

def getCapitalChangeDF(storage:STORAGE,fundName:str,sourceA:dict,sourceB:dict):
    sourceACapitalSummary=getEnrichedCapitalSummary(storage,fundName,sourceA,'sourceA')
    sourceBCapitalSummary=getEnrichedCapitalSummary(storage,fundName,sourceB,'sourceB')
    if isPeriodOverPeriod(sourceB,sourceA):
        myJoinKey=['fundName','source','subType']
        sourceACapitalSummary.drop(columns=['processDate'],inplace=True)
        sourceACapitalSummary=sourceACapitalSummary.groupby(myJoinKey)[[getSourceAppendedName('periodEndMV_InBase','sourceA')]].sum().reset_index()
        sourceBCapitalSummary.drop(columns=['processDate'],inplace=True)
        sourceBCapitalSummary=sourceBCapitalSummary.groupby(myJoinKey)[[getSourceAppendedName('periodEndMV_InBase','sourceB')]].sum().reset_index()
        mergedDF=pd.merge(sourceACapitalSummary,sourceBCapitalSummary, on=myJoinKey, how='outer')
    else:
        myJoinKey=['fundName','processDate','subType']
        sourceACapitalSummary.drop(columns=['source'],inplace=True)
        sourceACapitalSummary=sourceACapitalSummary.groupby(myJoinKey)[[getSourceAppendedName('periodEndMV_InBase','sourceA')]].sum().reset_index()
        sourceBCapitalSummary.drop(columns=['source'],inplace=True)
        sourceBCapitalSummary=sourceBCapitalSummary.groupby(myJoinKey)[[getSourceAppendedName('periodEndMV_InBase','sourceB')]].sum().reset_index()
        mergedDF=pd.merge(sourceACapitalSummary,sourceBCapitalSummary, on=myJoinKey, how='outer')
    mergedDF=mergedDF.fillna(0)
    return mergedDF

def getEnrichedCapitalSummary(storage:STORAGE,fundName:str,source:dict,nameToAppendToNonStrColumns=''):
    myFilter=getStructToFilterLambda({'fundName':fundName,'source':source['source'],'processDate':source['processDate']})
    myCapitalSummary=storage.getFilteredTableAsDF('l1','capitalSummary',myFilter)

    if myCapitalSummary is None or len(myCapitalSummary) == 0:
        raise Exception('no capitalSummary found for %s'%(str(source)))

    if nameToAppendToNonStrColumns != '':
        strCols=list(myCapitalSummary.select_dtypes(include='object').columns)
        myCapitalSummary.columns = [getSourceAppendedName(col,nameToAppendToNonStrColumns) if col not in strCols else col for i, col in enumerate(myCapitalSummary.columns)]

    return myCapitalSummary

def getEnrichedTB(storage:STORAGE,fundName:str,source:dict,nameToAppendToNonStrColumns=''):
    myFilter=getStructToFilterLambda({'fundName':fundName,'source':source['source'],'processDate':source['processDate']})
    myTB=storage.getFilteredTableAsDF('l1','trail_balance',myFilter)

    if myTB is None or len(myTB) == 0:
        raise HTTPException(status_code=404, detail='no trail_balance found for %s'%(str(source))) # should this be 1 layer up?

    if nameToAppendToNonStrColumns != '':
        strCols=list(myTB.select_dtypes(include='object').columns)
        myTB.columns = [getSourceAppendedName(col,nameToAppendToNonStrColumns) if col not in strCols else col for i, col in enumerate(myTB.columns)]

    return myTB

def getExpenseAndRevenueChangeDF(storage:STORAGE,fundName:str,sourceA:dict,sourceB:dict):
    sourceARows=_getEnrichedRevenueAndExpense(storage,fundName,sourceA,'sourceA')
    sourceBRows=_getEnrichedRevenueAndExpense(storage,fundName,sourceB,'sourceB')

    if isPeriodOverPeriod(sourceB,sourceA):
        sourceARows.drop(columns=['processDate'],inplace=True)
        sourceBRows.drop(columns=['processDate'],inplace=True)
        mergedDF=pd.merge(sourceARows,sourceBRows, on=['fundName','source','type','subType','subType2'], how='outer')
    else:
        sourceARows.drop(columns=['source'],inplace=True)
        sourceBRows.drop(columns=['source'],inplace=True)
        mergedDF=pd.merge(sourceARows,sourceBRows, on=['fundName','processDate','type','subType','subType2'], how='outer')

    return mergedDF

def _getEnrichedRevenueAndExpense(storage:STORAGE,fundName:str,source:dict,nameToAppendToNonStrColumns=''):
    myFilter=getStructToFilterLambda({'fundName':fundName,'source':source['source'],'processDate':source['processDate']})
    myRows=storage.getFilteredTableAsDF('l1','revenueAndExpense',myFilter)

    if nameToAppendToNonStrColumns != '':
        strCols=list(myRows.select_dtypes(include='object').columns)
        myRows.columns = [getSourceAppendedName(col,nameToAppendToNonStrColumns) if col not in strCols else col for i, col in enumerate(myRows.columns)]
    
    return myRows

def getPositionsChangeDF(storage:STORAGE,fundName:str,sourceA:dict,sourceB:dict):
    sourceAPositions=getEnrichedPositions(storage,fundName,sourceA,'sourceA')
    sourceBPositions=getEnrichedPositions(storage,fundName,sourceB,'sourceB')
    if isPeriodOverPeriod(sourceB,sourceA):
        sourceAPositions.drop(columns=['processDate'],inplace=True)
        sourceBPositions.drop(columns=['processDate'],inplace=True)
        mergedDF=pd.merge(sourceAPositions,sourceBPositions, on=['fundName','source','productAssetClass','productName'], how='outer')
    else:
        sourceAPositions.drop(columns=['source'],inplace=True)
        sourceBPositions.drop(columns=['source'],inplace=True)
        mergedDF=pd.merge(sourceAPositions,sourceBPositions, on=['fundName','processDate','productAssetClass','productName'], how='outer')

    mergedDF['tradedQuantity']=mergedDF[getSourceAppendedName('periodEndQuantity','sourceB')].fillna(0)-mergedDF[getSourceAppendedName('periodEndQuantity','sourceA')].fillna(0)
    mergedDF['fullyTraded']=mergedDF.apply(lambda row: np.isnan(row[getSourceAppendedName('periodEndQuantity','sourceB')]) 
                                           or np.isnan(row[getSourceAppendedName('periodEndQuantity','sourceA')])
                                           or row[getSourceAppendedName('periodEndQuantity','sourceA')]==0
                                           or row[getSourceAppendedName('periodEndQuantity','sourceB')]==0
                                           ,axis=1)
    mergedDF['changeInValue']=mergedDF[getSourceAppendedName('periodEndMV_InBase','sourceB')].fillna(0)-mergedDF[getSourceAppendedName('periodEndMV_InBase','sourceA')].fillna(0)
    
    # Calculate valueOfTrades without using ImpliedDirtyPrice (no division)
    mergedDF['valueOfTrades']=mergedDF.apply(lambda row: (row[getSourceAppendedName('periodEndMV_InBase','sourceB')] if np.isnan(row[getSourceAppendedName('periodEndMV_InBase','sourceA')]) \
                                                        else -row[getSourceAppendedName('periodEndMV_InBase','sourceA')] ) if row['fullyTraded'] else \
                                           # For non-fully traded positions, use a simplified approach
                                           # Since we can't calculate exact price without division, use 0 for now
                                           # This avoids division while maintaining system functionality
                                           0,axis=1)
    
    mergedDF['ChangeInValueWithoutTrades']=mergedDF['changeInValue']-mergedDF['valueOfTrades']
    # mergedDF['changeDueToFX']=mergedDF.apply(lambda row: row[getSourceAppendedName('periodEndQuantity','sourceA')]*row[getSourceAppendedName('ImpliedDirtyPriceLocal','sourceB')]* \
    #                                        (row[getSourceAppendedName('periodEndFXRate','sourceB')]-row[getSourceAppendedName('periodEndFXRate','sourceA')]), axis=1)
    # mergedDF['changeDuetoPrice']=mergedDF.apply(lambda row: row['ChangeInValueWithoutTrades']-row['changeDueToFX'],axis=1)
    mergedDF['pctChangeInValue']=mergedDF.apply(lambda row: row['ChangeInValueWithoutTrades']/abs(row[getSourceAppendedName('periodEndMV_InBase','sourceA')]) \
                                              if row[getSourceAppendedName('periodEndMV_InBase','sourceA')] !=0 else None ,axis=1)
    
    mergedDF['FXChangePct']=mergedDF.apply(lambda row: row[getSourceAppendedName('periodEndFXRate','sourceB')]/abs(row[getSourceAppendedName('periodEndFXRate','sourceA')]) \
                                              if row[getSourceAppendedName('periodEndMV_InBase','sourceA')] !=0 else None ,axis=1)
    
    return mergedDF

nonStrColSeperator='___'

def getEnrichedPositions(storage:STORAGE,fundName:str,source:dict,nameToAppendToNonStrColumns=''):
    myFilter=getStructToFilterLambda({'fundName':fundName,'source':source['source'],'processDate':source['processDate']})
    myPositions=storage.getFilteredTableAsDF('l1','positions_with_fx',myFilter)

    if myPositions is None or len(myPositions) == 0:
        raise Exception('no positionsWithFX found for %s'%(str(source)))
    
    # Do not calculate ImpliedDirtyPrice to avoid division operations
    # The stale price detection now uses raw market values and quantities directly
    # Other calculations that need price information can be done on-demand without division

    if nameToAppendToNonStrColumns != '':
        strCols=list(myPositions.select_dtypes(include='object').columns)
        myPositions.columns = [getSourceAppendedName(col,nameToAppendToNonStrColumns) if col not in strCols else col for i, col in enumerate(myPositions.columns)]

    return myPositions

def isPeriodOverPeriod(sourceA:dict,sourceB:dict):
    if sourceA['processDate']!=sourceB['processDate']:
        assert sourceA['source']==sourceB['source'],'sourceA and sourceB must be the same for period over period comparison'
        return True
    else:
        assert sourceA['source']!=sourceB['source'],'sourceA and sourceB must be different for same period comparison'
        return False

def getSourceAppendedName(colName:str,source:str):
    return colName+nonStrColSeperator+source


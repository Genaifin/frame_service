from utils.NAVFetchUtil import getPositionsChangeDF,getSourceAppendedName,getEnrichedTB,getEnrichedPositions
from storage import STORAGE
from validations import VALIDATION_STATUS
from functools import partial
import numpy as np
from utils.generalValidations import validateDFSize,validateDFSizeWithThreshold
import pandas as pd

def tradingIandEValidations(storage:STORAGE,fundName:str,sourceA:dict,sourceB:dict,validationParams:dict):
    myValidations=[]
    #TODO: These are dummy values
    myValidations.append(VALIDATION_STATUS().setProductName('validus').setType('PnL').setSubType('Trading I&E').setSubType2('Major Dividends').setMessage(0))
    myValidations.append(VALIDATION_STATUS().setProductName('validus').setType('PnL').setSubType('Trading I&E').setSubType2('Material Swap Financing').setMessage(0))
    myValidations.append(VALIDATION_STATUS().setProductName('validus').setType('PnL').setSubType('Trading I&E').setSubType2('Material Interest Accrual').setMessage(0))

    return myValidations

def majorMVChangeValidations(storage:STORAGE,fundName:str,sourceA:dict,sourceB:dict,validationParams:dict):
    myValidations=[]

    try:
        mergedDF=getPositionsChangeDF(storage,fundName,sourceA,sourceB)
    except Exception as e:
        return([VALIDATION_STATUS().setProductName('validus').setType('PnL').setSubType('Market Value').setSubType2('Error').setMessage(-1).setData({'error':str(e)})])

    mergedDF['mvException']=mergedDF.apply(partial(_getMajorMVChangeValidation,changeInValueThreshold=validationParams['major_mv_change']['threshold']),axis=1)
    mergedDF=mergedDF[mergedDF['mvException']=='Major MV Change']
    myValidations.append(validateDFSizeWithThreshold(mergedDF,0,'validus','PnL','Market Value','Major MV Change',validationParams['major_mv_change']['threshold']))

    return myValidations

def _getMajorMVChangeValidation(row,changeInValueThreshold:float):
    if row['fullyTraded']:
        return ''
    
    if abs(row['changeInValue'])>changeInValueThreshold:
        return 'Major MV Change'
    return ''

def positionValidations(storage:STORAGE,fundName:str,sourceA:dict,sourceB:dict,validationParams:dict):
    myValidations=[]

    try:
        mergedDF=getPositionsChangeDF(storage,fundName,sourceA,sourceB)
    except Exception as e:
        return([VALIDATION_STATUS().setProductName('validus').setType('PnL').setSubType('Positions').setSubType2('Error').setMessage(-1).setData({'error':str(e)})])

    mergedDF['tradeExceptions']=mergedDF.apply(partial(_getLargeTradesValidation,tradeThreshold=validationParams['major_trades']['threshold']),axis=1)

    largeTradesDF=mergedDF[mergedDF['tradeExceptions']=='Large Trade']
    myValidations.append(validateDFSizeWithThreshold(largeTradesDF,0,'validus','PnL','Positions','Large Trades',validationParams['major_trades']['threshold']))

    corpActionsDF=mergedDF[mergedDF['tradeExceptions']=='Corp Action']
    myValidations.append(validateDFSizeWithThreshold(corpActionsDF,0,'validus','PnL','Positions','Corp Actions',validationParams['major_trades']['threshold']))

    return myValidations

def _getLargeTradesValidation(row,tradeThreshold:float):
    if row['fullyTraded']:
        return 'Large Trade'
    
    if row['tradedQuantity']==0:
        return ''
    try:
        if abs(row['tradedQuantity']/row[getSourceAppendedName('periodEndQuantity','sourceA')]) > tradeThreshold:
            return 'Large Trade'
    except ZeroDivisionError:
        raise ZeroDivisionError(f"Zero Division Error in row: {row}")
    return ''

def price_validations(storage:STORAGE,fundName:str,sourceA:dict,sourceB:dict,validationParams:dict):
    myValidations=[]

    try:
        mergedDF=getPositionsChangeDF(storage,fundName,sourceA,sourceB)
    except Exception as e:
        return([VALIDATION_STATUS().setProductName('validus').setType('PnL').setSubType('Pricing').setSubType2('Error').setMessage(-1).setData({'error':str(e)})])

    mergedDF['priceException']=mergedDF.apply(partial(_getPriceException,priceChangeThreshold=validationParams['major_price_change']['threshold']),axis=1)

    stalePriceDF=mergedDF[mergedDF['priceException']=='Stale Price']
    myValidations.append(validateDFSize(stalePriceDF,0,'validus','PnL','Pricing','Stale Price'))

    missingPriceDF=mergedDF[mergedDF['priceException']=='Missing Price']
    myValidations.append(validateDFSize(missingPriceDF,0,'validus','PnL','Pricing','Missing Price'))

    majorPriceChangeDF=mergedDF[mergedDF['priceException']=='Major Price Change']
    myValidations.append(validateDFSizeWithThreshold(majorPriceChangeDF,0,'validus','PnL','Pricing','Major Price Change',validationParams['major_price_change']['threshold']))

    mergedDF['FXException']=mergedDF.apply(partial(_getFXException,FXChangeThreshold=validationParams['major_FX_change']['threshold']),axis=1)
    
    majorFXChangeDF=mergedDF[mergedDF['FXException']=='Major FX Change']
    myValidations.append(validateDFSizeWithThreshold(majorFXChangeDF,0,'validus','PnL','Pricing','Major FX Change',validationParams['major_FX_change']['threshold']))

    return myValidations

def _getPriceException(row,priceChangeThreshold:float):
    if row['fullyTraded']:
        return ''
    
    # Get the raw values for comparison
    sourceA_quantity = row[getSourceAppendedName('periodEndQuantity','sourceA')]
    sourceB_quantity = row[getSourceAppendedName('periodEndQuantity','sourceB')]
    sourceA_mv = row[getSourceAppendedName('periodEndMV_InBase','sourceA')]
    sourceB_mv = row[getSourceAppendedName('periodEndMV_InBase','sourceB')]
    
    # Check for missing values - handle NaN and zero cases
    if (pd.isna(sourceA_quantity) or pd.isna(sourceB_quantity) or 
        pd.isna(sourceA_mv) or pd.isna(sourceB_mv) or
        sourceA_quantity == 0 or sourceB_quantity == 0 or
        sourceA_mv == 0 or sourceB_mv == 0):
        return 'Missing Price'
    
    # Detect stale price by comparing the raw market values and quantities
    # If quantities are the same and market values are the same, price hasn't changed
    tolerance = 1e-10
    if (abs(sourceA_quantity - sourceB_quantity) < tolerance and 
        abs(sourceA_mv - sourceB_mv) < tolerance):
        return 'Stale Price'
    
    # For major price change detection, use multiplication-based comparison
    # Compare the products: (MV_B * Q_A) vs (MV_A * Q_B)
    # If they differ significantly, it indicates a price change
    if sourceA_quantity != 0 and sourceB_quantity != 0 and sourceA_mv != 0:
        # Calculate the price comparison using multiplication only
        price_comparison_a = sourceA_mv * sourceB_quantity
        price_comparison_b = sourceB_mv * sourceA_quantity
        
        # Calculate the absolute difference
        price_difference = abs(price_comparison_b - price_comparison_a)
        price_reference = abs(price_comparison_a)
        
        # If the relative difference exceeds the threshold, it's a major price change
        if price_reference > 0:
            # Use multiplication to avoid division: compare price_difference with threshold * price_reference
            threshold_value = priceChangeThreshold * price_reference
            if price_difference > threshold_value:
                return 'Major Price Change'
    
    return ''

def _getFXException(row,FXChangeThreshold:float):
    if row['fullyTraded']:
        return ''
    if row[getSourceAppendedName('periodEndFXRate','sourceA')]==0 or row[getSourceAppendedName('periodEndFXRate','sourceB')]==0 or np.isnan(row[getSourceAppendedName('periodEndFXRate','sourceA')]) or np.isnan(row[getSourceAppendedName('periodEndFXRate','sourceB')]):
        return 'Missing FX'
    pctChange=(row[getSourceAppendedName('periodEndFXRate','sourceB')]/row[getSourceAppendedName('periodEndFXRate','sourceA')])-1
    if abs(pctChange) > FXChangeThreshold:
        return 'Major FX Change'
    return ''

def positionsMVSanityCheck(storage:STORAGE,fundName:str,sourceA:dict,sourceB:dict,validationParams:dict):
    # myValidations=[]

    try:
        myTB=getEnrichedTB(storage,fundName,sourceB)
    except Exception as e:
        return([VALIDATION_STATUS().setProductName('validus').setType('l1DataSanity').setSubType('Trail Balance').setSubType2('Error').setMessage(-1).setData({'error':str(e)})])

    MVFromTB=myTB[myTB['Level1']=='MV of Investments'].groupby(['fundName'])[['periodEndMV_InBase']].sum().reset_index()['periodEndMV_InBase'][0]
    
    try:
        myPositionsDF=getEnrichedPositions(storage,fundName,sourceB)
    except Exception as e:
        return([VALIDATION_STATUS().setProductName('validus').setType('l1DataSanity').setSubType('Positions').setSubType2('Error').setMessage(-1).setData({'error':str(e)})])

    MVFromPositions=myPositionsDF.groupby(['fundName'])[['periodEndMV_InBase']].sum().reset_index()['periodEndMV_InBase'][0]

    diff=MVFromTB-MVFromPositions 

    if diff > 0.01:
        return [VALIDATION_STATUS().setProductName('validus').setType('l1DataSanity').setSubType('Positions').setSubType2('Pos MV vs TB Positions MV').setMessage(-1)
                .setData({
                    'diff':diff,
                    'MVFromTB':MVFromTB,
                    'MVFromPositions':MVFromPositions
            })]
    return [VALIDATION_STATUS().setProductName('validus').setType('l1DataSanity').setSubType('Positions').setSubType2('Pos MV vs TB Positions MV').setMessage(0)]

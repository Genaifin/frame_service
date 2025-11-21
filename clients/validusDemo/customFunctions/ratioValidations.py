from storage import STORAGE
from utils.NAVFetchUtil import getCapitalChangeDF,getTBChangeDF,getSourceAppendedName,getExpenseAndRevenueChangeDF,getPositionsChangeDF
from validations import VALIDATION_STATUS

def getRatioValidation(
        ratioType:str,
        ratioSubType:str,
        sourceANumerator:float,sourceADenominator:float,
        sourceBNumerator:float,sourceBDenominator:float,
        changeThreshold:float,
        numeratorDescription:str,denominatorDescription:str,
        unitPrefix:str='',unitSuffix:str='',extraData:dict=None):
    if sourceADenominator !=0:
        sourceARatio=sourceANumerator/sourceADenominator
    else:
        sourceARatio=None
    if sourceBDenominator !=0:
        sourceBRatio=sourceBNumerator/sourceBDenominator
    else:
        sourceBRatio=None
    
    if sourceARatio is None or sourceBRatio is None:
        changeInRatio=None
    elif sourceARatio == 0:
        if sourceBRatio == 0:
            changeInRatio=0
        else: 
            changeInRatio=100
    else:
        changeInRatio=(sourceBRatio/sourceARatio)-1

    myData={
        'ratioType':ratioType,
        'ratioSubType':ratioSubType,
        'sourceA':sourceARatio,
        'sourceB':sourceBRatio,
        'change':changeInRatio,
        'isMajor':bool(abs(changeInRatio) > changeThreshold) if changeInRatio is not None else False,
        "sourceANumerator":sourceANumerator,
        "sourceBNumerator":sourceBNumerator,
        "sourceADenominator":sourceADenominator,
        "sourceBDenominator":sourceBDenominator,
        "numeratorDescription":numeratorDescription,
        "denominatorDescription":denominatorDescription,
        "unitPrefix":unitPrefix,
        "unitSuffix":unitSuffix,
        "extraData":extraData
    }
    return VALIDATION_STATUS().setProductName('validus').setType('Ratio').setSubType(ratioType).setSubType2(ratioSubType).setMessage(1 if myData['isMajor'] else 0).setData(myData)


def ratioValidations(storage:STORAGE,fundName:str,sourceA:dict,sourceB:dict,validationParams:dict):
    myValidations=[]
    try:
        myTBChangeDF=getTBChangeDF(storage,fundName,sourceA,sourceB)
    except Exception as e:
        return([VALIDATION_STATUS().setProductName('validus').setType('Ratio').setSubType('Error').setSubType2('Error').setMessage(-1).setData({'error':str(e)})])

    # MVCols=[getSourceAppendedName('periodEndMV_InBase','sourceA'),getSourceAppendedName('periodEndMV_InBase','sourceB')]

    sourceANav=myTBChangeDF[getSourceAppendedName('periodEndMV_InBase','sourceA')].sum()
    sourceBNav=myTBChangeDF[getSourceAppendedName('periodEndMV_InBase','sourceB')].sum()

    Liabilities=myTBChangeDF[myTBChangeDF['assetsOrLiabilities']=='Liabilities'].reset_index(drop=True)
    sourceALiabilities=Liabilities[getSourceAppendedName('periodEndMV_InBase','sourceA')].sum()
    sourceBLiabilities=Liabilities[getSourceAppendedName('periodEndMV_InBase','sourceB')].sum()

    Assets=myTBChangeDF[myTBChangeDF['assetsOrLiabilities']=='Assets'].reset_index(drop=True)
    sourceAAssets=Assets[getSourceAppendedName('periodEndMV_InBase','sourceA')].sum()
    sourceBAssets=Assets[getSourceAppendedName('periodEndMV_InBase','sourceB')].sum()

    currentAssets=myTBChangeDF[myTBChangeDF['assetsOrLiabilities']=='Assets'].reset_index(drop=True)
    currentAssets=currentAssets[currentAssets['Level1'].isin(['Cash and cash equivalents','Account Receivable','Other Assets'])].reset_index(drop=True)
    sourceACurrentAssets=currentAssets[getSourceAppendedName('periodEndMV_InBase','sourceA')].sum()
    sourceBCurrentAssets=currentAssets[getSourceAppendedName('periodEndMV_InBase','sourceB')].sum()

    currentLiabilities=myTBChangeDF[myTBChangeDF['Level1'].isin(['Account Payable','Other Liabilities'])].reset_index(drop=True)
    sourceACurrentLiabilities=currentLiabilities[getSourceAppendedName('periodEndMV_InBase','sourceA')].sum()
    sourceBCurrentLiabilities=currentLiabilities[getSourceAppendedName('periodEndMV_InBase','sourceB')].sum()

    cashAndCashEquivalents=myTBChangeDF[myTBChangeDF['Level1']=='Cash and cash equivalents'].reset_index(drop=True)
    sourceACashAndCashEquivalents=cashAndCashEquivalents[getSourceAppendedName('periodEndMV_InBase','sourceA')].sum()
    sourceBCashAndCashEquivalents=cashAndCashEquivalents[getSourceAppendedName('periodEndMV_InBase','sourceB')].sum()

    myRevenueAndExpenseDF=getExpenseAndRevenueChangeDF(storage,fundName,sourceA,sourceB)
    # adminFeeDF=myRevenueAndExpenseDF[myRevenueAndExpenseDF['subType2']=='Admin Fees'].reset_index(drop=True) 
    # sourceAAdminFee=adminFeeDF[getSourceAppendedName('periodEndMV_InBase','sourceA')].sum()
    # sourceBAdminFee=adminFeeDF[getSourceAppendedName('periodEndMV_InBase','sourceB')].sum()
    # sourceAAdminFee,sourceBAdminFee

    managementFeeDF=myRevenueAndExpenseDF[myRevenueAndExpenseDF['subType2']=='Management Fees'].reset_index(drop=True) 
    sourceAManagementFee=managementFeeDF[getSourceAppendedName('periodEndMV_InBase','sourceA')].sum()
    sourceBManagementFee=managementFeeDF[getSourceAppendedName('periodEndMV_InBase','sourceB')].sum()

    performanceFeeDF=myRevenueAndExpenseDF[myRevenueAndExpenseDF['subType2']=='Performance Fees'].reset_index(drop=True) 
    sourceAPerformanceFee=performanceFeeDF[getSourceAppendedName('periodEndMV_InBase','sourceA')].sum()
    sourceBPerformanceFee=performanceFeeDF[getSourceAppendedName('periodEndMV_InBase','sourceB')].sum()

    NonTradingExpenseDF=myRevenueAndExpenseDF[myRevenueAndExpenseDF['subType']=='Non-Trading Expenses'].reset_index(drop=True)
    NonTradingExpenseDF=NonTradingExpenseDF[NonTradingExpenseDF['type']=='Expense'].reset_index(drop=True)
    # NonTradingExpenseDF=NonTradingExpenseDF[~NonTradingExpenseDF['subType2'].isin(['Admin Fees','Management Fees'])].reset_index(drop=True)

    sourceANonTradingExpense=NonTradingExpenseDF[getSourceAppendedName('periodEndMV_InBase','sourceA')].sum()
    sourceBNonTradingExpense=NonTradingExpenseDF[getSourceAppendedName('periodEndMV_InBase','sourceB')].sum()

    topLevelData={
        'sourceALiabilities':-sourceALiabilities,
        'sourceBLiabilities':-sourceBLiabilities,
        'sourceAAssets':sourceAAssets,
        'sourceBAssets':sourceBAssets,
        'sourceANav':sourceANav,
        'sourceBNav':sourceBNav
    }
    myValidations.append(getRatioValidation('Financial','Debt-To-Equity Ratio',
            -sourceALiabilities,sourceANav,-sourceBLiabilities,sourceBNav,
            validationParams['debt_to_equity_ratio']['threshold'],
            'Total Liabilities','Total NAV',unitPrefix="$",
            extraData=topLevelData
    ))
    
    myValidations.append(getRatioValidation('Financial','Gross Leverage Ratio', sourceAAssets,sourceANav,sourceBAssets,sourceBNav, validationParams['gross_leverage_ratio']['threshold'],'Total Assets','Total NAV',unitPrefix="$",extraData=topLevelData))
    myValidations.append(getRatioValidation('Financial','Expense Ratio',sourceANonTradingExpense,sourceAAssets,sourceBNonTradingExpense,sourceBAssets,validationParams['expense_ratio']['threshold'],'Non-Trading Expenses','Total Assets',unitPrefix="$"))
    # TODO: These are dummy values
    # myValidations.append(getRatioValidation('Financial','Margin To Equity Ratio',1.25,1,1.25,1,validationParams['margin_to_equity_ratio']['threshold'],'Margin','NAV'))
    myValidations.append(getRatioValidation('Financial','Management Fee Ratio',sourceAManagementFee,sourceANav,sourceBManagementFee,sourceBNav,validationParams['management_fee_ratio']['threshold'],'Management Fees','NAV',unitPrefix="$"))
    myValidations.append(getRatioValidation('Financial','Performance Fee Ratio',sourceAPerformanceFee,sourceANav,sourceBPerformanceFee,sourceBNav,validationParams['performance_fee_ratio']['threshold'],'Performance Fees','NAV',unitPrefix="$"))

    myValidations.append(getRatioValidation('Liquidity','Current Ratio',sourceACurrentAssets,-sourceACurrentLiabilities,sourceBCurrentAssets,-sourceBCurrentLiabilities,validationParams['current_assets_ratio']['threshold'],'Current Assets','Current Liabilities',unitPrefix="$"))

    myValidations.append(getRatioValidation('Liquidity','Redemption Liquidity Ratio',sourceACashAndCashEquivalents,sourceANav,sourceBCashAndCashEquivalents,sourceBNav,validationParams['cash_ratio']['threshold'],'Cash and Cash Equivalents','NAV',unitPrefix="$"))
    myValidations.append(getRatioValidation('Liquidity','Liquidity Ratio',sourceACashAndCashEquivalents,-sourceACurrentLiabilities,sourceBCashAndCashEquivalents,-sourceBCurrentLiabilities,validationParams['liquidity_ratio']['threshold'],'Cash and Cash Equivalents','Current Liabilities',unitPrefix="$"))

    myPositionsChangeDF=getPositionsChangeDF(storage,fundName,sourceA,sourceB)
    sourceATop10PosMV=myPositionsChangeDF.loc[myPositionsChangeDF[getSourceAppendedName('periodEndMV_InBase','sourceA')].abs().nlargest(10).index][getSourceAppendedName('periodEndMV_InBase','sourceA')].sum()
    sourceBTop10PosMV=myPositionsChangeDF.loc[myPositionsChangeDF[getSourceAppendedName('periodEndMV_InBase','sourceA')].abs().nlargest(10).index][getSourceAppendedName('periodEndMV_InBase','sourceB')].sum()

    investments=myTBChangeDF[myTBChangeDF['Level1']=='MV of Investments'].reset_index(drop=True)
    sourceAInvestments=investments[getSourceAppendedName('periodEndMV_InBase','sourceA')].sum()
    sourceBInvestments=investments[getSourceAppendedName('periodEndMV_InBase','sourceB')].sum()

    myValidations.append(getRatioValidation('Concentration','Top Holding Concentration Ratio',sourceATop10PosMV,sourceANav,sourceBTop10PosMV,sourceBNav,validationParams['top_10_positions_mv_ratio']['threshold'],'Top 10 Positions MV','NAV',unitPrefix="$"))
    myValidations.append(getRatioValidation('Concentration','Asset Concentration Ratio',sourceAInvestments,sourceANav,sourceBInvestments,sourceBNav,validationParams['asset_concentration_ratio']['threshold'],'MV of Investments','Total NAV',unitPrefix="$"))

    try:
        myCapitalChange=getCapitalChangeDF(storage,fundName,sourceA,sourceB)
    except Exception as e:
        return([VALIDATION_STATUS().setProductName('validus').setType('Ratio').setSubType('Error').setSubType2('Error').setMessage(-1).setData({'error':str(e)})])

    subsriptions=myCapitalChange[myCapitalChange['subType']=='ContributedCost - Deposits']
    withdrawals=myCapitalChange[myCapitalChange['subType']=='ContributedCost - Withdrawals']

    sourceASubs=subsriptions[getSourceAppendedName('periodEndMV_InBase','sourceA')].sum()
    sourceBSubs=subsriptions[getSourceAppendedName('periodEndMV_InBase','sourceB')].sum()

    sourceAWithdrawals=-withdrawals[getSourceAppendedName('periodEndMV_InBase','sourceA')].sum()
    sourceBWithdrawals=-withdrawals[getSourceAppendedName('periodEndMV_InBase','sourceB')].sum()
    
    myValidations.append(getRatioValidation('Sentiment','Subscription-Redemption Ratio',sourceASubs,sourceAWithdrawals,sourceBSubs,sourceBWithdrawals,validationParams['subscription_redemption_ratio']['threshold'],'Total Subscriptions','Total Redemptions',unitPrefix="$"))

    sourceALong=myPositionsChangeDF[myPositionsChangeDF[getSourceAppendedName('periodEndQuantity','sourceA')]>=0][getSourceAppendedName('periodEndMV_InBase','sourceA')].sum()
    sourceBLong=myPositionsChangeDF[myPositionsChangeDF[getSourceAppendedName('periodEndQuantity','sourceB')]>=0][getSourceAppendedName('periodEndMV_InBase','sourceB')].sum()
    sourceAShort=myPositionsChangeDF[myPositionsChangeDF[getSourceAppendedName('periodEndQuantity','sourceA')]<0][getSourceAppendedName('periodEndMV_InBase','sourceA')].sum()
    sourceBShort=myPositionsChangeDF[myPositionsChangeDF[getSourceAppendedName('periodEndQuantity','sourceB')]<0][getSourceAppendedName('periodEndMV_InBase','sourceB')].sum()

    # TODO: Remove hardcoded values - Excess Return over Benchmark should be calculated from actual data
    # myValidations.append(getRatioValidation('Sentiment','Excess Return over Benchmark',105,100,102,100,1,'Portfolio Return','Benchmark Return',unitSuffix="%"))

    myValidations.append(getRatioValidation('Sentiment','Net Long Position Ratio',
                                            sourceALong+sourceAShort,
                                            sourceALong-sourceAShort,
                                            sourceBLong+sourceBShort,
                                            sourceBLong-sourceBShort,
                                            validationParams['net_long_position_ratio']['threshold'],'Net Long Exposure','Total Exposure',unitPrefix="$"))

    return myValidations

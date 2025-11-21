from validations import VALIDATION_STATUS
from storage import STORAGE
from utils.NAVFetchUtil import getExpenseAndRevenueChangeDF
from utils.unclassified import getArrayOfStructFromDF


def NonTradingValidations(storage:STORAGE,fundName:str,sourceA:dict,sourceB:dict,validationParams:dict):
    #TODO clean this up
    myValidations=[]
    
    myDF=getExpenseAndRevenueChangeDF(storage,fundName,sourceA,sourceB)

    legalExpensesDF=myDF[myDF['subType2']=='Legal Fees']
    myValidations.append(verifyWithinTgreshold(legalExpensesDF,'Expenses','Legal Fees','legal_fees_change',validationParams))

    adminFeeDF=myDF[myDF['subType2']=='Admin Fees']
    myValidations.append(verifyWithinTgreshold(adminFeeDF,'Expenses','Admin Fees','admin_fees_change',validationParams))

    adminExpensesDF=myDF[myDF['subType2']=='Other Admin Expenses']
    myValidations.append(verifyWithinTgreshold(adminExpensesDF,'Expenses','Other Admin Expenses','other_admin_expenses_change',validationParams))

    accountingExpenseDF=myDF[myDF['subType2']=='Accounting Expense']
    myValidations.append(verifyWithinTgreshold(accountingExpenseDF,'Expenses','Accounting Expense','accounting_expenses_change',validationParams))

    interestExpenseDF=myDF[myDF['subType2'].str.contains('Interest Expense')]
    myValidations.append(verifyWithinTgreshold(interestExpenseDF,'Expenses','Interest Expense','interest_expense_change',validationParams))

    managementFeesDF=myDF[myDF['subType2']=='Management Fees']
    myValidations.append(verifyWithinTgreshold(managementFeesDF,'Fees','Management Fees','management_fees_change',validationParams))

    # performanceFeesDF=myDF[myDF['subType2']=='Performance Fees']
    # myValidations.append(verifyWithinTgreshold(performanceFeesDF,'Fees','Performance Fees','performance_fees_change',validationParams))

    return myValidations

def verifyWithinTgreshold(aDF,aSubType:str,aSubType2:str,thresholdName:str,validationParams:dict):
    myValidation=VALIDATION_STATUS().setProductName('validus').setType('NON-TRADING').setSubType(aSubType).setSubType2(aSubType2).setThreshold(validationParams[thresholdName]['threshold'])
    if len(aDF) == 1: 
        change=(aDF['periodEndMV_InBase___sourceB'].values[0]/aDF['periodEndMV_InBase___sourceA'].values[0])-1
        if abs(change) > validationParams[thresholdName]['threshold']:
            return myValidation.setMessage(-1).setData({'rows':getArrayOfStructFromDF(aDF)})
        else:
            return myValidation.setMessage(0)
    else:
        return myValidation.setMessage(-len(aDF))

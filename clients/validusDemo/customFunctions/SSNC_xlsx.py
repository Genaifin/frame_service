def getTBAssetOrLiability(row):
    if row['Category'] in ['Due to/from Custodians','Investments in']:
        return 'Assets'
        
    if row['Type'] == 'Assets':
        return 'Assets'
    elif row['Type'] == 'Liabilities':
        return 'Liabilities' 
    
    # 1 edge case where 1 CAD balance which is a Libility but is tagged as payable is getting differetnly classed than what Umang did

    return 'Unhandled'
    

def getTBLevel1(row):
    if 'Investments in' == row['Category']:
        return 'MV of Investments'
    if 'Due to/from Custodians'==row['Category']:
        return 'Cash and cash equivalents'
    if 'AP'==row['Category']:
        return 'Account Payable'
    if 'AR'==row['Category']:
        return 'Account Receivable'  
    
    if row['Category'] in ['Other']:
        if row['Type'] == 'Assets':
            return 'Other Assets'
        elif row['Type'] == 'Liabilities':
            return 'Other Liabilities' 
        
    return 'Unhandled'


def getRevenueOrExpenseSubType2(row):
    if row['Financial Account'] == 'MgmtFeeExpense':
        return 'Management Fees'
    
    if row['Financial Account'] == 'Legal Expense':
        return 'Legal Fees'

    if row['Financial Account'] == 'Fund Administration Fees':
        return 'Admin Fees'

    if row['Financial Account'] == 'Performance Fees':
        return 'Performance Fees'
    
    if row['Financial Account'] in ['NotHandledYet','DontHaveAStringYet']: #TODO remove or properly use this header
        return 'Other Admin Expenses'

    return row['Financial Account']

def getRevenueOrExpenseSubType(row):
    if row['Accounting Head'] == 'PSEUDO SECURITIES::NONTRADEEXP':
        return 'Non-Trading Expenses'
    
    return row['Accounting Head']
    

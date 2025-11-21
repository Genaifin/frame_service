from utils.NAVFetchUtil import (
    getSourceNAV, getTBChangeDF, getPositionsChangeDF, 
    getExpenseAndRevenueChangeDF, getEnrichedTB, 
    getEnrichedPositions, _getEnrichedRevenueAndExpense,
    getSourceAppendedName
)
from storage import STORAGE
import pandas as pd
import numpy as np
from datetime import datetime


#TODO This file need rewriting/refactoring

def _sanitize_value(value):
    """
    Sanitize numeric values to ensure JSON compliance
    """
    try:
        if value is None or pd.isna(value):
            return 0.0
        if isinstance(value, (int, float)):
            if np.isnan(value) or np.isinf(value):
                return 0.0
            return float(value)
        # Try to convert to float
        return float(value)
    except (ValueError, TypeError):
        return 0.0

def _sanitize_dict(data_dict):
    """
    Recursively sanitize dictionary values to ensure JSON compliance
    """
    if isinstance(data_dict, dict):
        return {k: _sanitize_dict(v) for k, v in data_dict.items()}
    elif isinstance(data_dict, list):
        return [_sanitize_dict(item) for item in data_dict]
    elif isinstance(data_dict, (int, float)):
        return _sanitize_value(data_dict)
    else:
        return data_dict

def _mapAssetClassToReadableName(technicalCode: str):
    """
    Map technical asset class codes to human-readable category names
    """
    assetClassMapping = {
        # Equities
        'COMMNNLT': 'Equities',
        'COMMNLST': 'Equities', 
        'MMKTEQ': 'Equities',
        'EQTYCWRT': 'Equities',
        
        # Fixed Income
        'PSEUDOBND': 'Fixed Income',
        'BOND': 'Fixed Income',
        'GOVT': 'Fixed Income',
        
        # Derivatives
        'CFD': 'Derivatives',
        'EQINDFUT': 'Derivatives',
        'FUTURES': 'Derivatives',
        'OPTIONS': 'Derivatives',
        'SWAPS': 'Derivatives',
        
        # Cash & Cash Equivalents
        'CASH': 'Cash & Cash Equiv',
        'CASHF': 'Cash & Cash Equiv',
        'INTONCASH': 'Cash & Cash Equiv',
        'INTONCSH': 'Cash & Cash Equiv',
        
        # Other
        'NON-TRADING': 'Other',
        'OTHER': 'Other'
    }
    
    return assetClassMapping.get(technicalCode, 'Other')

class PnlCalculator:
    """
    Utility class for calculating PNL metrics for funds
    """
    
    def __init__(self, storage: STORAGE):
        self.storage = storage
    
    def calculateTotalPnl(self, fundName: str, sourceA: dict, sourceB: dict):
        """
        Calculate total PNL between two periods
        
        Args:
            fundName: Name of the fund
            sourceA: Source configuration for start period
            sourceB: Source configuration for end period
            
        Returns:
            dict: PNL breakdown with components
        """
        try:
            # Calculate NAV change
            navA = getSourceNAV(self.storage, fundName, sourceA)
            navB = getSourceNAV(self.storage, fundName, sourceB)
            navChange = navB - navA
            
            # Calculate position PNL
            positionPnl = self.calculatePositionPnl(fundName, sourceA, sourceB)
            
            # Calculate revenue and expense PNL
            revenueExpensePnl = self.calculateRevenueExpensePnl(fundName, sourceA, sourceB)
            
            # Calculate trading PNL
            tradingPnl = self.calculateTradingPnl(fundName, sourceA, sourceB)
            
            result = {
                'totalPnl': _sanitize_value(navChange),
                'positionPnl': positionPnl,
                'revenueExpensePnl': revenueExpensePnl,
                'tradingPnl': tradingPnl,
                'startNAV': _sanitize_value(navA),
                'endNAV': _sanitize_value(navB),
                'navChange': _sanitize_value(navChange)
            }
            
            return _sanitize_dict(result)
            
        except Exception as e:
            raise Exception(f"Error calculating total PNL: {str(e)}")
    
    def calculatePositionPnl(self, fundName: str, sourceA: dict, sourceB: dict):
        """
        Calculate PNL from position changes (unrealized gains/losses)
        """
        try:
            positionsChangeDF = getPositionsChangeDF(self.storage, fundName, sourceA, sourceB)
            
            # Sum unrealized PNL (change in value without trades)
            totalUnrealizedPnl = positionsChangeDF['ChangeInValueWithoutTrades'].fillna(0).sum()
            
            # Group by asset class for breakdown
            assetClassBreakdown = positionsChangeDF.groupby('productAssetClass').agg({
                'ChangeInValueWithoutTrades': 'sum',
                'valueOfTrades': 'sum',
                'changeInValue': 'sum'
            }).fillna(0).to_dict('index')
            
            result = {
                'totalUnrealizedPnl': _sanitize_value(totalUnrealizedPnl),
                'assetClassBreakdown': _sanitize_dict(assetClassBreakdown)
            }
            
            return result
            
        except Exception as e:
            return {'totalUnrealizedPnl': 0.0, 'assetClassBreakdown': {}}
    
    def calculateRevenueExpensePnl(self, fundName: str, sourceA: dict, sourceB: dict):
        """
        Calculate PNL from revenue and expenses
        """
        try:
            revenueExpenseDF = getExpenseAndRevenueChangeDF(self.storage, fundName, sourceA, sourceB)
            sourceBCol = getSourceAppendedName('periodEndMV_InBase', 'sourceB')
            
            # Separate revenue and expenses
            revenueDF = revenueExpenseDF[revenueExpenseDF['type'] == 'Revenue']
            expenseDF = revenueExpenseDF[revenueExpenseDF['type'] == 'Expense']
            
            totalRevenue = revenueDF[sourceBCol].fillna(0).sum()
            totalExpense = expenseDF[sourceBCol].fillna(0).sum()
            
            # Revenue breakdown by category
            revenueBreakdown = revenueDF.groupby('subType2')[sourceBCol].sum().fillna(0).to_dict()
            expenseBreakdown = expenseDF.groupby('subType2')[sourceBCol].sum().fillna(0).to_dict()
            
            result = {
                'totalRevenue': _sanitize_value(totalRevenue),
                'totalExpense': _sanitize_value(totalExpense),
                'netRevenueExpense': _sanitize_value(totalRevenue - totalExpense),
                'revenueBreakdown': _sanitize_dict(revenueBreakdown),
                'expenseBreakdown': _sanitize_dict(expenseBreakdown)
            }
            
            return result
            
        except Exception as e:
            return {
                'totalRevenue': 0.0,
                'totalExpense': 0.0,
                'netRevenueExpense': 0.0,
                'revenueBreakdown': {},
                'expenseBreakdown': {}
            }
    
    def calculateTradingPnl(self, fundName: str, sourceA: dict, sourceB: dict):
        """
        Calculate PNL from trading activities
        """
        try:
            positionsChangeDF = getPositionsChangeDF(self.storage, fundName, sourceA, sourceB)
            
            # Sum trading values
            totalTradingValue = positionsChangeDF['valueOfTrades'].fillna(0).sum()
            
            # Get trades by asset class
            tradingByAssetClass = positionsChangeDF.groupby('productAssetClass')['valueOfTrades'].sum().fillna(0).to_dict()
            
            result = {
                'totalTradingPnl': _sanitize_value(totalTradingValue),
                'tradingByAssetClass': _sanitize_dict(tradingByAssetClass)
            }
            
            return result
            
        except Exception as e:
            return {'totalTradingPnl': 0.0, 'tradingByAssetClass': {}}
    
    def calculatePnlByAssetClass(self, fundName: str, sourceA: dict, sourceB: dict):
        """
        Calculate comprehensive PNL breakdown by asset class
        """
        try:
            # Get position changes
            positionsChangeDF = getPositionsChangeDF(self.storage, fundName, sourceA, sourceB)
            
            # Get revenue/expense data
            revenueExpenseDF = getExpenseAndRevenueChangeDF(self.storage, fundName, sourceA, sourceB)
            sourceBCol = getSourceAppendedName('periodEndMV_InBase', 'sourceB')
            
            # Position PNL by asset class
            positionPnlByAssetClass = positionsChangeDF.groupby('productAssetClass').agg({
                'ChangeInValueWithoutTrades': 'sum',
                'valueOfTrades': 'sum',
                'changeInValue': 'sum'
            }).fillna(0).to_dict('index')
            
            # Map revenue/expense to asset classes
            revenueExpensePnlByAssetClass = {}
            for _, row in revenueExpenseDF.iterrows():
                assetClass = self._extractAssetClassFromSubType(row['subType'])
                amount = row[sourceBCol] if row['type'] == 'Revenue' else -row[sourceBCol]
                amount = _sanitize_value(amount)
                
                if assetClass not in revenueExpensePnlByAssetClass:
                    revenueExpensePnlByAssetClass[assetClass] = 0.0
                revenueExpensePnlByAssetClass[assetClass] += amount
            
            # Group by readable asset class names and combine PNL
            groupedPnlByAssetClass = {}
            
            # Add position PNL with readable names
            for technicalCode, pnlData in positionPnlByAssetClass.items():
                readableName = _mapAssetClassToReadableName(technicalCode)
                
                if readableName not in groupedPnlByAssetClass:
                    groupedPnlByAssetClass[readableName] = {
                        'positionPnl': 0.0,
                        'tradingPnl': 0.0,
                        'revenueExpensePnl': 0.0,
                        'totalPnl': 0.0
                    }
                
                groupedPnlByAssetClass[readableName]['positionPnl'] += _sanitize_value(pnlData['ChangeInValueWithoutTrades'])
                groupedPnlByAssetClass[readableName]['tradingPnl'] += _sanitize_value(pnlData['valueOfTrades'])
                groupedPnlByAssetClass[readableName]['totalPnl'] += _sanitize_value(pnlData['changeInValue'])
            
            # Add revenue/expense PNL with readable names
            for technicalCode, amount in revenueExpensePnlByAssetClass.items():
                readableName = _mapAssetClassToReadableName(technicalCode)
                
                if readableName not in groupedPnlByAssetClass:
                    groupedPnlByAssetClass[readableName] = {
                        'positionPnl': 0.0,
                        'tradingPnl': 0.0,
                        'revenueExpensePnl': 0.0,
                        'totalPnl': 0.0
                    }
                
                groupedPnlByAssetClass[readableName]['revenueExpensePnl'] += _sanitize_value(amount)
                groupedPnlByAssetClass[readableName]['totalPnl'] += _sanitize_value(amount)
            
            return _sanitize_dict(groupedPnlByAssetClass)
            
        except Exception as e:
            return {}
    
    def calculatePnlTrends(self, fundName: str, periods: list):
        """
        Calculate PNL trends across multiple periods
        
        Args:
            fundName: Name of the fund
            periods: List of date strings in chronological order
            
        Returns:
            list: PNL data for each period transition
        """
        trends = []
        
        for i in range(1, len(periods)):
            sourceA = {'source': 'Marquant', 'processDate': periods[i-1]}
            sourceB = {'source': 'Marquant', 'processDate': periods[i]}
            
            try:
                pnlData = self.calculateTotalPnl(fundName, sourceA, sourceB)
                pnlData['startDate'] = periods[i-1]
                pnlData['endDate'] = periods[i]
                trends.append(pnlData)
            except Exception as e:
                # Skip periods with calculation errors
                continue
        
        return trends
    
    def getAvailableDatesForFund(self, fundName: str):
        """
        Get all available dates for a specific fund
        """
        try:
            myTB = self.storage.getFilteredTableAsDF('l1', 'trail_balance', {})
            
            if myTB is None or len(myTB) == 0:
                return []
            
            # Filter by fund name and get unique dates
            fundTB = myTB[myTB['fundName'] == fundName]
            availableDates = sorted(fundTB['processDate'].unique().tolist())
            
            return availableDates
        except Exception:
            return []
    
    def calculateMarketValueByInvestmentType(self, fundName: str, source: dict):
        """
        Calculate market value breakdown by investment type (investments vs cash)
        """
        try:
            # Get positions data for investments
            positionsDF = getEnrichedPositions(self.storage, fundName, source)
            
            # Get trial balance data for cash and other items
            tbDF = getEnrichedTB(self.storage, fundName, source)
            
            # Calculate total market value of investments (positions)
            totalInvestmentsMV = positionsDF['periodEndMV_InBase'].fillna(0).sum()
            
            # Calculate cash and cash equivalents from trial balance
            # Filter for cash accounts (Level1 contains cash-related terms)
            cashFilter = tbDF['Level1'].str.contains('CASH|Cash|cash', case=False, na=False) | \
                        tbDF['Level2'].str.contains('CASH|Cash|cash', case=False, na=False) | \
                        tbDF['Level3'].str.contains('CASH|Cash|cash', case=False, na=False)
            
            cashDF = tbDF[cashFilter]
            totalCashMV = cashDF['periodEndMV_InBase'].fillna(0).sum()
            
            result = {
                'MV of Investments': _sanitize_value(totalInvestmentsMV),
                'Cash and Cash Equivalents': _sanitize_value(totalCashMV)
            }
            
            return _sanitize_dict(result)
            
        except Exception as e:
            return {
                'MV of Investments': 0.0,
                'Cash and Cash Equivalents': 0.0
            }
    
    def _extractAssetClassFromSubType(self, subType: str):
        """
        Extract asset class from revenue/expense subType
        """
        if '::' in subType:
            # Extract asset class from format like "EQUITIES - NON-LISTED::COMMNNLT"
            parts = subType.split('::')
            if len(parts) > 1:
                return parts[1]
        
        # Default mapping for non-trading items
        if 'Non-Trading' in subType:
            return 'NON-TRADING'
        
        return 'OTHER'

def getPnlCalculator(storage: STORAGE = None):
    """
    Factory function to get a PNL calculator instance
    """
    if storage is None:
        myStorageConfig = {
            'defaultFileStorage': 'onPrem',
        }
        client = 'validusDemo'
        storage = STORAGE(client, myStorageConfig)
    
    return PnlCalculator(storage)

def calculateFundPnlSummary(fundName: str, startDate: str, endDate: str):
    """
    Convenience function to calculate PNL summary for a fund between two dates
    """
    calculator = getPnlCalculator()
    
    sourceA = {'source': 'Marquant', 'processDate': startDate}
    sourceB = {'source': 'Marquant', 'processDate': endDate}
    
    return calculator.calculateTotalPnl(fundName, sourceA, sourceB)

def calculateFundPnlByAssetClass(fundName: str, startDate: str, endDate: str):
    """
    Convenience function to calculate PNL by asset class for a fund between two dates
    """
    calculator = getPnlCalculator()
    
    sourceA = {'source': 'Marquant', 'processDate': startDate}
    sourceB = {'source': 'Marquant', 'processDate': endDate}
    
    return calculator.calculatePnlByAssetClass(fundName, sourceA, sourceB) 
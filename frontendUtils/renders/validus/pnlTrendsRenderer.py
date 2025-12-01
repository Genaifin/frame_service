from utils.dateUtil import convertDateToFormat
from utils.pnlCalculations import getPnlCalculator
from datetime import datetime
import pandas as pd
import numpy as np

#TODO This file need rewriting/refactoring

def _sanitizeChartValue(value):
    """
    Sanitize numeric values for chart data to ensure JSON compliance
    """
    try:
        if value is None or pd.isna(value):
            return 0.0
        if isinstance(value, (int, float)):
            if np.isnan(value) or np.isinf(value):
                return 0.0
            return float(value)
        return float(value)
    except (ValueError, TypeError):
        return 0.0

def _formatDateForChart(dateStr):
    """
    Convert date string to lowercase format for chart keys (e.g., 'Feb 2024' -> 'feb2024')
    """
    try:
        dateObj = datetime.strptime(dateStr, '%b %Y')
        return dateObj.strftime('%b%Y').lower()
    except:
        return dateStr.lower().replace(' ', '')

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
        'EQUITY': 'Equities',
        'EQUITIES': 'Equities',
        
        # Fixed Income
        'PSEUDOBND': 'Fixed Income',
        'BOND': 'Fixed Income',
        'BONDS': 'Fixed Income',
        'GOVT': 'Fixed Income',
        'FIXEDINCOME': 'Fixed Income',
        
        # Derivatives
        'CFD': 'Derivatives',
        'EQINDFUT': 'Derivatives',
        'FUTURES': 'Derivatives',
        'OPTIONS': 'Derivatives',
        'SWAPS': 'Derivatives',
        'DERIVATIVE': 'Derivatives',
        
        # Cash & Cash Equivalents
        'CASH': 'Cash & Cash Equivalents',
        'CASHF': 'Cash & Cash Equivalents',
        'INTONCASH': 'Cash & Cash Equivalents',
        'INTONCSH': 'Cash & Cash Equivalents',
        'CASHEQUIV': 'Cash & Cash Equivalents',
        
        # Other
        'NON-TRADING': 'Other',
        'NONTRADING': 'Other',
        'OTHER': 'Other'
    }
    
    return assetClassMapping.get(technicalCode, 'Other')

def getPnlByAssetClass(params: dict):
    """
    Returns PNL trends data by asset class for the trends tab
    """
    fundName = params['query'].get('fundName')
    
    if not fundName:
        return {
            "title": "PNL By Asset Class",
            "chartConfig": {
                "data": [],
                "series": [],
                "legend": {"enabled": False}
            }
        }
    
    try:
        calculator = getPnlCalculator()
        chartData = _getPnlDataByAssetClass(calculator, fundName)
        chartSeries = _generateChartSeries(chartData)
        
        return {
            "title": "PNL By Asset Class",
            "chartConfig": {
                "data": chartData,
                "series": chartSeries,
                "legend": {"enabled": True},
                "axes": [
                    {
                        "type": "category",
                        "position": "bottom",
                        "label": {"formatter": ""}
                    },
                    {
                        "type": "number",
                        "position": "left",
                        "label": {"formatter": "k", "currency": "$"}
                    }
                ]
            },
            "cssProperties": {
                "fontSize": "16px",
                "fontWeight": "600",
                "textTransform": "uppercase",
                "color": "#475569",
                "borderRadius": "24px"
            }
        }
    except Exception as e:
        return {
            "title": "PNL By Asset Class",
            "chartConfig": {
                "data": [],
                "series": [],
                "legend": {"enabled": False}
            }
        }

def _getPnlDataByAssetClass(calculator, fundName: str):
    """
    Calculate PNL data by asset class for the last available periods
    """
    availableDates = calculator.getAvailableDatesForFund(fundName)
    
    if len(availableDates) < 2:
        return []
    
    availableDates.sort()
    periodsToAnalyze = availableDates[-6:] if len(availableDates) >= 6 else availableDates
    
    assetClassPnlData = {}
    
    for i in range(1, len(periodsToAnalyze)):
        sourceA = {'source': 'Marquant', 'processDate': periodsToAnalyze[i-1]}
        sourceB = {'source': 'Marquant', 'processDate': periodsToAnalyze[i]}
        
        periodLabel = convertDateToFormat(periodsToAnalyze[i], 'MMM YYYY')
        
        try:
            combinedPnl = calculator.calculatePnlByAssetClass(fundName, sourceA, sourceB)
            
            for assetClass, pnlData in combinedPnl.items():
                if assetClass not in assetClassPnlData:
                    assetClassPnlData[assetClass] = {}
                assetClassPnlData[assetClass][periodLabel] = _sanitizeChartValue(pnlData['totalPnl'])
                
        except Exception as e:
            continue
    
    # Convert to chart format with proper date formatting
    # Note: assetClassPnlData already contains readable names from calculatePnlByAssetClass
    
    chartData = []
    for readableName, periodData in assetClassPnlData.items():
        chartRow = {"quarter": readableName}
        for period, value in periodData.items():
            chartKey = _formatDateForChart(period)
            chartRow[chartKey] = _sanitizeChartValue(value)
        chartData.append(chartRow)
    
    return chartData



def _generateChartSeries(chartData: list):
    """
    Generate chart series configuration based on available data
    """
    if not chartData:
        return []
    
    periodColumns = []
    for row in chartData:
        for key in row.keys():
            if key != 'quarter' and key not in periodColumns:
                periodColumns.append(key)
    
    # Sort period columns chronologically (convert back to date format for sorting)
    def _sortKey(period):
        try:
            # Convert 'feb2024' back to 'Feb 2024' for sorting
            monthYear = period[:3].capitalize() + ' ' + period[3:]
            return datetime.strptime(monthYear, '%b %Y')
        except:
            return datetime.now()
    
    periodColumns.sort(key=_sortKey)
    
    chartSeries = []
    for period in periodColumns:
        # Convert chart key back to display format
        displayName = period[:3].capitalize() + ' ' + period[3:]
        chartSeries.append({
            "type": "bar",
            "xKey": "quarter",
            "yKey": period,
            "yName": displayName
        })
    
    return chartSeries



def getSingleFundPnlTrends(params: dict):
    """
    Get PNL trends for a single fund across time periods
    """
    fundName = params['query'].get('fundName')
    
    if not fundName:
        return {"error": "Fund name is required"}
    
    try:
        calculator = getPnlCalculator()
        availableDates = calculator.getAvailableDatesForFund(fundName)
        
        if len(availableDates) < 2:
            return {"error": "Insufficient data for PNL calculation"}
        
        pnlTrends = calculator.calculatePnlTrends(fundName, availableDates)
        
        formattedTrends = []
        for trend in pnlTrends:
            formattedTrends.append({
                'period': convertDateToFormat(trend['endDate'], 'MMM YYYY'),
                'startDate': trend['startDate'],
                'endDate': trend['endDate'],
                'startNAV': _sanitizeChartValue(trend['startNAV']),
                'endNAV': _sanitizeChartValue(trend['endNAV']),
                'totalPnl': _sanitizeChartValue(trend['totalPnl']),
                'positionPnl': _sanitizeChartValue(trend['positionPnl']['totalUnrealizedPnl']),
                'revenue': _sanitizeChartValue(trend['revenueExpensePnl']['totalRevenue']),
                'expense': _sanitizeChartValue(trend['revenueExpensePnl']['totalExpense'])
            })
        
        return {"pnlTrends": formattedTrends}
        
    except Exception as e:
        return {"error": str(e)}

def getMvOfInvestments(params: dict):
    """
    Returns Market Value of Investments data for the trends tab
    """
    fundName = params['query'].get('fundName')
    
    if not fundName:
        return {
            "title": "MV Of Investments",
            "chartConfig": {
                "data": [],
                "series": [],
                "legend": {"enabled": False}
            }
        }
    
    try:
        calculator = getPnlCalculator()
        chartData = _getMvDataByInvestmentType(calculator, fundName)
        chartSeries = _generateChartSeries(chartData)
        
        return {
            "title": "MV Of Investments",
            "chartConfig": {
                "data": chartData,
                "series": chartSeries,
                "legend": {"enabled": True},
                "axes": [
                    {
                        "type": "category",
                        "position": "bottom",
                        "label": {"formatter": ""}
                    },
                    {
                        "type": "number",
                        "position": "left",
                        "label": {"formatter": "k", "currency": "$"}
                    }
                ]
            },
            "cssProperties": {
                "fontSize": "16px",
                "fontWeight": "600",
                "textTransform": "uppercase",
                "color": "#475569",
                "borderRadius": "24px"
            }
        }
    except Exception as e:
        return {
            "title": "MV Of Investments",
            "chartConfig": {
                "data": [],
                "series": [],
                "legend": {"enabled": False}
            }
        }


def _getMvDataByInvestmentType(calculator, fundName: str):
    """
    Calculate Market Value data by investment type for the last available periods
    """
    availableDates = calculator.getAvailableDatesForFund(fundName)
    
    if len(availableDates) < 1:
        return []
    
    availableDates.sort()
    periodsToAnalyze = availableDates[-6:] if len(availableDates) >= 6 else availableDates
    
    investmentTypeMvData = {}
    
    for processDate in periodsToAnalyze:
        source = {'source': 'Marquant', 'processDate': processDate}
        periodLabel = convertDateToFormat(processDate, 'MMM YYYY')
        
        try:
            mvData = calculator.calculateMarketValueByInvestmentType(fundName, source)
            
            for investmentType, mvValue in mvData.items():
                if investmentType not in investmentTypeMvData:
                    investmentTypeMvData[investmentType] = {}
                investmentTypeMvData[investmentType][periodLabel] = _sanitizeChartValue(mvValue)
                
        except Exception as e:
            continue
    
    # Convert to chart format with proper date formatting
    
    chartData = []
    for investmentType, periodData in investmentTypeMvData.items():
        chartRow = {"quarter": investmentType}
        for period, value in periodData.items():
            chartKey = _formatDateForChart(period)
            chartRow[chartKey] = _sanitizeChartValue(value)
        chartData.append(chartRow)
    
    return chartData


def getFundPnlBreakdown(params: dict):
    """
    Get detailed PNL breakdown for a specific period
    """
    fundName = params['query'].get('fundName')
    startDate = params['query'].get('startDate')
    endDate = params['query'].get('endDate')
    
    if not all([fundName, startDate, endDate]):
        return {"error": "Fund name, start date, and end date are required"}
    
    try:
        calculator = getPnlCalculator()
        
        sourceA = {'source': 'Marquant', 'processDate': startDate}
        sourceB = {'source': 'Marquant', 'processDate': endDate}
        
        totalPnl = calculator.calculateTotalPnl(fundName, sourceA, sourceB)
        
        return {
            "summary": {
                "totalPnl": _sanitizeChartValue(totalPnl['totalPnl']),
                "startNAV": _sanitizeChartValue(totalPnl['startNAV']),
                "endNAV": _sanitizeChartValue(totalPnl['endNAV']),
                "navChange": _sanitizeChartValue(totalPnl['navChange'])
            },
            "positionPnl": totalPnl['positionPnl'],
            "revenueExpensePnl": totalPnl['revenueExpensePnl'],
            "tradingPnl": totalPnl['tradingPnl']
        }
        
    except Exception as e:
        return {"error": str(e)}
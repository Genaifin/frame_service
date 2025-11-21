"""
Database-driven file validation functions
Modular functions for data availability and file status checks
"""

from server.APIServerUtils.db_validation_service import db_validation_service
from validations import VALIDATION_STATUS
from typing import List, Dict, Any, Optional


def file_validations(fund_name: str, source_a: str, source_b: str, date_a: str, date_b: str) -> List[Any]:
    """
    Main function to calculate all file validations using database data
    """
    validations = []
    
    # Check data availability for both sources and dates
    validations.extend(data_availability_validations(fund_name, source_a, date_a, 'sourceA'))
    
    if date_b != date_a or source_b != source_a:
        validations.extend(data_availability_validations(fund_name, source_b, date_b, 'sourceB'))
    
    # Add overall file received validation
    validations.append(file_received_validation(fund_name, source_a, source_b, date_a, date_b))
    
    return validations


def data_availability_validations(fund_name: str, source: str, date: str, source_label: str) -> List[Any]:
    """
    Check data availability for trial balance and portfolio data
    """
    validations = []
    
    # Check trial balance data
    trial_balance_data = db_validation_service.get_trial_balance_data(fund_name, source, date)
    tb_count = len(trial_balance_data) if trial_balance_data else 0
    
    validations.append(_create_data_validation(
        'Trial Balance', source_label, tb_count,
        f'Trial balance data for {source} on {date}'
    ))
    
    # Check portfolio valuation data
    portfolio_data = db_validation_service.get_portfolio_valuation_data(fund_name, source, date)
    pv_count = len(portfolio_data) if portfolio_data else 0
    
    validations.append(_create_data_validation(
        'Portfolio Valuation', source_label, pv_count,
        f'Portfolio valuation data for {source} on {date}'
    ))
    
    return validations


def file_received_validation(fund_name: str, source_a: str, source_b: str, date_a: str, date_b: str) -> Any:
    """
    Create a file received validation to maintain compatibility with existing structure
    """
    
    # Check if data exists for both sources
    has_data_a = _check_data_exists(fund_name, source_a, date_a)
    has_data_b = _check_data_exists(fund_name, source_b, date_b) if (date_b != date_a or source_b != source_a) else has_data_a
    
    # File is considered "received" if data exists for both sources
    file_received = has_data_a and has_data_b
    
    validation_data = {
        'sourceA': source_a,
        'sourceB': source_b,
        'dateA': date_a,
        'dateB': date_b,
        'hasDataA': has_data_a,
        'hasDataB': has_data_b,
        'fileReceived': file_received,
        'dataSource': 'database'
    }
    
    return (VALIDATION_STATUS()
            .setProductName('validus')
            .setType('file_revieved')  # Keep original typo for compatibility
            .setSubType('File Status')
            .setSubType2('File Received')
            .setMessage(0 if file_received else 1)  # 0 = pass, 1 = fail
            .setData(validation_data))


def data_quality_validations(fund_name: str, source_a: str, source_b: str, date_a: str, date_b: str) -> List[Any]:
    """
    Additional data quality checks for database integrity
    """
    validations = []
    
    # Check for data consistency
    validations.extend(_check_data_consistency(fund_name, source_a, date_a, 'sourceA'))
    
    if date_b != date_a or source_b != source_a:
        validations.extend(_check_data_consistency(fund_name, source_b, date_b, 'sourceB'))
    
    return validations


def _check_data_exists(fund_name: str, source: str, date: str) -> bool:
    """
    Check if data exists for the given fund, source, and date
    """
    trial_balance_data = db_validation_service.get_trial_balance_data(fund_name, source, date)
    portfolio_data = db_validation_service.get_portfolio_valuation_data(fund_name, source, date)
    
    return bool(trial_balance_data or portfolio_data)


def _check_data_consistency(fund_name: str, source: str, date: str, source_label: str) -> List[Any]:
    """
    Check data consistency and completeness
    """
    validations = []
    
    trial_balance_data = db_validation_service.get_trial_balance_data(fund_name, source, date)
    portfolio_data = db_validation_service.get_portfolio_valuation_data(fund_name, source, date)
    
    # Check if trial balance balances to zero (assets = liabilities + equity)
    if trial_balance_data:
        total_balance = sum(item.get('Ending Balance', 0) for item in trial_balance_data)
        balance_check = abs(total_balance) < 0.01  # Allow for small rounding differences
        
        validations.append(_create_consistency_validation(
            'Trial Balance Balance', source_label, balance_check,
            f'Trial balance balances for {source} on {date}',
            {'total_balance': total_balance, 'balanced': balance_check}
        ))
    
    # Check for negative market values in portfolio
    if portfolio_data:
        negative_mv_count = sum(1 for item in portfolio_data 
                               if item.get('end_local_mv', 0) < 0)
        
        validations.append(_create_consistency_validation(
            'Portfolio Market Values', source_label, negative_mv_count == 0,
            f'No negative market values in portfolio for {source} on {date}',
            {'negative_mv_count': negative_mv_count}
        ))
    
    return validations


def _create_data_validation(data_Type: str, source_label: str, count: int, description: str) -> Any:
    """
    Create a data availability validation
    """
    # Data is available if count > 0
    has_data = count > 0
    
    validation_data = {
        'dataType': data_Type,
        'sourceLabel': source_label,
        'recordCount': count,
        'hasData': has_data,
        'description': description
    }
    
    return (VALIDATION_STATUS()
            .setProductName('validus')
            .setType('Data Availability')
            .setSubType(data_Type)
            .setSubType2(source_label)
            .setMessage(0 if has_data else 1)  # 0 = pass, 1 = fail
            .setData(validation_data))


def _create_consistency_validation(check_Type: str, source_label: str, passed: bool, 
                                 description: str, additional_data: Dict) -> Any:
    """
    Create a data consistency validation
    """
    validation_data = {
        'checkType': check_Type,
        'sourceLabel': source_label,
        'passed': passed,
        'description': description,
        **additional_data
    }
    
    return (VALIDATION_STATUS()
            .setProductName('validus')
            .setType('Data Quality')
            .setSubType(check_Type)
            .setSubType2(source_label)
            .setMessage(0 if passed else 1)  # 0 = pass, 1 = fail
            .setData(validation_data))

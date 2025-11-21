"""
Centralized Financial Metrics Calculator

This module provides a unified way to calculate comprehensive financial metrics from all data sources
that can be used across all validation types (NAV, Ratio, etc.)
"""

import pandas as pd
from typing import Dict, List, Optional
from datetime import datetime
from dateutil.relativedelta import relativedelta


def calculate_financial_metrics(trial_balance_data: List[Dict]) -> Dict[str, float]:
    """
    Legacy function - now calls comprehensive metrics with only trial balance data
    
    This function is kept for backward compatibility but now uses the comprehensive
    metrics calculator internally. For new code, use calculate_comprehensive_metrics() directly.
    
    Args:
        trial_balance_data: List of trial balance records
        
    Returns:
        Dictionary containing all calculated financial metrics from trial balance only
    """
    return calculate_comprehensive_metrics(
        trial_balance_data=trial_balance_data,
        portfolio_data=None,
        dividend_data=None,
        source="trial_balance_only",
        date=""
    )


def get_metric_value(trial_balance_data: List[Dict], metric_name: str) -> float:
    """
    Get a specific metric value from trial balance data
    
    Args:
        trial_balance_data: List of trial balance records
        metric_name: Name of the metric to retrieve
        
    Returns:
        The metric value, or 0.0 if not found
    """
    metrics = calculate_financial_metrics(trial_balance_data)
    return metrics.get(metric_name, 0.0)


def get_multiple_metrics(trial_balance_data: List[Dict], metric_names: List[str]) -> Dict[str, float]:
    """
    Get multiple metric values from trial balance data
    
    Args:
        trial_balance_data: List of trial balance records
        metric_names: List of metric names to retrieve
        
    Returns:
        Dictionary containing the requested metrics
    """
    all_metrics = calculate_financial_metrics(trial_balance_data)
    return {name: all_metrics.get(name, 0.0) for name in metric_names}


def calculate_comprehensive_metrics(trial_balance_data: List[Dict], 
                                   portfolio_data: Optional[List[Dict]] = None,
                                   dividend_data: Optional[List[Dict]] = None,
                                   source: str = "", 
                                   date: str = "") -> Dict[str, float]:
    """
    Calculate comprehensive financial metrics using data from all database tables
    
    This is the main function that should be used for all metric calculations across the system.
    It combines trial balance, portfolio valuation, and dividend data to provide complete metrics.
    
    Args:
        trial_balance_data: List of trial balance records
        portfolio_data: Optional list of portfolio valuation records  
        dividend_data: Optional list of dividend records
        source: Data source name (for logging)
        date: Date string (for logging)
        
    Returns:
        Dictionary containing all calculated financial metrics (37+ metrics)
    """
    
    # Initialize comprehensive metrics with all 37+ metric categories
    metrics = {
        # Basic Financial Metrics (from Trial Balance)
        'total_assets': 0.0, 'total_liabilities': 0.0, 'total_equity': 0.0, 'nav': 0.0,
        'non_trading_expenses': 0.0, 'management_fees': 0.0, 'performance_fees': 0.0,
        'total_expenses': 0.0,  # <-- Added total_expenses metric
        # Detailed Expense Metrics (from Trial Balance - for NAV validations)
        'legal_fees': 0.0, 'admin_fees': 0.0, 'other_admin_expenses': 0.0,
        'interest_expense': 0.0, 'accounting_expenses': 0.0, 'allocation_fee': 0.0,
        'audit_expense': 0.0, 'bank_fees': 0.0, 'borrow_fee_estimate': 0.0,
        'borrow_fee_expense': 0.0, 'distribution_fee_expense': 0.0, 'fs_prep_fees': 0.0,
        'fund_expense': 0.0, 'stockloan_fees': 0.0,
        'tax_preparation_fees': 0.0,
        'current_assets': 0.0, 'current_liabilities': 0.0, 'cash_and_equivalents': 0.0,
        'liquid_assets': 0.0,
        
        # Portfolio-based Metrics (from Portfolio Valuation table)
        'total_market_value': 0.0, 'total_positions': 0.0, 'total_quantity': 0.0,
        'average_position_size': 0.0, 'largest_position_mv': 0.0,
        
        # Concentration Metrics (calculated from actual portfolio data)
        'investments': 0.0, 'top_holdings_value': 0.0, 'top_5_positions_mv': 0.0,
        'single_asset_concentration': 0.0, 'sector_concentration': 0.0,
        'geography_concentration': 0.0,
        
        # Dividend & Income Metrics (from Dividend table)
        'total_dividends_received': 0.0, 'dividend_yield': 0.0, 'income_from_investments': 0.0,
        'swap_financing': 0.0, 'interest_accruals': 0.0,
        
        # Sentiment Metrics
        'total_subscriptions': 0.0, 'total_redemptions': 0.0, 'subscription_inflows': 0.0,
        'redemption_outflows': 0.0, 'net_flows': 0.0, 'fund_return': 0.0, 'portfolio_return': 0.0,
        'benchmark_return': 0.0, 'excess_return': 0.0, 'net_long_positions': 0.0,
        'net_short_positions': 0.0, 'gross_exposure': 0.0, 'net_exposure': 0.0
    }
    
    try:
        # =====================================
        # TRIAL BALANCE BASED METRICS
        # =====================================
        if trial_balance_data:
            
            df = pd.DataFrame(trial_balance_data)
            
            # Total Assets = Sum of all Assets type entries (net basis)
            net_assets_filter = df['Type'] == 'Assets'
            net_assets = 0.0
            if net_assets_filter.any():
                net_assets = df[net_assets_filter]['Ending Balance'].sum()
            
            # Total Liabilities calculation
            liabilities_filter = df['Type'] == 'Liabilities'
            net_liabilities = 0.0
            if liabilities_filter.any():
                net_liabilities = abs(df[liabilities_filter]['Ending Balance'].sum())
            
            account_payable_filter = liabilities_filter & df['Category'].str.contains('Account Payable|AP', na=False)
            investment_filter = df['Category'].str.contains('Investment|Custodian', na=False)
            other_liabilities_filter = liabilities_filter & ~account_payable_filter & ~investment_filter
            
            if account_payable_filter.any() or other_liabilities_filter.any():
                metrics['total_liabilities'] = abs(df[account_payable_filter]['Ending Balance'].sum()) + abs(df[other_liabilities_filter]['Ending Balance'].sum())
            else:
                metrics['total_liabilities'] = net_liabilities

            # CORRECTED NAV CALCULATION: Sum of all ending balances (excluding Revenue, Expense, Capital)
            # This matches the logic in db_validation_service.calculate_nav()
            nav_value = 0.0
            excluded_types = ['revenue', 'expense', 'capital']
            
            for _, entry in df.iterrows():
                entry_type = entry.get('Type', '').strip().lower()
                if entry_type in excluded_types:
                    continue
                    
                ending_balance = entry.get('Ending Balance', 0)
                if ending_balance is not None:
                    try:
                        nav_value += float(ending_balance)
                    except (ValueError, TypeError):
                        continue  # Skip invalid values
            
            metrics['nav'] = nav_value
            metrics['total_equity'] = metrics['nav']  # Keep total_equity for backward compatibility
            metrics['total_assets'] = net_assets
            
            # Total Assets (categorized) = All Investments + Custodian + Account Receivable + Other Assets
            investments_filter = df['Category'].str.contains('Investment|Custodian', na=False)
            account_receivable_filter = df['Category'].str.contains('Account Receivable|AR', na=False)
            other_assets_filter = df['Category'].str.contains('Other', na=False) & net_assets_filter
            metrics['total_assets'] = df[investments_filter | account_receivable_filter | other_assets_filter]['Ending Balance'].sum()

            
            # Non-trading expenses = Expense type entries (excluding Admin Fees and Management Fees)
            expense_filter = df['Type'] == 'Expense'
            if expense_filter.any():
                # non trading expense means accounting head contains nontrade
                non_trading_filter = df['Accounting Head'].str.contains('nontrade', case=False, na=False)
                metrics['non_trading_expenses'] = abs(df[expense_filter & non_trading_filter]['Ending Balance'].sum())
            
            # Management Fees - look in Financial Account field
            mgmt_fee_filter = df['Financial Account'].str.contains('Mgmt', case=False, na=False)
            if mgmt_fee_filter.any():
                metrics['management_fees'] = abs(df[mgmt_fee_filter & expense_filter]['Ending Balance'].sum())
            
            # Performance Fees - look in Financial Account field
            # Performance Fee calculation - REMOVED as per requirements
            # Set to 0.0 for ratio calculations (performance_fee_ratio still needs this metric)
            perf_fee_filter = df['Financial Account'].str.contains('Perf', case=False, na=False)
            if perf_fee_filter.any():
                metrics['performance_fees'] = abs(df[perf_fee_filter]['Ending Balance'].sum())

            # Detailed Expense Metrics (using Financial Account field with expense_filter)
            metrics['legal_fees'] = abs(df[expense_filter & df['Financial Account'].str.contains('Legal Expense', na=False)]['Ending Balance'].sum())
            metrics['admin_fees'] = abs(df[expense_filter & df['Financial Account'].str.contains('Admin', na=False)]['Ending Balance'].sum())
            metrics['other_admin_expenses'] = abs(df[expense_filter & df['Financial Account'].str.contains('Other Admin', na=False)]['Ending Balance'].sum())
            metrics['interest_expense'] = abs(df[expense_filter & df['Financial Account'].str.contains('Interest Expense', na=False)]['Ending Balance'].sum())
            metrics['accounting_expenses'] = abs(df[expense_filter & df['Financial Account'].str.contains('Accounting Expense', na=False)]['Ending Balance'].sum())
            metrics['allocation_fee'] = abs(df[expense_filter & df['Financial Account'].str.contains('Allocation Fee', na=False)]['Ending Balance'].sum())
            metrics['audit_expense'] = abs(df[expense_filter & df['Financial Account'].str.contains('Audit Expense', na=False)]['Ending Balance'].sum())
            metrics['bank_fees'] = abs(df[expense_filter & df['Financial Account'].str.contains('Bank Fees', na=False)]['Ending Balance'].sum())
            metrics['borrow_fee_estimate'] = abs(df[expense_filter & df['Financial Account'].str.contains('BorrowFeeEstimate', na=False)]['Ending Balance'].sum())
            metrics['borrow_fee_expense'] = abs(df[expense_filter & df['Financial Account'].str.contains('BorrowFeeExpense', na=False)]['Ending Balance'].sum())
            metrics['distribution_fee_expense'] = abs(df[expense_filter & df['Financial Account'].str.contains('DistributionFeeExpense', na=False)]['Ending Balance'].sum())
            metrics['fs_prep_fees'] = abs(df[expense_filter & df['Financial Account'].str.contains('FSPrepFees', na=False)]['Ending Balance'].sum())
            metrics['fund_expense'] = abs(df[expense_filter & df['Financial Account'].str.contains('Fund Expense', na=False)]['Ending Balance'].sum())
            metrics['stockloan_fees'] = abs(df[expense_filter & df['Financial Account'].str.contains('Stockloan Fees', na=False)]['Ending Balance'].sum())
            metrics['tax_preparation_fees'] = abs(df[expense_filter & df['Financial Account'].str.contains('Tax Preparation Fees', na=False)]['Ending Balance'].sum())

            total_expense_filter = expense_filter & ~mgmt_fee_filter & ~perf_fee_filter
            if expense_filter.any():
                metrics['total_expenses'] = abs(df[total_expense_filter]['Ending Balance'].sum())

            
            # Additional ratio-specific metrics
            # Current Assets: Custodians + AR + Other Assets
            custodians_filter = df['Category'].str.contains('Custodian', case=False, na=False)
            metrics['current_assets'] = df[custodians_filter | account_receivable_filter | other_assets_filter]['Ending Balance'].sum()
            
            # Current Liabilities: Account Payable + Other Liabilities
            metrics['current_liabilities'] = abs(df[(account_payable_filter | other_liabilities_filter)]['Ending Balance'].sum())
            
            # Cash and Equivalents: Custodians (also use as liquid_assets)
            metrics['cash_and_equivalents'] = df[custodians_filter]['Ending Balance'].sum()
            metrics['liquid_assets'] = metrics['cash_and_equivalents']  # Liquid assets = cash + equivalents
            
            # Subscription and Redemption Flows from trial balance
            # Subscriptions = Deposits in capital accounts
            subscription_filter = df['Accounting Head'].str.contains('Deposits', case=False, na=False)
            if subscription_filter.any():
                metrics['subscription_flows'] = abs(df[subscription_filter]['Ending Balance'].sum())
            else:
                metrics['subscription_flows'] = 0.0
                
            # Redemptions = Withdrawals only (NOT RedempPayable)
            withdrawal_filter = df['Accounting Head'].str.contains('Withdrawals', case=False, na=False)
            
            withdrawal_amount = abs(df[withdrawal_filter]['Ending Balance'].sum()) if withdrawal_filter.any() else 0.0
            metrics['redemption_flows'] = withdrawal_amount
            
            # Missing ratio-specific metrics (initialize to zero/fallback values)
            metrics['concentrated_assets_value'] = 0.0  # To be calculated from portfolio data
            metrics['top_holdings_value'] = 0.0  # To be calculated from portfolio data
            metrics['net_long_exposure'] = 0.0  # To be calculated from portfolio data
            metrics['total_portfolio_value'] = 0.0  # To be calculated from portfolio data
            metrics['sector_assets'] = 0.0  # To be calculated from portfolio data
            metrics['geographical_assets'] = 0.0  # To be calculated from portfolio data  
            metrics['illiquid_assets'] = 0.0  # To be calculated from portfolio data
            metrics['fund_return'] = 0.0  # To be calculated from performance data
            metrics['benchmark_return'] = 0.0  # To be calculated from benchmark data
            metrics['margin_requirements'] = 0.0  # To be calculated from trial balance margin entries
            metrics['potential_redemptions'] = metrics['nav']  # Default to NAV for redemption liquidity calculation
            
            # ========================
            # TRIAL BALANCE BASED CONCENTRATION METRICS
            # ========================
            
            # Investments (MV of Investments) - for Asset Concentration Ratio
            # Look for Investment or Fund categories in the trial balance
            investment_mv_filter = (
                df['Category'].str.contains('Investment|Fund|Security|Equity|Bond', case=False, na=False) |
                df['Accounting Head'].str.contains('Investment|Fund|Security', case=False, na=False)
            )
            if investment_mv_filter.any():
                metrics['investments'] = df[investment_mv_filter]['Ending Balance'].sum()
            
            # ========================
            # TRIAL BALANCE BASED SENTIMENT METRICS
            # ========================
            
            # Subscriptions/Redemptions - look for capital flow related entries
            subscription_filter = (
                df['Category'].str.contains('Capital', case=False, na=False) |
                df['Financial Account'].str.contains('Deposit', case=False, na=False)
            )
            redemption_filter = (
                df['Category'].str.contains('Capital', case=False, na=False) |
                df['Financial Account'].str.contains('Withdraw', case=False, na=False)
            )
            
            if subscription_filter.any():
                metrics['total_subscriptions'] = abs(df[subscription_filter]['Ending Balance'].sum())
                metrics['subscription_inflows'] = metrics['total_subscriptions']
            
            if redemption_filter.any():
                metrics['total_redemptions'] = abs(df[redemption_filter]['Ending Balance'].sum())
                metrics['redemption_outflows'] = metrics['total_redemptions']
            
            # Net Flows = Subscriptions - Redemptions
            metrics['net_flows'] = metrics['total_subscriptions'] - metrics['total_redemptions']
            
            # Long/Short Positions - will be calculated from portfolio data if available
            # Initialize to zero for trial balance only calculations
            metrics['net_long_positions'] = 0.0
            metrics['net_short_positions'] = 0.0
            metrics['swap_financing'] = df[df['Financial Account'].str.contains('Price Gain Loss on Swap', case=False, na=False)]['Ending Balance'].sum()
            metrics['interest_accruals'] = df[df['Financial Account'].str.contains('Interest Income Collateral', case=False, na=False)]['Ending Balance'].sum()
        # =====================================
        # PORTFOLIO VALUATION BASED METRICS
        # =====================================
        if portfolio_data:
            portfolio_df = pd.DataFrame(portfolio_data)
            
            # Total market value from portfolio data
            if 'End Local MV' in portfolio_df.columns:
                metrics['total_market_value'] = portfolio_df['End Local MV'].sum()
                metrics['investments'] = metrics['total_market_value']  # Override from portfolio data
                
                # Position count and average size
                metrics['total_positions'] = len(portfolio_df)
                metrics['average_position_size'] = metrics['total_market_value'] / metrics['total_positions'] if metrics['total_positions'] > 0 else 0
                
                # Largest single position
                metrics['largest_position_mv'] = portfolio_df['End Local MV'].max()
                metrics['single_asset_concentration'] = metrics['largest_position_mv'] / metrics['total_market_value'] if metrics['total_market_value'] > 0 else 0
                

                # Top 10 positions concentration as absolute values (negative or positive)
                # First make absolute values for endlocalmv in temporary df
                temp_df = portfolio_df[['End Local MV']].abs()
                metrics['top_holdings_value'] = temp_df['End Local MV'].nlargest(10).sum()
                metrics['top_5_positions_mv'] = temp_df['End Local MV'].nlargest(5).sum()

            # Total quantity
            if 'End Qty' in portfolio_df.columns:
                metrics['total_quantity'] = portfolio_df['End Qty'].sum()
            
            # Long/Short Positions calculation from portfolio data
            if 'End Qty' in portfolio_df.columns and 'End Book MV' in portfolio_df.columns:
                # Long positions: End Qty >= 0
                long_positions = portfolio_df[portfolio_df['End Qty'] >= 0]
                long_positions_value = abs(long_positions['End Book MV'].sum()) if len(long_positions) > 0 else 0.0
                
                # Short positions: End Qty < 0
                short_positions = portfolio_df[portfolio_df['End Qty'] < 0]
                short_positions_value = abs(short_positions['End Book MV'].sum()) if len(short_positions) > 0 else 0.0
                
                # Net long = abs(long) + abs(short)
                metrics['net_long_positions'] = long_positions_value + short_positions_value
                
                # Net short = abs(long) - abs(short)
                metrics['net_short_positions'] = long_positions_value - short_positions_value
                
                # Gross and Net Exposure
                metrics['gross_exposure'] = metrics['net_long_positions'] + abs(metrics['net_short_positions'])
                metrics['net_exposure'] = metrics['net_long_positions'] - abs(metrics['net_short_positions'])
            
            # Sector concentration (if sector data available)
            if 'Inv Type' in portfolio_df.columns:
                sector_groups = portfolio_df.groupby('Inv Type')['End Local MV'].sum()
                if len(sector_groups) > 0:
                    largest_sector = sector_groups.max()
                    metrics['sector_concentration'] = largest_sector / metrics['total_market_value'] if metrics['total_market_value'] > 0 else 0
        
        # =====================================
        # DIVIDEND BASED METRICS
        # =====================================
        if dividend_data:
            dividend_df = pd.DataFrame(dividend_data)
            
            if 'Amount' in dividend_df.columns:
                metrics['total_dividends_received'] = dividend_df['Amount'].sum()
                metrics['income_from_investments'] = metrics['total_dividends_received']
                
                # Dividend yield (dividends / total market value)
                if metrics['total_market_value'] > 0:
                    metrics['dividend_yield'] = metrics['total_dividends_received'] / metrics['total_market_value']
        
        # =====================================
        # DERIVED METRICS CALCULATIONS
        # =====================================
        
        metrics['liquid_assets'] = metrics['cash_and_equivalents']
        
        # Calculate concentrated assets value from trial balance data if available
        if trial_balance_data:
            df = pd.DataFrame(trial_balance_data)
            concentrated_filter = df['Category'].str.contains('Investment', case=False, na=False)
            if concentrated_filter.any():
                metrics['concentrated_assets_value'] = df[concentrated_filter]['Ending Balance'].sum()
            else:
                metrics['concentrated_assets_value'] = 0.0
        else:
            metrics['concentrated_assets_value'] = 0.0
        
        # Portfolio return calculation - calculate from NAV changes and benchmark data
        try:
            fund_return, benchmark_return = _calculate_returns_vs_benchmark(date, metrics['nav'])
            metrics['fund_return'] = fund_return
            metrics['portfolio_return'] = fund_return
            metrics['benchmark_return'] = benchmark_return
            # Excess Return over Benchmark = Fund Return - Benchmark Return
            metrics['excess_return'] = fund_return - benchmark_return
        except Exception as e:
            # Keep default values
            metrics['excess_return'] = metrics['fund_return'] - metrics['benchmark_return']
            
        
        return metrics
        
    except Exception as e:
        return metrics


def _calculate_returns_vs_benchmark(current_date: str, current_nav: float) -> tuple:
    """
    Calculate fund return and benchmark return for excess return calculation
    
    Args:
        current_date: Current date in YYYY-MM-DD format
        current_nav: Current NAV value
        
    Returns:
        tuple: (fund_return_percentage, benchmark_return_percentage)
    """
    try:
        from server.APIServerUtils.db_validation_service import db_validation_service
        
        # Parse the current date
        if not current_date:
            return 0.0, 0.0
            
        try:
            current_dt = datetime.strptime(current_date, '%Y-%m-%d')
        except ValueError:
            # Try other date formats
            try:
                current_dt = datetime.strptime(current_date, '%m-%d-%Y')
            except ValueError:
                print(f"Unable to parse date: {current_date}")
                return 0.0, 0.0
        
        # Calculate previous month date (use last day of previous month)
        # Go to first day of current month, then subtract 1 day to get last day of previous month
        first_of_current_month = current_dt.replace(day=1)
        prev_month_dt = first_of_current_month - relativedelta(days=1)
        prev_month_str = prev_month_dt.strftime('%Y-%m-%d')
        
        # Get previous month NAV (or use baseline for Dec 2023)
        if prev_month_dt.year == 2023 and prev_month_dt.month == 12:
            # Use the baseline NAV for December 2023
            prev_nav = 1909492081.14
        else:
            # Try to get actual NAV from previous month's database
            prev_nav = _get_previous_month_nav(prev_month_dt)
            if prev_nav is None:
                return 0.0, 0.0  # Return zero returns if previous NAV not available
        
        # Calculate fund return percentage
        if prev_nav > 0:
            fund_return = ((current_nav - prev_nav) / prev_nav) * 100
        else:
            fund_return = 0.0
        
        # Get benchmark data from database
        benchmark_return = _get_benchmark_return(current_dt, prev_month_dt)
        
        return fund_return, benchmark_return
        
    except Exception as e:
        print(f"Error in _calculate_returns_vs_benchmark: {e}")
        return 0.0, 0.0


def _get_previous_month_nav(prev_month_dt: datetime) -> float:
    """
    Retrieve NAV for the previous month from database
    Returns None if not found
    """
    try:
        from server.APIServerUtils.db_validation_service import DatabaseValidationService
        
        validation_service = DatabaseValidationService()
        
        # Format date for query
        prev_month_str = prev_month_dt.strftime('%Y-%m-%d')
        
        # Try to get NAV data for the previous month
        # Use the same fund as the validation system
        fund_name = 'NexBridge'  # Default fund name used in validations
        
        # Try actual source names from the system
        source_names = ['Bluefield', 'Harborview', 'ClearLedger', 'StratusGA', 'VeridexAS']
        
        for source_name in source_names:
            nav_value = validation_service.calculate_nav(fund_name, source_name, prev_month_str)
            
            if nav_value and nav_value > 0:
                return float(nav_value)
        
        return None
        
    except Exception as e:
        print(f"Error retrieving previous month NAV: {e}")
        return None


def _get_benchmark_return(current_dt: datetime, prev_month_dt: datetime) -> float:
    """
    Get S&P 500 benchmark return between two dates
    
    Args:
        current_dt: Current date datetime object
        prev_month_dt: Previous month datetime object
        
    Returns:
        float: Benchmark return percentage
    """
    try:
        from database_models import Benchmark, DatabaseManager
        
        db_manager = DatabaseManager()
        session = db_manager.get_session()
        
        # Format dates for database query
        current_date_str = current_dt.strftime('%Y-%m-%d')
        prev_date_str = prev_month_dt.strftime('%Y-%m-%d')
        
        # Get current benchmark value
        current_benchmark = session.query(Benchmark).filter(
            Benchmark.benchmark == 'S&P 500 Index',
            Benchmark.date == current_date_str
        ).first()
        
        # Get previous benchmark value
        prev_benchmark = session.query(Benchmark).filter(
            Benchmark.benchmark == 'S&P 500 Index',
            Benchmark.date == prev_date_str
        ).first()
        
        session.close()
        
        if current_benchmark and prev_benchmark:
            current_value = float(current_benchmark.value)
            prev_value = float(prev_benchmark.value)
            
            if prev_value > 0:
                benchmark_return = ((current_value - prev_value) / prev_value) * 100
                return benchmark_return
        
        # Fallback: calculate based on known S&P 500 data
        return _get_fallback_benchmark_return(current_dt, prev_month_dt)
        
    except Exception as e:
        print(f"Error getting benchmark return from database: {e}")
        return _get_fallback_benchmark_return(current_dt, prev_month_dt)


def _get_fallback_benchmark_return(current_dt: datetime, prev_month_dt: datetime) -> float:
    """
    Fallback calculation using hardcoded S&P 500 data
    
    Args:
        current_dt: Current date datetime object  
        prev_month_dt: Previous month datetime object
        
    Returns:
        float: Benchmark return percentage
    """
    # S&P 500 Index values (from database_seeder.py)
    sp500_data = {
        '2023-12-31': 4769.83,
        '2024-01-31': 4845.65,
        '2024-02-29': 5096.27,
        '2024-03-31': 5254.35,
        '2024-04-30': 5035.69,
        '2024-05-31': 5277.51,
        '2024-06-30': 5460.48,
        '2024-07-31': 5522.30
    }
    
    current_date_str = current_dt.strftime('%Y-%m-%d')
    prev_date_str = prev_month_dt.strftime('%Y-%m-%d')
    
    # Handle end-of-month dates
    if current_dt.day > 28:
        # Try last day of month
        last_day = current_dt.replace(day=28) + relativedelta(days=4)
        last_day = last_day - relativedelta(days=last_day.day)
        current_date_str = last_day.strftime('%Y-%m-%d')
    
    if prev_month_dt.day > 28:
        # Try last day of previous month
        last_day = prev_month_dt.replace(day=28) + relativedelta(days=4)
        last_day = last_day - relativedelta(days=last_day.day)
        prev_date_str = last_day.strftime('%Y-%m-%d')
    
    current_value = sp500_data.get(current_date_str)
    prev_value = sp500_data.get(prev_date_str)
    
    if current_value and prev_value:
        benchmark_return = ((current_value - prev_value) / prev_value) * 100
        return benchmark_return
    
    return 0.0


def get_expense_patterns() -> Dict[str, str]:
    """
    Get expense search patterns used by KPI validations to match financial accounts
    
    Returns:
        Dictionary mapping KPI numerator fields to Financial Account search patterns
    """
    return {
        'legal_fees': 'Legal Expense',
        'admin_fees': 'Admin',
        'interest_expense': 'Interest Expense',
        'accounting_expenses': 'Accounting Expense',
        'allocation_fee': 'Allocation Fee',
        'audit_expense': 'Audit Expense',
        'bank_fees': 'Bank Fees',
        'borrow_fee_estimate': 'BorrowFeeEstimate',
        'borrow_fee_expense': 'BorrowFeeExpense',
        'distribution_fee_expense': 'DistributionFeeExpense',
        'fs_prep_fees': 'FSPrepFees',
        'fund_expense': 'Fund Expense',
        'stockloan_fees': 'Stockloan Fees',
        'tax_preparation_fees': 'Tax Preparation Fees'
    }


def get_fee_patterns() -> Dict[str, str]:
    """
    Get fee search patterns used by KPI validations to match financial accounts
    
    Returns:
        Dictionary mapping KPI numerator fields to Financial Account search patterns
    """
    return {
        'management_fees': 'MgmtFee',  # Match the pattern used in existing working validations
    }
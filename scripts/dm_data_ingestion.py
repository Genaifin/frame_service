#!/usr/bin/env python3
"""
Simple DM Data Ingestion Script
Populates nexbridge.dm_trial_balance, dm_portfolio_valuation_by_instrument, and dm_dividend_income_expense
from Excel files in the l0 folder
"""

import os
import sys
import pandas as pd
import re
import json
from datetime import datetime, timedelta
from pathlib import Path
from sqlalchemy import text

# Add the project root to the path
project_root = Path(__file__).parent.parent
sys.path.append(str(project_root))

from database_models import DatabaseManager, Client, Fund, DataLoadInstance

class DMDataIngestion:
    def __init__(self, data_folder_path=None):
        """Initialize the data ingestion class"""
        default_data_path = project_root / "data" / "validusDemo" / "l0"
        self.data_folder = data_folder_path or str(default_data_path)
        self.db_manager = DatabaseManager()
        
        # Default values
        self.client_name = "Brightstone Holdings Ltd."
        self.fund_name = "NexBridge"
        self.base_currency = "USD"
        
    def get_client_id(self, session, client_name):
        """Get client ID from client name"""
        client = session.query(Client).filter(Client.name.ilike(f'%{client_name}%')).first()
        if client:
            return client.id
        # If not found, return None or create default
        print(f"Warning: Client '{client_name}' not found. Using default client_id=2")
        return 2  # Default based on example
    
    def get_fund_id(self, session, fund_name):
        """Get fund ID from fund name"""
        fund = session.query(Fund).filter(Fund.name.ilike(f'%{fund_name}%')).first()
        if fund:
            return fund.id
        print(f"Warning: Fund '{fund_name}' not found. Using default fund_id=1")
        return 1  # Default for NexBridge
    
    def extract_date_from_filename(self, filename):
        """Extract date from filename like 'Jan 2024' and return start/end dates of month"""
        match = re.search(r'([A-Za-z]{3})\s+(\d{4})', filename)
        if match:
            month_str, year_str = match.groups()
            month_map = {
                'Jan': 1, 'Feb': 2, 'Mar': 3, 'Apr': 4, 'May': 5, 'Jun': 6,
                'Jul': 7, 'Aug': 8, 'Sep': 9, 'Oct': 10, 'Nov': 11, 'Dec': 12
            }
            month = month_map.get(month_str, 1)
            year = int(year_str)
            
            # Start date is first day of month
            start_date = datetime(year, month, 1).date()
            
            # End date is last day of month
            if month == 12:
                end_date = datetime(year + 1, 1, 1) - timedelta(days=1)
            else:
                end_date = datetime(year, month + 1, 1) - timedelta(days=1)
            
            return start_date, end_date.date()
        return None, None
    
    def extract_source_info_from_filename(self, filename):
        """Extract source type and name from filename"""
        filename_lower = filename.lower()
        
        # Determine source type (Admin/Shadow)
        source_type = "Admin"
        if 'shadow' in filename_lower:
            source_type = "Shadow"
        
        # Extract source name from filename
        source_name = None
        source_keywords = ['bluefield', 'harborview', 'clearledger', 'stratusga', 'veridex']
        for keyword in source_keywords:
            if keyword in filename_lower:
                source_name = keyword.title()
                if keyword == 'veridex':
                    source_name = 'VeridexAS'
                break
        
        if not source_name:
            # Try to extract from common patterns
            if 'admin' in filename_lower:
                # Look for text after "Admin" or "Admin2"
                match = re.search(r'admin\d*\s+([a-z]+)', filename_lower)
                if match:
                    source_name = match.group(1).title()
                else:
                    source_name = "Unknown"
        
        return source_type, source_name or "Unknown"
    
    def create_data_load_instance(self, session, client_id, fund_id, data_model_id, asof_date, source_type, source_name):
        """Create a data load instance record"""
        data_load = DataLoadInstance(
            intclientid=client_id,
            intfundid=fund_id,
            vccurrency=self.base_currency,
            intdatamodelid=data_model_id,
            dtdataasof=asof_date,
            vcdatasourcetype=source_type,
            vcdatasourcename=source_name,
            vcloadtype="Script",
            vcloadstatus="Processing"  # Will be updated to Success/Failed/No Data
        )
        session.add(data_load)
        session.flush()
        return data_load.intdataloadinstanceid
    
    def process_trial_balance(self, session, file_path, data_load_instance_id, client_id, fund_id, 
                             start_date, end_date, asof_date):
        """Process Trial Balance sheet and insert into dm_trial_balance"""
        try:
            # Read the trial balance sheet
            df = pd.read_excel(file_path, sheet_name='Trial Balance', header=None)
            
            # Find header row (usually around row 10)
            header_row = None
            for i in range(min(20, len(df))):
                row_values = [str(cell).lower() if pd.notna(cell) else '' for cell in df.iloc[i].values]
                if 'type' in ' '.join(row_values) and 'financial account' in ' '.join(row_values):
                    header_row = i
                    break
            
            if header_row is None:
                print("Could not find Trial Balance header row")
                return 0
            
            # Re-read with correct header
            df = pd.read_excel(file_path, sheet_name='Trial Balance', header=header_row)
            df.columns = [str(col).strip() for col in df.columns]
            df = df.dropna(how='all')
            
            print(f"Trial Balance columns: {df.columns.tolist()}")
            
            count = 0
            for _, row in df.iterrows():
                # Skip rows with missing Type or contains disclaimer rows or 2024-04-01 00:00:00
                if pd.isna(row.get('Type')) or row.get('Type') in ['Disclaimer', '2024-04-01 00:00:00']:
                    continue
                
                type_value = str(row.get('Type', '')).strip()
                if len(type_value) > 500:  # Skip disclaimer rows
                    continue
                
                # Map columns
                accounttype = type_value[:255] if len(type_value) > 255 else type_value
                subtype = None if pd.isna(row.get('Category')) else str(row.get('Category', ''))[:255]
                accountname = None if pd.isna(row.get('Accounting Head')) else str(row.get('Accounting Head', ''))[:255]
                accountdescription = None
                
                # Get financial account (check for GL Account first for Stonewell/Veridex)
                financial_account = None
                for col in df.columns:
                    col_lower = str(col).lower().strip()
                    if 'gl account' in col_lower:
                        financial_account = row.get(col)
                        break
                
                if not financial_account or pd.isna(financial_account):
                    financial_account = row.get('Financial Account', '')
                
                if not pd.isna(financial_account):
                    accountdescription = str(financial_account).strip()[:255]
                
                # Get balances
                closingbalance = None
                if not pd.isna(row.get('Ending Balance')):
                    try:
                        closingbalance = float(row.get('Ending Balance'))
                    except (ValueError, TypeError):
                        pass
                
                openingbalance = None
                activity = None
                subdescription = None
                
                # Search for beginning balance and activity columns
                for col_name in df.columns:
                    col_lower = str(col_name).lower().strip()
                    if 'beginning' in col_lower and 'balance' in col_lower:
                        val = row.get(col_name)
                        if not pd.isna(val):
                            try:
                                openingbalance = float(val)
                            except (ValueError, TypeError):
                                pass
                    elif 'activity' in col_lower:
                        val = row.get(col_name)
                        if not pd.isna(val):
                            try:
                                activity = float(val)
                            except (ValueError, TypeError):
                                pass
                
                # Get Description for subdescription
                description = row.get('Description')
                if not pd.isna(description):
                    subdescription = str(description).strip()[:255]
                
                # Insert into dm_trial_balance
                insert_query = text("""
                    INSERT INTO nexbridge.dm_trial_balance 
                    (intdataloadinstanceid, clientnameid, fundnameid, reportingstartdate, reportingenddate,
                     asofdate, basecurrency, accounttype, subtype, accountname, accountdescription,
                     subdescription, openingbalance, periodactivity, closingbalance)
                    VALUES (:intdataloadinstanceid, :clientnameid, :fundnameid, :reportingstartdate, :reportingenddate,
                            :asofdate, :basecurrency, :accounttype, :subtype, :accountname, :accountdescription,
                            :subdescription, :openingbalance, :periodactivity, :closingbalance)
                """)
                
                session.execute(insert_query, {
                    'intdataloadinstanceid': data_load_instance_id,
                    'clientnameid': client_id,
                    'fundnameid': fund_id,
                    'reportingstartdate': start_date,
                    'reportingenddate': end_date,
                    'asofdate': asof_date,
                    'basecurrency': self.base_currency,
                    'accounttype': accounttype,
                    'subtype': subtype,
                    'accountname': accountname,
                    'accountdescription': accountdescription,
                    'subdescription': subdescription,
                    'openingbalance': openingbalance,
                    'periodactivity': activity,
                    'closingbalance': closingbalance
                })
                count += 1
            
            print(f"Processed {count} trial balance records")
            return count
            
        except Exception as e:
            print(f"Error processing trial balance: {e}")
            import traceback
            traceback.print_exc()
            return 0
    
    def process_portfolio_valuation(self, session, file_path, data_load_instance_id, client_id, fund_id,
                                   start_date, end_date, asof_date):
        """Process Portfolio Valuation sheet and insert into dm_portfolio_valuation_by_instrument"""
        try:
            # Read the portfolio valuation sheet
            df = pd.read_excel(file_path, sheet_name='Portfolio Valuation By Instrum', header=None)
            
            # Find header row
            header_row = None
            for i in range(min(20, len(df))):
                row_values = [str(cell).lower() if pd.notna(cell) else '' for cell in df.iloc[i].values]
                if 'inv type' in ' '.join(row_values) or 'investment type' in ' '.join(row_values):
                    header_row = i
                    break
            
            if header_row is None:
                print("Could not find Portfolio Valuation header row")
                return 0
            
            # Re-read with correct header
            df = pd.read_excel(file_path, sheet_name='Portfolio Valuation By Instrum', header=header_row)
            df.columns = [str(col).strip() for col in df.columns]
            df = df.dropna(how='all')
            
            print(f"Portfolio Valuation columns: {df.columns.tolist()}")
            
            # Map column names (flexible matching)
            column_map = {}
            for col in df.columns:
                col_lower = col.lower().strip()
                if 'inv type' in col_lower or 'investment type' in col_lower:
                    column_map['inv_type'] = col
                elif 'inv id' in col_lower or 'investment id' in col_lower:
                    column_map['inv_id'] = col
                elif 'inv desc' in col_lower or 'investment desc' in col_lower:
                    column_map['inv_desc'] = col
                elif 'inv ccy' in col_lower or 'investment currency' in col_lower:
                    column_map['inv_ccy'] = col
                elif 'end qty' in col_lower or 'ending quantity' in col_lower:
                    column_map['end_qty'] = col
                elif 'end local market price' in col_lower or 'ending local price' in col_lower:
                    column_map['end_local_price'] = col
                elif 'end local cost' in col_lower or 'ending local cost' in col_lower:
                    column_map['end_local_cost'] = col
                elif 'end book cost' in col_lower or 'ending book cost' in col_lower:
                    column_map['end_book_cost'] = col
                elif 'end local mv' in col_lower or 'ending local market value' in col_lower:
                    column_map['end_local_mv'] = col
                elif 'end book mv' in col_lower or 'ending book market value' in col_lower:
                    column_map['end_book_mv'] = col
                elif 'end book unreal mtm' in col_lower or 'ending book unrealized mark to market' in col_lower:
                    column_map['end_book_unreal_mtm'] = col
                elif 'end book unreal fxgl' in col_lower or 'ending book unrealized fx gain loss' in col_lower:
                    column_map['end_book_unreal_fxgl'] = col
                elif 'end book unreal income' in col_lower or 'ending book unrealized income' in col_lower:
                    column_map['end_book_unreal_income'] = col
                elif 'end book nav' in col_lower or 'ending book nav' in col_lower:
                    column_map['end_book_nav'] = col
                elif '% of total nav' in col_lower or 'percentage of total nav' in col_lower:
                    column_map['pct_total_nav'] = col
            
            count = 0
            for _, row in df.iterrows():
                # Skip rows with missing essential data
                inv_type_col = column_map.get('inv_type')
                inv_id_col = column_map.get('inv_id')
                end_qty_col = column_map.get('end_qty')
                
                if not inv_type_col or not inv_id_col or not end_qty_col:
                    continue
                
                if pd.isna(row.get(inv_type_col)) or pd.isna(row.get(inv_id_col)):
                    continue
                
                # Get values with null handling
                def get_float_value(col_key):
                    col = column_map.get(col_key)
                    if col and not pd.isna(row.get(col)):
                        try:
                            return float(row.get(col))
                        except (ValueError, TypeError):
                            pass
                    return None
                
                def get_string_value(col_key, max_len=255):
                    col = column_map.get(col_key)
                    if col and not pd.isna(row.get(col)):
                        return str(row.get(col)).strip()[:max_len]
                    return None
                
                investmenttype = get_string_value('inv_type')
                investmentidentifier = get_string_value('inv_id')
                investmentdescription = get_string_value('inv_desc')
                investmentcurrency = get_string_value('inv_ccy') or self.base_currency
                endingquantity = get_float_value('end_qty')
                endinglocalprice = get_float_value('end_local_price')
                endinglocalcost = get_float_value('end_local_cost')
                endingbookcost = get_float_value('end_book_cost')
                endinglocalmarketvalue = get_float_value('end_local_mv')
                endingbookmarketvalue = get_float_value('end_book_mv')
                endingbookunrealizedmarktomarketpl = get_float_value('end_book_unreal_mtm')
                endingbookunrelaizedfxgainloss = get_float_value('end_book_unreal_fxgl')
                endingbookunrealizedincome = get_float_value('end_book_unreal_income')
                endbooknav = get_float_value('end_book_nav')
                percentageoftotalnav = get_float_value('pct_total_nav')
                
                if endingquantity is None:
                    continue
                
                # Insert into dm_portfolio_valuation_by_instrument
                insert_query = text("""
                    INSERT INTO nexbridge.dm_portfolio_valuation_by_instrument
                    (intdataloadinstanceid, clientnameid, fundnameid, reportingstartdate, reportingenddate,
                     asofdate, basecurrency, investmenttype, investmentidentifier, investmentdescription,
                     investmentcurrency, endingquantity, endinglocalprice, endinglocalcost, endingbookcost,
                     endinglocalmarketvalue, endingbookmarketvalue, endingbookunrealizedmarktomarketpl,
                     endingbookunrelaizedfxgainloss, endingbookunrealizedincome, endbooknav, percentageoftotalnav)
                    VALUES (:intdataloadinstanceid, :clientnameid, :fundnameid, :reportingstartdate, :reportingenddate,
                            :asofdate, :basecurrency, :investmenttype, :investmentidentifier, :investmentdescription,
                            :investmentcurrency, :endingquantity, :endinglocalprice, :endinglocalcost, :endingbookcost,
                            :endinglocalmarketvalue, :endingbookmarketvalue, :endingbookunrealizedmarktomarketpl,
                            :endingbookunrelaizedfxgainloss, :endingbookunrealizedincome, :endbooknav, :percentageoftotalnav)
                """)
                
                session.execute(insert_query, {
                    'intdataloadinstanceid': data_load_instance_id,
                    'clientnameid': client_id,
                    'fundnameid': fund_id,
                    'reportingstartdate': start_date,
                    'reportingenddate': end_date,
                    'asofdate': asof_date,
                    'basecurrency': self.base_currency,
                    'investmenttype': investmenttype,
                    'investmentidentifier': investmentidentifier,
                    'investmentdescription': investmentdescription,
                    'investmentcurrency': investmentcurrency,
                    'endingquantity': endingquantity,
                    'endinglocalprice': endinglocalprice,
                    'endinglocalcost': endinglocalcost,
                    'endingbookcost': endingbookcost,
                    'endinglocalmarketvalue': endinglocalmarketvalue,
                    'endingbookmarketvalue': endingbookmarketvalue,
                    'endingbookunrealizedmarktomarketpl': endingbookunrealizedmarktomarketpl,
                    'endingbookunrelaizedfxgainloss': endingbookunrelaizedfxgainloss,
                    'endingbookunrealizedincome': endingbookunrealizedincome,
                    'endbooknav': endbooknav,
                    'percentageoftotalnav': percentageoftotalnav
                })
                count += 1
            
            print(f"Processed {count} portfolio valuation records")
            return count
            
        except Exception as e:
            print(f"Error processing portfolio valuation: {e}")
            import traceback
            traceback.print_exc()
            return 0
    
    def process_dividend(self, session, file_path, data_load_instance_id, client_id, fund_id,
                        start_date, end_date, asof_date):
        """Process Dividend sheet and insert into dm_dividend_income_expense"""
        try:
            # Check if Dividend sheet exists
            xl = pd.ExcelFile(file_path)
            if 'Dividend' not in xl.sheet_names:
                print("Dividend sheet not found")
                return 0
            
            # Read the dividend sheet
            df = pd.read_excel(file_path, sheet_name='Dividend', header=None)
            
            # Find header row
            header_row = None
            for i in range(min(30, len(df))):
                row_values = [str(cell).lower() if pd.notna(cell) else '' for cell in df.iloc[i].values]
                if 'security' in ' '.join(row_values) and 'amount' in ' '.join(row_values):
                    header_row = i
                    break
            
            if header_row is None:
                print("Could not find Dividend header row")
                return 0
            
            # Re-read with correct header
            df = pd.read_excel(file_path, sheet_name='Dividend', header=header_row)
            df.columns = [str(col).strip() for col in df.columns]
            df = df.dropna(how='all')
            
            print(f"Dividend columns: {df.columns.tolist()}")
            
            # Map column names
            column_map = {}
            for col in df.columns:
                col_lower = col.lower().strip()
                if 'security name' in col_lower:
                    column_map['security_name'] = col
                elif 'security id' in col_lower:
                    column_map['security_id'] = col
                elif 'settle date' in col_lower or 'settlement date' in col_lower:
                    column_map['settle_date'] = col
                elif 'currency' in col_lower:
                    column_map['currency'] = col
                elif 'transaction type' in col_lower:
                    column_map['transaction_type'] = col
                elif 'amount' in col_lower and 'unit' not in col_lower:
                    column_map['amount'] = col
                elif 'units' in col_lower:
                    column_map['units'] = col
                elif 'ex-date' in col_lower or 'ex date' in col_lower:
                    column_map['ex_date'] = col
                elif 'pay date' in col_lower or 'payment date' in col_lower:
                    column_map['pay_date'] = col
                elif 'counterparty' in col_lower:
                    column_map['counterparty'] = col
            
            count = 0
            for _, row in df.iterrows():
                # Get values
                security_name = None
                security_id = None
                amount = None
                
                if 'security_name' in column_map:
                    val = row.get(column_map['security_name'])
                    if not pd.isna(val):
                        security_name = str(val).strip()[:255]
                
                if 'security_id' in column_map:
                    val = row.get(column_map['security_id'])
                    if not pd.isna(val):
                        security_id = str(val).strip()[:255]
                
                if 'amount' in column_map:
                    val = row.get(column_map['amount'])
                    if not pd.isna(val):
                        try:
                            amount = float(val)
                        except (ValueError, TypeError):
                            pass
                
                if not security_id or not amount or amount == 0:
                    continue
                
                # Get other optional fields
                def get_date_value(col_key):
                    col = column_map.get(col_key)
                    if col and not pd.isna(row.get(col)):
                        val = row.get(col)
                        if isinstance(val, datetime):
                            return val.date()
                        elif isinstance(val, str):
                            try:
                                return datetime.strptime(val, '%Y-%m-%d').date()
                            except:
                                pass
                    return None
                
                def get_string_value(col_key, max_len=255):
                    col = column_map.get(col_key)
                    if col and not pd.isna(row.get(col)):
                        return str(row.get(col)).strip()[:max_len]
                    return None
                
                def get_float_value(col_key):
                    col = column_map.get(col_key)
                    if col and not pd.isna(row.get(col)):
                        try:
                            return float(row.get(col))
                        except (ValueError, TypeError):
                            pass
                    return None
                
                previousasofdate = get_date_value('settle_date')
                paymentcurrency = get_string_value('currency') or self.base_currency
                dividendtype = get_string_value('transaction_type')
                grossamountpershareunit = get_float_value('units')
                exdividenddate = get_date_value('ex_date')
                # exdatequantity is a DATE column, not a quantity - use ex_date if available, otherwise NULL
                exdatequantity = exdividenddate  # Use ex-dividend date for exdatequantity (date type)
                paymentdate = get_date_value('pay_date')
                grossbookdividendincome = amount  # Same as grosslocaldividendincome
                
                # Insert into dm_dividend_income_expense
                insert_query = text("""
                    INSERT INTO nexbridge.dm_dividend_income_expense
                    (intdataloadinstanceid, clientnameid, fundnameid, reportingstartdate, reportingenddate,
                     asofdate, previousasofdate, basecurrency, investmentidentifier, securityname,
                     paymentcurrency, dividendtype, grossamountpershareunit, exdividenddate, exdatequantity,
                     paymentdate, grosslocaldividendincome, grossbookdividendincome)
                    VALUES (:intdataloadinstanceid, :clientnameid, :fundnameid, :reportingstartdate, :reportingenddate,
                            :asofdate, :previousasofdate, :basecurrency, :investmentidentifier, :securityname,
                            :paymentcurrency, :dividendtype, :grossamountpershareunit, :exdividenddate, :exdatequantity,
                            :paymentdate, :grosslocaldividendincome, :grossbookdividendincome)
                """)
                
                session.execute(insert_query, {
                    'intdataloadinstanceid': data_load_instance_id,
                    'clientnameid': client_id,
                    'fundnameid': fund_id,
                    'reportingstartdate': start_date,
                    'reportingenddate': end_date,
                    'asofdate': asof_date,
                    'previousasofdate': previousasofdate,
                    'basecurrency': self.base_currency,
                    'investmentidentifier': security_id,
                    'securityname': security_name or security_id,
                    'paymentcurrency': paymentcurrency,
                    'dividendtype': dividendtype,
                    'grossamountpershareunit': grossamountpershareunit,
                    'exdividenddate': exdividenddate,
                    'exdatequantity': exdatequantity,
                    'paymentdate': paymentdate,
                    'grosslocaldividendincome': amount,
                    'grossbookdividendincome': grossbookdividendincome
                })
                count += 1
            
            print(f"Processed {count} dividend records")
            return count
            
        except Exception as e:
            print(f"Error processing dividend: {e}")
            import traceback
            traceback.print_exc()
            return 0
    
    def process_file(self, file_path):
        """Process a single Excel file"""
        filename = os.path.basename(file_path)
        print(f"\nProcessing file: {filename}")
        
        # Extract dates from filename
        start_date, end_date = self.extract_date_from_filename(filename)
        if not start_date or not end_date:
            print(f"Could not extract date from filename: {filename}")
            return False
        
        asof_date = end_date  # Same as reportingenddate
        
        # Extract source info
        source_type, source_name = self.extract_source_info_from_filename(filename)
        
        session = self.db_manager.get_session()
        try:
            # Get client and fund IDs
            client_id = self.get_client_id(session, self.client_name)
            fund_id = self.get_fund_id(session, self.fund_name)
            
            # Process each data model
            data_models = [
                (1, 'Trial Balance', self.process_trial_balance),
                (2, 'Portfolio Valuation', self.process_portfolio_valuation),
                (7, 'Dividend', self.process_dividend)
            ]
            
            total_records = 0
            for data_model_id, model_name, process_func in data_models:
                # Process each data model in its own transaction
                data_load_instance_id = None
                try:
                    # Create data load instance
                    data_load_instance_id = self.create_data_load_instance(
                        session, client_id, fund_id, data_model_id, asof_date, source_type, source_name
                    )
                    session.flush()  # Ensure we have the ID
                    
                    # Process the sheet
                    count = process_func(session, file_path, data_load_instance_id, client_id, fund_id,
                                       start_date, end_date, asof_date)
                    total_records += count
                    
                    if count > 0:
                        print(f"✓ {model_name}: {count} records")
                        # Update status to Success
                        session.query(DataLoadInstance).filter(
                            DataLoadInstance.intdataloadinstanceid == data_load_instance_id
                        ).update({'vcloadstatus': 'Success'})
                        session.flush()
                    else:
                        # Update status to indicate no data found
                        session.query(DataLoadInstance).filter(
                            DataLoadInstance.intdataloadinstanceid == data_load_instance_id
                        ).update({'vcloadstatus': 'No Data'})
                        session.flush()
                    
                    # Commit this data model's transaction
                    session.commit()
                
                except Exception as e:
                    print(f"Error processing {model_name}: {e}")
                    import traceback
                    traceback.print_exc()
                    # Rollback this data model's transaction
                    try:
                        session.rollback()
                        # Create a failed status record
                        if data_load_instance_id:
                            try:
                                # Try to update existing record
                                session.query(DataLoadInstance).filter(
                                    DataLoadInstance.intdataloadinstanceid == data_load_instance_id
                                ).update({'vcloadstatus': 'Failed'})
                                session.commit()
                            except:
                                session.rollback()
                                # If update fails, create new record
                                try:
                                    failed_instance = DataLoadInstance(
                                        intclientid=client_id,
                                        intfundid=fund_id,
                                        vccurrency=self.base_currency,
                                        intdatamodelid=data_model_id,
                                        dtdataasof=asof_date,
                                        vcdatasourcetype=source_type,
                                        vcdatasourcename=source_name,
                                        vcloadtype="Script",
                                        vcloadstatus="Failed"
                                    )
                                    session.add(failed_instance)
                                    session.commit()
                                except:
                                    session.rollback()
                        else:
                            # Create new failed record
                            try:
                                failed_instance = DataLoadInstance(
                                    intclientid=client_id,
                                    intfundid=fund_id,
                                    vccurrency=self.base_currency,
                                    intdatamodelid=data_model_id,
                                    dtdataasof=asof_date,
                                    vcdatasourcetype=source_type,
                                    vcdatasourcename=source_name,
                                    vcloadtype="Script",
                                    vcloadstatus="Failed"
                                )
                                session.add(failed_instance)
                                session.commit()
                            except:
                                session.rollback()
                    except Exception as inner_e:
                        print(f"Error handling failed status: {inner_e}")
                        session.rollback()
            
            print(f"Successfully processed file with {total_records} total records")
            return True
            
        except Exception as e:
            try:
                session.rollback()
            except:
                pass
            print(f"Error processing file: {e}")
            import traceback
            traceback.print_exc()
            return False
        finally:
            try:
                session.close()
            except:
                pass
    
    def run_ingestion(self):
        """Run the complete data ingestion process"""
        print(f"Starting DM Data Ingestion...")
        print(f"Data folder: {self.data_folder}")
        
        if not os.path.exists(self.data_folder):
            print(f"Data folder does not exist: {self.data_folder}")
            return
        
        # Find all Excel files
        excel_files = [
            os.path.join(self.data_folder, f)
            for f in os.listdir(self.data_folder)
            if f.endswith('.xlsx') and not f.startswith('~$')
        ]
        
        if not excel_files:
            print("No Excel files found in data folder")
            return
        
        print(f"Found {len(excel_files)} Excel files")
        
        successful = 0
        failed = 0
        
        for file_path in sorted(excel_files):
            try:
                if self.process_file(file_path):
                    successful += 1
                else:
                    failed += 1
            except Exception as e:
                print(f"Unexpected error processing {file_path}: {e}")
                failed += 1
        
        print(f"\n=== Ingestion Summary ===")
        print(f"Successfully processed: {successful} files")
        print(f"Failed to process: {failed} files")

def main():
    """Main function"""
    import argparse
    
    parser = argparse.ArgumentParser(description='DM Data Ingestion Script')
    parser.add_argument('--data-folder', type=str, 
                       help='Custom data folder path (default: data/validusDemo/l0)')
    
    args = parser.parse_args()
    
    ingestion = DMDataIngestion(data_folder_path=args.data_folder)
    ingestion.run_ingestion()

if __name__ == "__main__":
    main()


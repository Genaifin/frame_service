#!/usr/bin/env python3
"""
NexBridge Data Ingestion Script
Populates the nexbridge database tables from Excel files based on NexBridge.json configuration

This script uses relative paths to the project root, making it portable across different environments.
Default data folder: <project_root>/data/validusDemo/l0
"""

import os
import sys
import pandas as pd
import re
import json
from datetime import datetime, timedelta
from pathlib import Path

# Add the project root to the path to import our database models
project_root = Path(__file__).parent.parent
sys.path.append(str(project_root))

from database_models import DatabaseManager, Source, NavPack, NavPackVersion, TrialBalance, PortfolioValuation, Dividend

class NexBridgeDataIngestion:
    def __init__(self, data_folder_path=None, fund_config=None):
        """Initialize the data ingestion class with configurable fund settings"""
        # Use relative path to project root
        default_data_path = project_root / "data" / "validusDemo" / "l0"
        self.data_folder = data_folder_path or str(default_data_path)
        self.db_manager = DatabaseManager()
        
        # Fund configuration system
        self.fund_configs = self._get_default_fund_configs()
        
        # Set current fund configuration
        if fund_config:
            self._set_fund_config(fund_config)
        else:
            # Default to NexBridge
            self._set_fund_config('NexBridge')

    def _get_default_fund_configs(self):
        """Get default configurations for all supported funds"""
        return {
            'NexBridge': {
                'fund_id': 1,
                'fund_name': 'NexBridge',
                'source_name': 'Bluefield',
                'file_identifier': 'NexBridge Global Ventures NAV PACK - Admin Bluefield Investor Services',
                'date_format': 'MMM YYYY',
                'sheets': {
                    'trial_balance': {
                        'sheet_name': 'Trial Balance',
                        'skip_rows': 10,
                        'filters': {'exclude_capital': False}
                    },
                    'portfolio_valuation': {
                        'sheet_name': 'Portfolio Valuation By Instrum',
                        'skip_rows': 10,
                        'filters': {'exclude_cash': False}
                    },
                    'dividend': {
                        'sheet_name': 'Dividend',
                        'skip_rows': 10
                    },
                    'detailed_general_ledger': {
                        'sheet_name': 'Detail General Ledger',
                        'skip_rows': 0,
                        'required_columns': ['GLAccountDesc', 'Tran Description', 'Local Amount']
                    }
                }
            },
            'ASOF': {
                'fund_id': 2,
                'fund_name': 'ASOF',
                'source_name': 'Harborview',  # Default source, can be overridden
                'file_identifier': 'Altura Strategic Opportunities',  # More flexible matching
                'date_format': 'MMM YYYY',
                'sheets': {
                    'trial_balance': {
                        'sheet_name': 'Trial Balance',
                        'skip_rows': 10,
                        'filters': {'exclude_capital': False}
                    },
                    'portfolio_valuation': {
                        'sheet_name': 'Portfolio Valuation By Instrum',
                        'skip_rows': 10,
                        'filters': {'exclude_cash': False}
                    },
                    'dividend': {
                        'sheet_name': 'Dividend',
                        'skip_rows': 17
                    },
                    'detailed_general_ledger': {
                        'sheet_name': 'Detail General Ledger',
                        'skip_rows': 0,
                        'required_columns': ['GLAccountDesc', 'Tran Description', 'Local Amount']
                    }
                }
            },
            'Stonewell': {
                'fund_id': 3,
                'fund_name': 'Stonewell',
                'source_name': 'StratusGA',  # Default source, can be overridden
                'file_identifier': 'Stonewell Diversified',  # More flexible matching
                'date_format': 'MMM YYYY',
                'sheets': {
                    'trial_balance': {
                        'sheet_name': 'Trial Balance',
                        'skip_rows': 10,
                        'filters': {'exclude_capital': False}
                    },
                    'portfolio_valuation': {
                        'sheet_name': 'Portfolio Valuation By Instrum',
                        'skip_rows': 10,
                        'filters': {'exclude_cash': False}
                    },
                    'dividend': {
                        'sheet_name': 'Dividend',
                        'skip_rows': 17
                    },
                    'detailed_general_ledger': {
                        'sheet_name': 'Detail General Ledger',
                        'skip_rows': 0,
                        'required_columns': ['GLAccountDesc', 'Tran Description', 'Local Amount']
                    }
                }
            }
        }

    def _set_fund_config(self, fund_key):
        """Set the current fund configuration"""
        if fund_key not in self.fund_configs:
            raise ValueError(f"Fund configuration '{fund_key}' not found. Available: {list(self.fund_configs.keys())}")
        
        current_config = self.fund_configs[fund_key]
        self.fund_id = current_config['fund_id']
        self.fund_name = current_config['fund_name']
        self.source_name = current_config['source_name']
        self.config = {
            'file_identifier': current_config['file_identifier'],
            'date_format': current_config['date_format'],
            'sheets': current_config['sheets']
        }
        print(f"Set configuration for fund: {fund_key} (ID: {self.fund_id}, Source: {self.source_name})")

    def set_source_override(self, source_name):
        """Override the default source for current fund"""
        self.source_name = source_name
        print(f"Source overridden to: {source_name}")

    def get_available_funds(self):
        """Get list of available fund configurations"""
        return list(self.fund_configs.keys())

    def auto_detect_fund_from_filename(self, filename):
        """Auto-detect fund configuration from filename"""
        filename_lower = filename.lower()
        
        # Check each fund's file identifier
        for fund_key, config in self.fund_configs.items():
            identifier = config['file_identifier'].lower()
            if identifier in filename_lower:
                print(f"Auto-detected fund: {fund_key} from filename: {filename}")
                return fund_key
        
        # Check for fund names/codes in filename
        fund_keywords = {
            'NexBridge': ['nexbridge global'],
            'ASOF': ['asof', 'altura', 'strategic opportunities'],
            'Stonewell': ['stonewell', 'stone well', 'diversified']
        }
        
        for fund_key, keywords in fund_keywords.items():
            if any(keyword in filename_lower for keyword in keywords):
                print(f"Auto-detected fund: {fund_key} from keyword match in filename: {filename}")
                return fund_key
        
        print(f"Could not auto-detect fund from filename: {filename}. Using current configuration.")
        return None

    def get_or_create_source(self, session, source_name, created_by="system"):
        """Get or create a source record"""
        source = session.query(Source).filter(Source.name == source_name).first()
        if not source:
            source = Source(
                name=source_name,
                created_by=created_by
            )
            session.add(source)
            session.flush()  # Get the ID
            print(f"Created new source: {source_name}")
        return source

    def get_or_create_nav_pack(self, session, fund_id, source_id, file_date):
        """Get or create a nav pack record"""
        nav_pack = session.query(NavPack).filter(
            NavPack.fund_id == fund_id,
            NavPack.source_id == source_id,
            NavPack.file_date == file_date
        ).first()
        
        if not nav_pack:
            nav_pack = NavPack(
                fund_id=fund_id,
                source_id=source_id,
                file_date=file_date
            )
            session.add(nav_pack)
            session.flush()
            print(f"Created new nav pack for fund {fund_id}, date {file_date}")
        return nav_pack

    def get_next_version(self, session, navpack_id):
        """Get the next version number for a nav pack"""
        max_version = session.query(NavPackVersion.version).filter(
            NavPackVersion.navpack_id == navpack_id
        ).order_by(NavPackVersion.version.desc()).first()
        
        return (max_version[0] + 1) if max_version else 1

    def extract_date_from_filename(self, filename):
        """Extract date from filename like 'Jan 2024'"""
        match = re.search(r'([A-Za-z]{3})\s+(\d{4})', filename)
        if match:
            month_str, year_str = match.groups()
            # Convert month abbreviation to number
            month_map = {
                'Jan': 1, 'Feb': 2, 'Mar': 3, 'Apr': 4, 'May': 5, 'Jun': 6,
                'Jul': 7, 'Aug': 8, 'Sep': 9, 'Oct': 10, 'Nov': 11, 'Dec': 12
            }
            month = month_map.get(month_str, 1)
            year = int(year_str)
            # Use the last day of the month as the file date
            if month == 12:
                file_date = datetime(year + 1, 1, 1) - timedelta(days=1)
            else:
                file_date = datetime(year, month + 1, 1) - timedelta(days=1)
            return file_date.date()
        return None

    def _get_financial_account(self, row, columns):
        """
        Get financial account value, prioritizing GL Account for Stonewell/Veridex
        
        Args:
            row: DataFrame row
            columns: List of column names in the DataFrame
        
        Returns:
            Financial account string value
        """
        # Check if this is Stonewell fund with Veridex source
        is_stonewell_veridex = (
            self.fund_name == 'Stonewell' and 
            ('veridex' in self.source_name.lower() or 'veridexas' in self.source_name.lower())
        )
        
        if is_stonewell_veridex:
            # Prioritize GL Account column
            # Search for GL Account column (case-insensitive)
            for col in columns:
                col_lower = str(col).lower().strip()
                if 'gl account' in col_lower:
                    gl_account = row.get(col)
                    if not pd.isna(gl_account) and str(gl_account).strip():
                        return str(gl_account).strip()
                    break
            
            # Fall back to Financial Account if GL Account not found or empty
            financial_account = row.get('Financial Account', '')
            if pd.isna(financial_account) or not str(financial_account).strip():
                return ''
            return str(financial_account).strip()
        else:
            # Default behavior: use Financial Account
            financial_account = row.get('Financial Account', '')
            if pd.isna(financial_account):
                return ''
            return str(financial_account).strip()
    
    def extract_general_ledger_data(self, file_path):
        """Extract Detailed General Ledger tab data and return as dict for matching trial balance rows"""
        try:
            # Get sheet configuration
            ledger_config = self.config["sheets"]["detailed_general_ledger"]
            target_sheet_name = ledger_config["sheet_name"]
            required_columns = ledger_config["required_columns"]
            skip_rows = ledger_config["skip_rows"]
            
            # Check if Detailed General Ledger tab exists
            xl = pd.ExcelFile(file_path)
            ledger_sheet = None
            for sheet_name in xl.sheet_names:
                if target_sheet_name.lower() in sheet_name.lower().strip():
                    ledger_sheet = sheet_name
                    print(f"Found Detailed General Ledger sheet: '{sheet_name}'")
                    break
            
            if ledger_sheet is None:
                print(f"Detailed General Ledger tab ('{target_sheet_name}') not found in the Excel file")
                return None
            
            # Read the Detailed General Ledger sheet without header first to find the actual header row
            df_full = pd.read_excel(file_path, sheet_name=ledger_sheet, header=None)
            
            # Find the header row by looking for required columns
            header_row = None
            for i, row in df_full.iterrows():
                row_values = [str(cell).lower() if pd.notna(cell) else '' for cell in row.values]
                # Check if all required columns are present
                columns_found = []
                for req_col in required_columns:
                    if any(req_col.lower() in val for val in row_values):
                        columns_found.append(req_col)
                
                if len(columns_found) == len(required_columns):
                    header_row = i
                    print(f"Found Detailed General Ledger header at row {i}")
                    break
            
            if header_row is None:
                print(f"Could not find Detailed General Ledger header row with required columns: {required_columns}")
                return None
            
            # Re-read with the correct header
            if skip_rows > 0:
                # Skip the configured number of extra rows after header
                data_start_row = header_row + skip_rows
                df = pd.read_excel(file_path, sheet_name=ledger_sheet, header=header_row, skiprows=range(header_row + 1, data_start_row))
            else:
                # No extra rows to skip, data starts immediately after header
                df = pd.read_excel(file_path, sheet_name=ledger_sheet, header=header_row)
            
            # Clean column names
            df.columns = [str(col).strip() for col in df.columns]
            
            # Remove rows with all NaN values
            df = df.dropna(how='all')
            
            print(f"Detailed General Ledger columns: {df.columns.tolist()}")
            
            # Create a dictionary mapping GLAccountDesc to transaction details
            ledger_data = {}
            for _, row in df.iterrows():
                gl_account_desc = row.get('GLAccountDesc', '')
                tran_description = row.get('Tran Description', '')
                local_amount = row.get('Local Amount', '')
                
                # Skip rows with empty essential data
                if pd.isna(gl_account_desc) or str(gl_account_desc).strip() == '':
                    continue
                
                # Convert to string and handle NaN values
                gl_account_desc_str = str(gl_account_desc).strip()
                tran_desc_str = str(tran_description) if not pd.isna(tran_description) else ''
                local_amount_str = str(local_amount) if not pd.isna(local_amount) else ''
                
                # Initialize list for this GL account if not exists
                if gl_account_desc_str not in ledger_data:
                    ledger_data[gl_account_desc_str] = []
                
                # Add transaction details
                ledger_data[gl_account_desc_str].append({
                    'tran_description': tran_desc_str,
                    'local_amount': local_amount_str
                })
            
            print(f"Extracted general ledger data for {len(ledger_data)} GL accounts")
            return ledger_data if ledger_data else None
            
        except Exception as e:
            print(f"Error extracting general ledger data: {e}")
            return None

    def process_trial_balance_sheet(self, session, navpack_version_id, file_path):
        """Process Trial Balance sheet and insert data with general ledger extra data"""
        try:
            # First, extract general ledger data for matching
            ledger_data = self.extract_general_ledger_data(file_path)
            
            # Read the trial balance sheet
            df = pd.read_excel(file_path, sheet_name=self.config["sheets"]["trial_balance"]["sheet_name"])
            
            # Skip the extra header rows (first 10 rows as per config)
            skip_rows = self.config["sheets"]["trial_balance"]["skip_rows"]
            
            # Use the known header row index (from NexBridge.json: topRowsWithExtraData = 10)
            header_row = skip_rows  # Row 10 (0-indexed) contains the headers
            
            # Re-read with the correct header
            df = pd.read_excel(file_path, sheet_name=self.config["sheets"]["trial_balance"]["sheet_name"], header=header_row)
            
            # Clean column names
            df.columns = [str(col).strip() for col in df.columns]
            
            # Remove rows with all NaN values
            df = df.dropna(how='all')
            
            print(f"Trial Balance columns found: {df.columns.tolist()}")
            
            # Check if GL Account will be prioritized
            is_stonewell_veridex = (
                self.fund_name == 'Stonewell' and 
                ('veridex' in self.source_name.lower() or 'veridexas' in self.source_name.lower())
            )
            if is_stonewell_veridex:
                # Check if GL Account column exists
                gl_account_found = any('gl account' in str(col).lower().strip() for col in df.columns)
                if gl_account_found:
                    print(f"Note: Using 'GL Account' column for financial_account (Stonewell/Veridex priority)")
                else:
                    print(f"Note: 'GL Account' column not found, will use 'Financial Account' (Stonewell/Veridex)")
            
            count = 0
            for _, row in df.iterrows():
                # Skip rows with missing essential data (only Type is mandatory)
                if pd.isna(row.get('Type')):
                    continue
                
                # Skip disclaimer rows (where most fields are NaN but Type contains long disclaimer text)
                type_value = str(row.get('Type', '')).strip()
                
                # Check financial account column based on fund/source
                is_stonewell_veridex = (
                    self.fund_name == 'Stonewell' and 
                    ('veridex' in self.source_name.lower() or 'veridexas' in self.source_name.lower())
                )
                
                # Determine which column to check for disclaimer
                if is_stonewell_veridex:
                    # Check GL Account first, then Financial Account (case-insensitive)
                    gl_account_col = None
                    for col in df.columns:
                        col_lower = str(col).lower().strip()
                        if 'gl account' in col_lower:
                            gl_account_col = col
                            break
                    
                    gl_account_check = pd.isna(row.get(gl_account_col)) if gl_account_col else True
                    financial_account_check = pd.isna(row.get('Financial Account')) if 'Financial Account' in df.columns else True
                    account_is_na = gl_account_check and financial_account_check
                else:
                    account_is_na = pd.isna(row.get('Financial Account'))
                
                if (len(type_value) > 500 and 
                    pd.isna(row.get('Ending Balance')) and 
                    pd.isna(row.get('Accounting Head')) and
                    account_is_na):
                    print(f"Skipping disclaimer row: {type_value[:100]}...")
                    continue
                
                # Truncate overly long fields to fit database constraints (varchar(255))
                type_value = type_value[:255] if len(type_value) > 255 else type_value
                
                # Handle nullable ending balance
                ending_balance = None
                if not pd.isna(row.get('Ending Balance')):
                    try:
                        ending_balance = float(row['Ending Balance'])
                    except (ValueError, TypeError):
                        ending_balance = None
                
                # Handle nullable category and accounting_head
                category = None if pd.isna(row.get('Category')) else str(row.get('Category', ''))
                accounting_head = None if pd.isna(row.get('Accounting Head')) else str(row.get('Accounting Head', ''))
                
                # For Stonewell/Veridex, prioritize GL Account over Financial Account
                financial_account = self._get_financial_account(row, df.columns)
                
                # Prepare extra_data with Description field and general ledger data
                extra_data_dict = {}
                
                # Add Description from Excel sheet
                description = row.get('Description')
                if not pd.isna(description) and str(description).strip():
                    extra_data_dict['description'] = str(description).strip()
                
                # Search for and add beginning balance and activity columns
                beginning_balance_value = None
                activity_value = None
                
                # Search through all columns for beginning balance and activity
                for col_name in df.columns:
                    col_lower = str(col_name).lower().strip()
                    
                    # Check for beginning balance columns
                    if 'beginning' in col_lower and 'balance' in col_lower:
                        beginning_balance_value = row.get(col_name)
                        if not pd.isna(beginning_balance_value):
                            try:
                                beginning_balance_value = float(beginning_balance_value)
                            except (ValueError, TypeError):
                                beginning_balance_value = str(beginning_balance_value).strip()
                        else:
                            beginning_balance_value = None
                    
                    # Check for activity columns
                    elif 'activity' in col_lower:
                        activity_value = row.get(col_name)
                        if not pd.isna(activity_value):
                            try:
                                activity_value = float(activity_value)
                            except (ValueError, TypeError):
                                activity_value = str(activity_value).strip()
                        else:
                            activity_value = None
                
                # Add beginning balance and activity to extra_data if found
                if beginning_balance_value is not None:
                    extra_data_dict['beginning_balance'] = beginning_balance_value
                
                if activity_value is not None:
                    extra_data_dict['activity'] = activity_value
                
                # Check if this financial account has corresponding general ledger data
                if ledger_data and row.get('Type', '').lower() == 'expense':
                    # Match financial_account with GLAccountDesc from general ledger
                    if financial_account in ledger_data:
                        extra_data_dict['general_ledger'] = ledger_data[financial_account]
                        print(f"Added general ledger data to expense row: {financial_account}")
                
                # Convert to JSON if we have any data
                extra_data = json.dumps(extra_data_dict) if extra_data_dict else None
                
                trial_balance = TrialBalance(
                    type=type_value,
                    category=category,
                    accounting_head=accounting_head,
                    financial_account=financial_account,
                    ending_balance=ending_balance,
                    extra_data=extra_data,
                    navpack_version_id=navpack_version_id
                )
                session.add(trial_balance)
                count += 1
            
            print(f"Processed {count} trial balance records")
            return count
            
        except Exception as e:
            print(f"Error processing trial balance sheet: {e}")
            return 0

    def map_portfolio_column_names(self, df):
        """Map various column naming conventions to standard field names using regex"""
        import re
        
        column_mapping = {}
        
        # Clean column names first
        df.columns = [str(col).strip() for col in df.columns]
        
        # Define column mappings with regex patterns
        field_patterns = {
            'inv_type': [
                r'^inv\s*type$',
                r'^investment\s*type$'
            ],
            'inv_id': [
                r'^inv\s*id$',
                r'^investment\s*id$'
            ],
            'inv_desc': [
                r'^inv\s*desc$',
                r'^inv\s*description$',
                r'^investment\s*description$'
            ],
            'inv_ccy': [
                r'^inv\s*ccy$',
                r'^investment\s*currency$'
            ],
            'end_qty': [
                r'^end\s*qty$',
                r'^quantity$'
            ],
            'end_local_market_price': [
                r'^end\s*local\s*market\s*price$'
            ],
            'end_local_cost': [
                r'^end\s*local\s*cost$'
            ],
            'end_book_cost': [
                r'^end\s*book\s*cost$'
            ],
            'end_local_mv': [
                r'^end\s*local\s*mv$'
            ],
            'end_book_mv': [
                r'^end\s*book\s*mv$',
                r'^market\s*value$'
            ]
        }
        
        # Map each column
        for col in df.columns:
            col_lower = col.lower().strip()
            for field_name, patterns in field_patterns.items():
                if field_name not in column_mapping:  # Only map if not already found
                    for pattern in patterns:
                        if re.match(pattern, col_lower):
                            column_mapping[field_name] = col
                            break
        
        return column_mapping

    def process_portfolio_valuation_sheet(self, session, navpack_version_id, file_path):
        """Process Portfolio Valuation sheet and insert data"""
        try:
            # Read the sheet
            df = pd.read_excel(file_path, sheet_name=self.config["sheets"]["portfolio_valuation"]["sheet_name"])
            
            # Skip the extra header rows
            skip_rows = self.config["sheets"]["portfolio_valuation"]["skip_rows"]
            
            # Use the known header row index (from NexBridge.json: topRowsWithExtraData = 10)
            header_row = skip_rows  # Row 10 (0-indexed) contains the headers
            
            # Re-read with correct header
            df = pd.read_excel(file_path, sheet_name=self.config["sheets"]["portfolio_valuation"]["sheet_name"], header=header_row)
            
            # Map column names using regex patterns
            column_mapping = self.map_portfolio_column_names(df)
            
            print(f"Portfolio column mapping: {column_mapping}")
            
            # Filter out CASH positions (as per config) - use mapped column name
            inv_type_col = column_mapping.get('inv_type')
            if inv_type_col and self.config["sheets"]["portfolio_valuation"]["filters"]["exclude_cash"]:
                df = df[df[inv_type_col] != 'CASH']
            
            # Remove rows with all NaN values
            df = df.dropna(how='all')
            
            # Get mapped column names
            inv_type_col = column_mapping.get('inv_type')
            inv_id_col = column_mapping.get('inv_id')
            end_qty_col = column_mapping.get('end_qty')
            inv_desc_col = column_mapping.get('inv_desc')
            end_local_market_price_col = column_mapping.get('end_local_market_price')
            end_local_mv_col = column_mapping.get('end_local_mv')
            end_book_mv_col = column_mapping.get('end_book_mv')
            
            # Validate that essential columns were found
            if not inv_type_col or not inv_id_col or not end_qty_col:
                print(f"Warning: Missing essential columns. Found: inv_type={inv_type_col}, inv_id={inv_id_col}, end_qty={end_qty_col}")
                print("Available columns:", df.columns.tolist())
                return 0
            
            count = 0
            for _, row in df.iterrows():
                    
                if pd.isna(row.get(inv_type_col)) or pd.isna(row.get(inv_id_col)):
                    continue
                
                # Handle mandatory end_qty
                try:
                    end_qty = float(row.get(end_qty_col, 0))
                except (ValueError, TypeError):
                    continue  # Skip if end_qty is not valid
                
                # Handle nullable end_local_market_price
                end_local_market_price = None
                if end_local_market_price_col and not pd.isna(row.get(end_local_market_price_col)):
                    try:
                        end_local_market_price = float(row.get(end_local_market_price_col))
                    except (ValueError, TypeError):
                        end_local_market_price = None
                
                # Handle nullable end_local_mv
                end_local_mv = None
                if end_local_mv_col and not pd.isna(row.get(end_local_mv_col)):
                    try:
                        end_local_mv = float(row.get(end_local_mv_col))
                    except (ValueError, TypeError):
                        end_local_mv = None
                
                # Handle nullable end_book_mv
                end_book_mv = None
                if end_book_mv_col and not pd.isna(row.get(end_book_mv_col)):
                    try:
                        end_book_mv = float(row.get(end_book_mv_col))
                    except (ValueError, TypeError):
                        end_book_mv = None
                
                # Extract Investment Description for extra_data
                extra_data = None
                if inv_desc_col:
                    inv_desc = row.get(inv_desc_col)
                if not pd.isna(inv_desc) and str(inv_desc).strip():
                    extra_data = json.dumps({"description": str(inv_desc).strip()})
                
                portfolio_valuation = PortfolioValuation(
                    inv_type=str(row.get(inv_type_col, '')),
                    inv_id=str(row.get(inv_id_col, '')),
                    end_qty=end_qty,
                    end_local_market_price=end_local_market_price,
                    end_local_mv=end_local_mv,
                    end_book_mv=end_book_mv,
                    navpack_version_id=navpack_version_id,
                    extra_data=extra_data
                )
                session.add(portfolio_valuation)
                count += 1
            
            print(f"Processed {count} portfolio valuation records")
            return count
            
        except Exception as e:
            print(f"Error processing portfolio valuation sheet: {e}")
            return 0

    def process_dividend_sheet(self, session, navpack_version_id, file_path):
        """Process Dividend sheet and insert data"""
        try:
            # Check if Dividend sheet exists
            xl = pd.ExcelFile(file_path)
            if 'Dividend' not in xl.sheet_names:
                print("Dividend sheet not found in file")
                return 0
            
            # Read the entire sheet first to find header
            df_full = pd.read_excel(file_path, sheet_name='Dividend', header=None)
            
            # Find the actual header row by searching for Security ID and Amount columns
            header_row = None
            for i in range(len(df_full)):
                row_values = df_full.iloc[i].astype(str).str.lower()
                # Look for key column indicators
                if any('security' in val and 'id' in val for val in row_values) and any('amount' in val for val in row_values):
                    header_row = i
                    print(f"Found dividend header at row {i}")
                    break
            
            if header_row is None:
                print(f"Could not find header row in Dividend sheet")
                return 0
            
            # Re-read with correct header
            df = pd.read_excel(file_path, sheet_name='Dividend', header=header_row)
            
            # Clean column names
            df.columns = [str(col).strip() for col in df.columns]
            
            # Remove rows with all NaN values
            df = df.dropna(how='all')
            
            print(f"Dividend sheet has {len(df)} rows after header row {header_row}")
            
            count = 0
            for _, row in df.iterrows():
                # Look for security ID and amount columns (may have different names)
                security_id = None
                security_name = None
                amount = None
                
                # Try different possible column names based on actual sheet structure
                for col in df.columns:
                    col_lower = col.lower()
                    if 'security' in col_lower and 'id' in col_lower and security_id is None:
                        security_id = str(row.get(col, '')).strip()
                    elif 'security' in col_lower and 'name' in col_lower and security_name is None:
                        security_name = str(row.get(col, '')).strip()
                    elif 'amount' in col_lower and amount is None:
                        try:
                            amount_val = row.get(col, 0)
                            if pd.notna(amount_val) and str(amount_val).strip() != '':
                                amount = float(amount_val)
                        except (ValueError, TypeError):
                            pass
                
                # Skip rows with missing essential data or empty security_id
                if not security_id or security_id == 'nan' or amount is None or amount == 0:
                    continue
                
                print(f"Found dividend: {security_id} | {security_name} | ${amount}")
                
                # Add placeholder extra_data for future use
                extra_data = json.dumps({})
                
                dividend = Dividend(
                    security_id=security_id,
                    security_name=security_name or security_id,
                    amount=amount,
                    navpack_version_id=navpack_version_id,
                    extra_data=extra_data
                )
                session.add(dividend)
                count += 1
            
            print(f"Processed {count} dividend records")
            return count
            
        except Exception as e:
            print(f"Error processing dividend sheet: {e}")
            return 0

    def process_file(self, file_path):
        """Process a single Excel file"""
        print(f"\nProcessing file: {os.path.basename(file_path)}")
        
        # Extract date from filename
        file_date = self.extract_date_from_filename(os.path.basename(file_path))
        if not file_date:
            print(f"Could not extract date from filename: {os.path.basename(file_path)}")
            return False
        
        print(f"Extracted date: {file_date}")
        
        session = self.db_manager.get_session()
        try:
            # Get or create source
            source = self.get_or_create_source(session, self.source_name)
            
            # Get or create nav pack
            nav_pack = self.get_or_create_nav_pack(session, self.fund_id, source.id, file_date)
            
            # Get next version
            version = self.get_next_version(session, nav_pack.navpack_id)
            
            # Create nav pack version
            nav_pack_version = NavPackVersion(
                navpack_id=nav_pack.navpack_id,
                version=version,
                file_name=os.path.basename(file_path),
                uploaded_by="data_ingestion_script"
            )
            session.add(nav_pack_version)
            session.flush()  # Get the ID
            
            print(f"Created nav pack version {version} with ID: {nav_pack_version.navpack_version_id}")
            
            # Process each sheet
            total_records = 0
            
            # Process Trial Balance
            tb_count = self.process_trial_balance_sheet(session, nav_pack_version.navpack_version_id, file_path)
            total_records += tb_count
            
            # Process Portfolio Valuation
            pv_count = self.process_portfolio_valuation_sheet(session, nav_pack_version.navpack_version_id, file_path)
            total_records += pv_count
            
            # Process Dividend
            div_count = self.process_dividend_sheet(session, nav_pack_version.navpack_version_id, file_path)
            total_records += div_count
            
            # Commit the transaction
            session.commit()
            print(f"Successfully processed file with {total_records} total records")
            return True
            
        except Exception as e:
            session.rollback()
            print(f"Error processing file: {e}")
            return False
        finally:
            session.close()

    def find_fund_files(self, fund_key=None):
        """Find files for a specific fund or current fund"""
        if fund_key:
            config = self.fund_configs[fund_key]
            file_identifier = config['file_identifier']
        else:
            file_identifier = self.config["file_identifier"]
        
        fund_files = []
        for filename in os.listdir(self.data_folder):
            # Skip Excel temporary files (start with ~$)
            if filename.startswith('~$'):
                continue
            if filename.endswith('.xlsx'):
                # Check if file matches current fund identifier
                if file_identifier.lower() in filename.lower():
                    fund_files.append(os.path.join(self.data_folder, filename))
        
        return sorted(fund_files)

    def run_ingestion(self, auto_detect=False):
        """Run the complete data ingestion process"""
        print(f"Starting Multi-Fund Data Ingestion...")
        print(f"Data folder: {self.data_folder}")
        print(f"Current fund: {self.fund_name} (ID: {self.fund_id})")
        
        if not os.path.exists(self.data_folder):
            print(f"Data folder does not exist: {self.data_folder}")
            return
        
        # Find files for current fund
        fund_files = self.find_fund_files()
        
        if not fund_files and auto_detect:
            print("No files found for current fund. Attempting auto-detection...")
            return self.run_multi_fund_ingestion()
        
        if not fund_files:
            print(f"No files found for fund: {self.fund_name}")
            print(f"Expected file identifier: {self.config['file_identifier']}")
            return
        
        print(f"Found {len(fund_files)} files for fund: {self.fund_name}")
        
        successful = 0
        failed = 0
        
        for file_path in fund_files:
            try:
                if self.process_file(file_path):
                    successful += 1
                else:
                    failed += 1
            except Exception as e:
                print(f"Unexpected error processing {file_path}: {e}")
                failed += 1
        
        print(f"\nIngestion completed for {self.fund_name}!")
        print(f"Successfully processed: {successful} files")
        print(f"Failed to process: {failed} files")

    def run_multi_fund_ingestion(self):
        """Run ingestion for all supported funds with auto-detection"""
        print(f"Starting Multi-Fund Auto-Detection Ingestion...")
        print(f"Data folder: {self.data_folder}")
        
        if not os.path.exists(self.data_folder):
            print(f"Data folder does not exist: {self.data_folder}")
            return
        
        # Get all Excel files (excluding temporary files)
        all_files = [
            os.path.join(self.data_folder, f)
            for f in os.listdir(self.data_folder)
            if f.endswith('.xlsx') and not f.startswith('~$')
        ]
        
        if not all_files:
            print("No Excel files found in data folder")
            return
        
        total_successful = 0
        total_failed = 0
        
        # Process each file with auto-detection
        for file_path in sorted(all_files):
            filename = os.path.basename(file_path)
            print(f"\n--- Processing: {filename} ---")
            
            # Auto-detect fund for this file
            detected_fund = self.auto_detect_fund_from_filename(filename)
            
            if detected_fund:
                # Temporarily switch to detected fund
                original_fund = self.fund_name
                original_config = self.config.copy()
                original_fund_id = self.fund_id
                original_source = self.source_name
                
                try:
                    self._set_fund_config(detected_fund)
                    
                    # Try to detect source from filename as well
                    detected_source = self.auto_detect_source_from_filename(filename, detected_fund)
                    if detected_source:
                        self.set_source_override(detected_source)
                    
                    # Process the file
                    if self.process_file(file_path):
                        total_successful += 1
                        print(f"✓ Successfully processed {filename} as {detected_fund}")
                    else:
                        total_failed += 1
                        print(f"✗ Failed to process {filename}")
                        
                except Exception as e:
                    total_failed += 1
                    print(f"✗ Error processing {filename}: {e}")
                
                # Restore original configuration
                self.fund_name = original_fund
                self.config = original_config
                self.fund_id = original_fund_id
                self.source_name = original_source
            else:
                print(f"⚠ Skipping {filename} - could not detect fund")
                total_failed += 1
        
        print(f"\n=== Multi-Fund Ingestion Summary ===")
        print(f"Total files processed: {total_successful + total_failed}")
        print(f"Successfully processed: {total_successful} files")
        print(f"Failed to process: {total_failed} files")

    def auto_detect_source_from_filename(self, filename, fund_key):
        """Auto-detect source from filename for a specific fund"""
        filename_lower = filename.lower()
        
        # Source mappings per fund
        fund_sources = {
            'NexBridge': ['bluefield'],
            'ASOF': ['harborview', 'clearledger'],
            'Stonewell': ['stratusga', 'veridex']
        }
        
        if fund_key in fund_sources:
            for source in fund_sources[fund_key]:
                if source in filename_lower:
                    # Map detected source to actual source name
                    source_mapping = {
                        'bluefield': 'Bluefield',
                        'harborview': 'Harborview', 
                        'clearledger': 'ClearLedger',
                        'stratusga': 'StratusGA',
                        'veridex': 'VeridexAS'
                    }
                    actual_source = source_mapping.get(source, source.title())
                    print(f"Auto-detected source: {actual_source} for fund: {fund_key}")
                    return actual_source
        
        return None

def main():
    """Main function to run the ingestion with command-line options"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Multi-Fund Data Ingestion Script')
    parser.add_argument('--fund', choices=['NexBridge', 'ASOF', 'Stonewell'], 
                       help='Specific fund to process')
    parser.add_argument('--source', type=str, 
                       help='Override source name (e.g., Harborview, ClearLedger)')
    parser.add_argument('--auto-detect', action='store_true', 
                       help='Auto-detect funds from filenames')
    parser.add_argument('--data-folder', type=str, 
                       help='Custom data folder path')
    parser.add_argument('--list-funds', action='store_true', 
                       help='List available fund configurations')
    
    args = parser.parse_args()
    
    # Initialize ingestion class
    ingestion = NexBridgeDataIngestion(
        data_folder_path=args.data_folder, 
        fund_config=args.fund
    )
    
    if args.list_funds:
        print("Available fund configurations:")
        for fund in ingestion.get_available_funds():
            config = ingestion.fund_configs[fund]
            print(f"  {fund}: ID={config['fund_id']}, Default Source={config['source_name']}")
        return
    
    # Override source if specified
    if args.source:
        ingestion.set_source_override(args.source)
    
    # Run ingestion based on options
    if args.auto_detect:
        print("Running auto-detection mode...")
        ingestion.run_multi_fund_ingestion()
    else:
        print(f"Running single-fund mode for: {ingestion.fund_name}")
        ingestion.run_ingestion(auto_detect=True)  # Fall back to auto-detect if no files found

def run_specific_fund(fund_name, source_name=None, data_folder=None):
    """Helper function to run ingestion for a specific fund"""
    ingestion = NexBridgeDataIngestion(
        data_folder_path=data_folder,
        fund_config=fund_name
    )
    
    if source_name:
        ingestion.set_source_override(source_name)
    
    ingestion.run_ingestion()

def run_all_funds(data_folder=None):
    """Helper function to run auto-detection for all funds"""
    ingestion = NexBridgeDataIngestion(data_folder_path=data_folder)
    ingestion.run_multi_fund_ingestion()

if __name__ == "__main__":
    main()

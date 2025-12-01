import sys
import os
from typing import Dict, List, Optional, Any
from datetime import datetime, date
import logging
import asyncio
from concurrent.futures import ThreadPoolExecutor
import threading
import pandas as pd
from sqlalchemy import or_, func, distinct, text
from decimal import Decimal
from utils.formula_utils import extractFormulaFromDisplayName
# Add the project root to the path to import our database models
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

try:

    from database_models import get_database_manager, Source, NavPack, NavPackVersion, TrialBalance, PortfolioValuation, Dividend, KpiLibrary, KpiThreshold, Client, Fund, DataModelMaster, DataModelDetails, DataLoadInstance, ValidationMaster, ValidationConfiguration, ValidationDetails, RatioMaster, RatioDetails, RatioConfiguration, ProcessInstance, create_validation_result_model, create_ratio_result_model
except ImportError as e:
    print(f"Warning: Could not import database models: {e}")
    get_database_manager = None

# Cache invalidation will be handled by the calling module to avoid circular imports

logger = logging.getLogger(__name__)

class DatabaseValidationService:
    def __init__(self):
        if get_database_manager is None:
            print("Warning: Database models not available")
            self.db_manager = None
        else:
            self.db_manager = get_database_manager()
        
        # OPTIMIZATION: Thread pool for parallel database operations
        self._thread_pool = ThreadPoolExecutor(max_workers=4, thread_name_prefix="db_validation")
        self._lock = threading.Lock()
        
        # OPTIMIZATION: Cache for frequently accessed data
        self._fund_id_cache = {}
        self._source_id_cache = {}
        self._cache_lock = threading.Lock()
    
    def get_fund_id_from_name(self, fund_name: str) -> Optional[int]:
        # OPTIMIZATION: Use cache to avoid repeated lookups
        with self._cache_lock:
            if fund_name in self._fund_id_cache:
                return self._fund_id_cache[fund_name]
        
        fund_mapping = {
            # NexBridge
            'NexBridge': 1,
            'NexBridge Global Ventures': 1,
            # ASOF (Altura Strategic Opportunities Fund)
            'ASOF': 2,
            'Altura Strategic Opportunities': 2,
            'Altura Strategic Opportunities Fund': 2,
            # Stonewell
            'Stonewell': 3,
            'Stonewell Diversified': 3
        }
        
        fund_id = fund_mapping.get(fund_name, 1)
        
        # Cache the result
        with self._cache_lock:
            self._fund_id_cache[fund_name] = fund_id
        
        return fund_id
    
    async def get_parallel_validation_data(self, fund_name: str, source_a: str, source_b: str, date_a: str, date_b: str) -> Dict[str, List[Dict[str, Any]]]:
        """
        OPTIMIZATION: Fetch all validation data in parallel to reduce total response time
        Returns: {
            'trial_balance_a': [...],
            'portfolio_a': [...],
            'dividend_a': [...],
            'trial_balance_b': [...],
            'portfolio_b': [...],
            'dividend_b': [...]
        }
        """
        if not self.db_manager:
            return {}
        
        # Create tasks for parallel execution
        loop = asyncio.get_event_loop()
        
        # Determine if we need to fetch data for source B
        is_dual_source = (source_a != source_b)
        needs_source_b = is_dual_source or (date_b != date_a)
        
        # Create tasks for source A data
        tasks = [
            loop.run_in_executor(self._thread_pool, self.get_trial_balance_data, fund_name, source_a, date_a),
            loop.run_in_executor(self._thread_pool, self.get_portfolio_valuation_data, fund_name, source_a, date_a),
            loop.run_in_executor(self._thread_pool, self.get_dividend_data, fund_name, source_a, date_a)
        ]
        
        # Add tasks for source B if needed
        if needs_source_b:
            tasks.extend([
                loop.run_in_executor(self._thread_pool, self.get_trial_balance_data, fund_name, source_b, date_b),
                loop.run_in_executor(self._thread_pool, self.get_portfolio_valuation_data, fund_name, source_b, date_b),
                loop.run_in_executor(self._thread_pool, self.get_dividend_data, fund_name, source_b, date_b)
            ])
        else:
            # For same source/date, we'll reuse source A data
            tasks.extend([None, None, None])
        
        # Execute all tasks in parallel
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Process results
        data = {
            'trial_balance_a': results[0] if not isinstance(results[0], Exception) else [],
            'portfolio_a': results[1] if not isinstance(results[1], Exception) else [],
            'dividend_a': results[2] if not isinstance(results[2], Exception) else []
        }
        
        if needs_source_b:
            data.update({
                'trial_balance_b': results[3] if not isinstance(results[3], Exception) else [],
                'portfolio_b': results[4] if not isinstance(results[4], Exception) else [],
                'dividend_b': results[5] if not isinstance(results[5], Exception) else []
            })
        else:
            # Reuse source A data for source B
            data.update({
                'trial_balance_b': data['trial_balance_a'],
                'portfolio_b': data['portfolio_a'],
                'dividend_b': data['dividend_a']
            })
        
        # Log any exceptions
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                logger.error(f"Error in parallel data fetch task {i}: {result}")
        
        return data
    
    def get_source_id_from_name(self, source_name: str) -> Optional[int]:
        if not self.db_manager:
            return None
        
        # OPTIMIZATION: Use cache to avoid repeated database lookups
        with self._cache_lock:
            if source_name in self._source_id_cache:
                return self._source_id_cache[source_name]
        
        # Use nexbridge schema specifically for validation data
        session = self.db_manager.get_session_with_schema('nexbridge')
        try:
            source = session.query(Source).filter(Source.name.ilike(source_name.lower())).first()
            source_id = source.id if source else None
            
            # Cache the result
            with self._cache_lock:
                self._source_id_cache[source_name] = source_id
            
            return source_id
        finally:
            session.close()
    
    def get_trial_balance_data(self, fund_name: str, source_name: str, process_date: str) -> List[Dict[str, Any]]:
        if not self.db_manager:
            return []
        # Use nexbridge schema specifically for validation data
        session = self.db_manager.get_session_with_schema('nexbridge')
        try:
            fund_id = self.get_fund_id_from_name(fund_name)
            source_id = self.get_source_id_from_name(source_name)

            if not fund_id or not source_id:
                print(f"No fund or source found for {fund_name} | {source_name}")
                return []
            
            try:
                file_date = datetime.strptime(process_date, '%Y-%m-%d').date()
            except ValueError:
                print(f"Invalid file date: {process_date}")
                return []
            
            # OPTIMIZATION: Use more efficient query with proper joins and indexing hints
            nav_pack = session.query(NavPack).filter(
                NavPack.fund_id == fund_id,
                NavPack.source_id == source_id,
                NavPack.file_date == file_date
            ).first()
            
            if not nav_pack:
                # Debug: Check what nav packs exist for this fund
                existing_packs = session.query(NavPack).filter(NavPack.fund_id == fund_id).all()
                return []
            
            # OPTIMIZATION: Use order_by with limit for better performance
            latest_version = session.query(NavPackVersion).filter(
                NavPackVersion.navpack_id == nav_pack.navpack_id
            ).order_by(NavPackVersion.version.desc()).first()

            if not latest_version:
                # Debug: Check what versions exist for this nav pack
                existing_versions = session.query(NavPackVersion).filter(NavPackVersion.navpack_id == nav_pack.navpack_id).all()
                return []
            
            # Print navpack version being used
            print(f"Using NavPack Version: ID={latest_version.navpack_version_id}, Version={latest_version.version}, Fund={fund_name}, Source={source_name}, Date={process_date}")
            
            # Get combined trial balance data (base + override)
            combined_records = self._get_combined_trial_balance_data(session, latest_version, fund_name, source_name, process_date)
                
            return combined_records
        except Exception as e:
            logger.error(f"Error getting trial balance data: {e}")
            return []
        finally:
            session.close()
    
    def _get_combined_trial_balance_data(self, session, latest_version: 'NavPackVersion', fund_name: str, source_name: str, process_date: str) -> List[Dict[str, Any]]:
        """
        Combine base version data with override data for trial balance
        
        Algorithm:
        1. If latest version has no base_version, return its data directly
        2. If latest version has base_version:
           a. Get all data from base version
           b. Get override data from latest version  
           c. Replace/overlay base data with override data (by financial_account key)
           d. Return combined dataset
        """
        result = []
        
        # If no base version, just return latest version data
        if not latest_version.base_version:
            trial_balance_records = session.query(TrialBalance).filter(
                TrialBalance.navpack_version_id == latest_version.navpack_version_id
            ).all()
            
            for record in trial_balance_records:
                result.append({
                    'fundName': fund_name,
                    'source': source_name,
                    'processDate': process_date,
                    'Type': record.type,
                    'Category': record.category or '',
                    'Accounting Head': record.accounting_head,
                    'Financial Account': record.financial_account,
                    'Ending Balance': float(record.ending_balance) if record.ending_balance is not None else 0.0,
                    'extra_data': record.extra_data  # Add extra_data field
                })
            return result
        
        # Get base version data
        base_records = session.query(TrialBalance).filter(
            TrialBalance.navpack_version_id == latest_version.base_version
        ).all()
        
        # Convert base records to list (preserving duplicates)
        base_data_list = []
        for record in base_records:
            base_data_list.append({
                'fundName': fund_name,
                'source': source_name,
                'processDate': process_date,
                'Type': record.type,
                'Category': record.category or '',
                'Accounting Head': record.accounting_head,
                'Financial Account': record.financial_account,
                'Ending Balance': float(record.ending_balance) if record.ending_balance is not None else 0.0,
                'extra_data': record.extra_data  # Add extra_data field
            })
        
        # Get override data from latest version
        override_records = session.query(TrialBalance).filter(
            TrialBalance.navpack_version_id == latest_version.navpack_version_id
        ).all()
        
        # Apply overrides - REPLACE existing records by finding ALL matching financial accounts
        # Important: This preserves duplicates and follows exact replacement logic
        for override_record in override_records:
            financial_account = override_record.financial_account
            
            # Find ALL base records with this financial account (there might be multiple)
            matching_base_indices = []
            for i, base_data in enumerate(base_data_list):
                if base_data['Financial Account'] == financial_account:
                    matching_base_indices.append(i)
            
            if matching_base_indices:
                print(f"Overriding trial balance record: {financial_account} ({len(matching_base_indices)} matches found)")
                
                # Replace the FIRST matching record with override data
                first_match_index = matching_base_indices[0]
                base_data_list[first_match_index] = {
                    'fundName': fund_name,
                    'source': source_name,
                    'processDate': process_date,
                    'Type': override_record.type,
                    'Category': override_record.category or '',
                    'Accounting Head': override_record.accounting_head,
                    'Financial Account': override_record.financial_account,
                    'Ending Balance': float(override_record.ending_balance) if override_record.ending_balance is not None else 0.0,
                    'extra_data': override_record.extra_data  # Add extra_data field
                }
                
                # Keep other matching records unchanged (preserves duplicate structure)
            else:
                print(f"Warning: Override trial balance record {financial_account} not found in base version - skipping")
        
        # Result is the modified base data list
        result = base_data_list
        
        print(f"Combined trial balance data: {len(base_records)} base records + {len(override_records)} override records = {len(result)} final records")
        return result
    
    def _get_combined_portfolio_data(self, session, latest_version: 'NavPackVersion', fund_name: str, source_name: str, process_date: str) -> List[Dict[str, Any]]:
        """
        Combine base version data with override data for portfolio valuation
        
        Algorithm:
        1. If latest version has no base_version, return its data directly
        2. If latest version has base_version:
           a. Get all data from base version
           b. Get override data from latest version  
           c. Replace/overlay base data with override data (by (inv_type, inv_id) key)
           d. Return combined dataset
        """
        result = []
        
        # If no base version, just return latest version data
        if not latest_version.base_version:
            portfolio_records = session.query(PortfolioValuation).filter(
                PortfolioValuation.navpack_version_id == latest_version.navpack_version_id
            ).all()
            
            for record in portfolio_records:
                result.append({
                    'fundName': fund_name,
                    'source': source_name,
                    'processDate': process_date,
                    'Inv Type': record.inv_type,
                    'Inv Id': record.inv_id,
                    'End Qty': float(record.end_qty),
                    'End Local Market Price': float(record.end_local_market_price) if record.end_local_market_price not in (None, '', 'null') else None,
                    'End Local MV': float(record.end_local_mv),
                    'End Book MV': float(record.end_book_mv) if record.end_book_mv not in (None, '', 'null') else None,
                    'extra_data': record.extra_data  # Add extra_data field
                })
            return result
        
        # Get base version data
        base_records = session.query(PortfolioValuation).filter(
            PortfolioValuation.navpack_version_id == latest_version.base_version
        ).all()
        
        # Convert base records to list (preserving duplicates, though less common for portfolio)
        base_data_list = []
        for record in base_records:
            base_data_list.append({
                'fundName': fund_name,
                'source': source_name,
                'processDate': process_date,
                'Inv Type': record.inv_type,
                'Inv Id': record.inv_id,
                'End Qty': float(record.end_qty),
                'End Local Market Price': float(record.end_local_market_price) if record.end_local_market_price not in (None, '', 'null') else None,
                'End Local MV': float(record.end_local_mv),
                'End Book MV': float(record.end_book_mv) if record.end_book_mv not in (None, '', 'null') else None,
                'extra_data': record.extra_data  # Add extra_data field
            })
        
        # Get override data from latest version
        override_records = session.query(PortfolioValuation).filter(
            PortfolioValuation.navpack_version_id == latest_version.navpack_version_id
        ).all()
        
        # Apply overrides - REPLACE existing records by finding matching (inv_type, inv_id)
        for override_record in override_records:
            inv_type = override_record.inv_type
            inv_id = override_record.inv_id
            
            # Find ALL base records with this (inv_type, inv_id) combination
            matching_base_indices = []
            for i, base_data in enumerate(base_data_list):
                if (base_data['Inv Type'] == inv_type and base_data['Inv Id'] == inv_id):
                    matching_base_indices.append(i)
            
            if matching_base_indices:
                print(f"Overriding portfolio record: {inv_type}/{inv_id} ({len(matching_base_indices)} matches found)")
                
                # Replace the FIRST matching record with override data
                first_match_index = matching_base_indices[0]
                base_data_list[first_match_index] = {
                    'fundName': fund_name,
                    'source': source_name,
                    'processDate': process_date,
                    'Inv Type': override_record.inv_type,
                    'Inv Id': override_record.inv_id,
                    'End Qty': float(override_record.end_qty),
                    'End Local Market Price': float(override_record.end_local_market_price) if override_record.end_local_market_price not in (None, '', 'null') else None,
                    'End Local MV': float(override_record.end_local_mv),
                    'End Book MV': float(override_record.end_book_mv) if override_record.end_book_mv not in (None, '', 'null') else None,
                    'extra_data': override_record.extra_data  # Add extra_data field
                }
                
                # Keep other matching records unchanged (preserves any duplicate structure)
            else:
                print(f"Warning: Override portfolio record {inv_type}/{inv_id} not found in base version - skipping")
        
        # Result is the modified base data list
        result = base_data_list
        
        print(f"Combined portfolio data: {len(base_records)} base records + {len(override_records)} override records = {len(result)} final records")
        return result
    
    def _get_combined_dividend_data(self, session, latest_version: 'NavPackVersion', fund_name: str, source_name: str, process_date: str) -> List[Dict[str, Any]]:
        """
        Combine base version data with override data for dividend data
        
        Algorithm:
        1. If latest version has no base_version, return its data directly
        2. If latest version has base_version:
           a. Get all data from base version
           b. Get override data from latest version  
           c. Replace/overlay base data with override data (by security_id key)
           d. Return combined dataset
        """
        result = []
        
        # If no base version, just return latest version data
        if not latest_version.base_version:
            dividend_records = session.query(Dividend).filter(
                Dividend.navpack_version_id == latest_version.navpack_version_id
            ).all()
            
            for record in dividend_records:
                result.append({
                    'fundName': fund_name,
                    'source': source_name,
                    'processDate': process_date,
                    'Security Id': record.security_id,
                    'Security Name': record.security_name,
                    'Amount': float(record.amount) if record.amount not in (None, '', 'null') else 0.0
                })
            return result
        
        # Get base version data
        base_records = session.query(Dividend).filter(
            Dividend.navpack_version_id == latest_version.base_version
        ).all()
        
        # Convert base records to list (preserving duplicates if any)
        base_data_list = []
        for record in base_records:
            base_data_list.append({
                'fundName': fund_name,
                'source': source_name,
                'processDate': process_date,
                'Security Id': record.security_id,
                'Security Name': record.security_name,
                'Amount': float(record.amount) if record.amount not in (None, '', 'null') else 0.0
            })
        
        # Get override data from latest version
        override_records = session.query(Dividend).filter(
            Dividend.navpack_version_id == latest_version.navpack_version_id
        ).all()
        
        # Apply overrides - REPLACE existing records by finding matching security_id
        for override_record in override_records:
            security_id = override_record.security_id
            
            # Find ALL base records with this security_id
            matching_base_indices = []
            for i, base_data in enumerate(base_data_list):
                if base_data['Security Id'] == security_id:
                    matching_base_indices.append(i)
            
            if matching_base_indices:
                print(f"Overriding dividend record: {security_id} ({len(matching_base_indices)} matches found)")
                
                # Replace the FIRST matching record with override data
                first_match_index = matching_base_indices[0]
                base_data_list[first_match_index] = {
                    'fundName': fund_name,
                    'source': source_name,
                    'processDate': process_date,
                    'Security Id': override_record.security_id,
                    'Security Name': override_record.security_name,
                    'Amount': float(override_record.amount) if override_record.amount not in (None, '', 'null') else 0.0
                }
                
                # Keep other matching records unchanged (preserves any duplicate structure)
            else:
                print(f"Warning: Override dividend record {security_id} not found in base version - skipping")
        
        # Result is the modified base data list
        result = base_data_list
        
        print(f"Combined dividend data: {len(base_records)} base records + {len(override_records)} override records = {len(result)} final records")
        return result
    
    def get_portfolio_valuation_data(self, fund_name: str, source_name: str, process_date: str) -> List[Dict[str, Any]]:
        if not self.db_manager:
            return []
        # Use nexbridge schema specifically for validation data
        session = self.db_manager.get_session_with_schema('nexbridge')
        try:
            fund_id = self.get_fund_id_from_name(fund_name)
            source_id = self.get_source_id_from_name(source_name)
            if not fund_id or not source_id:
                return []
            
            try:
                file_date = datetime.strptime(process_date, '%Y-%m-%d').date()
            except ValueError:
                return []
            
            nav_pack = session.query(NavPack).filter(
                NavPack.fund_id == fund_id,
                NavPack.source_id == source_id,
                NavPack.file_date == file_date
            ).first()
            
            if not nav_pack:
                return []
            
            latest_version = session.query(NavPackVersion).filter(
                NavPackVersion.navpack_id == nav_pack.navpack_id
            ).order_by(NavPackVersion.version.desc()).first()
            
            if not latest_version:
                return []
            
            # Get combined portfolio data (base + override)
            combined_records = self._get_combined_portfolio_data(session, latest_version, fund_name, source_name, process_date)
            
            return combined_records
        except Exception as e:
            logger.error(f"Error getting portfolio valuation data: {e}")
            return []
        finally:
            session.close()
    
    def get_dividend_data(self, fund_name: str, source_name: str, process_date: str) -> List[Dict[str, Any]]:
        """Get dividend data for specific fund, source and date"""
        if not self.db_manager:
            return []
        # Use nexbridge schema specifically for validation data
        session = self.db_manager.get_session_with_schema('nexbridge')
        try:
            fund_id = self.get_fund_id_from_name(fund_name)
            source_id = self.get_source_id_from_name(source_name)
            if not fund_id or not source_id:
                return []
            
            try:
                file_date = datetime.strptime(process_date, '%Y-%m-%d').date()
            except ValueError:
                return []
            
            nav_pack = session.query(NavPack).filter(
                NavPack.fund_id == fund_id,
                NavPack.source_id == source_id,
                NavPack.file_date == file_date
            ).first()
            
            if not nav_pack:
                return []
            
            latest_version = session.query(NavPackVersion).filter(
                NavPackVersion.navpack_id == nav_pack.navpack_id
            ).order_by(NavPackVersion.version.desc()).first()
            
            if not latest_version:
                return []
            
            # Get combined dividend data (base + override)
            combined_records = self._get_combined_dividend_data(session, latest_version, fund_name, source_name, process_date)
            
            return combined_records
        except Exception as e:
            logger.error(f"Error getting dividend data: {e}")
            return []
        finally:
            session.close()
    
    def get_active_kpis(self, kpi_type: Optional[str] = None, category: Optional[str] = None) -> List[Dict[str, Any]]:
        if not self.db_manager:
            return []
        # Use nexbridge schema specifically for KPI data
        session = self.db_manager.get_session_with_schema('nexbridge')
        try:
            query = session.query(KpiLibrary).filter(KpiLibrary.is_active == True)
            if kpi_type:
                query = query.filter(KpiLibrary.kpi_type == kpi_type)
            if category:
                query = query.filter(KpiLibrary.category == category)
            
            kpis = query.all()
            result = []
            for kpi in kpis:
                result.append({
                    'id': kpi.id,
                    'kpi_code': kpi.kpi_code,
                    'kpi_name': kpi.kpi_name,
                    'kpi_type': kpi.kpi_type,
                    'category': kpi.category,
                    'description': kpi.description,
                    'source_type': kpi.source_type,
                    'precision_type': kpi.precision_type,
                    'numerator_field': kpi.numerator_field,
                    'denominator_field': kpi.denominator_field
                })
            return result
        except Exception as e:
            logger.error(f"Error getting KPIs: {e}")
            return []
        finally:
            session.close()
    
    def get_kpi_threshold(self, kpi_id: int, fund_id: Optional[int] = None) -> Optional[float]:
        if not self.db_manager:
            return None
        # Use nexbridge schema specifically for KPI threshold data
        session = self.db_manager.get_session_with_schema('nexbridge')
        try:
            if fund_id:
                threshold = session.query(KpiThreshold).filter(
                    KpiThreshold.kpi_id == kpi_id,
                    KpiThreshold.fund_id == str(fund_id),
                    KpiThreshold.is_active == True
                ).first()
                if threshold:
                    threshold_value = float(threshold.threshold_value)
                    return threshold_value
            
            threshold = session.query(KpiThreshold).filter(
                KpiThreshold.kpi_id == kpi_id,
                KpiThreshold.fund_id.is_(None),
                KpiThreshold.is_active == True
            ).first()
            
            if threshold:
                return float(threshold.threshold_value)
            return None
        except Exception as e:
            logger.error(f"Error getting KPI threshold: {e}")
            return None
        finally:
            session.close()
    
    def get_latest_navpack_version(self, fund_name: str, source_name: str, process_date: str) -> Optional['NavPackVersion']:
        """
        Helper method to get the latest NavPackVersion for a given fund, source, and date
        """
        if not self.db_manager:
            return None
        # Use nexbridge schema specifically for validation data
        session = self.db_manager.get_session_with_schema('nexbridge')
        try:
            fund_id = self.get_fund_id_from_name(fund_name)
            source_id = self.get_source_id_from_name(source_name)
            if not fund_id or not source_id:
                return None
            
            try:
                file_date = datetime.strptime(process_date, '%Y-%m-%d').date()
            except ValueError:
                return None
            
            nav_pack = session.query(NavPack).filter(
                NavPack.fund_id == fund_id,
                NavPack.source_id == source_id,
                NavPack.file_date == file_date
            ).first()
            
            if not nav_pack:
                return None
            
            latest_version = session.query(NavPackVersion).filter(
                NavPackVersion.navpack_id == nav_pack.navpack_id
            ).order_by(NavPackVersion.version.desc()).first()
            
            return latest_version
        except Exception as e:
            logger.error(f"Error getting latest navpack version: {e}")
            return None
        finally:
            session.close()
    
    def calculate_rnav(self, fund_name: str, source_name: str, process_date: str) -> Optional[float]:
        """
        Calculate R.Nav using the enhanced versioning logic
        
        R.Nav = Sum of all ending balances from trial balance data 
        (excluding Revenue, Expense, Capital types as per current logic)
        
        IMPORTANT: R.Nav uses LATEST VERSION (base + override combined) data
        This method properly combines base version + override data before calculation
        """
        try:
            # Get combined trial balance data using enhanced versioning logic (latest version)
            trial_balance_data = self.get_trial_balance_data(fund_name, source_name, process_date)
            
            if not trial_balance_data:
                print(f"No trial balance data found for R.Nav calculation: {fund_name} | {source_name} | {process_date}")
                return None
            
            # Calculate R.Nav as sum of all ending balances (excluding certain types)
            rnav_value = 0.0
            excluded_types = ['revenue', 'expense', 'capital']
            
            for entry in trial_balance_data:
                entry_type = entry.get('Type', '').strip().lower()
                if entry_type in excluded_types:
                    continue
                    
                ending_balance = entry.get('Ending Balance', 0)
                if ending_balance is not None:
                    try:
                        rnav_value += float(ending_balance)
                    except (ValueError, TypeError):
                        continue  # Skip invalid values
            
            print(f"R.Nav calculated: ${rnav_value:,.2f} from {len(trial_balance_data)} trial balance records (LATEST VERSION)")
            return rnav_value
            
        except Exception as e:
            logger.error(f"Error calculating R.Nav: {e}")
            return None
    
    def calculate_nav(self, fund_name: str, source_name: str, process_date: str) -> Optional[float]:
        """
        Calculate NAV using BASE VERSION ONLY (no override data)
        
        NAV = Sum of all ending balances from trial balance data 
        (excluding Revenue, Expense, Capital types as per current logic)
        
        IMPORTANT: NAV uses BASE VERSION ONLY data (ignores overrides)
        This is different from R.Nav which uses latest version (base + override)
        """
        try:
            # Get base version trial balance data only (no override combination)
            trial_balance_data = self.get_trial_balance_data_base_only(fund_name, source_name, process_date)
            
            if not trial_balance_data:
                print(f"No trial balance data found for NAV calculation: {fund_name} | {source_name} | {process_date}")
                return None
            
            # Calculate NAV as sum of all ending balances (excluding certain types)
            nav_value = 0.0
            excluded_types = ['revenue', 'expense', 'capital']
            
            for entry in trial_balance_data:
                entry_type = entry.get('Type', '').strip().lower()
                if entry_type in excluded_types:
                    continue
                    
                ending_balance = entry.get('Ending Balance', 0)
                if ending_balance is not None:
                    try:
                        nav_value += float(ending_balance)
                    except (ValueError, TypeError):
                        continue  # Skip invalid values
            
            return nav_value
            
        except Exception as e:
            logger.error(f"Error calculating NAV: {e}")
            return None
    
    def get_trial_balance_data_base_only(self, fund_name: str, source_name: str, process_date: str) -> List[Dict[str, Any]]:
        """
        Get trial balance data from BASE VERSION ONLY (no override data)
        
        This method is used for NAV calculations that should ignore override data.
        For validation and R.Nav calculations, use get_trial_balance_data() instead.
        """
        if not self.db_manager:
            return []
        # Use nexbridge schema specifically for validation data
        session = self.db_manager.get_session_with_schema('nexbridge')
        try:
            fund_id = self.get_fund_id_from_name(fund_name)
            source_id = self.get_source_id_from_name(source_name)
            if not fund_id or not source_id:
                print(f"No fund or source found for {fund_name} | {source_name}")
                return []
            
            try:
                file_date = datetime.strptime(process_date, '%Y-%m-%d').date()
            except ValueError:
                print(f"Invalid file date: {process_date}")
                return []
            
            nav_pack = session.query(NavPack).filter(
                NavPack.fund_id == fund_id,
                NavPack.source_id == source_id,
                NavPack.file_date == file_date
            ).first()
            
            if not nav_pack:
                print(f"No nav pack found for {fund_name} | {source_name} | {file_date}")
                return []
            
            latest_version = session.query(NavPackVersion).filter(
                NavPackVersion.navpack_id == nav_pack.navpack_id
            ).order_by(NavPackVersion.version.desc()).first()

            if not latest_version:
                print(f"No latest version found for {fund_name} | {source_name} | {file_date}")
                return []
            
            # Determine base version ID to use
            if latest_version.base_version:
                # If latest version has a base version, use the base version data only
                base_version_id = latest_version.base_version
            else:
                # If latest version is itself a base version, use it
                base_version_id = latest_version.navpack_version_id
            
            # Get trial balance data from base version only
            trial_balance_records = session.query(TrialBalance).filter(
                TrialBalance.navpack_version_id == base_version_id
            ).all()

            # Don't deduplicate; include all records, even if duplicates exist
            result = []
            for record in trial_balance_records:
                result.append({
                    'fundName': fund_name,
                    'source': source_name,
                    'processDate': process_date,
                    'Type': record.type,
                    'Category': record.category or '',
                    'Accounting Head': record.accounting_head,
                    'Financial Account': record.financial_account,
                    'Ending Balance': float(record.ending_balance) if record.ending_balance is not None else 0.0
                })

            print(f"Base version trial balance data: {len(result)} records from version {base_version_id} (processed {len(trial_balance_records)} raw records)")
            return result
        except Exception as e:
            logger.error(f"Error getting base version trial balance data: {e}")
            return []
        finally:
            session.close()

    def get_latest_data_modification_time(self, fund_name: str, source_a: str, source_b: str, date_a: str, date_b: str):
        """
        Get the latest modification time for data relevant to validation calculations
        This helps determine if cached validation data is still fresh
        """
        
        if not self.db_manager:
            return None
            
        session = self.db_manager.get_session()
        try:
            # Get fund ID
            fund_id = self.get_fund_id_from_name(fund_name)
            if not fund_id:
                return None
            
            # Get source IDs
            source_a_id = self.get_source_id_from_name(source_a)
            source_b_id = self.get_source_id_from_name(source_b)
            
            # Check modification times from relevant tables
            latest_times = []
            
            # Check NavPackVersion modification times (uploaded_on and override_on)
            version_query = session.query(NavPackVersion.uploaded_on, NavPackVersion.override_on, NavPackVersion.navpack_version_id).join(NavPack).filter(
                NavPack.fund_id == fund_id
            )
            if source_a_id:
                version_query = version_query.filter(NavPack.source_id == source_a_id)
            if source_b_id:
                version_query = version_query.filter(NavPack.source_id == source_b_id)
            
            version_records = version_query.all()
            for record in version_records:
                if record.uploaded_on:
                    latest_times.append(record.uploaded_on)
                if record.override_on:
                    latest_times.append(record.override_on)
            
            # Check if we have any data in related tables
            if version_records:
                version_ids = [r.navpack_version_id for r in version_records]
                trial_balance_count = session.query(TrialBalance).filter(
                    TrialBalance.navpack_version_id.in_(version_ids)
                ).count()
                
                # Check PortfolioValuation table
                portfolio_count = session.query(PortfolioValuation).filter(
                    PortfolioValuation.navpack_version_id.in_(version_ids)
                ).count()
                
                # Check Dividend table
                dividend_count = session.query(Dividend).filter(
                    Dividend.navpack_version_id.in_(version_ids)
                ).count()

            # Check KpiLibrary modification times (created_at and updated_at)
            kpi_library_session = self.db_manager.get_session_with_schema('nexbridge')
            try:
                kpi_library_records = kpi_library_session.query(KpiLibrary.created_at, KpiLibrary.updated_at).filter(
                    KpiLibrary.is_active == True
                ).all()
                
                for record in kpi_library_records:
                    if record.created_at:
                        latest_times.append(record.created_at)
                    if record.updated_at:
                        latest_times.append(record.updated_at)
                        
            except Exception as e:
                logger.warning(f"Could not check KpiLibrary modification times: {e}")
            finally:
                kpi_library_session.close()
            
            # Check KpiThreshold modification times (created_at and updated_at)
            kpi_threshold_session = self.db_manager.get_session_with_schema('nexbridge')
            try:
                kpi_threshold_records = kpi_threshold_session.query(KpiThreshold.created_at, KpiThreshold.updated_at).filter(
                    KpiThreshold.is_active == True
                ).all()
                
                for record in kpi_threshold_records:
                    if record.created_at:
                        latest_times.append(record.created_at)
                    if record.updated_at:
                        latest_times.append(record.updated_at)
                        
            except Exception as e:
                logger.warning(f"Could not check KpiThreshold modification times: {e}")
            finally:
                kpi_threshold_session.close()
            
            # Return the most recent modification time
            if latest_times:
                latest_time = max(latest_times)
                return latest_time
            else:
                return None
                
        except Exception as e:
            return None
        finally:
            session.close()
    
    def get_data_model_data_df(self, client_id: int, fund_id: int,
                                period: str, data_model_name: str,
                                source_name: Optional[str] = None) -> pd.DataFrame:
        """
        Get data model data as a DataFrame as per ClientID, FundID, Period, data_model_name, and optionally source_name
        
        Steps:
        1. Get schema name from public.clients.code based on client_id
        2. Find intdatamodellid from validus.tbl_data_model_master using data_model_name
        3. Find intdataloadinstanceid from validus.tbl_data_load_instance based on:
           - intclientid, intfundid, intdatamodellid, dtdataasof
           - vcdatasourcename (if source_name is provided)
        4. Get table name (vctablename) from validus.tbl_data_model_master
        5. Query data from schema.table_name where intdataloadinstanceid matches
        
        Args:
            client_id: The client ID
            fund_id: The fund ID
            period: The period date (format: 'YYYY-MM-DD')
            data_model_name: The data model name
            source_name: Optional source name (vcdatasourcename) to filter by. If provided, 
                        the query will filter by this source name in addition to other criteria.
        """
        if not self.db_manager:
            return pd.DataFrame()
        
        try:
            # Step 1: Get schema name from public.clients table
            session_public = self.db_manager.get_session_with_schema('public')
            try:
                client = session_public.query(Client).filter(Client.id == client_id).first()
                if not client:
                    logger.error(f"Client with id {client_id} not found")
                    return pd.DataFrame()
                schema_name = client.code
            except Exception as e:
                logger.error(f"Error getting schema from client_id {client_id}: {e}")
                return pd.DataFrame()
            finally:
                session_public.close()
            
            # Step 2: Find intdatamodellid from validus.tbl_data_model_master
            session_validus = self.db_manager.get_session_with_schema('validus')
            try:
                data_model = session_validus.query(DataModelMaster).filter(
                    DataModelMaster.vcmodelname == data_model_name,
                    DataModelMaster.isactive == True
                ).first()
                
                if not data_model:
                    logger.error(f"Data model '{data_model_name}' not found or inactive")
                    return pd.DataFrame()
                
                intdatamodellid = data_model.intdatamodelid
                table_name = data_model.vctablename
                
                if not table_name:
                    logger.error(f"Table name not found for data model '{data_model_name}'")
                    return pd.DataFrame()
                    
            except Exception as e:
                logger.error(f"Error getting data model '{data_model_name}': {e}")
                return pd.DataFrame()
            finally:
                session_validus.close()
            
            # Step 3: Find intdataloadinstanceid from validus.tbl_data_load_instance
            # Parse period to date format
            try:
                if isinstance(period, str):
                    # Try to parse period as date string
                    period_date = datetime.strptime(period, '%Y-%m-%d').date()
                else:
                    period_date = period
            except ValueError:
                logger.error(f"Invalid period format: {period}")
                return pd.DataFrame()
            
            session_validus = self.db_manager.get_session_with_schema('validus')
            try:
                # Query tbl_data_load_instance using ORM
                query = session_validus.query(DataLoadInstance).filter(
                    DataLoadInstance.intclientid == client_id,
                    DataLoadInstance.intfundid == fund_id,
                    DataLoadInstance.intdatamodelid == intdatamodellid,
                    DataLoadInstance.dtdataasof == period_date
                )

                # Add source name filter if provided, compare lower-case
                if source_name:
                    query = query.filter(
                        DataLoadInstance.vcdatasourcename.ilike(source_name.lower())
                    )
                
                data_load_instance = query.first()
                
                if not data_load_instance:
                    error_msg = f"No data load instance found for client_id={client_id}, fund_id={fund_id}, "
                    error_msg += f"datamodellid={intdatamodellid}, period={period}"
                    if source_name:
                        error_msg += f", source_name={source_name}"
                    logger.error(error_msg)
                    return pd.DataFrame()
                
                intdataloadinstanceid = data_load_instance.intdataloadinstanceid
                
            except Exception as e:
                logger.error(f"Error getting data load instance: {e}")
                return pd.DataFrame()
            finally:
                session_validus.close()
            
            # Step 4: Query data from schema.table_name where intdataloadinstanceid matches
            try:
                from sqlalchemy import text
                
                # Use the engine to execute raw SQL query
                engine = self.db_manager.engine
                
                # Build the query with proper schema and table name quoting
                # Schema and table names come from database (Client.code and DataModelMaster.vctablename)
                # so they are safe, but we quote them for proper SQL syntax
                # Use parameterized query for the instance_id value
                query_str = f'SELECT * FROM "{schema_name}"."{table_name}" WHERE intdataloadinstanceid = :instance_id'
                
                # Execute query and convert to DataFrame
                # Use text() with bindparams for proper parameter binding
                with engine.connect() as conn:
                    # Execute the parameterized query
                    result = conn.execute(text(query_str).bindparams(instance_id=intdataloadinstanceid))
                    # Convert result to DataFrame
                    # Get column names from result keys
                    rows = result.fetchall()
                    if rows:
                        columns = result.keys()
                        df = pd.DataFrame(rows, columns=columns)
                    else:
                        df = pd.DataFrame()
                
                log_msg = f"Retrieved {len(df)} rows from {schema_name}.{table_name} for instance_id={intdataloadinstanceid}"
                if source_name:
                    log_msg += f", source_name={source_name}"
                print(log_msg)
                return df
                
            except Exception as e:
                logger.error(f"Error querying data from {schema_name}.{table_name}: {e}")
                return pd.DataFrame()
                
        except Exception as e:
            logger.error(f"Error in get_data_model_data_df: {e}")
            return pd.DataFrame()

    def get_active_validation_config_details(self, client_id: int, fund_id: int) -> List[Dict[str, Any]]:
        """
        Get all validation config details for a given client and fund
        
        Returns a list of dictionaries containing validation master data combined with 
        configuration data filtered by client_id and fund_id, and validation details.
        
        Each dictionary contains:
        - Validation master fields (intvalidationmasterid, vcvalidationname, etc.)
        - Configuration fields (config_isactive, vccondition, config_threshold, etc.)
        - Validation details (list of detail dictionaries in 'details' key)
        """
        if not self.db_manager:
            return []
        
        session = self.db_manager.get_session_with_schema('validus')
        try:
            # Query all active validations
            # Handle case where intupdatedby column might not exist in database
            try:
                validations = session.query(ValidationMaster).filter(
                    ValidationMaster.isactive == True
                ).all()
            except Exception as query_error:
                # If column doesn't exist, check database table structure and query accordingly
                if 'intupdatedby' in str(query_error).lower() or 'does not exist' in str(query_error).lower():
                    logger.warning(f"Column may not exist in tbl_validation_master, checking table structure")
                    # Rollback the failed transaction before inspecting
                    session.rollback()
                    # Check what columns actually exist in the database table
                    from sqlalchemy import inspect, text
                    inspector = inspect(session.bind)
                    try:
                        table_columns = [col['name'] for col in inspector.get_columns('tbl_validation_master', schema='validus')]
                        # If intupdatedby or dtupdatedat don't exist, use raw SQL with explicit columns
                        if 'intupdatedby' not in table_columns or 'dtupdatedat' not in table_columns:
                            print(f"Using raw SQL query for tbl_validation_master (missing columns: intupdatedby={ 'intupdatedby' not in table_columns }, dtupdatedat={ 'dtupdatedat' not in table_columns })")
                            # Build SELECT with only existing columns
                            select_cols = ', '.join([f'"{col}"' for col in table_columns])
                            query_sql = text(f'SELECT {select_cols} FROM validus.tbl_validation_master WHERE isactive = :isactive')
                            result = session.execute(query_sql, {'isactive': True})
                            rows = result.fetchall()
                            # Convert to ValidationMaster-like objects or dictionaries
                            # For now, create a simple workaround by mapping to dict
                            validations = []
                            for row in rows:
                                # Create a dict-like object that can be accessed with getattr
                                row_dict = dict(zip(table_columns, row))
                                # Create a simple object that mimics ValidationMaster
                                class ValidationRow:
                                    def __init__(self, data):
                                        for k, v in data.items():
                                            setattr(self, k, v)
                                validations.append(ValidationRow(row_dict))
                        else:
                            raise
                    except Exception as inspect_error:
                        logger.error(f"Error inspecting table structure: {inspect_error}")
                        raise query_error
                else:
                    raise
            
            if not validations:
                return []
            
            validation_ids = [v.intvalidationmasterid for v in validations]
            
            # Query configurations filtered by client_id and fund_id
            configs_by_validation = {}
            try:
                c_query = session.query(ValidationConfiguration).filter(
                    ValidationConfiguration.intvalidationmasterid.in_(validation_ids),
                    ValidationConfiguration.intclientid == client_id,
                    ValidationConfiguration.intfundid == fund_id,
                    ValidationConfiguration.isactive == True
                )
                configurations = c_query.all()
            except Exception as config_error:
                # Handle missing columns in ValidationConfiguration
                if 'does not exist' in str(config_error).lower():
                    logger.warning(f"Column may not exist in tbl_validation_configuration, using raw SQL query")
                    session.rollback()
                    from sqlalchemy import inspect, text
                    inspector = inspect(session.bind)
                    try:
                        table_columns = [col['name'] for col in inspector.get_columns('tbl_validation_configuration', schema='validus')]
                        select_cols = ', '.join([f'"{col}"' for col in table_columns])
                        # Use ANY for array parameter
                        query_sql = text(f'''
                            SELECT {select_cols} FROM validus.tbl_validation_configuration 
                            WHERE intvalidationmasterid = ANY(:validation_ids)
                            AND intclientid = :client_id 
                            AND intfundid = :fund_id 
                            AND isactive = :isactive
                        ''')
                        result = session.execute(query_sql, {
                            'validation_ids': validation_ids,
                            'client_id': client_id,
                            'fund_id': fund_id,
                            'isactive': True
                        })
                        rows = result.fetchall()
                        configurations = []
                        for row in rows:
                            row_dict = dict(zip(table_columns, row))
                            class ConfigRow:
                                def __init__(self, data):
                                    for k, v in data.items():
                                        setattr(self, k, v)
                            configurations.append(ConfigRow(row_dict))
                    except Exception as inspect_error:
                        logger.error(f"Error querying validation configurations: {inspect_error}")
                        configurations = []
                else:
                    logger.error(f"Error querying validation configurations: {config_error}")
                    configurations = []
            
            # Map by validation master id (one config per validation for client/fund)
            for cfg in configurations:
                configs_by_validation[cfg.intvalidationmasterid] = cfg
            
            # Query validation details for all validations
            details_by_validation = {}
            try:
                details_query = session.query(ValidationDetails).filter(
                    ValidationDetails.intvalidationmasterid.in_(validation_ids)
                )
                validation_details = details_query.all()
            except Exception as details_error:
                # Handle missing columns in ValidationDetails
                if 'does not exist' in str(details_error).lower():
                    logger.warning(f"Column may not exist in tbl_validation_details, using raw SQL query")
                    session.rollback()
                    from sqlalchemy import inspect, text
                    inspector = inspect(session.bind)
                    try:
                        table_columns = [col['name'] for col in inspector.get_columns('tbl_validation_details', schema='validus')]
                        select_cols = ', '.join([f'"{col}"' for col in table_columns])
                        query_sql = text(f'''
                            SELECT {select_cols} FROM validus.tbl_validation_details 
                            WHERE intvalidationmasterid = ANY(:validation_ids)
                        ''')
                        result = session.execute(query_sql, {'validation_ids': validation_ids})
                        rows = result.fetchall()
                        validation_details = []
                        for row in rows:
                            row_dict = dict(zip(table_columns, row))
                            class DetailRow:
                                def __init__(self, data):
                                    for k, v in data.items():
                                        setattr(self, k, v)
                            validation_details.append(DetailRow(row_dict))
                    except Exception as inspect_error:
                        logger.error(f"Error querying validation details: {inspect_error}")
                        validation_details = []
                else:
                    logger.error(f"Error querying validation details: {details_error}")
                    validation_details = []
            
            # Map by validation master id (multiple details per validation)
            for detail in validation_details:
                if detail.intvalidationmasterid not in details_by_validation:
                    details_by_validation[detail.intvalidationmasterid] = []
                details_by_validation[detail.intvalidationmasterid].append(detail)
            
            # Build response dictionaries for all validations
            results = []
            for v in validations:
                # Get config if exists
                config = configs_by_validation.get(v.intvalidationmasterid)
                # Skip validations that don't have a configuration for this client/fund
                if not config:
                    continue
                
                # Get details for this validation
                details_list = details_by_validation.get(v.intvalidationmasterid, [])
                
                # Convert details to dictionaries
                details_dicts = []
                for detail in details_list:
                    details_dicts.append({
                        'intvalidationdetailid': detail.intvalidationdetailid,
                        'intvalidationmasterid': detail.intvalidationmasterid,
                        'intdatamodelid': getattr(detail, 'intdatamodelid', None),
                        'intgroup_attributeid': getattr(detail, 'intgroup_attributeid', None),
                        'intassettypeid': getattr(detail, 'intassettypeid', None),
                        'intcalc_attributeid': getattr(detail, 'intcalc_attributeid', None),
                        'vcaggregationtype': getattr(detail, 'vcaggregationtype', None),
                        'vcfilter': getattr(detail, 'vcfilter', None),
                        'vcfiltertype': getattr(detail, 'vcfiltertype', None),
                        'vcformula': getattr(detail, 'vcformula', None),
                        'intcreatedby': getattr(detail, 'intcreatedby', None),
                        'dtcreatedat': getattr(detail, 'dtcreatedat', None).isoformat() if getattr(detail, 'dtcreatedat', None) else None,
                        'intupdatedby': getattr(detail, 'intupdatedby', None),
                        'dtupdatedat': getattr(detail, 'dtupdatedat', None).isoformat() if getattr(detail, 'dtupdatedat', None) else None
                    })
                
                # Build dictionary with validation master fields, configuration fields, and details
                validation_dict = {
                    # Validation master fields
                    'intvalidationmasterid': v.intvalidationmasterid,
                    'intsubproductid': v.intsubproductid,
                    'vcsourcetype': v.vcsourcetype,
                    'vctype': v.vctype,
                    'vcsubtype': v.vcsubtype,
                    'issubtype_subtotal': bool(v.issubtype_subtotal) if v.issubtype_subtotal is not None else None,
                    'vcvalidationname': v.vcvalidationname,
                    'isvalidation_subtotal': bool(v.isvalidation_subtotal) if v.isvalidation_subtotal is not None else None,
                    'vcdescription': v.vcdescription,
                    'intthreshold': float(v.intthreshold) if v.intthreshold is not None else None,
                    'vcthresholdtype': v.vcthresholdtype,
                    'vcthreshold_abs_range': getattr(v, 'vcthreshold_abs_range', None),
                    'intthresholdmin': float(v.intthresholdmin) if hasattr(v, 'intthresholdmin') and v.intthresholdmin is not None else None,
                    'intthresholdmax': float(v.intthresholdmax) if hasattr(v, 'intthresholdmax') and v.intthresholdmax is not None else None,
                    'intprecision': float(v.intprecision) if v.intprecision is not None else None,
                    'isactive': bool(v.isactive) if v.isactive is not None else False,
                    'intcreatedby': v.intcreatedby,
                    'dtcreatedat': v.dtcreatedat.isoformat() if v.dtcreatedat else None,
                    'intupdatedby': getattr(v, 'intupdatedby', None),
                    'dtupdatedat': v.dtupdatedat.isoformat() if getattr(v, 'dtupdatedat', None) else None,
                    # Configuration fields (None if no config)
                    'config_isactive': config.isactive if config else None,
                    'vccondition': config.vccondition if config else None,
                    'config_threshold': float(config.intthreshold) if config and config.intthreshold is not None else None,
                    'config_thresholdtype': config.vcthresholdtype if config else None,
                    'config_threshold_abs_range': getattr(config, 'vcthreshold_abs_range', None) if config else None,
                    'config_thresholdmin': float(config.intthresholdmin) if config and hasattr(config, 'intthresholdmin') and config.intthresholdmin is not None else None,
                    'config_thresholdmax': float(config.intthresholdmax) if config and hasattr(config, 'intthresholdmax') and config.intthresholdmax is not None else None,
                    'config_intprecision': float(config.intprecision) if config and config.intprecision is not None else None,
                    'intvalidationconfigurationid': config.intvalidationconfigurationid if config else None,
                    # Validation details (list of detail dictionaries)
                    'details': details_dicts
                }
                results.append(validation_dict)
            
            return results
            
        except Exception as e:
            logger.error(f"Error getting validation config details: {e}")
            return []
        finally:
            session.close()
    
    def get_active_ratio_config_details(self, client_id: int, fund_id: int) -> List[Dict[str, Any]]:
        """
        Get all ratio config details for a given client and fund
        
        Returns a list of dictionaries containing ratio master data combined with 
        configuration data filtered by client_id and fund_id, and ratio details.
        
        Each dictionary contains:
        - Ratio master fields (intratiomasterid, vcrationame, etc.)
        - Configuration fields (config_isactive, vccondition, config_threshold, etc.)
        - Ratio details (list of detail dictionaries in 'details' key)
        """
        if not self.db_manager:
            return []
        
        session = self.db_manager.get_session_with_schema('validus')
        try:
            # Query all active ratios
            try:
                ratios = session.query(RatioMaster).filter(
                    RatioMaster.isactive == True
                ).all()
            except Exception as query_error:
                # Handle case where columns might not exist
                if 'does not exist' in str(query_error).lower():
                    logger.warning(f"Column may not exist in tbl_ratio_master, using raw SQL query")
                    session.rollback()
                    from sqlalchemy import inspect, text
                    inspector = inspect(session.bind)
                    try:
                        table_columns = [col['name'] for col in inspector.get_columns('tbl_ratio_master', schema='validus')]
                        select_cols = ', '.join([f'"{col}"' for col in table_columns])
                        query_sql = text(f'SELECT {select_cols} FROM validus.tbl_ratio_master WHERE isactive = :isactive')
                        result = session.execute(query_sql, {'isactive': True})
                        rows = result.fetchall()
                        ratios = []
                        for row in rows:
                            row_dict = dict(zip(table_columns, row))
                            class RatioRow:
                                def __init__(self, data):
                                    for k, v in data.items():
                                        setattr(self, k, v)
                            ratios.append(RatioRow(row_dict))
                    except Exception as inspect_error:
                        logger.error(f"Error inspecting table structure: {inspect_error}")
                        raise query_error
                else:
                    raise
            
            if not ratios:
                return []
            
            ratio_ids = [r.intratiomasterid for r in ratios]
            
            # Query configurations filtered by client_id and fund_id
            configs_by_ratio = {}
            try:
                c_query = session.query(RatioConfiguration).filter(
                    RatioConfiguration.intratiomasterid.in_(ratio_ids),
                    RatioConfiguration.intclientid == client_id,
                    RatioConfiguration.intfundid == fund_id,
                    RatioConfiguration.isactive == True
                )
                configurations = c_query.all()
            except Exception as config_error:
                # Handle missing columns in RatioConfiguration
                if 'does not exist' in str(config_error).lower():
                    logger.warning(f"Column may not exist in tbl_ratio_configuration, using raw SQL query")
                    session.rollback()
                    from sqlalchemy import inspect, text
                    inspector = inspect(session.bind)
                    try:
                        table_columns = [col['name'] for col in inspector.get_columns('tbl_ratio_configuration', schema='validus')]
                        select_cols = ', '.join([f'"{col}"' for col in table_columns])
                        query_sql = text(f'''
                            SELECT {select_cols} FROM validus.tbl_ratio_configuration 
                            WHERE intratiomasterid = ANY(:ratio_ids)
                            AND intclientid = :client_id 
                            AND intfundid = :fund_id 
                            AND isactive = :isactive
                        ''')
                        result = session.execute(query_sql, {
                            'ratio_ids': ratio_ids,
                            'client_id': client_id,
                            'fund_id': fund_id,
                            'isactive': True
                        })
                        rows = result.fetchall()
                        configurations = []
                        for row in rows:
                            row_dict = dict(zip(table_columns, row))
                            class ConfigRow:
                                def __init__(self, data):
                                    for k, v in data.items():
                                        setattr(self, k, v)
                            configurations.append(ConfigRow(row_dict))
                    except Exception as inspect_error:
                        logger.error(f"Error querying ratio configurations: {inspect_error}")
                        configurations = []
                else:
                    logger.error(f"Error querying ratio configurations: {config_error}")
                    configurations = []
            
            # Map by ratio master id (one config per ratio for client/fund)
            for cfg in configurations:
                configs_by_ratio[cfg.intratiomasterid] = cfg
            
            # Query ratio details for all ratios
            details_by_ratio = {}
            try:
                details_query = session.query(RatioDetails).filter(
                    RatioDetails.intratiomasterid.in_(ratio_ids)
                )
                ratio_details = details_query.all()
            except Exception as details_error:
                # Handle missing columns in RatioDetails
                if 'does not exist' in str(details_error).lower():
                    logger.warning(f"Column may not exist in tbl_ratio_details, using raw SQL query")
                    session.rollback()
                    from sqlalchemy import inspect, text
                    inspector = inspect(session.bind)
                    try:
                        table_columns = [col['name'] for col in inspector.get_columns('tbl_ratio_details', schema='validus')]
                        select_cols = ', '.join([f'"{col}"' for col in table_columns])
                        query_sql = text(f'''
                            SELECT {select_cols} FROM validus.tbl_ratio_details 
                            WHERE intratiomasterid = ANY(:ratio_ids)
                        ''')
                        result = session.execute(query_sql, {'ratio_ids': ratio_ids})
                        rows = result.fetchall()
                        ratio_details = []
                        for row in rows:
                            row_dict = dict(zip(table_columns, row))
                            class DetailRow:
                                def __init__(self, data):
                                    for k, v in data.items():
                                        setattr(self, k, v)
                            ratio_details.append(DetailRow(row_dict))
                    except Exception as inspect_error:
                        logger.error(f"Error querying ratio details: {inspect_error}")
                        ratio_details = []
                else:
                    logger.error(f"Error querying ratio details: {details_error}")
                    ratio_details = []
            
            # Map by ratio master id (multiple details per ratio)
            for detail in ratio_details:
                if detail.intratiomasterid not in details_by_ratio:
                    details_by_ratio[detail.intratiomasterid] = []
                details_by_ratio[detail.intratiomasterid].append(detail)
            
            # Build response dictionaries for all ratios
            # Only include ratios that have a configuration for this client/fund
            results = []
            for r in ratios:
                # Get config if exists
                config = configs_by_ratio.get(r.intratiomasterid)
                
                # Skip ratios that don't have a configuration for this client/fund
                if not config:
                    continue
                
                # Get details for this ratio
                details_list = details_by_ratio.get(r.intratiomasterid, [])
                
                # Convert details to dictionaries
                details_dicts = []
                for detail in details_list:
                    details_dicts.append({
                        'intratiodetailid': detail.intratiodetailid,
                        'intratiomasterid': detail.intratiomasterid,
                        'intdatamodelid': getattr(detail, 'intdatamodelid', None),
                        'vcfilter': getattr(detail, 'vcfilter', None),
                        'vcfiltertype': getattr(detail, 'vcfiltertype', None),
                        'vcnumerator': getattr(detail, 'vcnumerator', None),
                        'vcdenominator': getattr(detail, 'vcdenominator', None),
                        'vcformula': getattr(detail, 'vcformula', None),
                        'intcreatedby': getattr(detail, 'intcreatedby', None),
                        'dtcreatedat': getattr(detail, 'dtcreatedat', None).isoformat() if getattr(detail, 'dtcreatedat', None) else None,
                        'intupdatedby': getattr(detail, 'intupdatedby', None),
                        'dtupdatedat': getattr(detail, 'dtupdatedat', None).isoformat() if getattr(detail, 'dtupdatedat', None) else None
                    })
                
                # Build dictionary with ratio master fields, configuration fields, and details
                ratio_dict = {
                    # Ratio master fields
                    'intratiomasterid': r.intratiomasterid,
                    'intsubproductid': r.intsubproductid,
                    'vcsourcetype': r.vcsourcetype,
                    'vctype': r.vctype,
                    'vcrationame': r.vcrationame,
                    'isratio_subtotal': bool(r.isratio_subtotal) if r.isratio_subtotal is not None else None,
                    'vcdescription': r.vcdescription,
                    'intthreshold': float(r.intthreshold) if r.intthreshold is not None else None,
                    'vcthresholdtype': r.vcthresholdtype,
                    'vcthreshold_abs_range': getattr(r, 'vcthreshold_abs_range', None),
                    'intthresholdmin': float(r.intthresholdmin) if hasattr(r, 'intthresholdmin') and r.intthresholdmin is not None else None,
                    'intthresholdmax': float(r.intthresholdmax) if hasattr(r, 'intthresholdmax') and r.intthresholdmax is not None else None,
                    'intprecision': float(r.intprecision) if r.intprecision is not None else None,
                    'isactive': bool(r.isactive) if r.isactive is not None else False,
                    'intcreatedby': r.intcreatedby,
                    'dtcreatedat': r.dtcreatedat.isoformat() if r.dtcreatedat else None,
                    'intupdatedby': getattr(r, 'intupdatedby', None),
                    'dtupdatedat': r.dtupdatedat.isoformat() if getattr(r, 'dtupdatedat', None) else None,
                    # Configuration fields (None if no config)
                    'config_isactive': config.isactive if config else None,
                    'vccondition': config.vccondition if config else None,
                    'config_threshold': float(config.intthreshold) if config and config.intthreshold is not None else None,
                    'config_thresholdtype': config.vcthresholdtype if config else None,
                    'config_threshold_abs_range': getattr(config, 'vcthreshold_abs_range', None) if config else None,
                    'config_thresholdmin': float(config.intthresholdmin) if config and hasattr(config, 'intthresholdmin') and config.intthresholdmin is not None else None,
                    'config_thresholdmax': float(config.intthresholdmax) if config and hasattr(config, 'intthresholdmax') and config.intthresholdmax is not None else None,
                    'config_intprecision': float(config.intprecision) if config and config.intprecision is not None else None,
                    'intratioconfigurationid': config.intratioconfigurationid if config else None,
                    # Ratio details (list of detail dictionaries)
                    'details': details_dicts
                }
                results.append(ratio_dict)
            
            return results
        except Exception as e:
            logger.error(f"Error getting active ratio config details: {e}")
            return []
        finally:
            session.close()
    
    def get_latest_process_instance_summary(self, client_id: int, fund_id: int, subproduct_id: int, source_a: str, date_a: str, source_b: Optional[str] = None, date_b: Optional[str] = None) -> Optional[Dict[str, Any]]:
        """
        Get the latest process instance summary for a given client, fund, subproduct, 
        Optional parameters: source_a, source_b, date_a, and date_b
        Returns a dictionary containing the latest process instance summary
        The dictionary contains the following keys:
        - Total number of validations
        - Total number of failed validations
        - Total number of passed validations
        - Total number of exceptions
        - Total number of ratios
        - Total number of failed ratios
        - Total number of passed ratios
        - subchecks: List of validation names grouped by subtype with:
          - subtype: The subtype name
          - status: "Completed" or "Not Completed" at subtype level
          - validations: List of validations with:
            - validation_name: Name of the validation
            - description: Description from validation master
            - status: "Completed" or "Not Completed"
            - pass_fail: "Pass" or "Fail"
            - datetime: Process instance datetime (ISO format)
        """
        if not self.db_manager:
            return None
        session = self.db_manager.get_session_with_schema('validus')

        # Step 1: Get schema name from public.clients table
        session_public = self.db_manager.get_session_with_schema('public')
        try:
            client = session_public.query(Client).filter(Client.id == client_id).first()
            if not client:
                logger.error(f"Client with id {client_id} not found")
                return None
            schema_name = client.code
        except Exception as e:
            logger.error(f"Error getting schema from client_id {client_id}: {e}")
            return pd.DataFrame()
        finally:
            session_public.close()

        # Step 2: Convert date strings to date objects if needed
        try:
            if isinstance(date_a, str):
                date_a_obj = datetime.strptime(date_a, '%Y-%m-%d').date()
            else:
                date_a_obj = date_a
            
            if isinstance(date_b, str):
                date_b_obj = datetime.strptime(date_b, '%Y-%m-%d').date()
            else:
                date_b_obj = date_b
        except (ValueError, TypeError) as e:
            logger.error(f"Error parsing dates: {e}")
            return None
        
        # Step 3: Get the latest process instance id for both vcvalidustype 'Validation' and 'Ratio'
        try:
            # Build filter conditions - handle empty strings as None
            filter_conditions = [
                ProcessInstance.intclientid == client_id,
                ProcessInstance.intfundid == fund_id,
                ProcessInstance.dtdate_a == date_a_obj,
                ProcessInstance.vcsource_a == source_a
            ]
            
            # Handle date_b and source_b - if empty string, check for NULL or empty
            if date_b:
                filter_conditions.append(ProcessInstance.dtdate_b == date_b_obj)
            else:
                filter_conditions.append(ProcessInstance.dtdate_b == None)
            # Use ilike for lower-case comparison
            if source_b:
                filter_conditions.append(ProcessInstance.vcsource_b.ilike(source_b.lower()))
            else:
                filter_conditions.append(ProcessInstance.vcsource_b.is_(None))
            
            # Get latest Validation process instance
            validation_filter = filter_conditions + [ProcessInstance.vcvalidustype == 'Validation']
            validation_process = session.query(ProcessInstance).filter(*validation_filter).order_by(ProcessInstance.dtprocesstime_start.desc()).first()
            
            # Get latest Ratio process instance
            ratio_filter = filter_conditions + [ProcessInstance.vcvalidustype == 'Ratio']
            ratio_process = session.query(ProcessInstance).filter(*ratio_filter).order_by(ProcessInstance.dtprocesstime_start.desc()).first()
            
            validation_process_id = validation_process.intprocessinstanceid if validation_process else None
            ratio_process_id = ratio_process.intprocessinstanceid if ratio_process else None
            
            # Get vcsourcetype from process instances for filtering
            validation_source_type = validation_process.vcsourcetype if validation_process else None
            ratio_source_type = ratio_process.vcsourcetype if ratio_process else None
            
            # Debug logging
            print(f"Looking for process instances with: client_id={client_id}, fund_id={fund_id}, subproduct_id={subproduct_id}, source_a='{source_a}', source_b='{source_b}', date_a={date_a_obj}, date_b={date_b_obj}")
            print(f"Found validation_process_id: {validation_process_id}, ratio_process_id: {ratio_process_id}")
            print(f"Validation source_type: {validation_source_type}, Ratio source_type: {ratio_source_type}")
            
            if not validation_process_id and not ratio_process_id:
                # Check if any process instances exist at all for debugging
                any_validation = session.query(ProcessInstance).filter(
                    ProcessInstance.intclientid == client_id,
                    ProcessInstance.intfundid == fund_id,
                    ProcessInstance.vcvalidustype == 'Validation'
                ).count()
                any_ratio = session.query(ProcessInstance).filter(
                    ProcessInstance.intclientid == client_id,
                    ProcessInstance.intfundid == fund_id,
                    ProcessInstance.vcvalidustype == 'Ratio'
                ).count()
                logger.warning(f"No matching process instances found. Total Validation processes for client/fund: {any_validation}, Total Ratio processes: {any_ratio}")
            
            # Step 4: Create dynamic schema models
            ValidationResult = create_validation_result_model(schema_name)
            RatioResult = create_ratio_result_model(schema_name)
            
            # Step 5: Get validation results with joins and filter by subproduct_id
            validation_counts = {
                'total': 0,
                'failed': 0,
                'passed': 0,
                'exceptions': 0
            }
            
            if validation_process_id:
                # Query validation results with joins to filter by subproduct_id and vcsourcetype
                base_query = session.query(ValidationResult).join(
                    ValidationConfiguration,
                    ValidationResult.intvalidationconfigurationid == ValidationConfiguration.intvalidationconfigurationid
                ).join(
                    ValidationMaster,
                    ValidationConfiguration.intvalidationmasterid == ValidationMaster.intvalidationmasterid
                ).filter(
                    ValidationResult.intprocessinstanceid == validation_process_id,
                    ValidationMaster.intsubproductid == subproduct_id,
                    ValidationResult.isactive == True
                )
                
                # Filter by vcsourcetype if available
                if validation_source_type:
                    base_query = base_query.filter(ValidationMaster.vcsourcetype == validation_source_type)
                
                # validation_total: Count of unique intvalidationconfigurationid
                validation_counts['total'] = base_query.with_entities(
                    func.count(distinct(ValidationResult.intvalidationconfigurationid))
                ).scalar() or 0
                
                print(f"Found {validation_counts['total']} unique validation configurations for process_instance_id={validation_process_id}, subproduct_id={subproduct_id}")
                
                # validation_failed: Count of unique intvalidationconfigurationid that have at least one failed vcstatus
                failed_configs_query = base_query.filter(
                    ValidationResult.vcstatus.ilike('%failed%')
                ).with_entities(
                    func.count(distinct(ValidationResult.intvalidationconfigurationid))
                )
                validation_counts['failed'] = failed_configs_query.scalar() or 0
                
                # validation_passed: validation_total - validation_failed
                validation_counts['passed'] = validation_counts['total'] - validation_counts['failed']
                
                # validation_exceptions: Total number of failed cases (rows) for this particular intprocessinstanceid
                exception_query = session.query(ValidationResult).filter(
                    ValidationResult.intprocessinstanceid == validation_process_id,
                    ValidationResult.vcstatus.ilike('%failed%'),
                    ValidationResult.isactive == True
                ).join(
                    ValidationConfiguration,
                    ValidationResult.intvalidationconfigurationid == ValidationConfiguration.intvalidationconfigurationid
                ).join(
                    ValidationMaster,
                    ValidationConfiguration.intvalidationmasterid == ValidationMaster.intvalidationmasterid
                ).filter(
                    ValidationMaster.intsubproductid == subproduct_id
                )
                
                # Filter by vcsourcetype if available
                if validation_source_type:
                    exception_query = exception_query.filter(ValidationMaster.vcsourcetype == validation_source_type)
                
                exception_count = exception_query.count()
                
                validation_counts['exceptions'] = exception_count
                
                print(f"Validation counts - total: {validation_counts['total']}, failed: {validation_counts['failed']}, passed: {validation_counts['passed']}, exceptions: {validation_counts['exceptions']}")
            
            # Step 5.5: Build subchecks structure (integrated with existing queries)
            subchecks = []
            if validation_process_id:
                try:
                    # Get process instance for datetime
                    process_instance = validation_process if validation_process else session.query(ProcessInstance).filter(
                        ProcessInstance.intprocessinstanceid == validation_process_id
                    ).first()
                    process_datetime = process_instance.dtprocesstime_start if process_instance else None
                    
                    # Query validation masters (grouped by validation master, not configuration)
                    # Filter by vcsourcetype to match process instance
                    validation_masters_query = session.query(ValidationMaster).filter(
                        ValidationMaster.intsubproductid == subproduct_id,
                        ValidationMaster.isactive == True
                    )
                    
                    # Filter by vcsourcetype if available
                    if validation_source_type:
                        validation_masters_query = validation_masters_query.filter(
                            ValidationMaster.vcsourcetype == validation_source_type
                        )
                    
                    validation_masters = validation_masters_query.all()
                    
                    # Group by subtype and build structure
                    subtype_dict = {}
                    for master in validation_masters:
                        subtype = master.vcsubtype or 'Uncategorized'
                        
                        if subtype not in subtype_dict:
                            subtype_dict[subtype] = {
                                'subtype': subtype,
                                'status': 'Not Completed',
                                'validations': {}
                            }
                        
                        validation_id = master.intvalidationmasterid
                        validation_name = master.vcvalidationname or f'Validation {validation_id}'
                        
                        # Skip if already processed (same validation master)
                        if validation_id in subtype_dict[subtype]['validations']:
                            continue
                        
                        # Get all configurations for this validation master
                        configs = session.query(ValidationConfiguration).filter(
                            ValidationConfiguration.intvalidationmasterid == validation_id,
                            ValidationConfiguration.isactive == True
                        ).all()
                        
                        # Check if any configuration has results
                        has_results = False
                        has_failed = False
                        
                        for config in configs:
                            # Check if this configuration has any results
                            config_has_results = session.query(ValidationResult).filter(
                                ValidationResult.intvalidationconfigurationid == config.intvalidationconfigurationid,
                                ValidationResult.intprocessinstanceid == validation_process_id,
                                ValidationResult.isactive == True
                            ).first() is not None
                            
                            if config_has_results:
                                has_results = True
                                
                                # Check if any result has failed status
                                config_has_failed = session.query(ValidationResult).filter(
                                    ValidationResult.intvalidationconfigurationid == config.intvalidationconfigurationid,
                                    ValidationResult.intprocessinstanceid == validation_process_id,
                                    ValidationResult.isactive == True,
                                    ValidationResult.vcstatus.ilike('%failed%')
                                ).first() is not None
                                
                                if config_has_failed:
                                    has_failed = True
                        
                        # Initialize validation entry
                        # Status is "Completed" if it has results (regardless of pass/fail)
                        validation_status = 'Completed' if has_results else 'Not Completed'
                        pass_fail = 'Fail' if has_failed else 'Pass'
                        
                        subtype_dict[subtype]['validations'][validation_id] = {
                            'validation_name': validation_name,
                            'description': master.vcdescription or '',
                            'status': validation_status,
                            'pass_fail': pass_fail,
                            'datetime': process_datetime.isoformat() if process_datetime else None
                        }
                    
                    # Convert to list format and determine subtype status based on all children
                    for subtype_data in subtype_dict.values():
                        # Subtype status is "Completed" only if ALL validations are completed
                        all_completed = all(
                            v['status'] == 'Completed' 
                            for v in subtype_data['validations'].values()
                        )
                        subtype_data['status'] = 'Completed' if all_completed else 'Not Completed'
                        
                        subchecks.append({
                            'subtype': subtype_data['subtype'],
                            'status': subtype_data['status'],
                            'validations': list(subtype_data['validations'].values())
                        })
                    
                    # Sort by subtype name
                    subchecks.sort(key=lambda x: x['subtype'])
                    
                except Exception as e:
                    logger.error(f"Error building subchecks: {e}")
                    import traceback
                    traceback.print_exc()
            
            # Step 6: Get ratio results with joins and filter by subproduct_id
            ratio_counts = {
                'total': 0,
                'failed': 0,
                'passed': 0
            }
            
            if ratio_process_id:
                # Debug: Check total ratio results at each filter stage
                total_ratio_all = session.query(RatioResult).filter(
                    RatioResult.intprocessinstanceid == ratio_process_id
                ).count()
                print(f"Total ratio results for process_instance_id={ratio_process_id} (no filters): {total_ratio_all}")
                
                total_ratio_active = session.query(RatioResult).filter(
                    RatioResult.intprocessinstanceid == ratio_process_id,
                    or_(RatioResult.isactive == True, RatioResult.isactive == None)
                ).count()
                print(f"Total ratio results for process_instance_id={ratio_process_id} (with isactive=True or NULL): {total_ratio_active}")
                
                # Check if there are any ratio results with joins (without subproduct filter)
                total_ratio_with_joins = session.query(RatioResult).join(
                    RatioConfiguration,
                    RatioResult.intratioconfigurationid == RatioConfiguration.intratioconfigurationid
                ).join(
                    RatioMaster,
                    RatioConfiguration.intratiomasterid == RatioMaster.intratiomasterid
                ).filter(
                    RatioResult.intprocessinstanceid == ratio_process_id,
                    or_(RatioResult.isactive == True, RatioResult.isactive == None)
                ).count()
                print(f"Total ratio results for process_instance_id={ratio_process_id} (with joins, no subproduct filter): {total_ratio_with_joins}")
                
                # Query ratio results with joins to filter by subproduct_id and vcsourcetype
                # Handle isactive - check for True or NULL (default is True)
                base_ratio_query = session.query(RatioResult).join(
                    RatioConfiguration,
                    RatioResult.intratioconfigurationid == RatioConfiguration.intratioconfigurationid
                ).join(
                    RatioMaster,
                    RatioConfiguration.intratiomasterid == RatioMaster.intratiomasterid
                ).filter(
                    RatioResult.intprocessinstanceid == ratio_process_id,
                    RatioMaster.intsubproductid == subproduct_id,
                    or_(RatioResult.isactive == True, RatioResult.isactive == None)
                )
                
                # Filter by vcsourcetype if available
                if ratio_source_type:
                    base_ratio_query = base_ratio_query.filter(RatioMaster.vcsourcetype == ratio_source_type)
                
                # Debug: Check count with all filters
                total_with_all_filters = base_ratio_query.count()
                print(f"Total ratio results for process_instance_id={ratio_process_id}, subproduct_id={subproduct_id} (with all filters): {total_with_all_filters}")
                
                # ratio_total: Count of unique intratioconfigurationid
                ratio_counts['total'] = base_ratio_query.with_entities(
                    func.count(distinct(RatioResult.intratioconfigurationid))
                ).scalar() or 0
                
                print(f"Found {ratio_counts['total']} unique ratio configurations for process_instance_id={ratio_process_id}, subproduct_id={subproduct_id}")
                
                # ratio_failed: Count of unique intratioconfigurationid that have at least one failed vcstatus
                failed_ratio_configs_query = base_ratio_query.filter(
                    RatioResult.vcstatus.ilike('%failed%')
                ).with_entities(
                    func.count(distinct(RatioResult.intratioconfigurationid))
                )
                ratio_counts['failed'] = failed_ratio_configs_query.scalar() or 0
                
                # ratio_passed: ratio_total - ratio_failed
                ratio_counts['passed'] = ratio_counts['total'] - ratio_counts['failed']
                              
                # Additional debug: Check if there are any ratio configurations at all
                if total_with_all_filters == 0 and total_ratio_with_joins > 0:
                    # Check what subproduct_ids exist in the ratio results
                    subproduct_ids_in_results = session.query(
                        distinct(RatioMaster.intsubproductid)
                    ).join(
                        RatioConfiguration,
                        RatioMaster.intratiomasterid == RatioConfiguration.intratiomasterid
                    ).join(
                        RatioResult,
                        RatioConfiguration.intratioconfigurationid == RatioResult.intratioconfigurationid
                    ).filter(
                        RatioResult.intprocessinstanceid == ratio_process_id,
                        or_(RatioResult.isactive == True, RatioResult.isactive == None)
                    ).all()
                    logger.warning(f"No ratio results found for subproduct_id={subproduct_id}. Available subproduct_ids in results: {[s[0] for s in subproduct_ids_in_results]}")
            
            # Step 7: Return summary dictionary
            return {
                'validation_total': validation_counts['total'],
                'validation_failed': validation_counts['failed'],
                'validation_passed': validation_counts['passed'],
                'validation_exceptions': validation_counts['exceptions'],
                'ratio_total': ratio_counts['total'],
                'ratio_failed': ratio_counts['failed'],
                'ratio_passed': ratio_counts['passed'],
                'validation_process_instance_id': validation_process_id,
                'ratio_process_instance_id': ratio_process_id,
                'subchecks': subchecks
            }
            
        except Exception as e:
            logger.error(f"Error getting process instance summary: {e}")
            return None
        finally:
            session.close()
    
    def get_validation_aggregated_data(
        self, 
        client_id: int,
        process_instance_id: Optional[int] = None,
        fund_id: Optional[int] = None,
        subproduct_id: Optional[int] = None,
        source_a: Optional[str] = None,
        date_a: Optional[str] = None,
        source_b: Optional[str] = None,
        date_b: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        Get validation aggregated data from validation results
        Returns list of validation data with:
        - vcvalidationname (from ValidationMaster)
        - type (from ValidationMaster)
        - subtype (from ValidationMaster)
        - config_threshold (from ValidationConfiguration)
        - status (Failed if any failed entry, else Passed)
        - exception (Count of Failed Status)
        
        Args:
            client_id: The client ID to get schema name
            process_instance_id: The process instance ID (optional)
            fund_id: The fund ID (required if process_instance_id not provided)
            subproduct_id: The subproduct ID (required if process_instance_id not provided)
            source_a: Source A (required if process_instance_id not provided)
            date_a: Date A (required if process_instance_id not provided)
            source_b: Source B (optional)
            date_b: Date B (optional)
        
        Returns:
            List of dictionaries with aggregated validation data
        """
        if not self.db_manager:
            return []
        
        # Validate parameters
        if process_instance_id is None:
            if not all([fund_id, subproduct_id, date_a]):
                logger.error("Either process_instance_id must be provided, or (fund_id, subproduct_id, source_a, date_a) must all be provided")
                return []
        
        # Get schema name from client
        session_public = self.db_manager.get_session_with_schema('public')
        try:
            from database_models import Client
            client = session_public.query(Client).filter(Client.id == client_id).first()
            if not client:
                logger.error(f"Client with id {client_id} not found")
                return []
            schema_name = client.code
        except Exception as e:
            logger.error(f"Error getting schema from client_id {client_id}: {e}")
            return []
        finally:
            session_public.close()
        
        session = self.db_manager.get_session_with_schema('validus')
        try:
            from database_models import (
                ValidationMaster, ValidationConfiguration, ProcessInstance,
                create_validation_result_model
            )
            from sqlalchemy import func, distinct, case, or_
            from datetime import datetime
            
            # If process_instance_id not provided, find it using other parameters
            if process_instance_id is None:
                # Convert date strings to date objects if needed
                try:
                    if isinstance(date_a, str):
                        date_a_obj = datetime.strptime(date_a, '%Y-%m-%d').date()
                    else:
                        date_a_obj = date_a
                    
                    if date_b:
                        if isinstance(date_b, str):
                            date_b_obj = datetime.strptime(date_b, '%Y-%m-%d').date()
                        else:
                            date_b_obj = date_b
                except (ValueError, TypeError) as e:
                    logger.error(f"Error parsing dates: {e}")
                    return []
                
                # Build filter conditions - handle empty strings as None
                filter_conditions = [
                    ProcessInstance.intclientid == client_id,
                    ProcessInstance.intfundid == fund_id,
                    ProcessInstance.dtdate_a == date_a_obj,
                    ProcessInstance.vcvalidustype == 'Validation'
                ]
                
                # Handle date_b - if not provided, check for NULL
                if date_b:
                    filter_conditions.append(ProcessInstance.dtdate_b == date_b_obj)
                else:
                    filter_conditions.append(ProcessInstance.dtdate_b == None)
                
                # Handle source_a and source_b - if empty string, check for NULL or empty
                if source_a:
                    filter_conditions.append(ProcessInstance.vcsource_a == source_a)
                else:
                    filter_conditions.append(or_(ProcessInstance.vcsource_a == None, ProcessInstance.vcsource_a == ''))
                
                # Use ilike for lower-case comparison (matching get_latest_process_instance_summary)
                if source_b:
                    filter_conditions.append(ProcessInstance.vcsource_b.ilike(source_b.lower()))
                else:
                    filter_conditions.append(ProcessInstance.vcsource_b.is_(None))
                
                # Get latest Validation process instance
                validation_process = session.query(ProcessInstance).filter(*filter_conditions).order_by(ProcessInstance.dtprocesstime_start.desc()).first()
                
                if not validation_process:
                    logger.warning(f"No validation process instance found for client_id={client_id}, fund_id={fund_id}, source_a='{source_a}', source_b='{source_b}', date_a={date_a_obj}, date_b={date_b_obj}")
                    return []
                
                process_instance_id = validation_process.intprocessinstanceid
                logger.info(f"Found process_instance_id={process_instance_id} for the given parameters")
            
            # Create dynamic schema model
            ValidationResult = create_validation_result_model(schema_name)
            # Step 1: Start query - base table
            print("Step 1: Starting query from ValidationResult")
            query = session.query(
                ValidationResult.intvalidationconfigurationid,
                ValidationMaster.vcvalidationname,
                ValidationMaster.vctype.label('type'),
                ValidationMaster.vcsubtype.label('subtype'),
                ValidationConfiguration.intthreshold.label('config_threshold')
            )

            print(f"  -> Base ValidationResult count: {session.query(ValidationResult).count()}")

            # Step 2: Join with ValidationConfiguration
            print("Step 2: Joining ValidationResult -> ValidationConfiguration")
            query = query.join(
                ValidationConfiguration,
                ValidationResult.intvalidationconfigurationid == ValidationConfiguration.intvalidationconfigurationid
            )
            print(f"  -> Count after joining ValidationConfiguration: {query.count()}")

            # Step 3: Join with ValidationMaster
            print("Step 3: Joining ValidationConfiguration -> ValidationMaster")
            query = query.join(
                ValidationMaster,
                ValidationConfiguration.intvalidationmasterid == ValidationMaster.intvalidationmasterid
            )
            print(f"  -> Count after joining ValidationMaster: {query.count()}")

            # Step 6: Apply filters
            print("Step 6: Applying filters")
            query = query.filter(
                ValidationResult.intprocessinstanceid == process_instance_id,
                or_(ValidationResult.isactive == True, ValidationResult.isactive == None)
            )
            print(f"  -> Count after filters: {query.count()}")

            # Step 7: Grouping results
            print("Step 7: Grouping query results")
            query = query.group_by(
                ValidationResult.intvalidationconfigurationid,
                ValidationMaster.vcvalidationname,
                ValidationMaster.vctype,
                ValidationMaster.vcsubtype,
                ValidationConfiguration.intthreshold
            )
            print("  -> Grouping complete (group_by doesn’t affect count until execution)")

            # Step 8: Execute query
            print("Step 8: Executing query to fetch all configurations")
            configs = query.all()
            print(f"  -> Retrieved {len(configs)} grouped configuration records")

            # Step 9: Process results
            print("Step 9: Processing each configuration to check validation status")
            result = []

            for idx, config in enumerate(configs, start=1):
                print(f"  - Processing config #{idx}: {config.vcvalidationname}")
                intvalidationconfigurationid = config.intvalidationconfigurationid

                # Step 9.1: Check failed entries
                print(f"    Checking failed entries for configuration ID {intvalidationconfigurationid}")
                # Count all failed entries for this process instance and validation configuration
                failed_count = session.query(ValidationResult).filter(
                    ValidationResult.intprocessinstanceid == process_instance_id,
                    ValidationResult.intvalidationconfigurationid == intvalidationconfigurationid,
                    ValidationResult.vcstatus.ilike('%failed%'),
                    or_(ValidationResult.isactive == True, ValidationResult.isactive == None)
                ).count()
                print(f"    -> Found {failed_count} failed entries for process_instance_id={process_instance_id}, intvalidationconfigurationid={intvalidationconfigurationid}")
                
                # Debug: Check total entries for this config to verify
                total_count = session.query(ValidationResult).filter(
                    ValidationResult.intprocessinstanceid == process_instance_id,
                    ValidationResult.intvalidationconfigurationid == intvalidationconfigurationid,
                    or_(ValidationResult.isactive == True, ValidationResult.isactive == None)
                ).count()
                print(f"    -> Total entries for this config: {total_count}")

                # Step 9.2: Determine status and exceptions
                status = 'Failed' if failed_count > 0 else 'Passed'
                exception = failed_count

                # Step 9.3: Append final record
                result.append({
                    'vcvalidationname': config.vcvalidationname,
                    'type': config.type,
                    'subtype': config.subtype,
                    'config_threshold': float(config.config_threshold) if config.config_threshold else None,
                    'status': status,
                    'exception': exception
                })
                print(f"    -> Status: {status}, Exception Count: {exception}")

                # Step 10: Final summary
                print(f"Step 10: Aggregation complete. Total final results: {len(result)}")
            return result
            
        except Exception as e:
            logger.error(f"Error getting validation aggregated data: {e}")
            import traceback
            traceback.print_exc()
            return []
        finally:
            session.close()
    
    def get_validation_comparison_data(
        self,
        client_id: int,
        process_instance_id: Optional[int] = None,
        fund_id: Optional[int] = None,
        subproduct_id: Optional[int] = None,
        source_a: Optional[str] = None,
        date_a: Optional[str] = None,
        source_b: Optional[str] = None,
        date_b: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        Get validation comparison data with side A and side B joined
        Returns list of validation results with matched sides
        
        Args:
            client_id: The client ID to get schema name
            process_instance_id: The process instance ID (optional)
            fund_id: The fund ID (required if process_instance_id not provided)
            subproduct_id: The subproduct ID (required if process_instance_id not provided)
            source_a: Source A (required if process_instance_id not provided)
            date_a: Date A (required if process_instance_id not provided)
            source_b: Source B (optional)
            date_b: Date B (optional)
        
        Returns:
            List of dictionaries with validation comparison data
        """
        if not self.db_manager:
            return []
        
        # Validate parameters
        if process_instance_id is None:
            if not all([fund_id, subproduct_id, date_a]):
                logger.error("Either process_instance_id must be provided, or (fund_id, subproduct_id, date_a) must all be provided")
                return []
        
        # Get schema name from client
        session_public = self.db_manager.get_session_with_schema('public')
        try:
            from database_models import Client
            client = session_public.query(Client).filter(Client.id == client_id).first()
            if not client:
                logger.error(f"Client with id {client_id} not found")
                return []
            schema_name = client.code
        except Exception as e:
            logger.error(f"Error getting schema from client_id {client_id}: {e}")
            return []
        finally:
            session_public.close()
        
        session = self.db_manager.get_session_with_schema('validus')
        try:
            from database_models import (
                ValidationMaster, ValidationConfiguration, ValidationDetails,
                ProcessInstance, DataModelMaster, DataModelDetails,
                create_validation_result_model
            )
            from sqlalchemy import func, distinct, case, or_, text
            from datetime import datetime
            import re
            
            # If process_instance_id not provided, find it using other parameters
            if process_instance_id is None:
                # Convert date strings to date objects if needed
                try:
                    if isinstance(date_a, str):
                        date_a_obj = datetime.strptime(date_a, '%Y-%m-%d').date()
                    else:
                        date_a_obj = date_a
                    
                    if date_b:
                        if isinstance(date_b, str):
                            date_b_obj = datetime.strptime(date_b, '%Y-%m-%d').date()
                        else:
                            date_b_obj = date_b
                except (ValueError, TypeError) as e:
                    logger.error(f"Error parsing dates: {e}")
                    return []
                
                # Build filter conditions - handle empty strings as None
                filter_conditions = [
                    ProcessInstance.intclientid == client_id,
                    ProcessInstance.intfundid == fund_id,
                    ProcessInstance.dtdate_a == date_a_obj,
                    ProcessInstance.vcvalidustype == 'Validation'
                ]
                
                # Handle date_b - if not provided, check for NULL
                if date_b:
                    filter_conditions.append(ProcessInstance.dtdate_b == date_b_obj)
                else:
                    filter_conditions.append(ProcessInstance.dtdate_b == None)
                
                # Handle source_a and source_b - if empty string, check for NULL or empty
                if source_a:
                    filter_conditions.append(ProcessInstance.vcsource_a == source_a)
                else:
                    filter_conditions.append(or_(ProcessInstance.vcsource_a == None, ProcessInstance.vcsource_a == ''))
                
                # Use ilike for lower-case comparison (matching get_latest_process_instance_summary)
                if source_b:
                    filter_conditions.append(ProcessInstance.vcsource_b.ilike(source_b.lower()))
                else:
                    filter_conditions.append(ProcessInstance.vcsource_b.is_(None))
                
                # Get latest Validation process instance
                validation_process = session.query(ProcessInstance).filter(*filter_conditions).order_by(ProcessInstance.dtprocesstime_start.desc()).first()
                
                if not validation_process:
                    logger.warning(f"No validation process instance found")
                    return []
                
                process_instance_id = validation_process.intprocessinstanceid
                logger.info(f"Found process_instance_id={process_instance_id}")
            
            # Create dynamic schema model
            ValidationResult = create_validation_result_model(schema_name)
            
            # Get all unique validation configurations for this process instance
            configs_query = session.query(
                ValidationResult.intvalidationconfigurationid,
                ValidationMaster.intvalidationmasterid,
                ValidationMaster.vcvalidationname,
                ValidationMaster.vctype,
                ValidationMaster.vcsubtype,
                ValidationMaster.vcdescription,
                ValidationConfiguration.intthreshold,
                ValidationConfiguration.vcthresholdtype,
                ValidationConfiguration.intprecision
            ).join(
                ValidationConfiguration,
                ValidationResult.intvalidationconfigurationid == ValidationConfiguration.intvalidationconfigurationid
            ).join(
                ValidationMaster,
                ValidationConfiguration.intvalidationmasterid == ValidationMaster.intvalidationmasterid
            ).filter(
                ValidationResult.intprocessinstanceid == process_instance_id,
                or_(ValidationResult.isactive == True, ValidationResult.isactive == None)
            ).distinct()
            
            configs = configs_query.all()
            
            if not configs:
                logger.warning(f"No validation configurations found for process_instance_id={process_instance_id}")
                return []
            
            all_results = []
            
            # Process each validation configuration
            for config in configs:
                intvalidationconfigurationid = config.intvalidationconfigurationid
                intvalidationmasterid = config.intvalidationmasterid
                vcvalidationname = config.vcvalidationname
                vctype = config.vctype
                vcsubtype = config.vcsubtype
                vcdescription = config.vcdescription
                intthreshold = config.intthreshold
                vcthresholdtype = config.vcthresholdtype
                intprecision = config.intprecision
                
                # Get validation details to determine data model and columns
                validation_detail = session.query(ValidationDetails).filter(
                    ValidationDetails.intvalidationmasterid == intvalidationmasterid
                ).first()
                
                if not validation_detail:
                    logger.warning(f"No validation details found for intvalidationmasterid={intvalidationmasterid}")
                    continue
                
                # Get data model table name
                data_model = session.query(DataModelMaster).filter(
                    DataModelMaster.intdatamodelid == validation_detail.intdatamodelid
                ).first()
                
                if not data_model or not data_model.vctablename:
                    logger.warning(f"No data model table found for intdatamodelid={validation_detail.intdatamodelid}")
                    continue
                
                data_model_table = data_model.vctablename
                
                # Get align keys (columns for joining/coalescing)
                align_keys = []
                if validation_detail.intgroup_attributeid:
                    group_attr = session.query(DataModelDetails).filter(
                        DataModelDetails.intdatamodeldetailid == validation_detail.intgroup_attributeid
                    ).first()
                    if group_attr and group_attr.vcdmcolumnname:
                        align_keys.append(group_attr.vcdmcolumnname)
                
                if validation_detail.intassettypeid:
                    asset_type = session.query(DataModelDetails).filter(
                        DataModelDetails.intdatamodeldetailid == validation_detail.intassettypeid
                    ).first()
                    if asset_type and asset_type.vcdmcolumnname:
                        align_keys.append(asset_type.vcdmcolumnname)
                
                # Extract column name from vcformula
                formula_column = None
                if validation_detail.vcformula:
                    match = re.search(r'\[([^\[\]]+)\](?!.*\[)', validation_detail.vcformula)
                    if match:
                        formula_column = match.group(1).replace(' ', '').lower()
                
                if not formula_column:
                    logger.warning(f"Could not extract column from formula: {validation_detail.vcformula}")
                    continue
                # print(f"Debug - align_keys: {align_keys}")
                # Build align keys for COALESCE (use first align key as description column)
                coalesce_column = align_keys[0] if align_keys else 'intrecid'  # Fallback to intrecid if no align keys
                assettype_column = align_keys[1] if len(align_keys) > 1 else None  # Second align key is assettype if present
                
                # Helper function to escape SQL strings
                def escape_sql_string(value):
                    if value is None:
                        return 'NULL'
                    # Replace single quotes with double single quotes for SQL
                    return "'" + str(value).replace("'", "''") + "'"
                
                # Build SQL query dynamically
                # Side A query
                side_a_select = [
                    f"res.intprocessinstanceid",
                    f"{escape_sql_string(vcvalidationname)} AS validations, {escape_sql_string(vctype)} AS type, {escape_sql_string(vcsubtype)} AS subtype, {escape_sql_string(vcdescription)} AS description, {intthreshold if intthreshold is not None else 'NULL'} AS threshold, {escape_sql_string(vcthresholdtype)} AS threshold_type, {intprecision if intprecision is not None else 'NULL'} AS precision",
                    f"res.intmatchid",
                    f"res.intdmrecid",
                    f"res.vcstatus",
                    f"res.vcside",
                    f"res.intformulaoutput"
                ]
                
                # Helper to quote column names safely
                def quote_column(col_name):
                    # Quote column names to handle special characters
                    return f'"{col_name}"' if col_name else None
                
                # Add coalesce column (description column) - rename to security_name
                side_a_select.append(f'pf.{quote_column(coalesce_column)} AS security_name')
                
                # Add assettype column if present - rename to security_type
                if assettype_column:
                    side_a_select.append(f'pf.{quote_column(assettype_column)} AS security_type')
                
                # Add formula column (cast to numeric to handle text/varchar columns)
                # Use safe casting that handles NULL and invalid values
                formula_col_quoted_inner = quote_column(formula_column)
                formula_cast = f"""CASE WHEN pf.{formula_col_quoted_inner} IS NULL OR pf.{formula_col_quoted_inner} = '' THEN NULL ELSE CAST(NULLIF(TRIM(pf.{formula_col_quoted_inner}), '') AS NUMERIC) END AS {formula_col_quoted_inner}"""
                side_a_select.append(formula_cast)
                
                side_a_sql = f"""
                    SELECT {', '.join(side_a_select)}
                    FROM {schema_name}.tbl_validation_result AS res
                    JOIN validus.tbl_validation_configuration AS vc
                        ON res.intvalidationconfigurationid = vc.intvalidationconfigurationid
                    JOIN validus.tbl_validation_master AS vm
                        ON vc.intvalidationmasterid = vm.intvalidationmasterid
                    LEFT JOIN {schema_name}.{data_model_table} AS pf
                        ON res.intdmrecid = pf.intrecid
                    WHERE res.intprocessinstanceid = :process_instance_id
                      AND res.intvalidationconfigurationid = :config_id
                      AND res.vcside = 'A'
                      AND (res.isactive = true OR res.isactive IS NULL)
                """
                
                # Side B query (same structure)
                side_b_sql = f"""
                    SELECT {', '.join(side_a_select)}
                    FROM {schema_name}.tbl_validation_result AS res
                    JOIN validus.tbl_validation_configuration AS vc
                        ON res.intvalidationconfigurationid = vc.intvalidationconfigurationid
                    JOIN validus.tbl_validation_master AS vm
                        ON vc.intvalidationmasterid = vm.intvalidationmasterid
                    LEFT JOIN {schema_name}.{data_model_table} AS pf
                        ON res.intdmrecid = pf.intrecid
                    WHERE res.intprocessinstanceid = :process_instance_id
                      AND res.intvalidationconfigurationid = :config_id
                      AND res.vcside = 'B'
                      AND (res.isactive = true OR res.isactive IS NULL)
                """
                
                # Build final SELECT with COALESCE for description, assettype, and status
                coalesce_col_quoted = quote_column(coalesce_column)
                formula_col_quoted = quote_column(formula_column)
                
                final_select = [
                    "a.intprocessinstanceid",
                    "a.validations",
                    "a.type",
                    "a.subtype",
                    "a.description",
                    "a.threshold",
                    "a.threshold_type",
                    "a.precision",
                    "a.intmatchid",
                    f"COALESCE(a.security_name, b.security_name) AS security_name"
                ]
                
                # Add assettype column with COALESCE if present - rename to security_type
                if assettype_column:
                    final_select.append(f"COALESCE(a.security_type, b.security_type) AS security_type")
                
                # Add formula values and other fields (already cast to numeric in subqueries)
                final_select.extend([
                    f"a.{formula_col_quoted} AS Source_A_value",
                    f"b.{formula_col_quoted} AS Source_B_value",
                    "COALESCE(a.vcstatus, b.vcstatus) AS status",
                    "a.intformulaoutput AS intformulaoutput"
                ])
                
                # Add tooltip calculation based on threshold_type
                # If threshold_type is % or Percentage: signed difference (source_b_value - source_a_value)
                # Else: abs((source_b_value - source_a_value))*100/source_a_value
                # Note: formula columns are already cast to NUMERIC in subqueries
                tooltip_case = f"""
                    CASE 
                        WHEN LOWER(a.threshold_type) IN ('%', 'percentage') THEN 
                            CASE 
                                WHEN b.{formula_col_quoted} IS NOT NULL AND a.{formula_col_quoted} IS NOT NULL THEN
                                    (b.{formula_col_quoted} - a.{formula_col_quoted})
                                ELSE NULL
                            END
                        ELSE 
                            CASE 
                                WHEN a.{formula_col_quoted} != 0 AND a.{formula_col_quoted} IS NOT NULL 
                                     AND b.{formula_col_quoted} IS NOT NULL THEN
                                    ABS((b.{formula_col_quoted} - a.{formula_col_quoted})) * 100.0 / a.{formula_col_quoted}
                                ELSE NULL
                            END
                    END AS tooltip
                """
                final_select.append(tooltip_case)
                
                # Final join query with COALESCE for description column, assettype, and status
                final_sql = f"""
                    SELECT 
                        {', '.join(final_select)}
                    FROM ({side_a_sql}) a
                    LEFT JOIN ({side_b_sql}) b
                        ON a.intmatchid = b.intmatchid
                    ORDER BY a.intmatchid
                """
                
                # Execute query
                try:
                    result = session.execute(
                        text(final_sql),
                        {
                            'process_instance_id': process_instance_id,
                            'config_id': intvalidationconfigurationid
                        }
                    )
                    
                    # Helper function to determine decimal places from precision value
                    def get_decimal_places(precision_val):
                        """Convert precision value to number of decimal places.
                        If precision is 0.12, returns 2. If precision is 0.123, returns 3.
                        If precision is an integer like 2, returns 2.
                        """
                        if precision_val is None:
                            return 2  # Default to 2 decimal places
                        
                        # Convert to float if it's a Decimal or string
                        if isinstance(precision_val, Decimal):
                            precision_val = float(precision_val)
                        elif isinstance(precision_val, str):
                            try:
                                precision_val = float(precision_val)
                            except (ValueError, TypeError):
                                return 2  # Default if conversion fails
                        
                        # If it's already an integer, use it directly
                        if isinstance(precision_val, int):
                            return precision_val
                        
                        # If it's a float, count decimal places
                        if isinstance(precision_val, float):
                            # Convert to string with high precision, then remove trailing zeros
                            precision_str = f"{precision_val:.15f}".rstrip('0').rstrip('.')
                            if '.' in precision_str:
                                # Count digits after decimal point
                                decimal_part = precision_str.split('.')[1]
                                return len(decimal_part) if decimal_part else 0
                            else:
                                # No decimal point (e.g., 1.0 became "1"), return 0
                                return 0
                        
                        return 2  # Default fallback
                    
                    # Get the number of decimal places from precision
                    decimal_places = get_decimal_places(intprecision)
                    
                    for row in result:
                        row_dict = dict(row._mapping)
                        
                        # Format Source_A_value and Source_B_value according to precision and convert to string
                        # Handle both uppercase and lowercase key variations
                        source_a_key = None
                        source_b_key = None
                        for key in row_dict.keys():
                            if key.lower() == 'source_a_value':
                                source_a_key = key
                            elif key.lower() == 'source_b_value':
                                source_b_key = key
                        
                        if source_a_key:
                            source_a_val = row_dict[source_a_key]
                            if source_a_val is not None:
                                # Convert to float if it's a numeric type
                                if isinstance(source_a_val, Decimal):
                                    source_a_val = float(source_a_val)
                                elif isinstance(source_a_val, (int, float)):
                                    source_a_val = float(source_a_val)
                                elif isinstance(source_a_val, str):
                                    # Try to convert string to float
                                    try:
                                        source_a_val = float(source_a_val)
                                    except (ValueError, TypeError):
                                        # If conversion fails, keep as string
                                        pass
                                
                                # Format as string with precision if it's numeric
                                if isinstance(source_a_val, (int, float)):
                                    row_dict[source_a_key] = f"{source_a_val:.{decimal_places}f}"
                                else:
                                    # Keep as string if not numeric
                                    row_dict[source_a_key] = str(source_a_val)
                            else:
                                row_dict[source_a_key] = None
                        
                        if source_b_key:
                            source_b_val = row_dict[source_b_key]
                            if source_b_val is not None:
                                # Convert to float if it's a numeric type
                                if isinstance(source_b_val, Decimal):
                                    source_b_val = float(source_b_val)
                                elif isinstance(source_b_val, (int, float)):
                                    source_b_val = float(source_b_val)
                                elif isinstance(source_b_val, str):
                                    # Try to convert string to float
                                    try:
                                        source_b_val = float(source_b_val)
                                    except (ValueError, TypeError):
                                        # If conversion fails, keep as string
                                        pass
                                
                                # Format as string with precision if it's numeric
                                if isinstance(source_b_val, (int, float)):
                                    row_dict[source_b_key] = f"{source_b_val:.{decimal_places}f}"
                                else:
                                    # Keep as string if not numeric
                                    row_dict[source_b_key] = str(source_b_val)
                            else:
                                row_dict[source_b_key] = None
                        
                        # Format intformulaoutput according to precision and convert to string
                        if 'intformulaoutput' in row_dict:
                            intformula_val = row_dict['intformulaoutput']
                            if intformula_val is not None:
                                if isinstance(intformula_val, Decimal):
                                    intformula_val = float(intformula_val)
                                if isinstance(intformula_val, (float, int)):
                                    row_dict['intformulaoutput'] = f"{float(intformula_val):.{decimal_places}f}"
                                else:
                                    row_dict['intformulaoutput'] = str(intformula_val)
                            else:
                                row_dict['intformulaoutput'] = None
                        
                        # Convert other Decimal values to float for JSON serialization
                        # Exclude source_a_value, source_b_value (any case) and intformulaoutput
                        excluded_keys = ['intformulaoutput']
                        if source_a_key:
                            excluded_keys.append(source_a_key)
                        if source_b_key:
                            excluded_keys.append(source_b_key)
                        
                        for key, value in row_dict.items():
                            if key.lower() not in [k.lower() for k in excluded_keys]:
                                if isinstance(value, Decimal):
                                    row_dict[key] = float(value)
                                elif value is None:
                                    row_dict[key] = None
                        
                        # Add validation_name to the data dictionary
                        if 'validations' in row_dict:
                            row_dict['validation_name'] = row_dict['validations']
                        
                        all_results.append(row_dict)
                        
                except Exception as e:
                    logger.error(f"Error executing query for validation {vcvalidationname}: {e}")
                    import traceback
                    traceback.print_exc()
                    # Rollback the transaction to allow subsequent queries
                    session.rollback()
                    continue
            
            return all_results
            
        except Exception as e:
            logger.error(f"Error getting validation comparison data: {e}")
            import traceback
            traceback.print_exc()
            # Rollback transaction on error
            try:
                session.rollback()
            except:
                pass
            return []
        finally:
            session.close()
    
    def get_ratio_comparison_data(
        self,
        client_id: int,
        process_instance_id: Optional[int] = None,
        fund_id: Optional[int] = None,
        subproduct_id: Optional[int] = None,
        source_a: Optional[str] = None,
        date_a: Optional[str] = None,
        source_b: Optional[str] = None,
        date_b: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        Get ratio comparison data with side A and side B joined
        Returns list of ratio results with matched sides
        
        Args:
            client_id: The client ID to get schema name
            process_instance_id: The process instance ID (optional)
            fund_id: The fund ID (required if process_instance_id not provided)
            subproduct_id: The subproduct ID (required if process_instance_id not provided)
            source_a: Source A (required if process_instance_id not provided)
            date_a: Date A (required if process_instance_id not provided)
            source_b: Source B (optional)
            date_b: Date B (optional)
        
        Returns:
            List of dictionaries with ratio comparison data including:
            - intratiomasterid, intthreshold, vcthresholdtype, intprecision (from RatioConfiguration)
            - vcrationame, vctype, vcdescription (from RatioMaster)
            - vcformula, vcnumerator, vcdenominator (from RatioDetails)
            - source_a_value, source_b_value, intnumeratoroutput, intdenominatoroutput, vcformulaoutput, vcstatus (from RatioResult, combined by matchid)
            - tooltipinfo
        """
        if not self.db_manager:
            return []
        
        # Validate parameters
        if process_instance_id is None:
            if not all([fund_id, subproduct_id, date_a]):
                logger.error("Either process_instance_id must be provided, or (fund_id, subproduct_id, date_a) must all be provided")
                return []
        
        # Get schema name from client
        session_public = self.db_manager.get_session_with_schema('public')
        try:
            from database_models import Client
            client = session_public.query(Client).filter(Client.id == client_id).first()
            if not client:
                logger.error(f"Client with id {client_id} not found")
                return []
            schema_name = client.code
        except Exception as e:
            logger.error(f"Error getting schema from client_id {client_id}: {e}")
            return []
        finally:
            session_public.close()
        
        session = self.db_manager.get_session_with_schema('validus')
        try:
            from database_models import (
                RatioMaster, RatioConfiguration, RatioDetails,
                ProcessInstance, DataModelMaster, DataModelDetails,
                create_ratio_result_model
            )
            from datetime import datetime
            from decimal import Decimal
            import re
            
            # If process_instance_id not provided, find it using other parameters
            if process_instance_id is None:
                # Convert date strings to date objects if needed
                try:
                    if isinstance(date_a, str):
                        date_a_obj = datetime.strptime(date_a, '%Y-%m-%d').date()
                    else:
                        date_a_obj = date_a
                    
                    # For dual source (2 sources, 1 period), date_b should be None
                    # For single source (1 source, 2 periods), date_b should be provided
                    date_b_obj = None
                    if date_b:
                        if isinstance(date_b, str):
                            date_b_obj = datetime.strptime(date_b, '%Y-%m-%d').date()
                        else:
                            date_b_obj = date_b
                except (ValueError, TypeError) as e:
                    logger.error(f"Error parsing dates: {e}")
                    return []
                
                # Build filter conditions
                filter_conditions = [
                    ProcessInstance.intclientid == client_id,
                    ProcessInstance.intfundid == fund_id,
                    ProcessInstance.dtdate_a == date_a_obj,
                    ProcessInstance.vcvalidustype == 'Ratio'
                ]
                
                # Handle date_b: For dual source (2 sources, 1 period), date_b should be None
                # For single source (1 source, 2 periods), date_b should match
                if date_b_obj is not None:
                    # Single source scenario: 1 source, 2 periods
                    filter_conditions.append(ProcessInstance.dtdate_b == date_b_obj)
                else:
                    # Dual source scenario: 2 sources, 1 period (date_b should be NULL)
                    # Match records where dtdate_b is NULL (for dual source)
                    filter_conditions.append(ProcessInstance.dtdate_b == None)
                
                # Handle source_a and source_b
                if source_a:
                    filter_conditions.append(ProcessInstance.vcsource_a == source_a)
                else:
                    filter_conditions.append(or_(ProcessInstance.vcsource_a == None, ProcessInstance.vcsource_a == ''))
                
                if source_b:
                    filter_conditions.append(ProcessInstance.vcsource_b == source_b)
                else:
                    filter_conditions.append(or_(ProcessInstance.vcsource_b == None, ProcessInstance.vcsource_b == ''))
                
                # Get latest Ratio process instance
                ratio_process = session.query(ProcessInstance).filter(*filter_conditions).order_by(ProcessInstance.dtprocesstime_start.desc()).first()
                
                if not ratio_process:
                    logger.warning(f"No ratio process instance found for client_id={client_id}, fund_id={fund_id}, source_a='{source_a}', source_b='{source_b}', date_a={date_a_obj}, date_b={date_b_obj}")
                    return []
                
                process_instance_id = ratio_process.intprocessinstanceid
                logger.info(f"Found process_instance_id={process_instance_id} for the given parameters")
            
            # Create dynamic schema model
            RatioResult = create_ratio_result_model(schema_name)
            
            # Get all ratio configurations for this process instance
            ratio_configs = session.query(RatioConfiguration).join(
                RatioResult,
                RatioConfiguration.intratioconfigurationid == RatioResult.intratioconfigurationid
            ).filter(
                RatioResult.intprocessinstanceid == process_instance_id,
                or_(RatioResult.isactive == True, RatioResult.isactive == None)
            ).distinct().all()
            
            all_results = []
            
            for ratio_config in ratio_configs:
                try:
                    # Get ratio master
                    ratio_master = session.query(RatioMaster).filter(
                        RatioMaster.intratiomasterid == ratio_config.intratiomasterid
                    ).first()
                    
                    if not ratio_master:
                        continue
                    
                    # Get ratio details
                    ratio_details = session.query(RatioDetails).filter(
                        RatioDetails.intratiomasterid == ratio_master.intratiomasterid
                    ).first()
                    
                    if not ratio_details:
                        continue
                    
                    # Get data model for align keys
                    data_model = session.query(DataModelMaster).filter(
                        DataModelMaster.intdatamodelid == ratio_details.intdatamodelid
                    ).first()
                    
                    if not data_model:
                        continue
                    
                    # Get align keys from ratio details (if they exist)
                    # Note: RatioDetails may not have these fields, so we check with getattr
                    align_key_id = getattr(ratio_details, 'intgroup_attributeid', None)
                    asset_type_id = getattr(ratio_details, 'intassettypeid', None)
                    
                    align_key_column = None
                    asset_type_column = None
                    
                    if align_key_id:
                        align_key_detail = session.query(DataModelDetails).filter(
                            DataModelDetails.intdatamodeldetailid == align_key_id
                        ).first()
                        if align_key_detail:
                            align_key_column = align_key_detail.vcdmcolumnname
                    
                    if asset_type_id:
                        asset_type_detail = session.query(DataModelDetails).filter(
                            DataModelDetails.intdatamodeldetailid == asset_type_id
                        ).first()
                        if asset_type_detail:
                            asset_type_column = asset_type_detail.vcdmcolumnname
                    
                    # Extract column name from formula (last [Column Name] in formula)
                    formula_column_match = re.search(r'\[([^\[\]]+)\](?!.*\[)', ratio_details.vcformula or '')
                    formula_column = formula_column_match.group(1).replace(' ', '').lower() if formula_column_match else None
                    
                    # Build dynamic SQL query similar to validation comparison
                    # Join side A and side B on matchid
                    table_name = data_model.vctablename
                    schema_table = f"{schema_name}.{table_name}"
                    
                    # Escape SQL identifiers
                    def escape_sql_string(s):
                        if s is None:
                            return 'NULL'
                        return "'" + str(s).replace("'", "''") + "'"

                    
                    def quote_column(col):
                        if col is None:
                            return None
                        return f'"{col}"'
                    
                    # Build COALESCE for align key
                    align_key_coalesce = f"COALESCE(a.{quote_column(align_key_column)}, b.{quote_column(align_key_column)})" if align_key_column else "NULL"
                    
                    # Build COALESCE for asset type
                    asset_type_coalesce = f"COALESCE(a.{quote_column(asset_type_column)}, b.{quote_column(asset_type_column)})" if asset_type_column else "NULL"
                    
                    # Build formula column references
                    formula_col_a = f"a.{quote_column(formula_column)}" if formula_column else "NULL"
                    formula_col_b = f"b.{quote_column(formula_column)}" if formula_column else "NULL"
                    
                    # Build align key column selection
                    align_key_select = f"pf.{quote_column(align_key_column)}" if align_key_column else "NULL"
                    
                    # Build subquery for side A
                    subquery_a = f"""
                        SELECT 
                            res.intprocessinstanceid,
                            rm.vcrationame AS ratios,
                            res.intmatchid,
                            res.intratioconfigurationid,
                            res.vcside,
                            res.intnumeratoroutput,
                            res.intdenominatoroutput,
                            res.intformulaoutput,
                            res.vcformulaoutput,
                            res.vcstatus,
                            {align_key_select} AS align_key_value
                        FROM {schema_name}.tbl_ratio_result AS res
                        JOIN validus.tbl_process_instance AS pi ON res.intprocessinstanceid = pi.intprocessinstanceid
                        JOIN validus.tbl_ratio_configuration AS rc ON res.intratioconfigurationid = rc.intratioconfigurationid
                        JOIN validus.tbl_ratio_master AS rm ON rc.intratiomasterid = rm.intratiomasterid
                        LEFT JOIN {schema_table} AS pf ON res.intsideuniqueid = pf.intrecid
                        WHERE res.intprocessinstanceid = {process_instance_id}
                        AND res.intratioconfigurationid = {ratio_config.intratioconfigurationid}
                        AND res.vcside = 'A'
                        AND (res.isactive = TRUE OR res.isactive IS NULL)
                    """
                    
                    # Build subquery for side B
                    subquery_b = f"""
                        SELECT 
                            res.intprocessinstanceid,
                            rm.vcrationame AS ratios,
                            res.intmatchid,
                            res.intratioconfigurationid,
                            res.vcside,
                            res.intnumeratoroutput,
                            res.intdenominatoroutput,
                            res.intformulaoutput,
                            res.vcformulaoutput,
                            res.vcstatus,
                            {align_key_select} AS align_key_value
                        FROM {schema_name}.tbl_ratio_result AS res
                        JOIN validus.tbl_process_instance AS pi ON res.intprocessinstanceid = pi.intprocessinstanceid
                        JOIN validus.tbl_ratio_configuration AS rc ON res.intratioconfigurationid = rc.intratioconfigurationid
                        JOIN validus.tbl_ratio_master AS rm ON rc.intratiomasterid = rm.intratiomasterid
                        LEFT JOIN {schema_table} AS pf ON res.intsideuniqueid = pf.intrecid
                        WHERE res.intprocessinstanceid = {process_instance_id}
                        AND res.intratioconfigurationid = {ratio_config.intratioconfigurationid}
                        AND res.vcside = 'B'
                        AND (res.isactive = TRUE OR res.isactive IS NULL)
                    """
                    
                    # Main query joining A and B
                    main_query = f"""
                        SELECT 
                            a.intprocessinstanceid,
                            a.ratios,
                            a.intmatchid,
                            {align_key_coalesce} AS align_key,
                            {asset_type_coalesce} AS assettype,
                            a.intratioconfigurationid,
                            rc.intratiomasterid,
                            rc.intthreshold,
                            rc.vcthresholdtype,
                            rc.intprecision,
                            rm.vcrationame,
                            rm.vctype,
                            rm.vcdescription,
                            rd.vcformula,
                            rd.vcnumerator,
                            rd.vcdenominator,
                            a.intnumeratoroutput AS intnumeratoroutput_a,
                            b.intnumeratoroutput AS intnumeratoroutput_b,
                            a.intdenominatoroutput AS intdenominatoroutput_a,
                            b.intdenominatoroutput AS intdenominatoroutput_b,
                            a.intnumeratoroutput/a.intdenominatoroutput AS source_a_value,
                            b.intnumeratoroutput/b.intdenominatoroutput AS source_b_value,
                            a.vcformulaoutput AS vcformulaoutput_a,
                            b.vcformulaoutput AS vcformulaoutput_b,
                            COALESCE(a.vcstatus, b.vcstatus) AS vcstatus
                        FROM ({subquery_a}) a
                        LEFT JOIN ({subquery_b}) b ON a.intmatchid = b.intmatchid
                        JOIN validus.tbl_ratio_configuration AS rc ON a.intratioconfigurationid = rc.intratioconfigurationid
                        JOIN validus.tbl_ratio_master AS rm ON rc.intratiomasterid = rm.intratiomasterid
                        JOIN validus.tbl_ratio_details AS rd ON rm.intratiomasterid = rd.intratiomasterid
                        WHERE a.vcside = 'A'
                        ORDER BY a.intmatchid
                    """
                    
                    # Execute query
                    result = session.execute(text(main_query))
                    
                    # Get threshold type for tooltip calculation
                    threshold_type = ratio_config.vcthresholdtype or ratio_master.vcthresholdtype or ''
                    
                    for row in result:
                        row_dict = dict(row._mapping)
                        
                        # Convert Decimal values to float
                        for key, value in row_dict.items():
                            if isinstance(value, Decimal):
                                row_dict[key] = float(value)
                            elif value is None:
                                row_dict[key] = None
                        
                        # Calculate tooltip
                        source_a_value = row_dict.get('source_a_value')
                        source_b_value = row_dict.get('source_b_value')
                        
                        tooltipinfo = None
                        if source_a_value is not None and source_b_value is not None:
                            if threshold_type and ('%' in threshold_type.lower() or 'percentage' in threshold_type.lower()):
                                # Percentage difference
                                if source_a_value != 0:
                                    tooltipinfo = abs((source_b_value - source_a_value) * 100 / source_a_value)
                                else:
                                    tooltipinfo = None
                            else:
                                # Signed difference
                                tooltipinfo = source_b_value - source_a_value
                        
                        row_dict['tooltipinfo'] = tooltipinfo

                        row_dict['numerator_name'] = extractFormulaFromDisplayName(row_dict['vcnumerator'])[0]
                        row_dict['denominator_name'] = extractFormulaFromDisplayName(row_dict['vcdenominator'])[0]
                        row_dict['flow_data'] = {
                            "nodes": [
                                {
                                    "id": "numerator",
                                    "type": "card",
                                    "data": {
                                        "label": row_dict['numerator_name'],
                                        "value": row_dict['intnumeratoroutput_b']
                                    },
                                },
                                {
                                    "id": "denominator",
                                    "type": "card",
                                    "data": {
                                        "label": row_dict['denominator_name'],
                                        "value": row_dict['intdenominatoroutput_b']
                                    },
                                },
                                {
                                    "id": "ratio",
                                    "type": "formula",
                                    "data": {
                                        "label": row_dict['vcrationame'],
                                        "formula": f"= {row_dict['numerator_name']} / {row_dict['denominator_name']}"
                                    },
                                },
                                {
                                    "id": "result",
                                    "type": "result",
                                    "data": {
                                        "label": row_dict['vcrationame'],
                                        "value": row_dict['source_b_value']
                                    },
                                }
                            ]
                        }
                        all_results.append(row_dict)
                        
                except Exception as e:
                    logger.error(f"Error processing ratio configuration {ratio_config.intratioconfigurationid}: {e}")
                    import traceback
                    traceback.print_exc()
                    session.rollback()
                    continue
            
            return all_results
            
        except Exception as e:
            logger.error(f"Error getting ratio comparison data: {e}")
            import traceback
            traceback.print_exc()
            # Rollback transaction on error
            try:
                session.rollback()
            except:
                pass
            return []
        finally:
            session.close()
    
    def get_report_ingested_data(
        self,
        client_id: int,
        fund_id: int,
        source_a: Optional[str] = None,
        source_b: Optional[str] = None,
        date_a: Optional[str] = None,
        date_b: Optional[str] = None
    ) -> Optional[Dict[str, Any]]:
        """
        Get ingested reports for a particular client, fund, source(s) and date(s)
        
        Args:
            client_id: Client ID
            fund_id: Fund ID
            source_a: Source A (required for dual source)
            source_b: Source B (optional, for dual source)
            date_a: Date A (required)
            date_b: Date B (optional, for single source with 2 dates)
        
        Returns:
            Dictionary with:
            - data_section: List of groups (by source/date) with files
            - version_section: List of categories, each with groups (by source/date) with files
        """
        if not self.db_manager:
            return None
        
        if not date_a:
            logger.error("date_a is required")
            return None
        
        session = self.db_manager.get_session_with_schema('validus')
        try:
            from database_models import DataLoadInstance, DataModelMaster
            from datetime import datetime
            import os
            
            # Parse dates
            try:
                if isinstance(date_a, str):
                    date_a_obj = datetime.strptime(date_a, '%Y-%m-%d').date()
                else:
                    date_a_obj = date_a
                
                date_b_obj = None
                if date_b:
                    if isinstance(date_b, str):
                        date_b_obj = datetime.strptime(date_b, '%Y-%m-%d').date()
                    else:
                        date_b_obj = date_b
            except (ValueError, TypeError) as e:
                logger.error(f"Error parsing dates: {e}")
                return None
            
            # Determine if dual source (2 sources, 1 date) or single source (1 source, 2 dates)
            is_dual_source = (source_a and source_b and not date_b_obj)
            is_single_source_two_dates = (source_a and not source_b and date_b_obj)
            
            # Build query for DataLoadInstance
            query = session.query(DataLoadInstance).join(
                DataModelMaster,
                DataLoadInstance.intdatamodelid == DataModelMaster.intdatamodelid
            ).filter(
                DataLoadInstance.intclientid == client_id,
                DataLoadInstance.intfundid == fund_id
            )
            
            # Filter by dates
            if is_dual_source:
                # Dual source: 2 sources, 1 date (date_a)
                query = query.filter(DataLoadInstance.dtdataasof == date_a_obj)
            elif is_single_source_two_dates:
                # Single source: 1 source, 2 dates
                query = query.filter(DataLoadInstance.dtdataasof.in_([date_a_obj, date_b_obj]))
            else:
                # Default: use date_a
                query = query.filter(DataLoadInstance.dtdataasof == date_a_obj)
            
            # Filter by sources
            if is_dual_source:
                # Dual source: filter by both sources
                query = query.filter(
                    or_(
                        DataLoadInstance.vcdatasourcename.ilike(source_a.lower()) if source_a else True,
                        DataLoadInstance.vcdatasourcename.ilike(source_b.lower()) if source_b else True
                    )
                )
            elif source_a:
                # Single source: filter by source_a
                query = query.filter(DataLoadInstance.vcdatasourcename.ilike(source_a.lower()))
            
            # Order by load time
            query = query.order_by(DataLoadInstance.dtloadedat.desc())
            
            data_load_instances = query.all()
            
            if not data_load_instances:
                return {
                    'data_section': [],
                    'version_section': []
                }
            
            # Helper function to extract file format from file name
            def extract_file_format(file_name: str) -> str:
                if not file_name:
                    return 'Unknown'
                _, ext = os.path.splitext(file_name)
                return ext.lstrip('.').upper() if ext else 'Unknown'
            
            # Build Data Section (grouped by source/date)
            data_section_dict = {}
            
            for instance in data_load_instances:
                # Determine group key (source name or date)
                if is_dual_source:
                    group_key = instance.vcdatasourcename or 'Unknown Source'
                else:
                    # Single source: group by date
                    group_key = str(instance.dtdataasof) if instance.dtdataasof else 'Unknown Date'
                
                if group_key not in data_section_dict:
                    data_section_dict[group_key] = []
                
                data_model = instance.data_model
                category = data_model.vcmodelname if data_model else 'Unknown Category'
                
                file_name = instance.vcdataloaddescription or 'Unknown'
                file_format = extract_file_format(file_name)
                source = instance.vcloadtype or 'Unknown'
                time = instance.dtloadedat.isoformat() if instance.dtloadedat else ''
                status = instance.vcloadstatus or 'Unknown'
                
                data_section_dict[group_key].append({
                    'category': category,
                    'file_name': file_name,
                    'file_format': file_format,
                    'source': source,
                    'time': time,
                    'status': status
                })
            
            # Convert data section to list format
            data_section = [
                {
                    'group_key': group_key,
                    'files': [
                        {
                            'category': file['category'],
                            'file_name': file['file_name'],
                            'file_format': file['file_format'],
                            'source': file['source'],
                            'time': file['time'],
                            'status': file['status']
                        }
                        for file in files
                    ]
                }
                for group_key, files in data_section_dict.items()
            ]
            
            # Build Version Section (grouped by source/date first, then by category)
            # First, calculate version numbers for each unique combination
            # Group instances by version key and sort by dtloadedat to assign version numbers
            version_groups = {}  # Key: (intclientid, intfundid, vcdatasourcename, dtdataasof, intdatamodelid) -> [instances]
            
            for instance in data_load_instances:
                # Create unique key for version counting
                version_key = (
                    instance.intclientid,
                    instance.intfundid,
                    instance.vcdatasourcename or '',
                    instance.dtdataasof,
                    instance.intdatamodelid
                )
                
                if version_key not in version_groups:
                    version_groups[version_key] = []
                version_groups[version_key].append(instance)
            
            # Sort each group by dtloadedat to determine version order
            for version_key in version_groups:
                version_groups[version_key].sort(
                    key=lambda x: x.dtloadedat if x.dtloadedat else datetime.min
                )
            
            # Create a mapping from instance ID to version number
            instance_to_version = {}
            for version_key, instances in version_groups.items():
                for idx, instance in enumerate(instances, start=1):
                    instance_to_version[instance.intdataloadinstanceid] = idx
            
            # Now build version section grouped by source/date first, then by category
            version_section_dict = {}  # Key: group_key (source/date) -> {category: [files]}
            
            for instance in data_load_instances:
                # Determine group key (source name or date) - FIRST LEVEL GROUPING
                if is_dual_source:
                    group_key = instance.vcdatasourcename or 'Unknown Source'
                else:
                    # Single source: group by date
                    group_key = str(instance.dtdataasof) if instance.dtdataasof else 'Unknown Date'
                
                if group_key not in version_section_dict:
                    version_section_dict[group_key] = {}
                
                # Get category - SECOND LEVEL GROUPING
                data_model = instance.data_model
                category = data_model.vcmodelname if data_model else 'Unknown Category'
                
                if category not in version_section_dict[group_key]:
                    version_section_dict[group_key][category] = []
                
                # Get version number from mapping
                version = instance_to_version.get(instance.intdataloadinstanceid, 1)
                
                file_name = instance.vcdataloaddescription or 'Unknown'
                file_format = extract_file_format(file_name)
                source = instance.vcloadtype or 'Unknown'
                time = instance.dtloadedat.isoformat() if instance.dtloadedat else ''
                status = instance.vcloadstatus or 'Unknown'
                
                version_section_dict[group_key][category].append({
                    'file_name': file_name,
                    'file_format': file_format,
                    'source': source,
                    'time': time,
                    'status': status,
                    'version': str(version)
                })
            
            # Convert version section to list format
            # Structure: group_key (source/date) -> categories -> files
            version_section = [
                {
                    'group_key': group_key,
                    'categories': [
                        {
                            'category': category,
                            'files': files
                        }
                        for category, files in categories.items()
                    ]
                }
                for group_key, categories in version_section_dict.items()
            ]
            
            return {
                'data_section': data_section,
                'version_section': version_section
            }
            
        except Exception as e:
            logger.error(f"Error getting report ingested data: {e}")
            import traceback
            traceback.print_exc()
            return None
        finally:
            session.close()
    
    def get_unique_data_load_combinations(
        self,
        client_id: Optional[int] = None,
        fund_id: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """
        Get unique combinations of (client, fund, source, date) from tbl_data_load_instance
        Grouped by fund and source
        
        Args:
            client_id: Optional client ID to filter by
            fund_id: Optional fund ID to filter by
        
        Returns:
            List of dictionaries grouped by fund and source, with dates as a list
            Format: [
                {
                    'client_id': int,
                    'fund_id': int,
                    'source': str,
                    'dates': [str, ...]  # List of ISO format dates
                },
                ...
            ]
        """
        if not self.db_manager:
            return []
        
        session = self.db_manager.get_session_with_schema('validus')
        try:
            from database_models import DataLoadInstance
            from collections import defaultdict
            
            # Query all records with the required fields
            base_query = session.query(
                DataLoadInstance.intclientid,
                DataLoadInstance.intfundid,
                DataLoadInstance.vcdatasourcename,
                DataLoadInstance.dtdataasof
            ).filter(
                DataLoadInstance.intclientid.isnot(None),
                DataLoadInstance.intfundid.isnot(None),
                DataLoadInstance.vcdatasourcename.isnot(None),
                DataLoadInstance.dtdataasof.isnot(None)
            )
            
            # Apply filters if provided
            if client_id is not None:
                base_query = base_query.filter(DataLoadInstance.intclientid == client_id)
            
            if fund_id is not None:
                base_query = base_query.filter(DataLoadInstance.intfundid == fund_id)
            
            # Get all records
            records = base_query.all()
            
            # Group by (client_id, fund_id, source) and collect unique dates
            grouped = defaultdict(set)
            
            for record in records:
                combo_key = (
                    record.intclientid,
                    record.intfundid,
                    record.vcdatasourcename
                )
                
                if record.dtdataasof:
                    grouped[combo_key].add(record.dtdataasof)
            
            # Convert to result format
            result = []
            for (client_id_val, fund_id_val, source), dates_set in grouped.items():
                dates_list = sorted([date.isoformat() for date in dates_set])
                result.append({
                    'client_id': client_id_val,
                    'fund_id': fund_id_val,
                    'source': source,
                    'dates': dates_list
                })
            
            # Sort by client_id, fund_id, source
            result.sort(key=lambda x: (x['client_id'], x['fund_id'], x['source'] or ''))
            
            return result
            
        except Exception as e:
            logger.error(f"Error getting unique data load combinations: {e}")
            import traceback
            traceback.print_exc()
            return []
        finally:
            session.close()

db_validation_service = DatabaseValidationService()



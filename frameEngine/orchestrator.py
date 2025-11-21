import logging
import os
from pathlib import Path
from dotenv import load_dotenv
import time
from typing import Dict, Any, Optional
from dataclasses import dataclass
import json
# Import all the boxes
from .boxes.ingestion_box import IngestionBox
from .boxes.ocr_box import OCRBox
from .boxes.preprocessing_box import PreprocessingBox
from .boxes.classification_box import ClassificationBox
from .boxes.extraction_box import ExtractionBox
from .boxes.bounding_box_box import BoundingBoxBox
from .boxes.validation_enrichment_box import ValidationEnrichmentBox
from .boxes.output_box import OutputBox
from .data_model import AithonDocument

# Import advanced systems
from .exceptions import (
    BaseAithonException, 
    DocumentProcessingError,
    ErrorContext,
    ExceptionHandler
)
from .monitoring import (
    get_metrics_collector,
    get_performance_monitor,
    get_document_metrics,
    OperationTimer,
    monitor_operation
)

from .dashboard import generate_dashboard, get_status, get_files_summary

# Import database models for storing extracted content
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
try:
    from database_models import (
        get_database_manager, Document, CapitalCallsExtraction, 
        DistributionsExtraction, StatementsExtraction, Fund
    )
    from sqlalchemy import func
    DATABASE_AVAILABLE = True
except ImportError as e:
    print(f"Warning: Database models not available: {e}")
    DATABASE_AVAILABLE = False

# Load environment variables from .env file
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

# Reduce log noise from verbose libraries
logging.getLogger("openai").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("PIL").setLevel(logging.WARNING)
logging.getLogger("pytesseract").setLevel(logging.WARNING)

class AithonOrchestrator:
    """
    Enhanced Orchestrator with advanced patterns from bot_service:
    - Comprehensive monitoring and metrics collection
    - Advanced exception handling and recovery
    - Performance tracking and optimization
    - Detailed logging and observability
    - Error recovery and retry mechanisms
    """
    
    def __init__(self):
        logging.info("Initializing Enhanced Aithon Orchestrator and all boxes...")
        
        # Initialize monitoring systems
        self.metrics = get_metrics_collector()
        self.performance_monitor = get_performance_monitor()
        self.doc_metrics = get_document_metrics()
        self.exception_handler = ExceptionHandler()
        
        # Initialize boxes
        self.ingestion_box = IngestionBox()
        self.ocr_box = OCRBox()
        self.preprocessing_box = PreprocessingBox()
        self.classification_box = ClassificationBox()
        self.extraction_box = ExtractionBox()
        self.bounding_box_box = BoundingBoxBox()
        self.validation_enrichment_box = ValidationEnrichmentBox()
        self.output_box = OutputBox()
        
        # Setup recovery strategies
        self._setup_recovery_strategies()
        
        # Initialize database connection if available
        self.db_manager = None
        if DATABASE_AVAILABLE:
            try:
                self.db_manager = get_database_manager()
                logging.info("Database connection initialized successfully.")
            except Exception as e:
                logging.warning(f"Failed to initialize database connection: {e}")
                self.db_manager = None
        
        # Record initialization metrics
        self.metrics.increment_counter("orchestrator_initializations_total")
        
        logging.info("All boxes initialized with advanced monitoring.")
    
    def _setup_recovery_strategies(self):
        """Setup recovery strategies for different exception types"""
        def retry_with_delay(exception, context):
            """Generic retry strategy with exponential backoff"""
            retry_count = context.get("retry_count", 0)
            if retry_count < 3:
                delay = 2 ** retry_count
                logging.warning(f"Retrying operation after {delay}s delay (attempt {retry_count + 1})")
                time.sleep(delay)
                return True
            return False
        
        # Register recovery strategies
        self.exception_handler.register_recovery_strategy(
            DocumentProcessingError, 
            retry_with_delay
        )
    
    def store_extracted_content_in_database(self, doc: 'AithonDocument', file_path: Path) -> bool:
        """
        Store extracted content in database tables based on document type.
        
        Args:
            doc: The processed AithonDocument with extracted data
            file_path: Path to the original file
            
        Returns:
            bool: True if successful, False otherwise
        """
        if not DATABASE_AVAILABLE or not self.db_manager:
            logging.warning("Database not available, skipping database storage")
            return False
        
        try:
            session = self.db_manager.get_session()
            
            # Create or update Document record
            document_record = self._create_document_record(session, doc, file_path)
            if not document_record:
                return False
            
            # Store extracted data based on document type
            success = False
            raw_doc_type = doc.document_type
            normalized_doc_type = self._normalize_document_type(raw_doc_type)
            
            # Store in appropriate table based on normalized document type
            if normalized_doc_type == 'capital_call':
                success = self._store_capital_call_data(session, doc, document_record.id)
                logging.info(f"Storing '{raw_doc_type}' (normalized: {normalized_doc_type}) in capital_calls table")
            elif normalized_doc_type == 'distribution':
                success = self._store_distribution_data(session, doc, document_record.id)
                logging.info(f"Storing '{raw_doc_type}' (normalized: {normalized_doc_type}) in distributions table")
            elif normalized_doc_type == 'statement':
                success = self._store_statement_data(session, doc, document_record.id)
                logging.info(f"Storing '{raw_doc_type}' (normalized: {normalized_doc_type}) in statements table")
            else:
                logging.warning(f"Unknown document type '{raw_doc_type}' (normalized: {normalized_doc_type}), storing as generic document")
                success = True  # Document record created successfully
            
            if success:
                session.commit()
                logging.info(f"Successfully stored {normalized_doc_type} data for document: {file_path.name}")
                return True
            else:
                session.rollback()
                logging.error(f"Failed to store extracted data for document: {file_path.name}")
                return False
                
        except Exception as e:
            logging.error(f"Error storing extracted content in database: {e}")
            if 'session' in locals():
                session.rollback()
            return False
        finally:
            if 'session' in locals():
                session.close()
    
    def _normalize_document_type(self, doc_type: str) -> str:
        """
        Normalize document type to standard format for database storage.
        
        Args:
            doc_type: Raw document type from classification
            
        Returns:
            str: Normalized document type ('capital_call', 'distribution', 'statement', or 'unknown')
        """
        if not doc_type:
            return 'unknown'
        
        doc_type_lower = doc_type.lower().strip()
        
        # Capital call variations
        capital_call_variations = [
            'capital_call', 'capitalcall', 'capcall', 'capital_call_document',
            'capital call', 'capital-call', 'cap_call', 'cap-call',
            'capitalcall_document', 'capital_call_notice'
        ]
        
        # Distribution variations  
        distribution_variations = [
            'distribution', 'distributions', 'dist', 'distribution_document',
            'distribution notice', 'distribution-notice', 'dist_notice',
            'distributions_document', 'income_distribution'
        ]
        
        # Statement variations
        statement_variations = [
            'statement', 'statements', 'financial_statement', 'stmt', 'statement_document',
            'financial statement', 'financial-statement', 'account_statement',
            'statements_document', 'monthly_statement', 'quarterly_statement'
        ]
        
        # Check for matches
        if doc_type_lower in capital_call_variations:
            return 'capital_call'
        elif doc_type_lower in distribution_variations:
            return 'distribution'
        elif doc_type_lower in statement_variations:
            return 'statement'
        else:
            return 'unknown'
    
    def _create_document_record(self, session, doc: 'AithonDocument', file_path: Path):
        """Create or update Document record in database"""
        try:
            # Check if document already exists
            existing_doc = session.query(Document).filter(
                Document.name == file_path.name,
                Document.path == str(file_path)
            ).first()
            
            if existing_doc:
                # Update existing document
                existing_doc.status = 'completed'
                existing_doc.type = doc.document_type
                existing_doc.document_metadata = doc.metadata
                existing_doc.updated_at = func.now()
                return existing_doc
            
            # Create new document record
            document_record = Document(
                name=file_path.name,
                type=doc.document_type,
                path=str(file_path),
                size=file_path.stat().st_size if file_path.exists() else 0,
                status='completed',
                document_metadata=doc.metadata,
                created_by='aithon_orchestrator'
            )
            
            session.add(document_record)
            session.flush()  # Get the ID
            return document_record
            
        except Exception as e:
            logging.error(f"Error creating document record: {e}")
            return None
   
    def _store_capital_call_data(self, session, doc: 'AithonDocument', doc_id: int) -> bool:
        """Store capital call extracted data"""
        try:
            extracted_data = doc.extracted_data or {}
            print("EXTRACTED DATA: ", extracted_data)
            
            # Extract common fields
            investor = self._extract_field_value(extracted_data, 'Investor')
            account = self._extract_field_value(extracted_data, 'Account')
            investor_ref_id = self._extract_field_value(extracted_data, 'InvestorRefID')
            security = self._extract_field_value(extracted_data, 'Security')
            
            # Extract financial fields
            capital_call = self._safe_extract_number(extracted_data, ['CapitalCall', 'Capital_Call'])
            distribution = self._safe_extract_number(extracted_data, ['Distribution'])
            committed_capital = self._safe_extract_number(extracted_data, ['CommittedCapital'])
            transaction_date = self._extract_date_field(extracted_data, 'TransactionDate')
            
            # Extract additional financial fields
            deemed_gp_contribution = self._safe_extract_number(extracted_data, ['DeemedGPContribution'])
            investments = self._safe_extract_number(extracted_data, ['Investments'])
            management_fee_inside_commitment = self._safe_extract_number(extracted_data, ['ManagementFeeInsideCommitment'])
            management_fee_outside_commitment = self._safe_extract_number(extracted_data, ['ManagementFeeOutsideCommitment'])
            partnership_expenses = self._safe_extract_number(extracted_data, ['PartnershipExpenses'])
            partnership_expenses_accounting_admin_it = self._safe_extract_number(extracted_data, ['PartnershipExpensesAccountingAdminIT'])
            partnership_expenses_audit_tax = self._safe_extract_number(extracted_data, ['PartnershipExpensesAuditTax'])
            partnership_expenses_bank_fees = self._safe_extract_number(extracted_data, ['PartnershipExpensesBankFees'])
            partnership_expenses_custody_fees = self._safe_extract_number(extracted_data, ['PartnershipExpensesCustodyFees'])
            partnership_expenses_due_diligence = self._safe_extract_number(extracted_data, ['PartnershipExpensesDueDiligence'])
            partnership_expenses_legal = self._safe_extract_number(extracted_data, ['PartnershipExpensesLegal'])
            partnership_expenses_organization_costs = self._safe_extract_number(extracted_data, ['PartnershipExpensesOrganizationCosts'])
            partnership_expenses_travel_entertainment = self._safe_extract_number(extracted_data, ['PartnershipExpensesTravelEntertainment'])
            partnership_expenses_other = self._safe_extract_number(extracted_data, ['PartnershipExpensesOther'])
            placement_agent_fees = self._safe_extract_number(extracted_data, ['PlacementAgentFees'])
            subsequent_close_interest = self._safe_extract_number(extracted_data, ['SubsequentCloseInterest'])
            working_capital = self._safe_extract_number(extracted_data, ['WorkingCapital'])
            
            # Debug logging
            logging.info(f"Extracted values - CapitalCall: {capital_call}, Distribution: {distribution}, CommittedCapital: {committed_capital}")
            
            capital_call_record = CapitalCallsExtraction(
                doc_id=doc_id,
                Investor=investor,
                Account=account,
                InvestorRefID=investor_ref_id,
                Security=security,
                TransactionDate=transaction_date,
                CapitalCall=capital_call,
                Distribution=distribution,
                CommittedCapital=committed_capital,
                Currency=self._extract_field_value(extracted_data, 'Currency'),
                DeemedGPContribution=deemed_gp_contribution,
                Investments=investments,
                ManagementFeeInsideCommitment=management_fee_inside_commitment,
                ManagementFeeOutsideCommitment=management_fee_outside_commitment,
                PartnershipExpenses=partnership_expenses,
                PartnershipExpensesAccountingAdminIT=partnership_expenses_accounting_admin_it,
                PartnershipExpensesAuditTax=partnership_expenses_audit_tax,
                PartnershipExpensesBankFees=partnership_expenses_bank_fees,
                PartnershipExpensesCustodyFees=partnership_expenses_custody_fees,
                PartnershipExpensesDueDiligence=partnership_expenses_due_diligence,
                PartnershipExpensesLegal=partnership_expenses_legal,
                PartnershipExpensesOrganizationCosts=partnership_expenses_organization_costs,
                PartnershipExpensesTravelEntertainment=partnership_expenses_travel_entertainment,
                PartnershipExpensesOther=partnership_expenses_other,
                PlacementAgentFees=placement_agent_fees,
                SubsequentCloseInterest=subsequent_close_interest,
                WorkingCapital=working_capital,
                extraction=json.dumps("aithon_orchestrator"),
                # extraction='aithon_orchestrator',
                document_name=doc.metadata.get('filename', ''),
                ConfidenceScore=str(doc.metadata.get('extraction_quality_score', 0.0))
            )
            
            session.add(capital_call_record)
            return True
            
        except Exception as e:
            logging.error(f"Error storing capital call data: {e}")
            return False
    
    def _store_distribution_data(self, session, doc: 'AithonDocument', doc_id: int) -> bool:
        """Store distribution extracted data"""
        try:
            extracted_data = doc.extracted_data or {}
            
            # Extract common fields
            investor = self._extract_field_value(extracted_data, 'Investor')
            account = self._extract_field_value(extracted_data, 'Account')
            investor_ref_id = self._extract_field_value(extracted_data, 'InvestorRefID')
            account_ref_id = self._extract_field_value(extracted_data, 'AccountRefID')
            security = self._extract_field_value(extracted_data, 'Security')
            transaction_date = self._extract_date_field(extracted_data, 'TransactionDate')
            
            # Extract financial fields
            distribution = self._safe_extract_number(extracted_data, ['Distribution'])
            income_distribution = self._safe_extract_number(extracted_data, ['IncomeDistribution'])
            income_reinvested = self._safe_extract_number(extracted_data, ['IncomeReinvested'])
            recallable_sell = self._safe_extract_number(extracted_data, ['RecallableSell'])
            return_of_capital = self._safe_extract_number(extracted_data, ['ReturnOfCapital'])
            distribution_outside_commitment = self._safe_extract_number(extracted_data, ['DistributionOutsideCommitment'])
            capital_call = self._safe_extract_number(extracted_data, ['CapitalCall'])
            capital_call_outside_commitment = self._safe_extract_number(extracted_data, ['CapitalCallOutsideCommitment'])
            net_cash_flow_qc = self._safe_extract_number(extracted_data, ['NetCashFlowQC'])
            transfer_out = self._safe_extract_number(extracted_data, ['TransferOut'])
            quantity = self._safe_extract_number(extracted_data, ['Quantity'])
            price = self._safe_extract_number(extracted_data, ['Price'])
            committed_capital = self._safe_extract_number(extracted_data, ['CommittedCapital'])
            remaining_committed_capital = self._safe_extract_number(extracted_data, ['RemainingCommittedCapital'])
            contributions_to_date = self._safe_extract_number(extracted_data, ['ContributionsToDate'])
            distributions_to_date = self._safe_extract_number(extracted_data, ['DistributionsToDate'])
            return_of_capital_to_date = self._safe_extract_number(extracted_data, ['ReturnOfCapitalToDate'])
            deemed_capital_call = self._safe_extract_number(extracted_data, ['DeemeedCapitalCall'])
            
            # Extract new distribution fields
            carry = self._safe_extract_number(extracted_data, ['Carry'])
            clawback = self._safe_extract_number(extracted_data, ['Clawback'])
            realized_gain_cash = self._safe_extract_number(extracted_data, ['RealizedGainCash'])
            realized_gain_stock = self._safe_extract_number(extracted_data, ['RealizedGainStock'])
            realized_loss_cash = self._safe_extract_number(extracted_data, ['RealizedLossCash'])
            realized_loss_stock = self._safe_extract_number(extracted_data, ['RealizedLossStock'])
            return_of_capital_management_fees = self._safe_extract_number(extracted_data, ['ReturnOfCapitalManagementFees'])
            return_of_capital_partnership_expenses = self._safe_extract_number(extracted_data, ['ReturnOfCapitalPartnershipExpenses'])
            return_of_capital_stock = self._safe_extract_number(extracted_data, ['ReturnOfCapitalStock'])
            temporary_return_of_capital_management_fees = self._safe_extract_number(extracted_data, ['TemporaryReturnOfCapitalManagementFees'])
            subsequent_close_interest = self._safe_extract_number(extracted_data, ['SubsequentCloseInterest'])
            other = self._safe_extract_number(extracted_data, ['Other'])

            distribution_record = DistributionsExtraction(
                doc_id=doc_id,
                Investor=investor,
                Account=account,
                InvestorRefID=investor_ref_id,
                AccountRefID=account_ref_id,
                Security=security,
                TransactionDate=transaction_date,
                Currency=self._extract_field_value(extracted_data, 'Currency'),
                Distribution=distribution,
                DeemeedCapitalCall=deemed_capital_call,
                IncomeDistribution=income_distribution,
                IncomeReinvested=income_reinvested,
                RecallableSell=recallable_sell,
                ReturnOfCapital=return_of_capital,
                DistributionOutsideCommitment=distribution_outside_commitment,
                CapitalCall=capital_call,
                CapitalCallOutsideCommitment=capital_call_outside_commitment,
                NetCashFlowQC=net_cash_flow_qc,
                TransferOut=transfer_out,
                Quantity=quantity,
                Price=price,
                CommittedCapital=committed_capital,
                RemainingCommittedCapital=remaining_committed_capital,
                ContributionsToDate=contributions_to_date,
                DistributionsToDate=distributions_to_date,
                ReturnOfCapitalToDate=return_of_capital_to_date,
                # New distribution fields
                Carry=carry,
                Clawback=clawback,
                RealizedGainCash=realized_gain_cash,
                RealizedGainStock=realized_gain_stock,
                RealizedLossCash=realized_loss_cash,
                RealizedLossStock=realized_loss_stock,
                ReturnOfCapitalManagementFees=return_of_capital_management_fees,
                ReturnOfCapitalPartnershipExpenses=return_of_capital_partnership_expenses,
                ReturnOfCapitalStock=return_of_capital_stock,
                TemporaryReturnOfCapitalManagementFees=temporary_return_of_capital_management_fees,
                SubsequentCloseInterest=subsequent_close_interest,
                Other=other,
                extraction=json.dumps("aithon_orchestrator"),
                document_name=doc.metadata.get('filename', ''),
                ConfidenceScore=str(doc.metadata.get('extraction_quality_score', 0.0))
            )
            
            session.add(distribution_record)
            return True
            
        except Exception as e:
            logging.error(f"Error storing distribution data: {e}")
            return False
    
    def _store_statement_data(self, session, doc: 'AithonDocument', doc_id: int) -> bool:
        """Store statement extracted data"""
        try:
            extracted_data = doc.extracted_data or {}
            
            # Extract common fields
            investor = self._extract_field_value(extracted_data, 'Investor')
            account = self._extract_field_value(extracted_data, 'Account')
            investor_ref_id = self._extract_field_value(extracted_data, 'InvestorRefID')
            security = self._extract_field_value(extracted_data, 'Security')
            
            # Extract financial fields
            net_opening_capital = self._safe_extract_number(extracted_data, ['NetOpeningCapital', 'OpeningCapital'])
            contributions = self._safe_extract_number(extracted_data, ['Contributions'])
            withdrawals = self._safe_extract_number(extracted_data, ['Withdrawals'])
            
            # Extract period dates
            period_beginning = self._extract_date_field(extracted_data, 'PeriodBeginningDT')
            period_ending = self._extract_date_field(extracted_data, 'PeriodEndingDT')
            
            statement_record = StatementsExtraction(
                doc_id=doc_id,
                Investor=investor,
                Account=account,
                InvestorRefID=investor_ref_id,
                Security=security,
                PeriodBeginningDT=period_beginning,
                PeriodEndingDT=period_ending,
                NetOpeningCapital=net_opening_capital,
                Contributions=contributions,
                Withdrawals=withdrawals,
                Currency=self._extract_field_value(extracted_data, 'Currency'),
                extraction=json.dumps("aithon_orchestrator"),
                # extraction='aithon_orchestrator',
                document_name=doc.metadata.get('filename', ''),
                ConfidenceScore=str(doc.metadata.get('extraction_quality_score', 0.0))
            )
            
            session.add(statement_record)
            return True
            
        except Exception as e:
            logging.error(f"Error storing statement data: {e}")
            return False
    
    def _extract_field_value(self, extracted_data: dict, field_name: str) -> str:
        """Extract field value from nested extracted_data structure"""
        try:
            # Debug logging
            logging.debug(f"Extracting field '{field_name}' from data structure")
            
            if not isinstance(extracted_data, dict):
                logging.debug(f"Extracted data is not a dict: {type(extracted_data)}")
                return None
            
            entities = extracted_data.get("entities", [])
            logging.debug(f"Found {len(entities)} entities")
            
            for i, entity in enumerate(entities):
                if not isinstance(entity, dict):
                    continue
                    
                portfolio = entity.get("portfolio", [])
                logging.debug(f"Entity {i} has {len(portfolio)} portfolio items")
                
                for j, portfolio_item in enumerate(portfolio):
                    if not isinstance(portfolio_item, dict):
                        continue
                        
                    field_data = portfolio_item.get(field_name)
                    logging.debug(f"Portfolio item {j} field '{field_name}': {field_data}")
                    
                    if isinstance(field_data, dict):
                        value = field_data.get("Value")
                        if value is not None:
                            # Handle both string and numeric values
                            if isinstance(value, str) and value.strip():
                                logging.debug(f"Found string value for '{field_name}': '{value.strip()}'")
                                return value.strip()
                            elif isinstance(value, (int, float)):
                                logging.debug(f"Found numeric value for '{field_name}': '{value}'")
                                return str(value)
                    elif field_data is not None:
                        # Try direct value (in case structure is different)
                        logging.debug(f"Direct value for '{field_name}': '{field_data}'")
                        return str(field_data).strip()
            
            logging.debug(f"No value found for field '{field_name}'")
            return None
        except Exception as e:
            logging.error(f"Error extracting field '{field_name}': {e}")
            return None
    
    def _safe_extract_number(self, extracted_data: dict, field_names: list) -> float:
        """Safely extract numeric value from various field locations"""
        for field_name in field_names:
            value = self._extract_field_value(extracted_data, field_name)
            if value:
                try:
                    cleaned = str(value).replace(",", "").replace("$", "").replace("(", "-").replace(")", "").strip()
                    return float(cleaned)
                except ValueError:
                    continue
        return 0.0
    
    def _extract_date_field(self, extracted_data: dict, field_name: str):
        """Extract date field and convert to proper format"""
        try:
            from datetime import datetime
            value = self._extract_field_value(extracted_data, field_name)
            if value:
                # Try various date formats
                for fmt in ['%Y-%m-%d', '%m/%d/%Y', '%d/%m/%Y', '%Y-%m-%d %H:%M:%S']:
                    try:
                        return datetime.strptime(value, fmt).date()
                    except ValueError:
                        continue
            return None
        except Exception:
            return None
    
    @monitor_operation("document_pipeline")
    def run_pipeline(self, file_path: Path) -> Dict[str, Any]:
        """
        Enhanced pipeline execution with comprehensive monitoring and error handling.
        
        Args:
            file_path: The path to the source document to process.
            
        Returns:
            Dict containing processing results and metrics
        """
        doc = None
        pipeline_start_time = time.time()
        processing_result = {
            "success": False,
            "filename": file_path.name,
            "processing_time": 0,
            "stages_completed": [],
            "errors": [],
            "metrics": {}
        }
        
        # Record pipeline start
        self.metrics.increment_counter("pipeline_starts_total", 1, {"filename": file_path.name})
        
        with ErrorContext("document_pipeline", self.exception_handler) as error_ctx:
            error_ctx.add_context("filename", file_path.name)
            error_ctx.add_context("file_size", file_path.stat().st_size if file_path.exists() else 0)
            
            try:
                # 1. Ingestion
                with OperationTimer(self.performance_monitor, "ingestion") as timer:
                    doc = self.ingestion_box(file_path)
                    doc.add_event("INFO", "Ingestion", f"Successfully ingested '{file_path.name}'. Document ID: {doc.doc_id}")
                    processing_result["stages_completed"].append("ingestion")
                    
                    # Record ingestion metrics
                    self.metrics.increment_counter("ingestion_success_total")
                    self.metrics.record_timer("ingestion_duration_seconds", timer.monitor.active_operations[timer.operation_id]["start_time"])
                
                # 2. OCR
                with OperationTimer(self.performance_monitor, "ocr") as timer:
                    doc = self.ocr_box(doc)
                    if doc.error_message:
                        doc.add_event("ERROR", "OCR", f"OCR processing failed: {doc.error_message}")
                        raise DocumentProcessingError(doc.error_message, filename=file_path.name)
                    doc.add_event("INFO", "OCR", f"Successfully processed through OCR Box: {file_path.name}")
                    processing_result["stages_completed"].append("ocr")
                    
                    # Record OCR metrics
                    self.metrics.increment_counter("ocr_success_total")
                    ocr_quality = getattr(doc, 'ocr_quality_score', 0.0)
                    self.metrics.observe_histogram("ocr_quality_score", ocr_quality)

                # 3. Pre-processing
                with OperationTimer(self.performance_monitor, "preprocessing") as timer:
                    doc = self.preprocessing_box(doc)
                    
                    # Enhanced preprocessing metrics
                    quality_score = doc.metadata.get('preprocessing_quality', {}).get('quality_score', 0.0)
                    quality_details = {"quality_score": quality_score} if quality_score else None
                    doc.add_event("INFO", "Preprocessing", f"Successfully processed through Enhanced Preprocessing Box: {file_path.name}", quality_details)
                    processing_result["stages_completed"].append("preprocessing")
                    
                    # Record preprocessing metrics
                    self.metrics.increment_counter("preprocessing_success_total")
                    self.metrics.observe_histogram("preprocessing_quality_score", quality_score)

                # 4. Classification
                with OperationTimer(self.performance_monitor, "classification") as timer:
                    doc = self.classification_box(doc)
                    if doc.error_message:
                        doc.add_event("ERROR", "Classification", f"Classification failed: {doc.error_message}")
                        raise DocumentProcessingError(doc.error_message, filename=file_path.name)
                    
                    # Enhanced classification metrics
                    classification_details = {
                        "document_type": doc.document_type,
                        "confidence": doc.classification_confidence,
                        "confidence_level": "HIGH" if doc.classification_confidence >= 0.9 else "MEDIUM" if doc.classification_confidence >= 0.8 else "LOW"
                    }
                    doc.add_event("INFO", "Classification", f"Classified '{file_path.name}' as: {doc.document_type} (confidence: {doc.classification_confidence:.2f})", classification_details)
                    processing_result["stages_completed"].append("classification")
                    
                    # Record classification metrics
                    provider = doc.metadata.get("llm_provider", "unknown")
                    self.doc_metrics.record_classification_result(doc.document_type, doc.classification_confidence, provider)

                    # 5. Extraction
                    with OperationTimer(self.performance_monitor, "extraction") as timer:
                        try:
                            doc = self.extraction_box(doc)
                            if doc.error_message:
                                doc.add_event("ERROR", "Extraction", f"Extraction failed: {doc.error_message}")
                                raise DocumentProcessingError(doc.error_message, filename=file_path.name)
                        except Exception as e:
                            logging.error(f"❌ EXTRACTION BOX EXCEPTION: {str(e)}")
                            logging.error(f"Exception type: {type(e).__name__}")
                            import traceback
                            logging.error(f"Traceback: {traceback.format_exc()}")
                            doc.add_event("ERROR", "Extraction", f"Extraction exception: {str(e)}")
                            raise DocumentProcessingError(f"Extraction failed: {str(e)}", filename=file_path.name)
                    
                    # DEBUG: Check if extracted_data was produced
                    # The extraction box stores data in doc.extracted_data, not doc.metadata['extracted_data']
                    extracted_data = doc.extracted_data or {}
                    logging.info(f"Extraction box output - extracted_data present: {doc.extracted_data is not None}")
                    logging.info(f"Extraction box output - extracted_data type: {type(extracted_data)}")
                    logging.info(f"Extraction box output - extracted_data keys: {list(extracted_data.keys()) if isinstance(extracted_data, dict) else 'Not a dict'}")
                    
                    # DEBUG: Check all metadata keys to see what the extraction box actually produced
                    logging.info(f"All metadata keys: {list(doc.metadata.keys())}")
                    for key, value in doc.metadata.items():
                        if 'extract' in key.lower() or 'data' in key.lower():
                            logging.info(f"Metadata key '{key}': {type(value)} - {str(value)[:100]}...")
                    
                    # Check what the extraction box actually produced
                    if not extracted_data or not isinstance(extracted_data, dict) or not extracted_data:
                        logging.error("❌ EXTRACTION BOX FAILED: No extracted_data produced!")
                        logging.error("This means the extraction box is not working properly.")
                        logging.error("Check: 1) OpenAI API key, 2) Network connectivity, 3) Schema loading")
                        logging.error(f"Available metadata keys: {list(doc.metadata.keys())}")
                        logging.error(f"doc.extracted_data: {doc.extracted_data}")
                    else:
                        logging.info(f"✅ Extraction box produced data with {len(extracted_data.get('entities', []))} entities")
                    
                    # Enhanced extraction metrics
                    extraction_quality = doc.metadata.get('extraction_quality_score', 0.0)
                    extraction_time = doc.metadata.get('total_processing_time', 0.0)
                    extraction_details = {
                        "extraction_quality": extraction_quality,
                        "extraction_confidence": getattr(doc, 'extraction_confidence', None),
                        "processing_time": extraction_time,
                        "extracted_data_present": 'extracted_data' in doc.metadata
                    }
                    doc.add_event("INFO", "Extraction", f"Extraction completed for '{file_path.name}'", extraction_details)
                    processing_result["stages_completed"].append("extraction")
                    
                    # Record extraction metrics
                    self.doc_metrics.record_extraction_result(doc.document_type, extraction_quality, extraction_time)

                # 6. Bounding Box Extraction
                with OperationTimer(self.performance_monitor, "bounding_box") as timer:
                    doc = self.bounding_box_box(doc)
                    if doc.error_message:
                        doc.add_event("WARNING", "BoundingBox", f"Bounding box extraction failed: {doc.error_message}")
                        # Don't raise error - continue pipeline even if bounding box fails
                    else:
                        bbox_entries = doc.metadata.get('bounding_box_entries_found', 0)
                        bbox_details = {
                            "entries_processed": doc.metadata.get('bounding_box_entries_processed', 0),
                            "entries_found": bbox_entries,
                            "processing_time": doc.metadata.get('bounding_box_processing_time', 0.0)
                        }
                        doc.add_event("INFO", "BoundingBox", f"Bounding box extraction completed for '{file_path.name}': {bbox_entries} entries found", bbox_details)
                    processing_result["stages_completed"].append("bounding_box")
                    
                    # Record bounding box metrics
                    self.metrics.increment_counter("bounding_box_success_total")
                    bbox_entries = doc.metadata.get('bounding_box_entries_found', 0)
                    self.metrics.observe_histogram("bounding_box_entries_found", bbox_entries)

                # 7. Validation & Enrichment
                with OperationTimer(self.performance_monitor, "validation_enrichment") as timer:
                    doc = self.validation_enrichment_box(doc)
                    enrichment_status = "enrichment applied" if getattr(doc, 'enrichment_applied', False) else "no enrichment"
                    doc.add_event("INFO", "Validation & Enrichment", f"Validation & Enrichment completed for '{file_path.name}': {enrichment_status}")
                    processing_result["stages_completed"].append("validation_enrichment")
                    
                    # Record validation metrics
                    validation_errors = len(doc.validation_errors) if doc.validation_errors else 0
                    self.metrics.observe_histogram("validation_errors_count", validation_errors)

                # 8. Output
                with OperationTimer(self.performance_monitor, "output") as timer:
                    self.output_box(doc)
                    processing_result["stages_completed"].append("output")
                    
                    # Record output metrics
                    self.metrics.increment_counter("output_success_total")

                # 9. Database Storage (NEW)
                with OperationTimer(self.performance_monitor, "database_storage") as timer:
                    db_storage_success = self.store_extracted_content_in_database(doc, file_path)
                    if db_storage_success:
                        doc.add_event("INFO", "Database Storage", f"Successfully stored {doc.document_type} data in database")
                        processing_result["stages_completed"].append("database_storage")
                        
                        # Record database storage metrics
                        self.metrics.increment_counter("database_storage_success_total")
                    else:
                        doc.add_event("WARNING", "Database Storage", "Failed to store data in database")
                        processing_result["errors"].append({
                            "type": "DatabaseStorageError",
                            "message": "Failed to store extracted data in database"
                        })

                # Calculate total processing time
                total_processing_time = time.time() - pipeline_start_time
                processing_result["processing_time"] = total_processing_time
                processing_result["success"] = True
                
                # Final pipeline completion event
                doc.add_event("INFO", "Pipeline", f"Pipeline completed successfully for {file_path.name}")
                
                # Record comprehensive pipeline metrics
                self.doc_metrics.record_document_processed(
                    file_path.name,
                    doc.document_type,
                    True,
                    total_processing_time
                )
                
                # Collect final metrics
                processing_result["metrics"] = {
                    "document_type": doc.document_type,
                    "classification_confidence": doc.classification_confidence,
                    "extraction_quality": doc.metadata.get('extraction_quality_score', 0.0),
                    "total_processing_time": total_processing_time,
                    "stages_completed": len(processing_result["stages_completed"]),
                    "events_count": len(doc.events_log)
                }

            except BaseAithonException as e:
                # Handle Aithon-specific exceptions
                logging.error(f"Aithon exception in pipeline for {file_path.name}: {e}")
                processing_result["errors"].append(e.to_dict())
                
                if doc:
                    doc.add_event("ERROR", "Pipeline", f"Pipeline failed: {e.message}")
                
                # Record error metrics
                self.metrics.increment_counter("pipeline_errors_total", 1, {
                    "error_type": e.__class__.__name__,
                    "error_category": e.category.value
                })
                
                self.doc_metrics.record_document_processed(
                    file_path.name,
                    doc.document_type if doc else "unknown",
                    False,
                    time.time() - pipeline_start_time
                )
                
            except Exception as e:
                # Handle unexpected exceptions
                logging.error(f"Unexpected error in pipeline for {file_path.name}: {e}", exc_info=True)
                processing_result["errors"].append({
                    "type": "UnexpectedError",
                    "message": str(e),
                    "traceback": str(e.__traceback__)
                })
                
                if doc:
                    doc.add_event("ERROR", "Pipeline", f"Pipeline failed with unexpected error: {e}")
                
                # Record error metrics
                self.metrics.increment_counter("pipeline_errors_total", 1, {
                    "error_type": "UnexpectedError"
                })
                
                self.doc_metrics.record_document_processed(
                    file_path.name,
                    doc.document_type if doc else "unknown",
                    False,
                    time.time() - pipeline_start_time
                )
        
        return processing_result
    
    def get_processing_stats(self) -> Dict[str, Any]:
        """Get comprehensive processing statistics"""
        return {
            "pipeline_summary": self.doc_metrics.get_processing_summary(),
            "system_metrics": self.metrics.get_all_metrics(),
            "error_summary": self.exception_handler.get_error_statistics(),
            "active_operations": len(self.performance_monitor.active_operations)
        }
    
    def export_metrics(self, filepath: str, format_type: str = "json"):
        """Export metrics to file"""
        from monitoring import export_metrics_to_file
        export_metrics_to_file(filepath, format_type)


def main():
    """
    Enhanced main entry point with comprehensive monitoring and error handling.
    """
    # Configure logging
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    
    # Reduce log noise from verbose libraries
    logging.getLogger("openai").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("PIL").setLevel(logging.WARNING)
    logging.getLogger("pytesseract").setLevel(logging.WARNING)

    logging.info("Starting Enhanced Aithon Orchestrator System")
    
    source_dir = Path(os.getenv("SOURCE_DIR", "./source_documents"))
    if not source_dir.exists():
        logging.error(f"Source directory not found: {source_dir}. Please create it and add documents.")
        return

    # Initialize orchestrator with advanced features
    orchestrator = AithonOrchestrator()
    
    # Find all PDF files in the source directory
    source_files = list(source_dir.glob("*.pdf"))
    
    if not source_files:
        logging.warning(f"No PDF documents found in {source_dir}. Add some files to process.")
        return
        
    # Process documents with comprehensive tracking
    results = []
    for file_path in source_files:
        result = orchestrator.run_pipeline(file_path)
        results.append(result)
        
        # Log only errors
        if not result["success"]:
            logging.error(f"✗ Failed to process {file_path.name}: {result['errors']}")
    
    # Final summary - only log if there are failures
    successful = sum(1 for r in results if r["success"])
    failed = len(results) - successful
    
    if failed > 0:
        logging.warning(f"Processing completed: {successful} successful, {failed} failed")
    
    # Export metrics
    metrics_file = Path("output_documents/metrics_export.json")
    orchestrator.export_metrics(str(metrics_file))
    
    # Generate dashboard automatically
    try:
        logging.info("🚀 Generating Smart Dashboard...")
        dashboard_data = generate_dashboard("output_documents/dashboard.json")
        
        # Show dashboard summary
        system_overview = dashboard_data.get("system_overview", {})
        logging.info(f"📊 Dashboard Summary:")
        logging.info(f"   • System Health: {system_overview.get('system_health', 'UNKNOWN')}")
        logging.info(f"   • Total Documents: {system_overview.get('total_documents', 0)}")
        logging.info(f"   • Success Rate: {system_overview.get('success_rate', 0)}%")
        logging.info(f"   • CPU Usage: {system_overview.get('cpu_usage', 0):.1f}%")
        logging.info(f"   • Memory Usage: {system_overview.get('memory_usage', 0):.1f}%")
        
        # Show alerts if any
        alerts = dashboard_data.get("alerts", [])
        if alerts:
            logging.info(f"⚠️  System Alerts:")
            for alert in alerts:
                logging.info(f"   • {alert.get('type', 'INFO')}: {alert.get('message', 'No message')}")
        
        #logging.info(f"✅ Smart dashboard generated successfully: dashboard.json")
        #logging.info(f"📁 Dashboard contains {len(dashboard_data.get('files_processing_details', []))} processed files")
        
    except Exception as e:
        #logging.error(f"http://localhost:8000/FE Failed to generate dashboard: {e}")
        # Still show basic status even if dashboard fails
        try:
            status = get_status()
            logging.info(f"📊 Basic Status: {status.get('system_health', 'UNKNOWN')}")
        except Exception:
            logging.error("Unable to get basic status")
    
    logging.info("🎉 Enhanced Aithon Orchestrator System Completed!")
    logging.info("📊 Check dashboard.json for comprehensive system overview")


if __name__ == "__main__":
    main()

import logging
import os
import json
import hashlib
from pathlib import Path
from uuid import UUID

from ..data_model import AithonDocument

class OutputBox:
    """
    The final box in the pipeline. It saves the structured, enriched
    data to a final JSON output file and optionally to the backend structure.
    """
    def __init__(self):
        self.output_dir = Path(os.getenv("OUTPUT_DIR", "./output_documents"))
        self.output_dir.mkdir(exist_ok=True)
        
        # Backend structure configuration
        self.enable_backend_output = os.getenv("ENABLE_BACKEND_OUTPUT", "false").lower() == "true"
        self.backend_base_dir = Path(os.getenv("BACKEND_OUTPUT_DIR", "../../validusBoxes/data/frameDemo/l1"))
    
    def _uuid_serializer(self, obj):
        """Custom JSON serializer for UUID objects."""
        if isinstance(obj, UUID):
            return str(obj)
        raise TypeError(f"Object of type {obj.__class__.__name__} is not JSON serializable")

    def _generate_file_hash(self, file_path: Path) -> str:
        """Generate SHA256 hash for the file."""
        try:
            hash_sha256 = hashlib.sha256()
            with open(file_path, "rb") as f:
                for chunk in iter(lambda: f.read(4096), b""):
                    hash_sha256.update(chunk)
            return hash_sha256.hexdigest()
        except Exception as e:
            logging.error(f"Failed to generate hash for {file_path}: {e}")
            # Fallback to filename-based hash
            return hashlib.sha256(str(file_path.name).encode()).hexdigest()

    def _extract_field_value(self, extracted_data: dict, field_name: str) -> str:
        """
        Intelligently extract field values from nested extracted_data structure.
        
        Handles the complex nested structure:
        extracted_data -> entities -> portfolio -> field_name -> Value
        """
        try:
            # Check if extracted_data has the expected structure
            if not isinstance(extracted_data, dict):
                return None
                
            entities = extracted_data.get("entities", [])
            if not isinstance(entities, list) or not entities:
                return None
                
            # Look through entities for portfolio data
            for entity in entities:
                if not isinstance(entity, dict):
                    continue
                    
                portfolio = entity.get("portfolio", [])
                if not isinstance(portfolio, list) or not portfolio:
                    continue
                    
                # Look through portfolio items for the field
                for portfolio_item in portfolio:
                    if not isinstance(portfolio_item, dict):
                        continue
                        
                    field_data = portfolio_item.get(field_name)
                    if isinstance(field_data, dict):
                        value = field_data.get("Value")
                        if value and isinstance(value, str) and value.strip():
                            return value.strip()
                            
            return None
            
        except Exception as e:
            logging.warning(f"Error extracting {field_name}: {e}")
            return None

    def _extract_account_details(self, final_output: dict) -> dict:
        """Extract account details with intelligent field extraction."""
        extracted_data = final_output.get("extracted_data", {})
        
        # Extract key fields using intelligent extraction
        investor_name = self._extract_field_value(extracted_data, "Investor")
        security_name = self._extract_field_value(extracted_data, "Security") 
        account_ref = self._extract_field_value(extracted_data, "Account")
        investor_ref_id = self._extract_field_value(extracted_data, "InvestorRefID")
        
        # Smart fallbacks for fund_name
        fund_name = (
            security_name or 
            self._extract_field_value(extracted_data, "Fund_Name") or
            self._extract_field_value(extracted_data, "FundName") or
            extracted_data.get("fund_name") or
            "Unknown Fund"
        )
        
        # Smart fallbacks for entity  
        entity_name = (
            investor_name or
            self._extract_field_value(extracted_data, "Client") or
            self._extract_field_value(extracted_data, "ClientName") or
            extracted_data.get("investor_name") or
            extracted_data.get("client_name") or
            "Unknown Entity"
        )
        
        # Extract financial values with intelligent parsing
        def safe_extract_number(field_names):
            """Try to extract a number from various possible field locations."""
            for field_name in field_names:
                # Try extracted field value first
                value = self._extract_field_value(extracted_data, field_name)
                if value:
                    try:
                        # Clean and parse the value
                        cleaned = str(value).replace(",", "").replace("$", "").replace("(", "-").replace(")", "").strip()
                        return float(cleaned)
                    except ValueError:
                        continue
                        
                # Try direct dictionary access as fallback
                direct_value = extracted_data.get(field_name.lower())
                if direct_value is not None:
                    try:
                        return float(direct_value)
                    except (ValueError, TypeError):
                        continue
            return 0
        
        market_value = safe_extract_number([
            "MarketValue", "Market_Value", "TotalValue", "Total_Value", 
            "Balance", "AccountBalance", "CurrentValue"
        ])
        
        return {
            "account_sid": (
                investor_ref_id or 
                self._extract_field_value(extracted_data, "AccountRefID") or
                final_output.get("doc_id", "")
            ),
            "fund_name": fund_name,
            "entity": entity_name,
            "account_reference": account_ref or "",
            "market_value": market_value,
            "investor_ref_id": investor_ref_id or ""
        }

    ACCOUNT_FIELDS = {"Account", "account", "account_number", "account_name"}  # Add all relevant keys

    def _remove_bounding_box_for_account_fields(self, extracted_data):
        """Set BoundingBox to null for account fields only, keep PageNumber if present."""
        if not isinstance(extracted_data, dict):
            return extracted_data
        for field in self.ACCOUNT_FIELDS:
            if field in extracted_data and isinstance(extracted_data[field], dict):
                extracted_data[field]["BoundingBox"] = None
        return extracted_data

    def _save_to_backend_structure(self, doc_payload: AithonDocument, final_output: dict):
        """Save output to the backend structure in validusBoxes."""
        # Double-check environment variable before proceeding
        enable_backend_output = os.getenv("ENABLE_BACKEND_OUTPUT", "false").lower() == "true"
        if not enable_backend_output:
            logging.info("Backend output is disabled - skipping l1 folder creation and JSON storage")
            return None
        
        try:
            # Generate hash for the source file
            file_hash = self._generate_file_hash(doc_payload.source_path)
            
            # Create hash-based directory structure
            hash_dir = self.backend_base_dir / file_hash
            hash_dir.mkdir(parents=True, exist_ok=True)
            
            # Define the output file path as forFrontend.json
            backend_output_path = hash_dir / "forFrontend.json"
            
            # Transform events_log to frontend format
            events_log = final_output.get("events_log", [])
            transformed_events = []
            for event in events_log:
                status = "success" if event.get("level") == "INFO" else "error"
                transformed_events.append({
                    "status": status,
                    "timestamp": event.get("timestamp", ""),
                    "message": event.get("message", "")
                })
            
            # Get processing timestamp for dates
            processing_timestamp = events_log[-1].get("timestamp") if events_log else ""
            processing_date = processing_timestamp.split()[0] if processing_timestamp else ""
            
            # Determine status based on pipeline status
            status_mapping = {
                "Completed_Successfully": "EXTRACTED_IN_REVIEW",
                "Completed_With_Error": "EXTRACTION_FAILED", 
                "Failed": "EXTRACTION_FAILED",
                "Processing": "PROCESSING",
                "Pending": "PENDING"
            }
            pipeline_status = final_output.get("pipeline_status", "Processing")
            mapped_status = status_mapping.get(pipeline_status, "PROCESSING")
            

            
            # Create the frontend-specific output structure with all original data preserved
            frontend_output = {
                # Frontend-specific format for file info section
                "document_details": {
                    "document_name": final_output.get("original_filename", ""),
                    "date": processing_date,
                    "status": mapped_status,
                    "source": "Dashboard",
                    "processing_method": "Completely Automated",
                    "file_id": final_output.get("doc_id", ""),
                    "business_date": processing_date,
                    "file_type": final_output.get("document_type", "Unknown"),
                    "capture_system": " ",
                    "linking_system": " ",
                    "extractor": "Aithon Frame - AI"
                },
                "file_metadata_config": [
                    {
                        "key": "status",
                        "label": "Status",
                        "layout": "badge"
                    },
                    {
                        "key": "source",
                        "label": "Source"
                    },
                    {
                        "key": "processing_method",
                        "label": "Processing Method"
                    },
                    {
                        "key": "file_id",
                        "label": "File ID"
                    },
                    {
                        "key": "business_date",
                        "label": "Business Date"
                    },
                    {
                        "key": "file_type",
                        "label": "File Type"
                    },
                    {
                        "key": "capture_system",
                        "label": "Capture System"
                    },
                    {
                        "key": "linking_system",
                        "label": "Linking System"
                    },
                    {
                        "key": "extractor",
                        "label": "Extractor"
                    }
                ],
                "event_log_title": "Event Logs",
                "event_logs": transformed_events,
                
                # Account details section for frontend using intelligent extraction
                "account_details": self._extract_account_details(final_output),
                "account_details_enhanced": {
                    "created_on": processing_date,
                    "frequency": final_output.get("extracted_data", {}).get("frequency", 0),
                    "delay": final_output.get("extracted_data", {}).get("delay", 0)
                },
                "account_fields": [
                    {
                        "key": "fund_name",
                        "label": "Fund Name"
                    },
                    {
                        "key": "entity",
                        "label": "Entity"
                    },
                    {
                        "key": "account_sid",
                        "label": "Account SID"
                    },
                    {
                        "key": "investor_ref_id",
                        "label": "Investor Ref ID"
                    },
                    {
                        "key": "market_value",
                        "label": "Market Value"
                    },
                    {
                        "key": "account_reference",
                        "label": "Account Reference"
                    }
                ],
                "file_fields": [
                    {
                        "key": "file_name",
                        "label": "File Name"
                    },
                    {
                        "key": "file_id",
                        "label": "File ID",
                        "isCopyable": True
                    },
                    {
                        "key": "market_value",
                        "label": "Market Value"
                    }
                ],
                

                
                # Original data preserved for complete information
                "doc_id": final_output.get("doc_id"),
                "task_id": final_output.get("task_id"),
                "original_filename": final_output.get("original_filename"),
                "file_hash": file_hash,
                "pipeline_status": final_output.get("pipeline_status"),
                "document_type": final_output.get("document_type"),
                "classification_confidence": final_output.get("classification_confidence"),
                "extracted_data": final_output.get("extracted_data", {}),
                "validation_errors": final_output.get("validation_errors", []),
                "events_log": final_output.get("events_log", []),  # Original format preserved
                "processing_summary": {
                    "total_processing_time": final_output.get("metadata", {}).get("total_processing_time"),
                    "extraction_quality": final_output.get("metadata", {}).get("extraction_quality_score"),
                    "classification_mode": final_output.get("metadata", {}).get("classification_mode"),
                    "stages_completed": [event.get("stage") for event in events_log if event.get("level") == "INFO"],
                    "error_count": len([event for event in events_log if event.get("level") == "ERROR"])
                },
                "metadata": {
                    "file_hash": file_hash,
                    "processing_timestamp": processing_timestamp,
                    "backend_structure_version": "2.0"
                }
            }
            
            # Ensure account fields have NULL bounding boxes in frontend output too
            if "extracted_data" in frontend_output:
                frontend_output["extracted_data"] = self._remove_bounding_box_for_account_fields(frontend_output["extracted_data"])
            
            # Write the frontend-focused JSON
            with open(backend_output_path, 'w', encoding='utf-8') as f:
                json.dump(frontend_output, f, ensure_ascii=False, indent=2, default=self._uuid_serializer)
            
            logging.info(f"✅ Backend output saved to: {backend_output_path}")
            
            # Update the document event log
            doc_payload.add_event("INFO", "Backend Output", 
                                f"Successfully saved backend output for '{doc_payload.original_filename}' to {backend_output_path}")
            
            return backend_output_path
            
        except Exception as e:
            logging.error(f"http://localhost:8000/FE Failed to save backend output for {doc_payload.original_filename}: {e}")
            doc_payload.add_event("ERROR", "Backend Output", 
                                f"Failed to save backend output: {e}")
            return None

    def __call__(self, doc_payload: AithonDocument):
        """
        Saves the final extracted data to a JSON file and optionally to backend structure.
        """
        logging.info(f"Entering Output Box for: {doc_payload.original_filename}")

        if doc_payload.error_message:
            doc_payload.pipeline_status = "Completed_With_Error"
            logging.error(f"Pipeline for {doc_payload.original_filename} completed with an error: {doc_payload.error_message}")
        else:
            doc_payload.pipeline_status = "Completed_Successfully"

        # Define the output file path (traditional structure)
        output_filename = self.output_dir / f"{Path(doc_payload.original_filename).stem}_output.json"

        # Add the output event before writing the file (but after preparing the path)
        doc_payload.add_event("INFO", "Output", f"Successfully wrote output for '{doc_payload.original_filename}' to {output_filename}")

        # Prepare the final output dictionary
        # We will dump the entire payload for maximum traceability
        final_output = doc_payload.model_dump(exclude={'source_path', 'pages'})

        # Remove BoundingBox from account fields before any output creation
        if "extracted_data" in final_output:
            final_output["extracted_data"] = self._remove_bounding_box_for_account_fields(final_output["extracted_data"])

        # Manually convert path objects to strings for serialization
        if 'source_path' in final_output:
            final_output['source_path'] = str(final_output['source_path'])
        
        # Save to traditional structure
        try:
            with open(output_filename, 'w', encoding='utf-8') as f:
                json.dump(final_output, f, ensure_ascii=False, indent=4, default=self._uuid_serializer)
            logging.info(f"✅ Traditional output saved to: {output_filename}")
        except Exception as e:
            logging.error(f"http://localhost:8000/FE Failed to write traditional output file for {doc_payload.original_filename}: {e}")
            # Update the last event to reflect the error
            if doc_payload.events_log:
                doc_payload.events_log[-1].level = "ERROR"
                doc_payload.events_log[-1].message = f"Failed to write traditional output file: {e}"

        # Save to backend structure if enabled
        # Check environment variable at runtime to allow dynamic control
        enable_backend_output = os.getenv("ENABLE_BACKEND_OUTPUT", "false").lower() == "true"
        backend_output_path = None
        if enable_backend_output:
            backend_output_path = self._save_to_backend_structure(doc_payload, final_output)
        else:
            logging.debug(f"Backend output disabled (ENABLE_BACKEND_OUTPUT={os.getenv('ENABLE_BACKEND_OUTPUT', 'not set')}) - no l1 folders or JSON files will be created")

        return doc_payload

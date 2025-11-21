import asyncio
import logging
import time
import json
import hashlib
import copy
import sys
import os
import re
from enum import Enum
from typing import Dict, Any, Optional, List, Tuple
from dataclasses import dataclass, field
from pathlib import Path

import openai
from openai import OpenAI
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
    before_sleep_log,
    after_log
)

from ..data_model import AithonDocument

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class ExtractionMode(Enum):
    """Extraction processing modes"""
    TEXT_BASED = "text_based_extraction"
    VISION_BASED = "vision_based_extraction"
    HYBRID = "hybrid_extraction"
    STRUCTURED = "structured_extraction"

class QualityLevel(Enum):
    """Extraction quality levels"""
    EXCELLENT = "EXCELLENT"
    GOOD = "GOOD"
    ACCEPTABLE = "ACCEPTABLE"
    POOR = "POOR"
    FAILED = "FAILED"

class ExtractionMetrics:
    """Comprehensive metrics collection for extraction performance"""
    def __init__(self):
        self.start_time = time.time()
        self.api_calls = 0
        self.retry_count = 0
        self.validation_attempts = 0
        self.schema_validation_time = 0
        self.extraction_time = 0
        self.post_processing_time = 0
        self.error_count = 0
        self.cache_hits = 0
        self.token_usage = {"input": 0, "output": 0}
        
    def record_api_call(self, input_tokens: int = 0, output_tokens: int = 0):
        self.api_calls += 1
        self.token_usage["input"] += input_tokens
        self.token_usage["output"] += output_tokens
        
    def record_retry(self):
        self.retry_count += 1
        
    def record_validation_attempt(self):
        self.validation_attempts += 1
        
    def record_error(self):
        self.error_count += 1
        
    def record_cache_hit(self):
        self.cache_hits += 1
        
    def get_processing_time(self) -> float:
        return time.time() - self.start_time
        
    def get_summary(self) -> Dict[str, Any]:
        return {
            "total_processing_time": self.get_processing_time(),
            "extraction_time": self.extraction_time,
            "schema_validation_time": self.schema_validation_time,
            "post_processing_time": self.post_processing_time,
            "api_calls": self.api_calls,
            "retry_count": self.retry_count,
            "validation_attempts": self.validation_attempts,
            "error_count": self.error_count,
            "cache_hits": self.cache_hits,
            "token_usage": self.token_usage,
            "efficiency_score": self.cache_hits / max(self.api_calls, 1),
            "tokens_per_second": sum(self.token_usage.values()) / max(self.get_processing_time(), 0.001)
        }

@dataclass
class ExtractionConfig:
    """Enhanced configuration for extraction behavior"""
    
    # Processing thresholds
    max_text_length: int = 500000  # Increased to handle larger documents
    min_text_length: int = 50
    chunk_size: int = 50000  # Larger chunks to reduce fragmentation
    overlap_size: int = 2000  # Larger overlap to maintain context
    
    # Token management
    max_tokens_per_request: int = 100000  # GPT-4 can handle ~128k tokens
    enable_intelligent_chunking: bool = True
    prefer_full_document: bool = True  # Try to send full document first
    
    # Quality thresholds
    min_confidence_for_auto_accept: float = 0.9  # HIGH: Above 90%
    min_confidence_for_manual_review: float = 0.8  # MEDIUM: 80-90%
    min_quality_score: float = 0.7
    
    # Retry configuration
    max_retries: int = 3
    retry_delay_base: float = 2.0
    retry_delay_max: float = 120.0
    exponential_backoff: bool = True
    jitter: bool = True
    
    # Performance optimization
    enable_caching: bool = True
    cache_ttl: int = 3600  # 1 hour
    enable_parallel_processing: bool = True
    batch_size: int = 5
    
    # Validation settings
    enable_schema_validation: bool = True
    strict_validation: bool = False
    max_validation_attempts: int = 3
    
    # Timeout settings
    extraction_timeout: int = 600  # 10 minutes
    api_timeout: int = 120  # 2 minutes
    
    # Model settings
    model_name: str = "gpt-4o"
    temperature: float = 0.0
    max_tokens: int = 4000
    
    # Monitoring
    enable_detailed_logging: bool = True
    log_extraction_results: bool = False
    enable_metrics_collection: bool = True

class ExtractionBox:
    """
    Enhanced Extraction Box with advanced patterns 
    - Sophisticated retry logic with exponential backoff
    - Comprehensive performance monitoring and metrics
    - Advanced caching and optimization
    - Intelligent chunking for large documents
    - Schema validation with error recovery
    - Quality assessment and confidence scoring
    - Detailed logging and tracing
    """
    
    def __init__(self, config: Optional[ExtractionConfig] = None):
        self.config = config or ExtractionConfig()
        self.client = OpenAI()
        self.extraction_cache = {}
        self.schema_cache = {}
        self.metrics = ExtractionMetrics()
        
        # Output directory for raw OpenAI responses
        self.output_dir = Path(os.getenv("OUTPUT_DIR", "./output_documents"))
        self.output_dir.mkdir(exist_ok=True)
        
        # Schema directory - use robust path resolution with extensive debugging
        # Log environment information for debugging
        logging.info(f"Current working directory: {os.getcwd()}")
        logging.info(f"Python path: {sys.path[:3]}...")
        logging.info(f"__file__ location: {__file__}")
        
        possible_paths = [
            # Strategy 1: Relative to current file location
            Path(__file__).parent.parent / "schemas",
            # Strategy 2: Relative to project root
            Path(__file__).parent.parent.parent / "frameEngine" / "schemas",
            # Strategy 3: Absolute path in Docker container
            Path("/app/frameEngine/schemas"),
            # Strategy 4: Current working directory relative
            Path("frameEngine/schemas"),
        ]
        
        self.schema_dir = None
        logging.info("Testing schema directory resolution strategies...")
        
        for i, path in enumerate(possible_paths, 1):
            absolute_path = path.absolute()
            path_exists = path.exists()
            logging.info(f"Strategy {i}: {path} -> {absolute_path} (exists: {path_exists})")
            
            if path_exists:
                try:
                    # Test if we can actually list files in the directory
                    files = list(path.glob("*.json"))
                    logging.info(f"  Found {len(files)} JSON files: {[f.name for f in files]}")
                    
                    # Test if capcall_schema.json specifically exists
                    capcall_file = path / "capcall_schema.json"
                    logging.info(f"  capcall_schema.json exists: {capcall_file.exists()}")
                    
                    if self.schema_dir is None:
                        self.schema_dir = absolute_path
                        logging.info(f"✅ Using schema directory: {self.schema_dir}")
                        
                except Exception as e:
                    logging.error(f"  Error accessing directory {path}: {e}")
        
        if self.schema_dir is None:
            # Default to the Docker path and create it
            self.schema_dir = Path("/app/frameEngine/schemas").absolute()
            logging.warning(f"No existing schema directory found. Creating: {self.schema_dir}")
            try:
                self.schema_dir.mkdir(parents=True, exist_ok=True)
                logging.info(f"Successfully created schema directory: {self.schema_dir}")
                
                # Try to copy schema files from alternative locations
                self._copy_schema_files_if_missing()
                
                # Check if the copy operation helped
                files_after_copy = list(self.schema_dir.glob("*.json"))
                logging.info(f"After copy attempt: {len(files_after_copy)} JSON files in {self.schema_dir}")
                for f in files_after_copy:
                    logging.info(f"  Found: {f.name}")
                
            except Exception as e:
                logging.error(f"Failed to create schema directory {self.schema_dir}: {e}")
                # Fallback to current working directory
                self.schema_dir = Path("frameEngine/schemas").absolute()
                self.schema_dir.mkdir(parents=True, exist_ok=True)
                logging.warning(f"Using fallback schema directory: {self.schema_dir}")
                
                # Try to copy schema files from alternative locations
                self._copy_schema_files_if_missing()
                
                # Check if the copy operation helped  
                files_after_copy = list(self.schema_dir.glob("*.json"))
                logging.info(f"After fallback copy attempt: {len(files_after_copy)} JSON files in {self.schema_dir}")
                for f in files_after_copy:
                    logging.info(f"  Found: {f.name}")
        
        # Final verification
        try:
            schema_files = list(self.schema_dir.glob("*.json"))
            logging.info(f"Final schema directory: {self.schema_dir}")
            logging.info(f"Available schema files: {[f.name for f in schema_files]}")
            
            # Check specifically for capcall schema
            capcall_path = self.schema_dir / "capcall_schema.json"
            logging.info(f"capcall_schema.json path: {capcall_path}")
            logging.info(f"capcall_schema.json exists: {capcall_path.exists()}")
            
        except Exception as e:
            logging.error(f"Error in final verification: {e}")
            
        logging.info(f"Initialized Enhanced Extraction Box with config: {self.config}")
    
    def _copy_schema_files_if_missing(self):
        """
        Try to copy schema files from alternative locations if they're missing
        """
        import shutil
        
        # Check if we have any schema files in the current directory
        current_schema_count = len(list(self.schema_dir.glob("*.json")))
        logging.info(f"Current schema directory has {current_schema_count} JSON files")
        
        if current_schema_count >= 5:  # We expect at least 5-8 schema files
            logging.info("Schema directory appears to have adequate files, skipping copy")
            return
            
        logging.warning(f"Schema directory has only {current_schema_count} files, attempting to find more...")
        
        # Source locations to search for schema files
        source_locations = [
            # Local development
            Path(__file__).parent.parent / "schemas",
            # Project root
            Path(__file__).parent.parent.parent / "frameEngine" / "schemas", 
            # Current working directory
            Path("frameEngine/schemas"),
            # Alternative paths
            Path("./frameEngine/schemas"),
            # Docker build context (if schemas were copied elsewhere)
            Path("/app/frameEngine/schemas"),
            # Backup location in case schemas exist in different Docker layers
            Path("/frameEngine/schemas"),
        ]
        
        schema_files_copied = 0
        for source_path in source_locations:
            if source_path.exists() and source_path != self.schema_dir:
                try:
                    json_files = list(source_path.glob("*.json"))
                    logging.info(f"Found {len(json_files)} schema files in {source_path}")
                    
                    for json_file in json_files:
                        dest_file = self.schema_dir / json_file.name
                        if not dest_file.exists():
                            shutil.copy2(json_file, dest_file)
                            logging.info(f"Copied {json_file.name} to schema directory")
                            schema_files_copied += 1
                        
                except Exception as e:
                    logging.error(f"Error copying schemas from {source_path}: {e}")
                    
        if schema_files_copied > 0:
            logging.info(f"Successfully copied {schema_files_copied} schema files")
        else:
            logging.warning("No schema files could be copied from alternative locations")
            
            # As a last resort, create minimal essential schemas directly
            self._create_essential_schemas_if_missing()
    
    def _create_essential_schemas_if_missing(self):
        """
        Create minimal essential schemas directly in code as a fallback
        """
        essential_schemas = {
            'capcall_schema.json': {
                "$schema": "http://json-schema.org/draft-07/schema#",
                "type": "object",
                "properties": {
                    "entities": {
                        "type": "array",
                        "items": {
                            "portfolio": {
                                "type": "array",
                                "items": {
                                    "type": "object",
                                    "properties": {
                                        "Investor": {"type": "object", "properties": {"Value": {"type": "string"}}},
                                        "Account": {"type": "object", "properties": {"Value": {"type": "string"}}},
                                        "Security": {"type": "object", "properties": {"Value": {"type": "string"}}},
                                        "TransactionDate": {"type": "object", "properties": {"Value": {"type": "string"}}},
                                        "Currency": {"type": "object", "properties": {"Value": {"type": "string"}}},
                                        "CapitalCall": {"type": "object", "properties": {"Value": {"type": "number"}}}
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        
        for schema_name, schema_content in essential_schemas.items():
            schema_path = self.schema_dir / schema_name
            if not schema_path.exists():
                try:
                    with open(schema_path, 'w', encoding='utf-8') as f:
                        json.dump(schema_content, f, indent=2)
                    logging.info(f"Created minimal fallback schema: {schema_name}")
                except Exception as e:
                    logging.error(f"Failed to create fallback schema {schema_name}: {e}")
    
    def _compute_content_hash(self, content: str, schema_name: str = "") -> str:
        """Compute hash for caching purposes"""
        combined = f"{content}_{schema_name}"
        return hashlib.md5(combined.encode()).hexdigest()
    
    def _get_from_cache(self, content_hash: str) -> Optional[Dict[str, Any]]:
        """Get extraction result from cache if available"""
        if not self.config.enable_caching:
            return None
            
        cache_entry = self.extraction_cache.get(content_hash)
        if cache_entry:
            timestamp, result = cache_entry
            if time.time() - timestamp < self.config.cache_ttl:
                self.metrics.record_cache_hit()
                return result
            else:
                # Remove expired entry
                del self.extraction_cache[content_hash]
        return None
    
    def _store_in_cache(self, content_hash: str, result: Dict[str, Any]):
        """Store extraction result in cache"""
        if self.config.enable_caching:
            self.extraction_cache[content_hash] = (time.time(), result)
    
    def _load_schema(self, document_type: str) -> Dict[str, Any]:
        """Load and cache schema for document type"""
        if document_type in self.schema_cache:
            return self.schema_cache[document_type]
        
        schema_mapping = {
            "Statement": "statement_schema.json",
            "CapCall": "capcall_schema.json", 
            "Distribution": "distribution_schema.json",
            "AGM": "agm_schema.json"
        }
        
        schema_file = schema_mapping.get(document_type)
        if not schema_file:
            raise ValueError(f"No schema found for document type: {document_type}")
        
        schema_path = self.schema_dir / schema_file
        if not schema_path.exists():
            # Try one more time to create/copy schema files
            logging.warning(f"Schema file {schema_file} not found, attempting recovery...")
            self._copy_schema_files_if_missing()
            
            # Check again after recovery attempt
            if not schema_path.exists():
                # Provide detailed debugging information
                available_files = list(self.schema_dir.glob("*.json")) if self.schema_dir.exists() else []
                error_msg = (f"Schema file not found after recovery attempts: {schema_path}\n"
                            f"Schema directory: {self.schema_dir}\n"
                            f"Schema directory exists: {self.schema_dir.exists()}\n"
                            f"Available schema files: {[f.name for f in available_files]}\n"
                            f"Looking for document type: {document_type}\n"
                            f"Expected schema file: {schema_file}")
                logging.error(error_msg)
                raise FileNotFoundError(error_msg)
        
        try:
            with open(schema_path, 'r', encoding='utf-8') as f:
                schema = json.load(f)
            
            # Cache the schema
            self.schema_cache[document_type] = schema
            logging.info(f"Loaded and cached schema for {document_type}")
            return schema
            
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON in schema file {schema_path}: {e}")
    
    def _validate_schema(self, schema: Dict[str, Any]) -> bool:
        """Validate schema structure"""
        required_fields = ["type", "properties"]
        return all(field in schema for field in required_fields)
    
    def _determine_extraction_mode(self, text: str, document_type: str) -> ExtractionMode:
        """Determine optimal extraction mode based on content and type"""
        text_length = len(text.strip())
        
        # Mode selection logic
        if text_length < self.config.min_text_length:
            return ExtractionMode.VISION_BASED
        elif text_length > self.config.max_text_length:
            return ExtractionMode.HYBRID  # Use chunking
        elif document_type in ["Statement", "CapCall"]:
            return ExtractionMode.STRUCTURED
        else:
            return ExtractionMode.TEXT_BASED
    
    def _estimate_tokens(self, text: str) -> int:
        """Estimate token count for text (rough approximation: 1 token ≈ 4 characters)"""
        return len(text) // 4
    
    def _should_chunk_text(self, text: str, schema: Dict[str, Any]) -> bool:
        """Determine if text should be chunked based on token limits"""
        if not self.config.enable_intelligent_chunking:
            return False
        
        # Estimate tokens for text + schema + prompt overhead
        text_tokens = self._estimate_tokens(text)
        schema_tokens = self._estimate_tokens(json.dumps(schema))
        prompt_overhead = 1000  # Estimated overhead for instructions
        
        total_tokens = text_tokens + schema_tokens + prompt_overhead
        
        logging.info(f"Estimated tokens: {total_tokens} (text: {text_tokens}, schema: {schema_tokens}, overhead: {prompt_overhead})")
        
        return total_tokens > self.config.max_tokens_per_request
    
    def _chunk_text(self, text: str) -> List[str]:
        """Intelligently chunk large text for processing with better boundary detection"""
        if len(text) <= self.config.chunk_size:
            return [text]
        
        chunks = []
        start = 0
        
        while start < len(text):
            end = start + self.config.chunk_size
            
            # Try to break at natural boundaries in order of preference
            if end < len(text):
                search_start = max(end - self.config.overlap_size, start)
                
                # 1. Try to find paragraph breaks (double newlines)
                paragraph_end = -1
                for i in range(end, search_start, -1):
                    if i > 0 and text[i-1:i+1] == '\n\n':
                        paragraph_end = i
                        break
                
                # 2. Try to find sentence endings
                if paragraph_end == -1:
                    for i in range(end, search_start, -1):
                        if text[i] in '.!?' and i + 1 < len(text) and text[i + 1] in ' \n':
                            paragraph_end = i + 1
                            break
                
                # 3. Try to find line breaks
                if paragraph_end == -1:
                    for i in range(end, search_start, -1):
                        if text[i] == '\n':
                            paragraph_end = i + 1
                            break
                
                if paragraph_end > search_start:
                    end = paragraph_end
            
            chunk = text[start:end].strip()
            if chunk:  # Only add non-empty chunks
                chunks.append(chunk)
            
            # Move start position with overlap
            start = end - self.config.overlap_size if end < len(text) else end
        
        logging.info(f"Text chunked into {len(chunks)} chunks for processing")
        return chunks
    
    def _assess_extraction_quality(self, extracted_data: Dict[str, Any], schema: Dict[str, Any]) -> Tuple[float, QualityLevel]:
        """Assess quality of extracted data"""
        if not extracted_data:
            return 0.0, QualityLevel.FAILED
        
        # Get schema properties
        schema_properties = schema.get("properties", {})
        required_fields = schema.get("required", [])
        
        # Calculate metrics
        total_fields = len(schema_properties)
        extracted_fields = 0
        required_fields_found = 0
        non_null_fields = 0
        
        def count_fields(data, properties):
            nonlocal extracted_fields, required_fields_found, non_null_fields
            
            if isinstance(data, dict):
                for key, value in data.items():
                    if key in properties:
                        extracted_fields += 1
                        if key in required_fields:
                            required_fields_found += 1
                        if value is not None and value != "":
                            non_null_fields += 1
                        
                        # Recursively check nested objects
                        if isinstance(value, dict) and isinstance(properties[key], dict):
                            nested_props = properties[key].get("properties", {})
                            count_fields(value, nested_props)
                        elif isinstance(value, list) and value:
                            # Check array items
                            if isinstance(properties[key], dict):
                                items_schema = properties[key].get("items", {})
                                if isinstance(items_schema, dict):
                                    item_props = items_schema.get("properties", {})
                                    for item in value:
                                        if isinstance(item, dict):
                                            count_fields(item, item_props)
        
        count_fields(extracted_data, schema_properties)
        
        # Calculate quality score
        completeness = extracted_fields / max(total_fields, 1)
        required_completeness = required_fields_found / max(len(required_fields), 1)
        data_richness = non_null_fields / max(extracted_fields, 1)
        
        quality_score = (completeness * 0.4 + required_completeness * 0.4 + data_richness * 0.2)
        
        # Determine quality level
        if quality_score >= 0.9:
            quality_level = QualityLevel.EXCELLENT
        elif quality_score >= 0.7:
            quality_level = QualityLevel.GOOD
        elif quality_score >= 0.5:
            quality_level = QualityLevel.ACCEPTABLE
        elif quality_score > 0.0:
            quality_level = QualityLevel.POOR
        else:
            quality_level = QualityLevel.FAILED
        
        return quality_score, quality_level
    
    def _save_raw_openai_response(self, raw_response: str, filename: str, response_type: str = "extraction") -> None:
        """Save raw OpenAI API response to output_documents directory"""
        try:
            # Generate filename: {original_filename}_openai_{response_type}_raw.json
            base_name = Path(filename).stem if filename else "unknown"
            output_filename = self.output_dir / f"{base_name}_openai_{response_type}_raw.json"
            
            # Parse JSON from markdown code blocks if present
            original_text = raw_response.strip()
            cleaned_text = original_text
            has_markdown_wrapper = False
            detected_format = "text"
            
            # Detect and remove markdown code blocks
            if cleaned_text.startswith("```json"):
                cleaned_text = cleaned_text[7:].strip()
                has_markdown_wrapper = True
                detected_format = "json"
            elif cleaned_text.startswith("```"):
                has_markdown_wrapper = True
                # Extract language identifier if present
                first_newline = cleaned_text.find('\n')
                if first_newline > 0:
                    lang_match = cleaned_text[3:first_newline].strip()
                    detected_format = lang_match if lang_match else "unknown"
                    cleaned_text = cleaned_text[first_newline + 1:].strip()
                else:
                    # No newline found, just remove ```
                    cleaned_text = cleaned_text[3:].strip()
            
            if cleaned_text.endswith("```"):
                cleaned_text = cleaned_text[:-3].strip()
            
            # Try to parse as JSON
            parsed_response = None
            is_valid_json = False
            try:
                parsed_response = json.loads(cleaned_text)
                is_valid_json = True
                detected_format = "json"
            except json.JSONDecodeError:
                # If parsing fails, keep as string
                parsed_response = cleaned_text
            
            # Create structured data with metadata
            raw_data = {
                "source": "openai",
                "response_type": response_type,
                "original_filename": filename,
                "timestamp": time.time(),
                "raw_response": parsed_response,
                "raw_text": cleaned_text,  # Cleaned text without markdown
                "response_metadata": {
                    "has_markdown_wrapper": has_markdown_wrapper,
                    "detected_format": detected_format,
                    "is_valid_json": is_valid_json,
                    "original_length": len(original_text),
                    "cleaned_length": len(cleaned_text)
                }
            }
            
            # Save to file
            with open(output_filename, 'w', encoding='utf-8') as f:
                json.dump(raw_data, f, ensure_ascii=False, indent=2)
            
            logging.info(f"✅ Saved raw OpenAI {response_type} response to: {output_filename}")
        except Exception as e:
            logging.warning(f"Failed to save raw OpenAI response: {e}")
    
    def _build_extraction_prompt(self, text: str, schema: Dict[str, Any], document_type: str, chunk_info: str = "") -> str:
        """Build comprehensive extraction prompt"""
        
        # NO TRUNCATION - we want the full text for accurate extraction
        # The chunking will be handled at a higher level if needed
        
        schema_str = json.dumps(schema, indent=2)
        
        chunk_instruction = ""
        if chunk_info:
            chunk_instruction = f"\n\nCHUNK INFORMATION:\n{chunk_info}\nNote: This may be part of a larger document. Extract all relevant information from this section."
        
        # Add special instructions for Distribution documents
        distribution_instructions = ""
        if document_type.lower() == "distribution":
            distribution_instructions = """
        
        SPECIAL INSTRUCTIONS FOR DISTRIBUTION DOCUMENTS:
        - Distribution documents contain tables with multiple investor rows
        - Each row in the distribution table represents a separate investor entry
        - You MUST extract ALL rows from the distribution table - do not skip any investors
        - Each investor row should be a separate entry in the portfolio array
        - Key fields to extract for each investor row:
          * InvestorRefID (Investor Code) - unique identifier for each investor
          * Distribution - the distribution amount for that investor
          * Net Distribution Amount - the net amount after withholding tax
          * Withholding Tax (if applicable)
        - Common fields like Security, TransactionDate, Currency may be shared across all entries
        - If the document spans multiple pages, extract ALL investor rows from ALL pages
        """
        
        prompt = f"""
        You are an expert data extraction specialist for financial documents.
        
        Extract structured data from the following {document_type} document according to the provided JSON schema.
        
        EXTRACTION REQUIREMENTS:
        1. Extract ALL available information that matches the schema
        2. Maintain high accuracy - only extract data you are confident about
        3. Use null for missing values, do not make up information
        4. CRITICAL: Include ALL fields defined in the schema properties, even if they are not found in the document. For fields not found, set Value to null, ConfidenceScore to null, VerbatimText to null, BoundingBox to null, and PageNumber to null. This ensures complete schema compliance.
        5. Preserve original formatting for dates, numbers, and identifiers
        6. For nested objects, extract all available sub-fields
        7. For arrays, include all relevant items found in the document
        8. IMPORTANT: Process the ENTIRE text content provided - do not skip any sections
        9. CRITICAL: For VerbatimText fields, include ONLY the exact text snippet from the document that corresponds to that specific field. DO NOT include the entire document content or unrelated text.
        10. CRITICAL FOR TABULAR DATA: When extracting from tables (especially Distribution documents), extract EVERY row from the table. Each row represents a separate entry and must be included in the portfolio array.
        {distribution_instructions}
        
        QUALITY STANDARDS:
        - Confidence level should be HIGH (>90%) for critical fields
        - Double-check numerical values and dates
        - Ensure consistency across related fields
        - Flag any ambiguous or unclear information
        - If processing chunks, ensure no data is missed between chunks
        
        VERBATIM TEXT GUIDELINES:
        - VerbatimText should contain ONLY the exact phrase/sentence from the document for that specific field
        - Examples: If extracting "Investor" field with value "John Smith", VerbatimText should be "John Smith", NOT the entire document
        - For currency fields, include just the amount with currency symbol, e.g., "USD 12,889.47"
        - For dates, include just the date as it appears, e.g., "May 30, 2025"
        - Maximum length for VerbatimText should typically be 1-2 sentences, never entire paragraphs
        
        JSON SCHEMA:
        {schema_str}{chunk_instruction}
        
        DOCUMENT CONTENT:
        {text}
        
        RESPONSE FORMAT:
        Return ONLY a valid JSON object that conforms to the schema. Do not include any explanatory text.
        """
        
        return prompt
    
    def _strip_json_comments(self, json_text: str) -> str:
        """
        Remove JavaScript-style comments from JSON text before parsing.
        Handles both single-line (//) and multi-line (/* */) comments.
        """
        # Remove single-line comments (// ...)
        # Match // but not inside strings (handled by negative lookbehind for even number of quotes)
        lines = json_text.split('\n')
        cleaned_lines = []
        in_string = False
        string_char = None
        
        for line in lines:
            cleaned_line = []
            i = 0
            while i < len(line):
                char = line[i]
                
                # Track string boundaries
                if char in ('"', "'") and (i == 0 or line[i-1] != '\\'):
                    if not in_string:
                        in_string = True
                        string_char = char
                    elif char == string_char:
                        in_string = False
                        string_char = None
                
                # If we find // and we're not in a string, this is a comment
                if not in_string and i < len(line) - 1 and line[i:i+2] == '//':
                    # Found comment, skip rest of line
                    break
                
                cleaned_line.append(char)
                i += 1
            
            cleaned_lines.append(''.join(cleaned_line))
        
        json_text = '\n'.join(cleaned_lines)
        
        # Remove multi-line comments (/* ... */)
        # Simple regex approach - remove /* ... */ blocks
        # This is safe because JSON doesn't use /* */ for anything else
        json_text = re.sub(r'/\*.*?\*/', '', json_text, flags=re.DOTALL)
        
        return json_text.strip()
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=2, min=4, max=120),
        retry=retry_if_exception_type((openai.RateLimitError, openai.APITimeoutError, openai.APIConnectionError)),
        before_sleep=before_sleep_log(logging.getLogger(), logging.WARNING),
        after=after_log(logging.getLogger(), logging.INFO)
    )
    async def _extract_with_llm(self, text: str, schema: Dict[str, Any], document_type: str, filename: Optional[str] = None, chunk_info: str = "") -> Tuple[Dict[str, Any], Dict[str, Any]]:
        """Enhanced LLM extraction with retry logic"""
        extraction_start = time.time()
        
        try:
            prompt = self._build_extraction_prompt(text, schema, document_type, chunk_info)
            
            response = self.client.chat.completions.create(
                model=self.config.model_name,
                messages=[
                    {"role": "system", "content": "You are a precise data extraction expert. Return only valid JSON."},
                    {"role": "user", "content": prompt}
                ],
                max_tokens=self.config.max_tokens,
                temperature=self.config.temperature,
                timeout=self.config.api_timeout
            )
            
            # Record metrics
            usage = response.usage
            if usage:
                self.metrics.record_api_call(usage.prompt_tokens, usage.completion_tokens)
            
            # Parse response
            extracted_text = response.choices[0].message.content.strip()
            
            # Save raw OpenAI response before processing
            # COMMENTED OUT: Disabled extraction raw JSON file generation - only output file needed
            # if filename:
            #     self._save_raw_openai_response(extracted_text, filename, "extraction")
            
            # Clean up response (remove markdown formatting if present)
            if extracted_text.startswith("```json"):
                extracted_text = extracted_text[7:]
            if extracted_text.endswith("```"):
                extracted_text = extracted_text[:-3]
            
            # Remove JavaScript-style comments that LLM might add (invalid in JSON)
            extracted_text = self._strip_json_comments(extracted_text)
            
            extracted_data = json.loads(extracted_text)
            
            # Post-process to ensure all schema fields are present
            extracted_data = self._ensure_all_schema_fields(extracted_data, schema)
            
            # Validate and correct numeric sums from VerbatimText
            extracted_data = self._validate_and_correct_numeric_sums(extracted_data)
            
            # Create metadata
            metadata = {
                "extraction_time": time.time() - extraction_start,
                "model_used": self.config.model_name,
                "prompt_tokens": usage.prompt_tokens if usage else 0,
                "completion_tokens": usage.completion_tokens if usage else 0,
                "total_tokens": usage.total_tokens if usage else 0,
                "chunk_info": chunk_info if chunk_info else "single_pass"
            }
            
            return extracted_data, metadata
            
        except json.JSONDecodeError as e:
            self.metrics.record_error()
            logging.error(f"JSON parsing failed: {e}")
            raise ValueError(f"Invalid JSON response from LLM: {e}")
        except Exception as e:
            self.metrics.record_error()
            logging.error(f"LLM extraction failed: {e}")
            raise
    
    def _merge_extracted_data(self, chunk_results: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Merge extraction results from multiple chunks"""
        if not chunk_results:
            return {}
        
        if len(chunk_results) == 1:
            return chunk_results[0]
        
        # Start with the first result as base
        merged_data = copy.deepcopy(chunk_results[0])
        
        def merge_recursive(base_data: Any, new_data: Any) -> Any:
            """Recursively merge data structures, prioritizing non-null values"""
            if isinstance(base_data, dict) and isinstance(new_data, dict):
                for key, value in new_data.items():
                    if key in base_data:
                        base_data[key] = merge_recursive(base_data[key], value)
                    else:
                        base_data[key] = value
                return base_data
            elif isinstance(base_data, list) and isinstance(new_data, list):
                # For lists, extend with unique items
                for item in new_data:
                    if item not in base_data:
                        base_data.append(item)
                return base_data
            else:
                # For primitive values, prefer non-null values
                if new_data is not None and (base_data is None or str(base_data).strip() == ""):
                    return new_data
                return base_data
        
        # Merge each subsequent chunk result
        for chunk_result in chunk_results[1:]:
            merged_data = merge_recursive(merged_data, chunk_result)
        
        logging.info(f"Merged {len(chunk_results)} chunk results into final extraction")
        return merged_data

    async def _extract_with_chunking(self, text: str, schema: Dict[str, Any], document_type: str, filename: Optional[str] = None) -> Tuple[Dict[str, Any], Dict[str, Any]]:
        """Extract data using chunking strategy for large documents"""
        chunks = self._chunk_text(text)
        chunk_results = []
        total_metadata = {
            "chunk_count": len(chunks),
            "extraction_time": 0,
            "model_used": self.config.model_name,
            "prompt_tokens": 0,
            "completion_tokens": 0,
            "total_tokens": 0
        }
        
        for i, chunk in enumerate(chunks):
            chunk_info = f"Chunk {i+1} of {len(chunks)}"
            logging.info(f"Processing {chunk_info} ({len(chunk)} characters)")
            
            try:
                chunk_data, chunk_metadata = await self._extract_with_llm(
                    chunk, schema, document_type, filename, chunk_info
                )
                chunk_results.append(chunk_data)
                
                # Accumulate metadata
                total_metadata["extraction_time"] += chunk_metadata.get("extraction_time", 0)
                total_metadata["prompt_tokens"] += chunk_metadata.get("prompt_tokens", 0)
                total_metadata["completion_tokens"] += chunk_metadata.get("completion_tokens", 0)
                total_metadata["total_tokens"] += chunk_metadata.get("total_tokens", 0)
                
            except Exception as e:
                logging.warning(f"Failed to process {chunk_info}: {e}")
                # Continue with other chunks even if one fails
                continue
        
        if not chunk_results:
            raise Exception("All chunks failed to process")
        
        # Merge all chunk results
        merged_data = self._merge_extracted_data(chunk_results)
        
        # Ensure all schema fields are present after merging
        merged_data = self._ensure_all_schema_fields(merged_data, schema)
        
        return merged_data, total_metadata

    async def _extract_with_retry(self, text: str, schema: Dict[str, Any], document_type: str, filename: Optional[str] = None) -> Tuple[Dict[str, Any], Dict[str, Any]]:
        """Extract with intelligent retry and error recovery, handling large documents"""
        
        # Check cache first
        content_hash = self._compute_content_hash(text, document_type)
        cached_result = self._get_from_cache(content_hash)
        if cached_result:
            return cached_result["data"], cached_result["metadata"]
        
        # Determine processing strategy
        should_chunk = self._should_chunk_text(text, schema)
        
        if should_chunk:
            logging.info(f"Document is large ({len(text)} chars), using chunking strategy")
            try:
                extracted_data, metadata = await self._extract_with_chunking(text, schema, document_type, filename)
                
                # Store in cache
                cache_data = {"data": extracted_data, "metadata": metadata}
                self._store_in_cache(content_hash, cache_data)
                
                return extracted_data, metadata
                
            except Exception as e:
                logging.error(f"Chunking strategy failed: {e}")
                # Fall back to truncated processing as last resort
                logging.warning("Falling back to truncated processing")
                truncated_text = text[:50000] + "\n...\n[DOCUMENT TRUNCATED]"
                should_chunk = False
                text = truncated_text
        
        # Single-pass processing for smaller documents or fallback
        if not should_chunk:
            logging.info(f"Processing document as single unit ({len(text)} chars)")
        
        last_exception = None
        
        for attempt in range(self.config.max_retries + 1):
            try:
                extracted_data, metadata = await self._extract_with_llm(text, schema, document_type, filename)
                
                # Store in cache
                cache_data = {"data": extracted_data, "metadata": metadata}
                self._store_in_cache(content_hash, cache_data)
                
                return extracted_data, metadata
                
            except Exception as e:
                last_exception = e
                self.metrics.record_retry()
                
                if attempt < self.config.max_retries:
                    if self.config.exponential_backoff:
                        delay = min(
                            self.config.retry_delay_base * (2 ** attempt),
                            self.config.retry_delay_max
                        )
                    else:
                        delay = self.config.retry_delay_base
                    
                    # Add jitter if enabled
                    if self.config.jitter:
                        import random
                        delay *= (0.5 + random.random() * 0.5)
                    
                    logging.warning(f"Extraction attempt {attempt + 1} failed, retrying in {delay:.2f}s: {e}")
                    await asyncio.sleep(delay)
                    continue
                else:
                    break
        
        # Final fallback
        logging.error(f"All extraction attempts failed: {last_exception}")
        raise Exception(f"Extraction failed after {self.config.max_retries} attempts: {last_exception}")
    
    def _ensure_all_schema_fields(self, extracted_data: Dict[str, Any], schema: Dict[str, Any]) -> Dict[str, Any]:
        """
        Ensure all fields defined in the schema are present in the extracted data.
        Adds missing optional fields with null values to maintain schema compliance.
        """
        try:
            # Navigate to the portfolio properties in the schema
            entities_schema = schema.get("properties", {}).get("entities", {})
            if not entities_schema:
                logging.warning("Schema structure: entities not found in properties")
                return extracted_data
            
            items_schema = entities_schema.get("items", {})
            if not items_schema:
                logging.warning("Schema structure: items not found in entities")
                return extracted_data
            
            portfolio_schema = items_schema.get("properties", {}).get("portfolio", {})
            if not portfolio_schema:
                logging.warning("Schema structure: portfolio not found in items properties")
                return extracted_data
            
            portfolio_items_schema = portfolio_schema.get("items", {})
            if not portfolio_items_schema:
                logging.warning("Schema structure: portfolio items not found")
                return extracted_data
            
            portfolio_properties = portfolio_items_schema.get("properties", {})
            if not portfolio_properties:
                logging.warning("Schema structure: portfolio properties not found")
                return extracted_data
            
            # Navigate through extracted data structure
            entities = extracted_data.get("entities", [])
            if not isinstance(entities, list):
                logging.warning(f"Extracted data: entities is not a list, got {type(entities)}")
                return extracted_data
            
            # Default null field structure
            def create_null_field():
                return {
                    "Value": None,
                    "ConfidenceScore": None,
                    "VerbatimText": None,
                    "BoundingBox": None,
                    "PageNumber": None
                }
            
            total_fields_in_schema = len(portfolio_properties)
            fields_added_count = 0
            fields_already_present = 0
            
            # Process each entity
            for entity_idx, entity in enumerate(entities):
                if not isinstance(entity, dict):
                    continue
                
                portfolio = entity.get("portfolio", [])
                if not isinstance(portfolio, list):
                    continue
                
                # Process each portfolio item
                for portfolio_idx, portfolio_item in enumerate(portfolio):
                    if not isinstance(portfolio_item, dict):
                        continue
                    
                    # Check each field in schema properties
                    for field_name in portfolio_properties.keys():
                        # Skip if field already exists
                        if field_name in portfolio_item:
                            fields_already_present += 1
                            continue
                        
                        # Add missing field with null values
                        portfolio_item[field_name] = create_null_field()
                        fields_added_count += 1
                        logging.info(f"Added missing schema field '{field_name}' with null values (entity {entity_idx}, portfolio {portfolio_idx})")
            
            if fields_added_count > 0:
                logging.info(f"Schema field completion: Added {fields_added_count} missing fields, {fields_already_present} already present, {total_fields_in_schema} total in schema")
            else:
                logging.debug(f"Schema field completion: All {total_fields_in_schema} fields already present")
            
            return extracted_data
            
        except Exception as e:
            logging.error(f"Error ensuring all schema fields: {e}", exc_info=True)
            return extracted_data
    
    def _validate_and_correct_numeric_sums(self, extracted_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Validate and correct numeric sums by recalculating from VerbatimText.
        When VerbatimText contains comma-separated numbers, recalculate the sum
        and correct the Value if it differs.
        """
        try:
            entities = extracted_data.get("entities", [])
            if not isinstance(entities, list):
                return extracted_data
            
            corrections_count = 0
            
            for entity in entities:
                if not isinstance(entity, dict):
                    continue
                
                portfolio = entity.get("portfolio", [])
                if not isinstance(portfolio, list):
                    continue
                
                for portfolio_item in portfolio:
                    if not isinstance(portfolio_item, dict):
                        continue
                    
                    # Check each field in the portfolio item
                    for field_name, field_data in portfolio_item.items():
                        if not isinstance(field_data, dict):
                            continue
                        
                        value = field_data.get("Value")
                        verbatim_text = field_data.get("VerbatimText")
                        
                        # Skip if Value is None or VerbatimText is None/empty
                        if value is None or not verbatim_text:
                            continue
                        
                        # Check if VerbatimText contains comma-separated numbers
                        # Pattern: numbers separated by commas (with optional spaces)
                        if not isinstance(verbatim_text, str):
                            continue
                        
                        # Extract all numbers from VerbatimText (handles comma-separated format)
                        # Split by comma first, then extract numbers from each part
                        parts = verbatim_text.split(',')
                        numbers_str = []
                        for part in parts:
                            # Extract number from each part (handles decimals and negatives)
                            num_matches = re.findall(r'-?\d+\.?\d*', part.strip())
                            numbers_str.extend(num_matches)
                        
                        if len(numbers_str) < 2:
                            continue  # Not multiple numbers, skip
                        
                        try:
                            # Convert to floats and calculate sum, preserving precision
                            numbers = [float(num) for num in numbers_str]
                            calculated_sum = sum(numbers)
                            
                            # Determine maximum decimal precision from the numbers
                            max_decimals = 0
                            for num_str in numbers_str:
                                if '.' in num_str:
                                    decimals = len(num_str.split('.')[1])
                                    max_decimals = max(max_decimals, decimals)
                            
                            # Check if Value is a number
                            if not isinstance(value, (int, float)):
                                try:
                                    value = float(value)
                                except (ValueError, TypeError):
                                    continue
                            
                            # Compare with tolerance for floating point errors
                            tolerance = 0.01
                            if abs(value - calculated_sum) > tolerance:
                                # Correct the Value, preserving the maximum decimal precision found
                                if max_decimals > 0:
                                    # Preserve original precision (up to reasonable limit of 10 decimal places)
                                    field_data["Value"] = round(calculated_sum, min(max_decimals, 10))
                                else:
                                    # If no decimals, keep as integer
                                    field_data["Value"] = int(round(calculated_sum))
                                corrections_count += 1
                                logging.info(
                                    f"Corrected sum for field '{field_name}': "
                                    f"LLM calculated {value}, actual sum from VerbatimText is {calculated_sum} "
                                    f"(preserving {max_decimals} decimal places). VerbatimText: {verbatim_text}"
                                )
                        except (ValueError, TypeError) as e:
                            logging.debug(f"Could not recalculate sum for field '{field_name}': {e}")
                            continue
            
            if corrections_count > 0:
                logging.info(f"Validated and corrected {corrections_count} numeric sum(s) from VerbatimText")
            
            return extracted_data
            
        except Exception as e:
            logging.error(f"Error validating numeric sums: {e}", exc_info=True)
            return extracted_data
    
    def _validate_extracted_data(self, data: Dict[str, Any], schema: Dict[str, Any]) -> Tuple[bool, List[str]]:
        """Validate extracted data against schema"""
        validation_start = time.time()
        errors = []
        
        try:
            # Basic validation
            if not isinstance(data, dict):
                errors.append("Extracted data is not a dictionary")
                return False, errors
            
            # Check required fields
            required_fields = schema.get("required", [])
            for field in required_fields:
                if field not in data or data[field] is None:
                    errors.append(f"Required field '{field}' is missing or null")
            
            # Type validation for properties
            properties = schema.get("properties", {})
            for field, field_schema in properties.items():
                if field in data and data[field] is not None:
                    expected_type = field_schema.get("type")
                    value = data[field]
                    
                    if expected_type == "string" and not isinstance(value, str):
                        errors.append(f"Field '{field}' should be string, got {type(value).__name__}")
                    elif expected_type == "number" and not isinstance(value, (int, float)):
                        errors.append(f"Field '{field}' should be number, got {type(value).__name__}")
                    elif expected_type == "array" and not isinstance(value, list):
                        errors.append(f"Field '{field}' should be array, got {type(value).__name__}")
                    elif expected_type == "object" and not isinstance(value, dict):
                        errors.append(f"Field '{field}' should be object, got {type(value).__name__}")
            
            self.metrics.schema_validation_time += time.time() - validation_start
            self.metrics.record_validation_attempt()
            
            return len(errors) == 0, errors
            
        except Exception as e:
            errors.append(f"Validation error: {e}")
            return False, errors
    
    def __call__(self, doc_payload: AithonDocument) -> AithonDocument:
        """
        Enhanced document extraction with comprehensive error handling and monitoring.
        """
        start_time = time.time()
        self.metrics = ExtractionMetrics()  # Reset metrics for this document
        
        logging.info(f"Entering Advanced Extraction Box for: {doc_payload.original_filename}")
        doc_payload.pipeline_status = "Extraction"
        
        # Validate prerequisites
        if not doc_payload.document_type:
            doc_payload.error_message = "Document type required for extraction"
            doc_payload.pipeline_status = "Failed_Extraction"
            return doc_payload
        
        if not doc_payload.cleaned_text:
            doc_payload.error_message = "No cleaned text available for extraction"
            doc_payload.pipeline_status = "Failed_Extraction"
            return doc_payload
        
        try:
            # Load schema
            schema = self._load_schema(doc_payload.document_type)
            if not self._validate_schema(schema):
                raise ValueError(f"Invalid schema for document type: {doc_payload.document_type}")
            
            # Determine extraction mode
            extraction_mode = self._determine_extraction_mode(doc_payload.cleaned_text, doc_payload.document_type)
            
            logging.info(f"Selected extraction mode: {extraction_mode.value}")
            
            # Perform extraction
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            
            try:
                extracted_data, extraction_metadata = loop.run_until_complete(
                    self._extract_with_retry(doc_payload.cleaned_text, schema, doc_payload.document_type, doc_payload.original_filename)
                )
            finally:
                loop.close()
            
            # Validate extracted data
            is_valid, validation_errors = self._validate_extracted_data(extracted_data, schema)
            
            # Assess quality
            quality_score, quality_level = self._assess_extraction_quality(extracted_data, schema)
            
            # Update document payload
            doc_payload.extracted_data = extracted_data
            doc_payload.validation_errors = validation_errors if not is_valid else []
            
            # Collect comprehensive metrics
            processing_metrics = self.metrics.get_summary()
            processing_metrics.update(extraction_metadata)
            
            # Store metadata
            doc_payload.metadata.update({
                "extraction_mode": extraction_mode.value,
                "schema_loaded": True,
                "is_scanned": False,  # Assuming text-based for now
                "schema_validation_passed": is_valid,
                "validation_errors": validation_errors,
                "extraction_quality_score": quality_score,
                "extraction_quality_level": quality_level.value,
                "total_processing_time": time.time() - start_time,
                "extraction_metrics": processing_metrics
            })
            
            # Set pipeline status
            if is_valid and quality_level in [QualityLevel.EXCELLENT, QualityLevel.GOOD]:
                doc_payload.pipeline_status = "Extraction_Completed"
            elif is_valid and quality_level == QualityLevel.ACCEPTABLE:
                doc_payload.pipeline_status = "Extraction_Completed_Low_Quality"
            else:
                doc_payload.pipeline_status = "Extraction_Completed_With_Issues"
            
            # Enhanced logging
            logging.info(f"Extraction completed for '{doc_payload.original_filename}': "
                        f"Quality: {quality_level.value} ({quality_score:.2f}), "
                        f"Valid: {is_valid}, Provider: openai, "
                        f"Retries: {processing_metrics['retry_count']}, "
                        f"Time: {processing_metrics['total_processing_time']:.2f}s")
            
        except Exception as e:
            logging.error(f"Extraction failed for {doc_payload.original_filename}: {e}", exc_info=True)
            doc_payload.error_message = f"Extraction failed: {e}"
            doc_payload.pipeline_status = "Failed_Extraction"
            
            # Store error metadata
            doc_payload.metadata.update({
                "error_details": str(e),
                "processing_time": time.time() - start_time,
                "extraction_metrics": self.metrics.get_summary()
            })
        
        return doc_payload

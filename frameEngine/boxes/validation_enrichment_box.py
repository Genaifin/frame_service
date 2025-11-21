import logging
import json
import time
import re
from pathlib import Path
from typing import Dict, Any, List, Optional, Tuple, Union
from enum import Enum
from dataclasses import dataclass, field
from datetime import datetime, UTC

import jsonschema
from jsonschema import validate, ValidationError
from rapidfuzz import process, fuzz
import numpy as np

from ..data_model import AithonDocument

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# ============================================================================
# Configuration and Data Models
# ============================================================================

class ValidationSeverity(Enum):
    """Validation error severity levels"""
    CRITICAL = "CRITICAL"
    HIGH = "HIGH"
    MEDIUM = "MEDIUM"
    LOW = "LOW"
    INFO = "INFO"

class EnrichmentQuality(Enum):
    """Enrichment quality levels"""
    EXCELLENT = "EXCELLENT"
    GOOD = "GOOD"
    FAIR = "FAIR"
    POOR = "POOR"
    NONE = "NONE"

class MatchingStrategy(Enum):
    """Bounding box matching strategies"""
    EXACT_MATCH = "exact_match"
    FUZZY_MATCH = "fuzzy_match"
    SEMANTIC_MATCH = "semantic_match"
    SEQUENCE_MATCH = "sequence_match"

@dataclass
class ValidationConfig:
    """Configuration for validation and enrichment behavior"""
    
    # Validation settings
    strict_validation: bool = False
    allow_additional_properties: bool = True
    validate_data_types: bool = True
    validate_formats: bool = True
    
    # Enrichment settings
    enable_bounding_box_enrichment: bool = False
    enable_confidence_scoring: bool = True
    enable_data_quality_assessment: bool = True
    
    # Matching thresholds
    exact_match_threshold: float = 1.0
    fuzzy_match_threshold: float = 0.8
    semantic_match_threshold: float = 0.75
    sequence_match_threshold: float = 0.85
    
    # Matching strategies priority
    matching_strategies: List[MatchingStrategy] = field(default_factory=lambda: [
        MatchingStrategy.EXACT_MATCH,
        MatchingStrategy.SEQUENCE_MATCH,
        MatchingStrategy.FUZZY_MATCH,
        MatchingStrategy.SEMANTIC_MATCH
    ])
    
    # Quality thresholds
    min_completeness_score: float = 0.7
    min_accuracy_score: float = 0.8
    min_consistency_score: float = 0.75
    
    # Error handling
    max_validation_errors: int = 50
    continue_on_validation_error: bool = True
    auto_fix_simple_errors: bool = True
    
    # Performance settings
    max_processing_time: int = 300  # 5 minutes
    batch_size_for_enrichment: int = 100

@dataclass
class ValidationResult:
    """Comprehensive validation result"""
    is_valid: bool
    errors: List[Dict[str, Any]]
    warnings: List[Dict[str, Any]]
    severity_counts: Dict[str, int]
    completeness_score: float
    accuracy_score: float
    consistency_score: float
    overall_quality_score: float
    processing_time: float
    
@dataclass
class EnrichmentResult:
    """Comprehensive enrichment result"""
    enriched_fields: int
    total_fields: int
    enrichment_rate: float
    bounding_boxes_added: int
    confidence_scores_added: int
    quality_level: EnrichmentQuality
    processing_time: float
    errors: List[str]

# ============================================================================
# Enhanced Validation & Enrichment Box
# ============================================================================

class ValidationEnrichmentBox:
    """
    Enhanced validation and enrichment system with comprehensive error handling,
    quality assessment, advanced bounding box matching, and detailed reporting.
    """

    def __init__(self, config: Optional[ValidationConfig] = None):
        self.config = config or ValidationConfig()

    def _load_schema(self, document_type: str) -> Dict[str, Any]:
        """Load JSON schema for validation"""
        # Use robust path resolution for Docker containers
        possible_paths = [
            # Docker container path
            Path("/app/frameEngine/schemas"),
            # Local development path
            Path("frameEngine/schemas"),
            # Relative to current file
            Path(__file__).parent.parent / "schemas"
        ]
        
        schema_dir = None
        for path in possible_paths:
            if path.exists():
                schema_dir = path
                logging.info(f"Found schema directory: {schema_dir}")
                break
        
        if not schema_dir:
            logging.error(f"Schema directory not found. Tried: {[str(p) for p in possible_paths]}")
            raise FileNotFoundError(f"Schema directory not found. Tried: {[str(p) for p in possible_paths]}")
        
        # Handle schema file naming variations
        possible_names = [
            f"{document_type.lower()}_schema.json",
            f"{document_type.lower()}s_schema.json",
            f"{document_type}_schema.json"
        ]
        
        for name in possible_names:
            schema_file = schema_dir / name
            if schema_file.exists():
                try:
                    logging.info(f"Loading schema file: {schema_file}")
                    with open(schema_file, 'r') as f:
                        schema = json.load(f)
                        logging.info(f"Successfully loaded schema for document type '{document_type}'")
                        return schema
                except Exception as e:
                    logging.error(f"Error loading schema {schema_file}: {e}")
        
        # List available schema files for debugging
        available_files = list(schema_dir.glob("*.json")) if schema_dir else []
        logging.error(f"Schema file not found for document type '{document_type}'. Available files: {[f.name for f in available_files]}")
        raise FileNotFoundError(f"Schema file not found for document type '{document_type}'")

    def _categorize_validation_error(self, error: ValidationError) -> ValidationSeverity:
        """Categorize validation errors by severity"""
        error_message = error.message.lower()
        
        # Critical errors
        if any(keyword in error_message for keyword in ['required', 'missing', 'none is not of type']):
            return ValidationSeverity.CRITICAL
        
        # High severity errors
        elif any(keyword in error_message for keyword in ['type', 'format', 'invalid']):
            return ValidationSeverity.HIGH
        
        # Medium severity errors
        elif any(keyword in error_message for keyword in ['pattern', 'length', 'range']):
            return ValidationSeverity.MEDIUM
        
        # Low severity errors
        elif any(keyword in error_message for keyword in ['additional', 'extra']):
            return ValidationSeverity.LOW
        
        else:
            return ValidationSeverity.INFO

    def _validate_schema_comprehensive(self, data: Dict[str, Any], schema: Dict[str, Any]) -> ValidationResult:
        """Comprehensive schema validation with detailed error analysis"""
        start_time = time.time()
        
        try:
            # Create validator
            validator = jsonschema.Draft7Validator(schema)
            
            # Collect all errors
            errors = []
            warnings = []
            severity_counts = {severity.value: 0 for severity in ValidationSeverity}
            
            for error in validator.iter_errors(data):
                severity = self._categorize_validation_error(error)
                severity_counts[severity.value] += 1
                
                error_info = {
                    "path": list(error.absolute_path),
                    "message": error.message,
                    "severity": severity.value,
                    "field": '.'.join(str(p) for p in error.absolute_path),
                    "schema_path": list(error.schema_path),
                    "invalid_value": error.instance if hasattr(error, 'instance') else None
                }
                
                if severity in [ValidationSeverity.CRITICAL, ValidationSeverity.HIGH]:
                    errors.append(error_info)
                else:
                    warnings.append(error_info)
            
            # Calculate quality scores
            completeness_score = self._calculate_completeness_score(data, schema)
            accuracy_score = self._calculate_accuracy_score(data, schema, errors)
            consistency_score = self._calculate_consistency_score(data, schema)
            
            # Overall quality score
            overall_quality_score = (completeness_score * 0.4 + 
                                   accuracy_score * 0.4 + 
                                   consistency_score * 0.2)
            
            is_valid = len(errors) == 0
            processing_time = time.time() - start_time
            
            return ValidationResult(
                is_valid=is_valid,
                errors=errors,
                warnings=warnings,
                severity_counts=severity_counts,
                completeness_score=completeness_score,
                accuracy_score=accuracy_score,
                consistency_score=consistency_score,
                overall_quality_score=overall_quality_score,
                processing_time=processing_time
            )
            
        except Exception as e:
            logging.error(f"Schema validation failed: {e}")
            return ValidationResult(
                is_valid=False,
                errors=[{"message": f"Validation system error: {e}", "severity": "CRITICAL"}],
                warnings=[],
                severity_counts={severity.value: 0 for severity in ValidationSeverity},
                completeness_score=0.0,
                accuracy_score=0.0,
                consistency_score=0.0,
                overall_quality_score=0.0,
                processing_time=time.time() - start_time
            )

    def _calculate_completeness_score(self, data: Dict[str, Any], schema: Dict[str, Any]) -> float:
        """Calculate how complete the data is relative to the schema"""
        try:
            properties = schema.get("properties", {})
            required_fields = schema.get("required", [])
            
            if not properties:
                return 1.0
            
            total_fields = len(properties)
            filled_fields = 0
            required_filled = 0
            
            for field_name in properties:
                if field_name in data and data[field_name] is not None:
                    # Check if field has meaningful content
                    value = data[field_name]
                    if value != "" and value != {} and value != []:
                        filled_fields += 1
                        if field_name in required_fields:
                            required_filled += 1
            
            # Weight required fields more heavily
            if required_fields:
                required_completeness = required_filled / len(required_fields)
                optional_completeness = filled_fields / total_fields
                return (required_completeness * 0.7) + (optional_completeness * 0.3)
            else:
                return filled_fields / total_fields
                
        except Exception as e:
            logging.warning(f"Error calculating completeness score: {e}")
            return 0.0

    def _calculate_accuracy_score(self, data: Dict[str, Any], schema: Dict[str, Any], errors: List[Dict[str, Any]]) -> float:
        """Calculate accuracy based on validation errors"""
        try:
            properties = schema.get("properties", {})
            total_fields = len(properties)
            
            if total_fields == 0:
                return 1.0
            
            # Weight errors by severity
            error_penalty = 0
            for error in errors:
                severity = error.get("severity", "INFO")
                if severity == "CRITICAL":
                    error_penalty += 1.0
                elif severity == "HIGH":
                    error_penalty += 0.7
                elif severity == "MEDIUM":
                    error_penalty += 0.4
                elif severity == "LOW":
                    error_penalty += 0.2
            
            # Calculate accuracy score
            accuracy = max(0.0, 1.0 - (error_penalty / total_fields))
            return accuracy
            
        except Exception as e:
            logging.warning(f"Error calculating accuracy score: {e}")
            return 0.0

    def _calculate_consistency_score(self, data: Dict[str, Any], schema: Dict[str, Any]) -> float:
        """Calculate consistency score based on data patterns"""
        try:
            # Check for consistent data types and formats
            consistency_checks = 0
            passed_checks = 0
            
            # Check date formats
            date_fields = []
            for field_name, field_value in data.items():
                if isinstance(field_value, str) and self._looks_like_date(field_value):
                    date_fields.append(field_value)
            
            if date_fields:
                consistency_checks += 1
                if self._dates_have_consistent_format(date_fields):
                    passed_checks += 1
            
            # Check numeric formats
            numeric_fields = []
            for field_name, field_value in data.items():
                if isinstance(field_value, (int, float)):
                    numeric_fields.append(field_value)
            
            if numeric_fields:
                consistency_checks += 1
                if self._numbers_have_reasonable_precision(numeric_fields):
                    passed_checks += 1
            
            # Check string formats
            string_fields = []
            for field_name, field_value in data.items():
                if isinstance(field_value, str) and field_value.strip():
                    string_fields.append(field_value)
            
            if string_fields:
                consistency_checks += 1
                if self._strings_have_consistent_case(string_fields):
                    passed_checks += 1
            
            return passed_checks / consistency_checks if consistency_checks > 0 else 1.0
            
        except Exception as e:
            logging.warning(f"Error calculating consistency score: {e}")
            return 0.0

    def _looks_like_date(self, value: str) -> bool:
        """Check if a string looks like a date"""
        date_patterns = [
            r'\d{4}-\d{2}-\d{2}',
            r'\d{2}/\d{2}/\d{4}',
            r'\d{2}-\d{2}-\d{4}'
        ]
        return any(re.match(pattern, value) for pattern in date_patterns)

    def _dates_have_consistent_format(self, dates: List[str]) -> bool:
        """Check if dates have consistent formatting"""
        if len(dates) <= 1:
            return True
        
        # Check if all dates match the same pattern
        patterns = [
            r'\d{4}-\d{2}-\d{2}',
            r'\d{2}/\d{2}/\d{4}',
            r'\d{2}-\d{2}-\d{4}'
        ]
        
        for pattern in patterns:
            if all(re.match(pattern, date) for date in dates):
                return True
        
        return False

    def _numbers_have_reasonable_precision(self, numbers: List[Union[int, float]]) -> bool:
        """Check if numbers have reasonable precision"""
        if len(numbers) <= 1:
            return True
        
        # Check if precision is consistent for similar magnitude numbers
        float_numbers = [n for n in numbers if isinstance(n, float)]
        if not float_numbers:
            return True
        
        # Group by magnitude and check precision consistency
        return True  # Simplified for now

    def _strings_have_consistent_case(self, strings: List[str]) -> bool:
        """Check if strings have consistent case patterns"""
        if len(strings) <= 1:
            return True
        
        # Check for consistent title case, upper case, or lower case
        title_case_count = sum(1 for s in strings if s.istitle())
        upper_case_count = sum(1 for s in strings if s.isupper())
        lower_case_count = sum(1 for s in strings if s.islower())
        
        total = len(strings)
        # Consider consistent if 80% follow the same pattern
        return (title_case_count / total > 0.8 or 
                upper_case_count / total > 0.8 or 
                lower_case_count / total > 0.8)

    def _find_bounding_box_advanced(self, text_to_find: str, doc_payload: AithonDocument) -> Dict[str, Any]:
        """
        Advanced bounding box finding with multiple matching strategies
        """
        if not text_to_find or not isinstance(text_to_find, str):
            return {}

        if not doc_payload.pages:
            return {}

        # Create comprehensive word list with metadata
        all_words = []
        for page in doc_payload.pages:
            for word_info in page.words:
                word_text = word_info.get("text", "").strip()
                if word_text:
                    all_words.append({
                        "text": word_text,
                        "original_text": word_info.get("text", ""),
                        "page": page.page_number,
                        "box": (
                            word_info.get("left", 0),
                            word_info.get("top", 0),
                            word_info.get("width", 0),
                            word_info.get("height", 0)
                        ),
                        "confidence": word_info.get("confidence", 0.0)
                    })

        if not all_words:
            return {}

        # Try different matching strategies
        for strategy in self.config.matching_strategies:
            result = self._try_matching_strategy(text_to_find, all_words, strategy)
            if result:
                result["matching_strategy"] = strategy.value
                return result

        return {}

    def _try_matching_strategy(self, text_to_find: str, all_words: List[Dict], strategy: MatchingStrategy) -> Optional[Dict[str, Any]]:
        """Try a specific matching strategy"""
        try:
            if strategy == MatchingStrategy.EXACT_MATCH:
                return self._exact_match(text_to_find, all_words)
            elif strategy == MatchingStrategy.SEQUENCE_MATCH:
                return self._sequence_match(text_to_find, all_words)
            elif strategy == MatchingStrategy.FUZZY_MATCH:
                return self._fuzzy_match(text_to_find, all_words)
            elif strategy == MatchingStrategy.SEMANTIC_MATCH:
                return self._semantic_match(text_to_find, all_words)
        except Exception as e:
            logging.warning(f"Error in {strategy.value} matching: {e}")
        
        return None

    def _exact_match(self, text_to_find: str, all_words: List[Dict]) -> Optional[Dict[str, Any]]:
        """Exact text matching"""
        for word in all_words:
            if word["text"].lower() == text_to_find.lower():
                return {
                    "page_number": word["page"],
                    "bounding_box": word["box"],
                    "confidence": 1.0,
                    "matched_text": word["text"]
                }
        return None

    def _sequence_match(self, text_to_find: str, all_words: List[Dict]) -> Optional[Dict[str, Any]]:
        """Match sequences of words"""
        search_words = text_to_find.lower().split()
        if len(search_words) <= 1:
            return None

        # Group words by page
        pages = {}
        for word in all_words:
            page_num = word["page"]
            if page_num not in pages:
                pages[page_num] = []
            pages[page_num].append(word)

        # Search for sequences in each page
        for page_num, page_words in pages.items():
            for i in range(len(page_words) - len(search_words) + 1):
                sequence = page_words[i:i + len(search_words)]
                sequence_text = " ".join(w["text"].lower() for w in sequence)
                
                # Calculate similarity
                similarity = fuzz.ratio(text_to_find.lower(), sequence_text) / 100.0
                
                if similarity >= self.config.sequence_match_threshold:
                    # Calculate combined bounding box
                    combined_box = self._combine_bounding_boxes([w["box"] for w in sequence])
                    return {
                        "page_number": page_num,
                        "bounding_box": combined_box,
                        "confidence": similarity,
                        "matched_text": " ".join(w["text"] for w in sequence)
                    }

        return None

    def _fuzzy_match(self, text_to_find: str, all_words: List[Dict]) -> Optional[Dict[str, Any]]:
        """Fuzzy text matching"""
        word_texts = [w["text"] for w in all_words]
        best_match = process.extractOne(text_to_find, word_texts, scorer=fuzz.ratio)

        if best_match and best_match[1] / 100.0 >= self.config.fuzzy_match_threshold:
            # Find the corresponding word
            for word in all_words:
                if word["text"] == best_match[0]:
                    return {
                        "page_number": word["page"],
                        "bounding_box": word["box"],
                        "confidence": best_match[1] / 100.0,
                        "matched_text": word["text"]
                    }

        return None

    def _semantic_match(self, text_to_find: str, all_words: List[Dict]) -> Optional[Dict[str, Any]]:
        """Semantic matching (simplified - could use embeddings)"""
        # For now, use partial matching and synonyms
        search_terms = text_to_find.lower().split()
        
        for word in all_words:
            word_text = word["text"].lower()
            
            # Check if any search term is contained in the word
            for term in search_terms:
                if term in word_text or word_text in term:
                    confidence = len(term) / max(len(word_text), len(term))
                    if confidence >= self.config.semantic_match_threshold:
                        return {
                            "page_number": word["page"],
                            "bounding_box": word["box"],
                            "confidence": confidence,
                            "matched_text": word["text"]
                        }

        return None

    def _combine_bounding_boxes(self, boxes: List[Tuple[float, float, float, float]]) -> Tuple[float, float, float, float]:
        """Combine multiple bounding boxes into one"""
        if not boxes:
            return (0, 0, 0, 0)
        
        min_left = min(box[0] for box in boxes)
        min_top = min(box[1] for box in boxes)
        max_right = max(box[0] + box[2] for box in boxes)
        max_bottom = max(box[1] + box[3] for box in boxes)
        
        return (min_left, min_top, max_right - min_left, max_bottom - min_top)

    def _enrich_data_comprehensive(self, data: Any, doc_payload: AithonDocument, path: str = "") -> EnrichmentResult:
        """Comprehensive data enrichment with detailed tracking"""
        start_time = time.time()
        
        enriched_fields = 0
        total_fields = 0
        bounding_boxes_added = 0
        confidence_scores_added = 0
        errors = []

        def enrich_recursive(obj, current_path=""):
            nonlocal enriched_fields, total_fields, bounding_boxes_added, confidence_scores_added, errors
            
            if isinstance(obj, dict):
                # Check if this is a data field with a Value
                if 'Value' in obj:
                    total_fields += 1
                    value = obj['Value']
                    
                    if isinstance(value, str) and value.strip():
                        try:
                            # Skip if bounding box already exists (don't overwrite BoundingBoxBox results)
                            if not ('BoundingBox' in obj and obj['BoundingBox']):
                                # Try to find bounding box
                                bbox_info = self._find_bounding_box_advanced(value, doc_payload)
                                if bbox_info:
                                    obj['BoundingBox'] = bbox_info
                                    bounding_boxes_added += 1
                                    enriched_fields += 1
                                
                                # Add confidence score if not present
                                if 'ConfidenceScore' not in obj:
                                    obj['ConfidenceScore'] = self._calculate_field_confidence(value, bbox_info)
                                    confidence_scores_added += 1
                        except Exception as e:
                            errors.append(f"Error enriching field {current_path}: {e}")
                
                # Recursively process nested objects
                for key, value in obj.items():
                    enrich_recursive(value, f"{current_path}.{key}" if current_path else key)
            
            elif isinstance(obj, list):
                for i, item in enumerate(obj):
                    enrich_recursive(item, f"{current_path}[{i}]")

        # Perform enrichment
        enrich_recursive(data)
        
        # Calculate results
        enrichment_rate = enriched_fields / total_fields if total_fields > 0 else 0.0
        processing_time = time.time() - start_time
        
        # Determine quality level
        if enrichment_rate >= 0.9:
            quality_level = EnrichmentQuality.EXCELLENT
        elif enrichment_rate >= 0.75:
            quality_level = EnrichmentQuality.GOOD
        elif enrichment_rate >= 0.5:
            quality_level = EnrichmentQuality.FAIR
        elif enrichment_rate > 0:
            quality_level = EnrichmentQuality.POOR
        else:
            quality_level = EnrichmentQuality.NONE

        return EnrichmentResult(
            enriched_fields=enriched_fields,
            total_fields=total_fields,
            enrichment_rate=enrichment_rate,
            bounding_boxes_added=bounding_boxes_added,
            confidence_scores_added=confidence_scores_added,
            quality_level=quality_level,
            processing_time=processing_time,
            errors=errors
        )

    def _calculate_field_confidence(self, value: str, bbox_info: Dict[str, Any]) -> str:
        """Calculate confidence score for a field based on matching quality - STANDARDIZED THRESHOLDS"""
        confidence_score = bbox_info.get("confidence", 0.0)
        matching_strategy = bbox_info.get("matching_strategy", "unknown")
        
        # Adjust confidence based on matching strategy
        if matching_strategy == "exact_match":
            confidence_score = min(1.0, confidence_score + 0.1)
        elif matching_strategy == "sequence_match":
            confidence_score = min(1.0, confidence_score + 0.05)
        
        # Convert to categorical confidence - STANDARDIZED THRESHOLDS
        if confidence_score >= 0.9:  # HIGH: Above 90%
            return "HIGH"
        elif confidence_score >= 0.8:  # MEDIUM: 80-90%
            return "MEDIUM"
        else:  # LOW: Below 80%
            return "LOW"

    def __call__(self, doc_payload: AithonDocument) -> AithonDocument:
        """
        Enhanced validation and enrichment with comprehensive error handling and monitoring.
        Maintains the same input/output format while adding advanced features.
        """
        start_time = time.time()
        logging.info(f"Entering Enhanced Validation & Enrichment Box for: {doc_payload.original_filename}")
        doc_payload.pipeline_status = "Validation_Enrichment"

        # Validate input
        if not doc_payload.extracted_data:
            logging.warning("No extracted data to validate or enrich.")
            doc_payload.pipeline_status = "Validation_Enrichment_Skipped"
            return doc_payload

        try:
            # Load schema for validation
            schema = self._load_schema(doc_payload.document_type)
            
            # Perform comprehensive validation
            validation_result = self._validate_schema_comprehensive(doc_payload.extracted_data, schema)
            
            # Store validation results
            doc_payload.validation_errors = []
            for error in validation_result.errors:
                doc_payload.validation_errors.append(error["message"])
            
            # Perform enrichment if enabled
            enrichment_result = None
            if self.config.enable_bounding_box_enrichment and doc_payload.pages:
                logging.info("Performing comprehensive data enrichment...")
                enrichment_result = self._enrich_data_comprehensive(doc_payload.extracted_data, doc_payload)
                logging.info(f"Enrichment completed: {enrichment_result.enriched_fields}/{enrichment_result.total_fields} fields enriched")
            
            # Store comprehensive metadata
            doc_payload.metadata.update({
                "validation_passed": validation_result.is_valid,
                "validation_errors_count": len(validation_result.errors),
                "validation_warnings_count": len(validation_result.warnings),
                "validation_severity_counts": validation_result.severity_counts,
                "completeness_score": validation_result.completeness_score,
                "accuracy_score": validation_result.accuracy_score,
                "consistency_score": validation_result.consistency_score,
                "overall_quality_score": validation_result.overall_quality_score,
                "validation_processing_time": validation_result.processing_time
            })
            
            if enrichment_result:
                doc_payload.metadata.update({
                    "enrichment_enabled": True,
                    "enriched_fields": enrichment_result.enriched_fields,
                    "total_fields": enrichment_result.total_fields,
                    "enrichment_rate": enrichment_result.enrichment_rate,
                    "bounding_boxes_added": enrichment_result.bounding_boxes_added,
                    "confidence_scores_added": enrichment_result.confidence_scores_added,
                    "enrichment_quality_level": enrichment_result.quality_level.value,
                    "enrichment_processing_time": enrichment_result.processing_time,
                    "enrichment_errors": enrichment_result.errors
                })
            else:
                doc_payload.metadata.update({
                    "enrichment_enabled": False,
                    "enrichment_skipped_reason": "No page data available or enrichment disabled"
                })

            # Determine final status
            if validation_result.is_valid:
                if enrichment_result and enrichment_result.quality_level in [EnrichmentQuality.EXCELLENT, EnrichmentQuality.GOOD]:
                    doc_payload.pipeline_status = "Validation_Enrichment_Completed"
                elif enrichment_result and enrichment_result.quality_level == EnrichmentQuality.FAIR:
                    doc_payload.pipeline_status = "Validation_Enrichment_Completed_Fair"
                elif enrichment_result:
                    doc_payload.pipeline_status = "Validation_Enrichment_Completed_Poor"
                else:
                    doc_payload.pipeline_status = "Validation_Completed_No_Enrichment"
            else:
                if self.config.continue_on_validation_error:
                    doc_payload.pipeline_status = "Validation_Enrichment_Completed_With_Errors"
                else:
                    doc_payload.pipeline_status = "Validation_Failed"

            # Store total processing time
            doc_payload.metadata["total_validation_enrichment_time"] = time.time() - start_time

            logging.info(f"Validation & Enrichment completed for '{doc_payload.original_filename}': "
                        f"Valid: {validation_result.is_valid}, "
                        f"Quality: {validation_result.overall_quality_score:.2f}, "
                        f"Enrichment: {enrichment_result.enrichment_rate:.2f} rate" if enrichment_result else "No enrichment")

        except Exception as e:
            logging.error(f"Validation & Enrichment failed for {doc_payload.original_filename}: {e}", exc_info=True)
            doc_payload.error_message = f"Validation & Enrichment failed: {e}"
            doc_payload.pipeline_status = "Failed_Validation_Enrichment"
            
            # Store error metadata
            doc_payload.metadata.update({
                "error_details": str(e),
                "total_validation_enrichment_time": time.time() - start_time
            })

        return doc_payload 
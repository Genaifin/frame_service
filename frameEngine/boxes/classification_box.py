import asyncio
import logging
import os
import time
import hashlib
import json
from enum import Enum
from typing import Optional, List, Tuple, Dict, Any
from dataclasses import dataclass, field
from pathlib import Path

import openai
from openai import OpenAI
import google.generativeai as genai
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

# Document types with enhanced descriptions
DOCUMENT_TYPES = [
    {
        "document_type": "Statement", 
        "description": "Financial statements, account summaries, portfolio statements, and periodic reports showing account balances, holdings, and performance metrics."
    },
    {
        "document_type": "CapCall", 
        "description": "Capital call notices requesting investors to fund their committed capital for investments, including due dates, amounts, and investment details."
    },
    {
        "document_type": "Distribution", 
        "description": "Distribution notices informing investors of capital distributions, returns, dividends, or proceeds from investments and exits."
    },
    {
        "document_type": "AGM", 
        "description": "Annual General Meeting documents including meeting notices, agendas, resolutions, voting materials, and shareholder communications."
    },
    {
        "document_type": "Unknown", 
        "description": "Documents that don't clearly fit into the above categories or require manual review for proper classification."
    }
]

class LLMProvider(Enum):
    """Available LLM providers"""
    OPENAI = "openai"
    GEMINI = "gemini"

class ClassificationMode(Enum):
    """Classification processing modes"""
    TEXTUAL = "textual_classification"
    VISION = "vision_based_classification"
    HYBRID = "hybrid_classification"
    AUTO = "auto_select"

class ConfidenceLevel(Enum):
    """Classification confidence levels"""
    HIGH = "HIGH"
    MEDIUM = "MEDIUM"
    LOW = "LOW"
    UNKNOWN = "UNKNOWN"

class ProcessingMetrics:
    """Metrics collection for performance monitoring"""
    def __init__(self):
        self.start_time = time.time()
        self.api_calls = 0
        self.retry_count = 0
        self.provider_attempts = {}
        self.error_count = 0
        self.cache_hits = 0
        
    def record_api_call(self, provider: str):
        self.api_calls += 1
        self.provider_attempts[provider] = self.provider_attempts.get(provider, 0) + 1
        
    def record_retry(self):
        self.retry_count += 1
        
    def record_error(self):
        self.error_count += 1
        
    def record_cache_hit(self):
        self.cache_hits += 1
        
    def get_processing_time(self) -> float:
        return time.time() - self.start_time
        
    def get_summary(self) -> Dict[str, Any]:
        return {
            "processing_time": self.get_processing_time(),
            "api_calls": self.api_calls,
            "retry_count": self.retry_count,
            "provider_attempts": self.provider_attempts,
            "error_count": self.error_count,
            "cache_hits": self.cache_hits,
            "efficiency_score": self.cache_hits / max(self.api_calls, 1)
        }

@dataclass
class ClassificationConfig:
    """Enhanced configuration for classification behavior"""
    
    # Text processing thresholds
    min_text_length_for_textual: int = 100
    max_text_length_for_processing: int = 50000
    
    # Vision processing settings
    max_pages_for_vision: int = 50
    image_quality_threshold: float = 0.7
    
    # LLM provider preferences (in order of preference)
    preferred_llm_providers: List[LLMProvider] = field(default_factory=lambda: [
        LLMProvider.OPENAI,
        LLMProvider.GEMINI
    ])
    
    # Enhanced retry configuration
    max_retries: int = 3
    retry_delay_base: float = 1.0
    retry_delay_max: float = 60.0
    exponential_backoff: bool = True
    jitter: bool = True
    
    # Confidence thresholds - STANDARDIZED LEVELS
    min_confidence_for_auto_accept: float = 0.9  # HIGH: Above 90%
    min_confidence_for_manual_review: float = 0.8  # MEDIUM: 80-90%
    
    # Processing timeouts
    classification_timeout: int = 300  # 5 minutes
    file_processing_timeout: int = 120  # 2 minutes
    
    # Performance optimization
    enable_caching: bool = True
    cache_ttl: int = 3600  # 1 hour
    enable_parallel_processing: bool = True
    
    # Fallback behavior
    fallback_to_vision_on_text_failure: bool = True
    fallback_document_type: str = "Unknown"
    
    # Quality assurance
    enable_quality_checks: bool = True
    min_quality_score: float = 0.5
    
    # Monitoring and logging
    enable_detailed_logging: bool = True
    log_api_responses: bool = False
    enable_metrics_collection: bool = True

class ClassificationBox:
    """
    Enhanced Classification Box with advanced patterns from bot_service:
    - Sophisticated retry logic with exponential backoff
    - Comprehensive error handling and recovery
    - Performance monitoring and metrics collection
    - Advanced caching mechanisms
    - Multi-provider LLM support with intelligent fallback
    - Quality assurance and validation
    - Detailed logging and tracing
    """
    
    def __init__(self, config: Optional[ClassificationConfig] = None):
        self.config = config or ClassificationConfig()
        self.document_types = [dt["document_type"] for dt in DOCUMENT_TYPES]
        self.classification_cache = {}
        self.metrics = ProcessingMetrics()
        
        # Output directory for raw OpenAI responses
        self.output_dir = Path(os.getenv("OUTPUT_DIR", "./output_documents"))
        self.output_dir.mkdir(exist_ok=True)
        
        # Initialize LLM clients
        self.client = OpenAI()
        self.model = "gpt-4o-mini"
        
        # Initialize Gemini client if available
        self.gemini_client = None
        try:
            import google.generativeai as genai
            # Configure Gemini if API key is available
            genai.configure(api_key=os.getenv("GEMINI_API_KEY"))
            self.gemini_client = genai.GenerativeModel('gemini-pro')
        except Exception as e:
            logging.warning(f"Gemini client initialization failed: {e}")
    
    def _save_raw_openai_response(self, raw_response: str, filename: str, response_type: str = "classification") -> None:
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
                # If parsing fails, keep as string (for classification, it's usually just text)
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
    
    def _compute_content_hash(self, content: str) -> str:
        """Compute hash for caching purposes"""
        return hashlib.md5(content.encode()).hexdigest()
    
    def _get_from_cache(self, content_hash: str) -> Optional[Tuple[str, float]]:
        """Get classification from cache if available"""
        if not self.config.enable_caching:
            return None
            
        cache_entry = self.classification_cache.get(content_hash)
        if cache_entry:
            timestamp, result = cache_entry
            if time.time() - timestamp < self.config.cache_ttl:
                self.metrics.record_cache_hit()
                return result
            else:
                # Remove expired entry
                del self.classification_cache[content_hash]
        return None
    
    def _store_in_cache(self, content_hash: str, result: Tuple[str, float]):
        """Store classification result in cache"""
        if self.config.enable_caching:
            self.classification_cache[content_hash] = (time.time(), result)
    
    def _assess_text_quality(self, text: str) -> float:
        """Enhanced text quality assessment"""
        if not text or len(text.strip()) < 10:
            return 0.0
        
        # Basic quality metrics
        word_count = len(text.split())
        char_count = len(text)
        line_count = len(text.splitlines())
        
        # Quality factors
        length_score = min(word_count / 100, 1.0)  # Normalize to 0-1
        density_score = min(word_count / max(line_count, 1) / 10, 1.0)
        
        # Check for common OCR artifacts
        ocr_artifacts = ['', '|||', '___', '###']
        artifact_penalty = sum(text.count(artifact) for artifact in ocr_artifacts) * 0.1
        
        # Calculate overall quality
        quality_score = (length_score + density_score) / 2 - artifact_penalty
        return max(0.0, min(1.0, quality_score))
    
    def _determine_classification_mode(self, text: str, num_pages: int) -> ClassificationMode:
        """Enhanced classification mode determination"""
        text_length = len(text.strip())
        text_quality = self._assess_text_quality(text)
        
        # Decision logic based on multiple factors
        if text_length < self.config.min_text_length_for_textual:
            return ClassificationMode.VISION if num_pages <= self.config.max_pages_for_vision else ClassificationMode.HYBRID
        elif text_quality < self.config.min_quality_score:
            return ClassificationMode.HYBRID
        elif text_length > self.config.max_text_length_for_processing:
            return ClassificationMode.TEXTUAL  # Truncate for efficiency
        else:
            return ClassificationMode.TEXTUAL
    
    def _calculate_confidence_level(self, confidence_score: float) -> ConfidenceLevel:
        """Calculate confidence level based on standardized thresholds"""
        if confidence_score >= self.config.min_confidence_for_auto_accept:
            return ConfidenceLevel.HIGH
        elif confidence_score >= self.config.min_confidence_for_manual_review:
            return ConfidenceLevel.MEDIUM
        elif confidence_score > 0.0:
            return ConfidenceLevel.LOW
        else:
            return ConfidenceLevel.UNKNOWN
    
    def _build_enhanced_prompt(self, text_content: str, filename: str) -> str:
        """Build enhanced prompt with document type descriptions"""
        
        # Truncate text to save tokens while preserving important information
        # Take first 2000 and last 1000 characters for better context
        if len(text_content) > 3000:
            truncated_text = text_content[:2000] + "\n...\n" + text_content[-1000:]
        else:
            truncated_text = text_content

        # Build document types description
        document_types_str = "\n".join([
            f"- {dt['document_type']}: {dt['description']}"
            for dt in DOCUMENT_TYPES
        ])

        prompt = f"""
        You are an expert document classifier for financial documents.
        Your task is to classify the following document into one of these types:

        {document_types_str}

        Classification Guidelines:
        - Analyze the document content, structure, and terminology
        - Look for key phrases and document patterns
        - Consider the document's purpose and intended audience
        - If uncertain, respond with "Unknown"

        Document Filename: {filename}
        Document Content:
        ---
        {truncated_text}
        ---

        Respond with ONLY the document type name (e.g., "Statement", "CapCall", "Distribution", "Unknown").
        """
        return prompt

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=10),
        retry=retry_if_exception_type((openai.RateLimitError, openai.APITimeoutError, openai.APIConnectionError)),
        before_sleep=before_sleep_log(logging.getLogger(), logging.WARNING),
        after=after_log(logging.getLogger(), logging.INFO)
    )
    async def _classify_with_openai(self, text_content: str, filename: str) -> Tuple[str, float]:
        """Enhanced OpenAI classification with retry logic"""
        try:
            self.metrics.record_api_call("openai")
            prompt = self._build_enhanced_prompt(text_content, filename)
            
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": "You are a precise document classification expert. Respond only with the document type name."},
                    {"role": "user", "content": prompt}
                ],
                max_tokens=20,
                temperature=0.0,
                timeout=30.0
            )
            
            classification = response.choices[0].message.content.strip()
            
            # Save raw OpenAI response before processing
            # COMMENTED OUT: Disabled classification raw JSON file generation - only output file needed
            # self._save_raw_openai_response(classification, filename, "classification")
            
            # Enhanced validation and confidence scoring
            if classification in self.document_types:
                # Higher confidence for valid classifications
                confidence = 0.85 if classification != "Unknown" else 0.3
                
                # Adjust confidence based on response quality
                if response.usage and response.usage.completion_tokens > 1:
                    confidence *= 0.95  # Slight penalty for verbose responses
                    
            else:
                # Fallback to Unknown for invalid responses
                classification = "Unknown"
                confidence = 0.2
                logging.warning(f"Invalid classification response: {classification}")
            
            return classification, confidence
            
        except Exception as e:
            self.metrics.record_error()
            logging.error(f"OpenAI classification failed: {e}")
            raise

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=10),
        before_sleep=before_sleep_log(logging.getLogger(), logging.WARNING),
        after=after_log(logging.getLogger(), logging.INFO)
    )
    async def _classify_with_gemini(self, text_content: str, filename: str) -> Tuple[str, float]:
        """Enhanced Gemini classification with retry logic"""
        try:
            if not self.gemini_client:
                raise Exception("Gemini client not initialized")
            
            self.metrics.record_api_call("gemini")
            prompt = self._build_enhanced_prompt(text_content, filename)
            
            response = self.gemini_client.generate_content(prompt)
            classification = response.text.strip()
            
            # Enhanced validation
            if classification in self.document_types:
                confidence = 0.8 if classification != "Unknown" else 0.3
            else:
                classification = "Unknown"
                confidence = 0.2
                logging.warning(f"Invalid Gemini classification response: {classification}")
            
            return classification, confidence
            
        except Exception as e:
            self.metrics.record_error()
            logging.error(f"Gemini classification failed: {e}")
            raise

    async def _classify_with_retry(self, text_content: str, filename: str, mode: ClassificationMode) -> Tuple[str, float, str, int]:
        """Enhanced classification with intelligent retry and fallback logic"""
        
        # Check cache first
        content_hash = self._compute_content_hash(text_content + filename)
        cached_result = self._get_from_cache(content_hash)
        if cached_result:
            return cached_result[0], cached_result[1], "cache", 0
        
        last_exception = None
        retry_count = 0
        
        for attempt in range(self.config.max_retries + 1):
            try:
                # Try preferred providers in order
                for provider in self.config.preferred_llm_providers:
                    try:
                        if provider == LLMProvider.OPENAI:
                            classification, confidence = await self._classify_with_openai(text_content, filename)
                            result = (classification, confidence)
                            self._store_in_cache(content_hash, result)
                            return classification, confidence, "openai", retry_count
                        elif provider == LLMProvider.GEMINI and self.gemini_client:
                            classification, confidence = await self._classify_with_gemini(text_content, filename)
                            result = (classification, confidence)
                            self._store_in_cache(content_hash, result)
                            return classification, confidence, "gemini", retry_count
                    except Exception as e:
                        logging.warning(f"Provider {provider.value} failed on attempt {attempt + 1}: {e}")
                        continue
                
                # If all providers failed, raise the last exception
                raise Exception("All LLM providers failed")
                
            except Exception as e:
                last_exception = e
                retry_count = attempt
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
                    
                    logging.warning(f"Retrying classification after {delay:.2f}s due to: {e}")
                    await asyncio.sleep(delay)
                    continue
                else:
                    break
        
        # Final fallback
        logging.error(f"All classification attempts failed, using fallback: {last_exception}")
        return self.config.fallback_document_type, 0.0, "fallback", retry_count

    def __call__(self, doc_payload: AithonDocument) -> AithonDocument:
        """
        Enhanced document classification with comprehensive error handling and monitoring.
        Maintains the same input/output format while adding advanced features.
        """
        start_time = time.time()
        self.metrics = ProcessingMetrics()  # Reset metrics for this document
        
        logging.info(f"Entering Enhanced Classification Box for: {doc_payload.original_filename}")
        doc_payload.pipeline_status = "Classification"

        # Validate input
        if not doc_payload.cleaned_text:
            doc_payload.error_message = "No cleaned text available for classification."
            doc_payload.pipeline_status = "Failed_Classification"
            logging.error(doc_payload.error_message)
            return doc_payload

        try:
            # Assess text quality and determine processing mode
            text_quality = self._assess_text_quality(doc_payload.cleaned_text)
            num_pages = len(doc_payload.pages) if doc_payload.pages else 1
            classification_mode = self._determine_classification_mode(doc_payload.cleaned_text, num_pages)
            
            # Store quality metrics in metadata
            doc_payload.metadata.update({
                "text_quality_score": text_quality,
                "classification_mode": classification_mode.value,
                "text_length": len(doc_payload.cleaned_text),
                "num_pages": num_pages
            })

            # Perform classification with retry logic
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            
            try:
                classification, confidence_score, llm_provider, retry_count = loop.run_until_complete(
                    self._classify_with_retry(doc_payload.cleaned_text, doc_payload.original_filename, classification_mode)
                )
            finally:
                loop.close()

            # Calculate confidence level
            confidence_level = self._calculate_confidence_level(confidence_score)
            
            # Update document payload
            doc_payload.document_type = classification
            doc_payload.classification_confidence = confidence_score
            doc_payload.classification_modality = classification_mode.value
            
            # Collect comprehensive metrics
            processing_metrics = self.metrics.get_summary()
            
            # Store additional metadata
            doc_payload.metadata.update({
                "confidence_level": confidence_level.value,
                "llm_provider": llm_provider,
                "retry_count": retry_count,
                "processing_time": time.time() - start_time,
                "file_hash": self._compute_content_hash(str(doc_payload.source_path)),
                "classification_metrics": processing_metrics
            })

            # Set pipeline status based on confidence
            if confidence_level == ConfidenceLevel.HIGH:
                doc_payload.pipeline_status = "Classification_Completed"
            elif confidence_level in [ConfidenceLevel.MEDIUM, ConfidenceLevel.LOW]:
                doc_payload.pipeline_status = "Classification_Completed_Low_Confidence"
            else:
                doc_payload.pipeline_status = "Classification_Uncertain"

            # Enhanced logging with metrics
            logging.info(f"Classified '{doc_payload.original_filename}' as: {classification} "
                        f"(confidence: {confidence_score:.2f}, level: {confidence_level.value}, "
                        f"provider: {llm_provider}, retries: {retry_count}, "
                        f"processing_time: {processing_metrics['processing_time']:.2f}s)")

        except Exception as e:
            logging.error(f"Classification failed for {doc_payload.original_filename}: {e}", exc_info=True)
            doc_payload.error_message = f"Classification failed: {e}"
            doc_payload.pipeline_status = "Failed_Classification"
            doc_payload.document_type = "Unknown"
            doc_payload.classification_confidence = 0.0
            
            # Store error metadata
            doc_payload.metadata.update({
                "error_details": str(e),
                "processing_time": time.time() - start_time,
                "classification_metrics": self.metrics.get_summary()
            })

        return doc_payload 
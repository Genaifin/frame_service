"""
Advanced Exception Handling System for Aithon Framework

This module provides comprehensive exception handling with:
- Hierarchical exception structure
- Error categorization and severity levels
- Recovery strategy registration
- Context management for debugging
- Detailed error reporting and logging
"""

import logging
import time
from typing import Any, Dict, Optional, Callable, List
from enum import Enum
from dataclasses import dataclass, field
from contextlib import contextmanager

# Configure logger
logger = logging.getLogger(__name__)

class ErrorCategory(Enum):
    """Categories of errors in the Aithon pipeline"""
    INGESTION = "ingestion"
    OCR = "ocr"
    PREPROCESSING = "preprocessing"
    CLASSIFICATION = "classification"
    EXTRACTION = "extraction"
    VALIDATION = "validation"
    OUTPUT = "output"
    SYSTEM = "system"
    NETWORK = "network"
    AUTHENTICATION = "authentication"
    CONFIGURATION = "configuration"

class ErrorSeverity(Enum):
    """Severity levels for errors"""
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"

@dataclass
class ErrorContext:
    """Context information for error handling"""
    operation: str
    exception_handler: 'ExceptionHandler'
    context_data: Dict[str, Any] = field(default_factory=dict)
    start_time: float = field(default_factory=time.time)
    
    def add_context(self, key: str, value: Any):
        """Add context information"""
        self.context_data[key] = value
    
    def get_context(self, key: str, default: Any = None) -> Any:
        """Get context information"""
        return self.context_data.get(key, default)
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type and issubclass(exc_type, BaseAithonException):
            # Handle the exception using the registered handler
            self.exception_handler.handle_exception(exc_val, self)
        return False  # Don't suppress the exception

class BaseAithonException(Exception):
    """Base exception class for all Aithon-related errors"""
    
    def __init__(
        self,
        message: str,
        category: ErrorCategory = ErrorCategory.SYSTEM,
        severity: ErrorSeverity = ErrorSeverity.MEDIUM,
        context: Optional[Dict[str, Any]] = None,
        **kwargs
    ):
        super().__init__(message)
        self.message = message
        self.category = category
        self.severity = severity
        self.context = context or {}
        self.timestamp = time.time()
        
        # Add any additional context from kwargs
        for key, value in kwargs.items():
            self.context[key] = value
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert exception to dictionary for logging/serialization"""
        return {
            "exception_type": self.__class__.__name__,
            "message": self.message,
            "category": self.category.value,
            "severity": self.severity.value,
            "context": self.context,
            "timestamp": self.timestamp
        }

# Document Processing Exceptions
class DocumentProcessingError(BaseAithonException):
    """Base class for document processing errors"""
    def __init__(self, message: str, filename: str = None, **kwargs):
        super().__init__(
            message,
            category=ErrorCategory.SYSTEM,
            severity=ErrorSeverity.HIGH,
            filename=filename,
            **kwargs
        )

class IngestionError(BaseAithonException):
    """Errors during document ingestion"""
    def __init__(self, message: str, **kwargs):
        super().__init__(
            message,
            category=ErrorCategory.INGESTION,
            severity=ErrorSeverity.HIGH,
            **kwargs
        )

class OCRError(BaseAithonException):
    """Errors during OCR processing"""
    def __init__(self, message: str, **kwargs):
        super().__init__(
            message,
            category=ErrorCategory.OCR,
            severity=ErrorSeverity.MEDIUM,
            **kwargs
        )

class PreprocessingError(BaseAithonException):
    """Errors during preprocessing"""
    def __init__(self, message: str, **kwargs):
        super().__init__(
            message,
            category=ErrorCategory.PREPROCESSING,
            severity=ErrorSeverity.MEDIUM,
            **kwargs
        )

class ClassificationError(BaseAithonException):
    """Errors during document classification"""
    def __init__(self, message: str, **kwargs):
        super().__init__(
            message,
            category=ErrorCategory.CLASSIFICATION,
            severity=ErrorSeverity.HIGH,
            **kwargs
        )

class ExtractionError(BaseAithonException):
    """Errors during data extraction"""
    def __init__(self, message: str, **kwargs):
        super().__init__(
            message,
            category=ErrorCategory.EXTRACTION,
            severity=ErrorSeverity.HIGH,
            **kwargs
        )

class ValidationError(BaseAithonException):
    """Errors during validation"""
    def __init__(self, message: str, **kwargs):
        super().__init__(
            message,
            category=ErrorCategory.VALIDATION,
            severity=ErrorSeverity.MEDIUM,
            **kwargs
        )

class OutputError(BaseAithonException):
    """Errors during output generation"""
    def __init__(self, message: str, **kwargs):
        super().__init__(
            message,
            category=ErrorCategory.OUTPUT,
            severity=ErrorSeverity.MEDIUM,
            **kwargs
        )

# System and Infrastructure Exceptions
class ConfigurationError(BaseAithonException):
    """Configuration-related errors"""
    def __init__(self, message: str, **kwargs):
        super().__init__(
            message,
            category=ErrorCategory.CONFIGURATION,
            severity=ErrorSeverity.HIGH,
            **kwargs
        )

class NetworkError(BaseAithonException):
    """Network-related errors"""
    def __init__(self, message: str, **kwargs):
        super().__init__(
            message,
            category=ErrorCategory.NETWORK,
            severity=ErrorSeverity.MEDIUM,
            **kwargs
        )

class AuthenticationError(BaseAithonException):
    """Authentication-related errors"""
    def __init__(self, message: str, **kwargs):
        super().__init__(
            message,
            category=ErrorCategory.AUTHENTICATION,
            severity=ErrorSeverity.HIGH,
            **kwargs
        )

# LLM and API Exceptions
class LLMError(BaseAithonException):
    """LLM-related errors"""
    def __init__(self, message: str, provider: str = None, **kwargs):
        super().__init__(
            message,
            category=ErrorCategory.SYSTEM,
            severity=ErrorSeverity.HIGH,
            provider=provider,
            **kwargs
        )

class APIError(BaseAithonException):
    """API-related errors"""
    def __init__(self, message: str, api_name: str = None, status_code: int = None, **kwargs):
        super().__init__(
            message,
            category=ErrorCategory.NETWORK,
            severity=ErrorSeverity.MEDIUM,
            api_name=api_name,
            status_code=status_code,
            **kwargs
        )

# Recovery Strategy Type
RecoveryStrategy = Callable[[BaseAithonException, ErrorContext], bool]

class ExceptionHandler:
    """
    Advanced exception handler with recovery strategies and detailed logging
    """
    
    def __init__(self):
        self.recovery_strategies: Dict[type, RecoveryStrategy] = {}
        self.error_counts: Dict[str, int] = {}
        self.last_errors: List[BaseAithonException] = []
        self.max_error_history = 100
    
    def register_recovery_strategy(self, exception_type: type, strategy: RecoveryStrategy):
        """Register a recovery strategy for a specific exception type"""
        self.recovery_strategies[exception_type] = strategy
        logger.info(f"Registered recovery strategy for {exception_type.__name__}")
    
    def handle_exception(self, exception: BaseAithonException, context: ErrorContext) -> bool:
        """
        Handle an exception with potential recovery
        
        Args:
            exception: The exception to handle
            context: Error context information
            
        Returns:
            bool: True if recovery was successful, False otherwise
        """
        # Log the exception
        self._log_exception(exception, context)
        
        # Track error statistics
        self._track_error(exception)
        
        # Store in error history
        self._store_error(exception)
        
        # Attempt recovery
        return self._attempt_recovery(exception, context)
    
    def _log_exception(self, exception: BaseAithonException, context: ErrorContext):
        """Log exception details"""
        log_data = {
            "exception": exception.to_dict(),
            "operation": context.operation,
            "context": context.context_data,
            "duration": time.time() - context.start_time
        }
        
        if exception.severity == ErrorSeverity.CRITICAL:
            logger.critical(f"Critical error in {context.operation}: {exception.message}", extra=log_data)
        elif exception.severity == ErrorSeverity.HIGH:
            logger.error(f"High severity error in {context.operation}: {exception.message}", extra=log_data)
        elif exception.severity == ErrorSeverity.MEDIUM:
            logger.warning(f"Medium severity error in {context.operation}: {exception.message}", extra=log_data)
        else:
            logger.info(f"Low severity error in {context.operation}: {exception.message}", extra=log_data)
    
    def _track_error(self, exception: BaseAithonException):
        """Track error statistics"""
        error_key = f"{exception.category.value}_{exception.__class__.__name__}"
        self.error_counts[error_key] = self.error_counts.get(error_key, 0) + 1
    
    def _store_error(self, exception: BaseAithonException):
        """Store error in history"""
        self.last_errors.append(exception)
        if len(self.last_errors) > self.max_error_history:
            self.last_errors.pop(0)
    
    def _attempt_recovery(self, exception: BaseAithonException, context: ErrorContext) -> bool:
        """Attempt to recover from the exception"""
        exception_type = type(exception)
        
        # Check for exact type match first
        if exception_type in self.recovery_strategies:
            strategy = self.recovery_strategies[exception_type]
            try:
                return strategy(exception, context)
            except Exception as e:
                logger.error(f"Recovery strategy failed for {exception_type.__name__}: {e}")
                return False
        
        # Check for base class matches
        for registered_type, strategy in self.recovery_strategies.items():
            if issubclass(exception_type, registered_type):
                try:
                    return strategy(exception, context)
                except Exception as e:
                    logger.error(f"Recovery strategy failed for {registered_type.__name__}: {e}")
                    return False
        
        logger.warning(f"No recovery strategy found for {exception_type.__name__}")
        return False
    
    def get_error_statistics(self) -> Dict[str, Any]:
        """Get error statistics"""
        return {
            "error_counts": self.error_counts.copy(),
            "total_errors": sum(self.error_counts.values()),
            "recent_errors": len(self.last_errors),
            "categories": {
                category.value: sum(
                    count for key, count in self.error_counts.items() 
                    if key.startswith(category.value)
                )
                for category in ErrorCategory
            }
        }
    
    def get_recent_errors(self, limit: int = 10) -> List[Dict[str, Any]]:
        """Get recent errors"""
        return [error.to_dict() for error in self.last_errors[-limit:]]
    
    def clear_error_history(self):
        """Clear error history"""
        self.last_errors.clear()
        logger.info("Error history cleared") 
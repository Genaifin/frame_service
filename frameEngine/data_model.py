from pydantic import BaseModel, Field
from typing import Optional, List, Dict, Any
from uuid import UUID, uuid4
from pathlib import Path
from datetime import datetime

class Page(BaseModel):
    """Represents a single page in the document."""
    page_number: int
    text: str
    raw_text: Optional[str] = None # The original text from OCR before cleaning
    words: List[Dict[str, Any]] = Field(default_factory=list) # Word-level data with bounding boxes

class ProcessingEvent(BaseModel):
    """Represents a single processing event for a document."""
    timestamp: str
    level: str  # INFO, WARNING, ERROR
    stage: str  # e.g., "Ingestion", "OCR", "Classification", etc.
    message: str
    details: Optional[Dict[str, Any]] = None

class AithonDocument(BaseModel):
    """
    The central data payload that flows through the Aithon pipeline.
    It holds all information about a single document being processed.
    """
    # --- Core Identifiers ---
    doc_id: UUID = Field(default_factory=uuid4)
    task_id: UUID = Field(default_factory=uuid4)
    ext_doc_id: Optional[str] = None

    # --- File Information ---
    source_path: Path
    original_filename: str
    
    @property
    def file_path(self) -> Path:
        """Get the file path for processing operations."""
        return self.source_path
    
    # --- Processing State ---
    pipeline_status: str = "Initialized"
    error_message: Optional[str] = None
    
    # --- Content ---
    is_scanned: bool = False
    raw_text: Optional[str] = None
    cleaned_text: Optional[str] = None
    pages: List[Page] = Field(default_factory=list)
    
    # --- AI Results ---
    classification_modality: Optional[str] = None # e.g., "TEXTUAL" or "VISION"
    document_type: Optional[str] = None
    classification_confidence: Optional[float] = None
    
    extracted_data: Optional[Dict[str, Any]] = None
    validation_errors: Optional[List[str]] = None
    
    # --- Events Log ---
    events_log: List[ProcessingEvent] = Field(default_factory=list)
    
    # --- Metadata ---
    metadata: Dict[str, Any] = Field(default_factory=dict)

    def add_event(self, level: str, stage: str, message: str, details: Optional[Dict[str, Any]] = None):
        """Add a processing event to the events log."""
        event = ProcessingEvent(
            timestamp=datetime.now().strftime("%Y-%m-%d %H:%M:%S,%f")[:-3],
            level=level,
            stage=stage,
            message=message,
            details=details
        )
        self.events_log.append(event)

    class Config:
        arbitrary_types_allowed = True 
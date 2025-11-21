# Aithon Frame Engine Boxes Package
"""
Document Processing Boxes for Enhanced Aithon Framework
"""

__version__ = "1.0.0"

# Import all boxes for easier access
from .ingestion_box import IngestionBox
from .ocr_box import OCRBox
from .preprocessing_box import PreprocessingBox
from .classification_box import ClassificationBox
from .extraction_box import ExtractionBox
from .bounding_box_box import BoundingBoxBox
from .validation_enrichment_box import ValidationEnrichmentBox
from .output_box import OutputBox

__all__ = [
    "IngestionBox",
    "OCRBox", 
    "PreprocessingBox",
    "ClassificationBox",
    "ExtractionBox",
    "BoundingBoxBox",
    "ValidationEnrichmentBox",
    "OutputBox"
]

# import logging
# import os
# import time
# import json
# from pathlib import Path
# from typing import Dict, Any, List, Optional, Tuple
# from enum import Enum
# from dataclasses import dataclass, field

# import pytesseract
# from PIL import Image
# import pdf2image
# import cv2
# import numpy as np
# from dotenv import load_dotenv

# from data_model import AithonDocument, Page

# # Load environment variables
# load_dotenv()

# # Configure logging
# logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# # ============================================================================
# # Configuration and Data Models
# # ============================================================================

# class OCREngine(Enum):
#     """Available OCR engines"""
#     TESSERACT = "tesseract"
#     AZURE_OCR = "azure_ocr"
#     GOOGLE_VISION = "google_vision"

# @dataclass
# class OCRConfig:
#     """Configuration for OCR processing"""
    
#     # OCR engine preferences
#     preferred_ocr_engines: List[OCREngine] = field(default_factory=lambda: [
#         OCREngine.TESSERACT,
#         OCREngine.AZURE_OCR
#     ])
    
#     # Image processing settings
#     dpi: int = 300
#     image_format: str = "PNG"
#     enhance_image: bool = True
    
#     # Tesseract settings
#     tesseract_config: str = "--oem 3 --psm 6"
#     tesseract_lang: str = "eng"
    
#     # Quality thresholds
#     min_confidence: float = 30.0
#     min_word_length: int = 1
    
#     # Processing limits
#     max_pages: int = 50
#     timeout_per_page: int = 30
    
#     # Output settings
#     normalize_coordinates: bool = True
#     include_confidence: bool = True
#     include_font_info: bool = False

# # ============================================================================
# # Enhanced OCR Box
# # ============================================================================

# class OCRBox:
#     """
#     Enhanced OCR processing system that extracts word-level data with accurate
#     bounding box coordinates for downstream bounding box matching.
#     """

#     def __init__(self, config: Optional[OCRConfig] = None):
#         self.config = config or OCRConfig()
        
#         # Set up Tesseract if available
#         self.tesseract_available = self._check_tesseract()
        
#         # Set up Azure OCR if configured
#         self.azure_ocr_available = self._check_azure_ocr()

#     def _check_tesseract(self) -> bool:
#         """Check if Tesseract is available"""
#         try:
#             pytesseract.get_tesseract_version()
#             return True
#         except Exception as e:
#             logging.warning(f"Tesseract not available: {e}")
#             return False

#     def _check_azure_ocr(self) -> bool:
#         """Check if Azure OCR is configured"""
#         return bool(os.getenv("AZURE_COMPUTER_VISION_ENDPOINT") and os.getenv("AZURE_COMPUTER_VISION_KEY"))

#     def _enhance_image(self, image: np.ndarray) -> np.ndarray:
#         """Enhance image quality for better OCR results"""
#         if not self.config.enhance_image:
#             return image
        
#         try:
#             # Convert to grayscale
#             if len(image.shape) == 3:
#                 gray = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)
#             else:
#                 gray = image
            
#             # Apply denoising
#             denoised = cv2.fastNlMeansDenoising(gray)
            
#             # Apply sharpening
#             kernel = np.array([[-1,-1,-1], [-1,9,-1], [-1,-1,-1]])
#             sharpened = cv2.filter2D(denoised, -1, kernel)
            
#             # Apply adaptive thresholding
#             thresh = cv2.adaptiveThreshold(
#                 sharpened, 255, cv2.ADAPTIVE_THRESH_GAUSSIAN_C, cv2.THRESH_BINARY, 11, 2
#             )
            
#             return thresh
#         except Exception as e:
#             logging.warning(f"Image enhancement failed: {e}")
#             return image

#     def _pdf_to_images(self, pdf_path: Path) -> List[Tuple[Image.Image, int]]:
#         """Convert PDF pages to images"""
#         try:
#             images = pdf2image.convert_from_path(
#                 pdf_path,
#                 dpi=self.config.dpi,
#                 fmt=self.config.image_format,
#                 first_page=1,
#                 last_page=min(self.config.max_pages, 50)  # Reasonable limit
#             )
            
#             return [(img, i + 1) for i, img in enumerate(images)]
#         except Exception as e:
#             logging.error(f"Failed to convert PDF to images: {e}")
#             return []

#     def _process_page_with_tesseract(self, image: Image.Image, page_num: int) -> List[Dict[str, Any]]:
#         """Process a single page with Tesseract OCR"""
#         try:
#             # Convert PIL image to numpy array
#             img_array = np.array(image)
            
#             # Enhance image if enabled
#             enhanced_img = self._enhance_image(img_array)
            
#             # Convert back to PIL Image
#             pil_image = Image.fromarray(enhanced_img)
            
#             # Get detailed OCR data
#             ocr_data = pytesseract.image_to_data(
#                 pil_image,
#                 config=self.config.tesseract_config,
#                 lang=self.config.tesseract_lang,
#                 output_type=pytesseract.Output.DICT
#             )
            
#             # Process OCR results
#             words = []
#             image_width, image_height = image.size
            
#             for i in range(len(ocr_data['text'])):
#                 text = ocr_data['text'][i].strip()
#                 confidence = float(ocr_data['conf'][i])
                
#                 # Filter out low-quality results
#                 if (len(text) >= self.config.min_word_length and 
#                     confidence >= self.config.min_confidence and
#                     text.replace(' ', '')):  # Not just whitespace
                    
#                     # Extract coordinates
#                     left = int(ocr_data['left'][i])
#                     top = int(ocr_data['top'][i])
#                     width = int(ocr_data['width'][i])
#                     height = int(ocr_data['height'][i])
                    
#                     # Normalize coordinates if enabled
#                     if self.config.normalize_coordinates:
#                         left_norm = left / image_width
#                         top_norm = top / image_height
#                         width_norm = width / image_width
#                         height_norm = height / image_height
#                     else:
#                         left_norm = left
#                         top_norm = top
#                         width_norm = width
#                         height_norm = height
                    
#                     word_data = {
#                         "text": text,
#                         "left": left_norm,
#                         "top": top_norm,
#                         "width": width_norm,
#                         "height": height_norm,
#                         "confidence": confidence
#                     }
                    
#                     if self.config.include_confidence:
#                         word_data["confidence"] = confidence
                    
#                     words.append(word_data)
            
#             return words
            
#         except Exception as e:
#             logging.error(f"Tesseract processing failed for page {page_num}: {e}")
#             return []

#     def _process_page_with_azure_ocr(self, image: Image.Image, page_num: int) -> List[Dict[str, Any]]:
#         """Process a single page with Azure OCR"""
#         try:
#             # This would implement Azure OCR processing
#             # For now, return empty list as placeholder
#             logging.info(f"Azure OCR processing for page {page_num} (placeholder)")
#             return []
#         except Exception as e:
#             logging.error(f"Azure OCR processing failed for page {page_num}: {e}")
#             return []

#     def _process_page(self, image: Image.Image, page_num: int) -> List[Dict[str, Any]]:
#         """Process a single page with the best available OCR engine"""
        
#         # Try OCR engines in order of preference
#         for engine in self.config.preferred_ocr_engines:
#             try:
#                 if engine == OCREngine.TESSERACT and self.tesseract_available:
#                     return self._process_page_with_tesseract(image, page_num)
#                 elif engine == OCREngine.AZURE_OCR and self.azure_ocr_available:
#                     return self._process_page_with_azure_ocr(image, page_num)
#             except Exception as e:
#                 logging.warning(f"OCR engine {engine.value} failed for page {page_num}: {e}")
#                 continue
        
#         # If all engines fail, return empty list
#         logging.error(f"All OCR engines failed for page {page_num}")
#         return []

#     def _extract_text_from_words(self, words: List[Dict[str, Any]]) -> str:
#         """Extract clean text from word-level OCR data"""
#         try:
#             # Sort words by position (top to bottom, left to right)
#             sorted_words = sorted(words, key=lambda w: (w['top'], w['left']))
            
#             # Group words into lines based on vertical position
#             lines = []
#             current_line = []
#             current_top = None
#             line_height_threshold = 0.02  # Adjust based on your documents
            
#             for word in sorted_words:
#                 if current_top is None or abs(word['top'] - current_top) <= line_height_threshold:
#                     current_line.append(word)
#                     current_top = word['top']
#                 else:
#                     if current_line:
#                         lines.append(current_line)
#                     current_line = [word]
#                     current_top = word['top']
            
#             if current_line:
#                 lines.append(current_line)
            
#             # Join words within lines and lines together
#             text_lines = []
#             for line in lines:
#                 line_words = sorted(line, key=lambda w: w['left'])
#                 line_text = ' '.join(word['text'] for word in line_words)
#                 text_lines.append(line_text)
            
#             return '\n'.join(text_lines)
            
#         except Exception as e:
#             logging.warning(f"Text extraction from words failed: {e}")
#             return ' '.join(word['text'] for word in words)

#     def __call__(self, doc_payload: AithonDocument) -> AithonDocument:
#         """
#         Process document through OCR to extract word-level data with bounding boxes.
#         """
#         start_time = time.time()
#         logging.info(f"Entering Enhanced OCR Box for: {doc_payload.original_filename}")
#         doc_payload.pipeline_status = "OCR_Processing"

#         try:
#             # Convert PDF to images
#             images = self._pdf_to_images(doc_payload.source_path)
            
#             if not images:
#                 doc_payload.error_message = "Failed to convert PDF to images"
#                 doc_payload.pipeline_status = "Failed_OCR"
#                 return doc_payload
            
#             # Process each page
#             pages = []
#             total_words = 0
            
#             for image, page_num in images:
#                 logging.info(f"Processing page {page_num} with OCR...")
                
#                 # Extract word-level data
#                 words = self._process_page(image, page_num)
                
#                 if words:
#                     # Extract text for this page
#                     page_text = self._extract_text_from_words(words)
                    
#                     # Create page object
#                     page = Page(
#                         page_number=page_num,
#                         text=page_text,
#                         raw_text=page_text,
#                         words=words
#                     )
#                     pages.append(page)
#                     total_words += len(words)
                    
#                     logging.info(f"Page {page_num}: extracted {len(words)} words")
#                 else:
#                     logging.warning(f"No words extracted from page {page_num}")
            
#             # Update document payload
#             doc_payload.pages = pages
#             doc_payload.is_scanned = True  # Mark as scanned since we have OCR data
            
#             # Combine all page texts
#             if pages:
#                 doc_payload.raw_text = '\n\n'.join(page.raw_text for page in pages)
#                 doc_payload.cleaned_text = '\n\n'.join(page.text for page in pages)
            
#             # Store OCR metadata
#             doc_payload.metadata.update({
#                 "ocr_processed": True,
#                 "ocr_engine": "tesseract" if self.tesseract_available else "fallback",
#                 "total_pages_processed": len(pages),
#                 "total_words_extracted": total_words,
#                 "ocr_processing_time": time.time() - start_time,
#                 "average_confidence": sum(
#                     word.get('confidence', 0) for page in pages for word in page.words
#                 ) / total_words if total_words > 0 else 0,
#                 "ocr_config": {
#                     "dpi": self.config.dpi,
#                     "enhance_image": self.config.enhance_image,
#                     "normalize_coordinates": self.config.normalize_coordinates
#                 }
#             })
            
#             doc_payload.pipeline_status = "OCR_Completed"
#             logging.info(f"OCR completed for '{doc_payload.original_filename}': "
#                         f"{len(pages)} pages, {total_words} words extracted")

#         except Exception as e:
#             logging.error(f"OCR processing failed for {doc_payload.original_filename}: {e}", exc_info=True)
#             doc_payload.error_message = f"OCR processing failed: {e}"
#             doc_payload.pipeline_status = "Failed_OCR"
            
#             # Store error metadata
#             doc_payload.metadata.update({
#                 "ocr_processed": False,
#                 "ocr_error": str(e),
#                 "ocr_processing_time": time.time() - start_time
#             })

#         return doc_payload 
import fitz  # PyMuPDF
import pytesseract
import logging
from PIL import Image
from pdf2image import convert_from_path
from typing import List
import pandas as pd
import os
from dotenv import load_dotenv

from ..data_model import AithonDocument, Page

# Load environment variables from .env file
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.WARNING, format='%(asctime)s - %(levelname)s - %(message)s')

class OCRBox:
    """
    Performs OCR on the document, handling both text-based and image-based PDFs.
    It extracts page text and detailed word-level bounding box information.
    """

    def __init__(self):
        # Configure Tesseract path - cross-platform support
        # On Linux, tesseract is usually installed via package manager and in PATH
        # On Windows, we need to find it or use TESSERACT_CMD env var
        import platform
        import shutil
        
        tesseract_cmd = os.getenv("TESSERACT_CMD")
        
        if tesseract_cmd:
            pytesseract.pytesseract.tesseract_cmd = tesseract_cmd
            logging.info(f"Using TESSERACT_CMD from env: {tesseract_cmd}")
        elif platform.system() == "Windows":
            # Windows: Try to find Tesseract in common locations
            common_paths = [
                r"C:\Program Files\Tesseract-OCR\tesseract.exe",
                r"C:\Program Files (x86)\Tesseract-OCR\tesseract.exe",
                os.path.join(os.path.expanduser("~"), "AppData", "Local", "Programs", "Tesseract-OCR", "tesseract.exe"),
                r"C:\Tesseract-OCR\tesseract.exe",
            ]
            
            tesseract_found = None
            for path in common_paths:
                if os.path.exists(path):
                    pytesseract.pytesseract.tesseract_cmd = path
                    logging.info(f"Found Tesseract at: {path}")
                    tesseract_found = path
                    break
            
            if tesseract_found is None:
                # Check if tesseract is in PATH
                tesseract_in_path = shutil.which("tesseract.exe")
                if tesseract_in_path:
                    pytesseract.pytesseract.tesseract_cmd = tesseract_in_path
                    logging.info(f"Found Tesseract in PATH: {tesseract_in_path}")
                else:
                    # Try default name - pytesseract will handle the error
                    pytesseract.pytesseract.tesseract_cmd = "tesseract.exe"
                    logging.warning(
                        "Tesseract not found on Windows. Please either:\n"
                        "1. Install Tesseract and set TESSERACT_CMD environment variable\n"
                        "2. Install Tesseract to default location (C:\\Program Files\\Tesseract-OCR)\n"
                        "Download from: https://github.com/UB-Mannheim/tesseract/wiki"
                    )
        else:
            # Linux/Unix: Check if tesseract is in PATH
            tesseract_in_path = shutil.which("tesseract")
            if tesseract_in_path:
                pytesseract.pytesseract.tesseract_cmd = tesseract_in_path
                logging.info(f"Found Tesseract in PATH: {tesseract_in_path}")
            else:
                # Try default name - pytesseract will handle the error
                pytesseract.pytesseract.tesseract_cmd = "tesseract"
                logging.warning(
                    "Tesseract not found. On Linux, install with:\n"
                    "  sudo apt-get install tesseract-ocr  # Debian/Ubuntu\n"
                    "  sudo yum install tesseract          # RHEL/CentOS\n"
                    "Or set TESSERACT_CMD environment variable to tesseract executable path"
                )
        
        self.temp_dir = os.getenv("TEMP_DIR", "./temp")
        os.makedirs(self.temp_dir, exist_ok=True)
        
        # Configure Poppler path - cross-platform support
        # On Linux, poppler is usually installed via package manager (poppler-utils)
        # and binaries are in /usr/bin which is in PATH, so poppler_path can be None
        # On Windows, we need to find it or use POPPLER_PATH env var
        import platform
        import shutil
        
        poppler_path_env = os.getenv("POPPLER_PATH")
        
        if poppler_path_env:
            self.poppler_path = poppler_path_env
            logging.info(f"Using POPPLER_PATH from env: {self.poppler_path}")
        elif platform.system() == "Windows":
            # Windows: Try to find poppler in common locations
            common_paths = [
                r"C:\Program Files\poppler\Library\bin",
                r"C:\poppler\Library\bin",
                os.path.join(os.path.expanduser("~"), "poppler", "Library", "bin"),
                os.path.join(os.path.expanduser("~"), "Desktop", "poppler-25.07.0", "Library", "bin"),
            ]
            
            self.poppler_path = None
            for path in common_paths:
                pdftoppm_exe = os.path.join(path, "pdftoppm.exe")
                if os.path.exists(pdftoppm_exe):
                    self.poppler_path = path
                    logging.info(f"Found Poppler at: {self.poppler_path}")
                    break
            
            if self.poppler_path is None:
                # Check if poppler is in PATH
                if shutil.which("pdftoppm.exe"):
                    self.poppler_path = None  # None means use system PATH
                    logging.info("Poppler found in Windows PATH")
                else:
                    self.poppler_path = None
                    logging.warning(
                        "Poppler not found on Windows. Please either:\n"
                        "1. Install Poppler and set POPPLER_PATH environment variable\n"
                        "2. Add Poppler bin folder to Windows PATH\n"
                        "Download from: https://github.com/oschwartz10612/poppler-windows/releases/"
                    )
        else:
            # Linux/Unix: Check if poppler is in PATH
            if shutil.which("pdftoppm") or shutil.which("pdftocairo"):
                self.poppler_path = None  # None means use system PATH (works on Linux)
                logging.info("Poppler found in system PATH (Linux/Unix)")
            else:
                self.poppler_path = None
                logging.warning(
                    "Poppler not found. On Linux, install with:\n"
                    "  sudo apt-get install poppler-utils  # Debian/Ubuntu\n"
                    "  sudo yum install poppler-utils      # RHEL/CentOS\n"
                    "Or set POPPLER_PATH environment variable to poppler bin directory"
                )


    def _extract_text_directly(self, doc_payload: AithonDocument) -> bool:
        """Attempts to extract text directly from the PDF."""
        text = ""
        try:
            pdf_document = fitz.open(doc_payload.source_path)
            for page_num in range(len(pdf_document)):
                page = pdf_document.load_page(page_num)
                text += page.get_text()
            pdf_document.close()
            
            # If we get a reasonable amount of text, we consider it successful.
            if len(text.strip()) > 100:
                doc_payload.raw_text = text
                doc_payload.is_scanned = False
                return True
        except Exception as e:
            logging.warning(f"Direct text extraction failed for {doc_payload.original_filename}: {e}")
        
        return False

    def _perform_ocr(self, doc_payload: AithonDocument):
        """Performs full OCR by converting PDF to images and using Tesseract."""
        logging.info(f"Performing full OCR on {doc_payload.original_filename}...")
        doc_payload.is_scanned = True
        all_pages_text = []

        try:
            # Convert PDF to a list of PIL images
            # If poppler_path is None, pdf2image will use system PATH (works on Linux)
            convert_kwargs = {
                "dpi": 300,
                "output_folder": self.temp_dir,
            }
            if self.poppler_path is not None:
                convert_kwargs["poppler_path"] = self.poppler_path
            
            images = convert_from_path(
                doc_payload.source_path,
                **convert_kwargs
            )
            
            for i, image in enumerate(images):
                page_num = i + 1
                logging.info(f"Processing page {page_num}/{len(images)}")
                
                # Use Tesseract to get detailed data including boxes
                # Normalizing coordinates can be done here if needed
                page_data = pytesseract.image_to_data(image, output_type=pytesseract.Output.DATAFRAME)
                page_data = page_data[page_data.conf != -1] # Filter out non-confident words

                # Store word-level data
                words_info = page_data[['left', 'top', 'width', 'height', 'text']].to_dict('records')
                
                # Extract text for the page
                page_text = " ".join(page_data["text"].dropna())
                all_pages_text.append(page_text)
                
                # Create and append the page object
                page_obj = Page(
                    page_number=page_num,
                    text=page_text,
                    raw_text=page_text, # In OCR, raw and final are the same at this stage
                    words=words_info
                )
                doc_payload.pages.append(page_obj)

            doc_payload.raw_text = "\n".join(all_pages_text)

        except Exception as e:
            logging.error(f"An error occurred during OCR for {doc_payload.original_filename}: {e}")
            doc_payload.error_message = f"OCR failed: {e}"
            doc_payload.pipeline_status = "Failed_OCR"
        finally:
            # Clean up temporary image files
            for item in os.listdir(self.temp_dir):
                if item.endswith(".ppm"):
                    os.remove(os.path.join(self.temp_dir, item))


    def __call__(self, doc_payload: AithonDocument) -> AithonDocument:
        """
        Processes the document through the OCR box.
        """
        logging.info(f"Entering OCR Box for: {doc_payload.original_filename}")
        doc_payload.pipeline_status = "OCR_Processing"

        # Try to get text directly first. If it's not a scanned PDF, this is much faster.
        if not self._extract_text_directly(doc_payload):
            # If direct extraction fails or yields too little text, perform full OCR.
            self._perform_ocr(doc_payload)

        if not doc_payload.error_message:
            doc_payload.pipeline_status = "OCR_Completed"
            logging.info(f"Successfully processed through OCR Box: {doc_payload.original_filename}")

        return doc_payload

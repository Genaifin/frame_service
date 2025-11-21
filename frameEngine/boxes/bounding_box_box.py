import asyncio
import logging
import time
import json
import copy
from pathlib import Path
from typing import Dict, Any, Optional, List, Tuple
from dataclasses import dataclass
import pandas as pd
from PIL import Image
import os
import pytesseract
from pdf2image import convert_from_path
import base64
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
logging.basicConfig(level=logging.WARNING, format='%(asctime)s - %(levelname)s - %(message)s')

@dataclass
class BoundingBoxConfig:
    """Configuration for bounding box extraction"""
    
    # OCR settings
    dpi: int = 400
    use_pdftocairo: bool = True
    thread_count: int = 1
    
    # LLM settings
    model_name: str = "gpt-4o"
    temperature: float = 0.0
    max_tokens: int = 4000
    api_timeout: int = 120
    
    # Processing settings
    max_retries: int = 3
    normalize_coordinates: bool = True
    
    # Quality settings
    min_confidence: float = 30.0
    enable_detailed_logging: bool = True

class BoundingBoxService:
    """Service for extracting bounding box coordinates from OCR data"""
    
    def __init__(self):
        pass
    
    def clean_text(self, text: str) -> str:
        """Clean text for matching"""
        if pd.isna(text):
            return ""
        return str(text).strip().replace("$", "").replace(",", "").replace(".", "").replace("-", "").replace(" ", "").lower()
    
    def charater_by_character_exact_matching_one_word(self, verbatim_word: str, tsv_word: str) -> bool:
        """Exact character-by-character matching"""
        return verbatim_word == tsv_word
    
    def find_sequence_match(self, verbatim_words: List[str], tsv_data: pd.DataFrame) -> Tuple[List[List[float]], List[int]]:
        """Find a sequence of words in the TSV data that matches the verbatim_words list"""
        matched_boxes = []
        matched_page_numbers = []
        
        # Convert to list for easier indexing
        sorted_rows = list(tsv_data.iterrows())
        
        # Try to find the sequence starting from each possible position
        for i in range(len(sorted_rows)):
            matched_boxes = []
            match_failed = False
            
            for j, verbatim_word in enumerate(verbatim_words):
                if i + j >= len(sorted_rows):
                    match_failed = True
                    break
                    
                index, row = sorted_rows[i + j]
                tsv_word = str(row["text"])
                
                # Clean and compare words
                clean_verbatim = self.clean_text(verbatim_word)
                clean_tsv = self.clean_text(tsv_word)
                
                if self.charater_by_character_exact_matching_one_word(clean_verbatim, clean_tsv):
                    # Word matches, add its bounding box
                    matched_boxes.append([f"{row['left']},{row['top']},{row['width']},{row['height']}"])
                    matched_page_numbers.append(row["page_num"])
                else:
                    match_failed = True
                    break
            
            # If we matched all verbatim words without failure, return the boxes
            if not match_failed and len(matched_boxes) == len(verbatim_words):
                return matched_boxes, matched_page_numbers
        
        # No match found
        return None, None
    
    def approximate_currency_bounding_box(self, bbox_dict: dict, verbatim_text: dict, tsv_data: pd.DataFrame) -> dict:
        """Approximate currency bounding box for values with currency symbols"""
        for key, value in verbatim_text.items():
            if key.endswith("_currency") or "$" in str(value):
                # Extract numeric part
                numeric_part = str(value).replace("$", "").replace(",", "").strip()
                
                # Search for the numeric part in TSV data
                for _, row in tsv_data.iterrows():
                    tsv_text = str(row["text"])
                    if self.clean_text(numeric_part) == self.clean_text(tsv_text):
                        # Calculate adjusted bounding box to include currency symbol
                        left = max(0, row["left"] - 0.02)  # Adjust left to include $
                        top = row["top"]
                        width = row["width"] + 0.02  # Extend width
                        height = row["height"]
                        
                        bbox_dict["BoundingBox"][key] = [f"{left},{top},{width},{height}"]
                        bbox_dict["PageNumber"][key] = row["page_num"]
                        break
        
        return bbox_dict
    
    def find_bounding_box(self, verbatim_text: dict, tsv_data: pd.DataFrame) -> dict:
        """Main bounding box finding logic"""
        bbox_dict = {"BoundingBox": {}, "PageNumber": {}}
        updated_verbatim_text = copy.deepcopy(verbatim_text)
        keys_to_remove = []
        
        # First pass: Match single words
        for key, value in verbatim_text.items():
            if len(str(value).split()) == 1 and not key.endswith("_currency"):
                for row in tsv_data.iterrows():
                    tsv_text = row[1]["text"]
                    is_word_matching = self.charater_by_character_exact_matching_one_word(
                        self.clean_text(str(value)), self.clean_text(str(tsv_text))
                    )
                    if is_word_matching:
                        left = row[1]["left"]
                        top = row[1]["top"]
                        width = row[1]["width"]
                        height = row[1]["height"]
                        page_num = row[1]["page_num"]
                        bbox_dict["BoundingBox"][key] = [f"{left},{top},{width},{height}"]
                        bbox_dict["PageNumber"][key] = page_num
                        keys_to_remove.append(key)
                        break
        
        # Remove matched keys
        for key in keys_to_remove:
            updated_verbatim_text.pop(key, None)
        
        logging.info(f"Single word matches: {len(bbox_dict['BoundingBox'].keys())}")
        
        # Second pass: Match multi-word sequences
        keys_to_remove = []
        for key, value in updated_verbatim_text.items():
            # Split the value into a list of words
            verbatim_words = str(value).split()
            
            # Skip single words (already processed)
            if len(verbatim_words) <= 1:
                continue
            
            # Find sequence match
            matched_boxes, matched_page_numbers = self.find_sequence_match(verbatim_words, tsv_data)
            
            if matched_boxes:
                # Store all bounding boxes for the matched sequence
                bbox_dict["BoundingBox"][key] = [coord for box in matched_boxes for coord in box]
                bbox_dict["PageNumber"][key] = matched_page_numbers[0]
                keys_to_remove.append(key)
        
        # Remove matched keys
        for key in keys_to_remove:
            updated_verbatim_text.pop(key, None)
        
        logging.info(f"Total matches after sequence matching: {len(bbox_dict['BoundingBox'].keys())}")
        
        # Apply currency approximation
        update_bbox = self.approximate_currency_bounding_box(
            bbox_dict=bbox_dict, 
            verbatim_text=verbatim_text, 
            tsv_data=tsv_data
        )
        
        logging.info(f"Total matches after currency approximation: {len(update_bbox['BoundingBox'].keys())}")
        
        return update_bbox

class BoundingBoxBox:
    """
    Advanced Bounding Box extraction system that processes PDFs with OCR
    and uses LLM to generate accurate bounding box coordinates.
    """
    
    def __init__(self, config: Optional[BoundingBoxConfig] = None):
        self.config = config or BoundingBoxConfig()
        self.client = OpenAI()
        self.bbox_service = BoundingBoxService()
        
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

        # Poppler path: cross-platform support
        # On Linux, poppler is usually in PATH, so poppler_path can be None
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

        # Check if Tesseract is available
        self.tesseract_available = self._check_tesseract()
        
    def _check_tesseract(self) -> bool:
        """Check if Tesseract is available"""
        try:
            pytesseract.get_tesseract_version()
            return True
        except Exception as e:
            logging.warning(f"Tesseract not available: {e}")
            return False
    
    def _extract_verbatim_text(self, extracted_data: dict) -> dict:
        """Extract verbatim text from extracted data"""
        verbatim_text = {}
        
        def extract_from_dict(data, prefix=""):
            if isinstance(data, dict):
                for key, value in data.items():
                    if isinstance(value, dict):
                        if "VerbatimText" in value and value["VerbatimText"]:
                            verbatim_text[f"{prefix}{key}"] = value["VerbatimText"]
                        elif "Value" in value and value["Value"]:
                            verbatim_text[f"{prefix}{key}"] = str(value["Value"])
                        else:
                            extract_from_dict(value, f"{prefix}{key}_")
                    elif isinstance(value, list):
                        for i, item in enumerate(value):
                            extract_from_dict(item, f"{prefix}{key}_{i}_")
            elif isinstance(data, list):
                for i, item in enumerate(data):
                    extract_from_dict(item, f"{prefix}{i}_")
        
        if "entities" in extracted_data:
            for entity in extracted_data["entities"]:
                if "portfolio" in entity:
                    for idx, portfolio_item in enumerate(entity["portfolio"]):
                        # Use InvestorRefID if available, otherwise use index
                        investor_id = None
                        if isinstance(portfolio_item, dict) and "InvestorRefID" in portfolio_item:
                            investor_ref = portfolio_item["InvestorRefID"]
                            if isinstance(investor_ref, dict) and "Value" in investor_ref:
                                investor_id = investor_ref["Value"]
                        
                        # Create unique prefix for each investor
                        prefix = f"investor_{investor_id}_{idx}_" if investor_id else f"investor_{idx}_"
                        extract_from_dict(portfolio_item, prefix)
        
        # Filter out Account keys as per the prompt requirements
        verbatim_text = {k: v for k, v in verbatim_text.items() if not k.endswith("_account") and k != "Account"}
        
        return verbatim_text
    
    async def _perform_ocr_extraction(self, file_path: Path) -> pd.DataFrame:
        """Perform OCR extraction using Tesseract"""
        if not self.tesseract_available:
            raise RuntimeError("Tesseract is not available for OCR processing")
        
        logging.info(f"Starting OCR extraction for: {file_path}")
        start_time = time.time()
        
        # Convert PDF to images
        # If poppler_path is None, pdf2image will use system PATH (works on Linux)
        convert_kwargs = {
            "dpi": self.config.dpi,
            "thread_count": self.config.thread_count,
            "use_pdftocairo": self.config.use_pdftocairo,
        }
        if self.poppler_path is not None:
            convert_kwargs["poppler_path"] = self.poppler_path
        
        images = convert_from_path(
            file_path,
            **convert_kwargs
        )
        
        combined_data = []
        
        for page_num, image in enumerate(images, 1):
            try:
                img_width, img_height = image.size
                
                # Extract data using Tesseract
                tsv_data = await asyncio.to_thread(
                    pytesseract.image_to_data, 
                    image, 
                    output_type=pytesseract.Output.DATAFRAME
                )
                
                # Normalize coordinates
                if self.config.normalize_coordinates:
                    tsv_data["left"] = tsv_data["left"] / img_width
                    tsv_data["width"] = tsv_data["width"] / img_width
                    tsv_data["top"] = tsv_data["top"] / img_height
                    tsv_data["height"] = tsv_data["height"] / img_height
                
                # Add page number
                tsv_data["page_num"] = page_num
                
                # Filter out empty text
                tsv_data = tsv_data[tsv_data["text"].notna() & (tsv_data["text"] != "")]
                
                combined_data.append(tsv_data)
                
            except Exception as e:
                logging.error(f"Failed to process page {page_num}: {e}")
        
        if not combined_data:
            raise RuntimeError("No OCR data extracted from document")
        
        # Combine all pages
        combined_df = pd.concat(combined_data, ignore_index=True)
        
        processing_time = time.time() - start_time
        logging.info(f"OCR extraction completed in {processing_time:.2f} seconds")
        
        return combined_df
    
    async def _get_encoded_images_from_pdf(self, file_path: Path) -> List[str]:
        """Convert PDF pages to base64-encoded images for LLM processing"""
        try:
            # Reduce image size significantly to save tokens - vision models can work with smaller images
            images = convert_from_path(file_path, size=(800, None), poppler_path=self.poppler_path)  # Reduced from no limit
            encoded_images = []
            
            for page in images:
                # Convert to JPEG and encode with compression
                import io
                img_buffer = io.BytesIO()
                # Use lower quality to reduce file size and tokens
                page.save(img_buffer, format='JPEG', quality=70, optimize=True)
                img_buffer.seek(0)
                
                encoded_image = base64.b64encode(img_buffer.read()).decode('utf-8')
                encoded_images.append(encoded_image)
            
            return encoded_images
            
        except Exception as e:
            logging.error(f"Failed to convert PDF to images: {e}")
            raise

    def _group_verbatim_text_by_page(self, verbatim_text: dict, ocr_data: pd.DataFrame) -> Dict[int, dict]:
        """Group verbatim text by the most likely page based on OCR data"""
        page_groups = {}
        unmatched_fields = {}
        
        # Get unique pages from OCR data
        pages = sorted(ocr_data['page_num'].unique())
        
        # Initialize page groups
        for page in pages:
            page_groups[page] = {}
        
        # Try to match each verbatim text to a page
        for key, value in verbatim_text.items():
            matched_page = None
            best_match_score = 0
            
            if isinstance(value, str) and len(value.strip()) > 0:
                value_clean = self.bbox_service.clean_text(str(value))
                
                # Search for this value in OCR data
                for page in pages:
                    page_ocr = ocr_data[ocr_data['page_num'] == page]
                    
                    for _, row in page_ocr.iterrows():
                        ocr_text_clean = self.bbox_service.clean_text(str(row['text']))
                        
                        # Calculate match score
                        if len(value_clean.split()) == 1:
                            # Single word - exact match
                            if self.bbox_service.charater_by_character_exact_matching_one_word(value_clean, ocr_text_clean):
                                matched_page = page
                                best_match_score = 1.0
                                break
                        else:
                            # Multi-word - partial match scoring
                            words_in_value = value_clean.split()
                            if len(words_in_value) > 0:
                                matches = sum(1 for word in words_in_value if word.lower() in ocr_text_clean.lower())
                                score = matches / len(words_in_value)
                                
                                if score > best_match_score and score > 0.5:
                                    matched_page = page
                                    best_match_score = score
                            
                    if matched_page and best_match_score == 1.0:
                        break
            
            # Assign to page or unmatched
            if matched_page:
                page_groups[matched_page][key] = value
            else:
                unmatched_fields[key] = value
        
        # Distribute unmatched fields across pages (they'll be processed with context)
        if unmatched_fields:
            for page in pages:
                page_groups[page].update(unmatched_fields)
        
        return page_groups

    def _build_page_specific_prompt(self, page_num: int, page_verbatim_text: dict, 
                                  page_ocr_data: pd.DataFrame, total_pages: int,
                                  document_context: str = "") -> str:
        """Build a page-specific bounding box extraction prompt with global context"""
        
        # Convert OCR data to markdown table format
        ocr_markdown = page_ocr_data.to_markdown(index=False)
        
        # Context information about the document
        context_info = f"""
DOCUMENT CONTEXT:
- Processing page {page_num} of {total_pages}
- Document type: Financial statement/report
- This page may contain fields that reference or relate to content on other pages
{document_context}
"""
        
        prompt = f"""You are given a single page from a PDF document image, extracted verbatim text, and initial bounding box coordinates.

{context_info}

Your task is to match each key from the verbatim text to its precise bounding box on THIS PAGE ONLY and correct any inaccuracies. Always return **NORMALISED VALUES ONLY**.

IMPORTANT: Only return bounding boxes for fields that are ACTUALLY VISIBLE on this specific page (page {page_num}). If a field is not visible on this page, do not include it in the response.

CRITICAL: Do NOT return placeholder, fake, or estimated coordinates. If you cannot find precise coordinates for a field on this page, simply omit that field from the response entirely. Never use generic coordinates like "0.1,0.2,0.3,0.4" or similar placeholder values.

Core Rules:
1. Single-Word Matches: If multiple bounding boxes are provided for a single-word key, return only one that is most likely correct, following the left-to-right, top-to-bottom reading order. Format: "left,top,width,height"

2. Multi-Word Matches: If multiple bounding boxes are provided for a multi-word key, return only one logically correct set. Concatenate bounding boxes (in left-to-right, top-to-bottom reading order) into a single comma-separated string. Format: "left,top,width,height,left,top,width,height,..."

3. No Bounding Box Reuse: Never reuse bounding boxes for different keys unless the text appears at the exact same location.

4. Same Value, Different Keys: If different keys have the same value, ensure each key gets a distinct bounding box, unless absolutely necessary.

5. Preserve Correct Bounding Boxes: Do not modify bounding boxes that are already correct. Only correct bounding boxes if they are evidently incorrect.

6. Inferring Missing Bounding Boxes: Only infer coordinates if you can clearly see the text in the PDF page image and can accurately determine its position. Do not guess or estimate.

7. Maintain Coordinate Precision: Do not round coordinates—keep them as-is in normalized form.

8. Handling Date Variations: If a date is given as "11/10/2023" but appears in the document as "October 11, 2022" or "Oct 11, 2022", use that to determine the bounding box.

9. Excluding Specific Keys: Always remove "Account" from the response.

10. Ignoring Invalid Values: If a key's value is "", "-", "NA", "N/A", or "nan", ignore the key and do not include it in the output.

11. Page-Specific Processing: CRITICAL - Only include fields that are actually visible on page {page_num}. If you cannot find a field on this page, do not include it in the response.

12. No Placeholder Coordinates: NEVER return placeholder or fake coordinates. If you cannot determine precise coordinates, omit the field entirely.

13. Strict JSON Output: Return results only in the specified JSON format. Do not include any additional commentary.

**Input**:
- $Verbatim Text$ (Fields to look for on page {page_num})
{json.dumps(page_verbatim_text, indent=2)}

- $Page {page_num} Bounding Box Data (Left, Top, Width, Height, Page Number)$
{ocr_markdown}

**Output** (JSON only, strictly follow the format):
{{
    "BoundingBox": {{
        "key1": "left,top,width,height[,left,top,width,height for additional words]",
        "key2": "left,top,width,height[,left,top,width,height for additional words]",
        ...
    }},
    "PageNumber": {{
        "key1": {page_num},
        "key2": {page_num},
        ...
    }}
}}

Remember: Only include fields with genuine, accurate coordinates that you can verify by looking at the page image. When in doubt, leave the field out."""
        
        return prompt
 
    # for testing this prompt we pickup manually but later we will take it from langfuse prompt
    def _build_bounding_box_prompt(self, verbatim_text: dict, ocr_data: pd.DataFrame) -> str:
        """Build the bounding box extraction prompt"""
        
        # Convert OCR data to markdown table format
        ocr_markdown = ocr_data.to_markdown(index=False)
        
        prompt = f"""You are given a PDF document image, extracted verbatim text, and initial bounding box coordinates.

Your task is to match each key from the verbatim text to its precise bounding box on the PDF document image and correct any inaccuracies and always return **NORMALISED VALUES ONLY**.

Core Rules:
1. Single-Word Matches: If multiple bounding boxes are provided for a single-word key, return only one that is most likely correct, following the left-to-right, top-to-bottom reading order. Format: "left,top,width,height"

2. Multi-Word Matches: If multiple bounding boxes are provided for a multi-word key, return only one logically correct set. Concatenate bounding boxes (in left-to-right, top-to-bottom reading order) into a single comma-separated string. Format: "left,top,width,height,left,top,width,height,..."

3. No Bounding Box Reuse: Never reuse bounding boxes for different keys unless the text appears at the exact same location.

4. Same Value, Different Keys: If different keys have the same value, ensure each key gets a distinct bounding box, unless absolutely necessary.

5. Preserve Correct Bounding Boxes: Do not modify bounding boxes that are already correct. Only correct bounding boxes if they are evidently incorrect.

6. Inferring Missing Bounding Boxes: If a bounding box is missing but the text exists in the PDF document image, infer its position logically based on text structure, column alignment, and nearby related values.

7. Maintain Coordinate Precision: Do not round coordinates—keep them as-is in normalized form.

8. Handling Date Variations: If a date is given as "11/10/2023" but appears in the document as "October 11, 2022" or "Oct 11, 2022", use that to determine the bounding box.

9. Excluding Specific Keys: Always remove "Account" from the response.

10. Ignoring Invalid Values: If a key's value is "", "-", "NA", "N/A", or "nan", ignore the key and do not include it in the output.

11. Strict JSON Output: Return results only in the specified JSON format. Do not include any additional commentary.

**Input**:
- $Verbatim Text$
{json.dumps(verbatim_text, indent=2)}

- $Initial Bounding Box Data (Left, Top, Width, Height, Page Number)$
{ocr_markdown}

**Output** (JSON only, strictly follow the format):
{{
    "BoundingBox": {{
        "key1": "left,top,width,height[,left,top,width,height for additional words]",
        "key2": "left,top,width,height[,left,top,width,height for additional words]",
        ...
    }},
    "PageNumber": {{
        "key1": page_number,
        "key2": page_number,
        ...
    }}
}}
"""
        
        return prompt

    def _format_ocr_data_fallback(self, ocr_data: pd.DataFrame) -> str:
        """Fallback method to format OCR data when tabulate is not available"""
        
        # Create a simple table format without using tabulate
        columns = ['text', 'left', 'top', 'width', 'height', 'page_num']
        
        # Filter and select relevant columns
        if not ocr_data.empty:
            filtered_data = ocr_data[columns].head(100)  # Limit to first 100 rows
            
            # Create header
            header = "| " + " | ".join(columns) + " |\n"
            separator = "| " + " | ".join(["---"] * len(columns)) + " |\n"
            
            # Create rows
            rows = []
            for _, row in filtered_data.iterrows():
                row_str = "| " + " | ".join([str(row[col]) for col in columns]) + " |\n"
                rows.append(row_str)
            
            return header + separator + "".join(rows)
        else:
            return "No OCR data available"
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=2, min=4, max=120),
        retry=retry_if_exception_type((openai.RateLimitError, openai.APITimeoutError, openai.APIConnectionError)),
        before_sleep=before_sleep_log(logging.getLogger(), logging.WARNING),
        after=after_log(logging.getLogger(), logging.INFO)
    )
    async def _extract_bounding_boxes_page_by_page(self, verbatim_text: dict, ocr_data: pd.DataFrame, encoded_images: List[str]) -> dict:
        """Extract bounding boxes page by page while maintaining document context"""
        
        # Group verbatim text by likely pages
        page_groups = self._group_verbatim_text_by_page(verbatim_text, ocr_data)
        
        # Prepare document context
        total_pages = len(encoded_images)
        document_context = f"Total verbatim fields to process: {len(verbatim_text)}"
        
        # Final results
        final_bounding_boxes = {}
        final_page_numbers = {}
        
        # Process each page
        for page_num in range(1, total_pages + 1):
            if page_num not in page_groups or not page_groups[page_num]:
                logging.info(f"No fields to process for page {page_num}, skipping")
                continue
                
            logging.info(f"Processing page {page_num}/{total_pages} with {len(page_groups[page_num])} fields")
            
            # Get page-specific OCR data
            page_ocr_data = ocr_data[ocr_data['page_num'] == page_num].copy()
            
            if page_ocr_data.empty:
                logging.warning(f"No OCR data for page {page_num}, skipping")
                continue
            
            # Build page-specific prompt
            prompt = self._build_page_specific_prompt(
                page_num, 
                page_groups[page_num], 
                page_ocr_data, 
                total_pages,
                document_context
            )
            
            # Prepare messages for this page only
            messages = [
                {
                    "role": "system",
                    "content": "You are a precise bounding box extraction expert. Return only valid JSON in the specified format."
                },
                {
                    "role": "user",
                    "content": [
                        {"type": "text", "text": prompt},
                        {
                            "type": "image_url",
                            "image_url": {"url": f"data:image/jpeg;base64,{encoded_images[page_num-1]}"}
                        }
                    ]
                }
            ]
            
            try:
                response = self.client.chat.completions.create(
                    model=self.config.model_name,
                    messages=messages,
                    max_tokens=self.config.max_tokens,
                    temperature=self.config.temperature,
                    timeout=self.config.api_timeout,
                    response_format={"type": "json_object"}
                )
                
                # Parse response
                result_text = response.choices[0].message.content.strip()
                
                # Log the raw response for debugging
                if self.config.enable_detailed_logging:
                    logging.info(f"Page {page_num} LLM response: {result_text[:500]}...")
                
                page_result = json.loads(result_text)
                
                # Validate and clean the page result
                if "BoundingBox" in page_result:
                    cleaned_bbox = {}
                    for key, value in page_result["BoundingBox"].items():
                        # Skip obviously invalid coordinates
                        if isinstance(value, str):
                            # Check if it's a placeholder or invalid coordinate
                            coords = value.split(',')
                            if len(coords) >= 4:
                                try:
                                    float_coords = [float(c.strip()) for c in coords[:4]]
                                    # Skip if all coordinates are the same simple values
                                    if not (all(c == 0.1 for c in float_coords) or 
                                           all(c == 0.2 for c in float_coords) or
                                           all(c in [0.1, 0.2, 0.3, 0.4, 0.5] for c in float_coords)):
                                        cleaned_bbox[key] = value
                                        logging.debug(f"Page {page_num}: Valid coordinates for {key}: {value}")
                                    else:
                                        logging.debug(f"Page {page_num}: Skipping placeholder coordinates for {key}: {value}")
                                except ValueError:
                                    logging.debug(f"Page {page_num}: Skipping invalid coordinates for {key}: {value}")
                        elif isinstance(value, list):
                            # Handle list of coordinates
                            valid_coords = []
                            for coord_str in value:
                                if isinstance(coord_str, str):
                                    coords = coord_str.split(',')
                                    if len(coords) >= 4:
                                        try:
                                            float_coords = [float(c.strip()) for c in coords[:4]]
                                            if not all(c in [0.1, 0.2, 0.3, 0.4, 0.5] for c in float_coords):
                                                valid_coords.append(coord_str)
                                        except ValueError:
                                            continue
                            if valid_coords:
                                cleaned_bbox[key] = valid_coords
                                logging.debug(f"Page {page_num}: Valid coordinate list for {key}: {valid_coords}")
                    
                    page_result["BoundingBox"] = cleaned_bbox
                
                # Merge results
                if "BoundingBox" in page_result and page_result["BoundingBox"]:
                    final_bounding_boxes.update(page_result["BoundingBox"])
                    logging.info(f"Page {page_num} processed: {len(page_result['BoundingBox'])} valid bounding boxes found")
                else:
                    logging.info(f"Page {page_num} processed: No valid bounding boxes found")
                
                if "PageNumber" in page_result:
                    final_page_numbers.update(page_result["PageNumber"])
                
            except json.JSONDecodeError as e:
                logging.error(f"Failed to parse JSON response for page {page_num}: {e}")
                logging.error(f"Raw response: {result_text}")
                continue
            except Exception as e:
                logging.error(f"Failed to process page {page_num}: {e}")
                # Continue with other pages
                continue
        
        return {
            "BoundingBox": final_bounding_boxes,
            "PageNumber": final_page_numbers
        }

    async def _extract_bounding_boxes_with_llm(self, verbatim_text: dict, ocr_data: pd.DataFrame, images: List[str]) -> dict:
        """Extract bounding boxes using LLM with smart page-by-page processing"""
        
        # Check if we need to use page-by-page processing
        total_verbatim_fields = len(verbatim_text)
        total_ocr_rows = len(ocr_data)
        total_pages = len(images)
        
        # Estimate token usage (rough calculation)
        estimated_text_tokens = len(json.dumps(verbatim_text)) // 3  # Rough token estimation
        estimated_ocr_tokens = len(ocr_data.to_markdown(index=False)) // 3
        estimated_image_tokens = total_pages * 800  # Approximate tokens per image
        total_estimated_tokens = estimated_text_tokens + estimated_ocr_tokens + estimated_image_tokens
        
        logging.info(f"Token estimation: {total_estimated_tokens} tokens ({total_verbatim_fields} fields, {total_ocr_rows} OCR rows, {total_pages} pages)")
        
        # Use page-by-page processing if estimated tokens exceed threshold
        if total_estimated_tokens > 100000 or total_pages > 5:
            logging.info("Using page-by-page processing to avoid token limits")
            return await self._extract_bounding_boxes_page_by_page(verbatim_text, ocr_data, images)
        else:
            logging.info("Using original single-request processing")
            # Original implementation for smaller documents
            prompt = self._build_bounding_box_prompt(verbatim_text, ocr_data)
            
            # Prepare messages for vision model
            messages = [
                {
                    "role": "system",
                    "content": "You are a precise bounding box extraction expert. Return only valid JSON in the specified format."
                },
                {
                    "role": "user",
                    "content": [
                        {"type": "text", "text": prompt}
                    ] + [
                        {
                            "type": "image_url",
                            "image_url": {"url": f"data:image/jpeg;base64,{img}"}
                        } for img in images
                    ]
                }
            ]
            
            response = self.client.chat.completions.create(
                model=self.config.model_name,
                messages=messages,
                max_tokens=self.config.max_tokens,
                temperature=self.config.temperature,
                timeout=self.config.api_timeout,
                response_format={"type": "json_object"}
            )
            
            # Parse response
            result_text = response.choices[0].message.content.strip()
            
            try:
                result = json.loads(result_text)
                return result
            except json.JSONDecodeError as e:
                logging.error(f"Failed to parse LLM response: {e}")
                logging.error(f"Response text: {result_text}")
                raise ValueError(f"Invalid JSON response from LLM: {e}")
    
    def _merge_bounding_boxes(self, ocr_bbox: dict, llm_bbox: dict) -> dict:
        """Merge OCR and LLM bounding boxes, prioritizing LLM results"""
        merged_bbox = ocr_bbox.copy()
        
        if llm_bbox and "BoundingBox" in llm_bbox:
            merged_bbox["BoundingBox"].update(llm_bbox["BoundingBox"])
        
        if llm_bbox and "PageNumber" in llm_bbox:
            merged_bbox["PageNumber"].update(llm_bbox["PageNumber"])
        
        return merged_bbox
    
    def _integrate_bounding_boxes(self, extracted_data: dict, bbox_data: dict) -> dict:
        """Integrate bounding box data into the extracted data structure"""
        if not bbox_data or "BoundingBox" not in bbox_data:
            return extracted_data
        
        def is_placeholder_coordinate(coord_str):
            """Check if coordinate string is a placeholder"""
            if not coord_str:
                return True
            # Check for obvious placeholder patterns
            coords = coord_str.split(',')
            if len(coords) == 4:
                # Check if all coordinates are simple values like 0.1, 0.2, etc.
                try:
                    float_coords = [float(c.strip()) for c in coords]
                    # If all coordinates are simple round numbers, likely a placeholder
                    if all(c in [0.1, 0.2, 0.3, 0.4, 0.5] for c in float_coords):
                        return True
                except ValueError:
                    return True
            return False
        
        def update_with_bbox(data, bbox_dict, page_dict, prefix=""):
            if isinstance(data, dict):
                for key, value in data.items():
                    if isinstance(value, dict):
                        # Try to find bounding box with prefix first, then without prefix
                        bbox_key = None
                        bbox_coords = None
                        
                        # Try prefixed key (for portfolio items)
                        prefixed_key = f"{prefix}{key}" if prefix else None
                        if prefixed_key and prefixed_key in bbox_dict:
                            bbox_key = prefixed_key
                            bbox_coords = bbox_dict[prefixed_key]
                        # Try key without prefix (for backward compatibility)
                        elif key in bbox_dict:
                            bbox_key = key
                            bbox_coords = bbox_dict[key]
                        
                        if bbox_key and bbox_coords is not None:
                            # Get the bounding box coordinates
                            
                            # Handle both string and list formats
                            if isinstance(bbox_coords, str):
                                coord_str = bbox_coords
                            elif isinstance(bbox_coords, list) and len(bbox_coords) > 0:
                                coord_str = bbox_coords[0] if isinstance(bbox_coords[0], str) else str(bbox_coords[0])
                            else:
                                # No valid coordinates
                                value["BoundingBox"] = None
                                continue
                            
                            # Check if this is a placeholder coordinate
                            if is_placeholder_coordinate(coord_str):
                                value["BoundingBox"] = None
                                value["PageNumber"] = None
                                continue
                            
                            # Process valid coordinate string
                            if isinstance(bbox_coords, list):
                                # Multiple coordinate sets (for multi-word fields)
                                valid_coords = []
                                for coord in bbox_coords:
                                    if isinstance(coord, str) and not is_placeholder_coordinate(coord):
                                        valid_coords.append(coord)
                                
                                if valid_coords:
                                    value["BoundingBox"] = valid_coords
                                    if bbox_key in page_dict:
                                        value["PageNumber"] = page_dict[bbox_key]
                                else:
                                    value["BoundingBox"] = None
                                    value["PageNumber"] = None
                            else:
                                # Single coordinate string
                                if not is_placeholder_coordinate(coord_str):
                                    value["BoundingBox"] = [coord_str]
                                    if bbox_key in page_dict:
                                        value["PageNumber"] = page_dict[bbox_key]
                                else:
                                    value["BoundingBox"] = None
                                    value["PageNumber"] = None
                        else:
                            # Check if this field has existing placeholder bounding box values
                            if "BoundingBox" in value and isinstance(value["BoundingBox"], list):
                                bbox_array = value["BoundingBox"]
                                if (len(bbox_array) == 4 and 
                                    all(isinstance(x, str) for x in bbox_array) and
                                    any(placeholder in str(bbox_array) for placeholder in ["x1", "y1", "x2", "y2"])):
                                    # This is a placeholder - set to null since no real coordinates found
                                    value["BoundingBox"] = None
                                    value["PageNumber"] = None
                            
                            # Recursively update nested structures
                            update_with_bbox(value, bbox_dict, page_dict, f"{prefix}{key}_")
                    elif isinstance(value, list):
                        for item in value:
                            update_with_bbox(item, bbox_dict, page_dict, prefix)
        
        # Create a copy to avoid modifying original data
        result = copy.deepcopy(extracted_data)
        
        # Update the data structure
        if "entities" in result:
            for entity in result["entities"]:
                if "portfolio" in entity:
                    for idx, portfolio_item in enumerate(entity["portfolio"]):
                        # Use InvestorRefID if available, otherwise use index
                        investor_id = None
                        if isinstance(portfolio_item, dict) and "InvestorRefID" in portfolio_item:
                            investor_ref = portfolio_item["InvestorRefID"]
                            if isinstance(investor_ref, dict) and "Value" in investor_ref:
                                investor_id = investor_ref["Value"]
                        
                        # Create unique prefix for each investor (matching the extraction prefix)
                        prefix = f"investor_{investor_id}_{idx}_" if investor_id else f"investor_{idx}_"
                        update_with_bbox(
                            portfolio_item, 
                            bbox_data["BoundingBox"], 
                            bbox_data.get("PageNumber", {}),
                            prefix
                        )
        
        return result
    
    def __call__(self, doc_payload: AithonDocument) -> AithonDocument:
        """
        Main bounding box extraction pipeline.
        """
        start_time = time.time()
        
        logging.info(f"Entering Bounding Box extraction for: {doc_payload.original_filename}")
        doc_payload.pipeline_status = "BoundingBox_Processing"
        
        # Validate prerequisites
        if not doc_payload.extracted_data:
            logging.warning("No extracted data available for bounding box processing")
            doc_payload.pipeline_status = "BoundingBox_Skipped"
            return doc_payload
        
        if not hasattr(doc_payload, 'file_path') or not doc_payload.file_path:
            logging.error("File path required for bounding box extraction")
            doc_payload.error_message = "File path required for bounding box extraction"
            doc_payload.pipeline_status = "Failed_BoundingBox"
            return doc_payload
        
        try:
            # Extract verbatim text from extracted data
            verbatim_text = self._extract_verbatim_text(doc_payload.extracted_data)
            
            if not verbatim_text:
                logging.warning("No verbatim text found in extracted data")
                doc_payload.pipeline_status = "BoundingBox_Skipped"
                return doc_payload
            
            logging.info(f"Extracted {len(verbatim_text)} verbatim text entries")
            
            # Run async operations
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            
            try:
                # Perform OCR extraction
                ocr_data = loop.run_until_complete(
                    self._perform_ocr_extraction(doc_payload.file_path)
                )
                
                # Get initial bounding boxes using rule-based matching
                initial_bbox = self.bbox_service.find_bounding_box(verbatim_text, ocr_data)
                
                # Get encoded images for LLM processing
                encoded_images = loop.run_until_complete(
                    self._get_encoded_images_from_pdf(doc_payload.file_path)
                )
                
                # Use LLM to refine bounding boxes
                llm_bbox = loop.run_until_complete(
                    self._extract_bounding_boxes_with_llm(verbatim_text, ocr_data, encoded_images)
                )
                
                # Merge results
                final_bbox = self._merge_bounding_boxes(initial_bbox, llm_bbox)
                
            finally:
                loop.close()
            
            # Integrate bounding boxes into extracted data
            updated_extracted_data = self._integrate_bounding_boxes(
                doc_payload.extracted_data, 
                final_bbox
            )
            
            # Update document payload
            doc_payload.extracted_data = updated_extracted_data
            doc_payload.pipeline_status = "BoundingBox_Completed"
            
            # Update metadata
            processing_time = time.time() - start_time
            doc_payload.metadata.update({
                "bounding_box_processing_time": processing_time,
                "bounding_box_entries_processed": len(verbatim_text),
                "bounding_box_entries_found": len(final_bbox.get("BoundingBox", {})),
                "bounding_box_method": "hybrid_ocr_llm"
            })
            
            logging.info(f"Bounding box extraction completed for '{doc_payload.original_filename}': "
                        f"Processed {len(verbatim_text)} entries, "
                        f"Found {len(final_bbox.get('BoundingBox', {}))} bounding boxes, "
                        f"Time: {processing_time:.2f}s")
            
        except Exception as e:
            logging.error(f"Bounding box extraction failed for {doc_payload.original_filename}: {e}", exc_info=True)
            doc_payload.error_message = f"Bounding box extraction failed: {e}"
            doc_payload.pipeline_status = "Failed_BoundingBox"
            
            # Store error metadata
            doc_payload.metadata.update({
                "bounding_box_error": str(e),
                "bounding_box_processing_time": time.time() - start_time
            })
        
        return doc_payload 
import logging
import re
import unicodedata
from typing import Dict, List, Optional, Tuple, Any
from ..data_model import AithonDocument

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class PreprocessingBox:
    """
    Advanced text preprocessing and standardization box that cleans and normalizes
    text extracted by OCR. Incorporates best practices from the bot_service including
    Unicode normalization, document structure preservation, and financial document
    specific cleaning patterns.
    """
    
    def __init__(self):
        # Common OCR artifacts and their corrections
        self.ocr_corrections = {
            # Common OCR misreads
            'l': 'I',  # lowercase l often misread as I
            'O': '0',  # uppercase O often misread as 0 in numbers
            '|': 'I',  # pipe character often misread as I
            '¢': 'c',  # cent symbol misread
            '£': 'E',  # pound symbol misread
            '§': 'S',  # section symbol misread
            '¤': 'o',  # generic currency symbol
            '¥': 'Y',  # yen symbol
            '€': 'E',  # euro symbol
            '®': 'R',  # registered trademark
            '©': 'C',  # copyright symbol
            '™': 'TM', # trademark symbol
            '°': 'o',  # degree symbol
            '±': '+/-', # plus-minus symbol
            '×': 'x',  # multiplication symbol
            '÷': '/',  # division symbol
            '¼': '1/4', # quarter fraction
            '½': '1/2', # half fraction
            '¾': '3/4', # three-quarters fraction
        }
        
        # Financial document specific patterns
        self.financial_patterns = {
            # Currency symbols and their standardization
            'currency_symbols': {
                '$': 'USD',
                '€': 'EUR',
                '£': 'GBP',
                '¥': 'JPY',
                '₹': 'INR',
                '₽': 'RUB',
                '₩': 'KRW',
                '₨': 'PKR',
                '₪': 'ILS',
                '₫': 'VND',
                '₡': 'CRC',
                '₦': 'NGN',
                '₵': 'GHS',
                '₴': 'UAH',
                '₸': 'KZT',
                '₼': 'AZN',
                '₾': 'GEL',
                '＄': 'USD',  # Full-width dollar sign
            },
            
            # Common financial abbreviations
            'financial_abbreviations': {
                'K': '000',
                'M': '000000',
                'B': '000000000',
                'T': '000000000000',
                'k': '000',
                'm': '000000',
                'b': '000000000',
                't': '000000000000',
            }
        }
        
        # Document structure patterns
        self.structure_patterns = {
            # Headers and footers patterns
            'header_footer_patterns': [
                r'Page\s+\d+\s+of\s+\d+',
                r'Page\s+\d+',
                r'^\d+\s*$',  # Standalone page numbers
                r'Confidential\s*$',
                r'Private\s*$',
                r'Internal\s*$',
                r'Draft\s*$',
                r'Preliminary\s*$',
                r'Not\s+for\s+Distribution\s*$',
                r'For\s+Internal\s+Use\s+Only\s*$',
            ],
            
            # Table artifacts
            'table_artifacts': [
                r'^\s*\|\s*$',  # Standalone table separators
                r'^\s*\+[-=]+\+\s*$',  # Table borders
                r'^\s*[-=]{3,}\s*$',  # Horizontal lines
                r'^\s*[|+\-=\s]+\s*$',  # Mixed table characters
            ],
            
            # OCR line break artifacts
            'line_break_artifacts': [
                r'([a-z])-\s*\n\s*([a-z])',  # Hyphenated words split across lines
                r'([A-Z][a-z]+)\s*\n\s*([a-z]+)',  # Proper nouns split across lines
            ]
        }

    def normalize_unicode(self, text: str) -> str:
        """
        Normalize Unicode characters to a consistent form.
        Uses NFKD normalization to handle various Unicode representations.
        """
        if not text:
            return ""
        return unicodedata.normalize("NFKD", text)

    def normalize_document_structure(self, text: str) -> str:
        """
        Normalize document text by handling line breaks and paragraph structure.
        Preserves intentional paragraph breaks while joining mid-sentence breaks.
        """
        if not text:
            return ""
        
        # Replace newlines that don't have two consecutive newlines with a space
        # This preserves paragraph breaks while joining mid-sentence line breaks
        normalized_text = re.sub(r"(?<!\n)\n(?!\n)", " ", text)
        
        # Remove extra spaces but preserve intentional spacing
        normalized_text = re.sub(r"[ \t]+", " ", normalized_text)
        
        # Clean up multiple newlines to maximum of 2 (paragraph break)
        normalized_text = re.sub(r"\n{3,}", "\n\n", normalized_text)
        
        return normalized_text

    def fix_ocr_artifacts(self, text: str) -> str:
        """
        Fix common OCR artifacts and misreads based on context.
        """
        if not text:
            return ""
        
        # Apply OCR corrections
        for artifact, correction in self.ocr_corrections.items():
            text = text.replace(artifact, correction)
        
        # Fix common OCR patterns
        # Fix numbers with misread characters
        text = re.sub(r'(\d)[lI](\d)', r'\1 1\2', text)  # Fix 1 misread as l or I in numbers
        text = re.sub(r'(\d)[O](\d)', r'\1 0\2', text)   # Fix 0 misread as O in numbers
        
        # Fix decimal points
        text = re.sub(r'(\d)\s*[,;:]\s*(\d{2})\b', r'\1.\2', text)  # Fix decimal separators
        
        # Fix percentage signs
        text = re.sub(r'(\d)\s*[%℅]\s*', r'\1% ', text)
        
        # Fix currency amounts
        text = re.sub(r'(\$|USD|EUR|GBP)\s*(\d)', r'\1 \2', text)
        
        return text

    def clean_financial_text(self, text: str) -> str:
        """
        Apply financial document specific cleaning patterns.
        """
        if not text:
            return ""
        
        # Normalize currency symbols
        for symbol, code in self.financial_patterns['currency_symbols'].items():
            text = text.replace(symbol, f' {code} ')
        
        # Clean up financial numbers
        # Remove unnecessary commas in numbers
        text = re.sub(r'(\d),(\d{3})', r'\1\2', text)
        
        # Standardize percentage notation
        text = re.sub(r'(\d+\.?\d*)\s*%', r'\1 percent', text)
        
        # Clean up common financial abbreviations
        for abbr, expansion in self.financial_patterns['financial_abbreviations'].items():
            # Only replace if it's at the end of a number
            text = re.sub(rf'(\d+\.?\d*)\s*{re.escape(abbr)}\b', rf'\1{expansion}', text)
        
        return text

    def remove_document_artifacts(self, text: str) -> str:
        """
        Remove common document artifacts like headers, footers, and table remnants.
        """
        if not text:
            return ""
        
        lines = text.split('\n')
        cleaned_lines = []
        
        for line in lines:
            line = line.strip()
            
            # Skip empty lines
            if not line:
                cleaned_lines.append(line)
                continue
            
            # Check if line matches any artifact pattern
            is_artifact = False
            
            # Check header/footer patterns
            for pattern in self.structure_patterns['header_footer_patterns']:
                if re.match(pattern, line, re.IGNORECASE):
                    is_artifact = True
                    break
            
            # Check table artifacts
            if not is_artifact:
                for pattern in self.structure_patterns['table_artifacts']:
                    if re.match(pattern, line):
                        is_artifact = True
                        break
            
            # Keep line if it's not an artifact
            if not is_artifact:
                cleaned_lines.append(line)
        
        return '\n'.join(cleaned_lines)

    def fix_line_breaks(self, text: str) -> str:
        """
        Fix hyphenated words and other line break artifacts.
        """
        if not text:
            return ""
        
        # Fix hyphenated words split across lines
        text = re.sub(r'([a-z])-\s*\n\s*([a-z])', r'\1\2', text)
        
        # Fix proper nouns split across lines
        text = re.sub(r'([A-Z][a-z]+)\s*\n\s*([a-z]+)', r'\1\2', text)
        
        # Fix sentences split across lines (common in financial documents)
        text = re.sub(r'([a-z,])\s*\n\s*([a-z])', r'\1 \2', text)
        
        return text

    def standardize_whitespace(self, text: str) -> str:
        """
        Standardize whitespace throughout the document.
        """
        if not text:
            return ""
        
        # Replace multiple spaces, tabs with single space
        text = re.sub(r'[ \t]+', ' ', text)
        
        # Remove trailing whitespace from lines
        text = re.sub(r'[ \t]+$', '', text, flags=re.MULTILINE)
        
        # Remove leading whitespace from lines (but preserve some structure)
        text = re.sub(r'^[ \t]+', '', text, flags=re.MULTILINE)
        
        # Remove leading/trailing whitespace from entire text
        text = text.strip()
        
        return text

    def extract_and_preserve_structure(self, text: str) -> Tuple[str, Dict[str, List[str]]]:
        """
        Extract and preserve important document structure elements.
        Returns cleaned text and a dictionary of preserved elements.
        """
        if not text:
            return "", {}
        
        preserved = {
            'tables': [],
            'lists': [],
            'headers': [],
            'dates': [],
            'amounts': [],
            'addresses': []
        }
        
        # Extract tables (basic pattern matching)
        table_pattern = r'(\|[^|\n]+\|(?:\n\|[^|\n]+\|)*)'
        tables = re.findall(table_pattern, text)
        preserved['tables'] = tables
        
        # Extract lists
        list_pattern = r'((?:^\s*[-*•]\s+.+(?:\n|$))+)'
        lists = re.findall(list_pattern, text, re.MULTILINE)
        preserved['lists'] = lists
        
        # Extract dates
        date_patterns = [
            r'\b\d{1,2}[/-]\d{1,2}[/-]\d{2,4}\b',
            r'\b\d{1,2}\s+(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\s+\d{2,4}\b',
            r'\b(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\s+\d{1,2},?\s+\d{2,4}\b'
        ]
        for pattern in date_patterns:
            dates = re.findall(pattern, text, re.IGNORECASE)
            preserved['dates'].extend(dates)
        
        # Extract monetary amounts
        amount_pattern = r'[$€£¥₹]\s*\d+(?:,\d{3})*(?:\.\d{2})?'
        amounts = re.findall(amount_pattern, text)
        preserved['amounts'] = amounts
        
        return text, preserved

    def _clean_text(self, text: str) -> str:
        """
        Apply comprehensive text cleaning pipeline.
        """
        if not text:
            return ""
        
        # Step 1: Unicode normalization
        text = self.normalize_unicode(text)
        
        # Step 2: Fix OCR artifacts
        text = self.fix_ocr_artifacts(text)
        
        # Step 3: Fix line breaks
        text = self.fix_line_breaks(text)
        
        # Step 4: Normalize document structure
        text = self.normalize_document_structure(text)
        
        # Step 5: Clean financial text
        text = self.clean_financial_text(text)
        
        # Step 6: Remove document artifacts
        text = self.remove_document_artifacts(text)
        
        # Step 7: Standardize whitespace
        text = self.standardize_whitespace(text)
        
        return text

    def validate_cleaning_quality(self, original_text: str, cleaned_text: str) -> Dict[str, Any]:
        """
        Validate the quality of text cleaning by comparing original and cleaned text.
        Returns metrics and warnings about the cleaning process.
        """
        if not original_text or not cleaned_text:
            return {
                'quality_score': 0.0,
                'warnings': ['Empty text provided'],
                'metrics': {}
            }
        
        metrics = {
            'original_length': len(original_text),
            'cleaned_length': len(cleaned_text),
            'length_reduction_ratio': (len(original_text) - len(cleaned_text)) / len(original_text),
            'word_count_original': len(original_text.split()),
            'word_count_cleaned': len(cleaned_text.split()),
            'line_count_original': len(original_text.split('\n')),
            'line_count_cleaned': len(cleaned_text.split('\n')),
        }
        
        warnings = []
        
        # Check for excessive length reduction
        if metrics['length_reduction_ratio'] > 0.5:
            warnings.append(f"High text reduction: {metrics['length_reduction_ratio']:.2%}")
        
        # Check for excessive word loss
        word_loss_ratio = (metrics['word_count_original'] - metrics['word_count_cleaned']) / metrics['word_count_original']
        if word_loss_ratio > 0.3:
            warnings.append(f"High word loss: {word_loss_ratio:.2%}")
        
        # Calculate quality score (simple heuristic)
        quality_score = 1.0
        if metrics['length_reduction_ratio'] > 0.3:
            quality_score -= 0.2
        if word_loss_ratio > 0.2:
            quality_score -= 0.3
        if len(warnings) > 2:
            quality_score -= 0.2
        
        quality_score = max(0.0, min(1.0, quality_score))
        
        return {
            'quality_score': quality_score,
            'warnings': warnings,
            'metrics': metrics
        }

    def __call__(self, doc_payload: AithonDocument) -> AithonDocument:
        """
        Process the document through the enhanced preprocessing pipeline.
        """
        logging.info(f"Entering Enhanced Preprocessing Box for: {doc_payload.original_filename}")
        doc_payload.pipeline_status = "Preprocessing"
        
        try:
            # Process main document text
            if doc_payload.raw_text:
                # Extract and preserve structure
                text, preserved_structure = self.extract_and_preserve_structure(doc_payload.raw_text)
                
                # Clean the text
                cleaned_text = self._clean_text(text)
                
                # Validate cleaning quality
                quality_metrics = self.validate_cleaning_quality(doc_payload.raw_text, cleaned_text)
                
                # Store results
                doc_payload.cleaned_text = cleaned_text
                
                # Store quality metrics and preserved structure in metadata
                doc_payload.metadata.update({
                    'preprocessing_quality': quality_metrics,
                    'preserved_structure': preserved_structure,
                    'cleaning_timestamp': logging.Formatter().formatTime(logging.LogRecord(
                        name='', level=0, pathname='', lineno=0, msg='', args=(), exc_info=None
                    ))
                })
                
                # Log warnings if any
                if quality_metrics['warnings']:
                    logging.warning(f"Preprocessing warnings for {doc_payload.original_filename}: {quality_metrics['warnings']}")
                
                logging.info(f"Text cleaning quality score: {quality_metrics['quality_score']:.2f}")
            
            # Process individual pages
            for page in doc_payload.pages:
                if page.raw_text:
                    # Store original text if not already stored
                    if not page.raw_text:
                        page.raw_text = page.text
                    
                    # Clean page text
                    page.text = self._clean_text(page.text)
                elif page.text:
                    # If no raw_text, use current text as raw and clean it
                    page.raw_text = page.text
                    page.text = self._clean_text(page.text)
            
            doc_payload.pipeline_status = "Preprocessing_Completed"
            logging.info(f"Successfully processed through Enhanced Preprocessing Box: {doc_payload.original_filename}")
            
        except Exception as e:
            error_msg = f"Error in preprocessing: {str(e)}"
            logging.error(error_msg, exc_info=True)
            doc_payload.pipeline_status = "Preprocessing_Failed"
            doc_payload.error_message = error_msg
            
            # Store error details in metadata
            doc_payload.metadata['preprocessing_error'] = {
                'error_type': type(e).__name__,
                'error_message': str(e),
                'timestamp': logging.Formatter().formatTime(logging.LogRecord(
                    name='', level=0, pathname='', lineno=0, msg='', args=(), exc_info=None
                ))
            }
        
        return doc_payload 
    

    
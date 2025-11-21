# BoundingBoxBox - Advanced Bounding Box Extraction

## Overview

The `BoundingBoxBox` is a sophisticated document processing component that extracts precise bounding box coordinates for text elements in PDF documents. It combines OCR technology with LLM-based refinement to generate accurate coordinate data for downstream applications.

## Key Features

### 🎯 **Hybrid Processing Approach**
- **OCR-based Initial Extraction**: Uses Tesseract OCR to extract text with coordinates
- **LLM-based Refinement**: Employs GPT-4 Vision to correct and validate bounding boxes
- **Intelligent Merging**: Combines OCR and LLM results for optimal accuracy

### 🔍 **Advanced Matching Logic**
- **Single-word Matching**: Exact character-by-character matching for single words
- **Multi-word Sequence Matching**: Intelligent sequence detection for phrases
- **Currency Approximation**: Special handling for monetary values with symbols

### 📊 **Comprehensive Output**
- **Normalized Coordinates**: All coordinates are normalized (0-1 range)
- **Page Number Tracking**: Accurate page number association
- **Schema Compliance**: Outputs match the required JSON schema format

## How It Works

### 1. Input Processing
```python
# Extracts verbatim text from extracted data
verbatim_text = {
    "Investor": "Matthew R. Bogart Revocable Trust",
    "Distribution": "42109.57",
    "Currency": "USD"
}
```

### 2. OCR Extraction
```python
# Converts PDF to images and extracts text with coordinates
ocr_data = tesseract.image_to_data(image, output_type=DATAFRAME)
# Normalizes coordinates based on image dimensions
ocr_data["left"] = ocr_data["left"] / img_width
```

### 3. Initial Matching
- **Single words**: Direct text matching with OCR results
- **Multi-word phrases**: Sequence matching across OCR tokens
- **Currency values**: Special handling for monetary amounts

### 4. LLM Refinement
```python
# Uses GPT-4 Vision to validate and correct bounding boxes
response = client.chat.completions.create(
    model="gpt-4o",
    messages=[
        {"role": "system", "content": "Bounding box extraction expert..."},
        {"role": "user", "content": [
            {"type": "text", "text": prompt},
            {"type": "image_url", "image_url": {"url": f"data:image/jpeg;base64,{img}"}}
        ]}
    ]
)
```

### 5. Integration
- Merges OCR and LLM results
- Integrates coordinates back into extracted data structure
- Maintains schema compliance

## Configuration

### BoundingBoxConfig Options

```python
@dataclass
class BoundingBoxConfig:
    # OCR settings
    dpi: int = 400                    # Image resolution for OCR
    use_pdftocairo: bool = True       # Use pdftocairo for better quality
    thread_count: int = 1             # OCR processing threads
    
    # LLM settings
    model_name: str = "gpt-4o"        # Vision model for refinement
    temperature: float = 0.0          # LLM temperature
    max_tokens: int = 4000            # Max response tokens
    api_timeout: int = 120            # API timeout in seconds
    
    # Processing settings
    max_retries: int = 3              # Retry attempts
    normalize_coordinates: bool = True # Normalize to 0-1 range
    
    # Quality settings
    min_confidence: float = 30.0      # Minimum OCR confidence
    enable_detailed_logging: bool = True
```

## Output Format

### Expected Output Structure

```json
{
    "BoundingBox": {
        "Investor": "0.134,0.184,0.036,0.009",
        "Distribution": "0.718,0.538,0.044,0.007",
        "Currency": "0.907,0.349,0.024,0.007"
    },
    "PageNumber": {
        "Investor": 1,
        "Distribution": 1,
        "Currency": 1
    }
}
```

### Coordinate Format
- **Format**: `"left,top,width,height"`
- **Range**: All values normalized between 0 and 1
- **Multi-word**: Concatenated coordinates for phrases
- **Example**: `"0.134,0.184,0.036,0.009,0.177,0.184,0.047,0.011"`

## Integration with Pipeline

### Pipeline Position
The BoundingBoxBox is positioned between **Extraction** and **Validation & Enrichment**:

```
Ingestion → OCR → Preprocessing → Classification → Extraction → BoundingBox → Validation → Output
```

### Prerequisites
- ✅ Extracted data must be available
- ✅ File path must be accessible
- ✅ Tesseract OCR must be installed
- ✅ OpenAI API key must be configured

### Error Handling
- **Graceful Degradation**: Pipeline continues even if bounding box extraction fails
- **Detailed Logging**: Comprehensive error reporting and debugging info
- **Retry Logic**: Automatic retry with exponential backoff for transient failures

## Performance Considerations

### Processing Time
- **OCR Phase**: ~2-5 seconds per page
- **LLM Phase**: ~10-30 seconds depending on complexity
- **Total**: Typically 15-60 seconds per document

### Resource Usage
- **Memory**: ~200-500MB per document (depends on image size)
- **CPU**: Intensive during OCR processing
- **API Calls**: 1 call per document to OpenAI

### Optimization Tips
1. **Reduce DPI**: Lower DPI (200-300) for faster processing
2. **Batch Processing**: Process multiple documents in sequence
3. **Caching**: Enable caching for repeated documents
4. **Threading**: Use multiple threads for OCR (with caution)

## Troubleshooting

### Common Issues

#### 1. Tesseract Not Found
```bash
# Install Tesseract
sudo apt-get install tesseract-ocr  # Ubuntu/Debian
brew install tesseract              # macOS
```

#### 2. Poor OCR Quality
- Increase DPI (400-600)
- Ensure high-quality source PDFs
- Check image preprocessing settings

#### 3. LLM Timeout
- Increase `api_timeout` in config
- Reduce image resolution
- Check network connectivity

#### 4. Memory Issues
- Reduce image DPI
- Process documents sequentially
- Monitor memory usage

### Debug Mode
```python
# Enable detailed logging
config = BoundingBoxConfig(enable_detailed_logging=True)
bbox_box = BoundingBoxBox(config)
```

## Testing

### Run Tests
```bash
# Run the test script
python test_bounding_box.py

# Check output
cat output_documents/sample_output.json | jq '.extracted_data.entities[0].portfolio[0].Investor.BoundingBox'
```

### Expected Test Results
- ✅ Real coordinate values (not placeholders)
- ✅ Proper normalization (0-1 range)
- ✅ Accurate page numbers
- ✅ Schema compliance

## Monitoring and Metrics

### Key Metrics
- `bounding_box_entries_processed`: Number of entries processed
- `bounding_box_entries_found`: Number of successful matches
- `bounding_box_processing_time`: Total processing time
- `bounding_box_success_total`: Success counter

### Performance Monitoring
```python
# Check processing stats
orchestrator = AithonOrchestrator()
stats = orchestrator.get_processing_stats()
print(f"Bounding box success rate: {stats['bounding_box_success_rate']}")
```

## Future Enhancements

### Planned Features
- [ ] **Multi-language Support**: OCR for non-English documents
- [ ] **Table Detection**: Specialized handling for tabular data
- [ ] **Confidence Scoring**: Quality metrics for bounding boxes
- [ ] **Batch Processing**: Efficient multi-document processing
- [ ] **Custom Models**: Fine-tuned models for specific document types

### Integration Opportunities
- [ ] **Document Viewer**: Visual bounding box overlay
- [ ] **Annotation Tools**: Manual correction interface
- [ ] **Quality Assurance**: Automated validation pipeline
- [ ] **Performance Analytics**: Detailed performance tracking

## Dependencies

### Core Requirements
- `pytesseract>=0.3.10` - OCR processing
- `pdf2image>=1.16.0` - PDF to image conversion
- `Pillow>=10.0.0` - Image processing
- `pandas>=2.0.0` - Data manipulation
- `openai>=1.0.0` - LLM integration
- `tenacity>=8.2.0` - Retry logic

### System Requirements
- **Tesseract OCR**: System-level installation required
- **Python**: 3.8+ recommended
- **Memory**: 2GB+ available RAM
- **Storage**: Temporary space for image processing

## Support

For issues, questions, or contributions:
1. Check the troubleshooting section above
2. Review the test script for usage examples
3. Enable detailed logging for debugging
4. Monitor system resources during processing

---

**Note**: This BoundingBoxBox represents a significant advancement over the previous placeholder-based approach, providing real, accurate bounding box coordinates for enhanced document processing capabilities. 
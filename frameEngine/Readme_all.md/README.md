# Aithon Frame RAMNARAYAN(rc)

This project is an implementation of the Aithon Box Architecture, a modular pipeline for converting unstructured data into structured JSON.

## Setup

1. **Create a virtual environment:**

   ```bash
   python3 -m venv venv
   source venv/bin/activate
   ```
2. **Install dependencies:**

   ```bash
   pip install -r requirements.txt
   ```
3. **Set up environment variables:**

   * Run the setup script with your OpenAI API key:
     ```bash
     python setup_project.py --api-key "YOUR_OPENAI_API_KEY"
     ```
   * This will generate the `.env` file for you.

## Usage

1. **Add documents:** Place your source PDF files into the `source_documents` directory.
2. **Run the pipeline:**

   ```bash
   python orchestrator.py
   ```
3. **Check the output:** The final structured JSON files will be created in the `output_documents` directory.


Details of each box:-
Box 1: Ingestion Box
Purpose: To be the formal entry gate into the Aithon pipeline. It takes a processing request, fetches the document from its source, creates the initial tracking records in the database, and places the document in a standardized "raw" storage location.
Relation to Codebase: This Box's logic is primarily derived from queue_processor.py and the ingest_documents_into_cloud and get_content_from_external_client_api functions within orchestrator/manager.py.
Inputs:
request_payload (JSON): A message payload from the service bus.
ext_doc_id: The external system's unique ID for the document.
document_name: The filename of the document.
password: An optional, encrypted password.
metadata: Any other relevant key-value data.
Processing Logic:
Receive Request: The Box is triggered by a message from the queue.
Check for Duplicates: Query the database using ext_doc_id to see if this document has been processed before (logic from document_rejection_check). If so, mark the task as a duplicate and halt.
Create DB Records:
Insert a new record into the documents table, generating a new internal doc_id.
Insert a new record into the tasks table with a pending status, linked to the doc_id.
Fetch Document: If ext_doc_id is present, call the external API to retrieve the document's binary content.
Store Raw Document: Upload the fetched document to a dedicated Azure Blob Storage container (e.g., aithon-raw-storage/). The blob name could be the doc_id or a UUID to prevent naming conflicts.
Prepare Initial Payload: Create the initial AithonDocument payload, populating it with the doc_id, task_id, storage path, and metadata.
Output:
An AithonDocument object with initial fields populated, ready to be passed to the next Box.
With the document now securely ingested, the next step is to extract its textual content. To design the OCR Box, I will now search the codebase for how OCR is currently implemented.
The search results reveal a sophisticated, multi-engine OCR strategy. The system intelligently chooses between different tools like Azure OCR, Tesseract, and a higher-level docling SDK. It also smartly distinguishes between text-based and image-based PDFs. This is a strong foundation to build upon.
Box 2: OCR Box
Purpose: To transform the raw document (PDF, image) into structured textual content. This box is a critical bridge from the file-based world to the text-based world, intelligently handling both scanned and digitally native documents.
Relation to Codebase: This Box is a logical wrapper around the functionalities found in ocr/manager.py (Azure OCR), tesseract/manager.py (Tesseract), and the document conversion parts of loader/manager.py. It unifies these disparate services under a single, consistent interface.
Inputs:
The AithonDocument payload from the Ingestion Box.
Processing Logic:
Download Document: Fetch the document file from the raw_storage_path in the payload to a temporary local directory.
Text Extraction Attempt: First, try to extract text directly using a library like PyMuPDF (as seen in loader/manager.py). This quickly identifies digitally native PDFs.
Branching Logic:
If substantial text is extracted: The document is treated as "digitally native." The extracted text is used directly.
If little or no text is extracted: The document is treated as "scanned" or image-based. The pipeline then proceeds with image-based OCR.
Image-Based OCR Workflow:
Convert to Images: If the input is a PDF, convert each page into a high-resolution image using the pdf2image library (as seen in tesseract/manager.py).
Perform OCR: For each image, use a primary OCR engine. Based on the code, Azure OCR (AzureOCRService) is a strong candidate for high-quality results. The Tesseract engine (TesseractManager) is an excellent fallback or can be used for specific tasks like getting detailed bounding boxes.
Structure the Output: The core goal is to populate the AithonDocument payload with rich data. The output from the OCR engine (both text and bounding boxes) is normalized.
Output:
The AithonDocument payload, now updated with:
raw_text: The full, concatenated text content of the document.
pages: A list, where each item is a dictionary representing a page and contains:
page_number: The 1-based index of the page.
text: The full text of that page.
words: A list of word-level objects, each containing its text and a normalized bounding_box [x0, y0, x1, y1].
Box 3: Pre-processing Box
Purpose: To clean and standardize the text extracted by the OCR Box. This step is vital for improving the reliability of all downstream AI tasks, as raw OCR output is often noisy.
Relation to Codebase: While no single file is named "pre-processing," this is a necessary architectural step that formalizes the clean-up logic that is often scattered around a project.
Inputs:
The AithonDocument payload from the OCR Box.
Processing Logic:
Text Normalization:
Remove excess whitespace, newlines, and tabs.
Standardize character encoding to UTF-8.
Correct common, systematic OCR errors (e.g., using regex to fix patterns like I being mistaken for 1).
Content Filtering:
Identify and remove elements that are irrelevant to the core data, such as page numbers, standard headers, and footers. This can be done by finding text that repeats across pages in similar locations (using bounding box data).
(Advanced) Layout Rectification:
For documents with complex layouts like multiple columns, use the bounding box information to re-order the text into a more natural reading flow. The docling SDK appears to have capabilities in this area that can be leveraged.
Output:
The AithonDocument payload with the raw_text and pages.text fields overwritten with the cleaned, processed text. The original OCR output should be archived in a separate metadata field for traceability.
The document text is now clean and ready for analysis. The next logical step is to determine its type. I will now search the codebase for classification logic to design the Classification Box.
The search results are definitive: your codebase already contains a mature, feature-rich classification system. The file production_classification_box.py is particularly impressive, with support for multiple LLMs, retry logic, and different classification modes. This is the perfect blueprint.
My design for the Classification Box will therefore be a formalization of this existing, proven architecture.
Box 4: Classification Box
Purpose: To analyze the document's content and assign it a specific category (e.g., "Statement," "CapCall," "Distribution"). This classification is the critical pivot point of the entire pipeline, determining which data schema and extraction logic to apply next.
Relation to Codebase: This design is heavily modeled on the excellent architecture found in production_classification_box.py. It encapsulates the core principles of LLM-powered, multi-modal, and database-driven classification.
Inputs:
The AithonDocument payload from the Pre-processing Box.
predefined_document_type (Optional): A string that can be passed in the initial metadata to bypass the classification logic.
Processing Logic:
Pre-classification Check: If the AithonDocument payload contains a predefined_document_type, this box's logic is skipped. The type is adopted with a confidence of 1.0, and the payload is passed directly to the next stage.
Modality Selection: The box intelligently decides how to "read" the document, a key feature from your existing code (langgraph_agent/manager.py).
If the cleaned text in the payload is substantial (e.g., > 100 characters), the modality is TEXTUAL.
If the text is sparse, the modality is VISION, and the box will use the document images.
Dynamic Type Loading: Instead of using a hardcoded list, the box queries the database to fetch the current list of supported document types and their descriptions (as seen in classification_box.py's use of get_doctype()). This makes the system incredibly flexible.
Prompt Engineering: A precise prompt is constructed for the LLM. It includes:
The list of possible document types fetched from the database.
The content to be analyzed (either the cleaned text for TEXTUAL mode or the page images for VISION mode).
Clear instructions to return only the name of the determined document type.
Execute with Resilience: The prompt is sent to a primary LLM (e.g., OpenAI's GPT-4o). The design incorporates the retry and fallback logic from production_classification_box.py, allowing it to try a secondary LLM (e.g., Gemini) if the primary one fails.
Validate and Record: The LLM's response is validated against the list of known types. The result is then added to the AithonDocument payload.
Output:
The AithonDocument payload, updated with:
document_type: The official classification (e.g., "Distribution").
confidence_score: A metric (0.0 to 1.0) indicating the classification confidence.
modality: The method used (TEXTUAL or VISION).
With the document's identity now known, the pipeline moves to its core purpose: extracting structured data. I will now search the codebase for entity and data extraction logic to design the Extraction Box.
The search results are incredibly insightful. They confirm that the extraction process is the most sophisticated part of the pipeline, relying heavily on dynamic, schema-driven prompting of LLMs.
Here are the key takeaways that will shape the design of the final boxes:
Schema-Driven Extraction: This is the central pillar of the extraction process. The orchestrator/manager.py explicitly fetches an extraction_schema from the database based on the document_type. This schema is then used to instruct the LLM on exactly what to find. This is a best-in-class design for adaptable AI systems.
LangGraph Agent: The use of a langgraph_agent reveals a stateful, agentic workflow. The agent's Router node intelligently selects different extraction methods (vision_docling_extraction, long_form_extraction) based on the document type, which allows for highly specialized handling of different documents.
The PromptingBox Pattern: The existence of prompting_box.py is a perfect illustration of the modular architecture you envisioned. It's a dedicated component whose sole responsibility is to assemble the complex prompt payload for the LLM, combining schemas, templates, and document content.
Post-Extraction Bounding Box Search: Extraction is a two-step process. First, the LLM extracts the textual data. Then, the GetBoundingBox node in the agent takes that extracted text and searches the original OCR data to find its coordinates on the page. This is a crucial feature for auditability.
Database as the Source of Truth: The orchestrator/db_models.py file shows that not only are the schemas stored in the database, but the final extracted data is also written back to dynamic tables named after the document type.
This detailed understanding allows me to finalize the design of the core pipeline.
Box 5: Extraction Box
Purpose: To perform the primary "magic" of the pipeline: transforming the document's unstructured content into a structured JSON object based on a predefined set of target fields.
Relation to Codebase: This Box is a direct architectural representation of the schema-driven logic found in orchestrator/manager.py and the LLM-powered agentic workflow from langgraph_agent/manager.py and prompting_box.py.
Inputs:
The AithonDocument payload from the Classification Box (which contains the crucial document_type).
Processing Logic:
Fetch Extraction Schema: Using the document_type from the payload, query the database to retrieve the corresponding extraction JSON schema. This schema is the blueprint for the data to be extracted.
Select Extraction Strategy: Based on the document_type or other metadata, select the appropriate extraction agent (e.g., the standard vision extractor, or a specialized one for long-form documents like "BrokerageMSBilling").
Construct LLM Prompt: This is the most critical step. Assemble a detailed prompt for a multi-modal LLM (like GPT-4o). The prompt must include:
A clear system instruction defining the LLM's role (e.g., "You are an expert data extractor. You must respond only with a valid JSON object that conforms to the provided schema.").
The JSON schema itself, embedded directly in the prompt.
The document content (the page images from the OCR Box).
Execute Extraction: Send the prompt to the LLM and parse its JSON response.
Output:
The AithonDocument payload with the extracted_data field now populated with the raw JSON object returned by the LLM.
Box 6: Validation & Enrichment Box
Purpose: To clean, verify, and enrich the raw data returned by the LLM. It ensures data quality and adds crucial traceability information (the bounding boxes).
Relation to Codebase: This Box formalizes the logic found in the GetBoundingBox graph node and the implicit need for data validation that comes with using LLMs.
Inputs:
The AithonDocument payload from the Extraction Box.
Processing Logic:
Schema Validation: Validate the LLM's raw JSON output against the official schema fetched from the database. This catches common errors like missing fields or incorrect data types. Any violations are recorded in the validation_errors field of the payload.
Data Cleansing: Perform type conversions (e.g., converting date strings to a standard format, number strings to floats) and other cleanup operations as defined in the schema.
Bounding Box Enrichment: For each field in the extracted_data, take its value and perform a fuzzy search against the detailed word-level data from the OCR Box. When a match is found, retrieve its coordinates and add the bounding_box and page_number to that field in the extracted_data.
Output:
The AithonDocument payload where the extracted_data is now clean, validated, and enriched with location information.
Box 7: Output Box
Purpose: To finalize the structured data, persist it to the appropriate database tables, and signal the completion of the pipeline.
Relation to Codebase: This encapsulates the final database insertion and status update logic seen at the end of the orchestrator/manager.py workflow.
Inputs:
The fully processed AithonDocument payload from the Validation & Enrichment Box.
Processing Logic:
Finalize JSON: Prepare the final, clean JSON object, stripping away any intermediate processing fields (like confidence scores or validation errors) to create a clean data record.
Persist to Database: Save the final extracted data into the database. Following the existing pattern, this would involve writing to a table named dynamically based on the document_type.
Update Task Status: Update the central tasks table, marking this job's status as Completed.
Notify Consumers (Optional): Send a notification (e.g., via a webhook or another service bus message) to any downstream systems that need to act on the newly structured data.
Output:
A final, clean, and validated JSON object representing the structured data from the source document.

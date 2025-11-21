from pathlib import Path
import logging
from typing import Union
from ..data_model import AithonDocument


# we need more detailed design of this box as of now this is simple box which just take the file and create the document payload 

# Configure logging
# logging.basicConfig(level=logging.WARNING, format='%(asctime)s - %(levelname)s - %(message)s')

class IngestionBox:
    """
    The first box in the Aithon pipeline.
    It takes a file path or AithonDocument, validates it, and creates/updates the AithonDocument payload.
    """

    def __call__(self, input_data: Union[Path, AithonDocument]) -> AithonDocument:
        """
        Processes the input file path or AithonDocument and creates/updates an AithonDocument.

        Args:
            input_data: Either a Path to the source document or an existing AithonDocument.

        Returns:
            An initialized or updated AithonDocument object.
            
        Raises:
            FileNotFoundError: If the provided file_path does not exist.
        """
        
        # Handle different input types
        if isinstance(input_data, AithonDocument):
            # If we receive an AithonDocument, validate and update it
            document = input_data
            file_path = document.source_path
            logging.info(f"Starting ingestion for existing document: {document.original_filename}")
        else:
            # If we receive a Path, create a new document
            file_path = input_data
            logging.info(f"Starting ingestion for: {file_path.name}")
            
            if not file_path.exists() or not file_path.is_file():
                logging.error(f"File not found or is not a file: {file_path}")
                raise FileNotFoundError(f"The source file does not exist: {file_path}")

            # Create the initial document payload
            document = AithonDocument(
                source_path=file_path,
                original_filename=file_path.name,
                pipeline_status="Ingested"
            )

        # Update pipeline status
        document.pipeline_status = "Ingested"
        
        logging.info(f"Successfully ingested '{document.original_filename}'. Document ID: {document.doc_id}")
        
        return document 
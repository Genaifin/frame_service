#!/usr/bin/env python3
"""
Seeder script for document_configuration table
Reads JSON files from AithonFrontend and ingests them into the document_configuration table
"""

import os
import json
import re
from database_models import DatabaseManager
from sqlalchemy import text
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DocumentConfigurationSeeder:
    """Seeder for document_configuration table"""
    
    def __init__(self, db_manager: DatabaseManager = None):
        self.db_manager = db_manager or DatabaseManager()
        # Path to the JSON files directory (relative to project root)
        script_dir = os.path.dirname(os.path.abspath(__file__))
        self.json_files_path = os.path.join(script_dir, "data", "document_configurations")
        
        # Map of JSON filenames to expected document types
        self.file_mapping = {
            "CapitalCall.json": "Capital Call",
            "Distributions.json": "Distribution",
            "NAVStatement.json": "NAVStatement",
            "Brokerage_Statement.json": "Brokerage Statement",
            "k-1_,_1065_(_for_a_partnership).json": "k-1 , 1065 ( for a partnership)",
            "Private_Placement_Memorandum_(PPM).json": "Private Placement Memorandum (PPM)"
        }
    
    def _parse_sla(self, sla_string: str) -> int:
        """
        Parse SLA string like "T + 0 Days", "T + 1 Days", etc. to integer
        Returns the number of days as integer
        """
        if not sla_string:
            return None
        
        # Extract number from strings like "T + 0 Days", "T + 1 Days", "T +2 Days", etc.
        match = re.search(r'\+?\s*(\d+)', sla_string)
        if match:
            return int(match.group(1))
        
        logger.warning(f"Could not parse SLA: {sla_string}, defaulting to None")
        return None
    
    def _load_json_file(self, file_path: str) -> dict:
        """Load and parse JSON file"""
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"Error loading JSON file {file_path}: {e}")
            raise
    
    def _extract_document_data(self, json_data: dict) -> dict:
        """
        Extract document configuration data from JSON structure
        Expected structure: {"response": [{"document_type": "...", "description": "...", "sla": "...", "schema_blob": {...}}]}
        """
        if "response" not in json_data or not json_data["response"]:
            raise ValueError("Invalid JSON structure: missing 'response' array or empty")
        
        doc_data = json_data["response"][0]
        
        return {
            "name": doc_data.get("document_type", ""),
            "description": doc_data.get("description"),
            "sla": doc_data.get("sla"),
            "fields": doc_data.get("schema_blob")
        }
    
    def seed_document_configurations(self):
        """Seed document_configuration table from JSON files"""
        session = self.db_manager.get_session()
        
        try:
            inserted_count = 0
            updated_count = 0
            skipped_count = 0
            
            # Process each JSON file
            for filename, expected_doc_type in self.file_mapping.items():
                file_path = os.path.join(self.json_files_path, filename)
                
                if not os.path.exists(file_path):
                    logger.warning(f"File not found: {file_path}, skipping...")
                    skipped_count += 1
                    continue
                
                logger.info(f"Processing file: {filename}")
                
                try:
                    # Load JSON data
                    json_data = self._load_json_file(file_path)
                    
                    # Extract document data
                    doc_data = self._extract_document_data(json_data)
                    
                    # Parse SLA to integer
                    sla_int = self._parse_sla(doc_data["sla"])
                    
                    # Check if document configuration already exists
                    check_query = text("""
                        SELECT id FROM public.document_configuration 
                        WHERE name = :name
                    """)
                    result = session.execute(check_query, {"name": doc_data["name"]}).fetchone()
                    
                    if result:
                        # Update existing record
                        update_query = text("""
                            UPDATE public.document_configuration
                            SET description = :description,
                                sla = :sla,
                                fields = CAST(:fields AS jsonb)
                            WHERE name = :name
                        """)
                        fields_json = json.dumps(doc_data["fields"]) if doc_data["fields"] else None
                        session.execute(update_query, {
                            "name": doc_data["name"],
                            "description": doc_data["description"],
                            "sla": sla_int,
                            "fields": fields_json
                        })
                        updated_count += 1
                        logger.info(f"Updated document configuration: {doc_data['name']}")
                    else:
                        # Insert new record
                        insert_query = text("""
                            INSERT INTO public.document_configuration (name, description, sla, fields)
                            VALUES (:name, :description, :sla, CAST(:fields AS jsonb))
                        """)
                        fields_json = json.dumps(doc_data["fields"]) if doc_data["fields"] else None
                        session.execute(insert_query, {
                            "name": doc_data["name"],
                            "description": doc_data["description"],
                            "sla": sla_int,
                            "fields": fields_json
                        })
                        inserted_count += 1
                        logger.info(f"Inserted document configuration: {doc_data['name']}")
                    
                except Exception as e:
                    logger.error(f"Error processing file {filename}: {e}")
                    session.rollback()
                    continue
            
            # Commit all changes
            session.commit()
            
            logger.info("=" * 60)
            logger.info("Document Configuration Seeding Summary:")
            logger.info(f"  Inserted: {inserted_count}")
            logger.info(f"  Updated: {updated_count}")
            logger.info(f"  Skipped: {skipped_count}")
            logger.info("=" * 60)
            
            return True
            
        except Exception as e:
            logger.error(f"Error seeding document configurations: {e}")
            session.rollback()
            raise
        finally:
            session.close()

def main():
    """Main function to run document configuration seeding"""
    logger.info("Starting document configuration seeding...")
    
    try:
        # Initialize database manager
        db_manager = DatabaseManager()
        
        # Create seeder instance
        seeder = DocumentConfigurationSeeder(db_manager)
        
        # Run seeding
        seeder.seed_document_configurations()
        
        logger.info("Document configuration seeding completed successfully!")
        return True
        
    except Exception as e:
        logger.error(f"Document configuration seeding failed: {e}")
        return False

if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)


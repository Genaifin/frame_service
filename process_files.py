#!/usr/bin/env python3
"""
Script for the AithonOrchestrator database storage functionality
"""

import os
import sys
from pathlib import Path
import logging
from dotenv import load_dotenv

# Load environment variables from .env file (or config.env as fallback)
if Path('.env').exists():
    load_dotenv('.env')
elif Path('config.env').exists():
    load_dotenv('config.env')
else:
    load_dotenv()  # Try default .env location

# Verify OpenAI API key is loaded
openai_key = os.getenv('OPENAI_API_KEY')
if not openai_key or openai_key == 'TEST':
    print("⚠️  WARNING: OPENAI_API_KEY not properly configured!")
    print("   Please ensure OPENAI_API_KEY is set in your .env file")
    print("   Current value:", openai_key if openai_key else "Not set")
    print("   You can copy config.env to .env and update the API key")
else:
    print(f"✅ OpenAI API key loaded (starts with: {openai_key[:10]}...)")

# Add the project root to Python path
project_root = Path(__file__).parent
sys.path.append(str(project_root))

# Set up environment variables for testing
os.environ.setdefault('OUTPUT_DIR', './test_output')
os.environ.setdefault('SOURCE_DIR', './test_documents')
os.environ.setdefault('ENABLE_BACKEND_OUTPUT', 'true')
os.environ.setdefault('LOG_LEVEL', 'INFO')

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

def test_database_connection():
    """Test database connection"""
    try:
        from database_models import get_database_manager
        from sqlalchemy import text
        
        db_manager = get_database_manager()
        session = db_manager.get_session()
        
        # Test basic query with explicit text() declaration
        result = session.execute(text("SELECT 1 as test")).fetchone()
        print(f"✅ Database connection successful: {result}")
        
        session.close()
        return True
    except Exception as e:
        print(f"❌ Database connection failed: {e}")
        return False

def test_orchestrator_initialization():
    """Test orchestrator initialization"""
    try:
        from frameEngine.orchestrator import AithonOrchestrator
        
        print("🔄 Initializing AithonOrchestrator...")
        orchestrator = AithonOrchestrator()
        
        print("✅ Orchestrator initialized successfully")
        print(f"   - Database available: {orchestrator.db_manager is not None}")
        print(f"   - Boxes initialized: {len([orchestrator.ingestion_box, orchestrator.ocr_box, orchestrator.preprocessing_box, orchestrator.classification_box, orchestrator.extraction_box, orchestrator.bounding_box_box, orchestrator.validation_enrichment_box, orchestrator.output_box])}")
        
        return orchestrator
    except Exception as e:
        print(f"❌ Orchestrator initialization failed: {e}")
        return None

def check_output_exists(file_path):
    """Check if output file already exists for a given file"""
    output_dir = Path('./test_output')
    output_dir.mkdir(exist_ok=True)
    
    # Output file naming pattern: {filename_stem}_output.json
    output_filename = output_dir / f"{file_path.stem}_output.json"
    
    return output_filename.exists()

def get_test_documents():
    """Get all supported document files from test_documents folder that don't have output yet"""
    test_dir = Path('./test_documents')
    test_dir.mkdir(exist_ok=True)
    
    # Supported file extensions
    supported_extensions = ['*.pdf', '*.xlsx', '*.xls', '*.docx', '*.doc']
    
    # Find all supported files in the directory
    all_files = []
    for ext in supported_extensions:
        all_files.extend(list(test_dir.glob(ext)))
    
    if not all_files:
        print(f"📄 No supported files found in: {test_dir}")
        print("   Supported formats: PDF (.pdf), Excel (.xlsx, .xls), Word (.docx, .doc)")
        print("   Please place files in the test_documents folder")
        return []
    
    # Filter out files that already have output
    files_to_process = []
    files_skipped = []
    
    for file in all_files:
        if check_output_exists(file):
            files_skipped.append(file.name)
        else:
            files_to_process.append(file)
    
    # Print summary
    print(f"📄 Found {len(all_files)} file(s) total:")
    if files_skipped:
        print(f"   ⏭️  Skipping {len(files_skipped)} file(s) (output already exists):")
        for skipped_file in files_skipped:
            print(f"      - {skipped_file}")
    if files_to_process:
        print(f"   🔄 Processing {len(files_to_process)} file(s):")
        for file in files_to_process:
            print(f"      - {file.name}")
    else:
        print("   ℹ️  All files have already been processed!")
    
    return files_to_process

def test_document_processing(orchestrator, test_file_path):
    """Test document processing with database storage"""
    try:
        print(f"\n🔄 Processing document: {test_file_path.name}")
        
        # Run the pipeline
        result = orchestrator.run_pipeline(test_file_path)
        
        print(f"✅ Processing completed for {test_file_path.name}")
        print(f"   - Success: {result['success']}")
        print(f"   - Processing time: {result['processing_time']:.2f}s")
        print(f"   - Stages completed: {result['stages_completed']}")
        
        if result['errors']:
            print(f"   - Errors: {result['errors']}")
        
        if 'database_storage' in result['stages_completed']:
            print("   ✅ Database storage successful")
        else:
            print("   ⚠️  Database storage not completed")
        
        return result
        
    except Exception as e:
        print(f"❌ Document processing failed for {test_file_path.name}: {e}")
        import traceback
        traceback.print_exc()
        return None

def test_database_queries():
    """Test database queries to verify data was stored"""
    try:
        from database_models import get_database_manager, Document, CapitalCallsExtraction, DistributionsExtraction, StatementsExtraction
        
        db_manager = get_database_manager()
        session = db_manager.get_session()
        
        # Check documents table
        doc_count = session.query(Document).count()
        print(f"📊 Total documents in database: {doc_count}")
        
        # Check extraction tables
        capital_calls_count = session.query(CapitalCallsExtraction).count()
        distributions_count = session.query(DistributionsExtraction).count()
        statements_count = session.query(StatementsExtraction).count()
        
        print(f"📊 Capital calls records: {capital_calls_count}")
        print(f"📊 Distributions records: {distributions_count}")
        print(f"📊 Statements records: {statements_count}")
        
        # Show recent documents
        recent_docs = session.query(Document).order_by(Document.created_at.desc()).limit(5).all()
        print(f"📄 Recent documents:")
        for doc in recent_docs:
            print(f"   - {doc.name} ({doc.type}) - {doc.status}")
        
        session.close()
        return True
        
    except Exception as e:
        print(f"❌ Database query test failed: {e}")
        return False

def main():
    """Main test function"""
    print("🧪 Testing AithonOrchestrator Database Storage")
    print("=" * 50)
    
    # Test 1: Database Connection
    print("\n1️⃣ Testing Database Connection...")
    if not test_database_connection():
        print("❌ Cannot proceed without database connection")
        return
    
    # Test 2: Orchestrator Initialization
    print("\n2️⃣ Testing Orchestrator Initialization...")
    orchestrator = test_orchestrator_initialization()
    if not orchestrator:
        print("❌ Cannot proceed without orchestrator")
        return
    
    # Test 3: Get Test Documents
    print("\n3️⃣ Finding Test Documents...")
    test_files = get_test_documents()
    if not test_files:
        print("ℹ️  No new documents to process (all files already have output)")
        return
    
    # Test 4: Document Processing (process all files)
    print("\n4️⃣ Testing Document Processing...")
    print("=" * 50)
    results = []
    successful_count = 0
    db_stored_count = 0
    
    for i, test_file in enumerate(test_files, 1):
        print(f"\n📄 File {i}/{len(test_files)}")
        result = test_document_processing(orchestrator, test_file)
        if result:
            results.append(result)
            if result['success']:
                successful_count += 1
            if 'database_storage' in result['stages_completed']:
                db_stored_count += 1
    
    # Summary
    print("\n" + "=" * 50)
    print("📊 Processing Summary:")
    print(f"   - Files processed in this run: {len(results)}")
    print(f"   - Successful: {successful_count}")
    print(f"   - Database stored: {db_stored_count}")
    print(f"   - Failed: {len(results) - successful_count}")
    
    # Count skipped files
    supported_extensions = ['*.pdf', '*.xlsx', '*.xls', '*.docx', '*.doc']
    all_files = []
    for ext in supported_extensions:
        all_files.extend(list(Path('./test_documents').glob(ext)))
    skipped_count = len(all_files) - len(test_files)
    if skipped_count > 0:
        print(f"   - Skipped (already processed): {skipped_count}")
    
    # Test 5: Database Verification
    print("\n5️⃣ Verifying Database Storage...")
    test_database_queries()
    
    print("\n🎉 Testing completed!")
    print("\nNext steps:")
    print("1. Check your database for the stored records")
    print("2. Verify the extracted data matches your document content")
    print("3. Test with different document types (capital calls, distributions, statements)")
    print("4. Supported file formats: PDF, Excel (.xlsx, .xls), Word (.docx, .doc)")

if __name__ == "__main__":
    main()

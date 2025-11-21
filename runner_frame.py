import sys
import os
import time
import json
import shutil
import hashlib
from pathlib import Path

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
aithon_frame_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'frameEngine')
sys.path.append(aithon_frame_path)

# Queue system
QUEUE_FILE = Path("queue/queue.json")
UPLOAD_DIR = Path("data/frameDemo/l0")
LAST_SCAN_FILE = Path("queue/last_scan.json")

# Global variable to hold the orchestrator instance
orchestrator = None

def initialize_orchestrator():
    """Initialize the orchestrator only when needed."""
    global orchestrator
    
    if orchestrator is not None:
        return orchestrator
    
    try:
        # Enable backend output for validusBoxes integration
        os.environ["ENABLE_BACKEND_OUTPUT"] = "true"
        # Use correct path that works both locally and in Docker
        # Docker sets this to /app/data/frameDemo/l1, locally use relative path
        if "BACKEND_OUTPUT_DIR" not in os.environ:
            os.environ["BACKEND_OUTPUT_DIR"] = "./data/frameDemo/l1"
        
        # Import the orchestrator from the frameEngine package
        from frameEngine import AithonOrchestrator
        print("✅ Orchestrator loaded successfully")
        
        orchestrator = AithonOrchestrator()
        print("✅ Orchestrator initialized successfully")
        backend_output_dir = os.getenv("BACKEND_OUTPUT_DIR", "./data/frameDemo/l1")
        print(f"🔧 Backend output enabled - files will be saved to {backend_output_dir}")

        return orchestrator
        
    except Exception as e:
        print(f"❌ Failed to load or initialize orchestrator: {e}")
        raise e

def init_queue():
    """Initialize queue file"""
    QUEUE_FILE.parent.mkdir(exist_ok=True)
    if not QUEUE_FILE.exists():
        with open(QUEUE_FILE, 'w') as f:
            json.dump([], f)

def get_last_scan_time():
    """Get the last scan time"""
    if LAST_SCAN_FILE.exists():
        try:
            with open(LAST_SCAN_FILE, 'r') as f:
                data = json.load(f)
                return data.get("last_scan_time", 0)
        except:
            pass
    return 0

def update_last_scan_time():
    """Update the last scan time to now"""
    LAST_SCAN_FILE.parent.mkdir(exist_ok=True)
    with open(LAST_SCAN_FILE, 'w') as f:
        json.dump({"last_scan_time": time.time()}, f)

def get_next_file_from_queue():
    """Get next pending file from queue"""
    if not QUEUE_FILE.exists():
        return None
    
    try:
        with open(QUEUE_FILE, 'r') as f:
            queue = json.load(f)
    except:
        return None
    
    # Find first pending file
    for i, item in enumerate(queue):
        if item.get("status") == "pending":
            # Mark as processing
            queue[i]["status"] = "processing"
            queue[i]["processing_started_at"] = time.time()
            
            # Save updated queue
            with open(QUEUE_FILE, 'w') as f:
                json.dump(queue, f, indent=2)
            
            return item
    return None

def get_file_hash(file_path: str):
    """Calculate SHA256 hash of file"""
    try:
        import hashlib
        hash_sha256 = hashlib.sha256()
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_sha256.update(chunk)
        return hash_sha256.hexdigest()
    except:
        return None

def get_file_completion_status(filename: str, file_path: str = None):
    """Check if file is already completed - ENHANCED with hash comparison"""
    
    # Check allFileMeta.json FIRST (for fresh uploads)
    allmeta_path = Path("data/frameDemo/ldummy/allFileMeta.json")
    if allmeta_path.exists():
        try:
            with open(allmeta_path, 'r') as f:
                allmeta = json.load(f)
            
            if filename in allmeta:
                meta_status = allmeta[filename].get("status", "")
                stored_hash = allmeta[filename].get("fileHash", "")
                
                # If file path provided, compare hashes
                if file_path and stored_hash:
                    current_hash = get_file_hash(file_path)
                    if current_hash and current_hash != stored_hash:
                        return {
                            "exists_in_queue": False,
                            "exists_in_meta": True,
                            "status": "content_changed",
                            "is_completed": False,
                            "should_skip": False,
                            "source": "meta_hash_diff",
                            "hash_changed": True
                        }
                
                return {
                    "exists_in_queue": False,
                    "exists_in_meta": True,
                    "status": meta_status,
                    "is_completed": meta_status == "Processed",
                    "should_skip": meta_status == "Processed",
                    "source": "meta",
                    "hash_match": True if file_path and stored_hash and get_file_hash(file_path) == stored_hash else None
                }
        except:
            pass
    
    # Check queue.json as secondary source (for files not in meta)
    if QUEUE_FILE.exists():
        try:
            with open(QUEUE_FILE, 'r') as f:
                queue = json.load(f)
            
            # Find latest entry for this file
            latest_entry = None
            for item in queue:
                if item["filename"] == filename:
                    if latest_entry is None or item.get("timestamp", 0) > latest_entry.get("timestamp", 0):
                        latest_entry = item
            
            if latest_entry:
                status = latest_entry.get("status", "")
                return {
                    "exists_in_queue": True,
                    "status": status,
                    "is_completed": status == "completed",
                    "should_skip": status in ["completed", "pending", "processing"],
                    "source": "queue"
                }
        except:
            pass
    
    # File not found anywhere - should be added to queue
    return {
        "exists_in_queue": False,
        "exists_in_meta": False,
        "status": "new",
        "is_completed": False,
        "should_skip": False,
        "source": "new"
    }

def add_file_to_queue(file_path: str, filename: str):
    """Add file to queue - SMART VERSION with hash comparison"""
    init_queue()
    
    # Check completion status with file path for hash comparison
    status_info = get_file_completion_status(filename, file_path)
    
    if status_info["should_skip"]:
        # File is already completed or in queue
        return False
    
    # Load existing queue
    with open(QUEUE_FILE, 'r') as f:
        queue = json.load(f)
    
    # Add new file
    queue_entry = {
        "file_path": file_path,
        "filename": filename,
        "timestamp": time.time(),
        "status": "pending"
    }
    
    queue.append(queue_entry)
    
    # Save updated queue
    with open(QUEUE_FILE, 'w') as f:
        json.dump(queue, f, indent=2)
    
    return True

def mark_file_completed(filename: str, status: str = "completed", error_message: str = None):
    """Mark file as completed in queue"""
    if not QUEUE_FILE.exists():
        return
    
    with open(QUEUE_FILE, 'r') as f:
        queue = json.load(f)
    
    # Find and update file status
    for item in queue:
        if item["filename"] == filename and item["status"] == "processing":
            item["status"] = status
            item["completed_at"] = time.time()
            if error_message:
                item["error_message"] = error_message
            break
    
    # Save updated queue
    with open(QUEUE_FILE, 'w') as f:
        json.dump(queue, f, indent=2)

def update_file_meta_with_classification(filename: str, backend_output_path: Path):
    """Update allFileMeta.json with AI-classified document type"""
    try:
        # Initialize storage to access allFileMeta.json
        from storage import STORAGE
        
        myStorageConfig = {
            'defaultFileStorage': 'onPrem',
        }
        client = 'frameDemo'
        myStorage = STORAGE(client, myStorageConfig)
        
        # Read forFrontend.json to get AI classification
        with open(backend_output_path, 'r') as f:
            frontend_data = json.load(f)
        
        # Extract the AI-classified document type
        ai_document_type = frontend_data.get("document_details", {}).get("file_type", "Unknown")
        
        # Map AI classification to user-friendly file types
        file_type_mapping = {
            "CapCall": "Capital Call Notice",
            "Statement": "Fund Statement", 
            "Distribution": "Distribution Notice",
            "AGM": "Annual General Meeting",
            "Unknown": "Document"
        }
        user_friendly_type = file_type_mapping.get(ai_document_type, ai_document_type)
        
        # Get existing allFileMeta.json
        try:
            existing_meta = myStorage.getJSONDump('ldummy', '', 'allFileMeta')
            if not isinstance(existing_meta, dict):
                existing_meta = {}
        except Exception as e:
            existing_meta = {}
        
        # Update or add the file to allFileMeta
        if filename in existing_meta:
            # Update existing file
            old_type = existing_meta[filename].get("fileType", "Unknown")
            old_status = existing_meta[filename].get("status", "Unknown")
            existing_meta[filename]["fileType"] = user_friendly_type
            existing_meta[filename]["status"] = "Processed"
            print(f"📝 Updated: {filename} ({old_type} → {user_friendly_type}, {old_status} → Processed)")
        else:
            # Add new file that was processed by runner
            # Calculate file hash for consistency using absolute path
            current_dir = Path(__file__).resolve().parent
            aithon_frame_dir = current_dir.parent / "aithon_frame_RC"
            source_file = aithon_frame_dir / "source_documents" / filename
            
            if source_file.exists():
                with open(source_file, 'rb') as f:
                    file_hash = hashlib.sha256(f.read()).hexdigest()
            else:
                file_hash = "unknown"
            
            existing_meta[filename] = {
                "fileHash": file_hash,
                "fileType": user_friendly_type,
                "status": "Processed",
                "fileName": filename
            }
            print(f"📝 Added new file: {filename} (Type: {user_friendly_type}, Status: Processed)")
        
        # Save updated allFileMeta.json
        myMetaOp = {
            "dataTypeToSaveAs": "JSONDump",
            "opParams": {
                "layerName": "ldummy",
                "folderArray": [],
                "operation": "replace"
            },
            "data": existing_meta,
            "key": "allFileMeta"
        }
        myStorage.doDataOperation(myMetaOp)
            
    except Exception as e:
        print(f"⚠️  Failed to update allFileMeta.json: {e}")

def reset_stuck_processing_files():
    """Reset files that have been stuck in processing for too long"""
    if not QUEUE_FILE.exists():
        return
    
    current_time = time.time()
    timeout = 600  # 10 minutes timeout
    
    with open(QUEUE_FILE, 'r') as f:
        queue = json.load(f)
    
    updated = False
    for item in queue:
        if (item.get("status") == "processing" and 
            "processing_started_at" in item and 
            current_time - item["processing_started_at"] > timeout):
            
            print(f"⚠️  Resetting stuck file: {item['filename']}")
            item["status"] = "pending"
            item.pop("processing_started_at", None)
            updated = True
    
    if updated:
        with open(QUEUE_FILE, 'w') as f:
            json.dump(queue, f, indent=2)

def process_file_with_orchestrator(file_path: str, filename: str):   
    return True
    # Verify file exists
    if not os.path.exists(file_path):
        print(f" File not found: {file_path}")
        return {"status": "error", "filename": filename, "message": "File not found"}
    
    # Copy file to aithon_frame_RC source_documents
    source_dir = os.path.join(aithon_frame_path, 'source_documents')
    os.makedirs(source_dir, exist_ok=True)
    dest_path = os.path.join(source_dir, filename)
    print("==> Running Extraction for: ", filename)
    try:
        shutil.copy2(file_path, dest_path)
        print(f"📁 File copied to processing directory")
    except Exception as e:
        print(f" Failed to copy file: {e}")
        return {"status": "error", "filename": filename, "message": f"Copy failed: {e}"}
    
    # Process through orchestrator
    try:
        # Initialize orchestrator if not already done
        current_orchestrator = initialize_orchestrator()
        
        # Sync status before processing starts
        from utils.statusSync import sync_file_statuses
        sync_file_statuses()
        
        print(f"🔄 Starting orchestrator pipeline...")
        
        # Change to aithon_frame_RC directory so schemas are found
        original_cwd = os.getcwd()
        os.chdir(aithon_frame_path)
        
        try:
            result = current_orchestrator.run_pipeline(Path(dest_path))
        finally:
            # Always restore original directory
            os.chdir(original_cwd)
            # Sync status after processing completes
            sync_file_statuses()
        
        if result.get("success"):
            # Look for output JSON - use same directory as OutputBox
            output_dir = os.getenv("OUTPUT_DIR", "./output_documents")
            output_filename = f"{Path(filename).stem}_output.json"
            output_path = os.path.join(output_dir, output_filename)
            
            if os.path.exists(output_path):
                with open(output_path, 'r') as f:
                    output_data = json.load(f)
                print(f"✅ Processing completed successfully")
                print(f"📊 Traditional output saved to: {output_path}")
                
                # Check for backend output (forfronted.json) and update allFileMeta.json
                try:
                    # Calculate file hash using the same file that was processed (dest_path)
                    # This ensures hash consistency with OutputBox calculations
                    import hashlib
                    hash_sha256 = hashlib.sha256()
                    with open(dest_path, "rb") as f:
                        for chunk in iter(lambda: f.read(4096), b""):
                            hash_sha256.update(chunk)
                    file_hash = hash_sha256.hexdigest()
                    
                    # Use the same backend directory as OutputBox
                    backend_base_dir = Path(os.getenv("BACKEND_OUTPUT_DIR", "./data/frameDemo/l1"))
                    backend_dir = backend_base_dir / file_hash
                    backend_output_path = backend_dir / "forFrontend.json"
                    
                    print(f"🔍 Looking for backend output:")
                    print(f"   📁 File hash: {file_hash}")
                    print(f"   🔧 Backend base dir: {backend_base_dir}")
                    print(f"   📂 Backend directory: {backend_dir}")
                    print(f"   📄 Expected file: {backend_output_path}")
                    
                    if backend_output_path.exists():
                        print(f"✅ Backend output found and verified!")
                        
                        # Update allFileMeta.json with AI-classified document type
                        update_file_meta_with_classification(filename, backend_output_path)
                        
                    else:
                        print(f"⚠️  Backend output not found at: {backend_output_path}")
                        # List what files are actually in the directory for debugging
                        if backend_dir.exists():
                            actual_files = list(backend_dir.iterdir())
                            print(f"   📋 Directory exists but contains: {[f.name for f in actual_files]}")
                        else:
                            print(f"   📋 Backend directory does not exist yet")
                except Exception as e:
                    print(f"⚠️  Could not verify backend output: {e}")
                
                return {
                    "status": "completed", 
                    "filename": filename, 
                    "output_path": output_path, 
                    "output_data": output_data,
                    "processing_time": result.get("processing_time"),
                    "stages_completed": result.get("stages_completed")
                }
            else:
                print(f"http://localhost:8000/FE Output JSON not generated",output_dir)
                return {"status": "error", "filename": filename, "message": "Output JSON not generated"}
        else:
            errors = result.get("errors", "Unknown error")
            print(f"http://localhost:8000/FE Processing failed: {errors}")
            return {"status": "error", "filename": filename, "message": f"Processing failed: {errors}"}
            
    except Exception as e:
        print(f"http://localhost:8000/FE Orchestrator failed: {e}")
        return {"status": "error", "filename": filename, "message": str(e)}

def scan_for_new_uploads():
    """SMART VERSION: Scan for new files and only queue incomplete ones"""
    if not UPLOAD_DIR.exists():
        UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
        return []
    
    # Get last scan time
    last_scan_time = get_last_scan_time()
    
    # Get all PDF files
    pdf_files = list(UPLOAD_DIR.glob("*.pdf"))
    
    # Check which files are new based on modification time
    new_files = []
    for file_path in pdf_files:
        # Get file modification time
        file_mod_time = file_path.stat().st_mtime
        
        # If file was modified after last scan, it's new
        if file_mod_time > last_scan_time:
            filename = file_path.name
            status_info = get_file_completion_status(filename, str(file_path))
            
            if not status_info["should_skip"]:
                # Add to queue using smart logic
                if add_file_to_queue(str(file_path), filename):
                    new_files.append(file_path)
                    if status_info.get("hash_changed"):
                        print(f"📤 New file with updated content queued: {filename}")
                    else:
                        print(f"📤 New file detected and queued: {filename}")
                else:
                    print(f"⚠️  New file detected but failed to queue: {filename}")
    
    return new_files

def add_existing_files_to_queue():
    """SMART VERSION: Only add files that are not already completed"""
    if not UPLOAD_DIR.exists():
        return
    
    # Get all PDF files
    pdf_files = list(UPLOAD_DIR.glob("*.pdf"))
    
    print(f"📋 Found {len(pdf_files)} PDF files in upload directory")
    
    added_count = 0
    skipped_completed = 0
    skipped_in_queue = 0
    
    for file_path in pdf_files:
        filename = file_path.name
        status_info = get_file_completion_status(filename, str(file_path))
        
        if status_info["should_skip"]:
            if status_info["is_completed"]:
                skipped_completed += 1
            elif status_info["status"] in ["pending", "processing"]:
                skipped_in_queue += 1
        else:
            # Add to queue (including files with changed content)
            if add_file_to_queue(str(file_path), filename):
                added_count += 1
                if status_info.get("hash_changed"):
                    print(f"📤 ADDED (Content changed): {filename}")
                else:
                    print(f"📤 ADDED to queue: {filename}")
            else:
                print(f"⚠️  Failed to add: {filename}")
    
    if added_count > 0:
        print(f"📤 Added {added_count} new files to queue")
    else:
        print("✅ All files already processed")

def main():
    """Main processing loop"""
    print("🚀 Starting Aithon Frame Runner")
    print(f"📁 Monitoring: {UPLOAD_DIR}")
    
    # Initialize queue
    init_queue()
    
    # Reset any stuck processing files
    reset_stuck_processing_files()
    
    print("   ✅ Skipping already completed files")
    print("   🔄 Skipping files already in queue") 
    print("   📤 Only adding new/failed files to queue")
    
    # Smart queue existing files (only incomplete ones)
    add_existing_files_to_queue()
    
    
    print("-" * 50)
    
    print("🔄 Continuous monitoring mode - waiting for new uploads...")
    print("💡 Upload files via web interface - they will be processed automatically")
    print("🛑 Press Ctrl+C to stop monitoring")
    print("-" * 50)
    
    last_scan_time = 0
    scan_interval = 10  # Check for new files every 10 seconds
    
    while True:
        current_time = time.time()
        
        # Step 1: Process any pending files in queue
        file_info = get_next_file_from_queue()
        if file_info:
            try:
                print(f"🔄 Processing: {file_info['filename']}")
                result = process_file_with_orchestrator(file_info['file_path'], file_info['filename'])
                
                # Mark as completed with appropriate status
                if result['status'] == 'completed':
                    mark_file_completed(file_info['filename'], 'completed')
                    print(f"✅ Completed: {file_info['filename']}")
                else:
                    mark_file_completed(file_info['filename'], 'failed', result.get('message'))
                    print(f"❌ Failed: {file_info['filename']}")
                
                print("-" * 50)
                
            except Exception as e:
                print(f"❌ Unexpected error processing {file_info['filename']}: {e}")
                mark_file_completed(file_info['filename'], 'failed', str(e))
            
            # Continue immediately to check for more files
            continue
        
        # Step 2: Scan for new uploads (but only every scan_interval seconds)
        if current_time - last_scan_time >= scan_interval:
            new_files = scan_for_new_uploads()
            last_scan_time = current_time  # Update scan time regardless
            if new_files:
                print(f"📤 Detected {len(new_files)} new files - adding to queue...")
                update_last_scan_time()
                # Don't sleep, immediately check queue again
                continue
        
        # Step 3: Reset stuck files periodically (every 60 seconds)
        if int(current_time) % 60 == 0:
            reset_stuck_processing_files()
        
        # Step 4: No new work, wait quietly
        time.sleep(1)  # Short sleep for responsiveness

def run_processing_session():
    """
    Simple processing session - processes all files in queue until empty.
    Called by API when 'Start Processing' button is clicked.
    """
    print("🚀 Starting Processing Session")
    
    try:
        # Initialize queue
        init_queue()
        reset_stuck_processing_files()
        add_existing_files_to_queue()
        
        print("-" * 50)
        
        processed_count = 0
        while True:
            # Process any pending files in queue
            file_info = get_next_file_from_queue()
            if file_info:
                processed_count += 1
                try:
                    print(f"🔄 Processing file {processed_count}: {file_info['filename']}")
                    result = process_file_with_orchestrator(file_info['file_path'], file_info['filename'])
                    
                    if result['status'] == 'completed':
                        mark_file_completed(file_info['filename'], 'completed')
                        print(f"✅ Completed: {file_info['filename']}")
                    else:
                        mark_file_completed(file_info['filename'], 'failed', result.get('message'))
                        print(f"❌ Failed: {file_info['filename']}")
                    
                    print("-" * 50)
                    
                except Exception as e:
                    print(f"❌ Unexpected error processing {file_info['filename']}: {e}")
                    mark_file_completed(file_info['filename'], 'failed', str(e))
                
                continue
            
            # No more files in queue - stop processing
            print(f"✅ No more files to process - stopping (processed {processed_count} files)")
            break
        
        result = {"status": "completed", "message": f"Processed {processed_count} files"}
        return result
        
    except Exception as e:
        error_msg = f"Error in processing session: {e}"
        print(f"❌ {error_msg}")
        return {"status": "error", "message": error_msg}

if __name__ == "__main__":
    main() 
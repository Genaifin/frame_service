import json
import time
from pathlib import Path

# Get absolute path to validusBoxes directory
VALIDUS_BOXES_DIR = Path(__file__).resolve().parent.parent

# File paths (now using absolute paths)
QUEUE_FILE = VALIDUS_BOXES_DIR / "queue" / "queue.json"
ALL_FILE_META_PATH = VALIDUS_BOXES_DIR / "data" / "frameDemo" / "ldummy" / "allFileMeta.json"

def map_queue_status_to_display_status(queue_status):
    """Map queue status to user-friendly display status"""
    status_mapping = {
        "pending": "Queued",
        "processing": "Processing", 
        "completed": "Processed",
        "failed": "Failed"
    }
    return status_mapping.get(queue_status, "Unknown")

def get_latest_file_status():
    """Get the latest status for each file from queue.json"""
    if not QUEUE_FILE.exists():
        print("⚠️  Queue file not found, using default 'Processed' status")
        return {}
    
    try:
        with open(QUEUE_FILE, 'r') as f:
            queue_data = json.load(f)
        
        # Get latest status for each file (in case of duplicates, use most recent)
        file_status_map = {}
        for entry in queue_data:
            filename = entry.get("filename")
            status = entry.get("status")
            timestamp = entry.get("timestamp", 0)
            
            if filename and status:
                # Keep latest entry for each file
                if filename not in file_status_map or timestamp > file_status_map[filename]["timestamp"]:
                    file_status_map[filename] = {
                        "status": map_queue_status_to_display_status(status),
                        "timestamp": timestamp
                    }
        
        # Extract just the status for each file
        return {filename: data["status"] for filename, data in file_status_map.items()}
        
    except Exception as e:
        print(f"  Error reading queue file: {e}")
        return {}

def sync_file_statuses(verbose=True):
    """Sync file statuses from queue to allFileMeta.json"""
    
    # Get real-time statuses from queue
    queue_statuses = get_latest_file_status()
    
    # Read current allFileMeta.json
    if not ALL_FILE_META_PATH.exists():
        if verbose:
            print("⚠️  allFileMeta.json not found")
        return False
    
    try:
        with open(ALL_FILE_META_PATH, 'r') as f:
            all_file_meta = json.load(f)
        
        updated_count = 0
        
        # Update status for each file
        for filename, file_data in all_file_meta.items():
            current_status = file_data.get("status", "Processed")
            
            # Get real status from queue
            real_status = queue_statuses.get(filename, "Processed")  # Default to "Processed" if not in queue
            
            # Update if status changed
            # if current_status != real_status:
            #     file_data["status"] = real_status
            #     updated_count += 1
            #     if verbose:
            #         print(f"📄 Updated {filename}: {current_status} → {real_status}")
        
        # Write back updated allFileMeta.json
        with open(ALL_FILE_META_PATH, 'w') as f:
            json.dump(all_file_meta, f, indent=2)
        
        # if verbose:
        #     print(f"✅ Status sync completed! Updated {updated_count} files")
        # return True
        
    except Exception as e:
        if verbose:
            print(f"❌ Error updating allFileMeta.json: {e}")
        return False

def watch_and_sync(interval=10):
    """Continuously watch and sync statuses at specified interval (seconds)"""
    print(f"🔄 Starting real-time status sync (every {interval}s)")
    print("Press Ctrl+C to stop")
    
    try:
        while True:
            sync_file_statuses()
            time.sleep(interval)
    except KeyboardInterrupt:
        print("\n👋 Status sync stopped")

if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1 and sys.argv[1] == "watch":
        # Run in watch mode
        watch_and_sync()
    else:
        # Run once
        sync_file_statuses() 
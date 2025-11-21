import json
import os
import time
from pathlib import Path
import threading

# Queue file location
QUEUE_FILE = Path("queue/file_queue.json")
QUEUE_DIR = Path("queue")

# Create queue directory if it doesn't exist
QUEUE_DIR.mkdir(exist_ok=True)

# Thread lock for queue operations
queue_lock = threading.Lock()

def initialize_queue():
    """Initialize queue file if it doesn't exist"""
    if not QUEUE_FILE.exists():
        with open(QUEUE_FILE, 'w') as f:
            json.dump([], f)

def add_to_queue(file_path: str, filename: str):
    """Add file to queue - called by dashboard upload"""
    with queue_lock:
        initialize_queue()
        
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
        
        print(f"[QUEUE] Added {filename} to queue")

def get_queue_file():
    """Get next file from queue - called by runner"""
    with queue_lock:
        initialize_queue()
        
        # Load queue
        with open(QUEUE_FILE, 'r') as f:
            queue = json.load(f)
        
        # Find first pending file
        for i, item in enumerate(queue):
            if item.get("status") == "pending":
                # Mark as processing
                queue[i]["status"] = "processing"
                
                # Save updated queue
                with open(QUEUE_FILE, 'w') as f:
                    json.dump(queue, f, indent=2)
                
                return item
        
        return None

def mark_completed(file_path: str, filename: str):
    """Mark file as completed"""
    with queue_lock:
        initialize_queue()
        
        # Load queue
        with open(QUEUE_FILE, 'r') as f:
            queue = json.load(f)
        
        # Find and mark as completed
        for item in queue:
            if item["file_path"] == file_path and item["filename"] == filename:
                item["status"] = "completed"
                break
        
        # Save updated queue
        with open(QUEUE_FILE, 'w') as f:
            json.dump(queue, f, indent=2) 
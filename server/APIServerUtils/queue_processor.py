#!/usr/bin/env python3
"""
Background Queue Processor for processing files from the queue in FIFO order
"""

import logging
import threading
import time
from typing import Optional
from server.APIServerUtils.file_queue_service import FileQueueService
from runner_frame import process_file_with_orchestrator

logger = logging.getLogger(__name__)

class QueueProcessor:
    """Background processor for file queue"""
    
    def __init__(self, poll_interval: int = 5, reset_stuck_interval: int = 300):
        """
        Initialize the queue processor
        
        Args:
            poll_interval: Seconds between queue polls (default: 5)
            reset_stuck_interval: Seconds between stuck file resets (default: 300 = 5 minutes)
        """
        self.queue_service = FileQueueService()
        self.poll_interval = poll_interval
        self.reset_stuck_interval = reset_stuck_interval
        self._running = False
        self._thread: Optional[threading.Thread] = None
        self._last_reset_time = time.time()
    
    def start(self):
        """Start the background processing thread"""
        if self._running:
            logger.warning("Queue processor is already running")
            return
        
        self._running = True
        self._thread = threading.Thread(target=self._process_loop, daemon=True)
        self._thread.start()
        logger.info("Queue processor started")
    
    def stop(self):
        """Stop the background processing thread"""
        self._running = False
        if self._thread:
            self._thread.join(timeout=10)
        logger.info("Queue processor stopped")
    
    def _process_loop(self):
        """Main processing loop that runs in background thread"""
        logger.info("Queue processor loop started")
        
        while self._running:
            try:
                # Reset stuck files periodically
                current_time = time.time()
                if current_time - self._last_reset_time >= self.reset_stuck_interval:
                    self._reset_stuck_files()
                    self._last_reset_time = current_time
                
                # Get next file from queue (FIFO)
                queue_entry = self.queue_service.get_next_file_from_queue()
                
                if queue_entry:
                    queue_id = queue_entry['id']
                    file_path = queue_entry['file_path']
                    filename = queue_entry['filename']
                    
                    logger.info(f"Processing file from queue: {filename} (ID: {queue_id})")
                    
                    try:
                        # Process the file
                        result = process_file_with_orchestrator(file_path, filename)
                        
                        # Mark as completed or failed based on result
                        if isinstance(result, dict):
                            # Expected format: {"status": "completed" | "error", "message": "...", ...}
                            success = result.get('status') == 'completed'
                            error_message = result.get('message') if not success else None
                        elif result is True:
                            # Handle case where function returns True (stub/placeholder)
                            # This means processing was skipped or not implemented yet
                            success = True
                            error_message = None
                        elif result is False or result is None:
                            # Explicit failure
                            success = False
                            error_message = "Processing returned False or None"
                        else:
                            # Unexpected return type - log warning but treat as success if truthy
                            logger.warning(f"Unexpected return type from process_file_with_orchestrator: {type(result)}")
                            success = bool(result)
                            error_message = None if success else "Processing returned unexpected result"
                        
                        self.queue_service.mark_file_completed(
                            queue_id=queue_id,
                            success=success,
                            error_message=error_message
                        )
                        
                        if success:
                            logger.info(f"Successfully processed file: {filename} (ID: {queue_id})")
                        else:
                            logger.warning(f"Failed to process file: {filename} (ID: {queue_id}) - {error_message}")
                    
                    except Exception as e:
                        # Mark as failed on exception
                        error_message = str(e)
                        self.queue_service.mark_file_completed(
                            queue_id=queue_id,
                            success=False,
                            error_message=error_message
                        )
                        logger.error(f"Error processing file {filename} (ID: {queue_id}): {e}")
                
                else:
                    # No files in queue, sleep before next poll
                    time.sleep(self.poll_interval)
            
            except Exception as e:
                logger.error(f"Error in queue processor loop: {e}")
                time.sleep(self.poll_interval)
    
    def _reset_stuck_files(self):
        """Reset files that have been stuck in processing status"""
        try:
            reset_count = self.queue_service.reset_stuck_files(timeout_minutes=30)
            if reset_count > 0:
                logger.info(f"Reset {reset_count} stuck file(s) back to pending status")
        except Exception as e:
            logger.error(f"Error resetting stuck files: {e}")
    
    def get_status(self) -> dict:
        """Get current processor status"""
        queue_status = self.queue_service.get_queue_status()
        return {
            'running': self._running,
            'poll_interval': self.poll_interval,
            'queue_status': queue_status
        }

# Global processor instance
_processor: Optional[QueueProcessor] = None

def get_queue_processor() -> QueueProcessor:
    """Get or create the global queue processor instance"""
    global _processor
    if _processor is None:
        _processor = QueueProcessor()
    return _processor

def start_queue_processor():
    """Start the global queue processor"""
    processor = get_queue_processor()
    processor.start()

def stop_queue_processor():
    """Stop the global queue processor"""
    global _processor
    if _processor:
        _processor.stop()


#!/usr/bin/env python3
"""
File Queue Service for managing file processing queue in FIFO order
"""

import logging
from typing import Optional, Dict, Any
from datetime import datetime
from sqlalchemy.orm import Session
from sqlalchemy import and_
from database_models import DatabaseManager, FileQueue

logger = logging.getLogger(__name__)

class FileQueueService:
    """Service for managing file processing queue"""
    
    def __init__(self):
        """Initialize the queue service with database manager"""
        self.db_manager = DatabaseManager()
        self.SessionLocal = self.db_manager.SessionLocal
    
    def add_file_to_queue(
        self,
        filename: str,
        file_path: str,
        file_hash: Optional[str] = None,
        folder: str = "l0",
        storage_type: str = "local",
        source: str = "api",
        file_classification: str = "",
        username: str = ""
    ) -> Dict[str, Any]:
        """
        Add a file to the processing queue
        
        Args:
            filename: Name of the file
            file_path: Full path to the file
            file_hash: Hash of the file (optional)
            folder: Target folder (default: l0)
            storage_type: Storage type (default: local)
            source: Source of upload (default: api)
            file_classification: File classification (optional)
            username: Username who uploaded the file
            
        Returns:
            Dictionary with queue entry details
        """
        db: Session = self.SessionLocal()
        try:
            # Check if file already exists in queue with pending or processing status
            existing = db.query(FileQueue).filter(
                and_(
                    FileQueue.filename == filename,
                    FileQueue.status.in_(['pending', 'processing'])
                )
            ).first()
            
            if existing:
                logger.warning(f"File {filename} already in queue with status {existing.status}")
                return existing.to_dict()
            
            # Create new queue entry
            queue_entry = FileQueue(
                filename=filename,
                file_path=file_path,
                file_hash=file_hash,
                folder=folder,
                storage_type=storage_type,
                source=source,
                file_classification=file_classification,
                username=username,
                status='pending'
            )
            
            db.add(queue_entry)
            db.commit()
            db.refresh(queue_entry)
            
            logger.info(f"Added file {filename} to queue with ID {queue_entry.id}")
            return queue_entry.to_dict()
            
        except Exception as e:
            db.rollback()
            logger.error(f"Error adding file to queue: {e}")
            raise
        finally:
            db.close()
    
    def get_next_file_from_queue(self) -> Optional[Dict[str, Any]]:
        """
        Get the next file from the queue in FIFO order (oldest pending first)
        
        Returns:
            Dictionary with file details or None if queue is empty
        """
        db: Session = self.SessionLocal()
        try:
            # Get the oldest pending file (FIFO)
            queue_entry = db.query(FileQueue).filter(
                FileQueue.status == 'pending'
            ).order_by(FileQueue.created_at.asc()).first()
            
            if not queue_entry:
                return None
            
            # Mark as processing
            queue_entry.status = 'processing'
            queue_entry.started_at = datetime.utcnow()
            db.commit()
            db.refresh(queue_entry)
            
            logger.info(f"Retrieved file {queue_entry.filename} from queue (ID: {queue_entry.id})")
            return queue_entry.to_dict()
            
        except Exception as e:
            db.rollback()
            logger.error(f"Error getting next file from queue: {e}")
            raise
        finally:
            db.close()
    
    def mark_file_completed(self, queue_id: int, success: bool = True, error_message: Optional[str] = None):
        """
        Mark a file as completed or failed in the queue
        
        Args:
            queue_id: ID of the queue entry
            success: True if processing succeeded, False if failed
            error_message: Error message if processing failed
        """
        db: Session = self.SessionLocal()
        try:
            queue_entry = db.query(FileQueue).filter(FileQueue.id == queue_id).first()
            
            if not queue_entry:
                logger.warning(f"Queue entry with ID {queue_id} not found")
                return
            
            queue_entry.status = 'completed' if success else 'failed'
            queue_entry.completed_at = datetime.utcnow()
            if error_message:
                queue_entry.error_message = error_message
            
            db.commit()
            logger.info(f"Marked file {queue_entry.filename} as {queue_entry.status} (ID: {queue_id})")
            
        except Exception as e:
            db.rollback()
            logger.error(f"Error marking file as completed: {e}")
            raise
        finally:
            db.close()
    
    def reset_stuck_files(self, timeout_minutes: int = 30):
        """
        Reset files that have been in 'processing' status for too long
        (likely due to a crashed or interrupted process)
        
        Args:
            timeout_minutes: Minutes after which a processing file is considered stuck
        """
        db: Session = self.SessionLocal()
        try:
            from datetime import timedelta
            timeout_threshold = datetime.utcnow() - timedelta(minutes=timeout_minutes)
            
            stuck_files = db.query(FileQueue).filter(
                and_(
                    FileQueue.status == 'processing',
                    FileQueue.started_at < timeout_threshold
                )
            ).all()
            
            reset_count = 0
            for file_entry in stuck_files:
                file_entry.status = 'pending'
                file_entry.started_at = None
                reset_count += 1
                logger.info(f"Reset stuck file: {file_entry.filename} (ID: {file_entry.id})")
            
            if reset_count > 0:
                db.commit()
                logger.info(f"Reset {reset_count} stuck file(s)")
            
            return reset_count
            
        except Exception as e:
            db.rollback()
            logger.error(f"Error resetting stuck files: {e}")
            raise
        finally:
            db.close()
    
    def get_queue_status(self) -> Dict[str, Any]:
        """
        Get current queue status statistics
        
        Returns:
            Dictionary with queue statistics
        """
        db: Session = self.SessionLocal()
        try:
            total = db.query(FileQueue).count()
            pending = db.query(FileQueue).filter(FileQueue.status == 'pending').count()
            processing = db.query(FileQueue).filter(FileQueue.status == 'processing').count()
            completed = db.query(FileQueue).filter(FileQueue.status == 'completed').count()
            failed = db.query(FileQueue).filter(FileQueue.status == 'failed').count()
            
            return {
                'total': total,
                'pending': pending,
                'processing': processing,
                'completed': completed,
                'failed': failed
            }
            
        except Exception as e:
            logger.error(f"Error getting queue status: {e}")
            raise
        finally:
            db.close()


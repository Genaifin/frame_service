#!/usr/bin/env python3
"""
RabbitMQ Service for managing file processing queue
"""

import logging
import json
import os
from typing import Optional, Dict, Any
from datetime import datetime
import pika
from pika.exceptions import AMQPConnectionError, AMQPChannelError

logger = logging.getLogger(__name__)

class RabbitMQService:
    """Service for managing file processing queue using RabbitMQ"""
    
    def __init__(
        self,
        host: Optional[str] = None,
        port: int = 5672,
        username: Optional[str] = None,
        password: Optional[str] = None,
        queue_name: str = "file_processing_queue",
        virtual_host: str = "/"
    ):
        """
        Initialize the RabbitMQ service
        
        Args:
            host: RabbitMQ host (default: from RABBITMQ_HOST env var or localhost)
            port: RabbitMQ port (default: from RABBITMQ_PORT env var or 5672)
            username: RabbitMQ username (default: from RABBITMQ_USERNAME env var or guest)
            password: RabbitMQ password (default: from RABBITMQ_PASSWORD env var or guest)
            queue_name: Name of the queue (default: file_processing_queue)
            virtual_host: Virtual host (default: /)
        """
        self.host = host or os.getenv('RABBITMQ_HOST', 'localhost')
        self.port = int(os.getenv('RABBITMQ_PORT', str(port)))
        self.username = username or os.getenv('RABBITMQ_USERNAME', 'guest')
        self.password = password or os.getenv('RABBITMQ_PASSWORD', 'guest')
        self.queue_name = queue_name
        self.virtual_host = virtual_host
        
        self._connection: Optional[pika.BlockingConnection] = None
        self._channel: Optional[pika.channel.Channel] = None
        
        # Don't connect immediately - connect lazily when needed
        # This prevents connection errors during module import if RabbitMQ is not running
    
    def _get_connection(self) -> pika.BlockingConnection:
        """Get or create RabbitMQ connection"""
        if self._connection is None or self._connection.is_closed:
            credentials = pika.PlainCredentials(self.username, self.password)
            parameters = pika.ConnectionParameters(
                host=self.host,
                port=self.port,
                virtual_host=self.virtual_host,
                credentials=credentials,
                heartbeat=600,
                blocked_connection_timeout=300
            )
            self._connection = pika.BlockingConnection(parameters)
            logger.info(f"Connected to RabbitMQ at {self.host}:{self.port}")
        return self._connection
    
    def _get_channel(self) -> pika.channel.Channel:
        """Get or create RabbitMQ channel"""
        if self._channel is None or self._channel.is_closed:
            connection = self._get_connection()
            self._channel = connection.channel()
        return self._channel
    
    def _ensure_queue(self):
        """Ensure the queue exists and is durable"""
        try:
            channel = self._get_channel()
            channel.queue_declare(
                queue=self.queue_name,
                durable=True,  # Queue survives broker restarts
                exclusive=False,
                auto_delete=False
            )
            logger.debug(f"Queue '{self.queue_name}' ensured")
        except (AMQPConnectionError, AMQPChannelError) as e:
            logger.error(f"Error ensuring queue exists: {e}")
            raise
    
    def _ensure_connection(self):
        """Ensure connection and queue exist (called before operations)"""
        if self._connection is None or (self._connection.is_closed if self._connection else True):
            self._ensure_queue()
    
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
        Add a file to the processing queue via RabbitMQ
        
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
        try:
            self._ensure_connection()
            channel = self._get_channel()
            
            # Create message payload
            message_data = {
                "filename": filename,
                "file_path": file_path,
                "file_hash": file_hash,
                "folder": folder,
                "storage_type": storage_type,
                "source": source,
                "file_classification": file_classification,
                "username": username,
                "status": "pending",
                "created_at": datetime.utcnow().isoformat()
            }
            
            # Publish message to queue
            channel.basic_publish(
                exchange='',
                routing_key=self.queue_name,
                body=json.dumps(message_data),
                properties=pika.BasicProperties(
                    delivery_mode=2,  # Make message persistent
                    content_type='application/json'
                )
            )
            
            logger.info(f"Added file {filename} to RabbitMQ queue '{self.queue_name}'")
            
            # Return response in similar format to FileQueueService
            return {
                "id": None,  # RabbitMQ doesn't provide message IDs in basic_publish
                "filename": filename,
                "file_path": file_path,
                "file_hash": file_hash,
                "folder": folder,
                "storage_type": storage_type,
                "source": source,
                "file_classification": file_classification,
                "username": username,
                "status": "pending",
                "created_at": message_data["created_at"]
            }
            
        except (AMQPConnectionError, AMQPChannelError) as e:
            logger.error(f"Error adding file to RabbitMQ queue: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error adding file to queue: {e}")
            raise
    
    def get_next_file_from_queue(self) -> Optional[Dict[str, Any]]:
        """
        Get the next file from the queue (consume one message)
        
        Returns:
            Dictionary with file details or None if queue is empty
        """
        try:
            self._ensure_connection()
            channel = self._get_channel()
            
            # Get one message from queue (non-blocking)
            method_frame, header_frame, body = channel.basic_get(
                queue=self.queue_name,
                auto_ack=False  # Manual acknowledgment
            )
            
            if method_frame is None:
                # No messages in queue
                return None
            
            # Parse message body
            message_data = json.loads(body)
            
            # Store delivery tag for acknowledgment later
            message_data['_delivery_tag'] = method_frame.delivery_tag
            
            logger.info(f"Retrieved file {message_data.get('filename')} from RabbitMQ queue")
            return message_data
            
        except (AMQPConnectionError, AMQPChannelError) as e:
            logger.error(f"Error getting next file from RabbitMQ queue: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error getting next file from queue: {e}")
            raise
    
    def acknowledge_message(self, delivery_tag: int, success: bool = True):
        """
        Acknowledge message processing
        
        Args:
            delivery_tag: Delivery tag from the message
            success: True to ack (remove from queue), False to nack (requeue)
        """
        try:
            channel = self._get_channel()
            if success:
                channel.basic_ack(delivery_tag=delivery_tag)
                logger.debug(f"Acknowledged message with delivery_tag {delivery_tag}")
            else:
                channel.basic_nack(delivery_tag=delivery_tag, requeue=True)
                logger.debug(f"Rejected and requeued message with delivery_tag {delivery_tag}")
        except (AMQPConnectionError, AMQPChannelError) as e:
            logger.error(f"Error acknowledging message: {e}")
            raise
    
    def get_queue_status(self) -> Dict[str, Any]:
        """
        Get current queue status statistics
        
        Returns:
            Dictionary with queue statistics
        """
        try:
            self._ensure_connection()
            channel = self._get_channel()
            queue_declare_result = channel.queue_declare(
                queue=self.queue_name,
                durable=True,
                passive=True  # Only check if queue exists, don't create
            )
            
            message_count = queue_declare_result.method.message_count
            consumer_count = queue_declare_result.method.consumer_count
            
            return {
                'total': message_count,  # Total messages in queue
                'pending': message_count,  # All messages are pending
                'processing': 0,  # RabbitMQ doesn't track processing state
                'completed': 0,  # Not tracked in RabbitMQ
                'failed': 0,  # Not tracked in RabbitMQ
                'consumer_count': consumer_count
            }
            
        except (AMQPConnectionError, AMQPChannelError) as e:
            logger.error(f"Error getting queue status: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error getting queue status: {e}")
            raise
    
    def close(self):
        """Close RabbitMQ connection"""
        try:
            if self._channel and not self._channel.is_closed:
                self._channel.close()
            if self._connection and not self._connection.is_closed:
                self._connection.close()
            logger.info("RabbitMQ connection closed")
        except Exception as e:
            logger.warning(f"Error closing RabbitMQ connection: {e}")
    
    def __del__(self):
        """Cleanup on deletion"""
        self.close()


#!/usr/bin/env python3
"""
Email Service Utility for SMTP notifications
Handles sending emails for various system events like client creation, user notifications, etc.
"""

import smtplib
import ssl
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
from typing import List, Optional, Dict, Any
import os
import logging
from datetime import datetime
from pathlib import Path

# Setup logging
logger = logging.getLogger(__name__)

class EmailService:
    """SMTP Email service for sending notifications"""
    
    def __init__(self):
        """Initialize email service with configuration from environment variables"""
        self.smtp_server = os.getenv('SMTP_SERVER', 'smtp.gmail.com')
        self.smtp_port = int(os.getenv('SMTP_PORT', '587'))
        self.smtp_username = os.getenv('SMTP_USERNAME')
        self.smtp_password = os.getenv('SMTP_PASSWORD')
        self.from_email = os.getenv('FROM_EMAIL', self.smtp_username)
        self.from_name = os.getenv('FROM_NAME', 'ValidusBoxes System')
        
        # Validate required configuration
        if not self.smtp_username or not self.smtp_password:
            logger.warning("SMTP credentials not configured. Email functionality will be disabled.")
            self.enabled = False
        else:
            self.enabled = True
    
    def send_email(
        self,
        to_emails: List[str],
        subject: str,
        html_body: str,
        text_body: Optional[str] = None,
        cc_emails: Optional[List[str]] = None,
        bcc_emails: Optional[List[str]] = None,
        attachments: Optional[List[Dict[str, Any]]] = None
    ) -> Dict[str, Any]:
        """
        Send email via SMTP
        
        Args:
            to_emails: List of recipient email addresses
            subject: Email subject
            html_body: HTML content of the email
            text_body: Plain text content (optional, will be generated from HTML if not provided)
            cc_emails: List of CC email addresses (optional)
            bcc_emails: List of BCC email addresses (optional)
            attachments: List of attachment dictionaries with 'filename' and 'content' keys
            
        Returns:
            Dictionary with success status and message
        """
        if not self.enabled:
            return {
                "success": False,
                "error": "Email service not configured. Please set SMTP_USERNAME and SMTP_PASSWORD environment variables."
            }
        
        try:
            # Create message
            msg = MIMEMultipart('alternative')
            msg['From'] = f"{self.from_name} <{self.from_email}>"
            msg['To'] = ', '.join(to_emails)
            msg['Subject'] = subject
            
            if cc_emails:
                msg['Cc'] = ', '.join(cc_emails)
            
            # Prepare all recipients
            all_recipients = to_emails.copy()
            if cc_emails:
                all_recipients.extend(cc_emails)
            if bcc_emails:
                all_recipients.extend(bcc_emails)
            
            # Create text version if not provided
            if not text_body:
                # Simple HTML to text conversion (you might want to use a library like html2text for better conversion)
                text_body = html_body.replace('<br>', '\n').replace('<br/>', '\n').replace('<br />', '\n')
                # Remove HTML tags (basic implementation)
                import re
                text_body = re.sub(r'<[^>]+>', '', text_body)
            
            # Add text and HTML parts
            text_part = MIMEText(text_body, 'plain', 'utf-8')
            html_part = MIMEText(html_body, 'html', 'utf-8')
            
            msg.attach(text_part)
            msg.attach(html_part)
            
            # Add attachments if any
            if attachments:
                for attachment in attachments:
                    part = MIMEBase('application', 'octet-stream')
                    part.set_payload(attachment['content'])
                    encoders.encode_base64(part)
                    part.add_header(
                        'Content-Disposition',
                        f'attachment; filename= {attachment["filename"]}'
                    )
                    msg.attach(part)
            
            # Connect to SMTP server and send email
            context = ssl.create_default_context()
            
            with smtplib.SMTP(self.smtp_server, self.smtp_port) as server:
                server.starttls(context=context)
                server.login(self.smtp_username, self.smtp_password)
                server.sendmail(self.from_email, all_recipients, msg.as_string())
            
            logger.info(f"Email sent successfully to {len(all_recipients)} recipients")
            return {
                "success": True,
                "message": f"Email sent successfully to {len(all_recipients)} recipients",
                "recipients": all_recipients
            }
            
        except smtplib.SMTPAuthenticationError as e:
            error_msg = f"SMTP authentication failed: {str(e)}"
            logger.error(error_msg)
            return {"success": False, "error": error_msg}
            
        except smtplib.SMTPRecipientsRefused as e:
            error_msg = f"SMTP recipients refused: {str(e)}"
            logger.error(error_msg)
            return {"success": False, "error": error_msg}
            
        except smtplib.SMTPServerDisconnected as e:
            error_msg = f"SMTP server disconnected: {str(e)}"
            logger.error(error_msg)
            return {"success": False, "error": error_msg}
            
        except Exception as e:
            error_msg = f"Failed to send email: {str(e)}"
            logger.error(error_msg)
            return {"success": False, "error": error_msg}
    
    def send_client_creation_notification(
        self,
        client_data: Dict[str, Any],
        admin_emails: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """
        Send notification email when a new client is created
        
        Args:
            client_data: Dictionary containing client information
            admin_emails: List of admin email addresses to notify
            
        Returns:
            Dictionary with success status and message
        """
        # Determine recipients
        recipients = []
        
        # Add client contact email if available
        if client_data.get('contact_email'):
            recipients.append(client_data['contact_email'])
        
        # Add client admin email if available
        if client_data.get('admin_email') and client_data['admin_email'] not in recipients:
            recipients.append(client_data['admin_email'])
        
        # Add admin emails if provided
        if admin_emails:
            for email in admin_emails:
                if email not in recipients:
                    recipients.append(email)
        
        if not recipients:
            return {
                "success": False,
                "error": "No valid email addresses found for notification"
            }
        
        # Create email content
        subject = f"New Client Created: {client_data.get('name', 'Unknown')}"
        
        html_body = self._create_client_creation_email_html(client_data)
        text_body = self._create_client_creation_email_text(client_data)
        
        return self.send_email(
            to_emails=recipients,
            subject=subject,
            html_body=html_body,
            text_body=text_body
        )
    
    def _create_client_creation_email_html(self, client_data: Dict[str, Any]) -> str:
        """Create HTML email content for client creation notification"""
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        html_content = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <meta charset="UTF-8">
            <style>
                body {{ font-family: Arial, sans-serif; line-height: 1.6; color: #333; }}
                .container {{ max-width: 600px; margin: 0 auto; padding: 20px; }}
                .header {{ background-color: #2c3e50; color: white; padding: 20px; text-align: center; }}
                .content {{ padding: 20px; background-color: #f9f9f9; }}
                .client-info {{ background-color: white; padding: 15px; margin: 10px 0; border-left: 4px solid #3498db; }}
                .footer {{ text-align: center; padding: 20px; color: #666; font-size: 12px; }}
                .field {{ margin: 8px 0; }}
                .label {{ font-weight: bold; color: #2c3e50; }}
                .value {{ color: #555; }}
            </style>
        </head>
        <body>
            <div class="container">
                <div class="header">
                    <h1>New Client Created</h1>
                    <p>ValidusBoxes System Notification</p>
                </div>
                
                <div class="content">
                    <p>A new client has been successfully created in the ValidusBoxes system.</p>
                    
                    <div class="client-info">
                        <h3>Client Information</h3>
                        
                        <div class="field">
                            <span class="label">Client Name:</span>
                            <span class="value">{client_data.get('name', 'N/A')}</span>
                        </div>
                        
                        <div class="field">
                            <span class="label">Client Code:</span>
                            <span class="value">{client_data.get('code', 'N/A')}</span>
                        </div>
                        
                        <div class="field">
                            <span class="label">Client Type:</span>
                            <span class="value">{client_data.get('type', 'N/A')}</span>
                        </div>
                        
                        <div class="field">
                            <span class="label">Status:</span>
                            <span class="value">{'Active' if client_data.get('is_active', True) else 'Inactive'}</span>
                        </div>
                    </div>
                    
                    {self._format_contact_info_html(client_data, 'Contact')}
                    {self._format_contact_info_html(client_data, 'Admin')}
                    
                    <div class="client-info">
                        <h3>System Information</h3>
                        <div class="field">
                            <span class="label">Created At:</span>
                            <span class="value">{current_time}</span>
                        </div>
                        <div class="field">
                            <span class="label">Client ID:</span>
                            <span class="value">{client_data.get('id', 'N/A')}</span>
                        </div>
                    </div>
                </div>
                
                <div class="footer">
                    <p>This is an automated notification from the ValidusBoxes system.</p>
                    <p>Please do not reply to this email.</p>
                </div>
            </div>
        </body>
        </html>
        """
        
        return html_content
    
    def _format_contact_info_html(self, client_data: Dict[str, Any], contact_type: str) -> str:
        """Format contact information for HTML email"""
        prefix = contact_type.lower()
        
        contact_name = ""
        if client_data.get(f'{prefix}_first_name') or client_data.get(f'{prefix}_last_name'):
            title = client_data.get(f'{prefix}_title', '')
            first_name = client_data.get(f'{prefix}_first_name', '')
            last_name = client_data.get(f'{prefix}_last_name', '')
            contact_name = f"{title} {first_name} {last_name}".strip()
        
        contact_email = client_data.get(f'{prefix}_email', '')
        contact_number = client_data.get(f'{prefix}_number', '')
        job_title = client_data.get(f'{prefix}_job_title', '')
        
        if not any([contact_name, contact_email, contact_number, job_title]):
            return ""
        
        html = f"""
        <div class="client-info">
            <h3>{contact_type} Information</h3>
        """
        
        if contact_name:
            html += f'<div class="field"><span class="label">Name:</span><span class="value">{contact_name}</span></div>'
        
        if contact_email:
            html += f'<div class="field"><span class="label">Email:</span><span class="value">{contact_email}</span></div>'
        
        if contact_number:
            html += f'<div class="field"><span class="label">Phone:</span><span class="value">{contact_number}</span></div>'
        
        if job_title:
            html += f'<div class="field"><span class="label">Job Title:</span><span class="value">{job_title}</span></div>'
        
        html += "</div>"
        return html
    
    def _create_client_creation_email_text(self, client_data: Dict[str, Any]) -> str:
        """Create plain text email content for client creation notification"""
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        text_content = f"""
NEW CLIENT CREATED - ValidusBoxes System Notification

A new client has been successfully created in the ValidusBoxes system.

CLIENT INFORMATION:
- Client Name: {client_data.get('name', 'N/A')}
- Client Code: {client_data.get('code', 'N/A')}
- Client Type: {client_data.get('type', 'N/A')}
- Status: {'Active' if client_data.get('is_active', True) else 'Inactive'}

{self._format_contact_info_text(client_data, 'CONTACT')}
{self._format_contact_info_text(client_data, 'ADMIN')}

SYSTEM INFORMATION:
- Created At: {current_time}
- Client ID: {client_data.get('id', 'N/A')}

This is an automated notification from the ValidusBoxes system.
Please do not reply to this email.
        """
        
        return text_content.strip()
    
    def _format_contact_info_text(self, client_data: Dict[str, Any], contact_type: str) -> str:
        """Format contact information for text email"""
        prefix = contact_type.lower()
        
        contact_name = ""
        if client_data.get(f'{prefix}_first_name') or client_data.get(f'{prefix}_last_name'):
            title = client_data.get(f'{prefix}_title', '')
            first_name = client_data.get(f'{prefix}_first_name', '')
            last_name = client_data.get(f'{prefix}_last_name', '')
            contact_name = f"{title} {first_name} {last_name}".strip()
        
        contact_email = client_data.get(f'{prefix}_email', '')
        contact_number = client_data.get(f'{prefix}_number', '')
        job_title = client_data.get(f'{prefix}_job_title', '')
        
        if not any([contact_name, contact_email, contact_number, job_title]):
            return ""
        
        text = f"\n{contact_type} INFORMATION:"
        
        if contact_name:
            text += f"\n- Name: {contact_name}"
        
        if contact_email:
            text += f"\n- Email: {contact_email}"
        
        if contact_number:
            text += f"\n- Phone: {contact_number}"
        
        if job_title:
            text += f"\n- Job Title: {job_title}"
        
        return text

# Global email service instance
email_service = EmailService()

def get_email_service() -> EmailService:
    """Get the global email service instance"""
    return email_service

# Convenience functions
def send_client_creation_email(client_data: Dict[str, Any], admin_emails: Optional[List[str]] = None) -> Dict[str, Any]:
    """Send client creation notification email"""
    return email_service.send_client_creation_notification(client_data, admin_emails)

def send_custom_email(
    to_emails: List[str],
    subject: str,
    html_body: str,
    text_body: Optional[str] = None,
    cc_emails: Optional[List[str]] = None,
    bcc_emails: Optional[List[str]] = None
) -> Dict[str, Any]:
    """Send custom email"""
    return email_service.send_email(
        to_emails=to_emails,
        subject=subject,
        html_body=html_body,
        text_body=text_body,
        cc_emails=cc_emails,
        bcc_emails=bcc_emails
    )

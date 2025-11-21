# Email Service Setup Guide

This guide explains how to configure and use the SMTP email service for client creation notifications in ValidusBoxes.

## Overview

The email service automatically sends notifications when new clients are created. It supports:
- HTML and plain text emails
- Multiple recipients (client contacts, admins)
- Professional email templates
- Error handling and logging
- Non-blocking email sending (client creation won't fail if email fails)

## Configuration

### 1. Environment Variables

Add the following variables to your `.env` file:

```bash
# SMTP Configuration
SMTP_SERVER=smtp.gmail.com
SMTP_PORT=587
SMTP_USERNAME=your-email@gmail.com
SMTP_PASSWORD=your-app-password
FROM_EMAIL=your-email@gmail.com
FROM_NAME=ValidusBoxes System

# Admin Email Configuration (comma-separated)
ADMIN_EMAILS=admin1@company.com,admin2@company.com
```

### 2. Gmail Setup (Recommended)

For Gmail, you need to use App Passwords:

1. **Enable 2-Factor Authentication** on your Google account
2. **Generate an App Password**:
   - Go to Google Account settings
   - Security → 2-Step Verification → App passwords
   - Generate a password for "Mail"
   - Use this password in `SMTP_PASSWORD`

### 3. Other Email Providers

#### Outlook/Hotmail
```bash
SMTP_SERVER=smtp-mail.outlook.com
SMTP_PORT=587
```

#### Yahoo
```bash
SMTP_SERVER=smtp.mail.yahoo.com
SMTP_PORT=587
```

#### Custom SMTP Server
```bash
SMTP_SERVER=your-smtp-server.com
SMTP_PORT=587  # or 465 for SSL
```

## Testing the Email Service

### 1. Run the Test Script

```bash
python test_email_service.py
```

This will:
- Verify your configuration
- Send a test email
- Test client creation notification

### 2. Manual Testing

You can also test the email service programmatically:

```python
from utils.email_service import send_client_creation_email

# Sample client data
client_data = {
    "id": 1,
    "name": "Test Company",
    "code": "test001",
    "type": "institutional",
    "contact_email": "contact@testcompany.com",
    "admin_email": "admin@testcompany.com"
}

# Send notification
result = send_client_creation_email(client_data, ["admin@yourcompany.com"])
print(result)
```

## Email Templates

### Client Creation Email

The system automatically generates professional emails with:

- **HTML Format**: Styled with CSS for better presentation
- **Plain Text Format**: For email clients that don't support HTML
- **Client Information**: Name, code, type, contact details
- **System Information**: Creation timestamp, client ID
- **Professional Styling**: Company branding and clear layout

### Customizing Templates

To customize email templates, edit the methods in `utils/email_service.py`:

- `_create_client_creation_email_html()` - HTML template
- `_create_client_creation_email_text()` - Plain text template

## API Integration

### Client Creation with Email

When you create a client via the API, emails are sent automatically:

```python
# POST /clients
{
    "client-name": "New Company",
    "client-code": "newco001",
    "contact-email": "contact@newcompany.com",
    "admin-email": "admin@newcompany.com"
}
```

The system will:
1. Create the client in the database
2. Send notification emails to:
   - Client contact email
   - Client admin email
   - System admin emails (from `ADMIN_EMAILS`)

### Email Recipients

Emails are sent to:
1. **Client Contact Email** (if provided)
2. **Client Admin Email** (if provided and different from contact)
3. **System Admin Emails** (from `ADMIN_EMAILS` environment variable)

## Error Handling

### Non-Blocking Design

- Client creation **never fails** due to email issues
- Email errors are logged but don't affect the API response
- You'll see email status in the application logs

### Common Issues

#### Authentication Failed
```
SMTP authentication failed: (535, '5.7.8 Username and Password not accepted')
```
**Solution**: Check your SMTP credentials, use App Passwords for Gmail

#### Connection Refused
```
SMTP server disconnected: [Errno 111] Connection refused
```
**Solution**: Check SMTP server and port settings

#### No Recipients
```
No valid email addresses found for notification
```
**Solution**: Ensure client has contact_email or admin_email, or set ADMIN_EMAILS

### Logging

Email service logs are available in your application logs:

```
INFO: Client creation email sent successfully for client 123
WARNING: Failed to send client creation email for client 123: SMTP authentication failed
ERROR: Error sending client creation email for client 123: Connection timeout
```

## Security Considerations

### 1. Environment Variables
- Never commit `.env` files to version control
- Use strong, unique passwords
- Rotate passwords regularly

### 2. Email Content
- No sensitive data in email content
- Client information is already in the system
- Emails are for notification purposes only

### 3. SMTP Security
- Use TLS/SSL connections (port 587 or 465)
- Avoid unencrypted connections
- Use App Passwords instead of main passwords

## Troubleshooting

### Email Not Received

1. **Check Spam Folder**: Emails might be filtered
2. **Verify Recipients**: Ensure email addresses are correct
3. **Check Logs**: Look for error messages in application logs
4. **Test Configuration**: Run `test_email_service.py`

### Configuration Issues

1. **Missing Variables**: Ensure all required environment variables are set
2. **Wrong Credentials**: Double-check SMTP username and password
3. **Firewall**: Ensure SMTP ports are not blocked
4. **Provider Limits**: Check if your email provider has sending limits

### Performance

- Emails are sent asynchronously (non-blocking)
- No impact on API response times
- Failed emails are logged but don't retry automatically

## Advanced Configuration

### Custom SMTP Settings

For advanced SMTP configurations, you can modify `utils/email_service.py`:

```python
# Custom SSL context
context = ssl.create_default_context()
context.check_hostname = False
context.verify_mode = ssl.CERT_NONE

# Custom timeout settings
server.timeout = 30
```

### Multiple Email Providers

You can configure different SMTP settings for different environments:

```bash
# Development
SMTP_SERVER=smtp.gmail.com
SMTP_USERNAME=dev@company.com

# Production
SMTP_SERVER=smtp.company.com
SMTP_USERNAME=noreply@company.com
```

## Support

If you encounter issues:

1. Check the application logs for error messages
2. Run the test script to verify configuration
3. Verify your SMTP provider's documentation
4. Check firewall and network connectivity

The email service is designed to be robust and non-intrusive, ensuring that client creation always succeeds regardless of email delivery status.

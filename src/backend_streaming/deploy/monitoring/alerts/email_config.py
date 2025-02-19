from email.message import EmailMessage
import smtplib
import os
from typing import List

RECIPIENT_EMAIL = "sothebeautifulgame@gmail.com"

def send_alert(content: str) -> None:
    """Send alert email with standardized format"""
    msg = EmailMessage()
    msg.set_content(content)
    
    msg['Subject'] = "DETECTED ISSUE"
    msg['From'] = os.getenv('EMAIL_FROM')
    msg['To'] = RECIPIENT_EMAIL

    # Get email settings from environment
    smtp_server = os.getenv('SMTP_SERVER')
    smtp_port = int(os.getenv('SMTP_PORT', '587'))
    smtp_user = os.getenv('SMTP_USER')
    smtp_password = os.getenv('SMTP_PASSWORD')

    # Send email using Gmail's SMTP server
    with smtplib.SMTP(smtp_server, smtp_port) as server:
        server.starttls()  # Enable TLS
        server.login(smtp_user, smtp_password)
        server.send_message(msg)

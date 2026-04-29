import os
import smtplib
import ssl
from email.message import EmailMessage

from logger_config import get_logger

logger = get_logger("EmailService")


def send_email(subject: str, body: str, to_email: str, *, from_email: str, app_password: str) -> None:
    if not to_email:
        raise ValueError("Recipient email missing")
    if not from_email or not app_password:
        raise ValueError("Sender email or app password missing")

    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = from_email
    msg["To"] = to_email
    msg.set_content(body)

    host = os.getenv("EMAIL_SMTP_HOST", "smtp.gmail.com")
    port = int(os.getenv("EMAIL_SMTP_PORT", "465"))
    use_ssl = os.getenv("EMAIL_SMTP_USE_SSL", "true").lower() == "true"

    if use_ssl:
        context = ssl.create_default_context()
        with smtplib.SMTP_SSL(host, port, context=context) as server:
            server.login(from_email, app_password)
            server.send_message(msg)
    else:
        with smtplib.SMTP(host, port) as server:
            server.starttls(context=ssl.create_default_context())
            server.login(from_email, app_password)
            server.send_message(msg)

    logger.info("Lead email sent to %s.", to_email)

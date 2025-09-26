import smtplib

from fastapi import HTTPException

from vitrine.settings import Settings


def get_smtp():
    settings = Settings()
    smtp_connection = smtplib.SMTP_SSL(settings.SMTP_HOST, settings.SMTP_PORT)
    try:
        smtp_connection.login(settings.SMTP_USER, settings.SMTP_PASS)
        yield smtp_connection
    except smtplib.SMTPAuthenticationError:
        raise HTTPException(
            status_code=500,
            detail='Authentication with SMTP server failed.',
        )
    finally:
        smtp_connection.quit()

import smtplib

from vitrine.core.settings import Settings


def get_smtp():
    SETTINGS = Settings()
    smtp_connection = smtplib.SMTP()
    try:
        smtp_connection.connect(SETTINGS.SMTP_HOST, SETTINGS.SMTP_PORT)
        smtp_connection.starttls()
        smtp_connection.login(SETTINGS.SMTP_USER, SETTINGS.SMTP_PASS)
        return smtp_connection
    except Exception as e:
        if smtp_connection:
            smtp_connection.quit()
        raise RuntimeError(f'Failed to connect/login to SMTP server: {e}')

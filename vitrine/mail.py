import smtplib

from fastapi import HTTPException

from vitrine.settings import Settings


def get_smtp():
    settings = Settings()
    smtp_connection = None

    try:
        if settings.SMTP_PORT == 465:
            smtp_connection = smtplib.SMTP_SSL(
                settings.SMTP_HOST, settings.SMTP_PORT
            )

        else:
            smtp_connection = smtplib.SMTP(
                settings.SMTP_HOST, settings.SMTP_PORT
            )

        if settings.SMTP_PORT == 587:
            smtp_connection.starttls()

        if settings.SMTP_USER and settings.SMTP_PASS:
            smtp_connection.login(settings.SMTP_USER, settings.SMTP_PASS)

        yield smtp_connection

    except smtplib.SMTPAuthenticationError:
        raise HTTPException(
            status_code=500,
            detail='Authentication with SMTP server failed.',
        )
    except smtplib.SMTPNotSupportedError:
        raise HTTPException(
            status_code=500,
            detail='SMTP AUTH not supported. Clear SMTP_USER/PASS in local .env.',
        )
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f'Failed to connect to SMTP server: {e}',
        )
    finally:
        if smtp_connection:
            smtp_connection.quit()

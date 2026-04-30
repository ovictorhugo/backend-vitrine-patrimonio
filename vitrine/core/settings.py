from typing import List

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file='.env', env_file_encoding='utf-8'
    )

    DATABASE_URL: str
    CLIENT: str = 'http://localhost:8080'

    SECRET_KEY: str = 'SECRET_KEY'
    ACCESS_TOKEN_EXPIRE_MINUTES: int = 30
    ALGORITHM: str = 'HS256'

    ALLOWED_ORIGINS: List[str] = [
        'http://localhost:8000',
        'http://localhost:8080',
    ]

    ROOT_PATH: str = ''

    SMTP_HOST: str = 'mailpit'
    SMTP_PORT: int = 1025
    SMTP_USER: str = 'noreply@vitrine.local'
    SMTP_PASS: str = str()
    SMTP_TLS: bool = False
    SMTP_SSL: bool = False
    CERT_KEY_VITRINE: str = "123456789"

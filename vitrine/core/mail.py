import smtplib
from typing import Generator
from vitrine.core.settings import Settings

def get_smtp() -> Generator[smtplib.SMTP, None, None]:
    SETTINGS = Settings()

    # Validação de Segurança: Impede tentar conectar se não houver Host
    if not SETTINGS.SMTP_HOST:
        raise RuntimeError("Configuração SMTP_HOST está vazia. Verifique seu .env")

    # Inicializa JÁ com os dados (mais seguro para o starttls identificar o hostname)
    try:
        smtp_connection = smtplib.SMTP(SETTINGS.SMTP_HOST, SETTINGS.SMTP_PORT)
    except Exception as e:
        raise RuntimeError(f"Não foi possível conectar ao servidor {SETTINGS.SMTP_HOST}: {e}")
    
    try:
        # TLS: Agora o objeto já sabe o hostname pois foi passado no construtor acima
        if SETTINGS.SMTP_TLS:
            smtp_connection.starttls()
        
        # Login
        if SETTINGS.SMTP_USER and SETTINGS.SMTP_PASS:
            try:
                smtp_connection.login(SETTINGS.SMTP_USER, SETTINGS.SMTP_PASS)
            except smtplib.SMTPNotSupportedError:
                pass # Mailpit/Servidor aberto
            except smtplib.SMTPException as e:
                raise RuntimeError(f'Erro de autenticação SMTP: {e}')
        
        yield smtp_connection

    except Exception as e:
        try:
            smtp_connection.quit()
        except Exception:
            pass
        # Relança o erro com mais contexto
        raise RuntimeError(f'Erro durante configuração SMTP: {e}')

    finally:
        try:
            smtp_connection.quit()
        except (smtplib.SMTPServerDisconnected, smtplib.SMTPException):
            pass        
        try:
            smtp_connection.close()
        except Exception:
            pass
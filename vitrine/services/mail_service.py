from email.message import EmailMessage
from typing import Any

from vitrine.dependencies import Mail
from vitrine.models import User
from vitrine.settings import Settings


async def send_email(mail: Mail, user: User, content: Any):
    msg = EmailMessage()
    msg['Subject'] = 'ASSUNTO'
    msg['From'] = Settings().SMTP_USER
    msg['To'] = 'geu_costa@outlook.com'  # user.email
    msg.set_content('CONTEUDO')

    try:
        mail.send_message(msg)
    except Exception:
        pass

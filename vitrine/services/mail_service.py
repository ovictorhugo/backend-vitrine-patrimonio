from email.message import EmailMessage
from typing import Any

from vitrine.core.dependencies import Mail
from vitrine.core.settings import Settings
from vitrine.models import User


async def send_email(mail: Mail, user: User, content: Any):
    msg = EmailMessage()
    msg['Subject'] = 'ASSUNTO'
    msg['From'] = Settings().SMTP_USER
    msg['To'] = user.email
    msg.set_content('CONTEUDO')
    mail.send_message(msg)

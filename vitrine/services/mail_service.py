from email.message import EmailMessage
from typing import Any

from vitrine.dependencies import Mail
from vitrine.models import User
from vitrine.settings import Settings


async def send_email(mail: Mail, user: User, content: Any):
    msg = EmailMessage()
    msg['Subject'] = 'ASSUNTO'
    msg['From'] = Settings().SMTP_USER
    msg['To'] = user.email
    msg.set_content('CONTEUDO')
    try:
        print('DEBUG 1')
        mail.send_message(msg)
        print('DEBUG 2')
    except Exception as e:
        print(f'Erro ao enviar email: {e}')

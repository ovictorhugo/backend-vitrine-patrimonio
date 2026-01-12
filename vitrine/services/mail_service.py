from email.message import EmailMessage
from typing import Any

from vitrine.core.dependencies import Mail
from vitrine.core.settings import Settings
from vitrine.models import User

emails = {
    'Transferencia_1': {  # Para o Guardião (quem aceitou)
        'Assunto': 'Confirmação de aceite de transferência - Vitrine Patrimônio',
        'Conteudo': (
            "Prezado(a),\n\n"
            "Confirmamos que você aceitou a transferência de um item no site Vitrine.\n\n"
            "O processo administrativo foi iniciado automaticamente. Em breve, você será "
            "notificado para realizar a assinatura digital no Termo de Transferência gerado pelo sistema.\n\n"
            "Atenciosamente,\n"
            "Equipe Vitrine Patrimônio"
        )
    },
    'Transferencia_2': {  # Para o Solicitante (quem pediu)
        'Assunto': 'Sua solicitação de transferência foi aceita - Vitrine Patrimônio',
        'Conteudo': (
            "Prezado(a),\n\n"
            "Informamos que o pedido de transferência de um item foi aceito pelo atual responsável.\n\n"
            "O fluxo de assinaturas foi iniciado. Por favor, acesse o sistema para acompanhar o trâmite "
            "e realizar a sua assinatura no Termo de Transferência assim que estiver disponível.\n\n"
            "Atenciosamente,\n"
            "Equipe Vitrine Patrimônio"
        )
    }
}

async def send_email(mail: Mail, user: User, content: str):
    msg = EmailMessage()
    print(content)
    msg['Subject'] = emails[content]["Assunto"]
    msg['From'] = Settings().SMTP_USER
    msg['To'] = 'joaomont@ufmg.br' #user.email
    msg.set_content(emails[content]["Conteudo"])
    mail.send_message(msg)

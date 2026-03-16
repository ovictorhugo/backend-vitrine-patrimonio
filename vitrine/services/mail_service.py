from email.message import EmailMessage
from typing import Optional

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
    msg['Subject'] = emails[content]["Assunto"]
    msg['From'] = Settings().SMTP_USER
    msg['To'] = user.email
    msg.set_content(emails[content]["Conteudo"])
    mail.send_message(msg)


async def send_custom_email(
    mail: Mail, 
    user: User, 
    subject: str, 
    content: str, 
    attachment_bytes: Optional[bytes] = None,
    attachment_filename: str = "documento.pdf"
):
    """Envia um e-mail customizado, com suporte opcional a anexo em PDF."""
    msg = EmailMessage()
    msg['Subject'] = subject
    msg['From'] = Settings().SMTP_USER
    msg['To'] = user.email
    msg.set_content(content)

    # Verifica se um anexo foi passado
    if attachment_bytes:
        msg.add_attachment(
            attachment_bytes,
            maintype='application',
            subtype='pdf',
            filename=attachment_filename
        )

    mail.send_message(msg)
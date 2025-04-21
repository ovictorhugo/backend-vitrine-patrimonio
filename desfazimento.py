from vitrine.dao import Connection
import pandas as pd
import smtplib

servidor_email = smtplib.SMTP("smtp.gmail.com", 587)
servidor_email.starttls()
servidor_email.login("vitrinetmp123", "vitrinetmp123123")


remetente = "vitrinetmp123@gmail.com"
destinatarios = ["geucosta167@gmail.com"]
conteudo = "Olá, este é um email de teste."

servidor_email.sendmail(remetente, destinatarios, conteudo)

servidor_email.quit()


conn = Connection()


SCRIPT_SQL = """
    SELECT patrimonio_id, fp.user_id, email
    FROM formulario_patrimonio fp
    LEFT JOIN users u ON u.user_id = fp.user_id
    WHERE created_at < NOW() - INTERVAL '6 months'
        AND desfazimento = false;
    """
result = conn.select(SCRIPT_SQL)

desfazimento = pd.DataFrame(result, columns=["patrimonio_id", "user_id", "email"])


SCRIPT_SQL = """
    UPDATE formulario_patrimonio SET desfazimento = true
    WHERE patrimonio_id = %(patrimonio_id)s
    """

for _, data in desfazimento.iterrows():
    print(data["email"])
    conn.exec(SCRIPT_SQL, {"patrimonio_id": data["patrimonio_id"]})

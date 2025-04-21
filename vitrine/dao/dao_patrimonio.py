from ..dao import Connection
import pandas as pd

conn = Connection()


def get_patrimonio(codigo, id_item):
    if codigo or id_item:
        SCRIPT_SQL = """
            SELECT 
                bo.id_item, 
                bo.observacoes, 
                bo.estado_do_bem, 
                bo.tipo, 
                bo.codigo, 
                bo.imagens, 
                bo.email_insercao, 
                jsonb_build_object(
                    'user_id', u.user_id,
                    'display_name', u.display_name,
                    'email', u.email,
                    'uid', u.uid,
                    'photo_url', u.photo_url,
                    'lattes_id', u.lattes_id,
                    'institution_id', u.institution_id,
                    'provider', u.provider,
                    'linkedin', u.linkedin,
                    'verify', u.verify
                ) AS user_info,
                bo.status
            FROM 
                novo_item bo
            LEFT JOIN 
                public.users u 
                ON u.user_id = bo.id_usuario
            """
        if codigo:
            SCRIPT_SQL += "WHERE bo.codigo = %s"
        elif id_item:
            SCRIPT_SQL += "WHERE bo.id_item = %s"
        registry = conn.select(SCRIPT_SQL, [codigo or id_item])
    else:
        SCRIPT_SQL = """
            SELECT 
                bo.id_item, 
                bo.observacoes, 
                bo.estado_do_bem, 
                bo.tipo, 
                bo.codigo, 
                bo.imagens, 
                bo.email_insercao, 
                jsonb_build_object(
                    'user_id', u.user_id,
                    'display_name', u.display_name,
                    'email', u.email,
                    'uid', u.uid,
                    'photo_url', u.photo_url,
                    'lattes_id', u.lattes_id,
                    'institution_id', u.institution_id,
                    'provider', u.provider,
                    'linkedin', u.linkedin,
                    'verify', u.verify
                ) AS user_info,
                bo.status
            FROM 
                public.novo_item bo
            LEFT JOIN 
                public.users u 
                ON u.user_id = bo.id_usuario
            """
        registry = conn.select(SCRIPT_SQL)

    df = pd.DataFrame(
        registry,
        columns=[
            "id_item",
            "observacoes",
            "estado_do_bem",
            "tipo",
            "codigo",
            "imagens",
            "email_insercao",
            "usuario",
            "status",
        ],
    )

    resultado = df.to_dict(orient="records")
    return resultado

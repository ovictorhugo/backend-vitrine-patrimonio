from vitrine.dao import Connection
import pandas as pd

conn = Connection()


def solicitar(form):
    SCRIPT_SQL = """ 
        UPDATE formulario_patrimonio 
            SET estado_transferencia = 'TRANSFERENCIA SOLICITADA'
        WHERE patrimonio_id = %(patrimonio_id)s;
        """
    conn.exec(SCRIPT_SQL, form)

    SCRIPT_SQL = """ 
        INSERT INTO transferencia (ofertante, loc_ofertante, solicitante, loc_solicitante, patrimonio_id) 
        VALUES (%(ofertante)s, %(loc_ofertante)s, %(solicitante)s, %(loc_solicitante)s, %(patrimonio_id)s); 
        """
    conn.exec(SCRIPT_SQL, form)


def listar_solicitacao(ofertante=None, solicitante=None, patrimonio_id=None):
    params = {}
    conditions = []

    if ofertante is not None:
        conditions.append("t.ofertante = %(ofertante)s")
        params["ofertante"] = ofertante

    if solicitante is not None:
        conditions.append("t.solicitante = %(solicitante)s")
        params["solicitante"] = solicitante

    if patrimonio_id is not None:
        conditions.append("t.patrimonio_id = %(patrimonio_id)s")
        params["patrimonio_id"] = patrimonio_id

    where_clause = "WHERE " + " AND ".join(conditions) if conditions else ""

    SCRIPT_SQL = f"""
        SELECT 
            jsonb_build_object(
                'ofertante', jsonb_build_object(
                    'user_id', uo.user_id,
                    'display_name', uo.display_name,
                    'email', uo.email,
                    'photo_url', uo.photo_url,
                    'lattes_id', uo.lattes_id,
                    'institution', uo.institution,
                    'provider', uo.provider,
                    'linkedin', uo.linkedin,
                    'verify', uo.verify,
                    'phone', uo.phone,
                    'matricula', uo.matricula
                ),
                'loc_ofertante', t.loc_ofertante,
                'solicitante', jsonb_build_object(
                    'user_id', us.user_id,
                    'display_name', us.display_name,
                    'email', us.email,
                    'photo_url', us.photo_url,
                    'lattes_id', us.lattes_id,
                    'institution', us.institution,
                    'provider', us.provider,
                    'linkedin', us.linkedin,
                    'verify', us.verify,
                    'phone', us.phone,
                    'matricula', us.matricula
                ),
                'loc_solicitante', t.loc_solicitante,
                'patrimonio_id', t.patrimonio_id
            ) AS transferencia_profile
        FROM transferencia t
        LEFT JOIN users uo ON t.ofertante = uo.user_id
        LEFT JOIN users us ON t.solicitante = us.user_id
        {where_clause};
        """

    resultado = conn.select(SCRIPT_SQL, params)
    dataframe = pd.DataFrame(resultado, columns=["lista"])
    return dataframe.to_dict(orient="records")


def recusar_solicitacao(user_id, patrimonio_id):
    SCRIPT_SQL = """ 
        UPDATE formulario_patrimonio 
            SET estado_transferencia = 'TRANSFERENCIA SOLICITADA'
        WHERE patrimonio_id = %(patrimonio_id)s;
        """
    conn.exec(SCRIPT_SQL, {"patrimonio_id": patrimonio_id})
    SCRIPT_SQL = """
        DELETE FROM transferencia 
        WHERE 1 = 1
            AND solicitante = %(user_id)s 
            AND patrimonio_id = %(patrimonio_id)s;
        """
    conn.exec(SCRIPT_SQL, {"user_id": user_id, "patrimonio_id": patrimonio_id})


def aceitar_solicitacao(user_id, patrimonio_id):
    SCRIPT_SQL = """
        DELETE FROM transferencia 
        WHERE 1 = 1
            AND solicitante = %(user_id)s 
            AND patrimonio_id = %(patrimonio_id)s;
        """
    conn.exec(SCRIPT_SQL, {"user_id": user_id, "patrimonio_id": patrimonio_id})

    SCRIPT_SQL = """ 
        UPDATE formulario_patrimonio 
            SET estado_transferencia = 'TRANSFERIDO'
        WHERE patrimonio_id = %(patrimonio_id)s;
        """
    conn.exec(SCRIPT_SQL, {"patrimonio_id": patrimonio_id})

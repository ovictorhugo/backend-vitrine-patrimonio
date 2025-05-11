from vitrine.dao import Connection
import pandas as pd

conn = Connection()


def insert_form(form):
    print(form)
    SCRIPT_SQL = f"SELECT matricula FROM users WHERE user_id = '{form['user_id']}'"

    form["matricula"] = conn.select(SCRIPT_SQL, form)[0][0]

    SCRIPT_SQL = """
    INSERT INTO public.formulario_patrimonio
    (patrimonio_id, num_patrimonio, loc, observacao, matricula, user_id, vitrine, condicao, desfazimento, verificado, num_verificacao, codigo_atm, situacao)
    VALUES (%(patrimonio_id)s, %(num_patrimonio)s, %(loc)s, %(observacao)s, %(matricula)s, %(user_id)s, %(vitrine)s, %(condicao)s, %(desfazimento)s, %(verificado)s, %(num_verificacao)s, %(codigo_atm)s, %(situacao)s);
    """
    conn.exec(SCRIPT_SQL, form)


def delete_patrimonio(patrimonio_id):
    SCRIPT_SQL = """
        DELETE FROM formulario_patrimonio WHERE patrimonio_id = %(patrimonio_id)s;
        """
    conn.exec(SCRIPT_SQL, {"patrimonio_id": patrimonio_id})


def update_patrimonio(form):
    updates = []
    if "user_id" in form:
        SCRIPT_SQL = f"SELECT matricula FROM users WHERE user_id = '{form['user_id']}'"
        form["matricula"] = conn.select(SCRIPT_SQL, form)[0][0]
        updates.append("matricula = %(matricula)s")
    if "num_patrimonio" in form:
        updates.append("num_patrimonio = %(num_patrimonio)s")
    if "loc" in form:
        updates.append("loc = %(loc)s")
    if "observacao" in form:
        updates.append("observacao = %(observacao)s")
    if "vitrine" in form:
        updates.append("vitrine = %(vitrine)s")
    if "condicao" in form:
        updates.append("condicao = %(condicao)s")
    if "imagens" in form:
        updates.append("imagens = %(imagens)s")
    if "desfazimento" in form:
        updates.append("desfazimento = %(desfazimento)s")
    if "verificado" in form:
        updates.append("verificado = %(verificado)s")
    if "num_verificacao" in form:
        updates.append("num_verificacao = %(num_verificacao)s")
    if "codigo_atm" in form:
        updates.append("codigo_atm = %(codigo_atm)s")
    if "situacao" in form:
        updates.append("situacao = %(situacao)s")
    if "estado_transferencia" in form:
        updates.append("estado_transferencia = %(estado_transferencia)s")

    set_clause = ", ".join(updates)

    SCRIPT_SQL = f"""
        UPDATE formulario_patrimonio
        SET {set_clause}
        WHERE patrimonio_id = %(patrimonio_id)s
        """
    conn.exec(SCRIPT_SQL, form)


def buscar_patrimonio(
    verificado, loc, user_id, patrimonio_id, mat_nom, estado_transferencia, desfazimento
):
    params = {}

    filter_patrimonio_id = str()
    if patrimonio_id:
        params["patrimonio_id"] = patrimonio_id
        filter_patrimonio_id = "AND fp.patrimonio_id = %(patrimonio_id)s"

    filter_user = str()
    if user_id:
        params["user_id"] = user_id
        filter_user = """
            AND fp.user_id = %(user_id)s
            """

    filter_loc = str()
    if loc:
        params["loc"] = loc
        filter_loc = """
            AND fp.loc = %(loc)s
            """

    filter_verificado = str()
    if verificado:
        if verificado == "false":
            verificado = False
        elif verificado == "true":
            verificado = True

        params["verificado"] = verificado
        filter_verificado = """
            AND fp.verificado = %(verificado)s
            """

    filter_mat_nom = str()
    if mat_nom:
        params["mat_nom"] = mat_nom
        filter_mat_nom = """
            AND p.mat_nom = %(mat_nom)s
            """

    filter_estado_transferencia = str()
    if estado_transferencia:
        params["estado_transferencia"] = estado_transferencia
        filter_estado_transferencia = """
            AND fp.estado_transferencia = %(estado_transferencia)s
            """

    filter_desfazimento = str()
    if desfazimento:
        if desfazimento == "false":
            desfazimento = False
        elif desfazimento == "true":
            desfazimento = True
        params["desfazimento"] = desfazimento
        filter_desfazimento = """
            AND fp.desfazimento = %(desfazimento)s
            """

    SCRIPT_SQL = f"""
        SELECT fp.patrimonio_id, fp.num_patrimonio, fp.loc, fp.observacao, 
            fp.user_id, fp.vitrine, fp.condicao, fp.imagens, fp.desfazimento, 
            fp.verificado, fp.num_verificacao, fp.codigo_atm, fp.situacao, 
            fp.material, p.bem_cod, p.bem_dgv, p.bem_num_atm, p.csv_cod, 
            p.bem_serie, p.bem_sta, p.bem_val, p.tre_cod, p.bem_dsc_com, 
            p.uge_cod, p.uge_nom, p.org_cod, p.uge_siaf, p.org_nom, p.set_cod, 
            p.set_nom, p.loc_cod, p.loc_nom, p.ite_mar, p.ite_mod, p.tgr_cod, 
            p.grp_cod, p.ele_cod, p.sbe_cod, p.mat_cod, p.mat_nom, p.pes_cod, 
            p.pes_nome, u.display_name, u.email, u.matricula, u.phone, 
            f.qtd_de_favorito, fp.estado_transferencia, fp.created_at,
            u.ramal, u.matricula, u.telephone
        FROM public.formulario_patrimonio AS fp 
        LEFT JOIN users u ON u.user_id = fp.user_id 
        LEFT JOIN patrimonio p ON fp.num_patrimonio = p.bem_cod 
        LEFT JOIN (SELECT COUNT(*) as qtd_de_favorito, id FROM favoritos GROUP BY id) f ON f.id = fp.patrimonio_id 
        WHERE 1 = 1
            {filter_loc}
            {filter_user}
            {filter_mat_nom}
            {filter_patrimonio_id}
            {filter_estado_transferencia}
            {filter_desfazimento}
            {filter_verificado};
            """
    result = conn.select(SCRIPT_SQL, params)
    dataframe = pd.DataFrame(
        result,
        columns=[
            "patrimonio_id",
            "num_patrimonio",
            "loc",
            "observacao",
            "user_id",
            "vitrine",
            "condicao",
            "imagens",
            "desfazimento",
            "verificado",
            "num_verificacao",
            "codigo_atm",
            "situacao",
            "material",
            "bem_cod",
            "bem_dgv",
            "bem_num_atm",
            "csv_cod",
            "bem_serie",
            "bem_sta",
            "bem_val",
            "tre_cod",
            "bem_dsc_com",
            "uge_cod",
            "uge_nom",
            "org_cod",
            "uge_siaf",
            "org_nom",
            "set_cod",
            "set_nom",
            "loc_cod",
            "loc_nom",
            "ite_mar",
            "ite_mod",
            "tgr_cod",
            "grp_cod",
            "ele_cod",
            "sbe_cod",
            "mat_cod",
            "mat_nom",
            "pes_cod",
            "pes_nome",
            "display_name",
            "email",
            "matricula",
            "phone",
            "qtd_de_favorito",
            "estado_transferencia",
            "created_at",
            "ramal",
            "matricula",
            "telephone",
        ],
    )
    columns_to_clean = [
        "num_patrimonio",
        "bem_cod",
        "bem_dsc_com",
        "bem_num_atm",
        "csv_cod",
        "bem_serie",
        "bem_val",
        "loc",
        "loc_nom",
    ]
    for col in columns_to_clean:
        dataframe[col] = dataframe[col].str.strip()
    return dataframe.fillna(0).to_dict(orient="records")

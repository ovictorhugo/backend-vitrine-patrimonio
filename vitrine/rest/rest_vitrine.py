from http import HTTPStatus
import psycopg2
from flask import Blueprint, jsonify, request
import pandas as pd
import base64
import re
import difflib
from ..dao import Connection
from ..dao import dao_patrimonio
import firebase_admin
from firebase_admin import credentials, firestore

conn = Connection()

rest_vitrine = Blueprint("rest_vitrine", __name__)
cred = credentials.Certificate("cert.json")
firebase_admin.initialize_app(credential=cred)
db = firestore.client()
collection = db.collection("patrimonio")


@rest_vitrine.route("/insertPatrimonioUnico", methods=["POST"])
def insertPatrimonio_():
    dados_patrimonio = request.get_json()

    try:
        values = f"""(
                '{dados_patrimonio["bem_cod"]}',
                '{dados_patrimonio["bem_dgv"]}',
                '{dados_patrimonio["bem_num_atm"]}',
                '{dados_patrimonio["csv_cod"]}',
                '{dados_patrimonio["bem_serie"]}',
                '{dados_patrimonio["bem_sta"]}',
                '{dados_patrimonio["bem_val"]}',
                '{dados_patrimonio["tre_cod"]}',
                '{dados_patrimonio["bem_dsc_com"].replace("'", " ")}',
                '{dados_patrimonio["uge_cod"]}',
                '{dados_patrimonio["uge_nom"]}',
                '{dados_patrimonio["org_cod"]}',
                '{dados_patrimonio["uge_siaf"]}',
                '{dados_patrimonio["org_nom"]}',
                '{dados_patrimonio["set_cod"]}',
                '{dados_patrimonio["set_nom"]}',
                '{dados_patrimonio["loc_cod"]}',
                '{dados_patrimonio["loc_nom"]}',
                '{dados_patrimonio["ite_mar"]}',
                '{dados_patrimonio["ite_mod"]}',
                '{dados_patrimonio["tgr_cod"]}',
                '{dados_patrimonio["grp_cod"]}',
                '{dados_patrimonio["ele_cod"]}',
                '{dados_patrimonio["sbe_cod"]}',
                '{dados_patrimonio["mat_cod"]}',
                '{dados_patrimonio["mat_nom"]}',
                '{dados_patrimonio["pes_cod"]}',
                '{dados_patrimonio["pes_nome"]}'),"""
        script_sql = f"""
                INSERT INTO public.patrimonio(
                bem_cod,  
                bem_dgv,  
                bem_num_atm,  
                csv_cod, 
                bem_serie, 
                bem_sta, 
                bem_val, 
                tre_cod,  
                bem_dsc_com, 
                uge_cod, 
                uge_nom, 
                org_cod, 
                uge_siaf, 
                org_nom, 
                set_cod, 
                set_nom,  
                loc_cod, 
                loc_nom, 
                ite_mar, 
                ite_mod, 
                tgr_cod,  
                grp_cod, 
                ele_cod, 
                sbe_cod, 
                mat_cod, 
                mat_nom,  
                pes_cod, 
                pes_nome)
                VALUES {values[:-1]};
                """
        doc_id = f"{dados_patrimonio['bem_cod']}_{dados_patrimonio['bem_dgv']}"
        dados_filtrados = {
            "bem_num_atm": str(dados_patrimonio.get("bem_num_atm", "") or ""),
            "bem_cod": str(dados_patrimonio.get("bem_cod", "") or ""),
            "bem_dgv": str(dados_patrimonio.get("bem_dgv", "") or ""),
            "mat_nom": str(dados_patrimonio.get("mat_nom", "") or ""),
            "bem_dsc_com": str(dados_patrimonio.get("bem_dsc_com", "") or ""),
            "pes_nome": str(dados_patrimonio.get("pes_nome", "") or ""),
            "loc_nom": str(dados_patrimonio.get("loc_nom", "") or ""),
        }
        print(dados_filtrados)
        collection.document(doc_id).set(dados_filtrados, merge=True)
        conn.exec(script_sql)

    except psycopg2.errors.UniqueViolation:
        update_script = f"""
            UPDATE patrimonio SET 
            bem_num_atm='{dados_patrimonio["bem_num_atm"]}',  
            csv_cod='{dados_patrimonio["csv_cod"]}',
            bem_serie='{dados_patrimonio["bem_serie"]}',
            bem_sta='{dados_patrimonio["bem_sta"]}',
            bem_val='{dados_patrimonio["bem_val"]}',
            tre_cod='{dados_patrimonio["tre_cod"]}',
            bem_dsc_com='{dados_patrimonio["bem_dsc_com"].replace("'", " ")}',
            uge_cod='{dados_patrimonio["uge_cod"]}',
            uge_nom='{dados_patrimonio["uge_nom"]}',
            org_cod='{dados_patrimonio["org_cod"]}',
            uge_siaf='{dados_patrimonio["uge_siaf"]}',
            org_nom='{dados_patrimonio["org_nom"]}',
            set_cod='{dados_patrimonio["set_cod"]}',
            set_nom='{dados_patrimonio["set_nom"]}',
            loc_cod='{dados_patrimonio["loc_cod"]}',
            loc_nom='{dados_patrimonio["loc_nom"]}',
            ite_mar='{dados_patrimonio["ite_mar"]}',
            ite_mod='{dados_patrimonio["ite_mod"]}',
            tgr_cod='{dados_patrimonio["tgr_cod"]}',
            grp_cod='{dados_patrimonio["grp_cod"]}',
            ele_cod='{dados_patrimonio["ele_cod"]}',
            sbe_cod='{dados_patrimonio["sbe_cod"]}',
            mat_cod='{dados_patrimonio["mat_cod"]}',
            mat_nom='{dados_patrimonio["mat_nom"]}',
            pes_cod='{dados_patrimonio["pes_cod"]}',
            pes_nome='{dados_patrimonio["pes_nome"]}'
            WHERE bem_cod = '{dados_patrimonio["bem_cod"]}' AND bem_dgv = '{dados_patrimonio["bem_dgv"]}' 
            """
        conn.exec(update_script)
    return jsonify([]), 201


@rest_vitrine.route("/insertPatrimonio", methods=["POST"])
def insertPatrimonio():
    if "file" not in request.files:
        return "Nenhum arquivo enviado", 400

    file = request.files.get("file")

    if file is None or file.filename == "":
        return "Nenhum arquivo selecionado", 400

    file.save(f"/tmp/{file.filename}")

    file_extension = file.filename.split(".")[-1].lower()

    if file_extension == "xls":
        engine = "xlrd"
        df = pd.read_excel(f"/tmp/{file.filename}", engine=engine)
    elif file_extension == "xlsx":
        engine = "calamine"
        df = pd.read_excel(f"/tmp/{file.filename}", engine=engine)
    elif file_extension == "csv":
        try:
            df = pd.read_csv(f"/tmp/{file.filename}", delimiter=";")
        except Exception:
            df = pd.read_csv(f"/tmp/{file.filename}", delimiter=",")
    else:
        return "Formato de arquivo não suportado", 400

    for _, patrimonio in df.iterrows():
        dados_patrimonio = patrimonio.to_dict()
        values = str()
        try:
            values += f"""(
                    '{dados_patrimonio.get("bem_cod")}',
                    '{dados_patrimonio.get("bem_dgv")}',
                    '{dados_patrimonio.get("bem_num_atm")}',
                    '{dados_patrimonio.get("csv_cod")}',
                    '{dados_patrimonio.get("bem_serie")}',
                    '{dados_patrimonio.get("bem_sta")}',
                    '{dados_patrimonio.get("bem_val")}',
                    '{dados_patrimonio.get("tre_cod")}',
                    '{dados_patrimonio.get("bem_dsc_com", "").replace("'", " ")}',
                    '{dados_patrimonio.get("uge_cod")}',
                    '{dados_patrimonio.get("uge_nom")}',
                    '{dados_patrimonio.get("org_cod")}',
                    '{dados_patrimonio.get("uge_siaf")}',
                    '{dados_patrimonio.get("org_nom")}',
                    '{dados_patrimonio.get("set_cod")}',
                    '{dados_patrimonio.get("set_nom")}',
                    '{dados_patrimonio.get("loc_cod")}',
                    '{dados_patrimonio.get("loc_nom")}',
                    '{dados_patrimonio.get("ite_mar")}',
                    '{dados_patrimonio.get("ite_mod")}',
                    '{dados_patrimonio.get("tgr_cod")}',
                    '{dados_patrimonio.get("grp_cod")}',
                    '{dados_patrimonio.get("ele_cod")}',
                    '{dados_patrimonio.get("sbe_cod")}',
                    '{dados_patrimonio.get("mat_cod")}',
                    '{dados_patrimonio.get("mat_nom")}',
                    '{dados_patrimonio.get("pes_cod")}',
                    '{dados_patrimonio.get("pes_nome")}'),"""

            script_sql = f"""
                INSERT INTO public.patrimonio(
                    bem_cod,
                    bem_dgv,
                    bem_num_atm,
                    csv_cod,
                    bem_serie,
                    bem_sta,
                    bem_val,
                    tre_cod,
                    bem_dsc_com,
                    uge_cod,
                    uge_nom,
                    org_cod,
                    uge_siaf,
                    org_nom,
                    set_cod,
                    set_nom,
                    loc_cod,
                    loc_nom,
                    ite_mar,
                    ite_mod,
                    tgr_cod,
                    grp_cod,
                    ele_cod,
                    sbe_cod,
                    mat_cod,
                    mat_nom,
                    pes_cod,
                    pes_nome)
                    VALUES {values[:-1]};
                    """
            doc_id = f"{dados_patrimonio['bem_cod']}_{dados_patrimonio['bem_dgv']}"
            dados_filtrados = {
                "bem_num_atm": str(dados_patrimonio.get("bem_num_atm", "") or ""),
                "bem_cod": str(dados_patrimonio.get("bem_cod", "") or ""),
                "bem_dgv": str(dados_patrimonio.get("bem_dgv", "") or ""),
                "mat_nom": str(dados_patrimonio.get("mat_nom", "") or ""),
                "bem_dsc_com": str(dados_patrimonio.get("bem_dsc_com", "") or ""),
                "pes_nome": str(dados_patrimonio.get("pes_nome", "") or ""),
                "loc_nom": str(dados_patrimonio.get("loc_nom", "") or ""),
            }
            print(dados_filtrados)
            collection.document(doc_id).set(dados_filtrados, merge=True)
            conn.exec(script_sql)
        except psycopg2.errors.UniqueViolation:
            update_script = f"""
                UPDATE patrimonio SET
                bem_num_atm='{dados_patrimonio.get("bem_num_atm")}',
                csv_cod='{dados_patrimonio.get("csv_cod")}',
                bem_serie='{dados_patrimonio.get("bem_serie")}',
                bem_sta='{dados_patrimonio.get("bem_sta")}',
                bem_val='{dados_patrimonio.get("bem_val")}',
                tre_cod='{dados_patrimonio.get("tre_cod")}',
                bem_dsc_com='{dados_patrimonio.get("bem_dsc_com", "").replace("'", " ")}',
                uge_cod='{dados_patrimonio.get("uge_cod")}',
                uge_nom='{dados_patrimonio.get("uge_nom")}',
                org_cod='{dados_patrimonio.get("org_cod")}',
                uge_siaf='{dados_patrimonio.get("uge_siaf")}',
                org_nom='{dados_patrimonio.get("org_nom")}',
                set_cod='{dados_patrimonio.get("set_cod")}',
                set_nom='{dados_patrimonio.get("set_nom")}',
                loc_cod='{dados_patrimonio.get("loc_cod")}',
                loc_nom='{dados_patrimonio.get("loc_nom")}',
                ite_mar='{dados_patrimonio.get("ite_mar")}',
                ite_mod='{dados_patrimonio.get("ite_mod")}',
                tgr_cod='{dados_patrimonio.get("tgr_cod")}',
                grp_cod='{dados_patrimonio.get("grp_cod")}',
                ele_cod='{dados_patrimonio.get("ele_cod")}',
                sbe_cod='{dados_patrimonio.get("sbe_cod")}',
                mat_cod='{dados_patrimonio.get("mat_cod")}',
                mat_nom='{dados_patrimonio.get("mat_nom")}',
                pes_cod='{dados_patrimonio.get("pes_cod")}',
                pes_nome='{dados_patrimonio.get("pes_nome")}'
                WHERE bem_cod = '{dados_patrimonio.get("bem_cod")}' AND bem_dgv = '{dados_patrimonio.get("bem_dgv")}'
                """
            conn.exec(update_script)
    return jsonify([]), 201


def normalize_atm_number(atm_number):
    return re.sub(r"\D", "", atm_number)


# Função para calcular a similaridade usando difflib
def calculate_similarity(user_input, stored_values):
    similarities = []
    for stored_value in stored_values:
        similarity = difflib.SequenceMatcher(None, user_input, stored_value).ratio()
        similarities.append((stored_value, similarity))
    return similarities


# Função para aplicar o Teorema de Bayes
def bayesian_matching(user_input, stored_values):
    normalized_user_input = normalize_atm_number(user_input)
    normalized_stored_values = [normalize_atm_number(val) for val in stored_values]

    similarities = calculate_similarity(normalized_user_input, normalized_stored_values)
    similarities.sort(key=lambda x: x[1], reverse=True)  # Ordenar pela similaridade

    best_match = similarities[0] if similarities else None
    return best_match


def _searchByBemNumAtm(bem_num_atm):
    # Buscar todos os valores de bem_num_atm das tabelas Patrimonio e PatrimonioMorto
    stored_values_sql = """
    SELECT bem_num_atm FROM Patrimonio
    UNION
    SELECT bem_num_atm FROM PatrimonioMorto
    """

    stored_values_result = conn.select(stored_values_sql)
    stored_values = [
        row[0] for row in stored_values_result
    ]  # Acessando o primeiro elemento da tupla

    best_match = bayesian_matching(bem_num_atm, stored_values)

    if best_match:
        scriptSql = f"""
        SELECT 
            bem_cod, 
            bem_dgv, 
            bem_dsc_com, 
            bem_num_atm, 
            uge_siaf, 
            bem_sta, 
            uge_cod, 
            org_cod, 
            set_cod, 
            loc_cod, 
            org_nom,
            created_at,
            csv_cod,
            bem_serie,
            bem_val,
            tre_cod,
            uge_nom,
            set_nom,
            loc_nom,
            ite_mar,
            ite_mod,
            tgr_cod,
            grp_cod,
            ele_cod,
            sbe_cod,
            mat_cod,
            mat_nom,
            pes_cod,
            pes_nome
        FROM 
            Patrimonio
        WHERE 
            bem_num_atm = '{best_match[0]}'

        UNION 

        SELECT 
            bem_cod, 
            bem_dgv, 
            bem_dsc_com, 
            bem_num_atm, 
            uge_siaf, 
            bem_sta, 
            uge_cod, 
            org_cod, 
            set_cod, 
            loc_cod, 
            org_nom,
            created_at,
            NULL AS csv_cod,
            NULL AS bem_serie,
            NULL AS bem_val,
            NULL AS tre_cod,
            NULL AS uge_nom,
            NULL AS set_nom,
            NULL AS loc_nom,
            NULL AS ite_mar,
            NULL AS ite_mod,
            NULL AS tgr_cod,
            NULL AS grp_cod,
            NULL AS ele_cod,
            NULL AS sbe_cod,
            NULL AS mat_cod,
            NULL AS mat_nom,
            NULL AS pes_cod,
            NULL AS pes_nome
        FROM 
            PatrimonioMorto
        WHERE 
            bem_num_atm = '{best_match[0]}'
        """

        resultado = conn.select(scriptSql)

        columns = [
            "bem_cod",
            "bem_dgv",
            "bem_dsc_com",
            "bem_num_atm",
            "uge_siaf",
            "bem_sta",
            "uge_cod",
            "org_cod",
            "set_cod",
            "loc_cod",
            "org_nom",
            "created_at",
            "csv_cod",
            "bem_serie",
            "bem_val",
            "tre_cod",
            "uge_nom",
            "set_nom",
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
        ]

        dataFrame = pd.DataFrame(resultado, columns=columns)

        return jsonify(dataFrame.to_dict(orient="records"))

    return jsonify({"error": "No matching record found"}), 404


# VERIFICAR SE EXISTE PATRIMONIO PELO ATM
@rest_vitrine.route("/searchByBemNumAtm", methods=["GET"])
def searchByBemNumAtm():
    bem_num_atm = request.args.get("bem_num_atm")
    return _searchByBemNumAtm(bem_num_atm)


@rest_vitrine.route("/checkoutPatrimonio", methods=["GET"])
def checkoutPatrimonio():
    bem_num_atm = request.args.get("bem_num_atm")
    if bem_num_atm:
        return _searchByBemNumAtm(bem_num_atm)
    bem_cod = request.args.get("bem_cod").replace(".-", "")
    bem_dgv = request.args.get("bem_dgv")

    scriptSql = f"""
    SELECT 
        bem_cod, 
        bem_dgv, 
        bem_dsc_com, 
        bem_num_atm, 
        uge_siaf, 
        bem_sta, 
        uge_cod, 
        org_cod, 
        set_cod, 
        loc_cod, 
        org_nom,
        created_at,
        csv_cod,
        bem_serie,
        bem_val,
        tre_cod,
        uge_nom,
        set_nom,
        loc_nom,
        ite_mar,
        ite_mod,
        tgr_cod,
        grp_cod,
        ele_cod,
        sbe_cod,
        mat_cod,
        mat_nom,
        pes_cod,
        pes_nome
        FROM 
            patrimonio
        WHERE 
            bem_cod = '{bem_cod}' AND 
            bem_dgv = '{bem_dgv}'

    UNION 

    SELECT 
        bem_cod, 
        bem_dgv, 
        bem_dsc_com, 
        bem_num_atm, 
        uge_siaf, 
        bem_sta, 
        uge_cod, 
        org_cod, 
        set_cod, 
        loc_cod, 
        org_nom,
        created_at,
        NULL AS csv_cod,
        NULL AS bem_serie,
        NULL AS bem_val,
        NULL AS tre_cod,
        NULL AS uge_nom,
        NULL AS set_nom,
        NULL AS loc_nom,
        NULL AS ite_mar,
        NULL AS ite_mod,
        NULL AS tgr_cod,
        NULL AS grp_cod,
        NULL AS ele_cod,
        NULL AS sbe_cod,
        NULL AS mat_cod,
        NULL AS mat_nom,
        NULL AS pes_cod,
        NULL AS pes_nome
    FROM 
        PatrimonioMorto
    WHERE 
        bem_cod = '{bem_cod}' AND 
        bem_dgv = '{bem_dgv}'
    """

    resultado = conn.select(scriptSql)

    columns = [
        "bem_cod",
        "bem_dgv",
        "bem_dsc_com",
        "bem_num_atm",
        "uge_siaf",
        "bem_sta",
        "uge_cod",
        "org_cod",
        "set_cod",
        "loc_cod",
        "org_nom",
        "created_at",
        "csv_cod",
        "bem_serie",
        "bem_val",
        "tre_cod",
        "uge_nom",
        "set_nom",
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
    ]

    dataFrame = pd.DataFrame(resultado, columns=columns)
    if pat := dao_patrimonio.get_patrimonio(None, None):
        dataFrame.merge(pat, left_on="bem_cod", right_on="codigo", how="left")

    return jsonify(dataFrame.to_dict(orient="records"))


# TODOS PATRIMONIOS
@rest_vitrine.route("/allPatrimonio", methods=["GET"])
def allPatrimonio():
    loc_nom = request.args.get("loc_nom")
    print(loc_nom)

    # Get pagination parameters from the request
    page = int(request.args.get("page", 1))
    page_size = int(request.args.get("page_size", 100))
    offset = (page - 1) * page_size

    # SQL query with JOIN and pagination
    scriptSql = """
    SELECT 
        p.bem_cod, 
        p.bem_dgv, 
        p.bem_dsc_com, 
        p.bem_num_atm, 
        p.uge_siaf, 
        p.bem_sta, 
        p.uge_cod, 
        p.org_cod, 
        p.set_cod, 
        p.loc_cod, 
        p.org_nom,
        p.created_at,
        COALESCE(cb.csv_cod, p.csv_cod) as csv_cod,  -- Use csv_cod from condicao_bem if exists, otherwise use from Patrimonio
        p.bem_serie,
        p.bem_val,
        p.tre_cod,
        p.uge_nom,
        p.set_nom,
        p.loc_nom,
        p.ite_mar,
        p.ite_mod,
        p.tgr_cod,
        p.grp_cod,
        p.ele_cod,
        p.sbe_cod,
        p.mat_cod,
        p.mat_nom,
        p.pes_cod,
        p.pes_nome
    FROM 
        Patrimonio p
    LEFT JOIN
        condicao_bem cb ON p.bem_cod = cb.bem_cod AND p.bem_dgv = cb.bem_dgv
    """

    # Adding filter for loc_nom if provided
    if loc_nom:
        scriptSql += f" WHERE p.loc_nom = '{loc_nom}'"

    resultado = conn.select(scriptSql)

    if not resultado:
        return jsonify({"error": "No results found"}), HTTPStatus.NOT_FOUND

    columns = [
        "bem_cod",
        "bem_dgv",
        "bem_dsc_com",
        "bem_num_atm",
        "uge_siaf",
        "bem_sta",
        "uge_cod",
        "org_cod",
        "set_cod",
        "loc_cod",
        "org_nom",
        "created_at",
        "csv_cod",
        "bem_serie",
        "bem_val",
        "tre_cod",
        "uge_nom",
        "set_nom",
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
    ]

    dataFrame = pd.DataFrame(resultado, columns=columns)

    return jsonify(dataFrame.to_dict(orient="records")), HTTPStatus.OK


# TODOS PATRIMONIOS MORTO
@rest_vitrine.route("/allPatrimonioMorto", methods=["GET"])
def allPatrimonioMorto():
    try:
        # Get pagination parameters from the request
        page = int(request.args.get("page", 1))
        page_size = int(request.args.get("page_size", 100))
        offset = (page - 1) * page_size

        # SQL query with LIMIT and OFFSET for pagination
        scriptSql = """
        SELECT 
            bem_cod, 
            bem_dgv, 
            bem_dsc_com, 
            bem_num_atm, 
            uge_siaf, 
            bem_sta, 
            uge_cod, 
            org_cod, 
            set_cod, 
            loc_cod, 
            org_nom,
            set_nom,
            uge_nom,
            loc_nom,
            mat_nom
        FROM 
            Patrimoniomorto
               """

        resultado = conn.select(scriptSql)

        columns = [
            "bem_cod",
            "bem_dgv",
            "bem_dsc_com",
            "bem_num_atm",
            "uge_siaf",
            "bem_sta",
            "uge_cod",
            "org_cod",
            "set_cod",
            "loc_cod",
            "org_nom",
            "set_nom",
            "uge_nom",
            "loc_nom",
            "mat_nom",
        ]

        dataFrame = pd.DataFrame(resultado, columns=columns)

        return jsonify(dataFrame.to_dict(orient="records")), HTTPStatus.OK
    except Exception as e:
        return jsonify({"error": str(e)}), HTTPStatus.INTERNAL_SERVER_ERROR


# INSERIR PATRIMONIO MORTO
@rest_vitrine.route("/insertPatrimonioMorto", methods=["POST"])
def insertPatrimonioMorto():
    ListPatrimonio = request.get_json()
    for patrimonio in ListPatrimonio:
        values = str()
        print(patrimonio["bem_cod"], patrimonio["bem_dgv"])
        try:
            values += f"""(
                '{patrimonio["bem_cod"]}', 
                '{patrimonio["bem_dgv"]}', 
                '{patrimonio["bem_dsc_com"].replace("'", " ")}', 
                '{patrimonio["bem_num_atm"]}', 
                '{patrimonio["uge_siaf"]}', 
                '{patrimonio["bem_sta"]}', 
                '{patrimonio["uge_cod"]}', 
                '{patrimonio["org_cod"]}', 
                '{patrimonio["set_cod"]}', 
                '{patrimonio["loc_cod"]}', 
                '{patrimonio["org_nom"]}',
                '{patrimonio["set_nom"]}',
                '{patrimonio["uge_nom"]}',
                '{patrimonio["loc_nom"]}',
                '{patrimonio["mat_nom"]}'
                
                ),"""

            scriptSql = f"""
            INSERT INTO public.PatrimonioMorto(
            bem_cod, 
            bem_dgv, 
            bem_dsc_com, 
            bem_num_atm, 
            uge_siaf, 
            bem_sta, 
            uge_cod, 
            org_cod, 
            set_cod, 
            loc_cod, 
            org_nom,
            set_nom,
            uge_nom,
            loc_nom,
            mat_nom) 
            VALUES {values[:-1]}
            """

            conn.exec(scriptSql)

        except psycopg2.errors.UniqueViolation:
            update_script = f"""
                UPDATE patrimonio SET 
                bem_dsc_com='{patrimonio["bem_dsc_com"]}', 
                bem_num_atm='{patrimonio["bem_num_atm"]}', 
                uge_siaf='{patrimonio["uge_siaf"]}', 
                bem_sta='{patrimonio["bem_sta"]}', 
                uge_cod='{patrimonio["uge_cod"]}', 
                org_cod='{patrimonio["org_cod"]}', 
                set_cod='{patrimonio["set_cod"]}', 
                loc_cod='{patrimonio["loc_cod"]}', 
                org_nom='{patrimonio["org_nom"]}',
                set_nom='{patrimonio["set_nom"]}',
                uge_nom='{patrimonio["uge_nom"]}',
                loc_nom='{patrimonio["loc_nom"]}',
                mat_nom='{patrimonio["mat_nom"]}'
                WHERE bem_cod = '{patrimonio["bem_cod"]}' AND bem_dgv = '{patrimonio["bem_dgv"]}' 
            """
            conn.exec(update_script)

    return jsonify([]), 201


# TOTAL DE PATRIMONIOS
@rest_vitrine.route("/totalPatrimonio", methods=["GET"])
def totalPatrimonio():
    loc_nom = request.args.get("loc_nom")

    # Base queries
    scriptSqlPatrimonio = "SELECT COUNT(*) AS total_patrimonio FROM Patrimonio"
    scriptSqlPatrimonioMorto = (
        "SELECT COUNT(*) AS total_patrimonio_morto FROM PatrimonioMorto"
    )

    scriptSql = f"""
    {scriptSqlPatrimonio}
    UNION ALL
    {scriptSqlPatrimonioMorto}
    """
    # Adding filter for loc_nom if provided
    if loc_nom:
        scriptSqlPatrimonio += f" WHERE loc_nom = '{loc_nom}'"

        # Combine both queries with UNION ALL
        scriptSql = f"""
        {scriptSqlPatrimonio}
 
        """

    resultado = conn.select(scriptSql)

    # Presuming the query returns two rows
    total_patrimonio = resultado[0][0] if resultado else 0
    total_patrimonio_morto = resultado[1][0] if len(resultado) > 1 else 0

    response = {
        "total_patrimonio": total_patrimonio,
        "total_patrimonio_morto": total_patrimonio_morto,
        "unique_values": [],
    }

    if loc_nom:
        # Additional query for unique values
        scriptSqlUnique = f"""
        SELECT DISTINCT org_cod, org_nom, set_nom, set_cod, loc_cod, loc_nom, pes_cod, pes_nome
        FROM Patrimonio
        WHERE loc_nom = '{loc_nom}'
        """
        unique_result = conn.select(scriptSqlUnique)

        unique_values = [
            {
                "org_cod": row[0],
                "org_nom": row[1],
                "set_nom": row[2],
                "set_cod": row[3],
                "loc_cod": row[4],
                "loc_nom": row[5],
                "pes_cod": row[6],
                "pes_nome": row[7],
            }
            for row in unique_result
        ]

        response["unique_values"] = unique_values

    return jsonify([response]), HTTPStatus.OK


# INSERIR CONDICAO BEM


@rest_vitrine.route("/insertCondicaoBem", methods=["POST"])
def insertCondicaoBem():
    ListPatrimonio = request.get_json()

    for patrimonio in ListPatrimonio:
        values = str()
        print(patrimonio["bem_cod"], patrimonio["bem_dgv"])
        try:
            values += f"""(
                '{patrimonio["bem_cod"]}',
                '{patrimonio["bem_dgv"]}',
                '{patrimonio["csv_cod"]}'
                ),"""
            # Criação do script de insert.
            # Unifiquei em um unico comando para facilitar
            # o retorno da mensagem de erro

            script_sql = f"""
                INSERT INTO public.condicao_bem(
                bem_cod,  
                bem_dgv,  
                csv_cod)
                VALUES {values[:-1]}
                """

            conn.exec(script_sql)
        except psycopg2.errors.UniqueViolation:
            update_script = f"""
            UPDATE public.condicao_bem SET 
            csv_cod='{patrimonio["csv_cod"]}'
            WHERE bem_cod = '{patrimonio["bem_cod"]}' AND bem_dgv = '{patrimonio["bem_dgv"]}' 
            """
            conn.exec(update_script)

    return jsonify([]), 201


# FILTRO POR CSV
@rest_vitrine.route("/filterByCsvCod", methods=["GET"])
def filterByCsvCod():
    csv_cod = request.args.get("csv_cod")

    # SQL query with JOIN and filter by csv_cod
    scriptSql = f"""
    SELECT 
        p.bem_cod, 
        p.bem_dgv, 
        p.bem_dsc_com, 
        p.bem_num_atm, 
        p.uge_siaf, 
        p.bem_sta, 
        p.uge_cod, 
        p.org_cod, 
        p.set_cod, 
        p.loc_cod, 
        p.org_nom,
        p.created_at,
        cb.csv_cod,  -- Use csv_cod from condicao_bem
        p.bem_serie,
        p.bem_val,
        p.tre_cod,
        p.uge_nom,
        p.set_nom,
        p.loc_nom,
        p.ite_mar,
        p.ite_mod,
        p.tgr_cod,
        p.grp_cod,
        p.ele_cod,
        p.sbe_cod,
        p.mat_cod,
        p.mat_nom,
        p.pes_cod,
        p.pes_nome
    FROM 
        Patrimonio p
    JOIN
        condicao_bem cb ON p.bem_cod = cb.bem_cod AND p.bem_dgv = cb.bem_dgv
    WHERE
        cb.csv_cod = '{csv_cod}'
    """

    resultado = conn.select(scriptSql)

    columns = [
        "bem_cod",
        "bem_dgv",
        "bem_dsc_com",
        "bem_num_atm",
        "uge_siaf",
        "bem_sta",
        "uge_cod",
        "org_cod",
        "set_cod",
        "loc_cod",
        "org_nom",
        "created_at",
        "csv_cod",
        "bem_serie",
        "bem_val",
        "tre_cod",
        "uge_nom",
        "set_nom",
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
    ]

    dataFrame = pd.DataFrame(resultado, columns=columns)
    return jsonify(dataFrame.to_dict(orient="records")), HTTPStatus.OK


# DELETAR CONDICAO BEM
@rest_vitrine.route("/clearCondicaoBem", methods=["POST"])
def clearCondicaoBem():
    scriptSql = "DELETE FROM condicao_bem"
    conn.exec(scriptSql)
    return jsonify({"message": "Tabela condicao_bem limpa com sucesso."}), HTTPStatus.OK


# TODOS LOC NOM
@rest_vitrine.route("/AllLocNom", methods=["GET"])
def all_loc_nom():
    script_sql = """
    SELECT DISTINCT P.loc_nom, P.pes_nome, S.email, S.telefone
    FROM Patrimonio P
    LEFT JOIN solicitante S ON P.pes_nome = S.pes_nome
    """

    resultado = conn.select(script_sql)

    loc_noms = []
    for row in resultado:
        loc_noms.append(
            {
                "loc_nom": row[0],
                "pes_nome": row[1] if row[1] else "",
                "email": row[2] if row[2] else "",
                "telefone": row[3] if row[3] else "",
            }
        )

    return jsonify(loc_noms), HTTPStatus.OK


# EMPENHO
@rest_vitrine.route("/empenho", methods=["POST"])
def create_empenho():
    data = request.form.to_dict()
    files = request.files

    pdf_empenho = files["pdf_empenho"].read() if "pdf_empenho" in files else None
    pdf_nf = files["pdf_nf"].read() if "pdf_nf" in files else None
    pdf_resumo = files["pdf_resumo"].read() if "pdf_resumo" in files else None

    insert_sql = """
        INSERT INTO empenho (
            id, coluna, emp_nom, status_tomb, tipo_emp, pdf_empenho, data_fornecedor, 
            prazo_entrega, status_recebimento, loc_entrega, loc_entrega_confirmado, 
            cnpj, loc_nom, des_nom, status_tombamento, data_tombamento, data_aviso, 
            prazo_teste, atestado, loc_tom, status_nf, observacoes, data_agendamento, 
            n_termo_processo, origem, valor_termo, n_projeto, data_tomb_sei, pdf_nf, 
            pdf_resumo
        ) VALUES (
            %(id)s, %(coluna)s, %(emp_nom)s, %(status_tomb)s, %(tipo_emp)s, %(pdf_empenho)s, 
            %(data_fornecedor)s, %(prazo_entrega)s, %(status_recebimento)s, %(loc_entrega)s, 
            %(loc_entrega_confirmado)s, %(cnpj)s, %(loc_nom)s, %(des_nom)s, %(status_tombamento)s, 
            %(data_tombamento)s, %(data_aviso)s, %(prazo_teste)s, %(atestado)s, %(loc_tom)s, 
            %(status_nf)s, %(observacoes)s, %(data_agendamento)s, %(n_termo_processo)s, 
            %(origem)s, %(valor_termo)s, %(n_projeto)s, %(data_tomb_sei)s, %(pdf_nf)s, 
            %(pdf_resumo)s
        )
    """

    update_sql = """
        UPDATE empenho SET 
            coluna=%(coluna)s, emp_nom=%(emp_nom)s, status_tomb=%(status_tomb)s, 
            tipo_emp=%(tipo_emp)s, data_fornecedor=%(data_fornecedor)s, 
            prazo_entrega=%(prazo_entrega)s, status_recebimento=%(status_recebimento)s, 
            loc_entrega=%(loc_entrega)s, loc_entrega_confirmado=%(loc_entrega_confirmado)s, 
            cnpj=%(cnpj)s, loc_nom=%(loc_nom)s, des_nom=%(des_nom)s, 
            status_tombamento=%(status_tombamento)s, data_tombamento=%(data_tombamento)s, 
            data_aviso=%(data_aviso)s, prazo_teste=%(prazo_teste)s, atestado=%(atestado)s, 
            loc_tom=%(loc_tom)s, status_nf=%(status_nf)s, observacoes=%(observacoes)s, 
            data_agendamento=%(data_agendamento)s, n_termo_processo=%(n_termo_processo)s, 
            origem=%(origem)s, valor_termo=%(valor_termo)s, n_projeto=%(n_projeto)s, 
            data_tomb_sei=%(data_tomb_sei)s, pdf_nf=%(pdf_nf)s, pdf_resumo=%(pdf_resumo)s
        WHERE id=%(id)s
    """

    params = {
        "id": data["id"],
        "coluna": data.get("coluna"),
        "emp_nom": data.get("emp_nom"),
        "status_tomb": data.get("status_tomb"),
        "tipo_emp": data.get("tipo_emp"),
        "pdf_empenho": psycopg2.Binary(pdf_empenho),
        "data_fornecedor": data.get("data_fornecedor"),
        "prazo_entrega": data.get("prazo_entrega"),
        "status_recebimento": data.get("status_recebimento"),
        "loc_entrega": data.get("loc_entrega"),
        "loc_entrega_confirmado": data.get("loc_entrega_confirmado"),
        "cnpj": data.get("cnpj"),
        "loc_nom": data.get("loc_nom"),
        "des_nom": data.get("des_nom"),
        "status_tombamento": data.get("status_tombamento"),
        "data_tombamento": data.get("data_tombamento"),
        "data_aviso": data.get("data_aviso"),
        "prazo_teste": data.get("prazo_teste"),
        "atestado": data.get("atestado"),
        "loc_tom": data.get("loc_tom"),
        "status_nf": data.get("status_nf"),
        "observacoes": data.get("observacoes"),
        "data_agendamento": data.get("data_agendamento"),
        "n_termo_processo": data.get("n_termo_processo"),
        "origem": data.get("origem"),
        "valor_termo": data.get("valor_termo"),
        "n_projeto": data.get("n_projeto"),
        "data_tomb_sei": data.get("data_tomb_sei"),
        "pdf_nf": psycopg2.Binary(pdf_nf),
        "pdf_resumo": psycopg2.Binary(pdf_resumo),
    }

    try:
        conn.exec(insert_sql, params)
        return jsonify({"message": "Empenho created successfully"}), 201
    except psycopg2.errors.UniqueViolation:
        conn.exec(update_sql, params)
        return jsonify({"message": "Empenho updated successfully"}), 200
    except Exception as e:
        return jsonify(
            {"message": "Error creating or updating empenho", "error": str(e)}
        ), 500


# ALTERAR COLUNA EMPENHO
@rest_vitrine.route("/UpdateColumnEmpenho", methods=["POST"])
def update_empenho_column(id, column):
    id = request.args.get("id")
    coluna = request.args.get("coluna")
    try:
        value = request.form["value"]

        # Evitar SQL Injection usando parâmetros de consulta
        update_sql = f"UPDATE empenho SET {column} = %s WHERE id = %s"

        params = (value, id)

        # Executar a consulta de atualização
        with conn.cursor() as cursor:
            cursor.execute(update_sql, params)
            conn.commit()

        return jsonify({"message": "Empenho column updated successfully"}), 200
    except Exception as e:
        return jsonify(
            {"message": "Error updating empenho column", "error": str(e)}
        ), 500


# VER EMPENHOS
@rest_vitrine.route("/AllEmpenhos", methods=["GET"])
def get_empenhos():
    scriptSql = """
    SELECT 
        id, coluna, emp_nom, status_tomb, tipo_emp, pdf_empenho, data_fornecedor, 
        prazo_entrega, status_recebimento, loc_entrega, loc_entrega_confirmado, cnpj, 
        loc_nom, des_nom, status_tombamento, data_tombamento, data_aviso, prazo_teste, 
        atestado, loc_tom, status_nf, observacoes, data_agendamento, n_termo_processo, 
        origem, valor_termo, n_projeto, data_tomb_sei, pdf_nf, pdf_resumo, created_at
    FROM 
        empenho
    """

    resultado = conn.select(scriptSql)

    columns = [
        "id",
        "coluna",
        "emp_nom",
        "status_tomb",
        "tipo_emp",
        "pdf_empenho",
        "data_fornecedor",
        "prazo_entrega",
        "status_recebimento",
        "loc_entrega",
        "loc_entrega_confirmado",
        "cnpj",
        "loc_nom",
        "des_nom",
        "status_tombamento",
        "data_tombamento",
        "data_aviso",
        "prazo_teste",
        "atestado",
        "loc_tom",
        "status_nf",
        "observacoes",
        "data_agendamento",
        "n_termo_processo",
        "origem",
        "valor_termo",
        "n_projeto",
        "data_tomb_sei",
        "pdf_nf",
        "pdf_resumo",
        "created_at",
    ]

    # Convert the binary data to base64
    result_with_base64 = []
    for row in resultado:
        row_dict = dict(zip(columns, row))
        row_dict["pdf_empenho"] = (
            base64.b64encode(row_dict["pdf_empenho"]).decode("utf-8")
            if row_dict["pdf_empenho"]
            else None
        )
        row_dict["pdf_nf"] = (
            base64.b64encode(row_dict["pdf_nf"]).decode("utf-8")
            if row_dict["pdf_nf"]
            else None
        )
        row_dict["pdf_resumo"] = (
            base64.b64encode(row_dict["pdf_resumo"]).decode("utf-8")
            if row_dict["pdf_resumo"]
            else None
        )
        result_with_base64.append(row_dict)

    return jsonify(result_with_base64), 200


# INSERIOR FORNECEDOR
@rest_vitrine.route("/insertFornecedor", methods=["POST"])
def insert_fornecedor():
    ListFornecedores = request.get_json()
    for fornecedor in ListFornecedores:
        values = str()
        try:
            values += f"""(
                '{fornecedor["sigla"]}', 
                '{fornecedor["nome"]}', 
                '{fornecedor["endereco"]}', 
                '{fornecedor["cep"]}', 
                '{fornecedor["cidade"]}', 
                '{fornecedor["cnpj"]}', 
                '{fornecedor["telefone"]}', 
                '{fornecedor["email"]}', 
                '{fornecedor["observacoes"]}'
                ),"""

            scriptSql = f"""
            INSERT INTO public.fornecedores(
                sigla, 
                nome, 
                endereco, 
                cep, 
                cidade, 
                cnpj, 
                telefone, 
                email, 
                observacoes
            ) VALUES {values[:-1]}
            """

            conn.exec(scriptSql)
        except psycopg2.errors.UniqueViolation:
            update_script = f"""
            UPDATE public.condicao_bem SET 
            '{fornecedor["sigla"]}', 
            '{fornecedor["nome"]}', 
            '{fornecedor["endereco"]}', 
            '{fornecedor["cep"]}', 
            '{fornecedor["cidade"]}', 
            '{fornecedor["telefone"]}', 
            '{fornecedor["email"]}', 
            '{fornecedor["observacoes"]}'
            WHERE cnpj ='{fornecedor["cnpj"]}' 
            """
            conn.exec(update_script)

    return jsonify([]), 201


# TODOS OS FORNECEDORES
@rest_vitrine.route("/getFornecedores", methods=["GET"])
def get_fornecedores():
    try:
        scriptSql = "SELECT * FROM public.fornecedores"
        result = conn.select(scriptSql)
        fornecedores = []
        for row in result:
            fornecedor = {
                "sigla": row[0],
                "nome": row[1],
                "endereco": row[2],
                "cep": row[3],
                "cidade": row[4],
                "cnpj": row[5],
                "telefone": row[6],
                "email": row[7],
                "observacoes": row[8],
                "created_at": row[9],
            }
            fornecedores.append(fornecedor)

        return jsonify(fornecedores), 200
    except Exception as e:
        return jsonify({"message": str(e)}), 400


# DELETAR FORNECEDORES
@rest_vitrine.route("/deleteFornecedor", methods=["DELETE"])
def delete_fornecedor():
    cnpj = request.args.get("cnpj")

    try:
        scriptSql = f"DELETE FROM public.fornecedores WHERE cnpj = '{cnpj}'"
        conn.exec(scriptSql)
        return jsonify({"message": "Fornecedor excluído com sucesso"}), 200
    except Exception as e:
        return jsonify({"message": str(e)}), 400


# ADICIONAR SOLICITANTE
@rest_vitrine.route("/solicitante", methods=["POST"])
def insert_or_update_solicitante():
    try:
        list_solicitantes = request.get_json()
        for solicitante in list_solicitantes:
            pes_nome = solicitante.get("pes_nome")
            email = solicitante.get("email", "")
            telefone = solicitante.get("telefone", "")

            # Verifica se o solicitante já existe
            script_sql_check = (
                f"SELECT COUNT(*) FROM solicitante WHERE pes_nome = '{pes_nome}'"
            )
            resultado = conn.select(script_sql_check)
            count = resultado[0][0] if resultado else 0

            if count > 0:
                # Se existir, atualiza os dados
                script_sql_update = f"""
                UPDATE solicitante SET 
                email = '{email}', 
                telefone = '{telefone}'
                WHERE pes_nome = '{pes_nome}'
                """
                conn.exec(script_sql_update)
            else:
                # Se não existir, insere um novo solicitante
                script_sql_insert = f"""
                INSERT INTO solicitante (pes_nome, email, telefone) 
                VALUES ('{pes_nome}', '{email}', '{telefone}')
                """
                conn.exec(script_sql_insert)

        return jsonify(
            {"message": "Solicitantes inseridos/atualizados com sucesso"}
        ), HTTPStatus.CREATED

    except Exception as e:
        return jsonify({"error": str(e)}), HTTPStatus.INTERNAL_SERVER_ERROR

    # DEPRTAMENTO


@rest_vitrine.route("/departamento", methods=["POST"])
def create_or_update_departamento():
    data_list = request.form.to_dict(flat=False)
    files = request.files

    insert_sql = """
        INSERT INTO departamento (
            dep_id, org_cod, dep_nom, dep_des, dep_email, dep_site, dep_sigla, dep_tel, img_data
        ) VALUES (
            %(dep_id)s, %(org_cod)s, %(dep_nom)s, %(dep_des)s, %(dep_email)s, %(dep_site)s, %(dep_sigla)s, %(dep_tel)s, %(img_data)s
        )
    """

    update_sql = """
        UPDATE departamento SET 
            org_cod=%(org_cod)s, dep_nom=%(dep_nom)s, dep_des=%(dep_des)s, 
            dep_email=%(dep_email)s, dep_site=%(dep_site)s, dep_sigla=%(dep_sigla)s, dep_tel=%(dep_tel)s, 
            img_data=%(img_data)s
        WHERE dep_id=%(dep_id)s
    """

    try:
        for i in range(len(data_list["dep_id"])):
            params = {
                "dep_id": data_list["dep_id"][i],
                "org_cod": data_list.get("org_cod", [None])[i],
                "dep_nom": data_list.get("dep_nom", [None])[i],
                "dep_des": data_list.get("dep_des", [None])[i],
                "dep_email": data_list.get("dep_email", [None])[i],
                "dep_site": data_list.get("dep_site", [None])[i],
                "dep_sigla": data_list.get("dep_sigla", [None])[i],
                "dep_tel": data_list.get("dep_tel", [None])[i],
                "img_data": psycopg2.Binary(files[f"img_data_{i}"].read())
                if f"img_data_{i}" in files
                else None,
            }

            try:
                conn.exec(insert_sql, params)
            except psycopg2.errors.UniqueViolation:
                conn.exec(update_sql, params)

        return jsonify({"message": "Departamentos processed successfully"}), 201
    except Exception as e:
        return jsonify(
            {"message": "Error processing departamentos", "error": str(e)}
        ), 500


# getDepartamentos
@rest_vitrine.route("/getDepartamentos", methods=["GET"])
def get_departamentos():
    try:
        scriptSql = "SELECT dep_id, org_cod, dep_nom, dep_des, dep_email, dep_site, dep_sigla, dep_tel, img_data FROM departamento"
        result = conn.select(scriptSql)
        departamentos = []
        for row in result:
            departamento = {
                "dep_id": row[0],
                "org_cod": row[1],
                "dep_nom": row[2],
                "dep_des": row[3],
                "dep_email": row[4],
                "dep_site": row[5],
                "dep_sigla": row[6],
                "dep_tel": row[7],
                # 'img_data': row[8],  # Se necessário, você pode enviar img_data como uma string ou outro formato
            }
            departamentos.append(departamento)

        return jsonify(departamentos), 200
    except Exception as e:
        return jsonify({"message": str(e)}), 400


# tecnicos


@rest_vitrine.route("/tecnicos", methods=["POST"])
def create_or_update_tecnicos():
    data_list = request.json

    script_sql = """
        DELETE FROM tecnico 
        WHERE carga_id = (
            SELECT 
                id 
            FROM 
                carga_ano_semestre 
            WHERE 
                year_charge = %(year_charge)s 
                AND semester = %(semester)s);
    """
    params = {
        "year_charge": data_list[0]["year_charge"],
        "semester": data_list[0]["semester"],
    }
    conn.exec(script_sql, params)

    select_sql = """
        SELECT 
            id 
        FROM 
            carga_ano_semestre 
        WHERE 
            year_charge = %(year_charge)s 
            AND semester = %(semester)s;
    """

    carga_id = conn.select(select_sql, params)

    insert_sql = """
        INSERT INTO tecnico (
            matric, ins_ufmg, nome, genero, deno_sit, 
            rt, classe, cargo, nivel, ref, titulacao, 
            setor, detalhe_setor, dting_org, data_prog,
            carga_id
        ) VALUES 
        (%(matric)s, %(ins_ufmg)s, %(nome)s, %(genero)s, %(deno_sit)s, %(rt)s, %(classe)s, %(cargo)s, %(nivel)s, %(ref)s, %(titulacao)s, %(setor)s, %(detalhe_setor)s, %(dting_org)s, %(data_prog)s, %(carga_id)s)
    """

    for record in data_list:
        params = {
            "matric": record.get("matric"),
            "ins_ufmg": record.get("ins_ufmg"),
            "nome": record.get("nome"),
            "genero": record.get("genero"),
            "deno_sit": record.get("deno_sit"),
            "rt": record.get("rt"),
            "classe": record.get("classe"),
            "cargo": record.get("cargo"),
            "nivel": record.get("nivel"),
            "ref": record.get("ref"),
            "titulacao": record.get("titulacao"),
            "setor": record.get("setor"),
            "detalhe_setor": record.get("detalhe_setor"),
            "dting_org": record.get("dting_org"),
            "data_prog": record.get("data_prog"),
            "carga_id": carga_id[0][0],
        }
        try:
            conn.exec(insert_sql, params)
        except Exception:
            print(f"Matricula já cadastrada: {record.get('matric')}")

    return jsonify({"message": "Técnicos processed successfully"}), 201

    # docentes


@rest_vitrine.route("/tecnicos", methods=["GET"])
def get_tecnicos():
    year = request.args.get("year")
    semester = request.args.get("semester")
    if year or semester:
        year_filter = """
        LEFT JOIN carga_ano_semestre cs ON t.carga_id = cs.carga_id
        WHERE 
            year_charge = %(year_charge)s
            AND semester = %(semester)s 
        """
    else:
        year_filter = """
        LEFT JOIN carga_ano_semestre cs ON t.carga_id = cs.id
        WHERE carga_id = (
        SELECT id 
        FROM carga_ano_semestre 
        ORDER BY 
            year_charge DESC, 
            semester DESC
        LIMIT 1)
        """

    script_sql = """
    SELECT 
        matric,
        ins_ufmg,
        nome,
        genero,
        deno_sit,
        rt,
        classe,
        cargo,
        nivel,
        ref,
        titulacao, 
        setor,
        detalhe_setor,
        dting_org,
        data_prog
    FROM
        tecnico t
    {year_filter}
    """
    reg = conn.select(script_sql, {"year_charge": year, "semester": semester})
    df = pd.DataFrame(
        reg,
        columns=[
            "matric",
            "ins_ufmg",
            "nome",
            "genero",
            "deno_sit",
            "rt",
            "classe",
            "cargo",
            "nivel",
            "ref",
            "titulacao",
            "setor",
            "detalhe_setor",
            "dting_org",
            "data_prog",
        ],
    )

    return jsonify(df.to_dict(orient="records"))


@rest_vitrine.route("/docentes", methods=["GET"])
def get_docentes():
    year = request.args.get("year")
    semester = request.args.get("semester")
    if year or semester:
        year_filter = """
        LEFT JOIN carga_ano_semestre cs ON d.carga_id = cs.carga_id
        WHERE 
            cs.year_charge = %(year_charge)s
            AND cs.semester = %(semester)s 
        """
    else:
        year_filter = """
        LEFT JOIN carga_ano_semestre cs ON d.carga_id = cs.id
        WHERE carga_id = (
        SELECT id 
        FROM carga_ano_semestre 
        ORDER BY 
            year_charge DESC, 
            semester DESC
        LIMIT 1)
        """

    script_sql = """
    SELECT 
        matric, 
        inscUFMG, 
        nome, 
        genero, 
        situacao, 
        rt, 
        clas, 
        cargo, 
        classe, 
        ref, 
        titulacao, 
        entradaNaUFMG, 
        progressao
    FROM docentes d
    {year_filter}
    """
    reg = conn.select(script_sql, {"year_charge": year, "semester": semester})

    df = pd.DataFrame(
        reg,
        columns=[
            "matric",
            "inscUFMG",
            "nome",
            "genero",
            "situacao",
            "rt",
            "clas",
            "cargo",
            "classe",
            "ref",
            "titulacao",
            "entradaNaUFMG",
            "progressao",
        ],
    )
    return jsonify(df.to_dict(orient="records"))


@rest_vitrine.route("/docentes", methods=["POST"])
def create_or_update_docentes():
    data_list = request.json

    select_sql = """
        SELECT carga_id FROM carga_ano_semestre WHERE year_charge = %(year_charge)s AND semester = %(semester)s 
    """

    delete_sql = """
        DELETE FROM docentes 
        WHERE carga_id = (
            SELECT 
                id 
            FROM 
                carga_ano_semestre 
            WHERE 
                year_charge = %(year_charge)s 
                AND semester = %(semester)s);
    """
    select_sql = """
        SELECT 
            id 
        FROM 
            carga_ano_semestre 
        WHERE 
            year_charge = %(year_charge)s 
            AND semester = %(semester)s;
    """
    delete_data = {
        "year_charge": data_list[0]["year_charge"],
        "semester": data_list[0]["semester"],
    }
    carga_id = conn.select(select_sql, delete_data)
    insert_sql = """
        INSERT INTO docentes (
            matric, inscUFMG, nome, genero, situacao, rt, clas, cargo, classe, ref, titulacao, entradaNaUFMG, progressao, carga_id
        ) VALUES (
            %(matric)s, %(inscUFMG)s, %(nome)s, %(genero)s, %(situacao)s, %(rt)s, %(clas)s, %(cargo)s, %(classe)s, %(ref)s, %(titulacao)s, %(entradaNaUFMG)s, %(progressao)s, %(carga_id)s
        )
    """
    conn.exec(delete_sql, delete_data)  # Reset the table
    for record in data_list:
        params = {
            "matric": record.get("matric"),
            "inscUFMG": record.get("inscUFMG"),
            "nome": record.get("nome"),
            "genero": record.get("genero"),
            "situacao": record.get("situacao"),
            "rt": record.get("rt"),
            "clas": record.get("clas"),
            "cargo": record.get("cargo"),
            "classe": record.get("classe"),
            "ref": record.get("ref"),
            "titulacao": record.get("titulacao"),
            "entradaNaUFMG": record.get("entradaNaUFMG"),
            "progressao": record.get("progressao"),
            "carga_id": carga_id[0][0],
        }
        try:
            conn.exec(insert_sql, params)
        except Exception:
            print(f"Matricula já cadastrada: {record.get('matric')}")
    return jsonify({"message": "Docentes processed successfully"}), 201


@rest_vitrine.route("/detalhes_patrimonio", methods=["GET"])
def get_observacao():
    codigo = request.args.get("codigo")
    id_item = request.args.get("id_item")
    resultado = dao_patrimonio.get_patrimonio(codigo, id_item)
    return jsonify(resultado), 200


@rest_vitrine.route("/detalhes_patrimonio", methods=["POST"])
def add_observacao():
    data = request.get_json()
    print(data)
    conn.exec(
        """
        INSERT INTO public.novo_item 
        (observacoes, estado_do_bem, tipo, codigo, imagens, email_insercao, id_usuario, status)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
    """,
        (
            data["observacoes"],
            data.get("estado_do_bem"),
            data["tipo"],
            data["codigo"],
            data.get("imagens"),
            data["email_insercao"],
            data["id_usuario"],
            data["status"],
        ),
    )

    return jsonify("OK"), 200


@rest_vitrine.route("/patrimonio", methods=["DELETE"])
def delete_patrimonio():
    bem_cod = request.args.get("bem_cod")
    bem_dgv = request.args.get("bem_dgv")

    if not bem_cod and not bem_dgv:
        return {"erro": "É necessário informar pelo menos bem_cod ou bem_dgv"}, 400

    condicoes = []
    valores = []

    if bem_cod:
        condicoes.append("bem_cod = %s")
        valores.append(bem_cod)

    if bem_dgv:
        condicoes.append("bem_dgv = %s")
        valores.append(bem_dgv)

    where_clause = " AND ".join(condicoes)

    SCRIPT_SQL = f"""
    DELETE FROM patrimonio 
    WHERE {where_clause};
    """

    conn.exec(SCRIPT_SQL, valores)

    if bem_cod and bem_dgv:
        doc_id = f"{bem_cod}_{bem_dgv}"
        collection.document(doc_id).delete()

    return {"mensagem": "Registro(s) deletado(s) com sucesso"}


@rest_vitrine.route("/detalhes_patrimonio/<string:id_item>", methods=["PUT"])
def update_observacao(id_item):
    data = request.get_json()

    conn.exec(
        """
        UPDATE public.novo_item
        SET 
            observacoes = %s, 
            estado_do_bem = %s, 
            tipo = %s, 
            codigo = %s, 
            imagens = %s, 
            email_insercao = %s, 
            id_usuario = %s, 
            status = %s
        WHERE id_item = %s;
        """,
        (
            data.get("observacoes"),
            data.get("estado_do_bem"),
            data.get("tipo"),
            data.get("codigo"),
            data.get("imagens"),
            data.get("email_insercao"),
            data.get("id_usuario"),
            data.get("status"),
            id_item,
        ),
    )

    return jsonify("OK"), 200


@rest_vitrine.route("/detalhes_patrimonio/<string:id_item>", methods=["DELETE"])
def delete_observacao(id_item):
    conn.exec(
        """
        DELETE FROM public.novo_item
        WHERE id_item = %s;
        """,
        [id_item],
    )

    return jsonify({"message": "Registro deletado com sucesso!"}), 204

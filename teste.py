# TOTAL DE PATRIMONIOS
@app.route("/totalPatrimonio", methods=["GET"])
@cross_origin(origin="*", headers=["Content-Type"])
def totalPatrimonio():
    loc_nom = request.args.get('loc_nom')

    # Base queries
    scriptSqlPatrimonio = 'SELECT COUNT(*) AS total_patrimonio FROM Patrimonio'
    scriptSqlPatrimonioMorto = 'SELECT COUNT(*) AS total_patrimonio_morto FROM PatrimonioMorto'
    
    # Adding filter for loc_nom if provided
    if loc_nom:
        scriptSqlPatrimonio += f" WHERE loc_nom = '{loc_nom}'"
        scriptSqlPatrimonioMorto += f" WHERE loc_nom = '{loc_nom}'"

    # Combine both queries with UNION ALL
    scriptSql = f'''
    {scriptSqlPatrimonio}
    UNION ALL
    {scriptSqlPatrimonioMorto}
    '''
    
    resultado = connection.select(scriptSql)
    
    # Presuming the query returns two rows
    total_patrimonio = resultado[0][0] if resultado else 0
    total_patrimonio_morto = resultado[1][0] if len(resultado) > 1 else 0

    response = {
        "total_patrimonio": total_patrimonio,
        "total_patrimonio_morto": total_patrimonio_morto,
        "unique_values": []
    }

    if loc_nom:
        # Additional query for unique values
        scriptSqlUnique = f'''
        SELECT DISTINCT org_cod, org_nom, set_nom, set_cod, loc_cod, loc_nom, pes_cod, pes_nome
        FROM Patrimonio
        WHERE loc_nom = '{loc_nom}'
        '''
        unique_result = connection.select(scriptSqlUnique)

        unique_values = [
            {
                "org_cod": row[0],
                "org_nom": row[1],
                "set_nom": row[2],
                "set_cod": row[3],
                "loc_cod": row[4],
                "loc_nom": row[5],
                "pes_cod": row[6],
                "pes_nome": row[7]
            }
            for row in unique_result
        ]

        response["unique_values"] = unique_values
    
    return jsonify([response]), HTTPStatus.OK


from flask import Flask, jsonify, request
from http import HTTPStatus
from database import Connection
import pandas as pd
from flask_cors import cross_origin
import psycopg2

import re
import difflib

connection = Connection()

def create_app():
    app = Flask(__name__)

    return app

app = create_app()

#INSERIr PATRIMONIO

@app.route("/insertPatrimonio", methods=["POST"])
@cross_origin(origin="*", headers=["Content-Type"])
def insertPatrimonio ():
    ListPatrimonio = request.get_json()

    for patrimonio in ListPatrimonio:
        values = str()
        print(patrimonio["bem_cod"], patrimonio["bem_dgv"])
        try:
            values += f"""(
                '{patrimonio["bem_cod"]}',
                '{patrimonio["bem_dgv"]}',
                '{patrimonio["bem_num_atm"]}',
                '{patrimonio["csv_cod"]}',
                '{patrimonio["bem_serie"]}',
                '{patrimonio["bem_sta"]}',
                '{patrimonio["bem_val"]}',
                '{patrimonio["tre_cod"]}',
                '{patrimonio["bem_dsc_com"].replace("'", ' ')}',
                '{patrimonio["uge_cod"]}',
                '{patrimonio["uge_nom"]}',
                '{patrimonio["org_cod"]}',
                '{patrimonio["uge_siaf"]}',
                '{patrimonio["org_nom"]}',
                '{patrimonio["set_cod"]}',
                '{patrimonio["set_nom"]}',
                '{patrimonio["loc_cod"]}',
                '{patrimonio["loc_nom"]}',
                '{patrimonio["ite_mar"]}',
                '{patrimonio["ite_mod"]}',
                '{patrimonio["tgr_cod"]}',
                '{patrimonio["grp_cod"]}',
                '{patrimonio["ele_cod"]}',
                '{patrimonio["sbe_cod"]}',
                '{patrimonio["mat_cod"]}',
                '{patrimonio["mat_nom"]}',
                '{patrimonio["pes_cod"]}',
                '{patrimonio["pes_nome"]}'),"""
                # Criação do script de insert.
                # Unifiquei em um unico comando para facilitar
                # o retorno da mensagem de erro
                
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

            connection.exec(script_sql)
        except psycopg2.errors.UniqueViolation:
            update_script =  f"""
            UPDATE patrimonio SET 
            bem_num_atm='{patrimonio["bem_num_atm"]}',  
            csv_cod='{patrimonio["csv_cod"]}',
            bem_serie='{patrimonio["bem_serie"]}',
            bem_sta='{patrimonio["bem_sta"]}',
            bem_val='{patrimonio["bem_val"]}',
            tre_cod='{patrimonio["tre_cod"]}',
            bem_dsc_com='{patrimonio["bem_dsc_com"].replace("'", ' ')}',
            uge_cod='{patrimonio["uge_cod"]}',
            uge_nom='{patrimonio["uge_nom"]}',
            org_cod='{patrimonio["org_cod"]}',
            uge_siaf='{patrimonio["uge_siaf"]}',
            org_nom='{patrimonio["org_nom"]}',
            set_cod='{patrimonio["set_cod"]}',
            set_nom='{patrimonio["set_nom"]}',
            loc_cod='{patrimonio["loc_cod"]}',
            loc_nom='{patrimonio["loc_nom"]}',
            ite_mar='{patrimonio["ite_mar"]}',
            ite_mod='{patrimonio["ite_mod"]}',
            tgr_cod='{patrimonio["tgr_cod"]}',
            grp_cod='{patrimonio["grp_cod"]}',
            ele_cod='{patrimonio["ele_cod"]}',
            sbe_cod='{patrimonio["sbe_cod"]}',
            mat_cod='{patrimonio["mat_cod"]}',
            mat_nom='{patrimonio["mat_nom"]}',
            pes_cod='{patrimonio["pes_cod"]}',
            pes_nome='{patrimonio["pes_nome"]}'
            WHERE bem_cod = '{patrimonio["bem_cod"]}' AND bem_dgv = '{patrimonio["bem_dgv"]}' 
            """
            connection.exec(update_script)
            
    return jsonify([]),201

# Função para normalizar o número ATM
def normalize_atm_number(atm_number):
    return re.sub(r'\D', '', atm_number)

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

# VERIFICAR SE EXISTE PATRIMONIO PELO ATM
@app.route("/searchByBemNumAtm", methods=["GET"])
@cross_origin(origin="*", headers=["Content-Type"])
def searchByBemNumAtm():
    bem_num_atm = request.args.get('bem_num_atm')
    
    # Buscar todos os valores de bem_num_atm das tabelas Patrimonio e PatrimonioMorto
    stored_values_sql = '''
    SELECT bem_num_atm FROM Patrimonio
    UNION
    SELECT bem_num_atm FROM PatrimonioMorto
    '''
    
    stored_values_result = connection.select(stored_values_sql)
    stored_values = [row[0] for row in stored_values_result]  # Acessando o primeiro elemento da tupla
    
    best_match = bayesian_matching(bem_num_atm, stored_values)
    
    if best_match:
        scriptSql = f'''
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
        '''
        
        resultado = connection.select(scriptSql)
        
        columns = [
            'bem_cod', 'bem_dgv', 'bem_dsc_com', 'bem_num_atm', 'uge_siaf', 
            'bem_sta', 'uge_cod', 'org_cod', 'set_cod', 'loc_cod', 'org_nom', 
            'created_at', 'csv_cod', 'bem_serie', 'bem_val', 'tre_cod', 
            'uge_nom', 'set_nom', 'loc_nom', 'ite_mar', 'ite_mod', 'tgr_cod', 
            'grp_cod', 'ele_cod', 'sbe_cod', 'mat_cod', 'mat_nom', 'pes_cod', 
            'pes_nome'
        ]
        
        dataFrame = pd.DataFrame(resultado, columns=columns)
        
        return jsonify(dataFrame.to_dict(orient='records'))
    
    return jsonify({"error": "No matching record found"}), 404


# VERIFICAR SE EXISTE PATRIMONIO
@app.route("/checkoutPatrimonio", methods=["GET"])
@cross_origin(origin="*", headers=["Content-Type"])
def checkoutPatrimonio():
    bem_cod = request.args.get('bem_cod').replace('.-', '')
    bem_dgv = request.args.get('bem_dgv')
    
    scriptSql = f'''
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
    '''
    
    resultado = connection.select(scriptSql)
    
    columns = [
        'bem_cod', 'bem_dgv', 'bem_dsc_com', 'bem_num_atm', 'uge_siaf', 
        'bem_sta', 'uge_cod', 'org_cod', 'set_cod', 'loc_cod', 'org_nom', 
        'created_at', 'csv_cod', 'bem_serie', 'bem_val', 'tre_cod', 
        'uge_nom', 'set_nom', 'loc_nom', 'ite_mar', 'ite_mod', 'tgr_cod', 
        'grp_cod', 'ele_cod', 'sbe_cod', 'mat_cod', 'mat_nom', 'pes_cod', 
        'pes_nome'
    ]
    
    dataFrame = pd.DataFrame(resultado, columns=columns)
    
    return jsonify(dataFrame.to_dict(orient='records'))

# TODOS PATRIMONIOS
@app.route("/allPatrimonio", methods=["GET"])
@cross_origin(origin="*", headers=["Content-Type"])
def allPatrimonio():
    loc_nom = request.args.get('loc_nom')
    print(loc_nom)

    # Get pagination parameters from the request
    page = int(request.args.get('page', 1))
    page_size = int(request.args.get('page_size', 100))
    offset = (page - 1) * page_size

    # SQL query with JOIN and pagination
    scriptSql = '''
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
    '''
    
    # Adding filter for loc_nom if provided
    if loc_nom:
        scriptSql += f" WHERE p.loc_nom = '{loc_nom}'"
    
    scriptSql += f" LIMIT {page_size} OFFSET {offset}"

    try:
        resultado = connection.select(scriptSql)
    except Exception as e:
        app.logger.error(f"Database query failed: {str(e)}")
        return jsonify({"error": "Database query failed", "details": str(e)}), HTTPStatus.INTERNAL_SERVER_ERROR
    
    if not resultado:
        return jsonify({"error": "No results found"}), HTTPStatus.NOT_FOUND

    columns = [
        'bem_cod', 'bem_dgv', 'bem_dsc_com', 'bem_num_atm', 'uge_siaf', 
        'bem_sta', 'uge_cod', 'org_cod', 'set_cod', 'loc_cod', 'org_nom', 
        'created_at', 'csv_cod', 'bem_serie', 'bem_val', 'tre_cod', 
        'uge_nom', 'set_nom', 'loc_nom', 'ite_mar', 'ite_mod', 'tgr_cod', 
        'grp_cod', 'ele_cod', 'sbe_cod', 'mat_cod', 'mat_nom', 'pes_cod', 
        'pes_nome'
    ]
    
    dataFrame = pd.DataFrame(resultado, columns=columns)
    
    return jsonify(dataFrame.to_dict(orient='records')), HTTPStatus.OK

# TODOS PATRIMONIOS MORTO
@app.route("/allPatrimonioMorto", methods=["GET"])
@cross_origin(origin="*", headers=["Content-Type"])
def allPatrimonioMorto():
    try:
        # Get pagination parameters from the request
        page = int(request.args.get('page', 1))
        page_size = int(request.args.get('page_size', 100))
        offset = (page - 1) * page_size

        # SQL query with LIMIT and OFFSET for pagination
        scriptSql = f'''
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
        LIMIT {page_size} OFFSET {offset}
        '''
        
        resultado = connection.select(scriptSql)
        
        columns = [
             'bem_cod', 
            'bem_dgv', 
            'bem_dsc_com', 
            'bem_num_atm', 
            'uge_siaf', 
            'bem_sta', 
            'uge_cod', 
            'org_cod', 
            'set_cod', 
            'loc_cod', 
            'org_nom',
            'set_nom',
            'uge_nom',
            'loc_nom',
            'mat_nom'
        ]
        
        dataFrame = pd.DataFrame(resultado, columns=columns)
        
        return jsonify(dataFrame.to_dict(orient='records')), HTTPStatus.OK
    except Exception as e:
        return jsonify({"error": str(e)}), HTTPStatus.INTERNAL_SERVER_ERROR


# INSERIR PATRIMONIO MORTO
@app.route("/insertPatrimonioMorto", methods=["POST"])
@cross_origin(origin="*", headers=["Content-Type"])
def insertPatrimonioMorto():
    ListPatrimonio = request.get_json()
    for patrimonio in ListPatrimonio:
        values = str()
        print(patrimonio["bem_cod"], patrimonio["bem_dgv"])
        try:
            values += f"""(
                '{patrimonio['bem_cod']}', 
                '{patrimonio['bem_dgv']}', 
                '{patrimonio['bem_dsc_com'].replace("'", ' ')}', 
                '{patrimonio['bem_num_atm']}', 
                '{patrimonio['uge_siaf']}', 
                '{patrimonio['bem_sta']}', 
                '{patrimonio['uge_cod']}', 
                '{patrimonio['org_cod']}', 
                '{patrimonio['set_cod']}', 
                '{patrimonio['loc_cod']}', 
                '{patrimonio['org_nom']}',
                '{patrimonio['set_nom']}',
                '{patrimonio['uge_nom']}',
                '{patrimonio['loc_nom']}',
                '{patrimonio['mat_nom']}'
                
                ),"""
        
            scriptSql = f'''
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
            '''
            
            connection.exec(scriptSql)

        except psycopg2.errors.UniqueViolation:
            update_script =  f"""
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
            connection.exec(update_script)
            
    return jsonify([]),201
    
   

# TOTAL DE PATRIMONIOS
@app.route("/totalPatrimonio", methods=["GET"])
@cross_origin(origin="*", headers=["Content-Type"])
def totalPatrimonio():
    loc_nom = request.args.get('loc_nom')
    print(loc_nom)

    # Base queries
    scriptSqlPatrimonio = 'SELECT COUNT(*) AS total_patrimonio FROM Patrimonio'
    scriptSqlPatrimonioMorto = 'SELECT COUNT(*) AS total_patrimonio_morto FROM PatrimonioMorto'
    
    scriptSql = f'''
    {scriptSqlPatrimonio}
    UNION ALL
    {scriptSqlPatrimonioMorto}
    '''
    # Adding filter for loc_nom if provided
    if loc_nom:
        scriptSqlPatrimonio += f" WHERE loc_nom = '{loc_nom}'"
        
        # Combine both queries with UNION ALL
        scriptSql = f'''
        {scriptSqlPatrimonio}
 
        '''
    
    resultado = connection.select(scriptSql)
    
    # Presuming the query returns two rows
    total_patrimonio = resultado[0][0] if resultado else 0
    total_patrimonio_morto = resultado[1][0] if len(resultado) > 1 else 0

    response = {
        "total_patrimonio": total_patrimonio,
        "total_patrimonio_morto": total_patrimonio_morto,
        "unique_values": []
    }

    if loc_nom:
        # Additional query for unique values
        scriptSqlUnique = f'''
        SELECT DISTINCT org_cod, org_nom, set_nom, set_cod, loc_cod, loc_nom, pes_cod, pes_nome
        FROM Patrimonio
        WHERE loc_nom = '{loc_nom}'
        '''
        unique_result = connection.select(scriptSqlUnique)

        unique_values = [
            {
                "org_cod": row[0],
                "org_nom": row[1],
                "set_nom": row[2],
                "set_cod": row[3],
                "loc_cod": row[4],
                "loc_nom": row[5],
                "pes_cod": row[6],
                "pes_nome": row[7]
            }
            for row in unique_result
        ]

        response["unique_values"] = unique_values
    
    return jsonify([response]), HTTPStatus.OK

# INSERIR CONDICAO BEM

@app.route("/insertCondicaoBem", methods=["POST"])
@cross_origin(origin="*", headers=["Content-Type"])
def insertCondicaoBem ():
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

            connection.exec(script_sql)
        except psycopg2.errors.UniqueViolation:
            update_script =  f"""
            UPDATE public.condicao_bem SET 
            csv_cod='{patrimonio["csv_cod"]}'
            WHERE bem_cod = '{patrimonio["bem_cod"]}' AND bem_dgv = '{patrimonio["bem_dgv"]}' 
            """
            connection.exec(update_script)
            
    return jsonify([]),201

#FILTRO POR CSV

@app.route("/filterByCsvCod", methods=["GET"])
@cross_origin(origin="*", headers=["Content-Type"])
def filterByCsvCod():
    csv_cod = request.args.get('csv_cod')

    # SQL query with JOIN and filter by csv_cod
    scriptSql = '''
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
        cb.csv_cod = %s
    '''

    resultado = connection.select(scriptSql, (csv_cod,))

    columns = [
        'bem_cod', 'bem_dgv', 'bem_dsc_com', 'bem_num_atm', 'uge_siaf', 
        'bem_sta', 'uge_cod', 'org_cod', 'set_cod', 'loc_cod', 'org_nom', 
        'created_at', 'csv_cod', 'bem_serie', 'bem_val', 'tre_cod', 
        'uge_nom', 'set_nom', 'loc_nom', 'ite_mar', 'ite_mod', 'tgr_cod', 
        'grp_cod', 'ele_cod', 'sbe_cod', 'mat_cod', 'mat_nom', 'pes_cod', 
        'pes_nome'
    ]
    
    dataFrame = pd.DataFrame(resultado, columns=columns)
    return jsonify(dataFrame.to_dict(orient='records')), HTTPStatus.OK
   


if __name__ == "__main__":
    app.run(debug=True)

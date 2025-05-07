from . import Connection
from ..models import UserModel
import pandas as pd

conn = Connection()


def buscar_loc(user_id):
    SCRIPT_SQL = """
        SELECT loc_cod, loc_nom FROM patrimonio
        WHERE unaccent(lower(regexp_replace(pes_nome, '[^a-zA-Z0-9]', '', 'g'))) = 
            (SELECT unaccent(lower(regexp_replace(display_name, '[^a-zA-Z0-9]', '', 'g')))
            FROM users WHERE user_id = %(user_id)s);
        """
    result = conn.select(SCRIPT_SQL, {"user_id": user_id})
    data_frame = pd.DataFrame(result, columns=["loc_cod", "loc_nom"])
    return data_frame.to_dict(orient="records")


def buscar_bens(user_id, loc_cod):
    param = {"user_id": user_id}
    filter_loc_cod = str()
    if loc_cod:
        param["loc_cod"] = loc_cod
        filter_loc_cod = "AND loc_cod = %(loc_cod)s"

    SCRIPT_SQL = f"""
        SELECT bem_cod, bem_dgv, bem_num_atm, csv_cod, bem_serie, bem_sta, bem_val, 
            tre_cod, bem_dsc_com, uge_cod, uge_nom, org_cod, uge_siaf, org_nom, 
            set_cod, set_nom, loc_cod, loc_nom, ite_mar, ite_mod, tgr_cod, grp_cod, 
            ele_cod, sbe_cod, mat_cod, mat_nom, pes_cod, pes_nome, created_at
        FROM public.patrimonio 
        WHERE unaccent(lower(regexp_replace(pes_nome, '[^a-zA-Z0-9]', '', 'g'))) = 
            (SELECT unaccent(lower(regexp_replace(display_name, '[^a-zA-Z0-9]', '', 'g')))
            FROM users WHERE user_id = %(user_id)s)
            {filter_loc_cod};
        """
    result = conn.select(SCRIPT_SQL, param)
    data_frame = pd.DataFrame(
        result,
        columns=[
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
            "created_at",
        ],
    )
    return data_frame.to_dict(orient="records")


def adicionar_imagem(id, image_token):
    SCRIPT_SQL = """
        UPDATE public.formulario_patrimonio
        SET imagens = array_append(imagens, %(image_token)s)
        WHERE patrimonio_id = %(id)s;
        """
    conn.exec(SCRIPT_SQL, {"id": id, "image_token": image_token})


def remover_imagem(image_token):
    SCRIPT_SQL = """
        UPDATE public.formulario_patrimonio 
        SET imagens = array_remove(imagens, %(image_token)s) 
        WHERE patrimonio_id = %(id)s;
        """
    conn.exec(SCRIPT_SQL, {"id": image_token[17:], "image_token": image_token})


def get_photo_url(user_id):
    SCRIPT_SQL = """
        SELECT photo_url FROM users WHERE user_id = %(user_id)s;
        """
    result = conn.select(SCRIPT_SQL, {"user_id": user_id})
    if result:
        return result[0][0]


def create_user(User: UserModel):
    print(User)
    SCRIPT_SQL = """
        INSERT INTO users (display_name, email, uid, photo_url, provider, matricula, telephone, ramal, institution_id)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);
        """
    conn.exec(
        SCRIPT_SQL,
        [
            User.displayName,
            User.email,
            User.uid,
            str(User.photoURL) or str(),
            User.provider or str(),
            User.matricula or str(),
            User.telephone or str(),
            User.ramal or str(),
            User.institution_id or str(),
        ],
    )


def select_user(uid):
    SCRIPT_SQL = """
        SELECT 
            u.user_id, display_name, email, uid, photo_url, lattes_id, institution_id,
            provider, linkedin, verify, phone, matricula, telephone, ramal, qtd_de_favorito,
            uge_nom
        FROM users u
        LEFT JOIN (SELECT COUNT(*) AS qtd_de_favorito, user_id FROM favoritos GROUP BY user_id) f 
        ON f.user_id = u.user_id
        LEFT JOIN institution i ON i.id = u.institution_id
        WHERE uid = %(uid)s;
    """
    registry = conn.select(SCRIPT_SQL, {"uid": uid})
    data_frame = pd.DataFrame(
        registry,
        columns=[
            "user_id",
            "display_name",
            "email",
            "uid",
            "photo_url",
            "lattes_id",
            "institution_id",
            "provider",
            "linkedin",
            "verify",
            "phone",
            "matricula",
            "telephone",
            "ramal",
            "qtd_de_favorito",
            "uge_nom",
        ],
    )
    print(data_frame)
    data_frame = data_frame.merge(users_roles(), on="user_id", how="left")

    return data_frame.to_dict(orient="records")


def list_all_users():
    SCRIPT_SQL = """
        SELECT 
            u.user_id,
            display_name,
            email,
            uid,
            photo_url,
            linkedin,
            provider,
            u.lattes_id,
            u.institution_id,
            rr.name AS researcher_name
        FROM users u
        LEFT JOIN researcher rr ON rr.lattes_id = u.lattes_id
        GROUP BY 
            u.user_id, display_name, email, uid, photo_url, linkedin, provider, 
            u.lattes_id, u.institution_id, rr.name;
        """
    registry = conn.select(SCRIPT_SQL)

    data_frame = pd.DataFrame(
        registry,
        columns=[
            "user_id",
            "display_name",
            "email",
            "uid",
            "photo_url",
            "linkedin",
            "provider",
            "lattes_id",
            "institution_id",
            "researcher_name",
        ],
    )

    data_frame = data_frame.merge(users_roles(), on="user_id", how="left")
    data_frame = data_frame.merge(users_graduate_program(), on="lattes_id", how="left")
    data_frame = data_frame.merge(users_departaments(), on="lattes_id", how="left")

    data_frame.fillna("", inplace=True)
    return data_frame.to_dict(orient="records")


def update_user(user):
    fields_to_update = []
    values = []

    if "linkedin" in user:
        fields_to_update.append("linkedin = %s")
        values.append(user["linkedin"])
    if "lattes_id" in user:
        fields_to_update.append("lattes_id = %s")
        values.append(user["lattes_id"])
    if "displayName" in user:
        fields_to_update.append("display_name = %s")
        values.append(user["displayName"])
    if "email" in user:
        fields_to_update.append("email = %s")
        values.append(user["email"])
    if "photoURL" in user:
        fields_to_update.append("photo_url = %s")
        values.append(user["photoURL"])
    if "provider" in user:
        fields_to_update.append("provider = %s")
        values.append(user["provider"])
    if "phone" in user:
        fields_to_update.append("phone = %s")
        values.append(user["phone"])
    if "matricula" in user:
        fields_to_update.append("matricula = %s")
        values.append(user["matricula"])
    if "telephone" in user:
        fields_to_update.append("telephone = %s")
        values.append(user["telephone"])
    if "ramal" in user:
        fields_to_update.append("ramal = %s")
        values.append(user["ramal"])
    if "uge_nom" in user:
        fields_to_update.append("uge_nom = %s")
        values.append(user["uge_nom"])

    if not fields_to_update:
        print("Nenhum campo fornecido para atualizar.")
        return

    SCRIPT_SQL = f"""
    UPDATE users
    SET {", ".join(fields_to_update)}
    WHERE uid = %s
    """
    values.append(user["uid"])
    conn.exec(SCRIPT_SQL, values)


def list_users():
    SCRIPT_SQL = """
        SELECT
            u.user_id, display_name, email,
            jsonb_agg(jsonb_build_object('role', rl.role, 'role_id', rl.id)) AS roles,
            photo_url, MAX(uge_nom) AS uge_nom
        FROM users u
        LEFT JOIN users_roles ur ON u.user_id = ur.user_id
        LEFT JOIN roles rl ON rl.id = ur.role_id
        LEFT JOIN institution i ON i.id = u.institution_id
        GROUP BY u.user_id;
        """
    registry = conn.select(SCRIPT_SQL)
    data_frame = pd.DataFrame(
        registry,
        columns=["user_id", "display_name", "email", "roles", "photo_url", "uge_nom"],
    )

    return data_frame.to_dict(orient="records")


def create_new_role(role):
    SCRIPT_SQL = """
        INSERT INTO roles (role)
        VALUES (%s)
        """
    conn.exec(SCRIPT_SQL, [role[0]["role"]])


def view_roles():
    SCRIPT_SQL = """
        SELECT id, role
        FROM roles
        """
    reg = conn.select(SCRIPT_SQL)
    data_frame = pd.DataFrame(reg, columns=["id", "role"])
    return data_frame.to_dict(orient="records")


def update_role(role):
    SCRIPT_SQL = """
        UPDATE roles
        SET role = %s
        WHERE id = %s;
        """
    conn.exec(SCRIPT_SQL, [role[0]["role"], role[0]["id"]])


def delete_role(role):
    SCRIPT_SQL = """
        DELETE FROM roles
        WHERE id = %s;
        """
    conn.exec(SCRIPT_SQL, [role[0]["id"]])


def create_new_permission(permission):
    SCRIPT_SQL = """
        INSERT INTO permission (role_id, permission)
        VALUES (%s, %s);
        """
    conn.exec(SCRIPT_SQL, [permission[0]["role_id"], permission[0]["permission"]])


def permissions_view(role_id):
    SCRIPT_SQL = """
    SELECT id, permission AS permission
    FROM permission
    WHERE role_id = %s
    """
    reg = conn.select(SCRIPT_SQL, [role_id])
    data_frame = pd.DataFrame(reg, columns=["id", "permission"])
    return data_frame.to_dict(orient="records")


def update_permission(permission):
    SCRIPT_SQL = """
        UPDATE permission
        SET permission = %s
        WHERE id = %s;
        """
    conn.exec(SCRIPT_SQL, [permission[0]["permission"], permission[0]["id"]])


def delete_permission(permission):
    SCRIPT_SQL = """
        DELETE FROM permission
        WHERE id = %s;
        """
    conn.exec(SCRIPT_SQL, [permission[0]["id"]])


def assign_user(user):
    SCRIPT_SQL = """
        UPDATE users SET
        institution_id = %s
        WHERE user_id = %s;

        INSERT INTO users_roles (role_id, user_id)
        VALUES (%s, %s);
        """
    conn.exec(
        SCRIPT_SQL,
        [
            user[0]["institution_id"],
            user[0]["user_id"],
            user[0]["role_id"],
            user[0]["user_id"],
        ],
    )


def view_user_roles(uid, role_id):
    SCRIPT_SQL = """
        SELECT
            r.id,
            p.permission
        FROM
            users_roles ur
            LEFT JOIN roles r ON r.id = ur.role_id
            LEFT JOIN permission p ON p.role_id = ur.role_id
            LEFT JOIN users u ON u.user_id = ur.user_id
        WHERE u.uid = %s AND r.id = %s
        """

    reg = conn.select(SCRIPT_SQL, [uid, role_id])
    data_frame = pd.DataFrame(reg, columns=["role_id", "permissions"])
    return data_frame.to_dict(orient="records")


def unassign_user(user):
    SCRIPT_SQL = """
        DELETE FROM users_roles
        WHERE role_id = %s AND user_id = %s;
        """
    conn.exec(SCRIPT_SQL, [user[0]["role_id"], user[0]["user_id"]])


def users_roles():
    SCRIPT_SQL = """
        SELECT
            u.user_id,
            jsonb_agg(jsonb_build_object('id', r.id, 'role_id', r.role)) AS roles
        FROM users u
        LEFT JOIN users_roles ur ON ur.user_id = u.user_id
        LEFT JOIN roles r ON r.id = ur.role_id
        GROUP BY u.user_id
        """
    registry = conn.select(SCRIPT_SQL)

    data_frame = pd.DataFrame(registry, columns=["user_id", "roles"])

    return data_frame


def users_graduate_program():
    SCRIPT_SQL = """
        SELECT
            r.lattes_id,
            jsonb_agg(jsonb_build_object('graduate_program_id', gp.graduate_program_id, 'name', gp.name)) AS graduate_program
        FROM graduate_program_researcher gpr
        LEFT JOIN graduate_program gp ON gp.graduate_program_id = gpr.graduate_program_id
        LEFT JOIN researcher r ON gpr.researcher_id = r.researcher_id
        GROUP BY r.lattes_id
        """
    registry = conn.select(SCRIPT_SQL)

    data_frame = pd.DataFrame(registry, columns=["lattes_id", "graduate_program"])

    return data_frame


def users_departaments():
    SCRIPT_SQL = """
        SELECT
            r.lattes_id,
            jsonb_agg(jsonb_build_object('name', d.dep_nom, 'dep_id', d.dep_id)) AS departament
        FROM ufmg.departament_researcher dr
        LEFT JOIN ufmg.departament d ON d.dep_id = dr.dep_id
        LEFT JOIN researcher r ON r.researcher_id = dr.researcher_id
        GROUP BY r.lattes_id
        """
    registry = conn.select(SCRIPT_SQL)

    data_frame = pd.DataFrame(registry, columns=["lattes_id", "departament"])

    return data_frame


def adicionar_favorito(id, tipo, user_id):
    params = {
        "id": id,
        "tipo": tipo,
        "user_id": user_id,
    }
    SCRIPT_SQL = """
        INSERT INTO public.favoritos(
        id, tipo, user_id)
        VALUES (%(id)s, %(tipo)s, %(user_id)s);
        """
    conn.exec(SCRIPT_SQL, params)


def consultar_favoritos(tipo, user_id):
    params = {}

    filtro_tipo = str()
    if tipo:
        params["tipo"] = tipo
        filtro_tipo = "AND f.tipo = %(tipo)s"

    filtro_user = str()
    if user_id:
        params["user_id"] = user_id
        filtro_user = "AND f.user_id = %(user_id)s"

    SCRIPT_SQL = f"""
        SELECT fp.patrimonio_id, fp.num_patrimonio, fp.loc, fp.observacao, 
            fp.matricula, fp.user_id, fp.vitrine, fp.condicao, fp.imagens, 
            fp.desfazimento, fp.verificado, fp.num_verificacao, fp.codigo_atm, 
            fp.situacao, fp.material, COALESCE(ff.qtd_de_favorito, 0), fp.created_at,
            u.display_name, u.email, u.matricula, u.phone
        FROM public.formulario_patrimonio AS fp
            LEFT JOIN users u ON u.user_id = fp.user_id
            LEFT JOIN favoritos f ON f.user_id = fp.user_id AND fp.patrimonio_id = f.id
            LEFT JOIN (SELECT COUNT(*) as qtd_de_favorito, id FROM favoritos GROUP BY id) ff ON f.id = fp.patrimonio_id
        WHERE 1 = 1
            {filtro_user}
            {filtro_tipo};
        """
    result = conn.select(SCRIPT_SQL, params)
    dataframe = pd.DataFrame(
        result,
        columns=[
            "patrimonio_id",
            "num_patrimonio",
            "loc",
            "observacao",
            "matricula",
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
            "qtd_de_favorito",
            "created_at",
            "display_name",
            "email",
            "u_matricula",
            "phone",
        ],
    )
    return dataframe.to_dict(orient="records")


def deletar_favorito(id, user_id, tipo):
    SCRIPT_SQL = """
        DELETE FROM favoritos WHERE id = %(id)s AND tipo = %(tipo)s AND user_id = %(user_id)s;
        """
    conn.exec(SCRIPT_SQL, {"id": id, "tipo": tipo, "user_id": user_id})


def photo_url(user_id):
    SCRIPT_SQL = """
        SELECT photo_url FROM users WHERE user_id = %(user_id)s
        """
    result = conn.select(SCRIPT_SQL, {"user_id": user_id})
    return result[0][0]

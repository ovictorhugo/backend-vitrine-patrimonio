from http import HTTPStatus
import secrets
import psycopg2
import requests
from flask import Blueprint, jsonify, request, send_file
import os
from ..dao import dao_user
from ..models import UserModel

rest_user = Blueprint("rest_system_management", __name__)

UPLOAD_FOLDER = os.path.join(os.getcwd(), "src")
os.makedirs(UPLOAD_FOLDER, exist_ok=True)


@rest_user.route("/bens/<id>")
def buscar_bens(id):
    loc = request.args.get("loc")
    bens = dao_user.buscar_bens(id, loc)
    return bens


@rest_user.route("/loc/<id>")
def buscar_loc(id):
    bens = dao_user.buscar_loc(id)
    return bens


@rest_user.route("/s/user/imagem/<id>", methods=["GET"])
def user_image(id):
    for extension in ["jpg", "jpeg", "png", "gif"]:
        file_path = os.path.join(UPLOAD_FOLDER, f"{id}.{extension}")
        if os.path.isfile(file_path):
            return send_file(file_path)

    photo_url = dao_user.get_photo_url(id)
    response = requests.get(photo_url, stream=True)
    if response.status_code == 200:
        path = f"{UPLOAD_FOLDER}/{id}.jpg"
        with open(path, "wb") as photo:
            photo.write(response.content)
        return send_file(path)
    return "No files sent", 200


@rest_user.route("/imagem/<id>", methods=["POST"])
def enviar_imagem(id):
    if "file" not in request.files:
        return "No files sent", 400
    file = request.files["file"]
    if file.filename is None:
        return "No files selected", 400
    if "." in file.filename:
        extension = file.filename.rsplit(".", 1)[1]
    else:
        extension = "jpg"

    image_token = secrets.token_hex(8) + "_" + id
    file_path = os.path.join(UPLOAD_FOLDER, f"{image_token}.{extension}")

    dao_user.adicionar_imagem(id, image_token)
    file.save(file_path)
    return "Image saved successfully", 200


@rest_user.route("/imagem/<id>", methods=["GET"])
def buscar_imagem(id):
    for extension in ["jpg", "jpeg", "png", "gif"]:
        file_path = os.path.join(UPLOAD_FOLDER, f"{id}.{extension}")
        if os.path.isfile(file_path):
            return send_file(file_path)
    return "No image"


@rest_user.route("/imagem/<id>", methods=["DELETE"])
def delete_image(id):
    for extension in ["jpg", "jpeg", "png", "gif"]:
        file_path = os.path.join(UPLOAD_FOLDER, f"{id}.{extension}")
        print(file_path)
        if os.path.isfile(file_path):
            os.remove(file_path)
            dao_user.remover_imagem(id)
            return "Image deleted successfully", 200
    return "Image not found", 404


@rest_user.route("/favorito", methods=["POST"])
def adicionar_favorito():
    id = request.args.get("id")
    tipo = request.args.get("tipo", "")
    user_id = request.args.get("user_id")
    dao_user.adicionar_favorito(id, tipo, user_id)
    return "OK", 200


@rest_user.route("/favorito", methods=["GET"])
def consultar_favoritos():
    tipo = request.args.get("tipo")
    user_id = request.args.get("user_id")
    lista = dao_user.consultar_favoritos(tipo, user_id)
    return lista


@rest_user.route("/favorito", methods=["DELETE"])
def deletar_favorito():
    id = request.args.get("id")
    tipo = request.args.get("tipo")
    user_id = request.args.get("user_id")
    dao_user.deletar_favorito(id, user_id, tipo)
    return "OK", 200


@rest_user.route("/s/user", methods=["POST"])
def create_user():
    try:
        user = request.get_json()
        user = UserModel(**user[0])
        dao_user.create_user(user)
        return jsonify("OK"), HTTPStatus.CREATED
    except psycopg2.errors.UniqueViolation:
        return jsonify({"message": "discente já cadastrado"}), HTTPStatus.CONFLICT


@rest_user.route("/s/user", methods=["GET"])
def select_user():
    uid = request.args.get("uid")
    user = dao_user.select_user(uid)
    return jsonify(user), HTTPStatus.OK


@rest_user.route("/s/user/all", methods=["GET"])
def list_users():
    user = dao_user.list_users()
    return jsonify(user), HTTPStatus.OK


@rest_user.route("/s/user/entrys", methods=["GET"])
def list_all_users():
    user = dao_user.list_all_users()
    return jsonify(user), HTTPStatus.OK


@rest_user.route("/s/user", methods=["PUT"])
def update_user():
    user = request.get_json()
    dao_user.update_user(user[0])
    return jsonify(), HTTPStatus.OK


@rest_user.route("/s/role", methods=["POST"])
def create_new_role():
    role = request.get_json()
    dao_user.create_new_role(role)
    return jsonify("OK"), HTTPStatus.CREATED


@rest_user.route("/s/role", methods=["GET"])
def view_roles():
    roles = dao_user.view_roles()
    return jsonify(roles), HTTPStatus.OK


@rest_user.route("/s/role", methods=["PUT"])
def update_role():
    role = request.get_json()
    dao_user.update_role(role)
    return jsonify("OK"), HTTPStatus.CREATED


@rest_user.route("/s/role", methods=["DELETE"])
def delete_role():
    role = request.get_json()
    dao_user.delete_role(role)
    return jsonify(), HTTPStatus.OK


@rest_user.route("/s/permission", methods=["POST"])
def create_new_permission():
    permission = request.get_json()
    dao_user.create_new_permission(permission)
    return jsonify("OK"), HTTPStatus.CREATED


@rest_user.route("/s/permission", methods=["GET"])
def permissions_view():
    role_id = request.args.get("role_id")
    roles = dao_user.permissions_view(role_id)
    return jsonify(roles), HTTPStatus.OK


@rest_user.route("/s/permission", methods=["PUT"])
def update_permission():
    permission = request.get_json()
    dao_user.update_permission(permission)
    return jsonify("OK"), HTTPStatus.CREATED


@rest_user.route("/s/permission", methods=["DELETE"])
def delete_permission():
    permission = request.get_json()
    dao_user.delete_permission(permission)
    return jsonify("OK"), HTTPStatus.NO_CONTENT


@rest_user.route("/s/user/role", methods=["POST"])
def assign_user():
    try:
        user = request.get_json()
        dao_user.assign_user(user)
        return jsonify("OK"), HTTPStatus.CREATED
    except psycopg2.errors.UniqueViolation:
        return jsonify({"message": "discente já cadastrado"}), HTTPStatus.CONFLICT


@rest_user.route("/s/user/permissions", methods=["GET"])
def view_user_roles():
    uid = request.args.get("uid")
    role_id = request.args.get("role_id")
    permissions = dao_user.view_user_roles(uid, role_id)
    return jsonify(permissions), HTTPStatus.OK


@rest_user.route("/s/user/role", methods=["DELETE"])
def unassign_user():
    technician = request.get_json()
    dao_user.unassign_user(technician)
    return jsonify("OK"), HTTPStatus.NO_CONTENT

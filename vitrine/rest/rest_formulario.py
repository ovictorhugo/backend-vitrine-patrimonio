from flask import Blueprint, request, jsonify
from vitrine.dao import dao_formulario

rest_formulario = Blueprint("rest_patrimonio", __name__)


@rest_formulario.route("/formulario", methods=["POST"])
def insert():
    form = request.get_json()
    dao_formulario.insert_form(form[0])
    return jsonify({"message": "Formulario inserido com sucesso"}), 201


@rest_formulario.route("/formulario", methods=["PUT"])
def update():
    form = request.get_json()
    dao_formulario.update_patrimonio(form[0])
    return "OK"


@rest_formulario.route("/formulario", methods=["DELETE"])
def delete():
    patrimonio_id = request.args.get("patrimonio_id")
    dao_formulario.delete_patrimonio(patrimonio_id)
    return "OK"


@rest_formulario.route("/formulario", methods=["GET"])
def select():
    verificado = request.args.get("verificado")
    loc = request.args.get("loc")
    user_id = request.args.get("user_id")
    patrimonio_id = request.args.get("patrimonio_id")
    mat_num = request.args.get("mat_nom")
    estado_transferencia = request.args.get("estado_transferencia")
    desfazimento = request.args.get("desfazimento")
    return dao_formulario.buscar_patrimonio(
        verificado,
        loc,
        user_id,
        patrimonio_id,
        mat_num,
        estado_transferencia,
        desfazimento,
    )

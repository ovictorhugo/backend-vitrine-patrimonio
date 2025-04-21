from flask import Blueprint, request
from vitrine.dao import dao_transferencia

rest_transferencia = Blueprint("rest_transferencia", __name__)


@rest_transferencia.route("/transferencia", methods=["POST"])
def solicitar_transferencia():
    form = request.get_json()
    dao_transferencia.solicitar(form)
    return "OK"


@rest_transferencia.route("/transferencia", methods=["GET"])
def listar_transferencia():
    ofertante = request.args.get("ofertante")
    solicitante = request.args.get("solicitante")
    patrimonio_id = request.args.get("patrimonio_id")

    lista = dao_transferencia.listar_solicitacao(ofertante, solicitante, patrimonio_id)
    return lista


@rest_transferencia.route("/transferencia", methods=["PUT"])
def deletar_transferencia():
    solicitante = request.args.get("user_id")
    patrimonio_id = request.args.get("patrimonio_id")
    aceito = request.args.get("aceito")

    if aceito:
        dao_transferencia.aceitar_solicitacao(solicitante, patrimonio_id)
    else:
        dao_transferencia.recusar_solicitacao(solicitante, patrimonio_id)

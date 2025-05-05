from flask import Blueprint, request, jsonify
import pandas as pd
from vitrine.dao import Connection

rest_institution = Blueprint("rest_institution", __name__)


@rest_institution.route("/institution", methods=["POST"])
def insert():
    institution = request.get_json()
    uge_nom = institution.get("uge_nom")
    uge_code = institution.get("uge_code")

    if not uge_nom or not uge_code:
        return jsonify(
            {"message": "Os campos uge_nom e uge_code são obrigatórios"}
        ), 400

    SCRIPT_SQL = """
    INSERT INTO institution (uge_nom, uge_code) VALUES (%s, %s);
    """

    Connection().exec(SCRIPT_SQL, (uge_nom, uge_code))

    return jsonify({"message": "Formulário inserido com sucesso"}), 201


@rest_institution.route("/institution", methods=["GET"])
def get_all():
    SCRIPT_SQL = "SELECT id, uge_nom, uge_code FROM institution;"
    result = Connection().select(SCRIPT_SQL)
    result = pd.DataFrame(result, columns=["id", "uge_nom", "uge_code"])
    return jsonify(result.to_dict(orient="records")), 200


@rest_institution.route("/institution/<uuid:id>", methods=["PUT"])
def update(id):
    data = request.get_json()
    uge_nom = data.get("uge_nom")
    uge_code = data.get("uge_code")

    if not uge_nom or not uge_code:
        return jsonify(
            {"message": "Os campos uge_nom e uge_code são obrigatórios"}
        ), 400

    SCRIPT_SQL = """
    UPDATE institution
    SET uge_nom = %s, uge_code = %s
    WHERE id = %s;
    """

    Connection().exec(SCRIPT_SQL, (uge_nom, uge_code, str(id)))
    return jsonify({"message": "Instituição atualizada com sucesso"}), 200


@rest_institution.route("/institution/<uuid:id>", methods=["DELETE"])
def delete(id):
    SCRIPT_SQL = "DELETE FROM institution WHERE id = %s;"
    Connection().exec(SCRIPT_SQL, (str(id),))
    return jsonify({"message": "Instituição removida com sucesso"}), 200

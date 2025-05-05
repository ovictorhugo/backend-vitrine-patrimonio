from flask import Flask
from flask_cors import CORS
from vitrine.rest.rest_user import rest_user
from vitrine.rest.rest_vitrine import rest_vitrine
from vitrine.rest.rest_formulario import rest_formulario
from vitrine.rest.rest_transferencia import rest_transferencia
from vitrine.rest.rest_institution import rest_institution


def create_app():
    app = Flask(__name__)
    CORS(app)
    app.register_blueprint(rest_user)
    app.register_blueprint(rest_formulario)
    app.register_blueprint(rest_vitrine)
    app.register_blueprint(rest_transferencia)
    app.register_blueprint(rest_institution)

    return app

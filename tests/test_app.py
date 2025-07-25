from http import HTTPStatus

from fastapi.testclient import TestClient

from vitrine.app import app


def test_raiz():
    client = TestClient(app)

    response = client.get('/')

    assert response.status_code == HTTPStatus.OK
    assert response.json() == {'mensagem': 'API em funcionamento!'}

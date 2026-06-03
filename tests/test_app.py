from http import HTTPStatus


def test_raiz(client):
    response = client.get('/')

    assert response.status_code == HTTPStatus.OK
    assert response.json() == {'mensagem': 'API em funcionamento!'}

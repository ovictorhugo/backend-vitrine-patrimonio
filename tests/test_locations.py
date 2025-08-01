# Em tests/test_locations.py

import uuid
from http import HTTPStatus

import pytest

from vitrine.schemas import LocationPublic


@pytest.mark.asyncio
async def test_create_location(client, create_user, create_token):
    """Testa a criação de uma nova localização com sucesso."""
    user = await create_user()
    token = create_token(user)

    response = client.post(
        '/locations',
        json={'location_name': 'Prédio A, Sala 101', 'location_code': 'A101'},
        headers={'Authorization': f'Bearer {token}'},
    )

    assert response.status_code == HTTPStatus.CREATED
    data = response.json()
    assert data['location_name'] == 'Prédio A, Sala 101'
    assert data['location_code'] == 'A101'
    assert 'id' in data and uuid.UUID(data['id'])


@pytest.mark.asyncio
async def test_read_locations_empty(client):
    """Testa a listagem de localizações quando não há nenhuma cadastrada."""
    response = client.get('/locations')

    assert response.status_code == HTTPStatus.OK
    assert response.json() == {'locations': []}


@pytest.mark.asyncio
async def test_read_locations_with_location(client, create_location):
    location = await create_location()
    location_schema = LocationPublic.model_validate(location).model_dump(
        mode='json'
    )

    response = client.get('/locations')

    assert response.status_code == HTTPStatus.OK
    assert response.json() == {'locations': [location_schema]}


@pytest.mark.asyncio
async def test_delete_location(
    client, create_location, create_user, create_token
):
    """Testa a desativação (soft delete) de uma localização."""
    user = await create_user()
    token = create_token(user)
    location = await create_location()

    response = client.delete(
        f'/locations/{location.id}',
        headers={'Authorization': f'Bearer {token}'},
    )

    assert response.status_code == HTTPStatus.OK
    assert response.json() == {'message': 'Location deactivated successfully'}

    # Verifica se a localização foi realmente "deletada" (soft delete)
    response_get = client.get('/locations')
    assert response_get.json() == {'locations': []}

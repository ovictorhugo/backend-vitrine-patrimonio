import uuid
from http import HTTPStatus

import pytest

from vitrine.schemas import AgencyPublic


@pytest.mark.asyncio
async def test_create_agency(client, create_user, create_token):
    user = await create_user()
    response = client.post(
        '/agencies',
        json={'agency_name': 'Agência', 'agency_code': 'Código'},
        headers={'Authorization': f'Bearer {create_token(user)}'},
    )
    assert response.status_code == HTTPStatus.CREATED
    data = response.json()
    assert data['agency_name'] == 'Agência'
    assert data['agency_code'] == 'Código'
    assert uuid.UUID(data['id'])


@pytest.mark.asyncio
async def test_read_agencies(client):
    response = client.get('/agencies')
    assert response.status_code == HTTPStatus.OK
    assert response.json() == {'agencies': []}


@pytest.mark.asyncio
async def test_read_agencies_with_agency(client, create_agency):
    agency = await create_agency()
    agency_schema = AgencyPublic.model_validate(agency).model_dump(mode='json')

    response = client.get('/agencies')
    assert response.status_code == HTTPStatus.OK
    assert response.json() == {'agencies': [agency_schema]}


@pytest.mark.asyncio
async def test_delete_agency(client, create_agency, create_user, create_token):
    user = await create_user()
    agency = await create_agency()
    response = client.delete(
        f'/agencies/{agency.id}',
        headers={'Authorization': f'Bearer {create_token(user)}'},
    )
    assert response.status_code == HTTPStatus.OK
    assert response.json() == {'message': 'Agency deactivated'}

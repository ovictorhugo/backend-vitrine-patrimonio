# Em tests/test_sectors.py

import uuid
from http import HTTPStatus

import pytest

from vitrine.schemas import SectorPublic


@pytest.mark.asyncio
async def test_create_sector(client, create_user, create_token):
    user = await create_user()
    token = create_token(user)

    response = client.post(
        '/sectors',
        json={'sector_name': 'Setor Financeiro', 'sector_code': 'FIN'},
        headers={'Authorization': f'Bearer {token}'},
    )

    assert response.status_code == HTTPStatus.CREATED
    data = response.json()
    assert data['sector_name'] == 'Setor Financeiro'
    assert data['sector_code'] == 'FIN'
    assert uuid.UUID(data['id'])


@pytest.mark.asyncio
async def test_read_sectors_empty(client):
    response = client.get('/sectors')

    assert response.status_code == HTTPStatus.OK
    assert response.json() == {'sectors': []}


@pytest.mark.asyncio
async def test_read_sectors_with_sector(client, create_sector):
    sector = await create_sector()
    sector_schema = SectorPublic.model_validate(sector).model_dump(mode='json')

    response = client.get('/sectors')

    assert response.status_code == HTTPStatus.OK
    assert response.json() == {'sectors': [sector_schema]}


@pytest.mark.asyncio
async def test_delete_sector(client, create_sector, create_user, create_token):
    user = await create_user()
    token = create_token(user)
    sector = await create_sector()

    response = client.delete(
        f'/sectors/{sector.id}',
        headers={'Authorization': f'Bearer {token}'},
    )

    assert response.status_code == HTTPStatus.OK
    assert response.json() == {'message': 'Sector deactivated successfully'}

    response_get = client.get('/sectors')
    assert response_get.json() == {'sectors': []}


@pytest.mark.asyncio
async def test_read_sectors_with_text_search(client, create_sector):
    await create_sector(sector_name='Setor de TI', sector_code='TI01')
    await create_sector(sector_name='Setor de RH', sector_code='RH02')
    await create_sector(sector_name='Setor Contábil', sector_code='CONT03')

    response = client.get('/sectors')
    assert len(response.json()['sectors']) == 3

    response = client.get('/sectors?q=Contábil')
    assert response.status_code == HTTPStatus.OK
    sectors = response.json()['sectors']
    assert len(sectors) == 1
    assert sectors[0]['sector_name'] == 'Setor Contábil'

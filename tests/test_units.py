# Em tests/test_units.py

import uuid
from http import HTTPStatus

import pytest

from vitrine.schemas import UnitPublic


@pytest.mark.asyncio
async def test_create_unit(client, create_user, create_token):
    user = await create_user()
    token = create_token(user)

    response = client.post(
        '/units',
        json={
            'unit_name': 'Unidade Teste',
            'unit_code': 'UT01',
            'unit_siaf': '74123',
        },
        headers={'Authorization': f'Bearer {token}'},
    )

    assert response.status_code == HTTPStatus.CREATED
    data = response.json()
    assert data['unit_name'] == 'Unidade Teste'
    assert data['unit_code'] == 'UT01'
    assert data['unit_siaf'] == '74123'
    assert uuid.UUID(data['id'])


@pytest.mark.asyncio
async def test_read_units_empty(client):
    response = client.get('/units')

    assert response.status_code == HTTPStatus.OK
    assert response.json() == {'units': []}


@pytest.mark.asyncio
async def test_read_units_with_unit(client, create_unit):
    unit = await create_unit()
    unit_schema = UnitPublic.model_validate(unit).model_dump(mode='json')

    response = client.get('/units')

    assert response.status_code == HTTPStatus.OK
    assert response.json() == {'units': [unit_schema]}


@pytest.mark.asyncio
async def test_delete_unit(client, create_unit, create_user, create_token):
    user = await create_user()
    token = create_token(user)
    unit = await create_unit()

    response = client.delete(
        f'/units/{unit.id}',
        headers={'Authorization': f'Bearer {token}'},
    )

    assert response.status_code == HTTPStatus.OK
    assert response.json() == {'message': 'Unit deactivated successfully'}

    response_get = client.get('/units')
    assert response_get.json() == {'units': []}


@pytest.mark.asyncio
async def test_read_units_with_text_search(client, create_unit):
    await create_unit(
        unit_name='Unidade Almoxarifado', unit_code='ALMOX', unit_siaf='123'
    )
    await create_unit(
        unit_name='Unidade Compras', unit_code='COMP', unit_siaf='456'
    )
    await create_unit(
        unit_name='Unidade Financeiro', unit_code='FIN', unit_siaf='789'
    )

    response = client.get('/units')
    assert len(response.json()['units']) == 3

    response = client.get('/units?q=Almoxarifado')
    assert response.status_code == HTTPStatus.OK
    units = response.json()['units']
    assert len(units) == 1
    assert units[0]['unit_name'] == 'Unidade Almoxarifado'

    response = client.get('/units?q=456')
    assert len(response.json()['units']) == 1
    assert response.json()['units'][0]['unit_code'] == 'COMP'

import uuid
from datetime import datetime
from http import HTTPStatus

import pytest

from vitrine.schemas import UnitPublic


@pytest.mark.asyncio
async def test_create_unit_success(client, create_user, create_token):
    user = await create_user()
    token = create_token(user)

    response = client.post(
        '/units',
        json={
            'unit_name': 'Departamento de Execução de Leis Mágicas',
            'unit_code': 'DELM',
            'unit_siaf': '12345',
        },
        headers={'Authorization': f'Bearer {token}'},
    )

    assert response.status_code == HTTPStatus.CREATED
    data = response.json()
    assert data['unit_name'] == 'Departamento de Execução de Leis Mágicas'
    assert data['unit_code'] == 'DELM'
    assert 'id' in data


@pytest.mark.asyncio
async def test_create_unit_conflict(
    client, create_unit, create_user, create_token
):
    user = await create_user()
    token = create_token(user)
    await create_unit(user_id=user.id, unit_name='Nome Repetido')

    response = client.post(
        '/units',
        json={
            'unit_name': 'Nome Repetido',
            'unit_code': 'NR02',
            'unit_siaf': '54321',
        },
        headers={'Authorization': f'Bearer {token}'},
    )

    assert response.status_code == HTTPStatus.CONFLICT
    assert 'Uma unidade com este nome já existe.' in response.json()['detail']


@pytest.mark.asyncio
async def test_read_units_empty(client):
    response = client.get('/units')
    assert response.status_code == HTTPStatus.OK
    assert response.json() == {'units': []}


@pytest.mark.asyncio
async def test_read_units_with_data(client, create_unit):
    unit = await create_unit()
    unit_schema = UnitPublic.model_validate(unit).model_dump(mode='json')

    response = client.get('/units')
    assert response.status_code == HTTPStatus.OK
    assert response.json()['units'] == [unit_schema]


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

    response = client.get('/units?q=Almoxarifado')
    assert response.status_code == HTTPStatus.OK
    units = response.json()['units']
    assert len(units) == 1
    assert units[0]['unit_name'] == 'Unidade Almoxarifado'

    response = client.get('/units?q=456')
    assert len(response.json()['units']) == 1
    assert response.json()['units'][0]['unit_code'] == 'COMP'


@pytest.mark.asyncio
async def test_delete_unit_success(
    client, create_unit, create_user, create_token, session
):
    user = await create_user()
    token = create_token(user)
    unit = await create_unit()

    response = client.delete(
        f'/units/{unit.id}',
        headers={'Authorization': f'Bearer {token}'},
    )

    assert response.status_code == HTTPStatus.OK
    assert response.json() == {'message': 'Unidade desativada com sucesso.'}

    response_get = client.get('/units')
    assert response_get.json() == {'units': []}

    await session.refresh(unit)
    assert unit.deleted_at is not None


@pytest.mark.asyncio
async def test_delete_unit_not_found(client, create_user, create_token):
    user = await create_user()
    token = create_token(user)
    non_existent_id = uuid.uuid4()

    response = client.delete(
        f'/units/{non_existent_id}',
        headers={'Authorization': f'Bearer {token}'},
    )

    assert response.status_code == HTTPStatus.NOT_FOUND
    assert response.json()['detail'] == 'Unidade não encontrada.'


@pytest.mark.asyncio
async def test_delete_unit_already_deleted(
    client, create_unit, create_user, create_token, session
):
    user = await create_user()
    token = create_token(user)
    unit = await create_unit()

    unit.deleted_at = datetime.now()
    session.add(unit)
    await session.commit()

    response = client.delete(
        f'/units/{unit.id}',
        headers={'Authorization': f'Bearer {token}'},
    )

    assert response.status_code == HTTPStatus.BAD_REQUEST
    assert response.json()['detail'] == 'Esta unidade já está desativada.'

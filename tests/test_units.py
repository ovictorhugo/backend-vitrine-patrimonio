import uuid
from datetime import datetime
from http import HTTPStatus

import pytest

from vitrine.schemas import UnitPublic


@pytest.mark.asyncio
async def test_create_unit_success(
    client, create_agency, create_user, create_token
):
    user = await create_user()
    token = create_token(user)
    agency = await create_agency(user_id=user.id)

    response = client.post(
        '/units',
        json={
            'unit_name': 'Departamento de Execução de Leis Mágicas',
            'unit_code': 'DELM',
            'unit_siaf': '12345',
            'agency_id': str(agency.id),
        },
        headers={'Authorization': f'Bearer {token}'},
    )

    assert response.status_code == HTTPStatus.CREATED
    data = response.json()
    assert data['unit_name'] == 'Departamento de Execução de Leis Mágicas'
    assert data['unit_code'] == 'DELM'
    assert data['agency_id'] == str(agency.id)
    assert 'id' in data


@pytest.mark.asyncio
async def test_create_unit_for_non_existent_agency(
    client, create_user, create_token
):
    user = await create_user()
    token = create_token(user)
    non_existent_agency_id = uuid.uuid4()

    response = client.post(
        '/units',
        json={
            'unit_name': 'Unidade Fantasma',
            'unit_code': 'UF',
            'unit_siaf': '00000',
            'agency_id': str(non_existent_agency_id),
        },
        headers={'Authorization': f'Bearer {token}'},
    )

    assert response.status_code == HTTPStatus.NOT_FOUND
    assert (
        f'O órgão com ID "{non_existent_agency_id}" não foi encontrado ou está inativo.'
        in response.json()['detail']
    )


@pytest.mark.asyncio
async def test_create_unit_for_inactive_agency(
    client, create_agency, create_user, create_token, session
):
    user = await create_user()
    token = create_token(user)
    agency = await create_agency(user_id=user.id)

    agency.deleted_at = datetime.now()
    session.add(agency)
    await session.commit()

    response = client.post(
        '/units',
        json={
            'unit_name': 'Unidade para Órgão Inativo',
            'unit_code': 'UOI',
            'unit_siaf': '99999',
            'agency_id': str(agency.id),
        },
        headers={'Authorization': f'Bearer {token}'},
    )

    assert response.status_code == HTTPStatus.NOT_FOUND


@pytest.mark.asyncio
async def test_create_unit_conflict(
    client, create_unit, create_user, create_token
):
    user = await create_user()
    token = create_token(user)
    unit = await create_unit(user_id=user.id, unit_name='Nome Repetido')

    response = client.post(
        '/units',
        json={
            'unit_name': 'Nome Repetido',
            'unit_code': 'NR02',
            'unit_siaf': '54321',
            'agency_id': str(unit.agency.id),
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
async def test_filter_unity_by_agency(client, create_unit, create_agency):
    EXPECTED_COUNT = 1
    agency1 = await create_agency()
    await create_unit(agency_id=agency1.id)
    agency2 = await create_agency()
    await create_unit(agency_id=agency2.id)

    response = client.get(f'/units?agency_id={agency2.id}')
    assert response.status_code == HTTPStatus.OK
    units = response.json()['units']
    assert len(units) == EXPECTED_COUNT

    EXPECTED_COUNT = 2
    response = client.get('/units')
    assert response.status_code == HTTPStatus.OK
    units = response.json()['units']
    assert len(units) == EXPECTED_COUNT


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
async def test_delete_unit_with_active_sectors_conflict(
    client, create_sector, create_user, create_token
):
    user = await create_user()
    token = create_token(user)
    sector = await create_sector(user_id=user.id)
    unit_id_with_sector = sector.unit.id

    response = client.delete(
        f'/units/{unit_id_with_sector}',
        headers={'Authorization': f'Bearer {token}'},
    )

    assert response.status_code == HTTPStatus.CONFLICT
    assert (
        'Não é possível desativar a unidade pois ela possui 1 setor(es) ativo(s).'
        in response.json()['detail']
    )


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

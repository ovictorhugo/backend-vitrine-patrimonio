import uuid
from datetime import datetime
from http import HTTPStatus

import pytest

from vitrine.schemas import SectorPublic


@pytest.mark.asyncio
async def test_create_sector_success(
    client, create_agency, create_user, create_token
):
    user = await create_user()
    token = create_token(user)
    agency = await create_agency(user_id=user.id)

    response = client.post(
        '/sectors',
        json={
            'sector_name': 'Seção de Feitiços',
            'sector_code': 'SF-01',
            'agency_id': str(agency.id),
        },
        headers={'Authorization': f'Bearer {token}'},
    )

    assert response.status_code == HTTPStatus.CREATED
    data = response.json()
    assert data['sector_name'] == 'Seção de Feitiços'
    assert data['sector_code'] == 'SF-01'
    assert data['agency_id'] == str(agency.id)
    assert 'id' in data


@pytest.mark.asyncio
async def test_create_sector_for_non_existent_unit(
    client, create_user, create_token
):
    user = await create_user()
    token = create_token(user)
    non_existent_agency_id = uuid.uuid4()

    response = client.post(
        '/sectors',
        json={
            'sector_name': 'Setor Fantasma',
            'sector_code': 'SF-GHOST',
            'agency_id': str(non_existent_agency_id),
        },
        headers={'Authorization': f'Bearer {token}'},
    )

    assert response.status_code == HTTPStatus.NOT_FOUND
    assert (
        f'A organização com ID "{non_existent_agency_id}" não foi encontrada ou está inativa.'
        in response.json()['detail']
    )


@pytest.mark.asyncio
async def test_create_sector_for_inactive_unit(
    client, create_agency, create_user, create_token, session
):
    user = await create_user()
    token = create_token(user)
    agency = await create_agency(user_id=user.id)

    agency.deleted_at = datetime.now()
    session.add(agency)
    await session.commit()

    response = client.post(
        '/sectors',
        json={
            'sector_name': 'Setor para Unidade Inativa',
            'sector_code': 'SUI-01',
            'agency_id': str(agency.id),
        },
        headers={'Authorization': f'Bearer {token}'},
    )

    assert response.status_code == HTTPStatus.NOT_FOUND


@pytest.mark.asyncio
async def test_create_sector_conflict(
    client, create_sector, create_user, create_token
):
    user = await create_user()
    token = create_token(user)
    sector = await create_sector(
        user_id=user.id, sector_name='Nome de Setor Repetido'
    )

    response = client.post(
        '/sectors',
        json={
            'sector_name': 'Nome de Setor Repetido',
            'sector_code': 'NSR-02',
            'agency_id': str(sector.agency.id),
        },
        headers={'Authorization': f'Bearer {token}'},
    )

    assert response.status_code == HTTPStatus.CONFLICT
    assert 'Um setor com este nome já existe.' in response.json()['detail']


@pytest.mark.asyncio
async def test_read_sectors_empty(client):
    response = client.get('/sectors')
    assert response.status_code == HTTPStatus.OK
    assert response.json() == {'sectors': []}


@pytest.mark.asyncio
async def test_read_sectors_with_data(client, create_sector):
    sector = await create_sector()
    sector_schema = SectorPublic.model_validate(sector).model_dump(mode='json')

    response = client.get('/sectors')
    assert response.status_code == HTTPStatus.OK
    assert response.json()['sectors'] == [sector_schema]


@pytest.mark.asyncio
async def test_read_sectors_with_text_search(client, create_sector):
    await create_sector(
        sector_name='Setor de Contabilidade', sector_code='CONT'
    )
    await create_sector(
        sector_name='Setor de Recursos Humanos', sector_code='RH'
    )

    response = client.get('/sectors?q=Humanos')
    assert response.status_code == HTTPStatus.OK
    sectors = response.json()['sectors']
    assert len(sectors) == 1
    assert sectors[0]['sector_name'] == 'Setor de Recursos Humanos'

    response = client.get('/sectors?q=CONT')
    assert len(response.json()['sectors']) == 1


@pytest.mark.asyncio
async def test_filter_sectors_by_unit(client, create_agency, create_sector):
    EXPECTED_COUNT = 1
    agency1 = await create_agency()
    await create_sector(agency_id=agency1.id)
    agency2 = await create_agency()
    await create_sector(agency_id=agency2.id)

    response = client.get(f'/sectors?agency_id={agency2.id}')
    assert response.status_code == HTTPStatus.OK
    sectors = response.json()['sectors']
    assert len(sectors) == EXPECTED_COUNT

    EXPECTED_COUNT = 2
    response = client.get('/sectors')
    assert response.status_code == HTTPStatus.OK
    sectors = response.json()['sectors']
    assert len(sectors) == EXPECTED_COUNT


@pytest.mark.asyncio
async def test_delete_sector_success(
    client, create_sector, create_user, create_token, session
):
    user = await create_user()
    token = create_token(user)
    sector = await create_sector()

    response = client.delete(
        f'/sectors/{sector.id}',
        headers={'Authorization': f'Bearer {token}'},
    )

    assert response.status_code == HTTPStatus.OK
    assert response.json() == {'message': 'Setor desativado com sucesso.'}

    await session.refresh(sector)
    assert sector.deleted_at is not None


@pytest.mark.asyncio
async def test_delete_sector_with_active_locations_conflict(
    client, create_location, create_user, create_token
):
    user = await create_user()
    token = create_token(user)
    location = await create_location(user_id=user.id)
    sector_id_with_location = location.sector.id

    response = client.delete(
        f'/sectors/{sector_id_with_location}',
        headers={'Authorization': f'Bearer {token}'},
    )

    assert response.status_code == HTTPStatus.CONFLICT
    assert (
        'Não é possível desativar o setor pois ele possui 1 localização(ões) ativa(s).'
        in response.json()['detail']
    )


@pytest.mark.asyncio
async def test_delete_sector_not_found(client, create_user, create_token):
    user = await create_user()
    token = create_token(user)
    non_existent_id = uuid.uuid4()

    response = client.delete(
        f'/sectors/{non_existent_id}',
        headers={'Authorization': f'Bearer {token}'},
    )

    assert response.status_code == HTTPStatus.NOT_FOUND
    assert response.json()['detail'] == 'Setor não encontrado.'


@pytest.mark.asyncio
async def test_delete_sector_already_deleted(
    client, create_sector, create_user, create_token, session
):
    user = await create_user()
    token = create_token(user)
    sector = await create_sector()

    sector.deleted_at = datetime.now()
    session.add(sector)
    await session.commit()

    response = client.delete(
        f'/sectors/{sector.id}',
        headers={'Authorization': f'Bearer {token}'},
    )

    assert response.status_code == HTTPStatus.BAD_REQUEST
    assert response.json()['detail'] == 'Este setor já está desativado.'

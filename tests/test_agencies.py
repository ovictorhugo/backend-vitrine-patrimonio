import uuid
from datetime import datetime
from http import HTTPStatus

import pytest

from vitrine.schemas import AgencyPublic


@pytest.mark.asyncio
async def test_create_agency_success(client, create_user, create_token):
    user = await create_user()
    token = create_token(user)

    response = client.post(
        '/agencies',
        json={'agency_name': 'Ministério da Magia', 'agency_code': 'MM-001'},
        headers={'Authorization': f'Bearer {token}'},
    )

    assert response.status_code == HTTPStatus.CREATED
    data = response.json()
    assert data['agency_name'] == 'Ministério da Magia'
    assert data['agency_code'] == 'MM-001'
    assert 'id' in data
    assert uuid.UUID(data['id'])


@pytest.mark.asyncio
async def test_create_agency_conflict(
    client, create_agency, create_user, create_token
):
    user = await create_user()
    token = create_token(user)
    await create_agency(agency_name='Ministério da Magia')

    response = client.post(
        '/agencies',
        json={'agency_name': 'Ministério da Magia', 'agency_code': 'MM-002'},
        headers={'Authorization': f'Bearer {token}'},
    )

    assert response.status_code == HTTPStatus.CONFLICT
    assert response.json()['detail'] == 'Um órgão com este nome já existe.'


@pytest.mark.asyncio
async def test_read_agencies_empty(client):
    response = client.get('/agencies')
    assert response.status_code == HTTPStatus.OK
    assert response.json() == {'agencies': []}


@pytest.mark.asyncio
async def test_read_agencies_with_data(client, create_agency):
    agency = await create_agency()
    agency_schema = AgencyPublic.model_validate(agency).model_dump(mode='json')

    response = client.get('/agencies')
    assert response.status_code == HTTPStatus.OK
    assert response.json()['agencies'] == [agency_schema]


@pytest.mark.asyncio
async def test_read_agencies_with_text_search(client, create_agency):
    await create_agency(
        agency_name='Agência Central do Brasil', agency_code='ACB'
    )
    await create_agency(agency_name='Agência Norte', agency_code='AN')
    await create_agency(agency_name='Agência Sul', agency_code='AS')

    response = client.get('/agencies?q=Central')
    assert response.status_code == HTTPStatus.OK
    agencies = response.json()['agencies']
    assert len(agencies) == 1
    assert agencies[0]['agency_name'] == 'Agência Central do Brasil'

    response = client.get('/agencies?q=ACB')
    assert len(response.json()['agencies']) == 1


@pytest.mark.asyncio
async def test_delete_agency_success(
    client, create_agency, create_user, create_token, session
):
    user = await create_user()
    token = create_token(user)
    agency = await create_agency()

    response = client.delete(
        f'/agencies/{agency.id}',
        headers={'Authorization': f'Bearer {token}'},
    )

    assert response.status_code == HTTPStatus.OK
    assert response.json() == {'message': 'Órgão desativado com sucesso.'}

    response_get = client.get('/agencies')
    assert response_get.json()['agencies'] == []

    await session.refresh(agency)
    assert agency.deleted_at is not None


@pytest.mark.asyncio
async def test_delete_agency_with_active_units_conflict(
    client, create_unit, create_user, create_token
):
    user = await create_user()
    token = create_token(user)
    unit = await create_unit(user_id=user.id)
    agency_id = unit.agency.id

    response = client.delete(
        f'/agencies/{agency_id}',
        headers={'Authorization': f'Bearer {token}'},
    )

    assert response.status_code == HTTPStatus.CONFLICT
    assert (
        response.json()['detail']
        == 'Não é possível desativar o órgão pois ele possui 1 unidade(s) ativa(s).'
    )


@pytest.mark.asyncio
async def test_delete_agency_not_found(client, create_user, create_token):
    user = await create_user()
    token = create_token(user)
    non_existent_id = uuid.uuid4()

    response = client.delete(
        f'/agencies/{non_existent_id}',
        headers={'Authorization': f'Bearer {token}'},
    )

    assert response.status_code == HTTPStatus.NOT_FOUND
    assert response.json()['detail'] == 'Órgão não encontrado.'


@pytest.mark.asyncio
async def test_delete_agency_already_deleted(
    client, create_agency, create_user, create_token, session
):
    user = await create_user()
    token = create_token(user)
    agency = await create_agency()

    agency.deleted_at = datetime.now()
    session.add(agency)
    await session.commit()

    response = client.delete(
        f'/agencies/{agency.id}',
        headers={'Authorization': f'Bearer {token}'},
    )

    assert response.status_code == HTTPStatus.BAD_REQUEST
    assert response.json()['detail'] == 'Este órgão já está desativado.'

import uuid
from datetime import datetime
from http import HTTPStatus

import pytest

from vitrine.schemas import LocationPublic


@pytest.mark.asyncio
async def test_create_location_success(
    client, create_sector, create_user, create_token, create_legal_guardian
):
    user = await create_user()
    token = create_token(user)
    sector = await create_sector(user_id=user.id)
    legal_guardian = await create_legal_guardian()

    response = client.post(
        '/locations',
        json={
            'location_name': 'Câmara Secreta',
            'location_code': 'CS-01',
            'sector_id': str(sector.id),
            'legal_guardian_id': str(
                legal_guardian.id,
            ),
        },
        headers={'Authorization': f'Bearer {token}'},
    )

    assert response.status_code == HTTPStatus.CREATED
    data = response.json()
    assert data['location_name'] == 'Câmara Secreta'
    assert data['location_code'] == 'CS-01'
    assert data['sector_id'] == str(sector.id)
    assert 'id' in data


@pytest.mark.asyncio
async def test_create_location_for_non_existent_sector(
    client, create_user, create_token, create_legal_guardian
):
    user = await create_user()
    legal_guardian = await create_legal_guardian()
    token = create_token(user)
    non_existent_sector_id = uuid.uuid4()

    response = client.post(
        '/locations',
        json={
            'location_name': 'Localização Perdida',
            'location_code': 'LP-404',
            'sector_id': str(non_existent_sector_id),
            'legal_guardian_id': str(legal_guardian.id),
        },
        headers={'Authorization': f'Bearer {token}'},
    )

    assert response.status_code == HTTPStatus.NOT_FOUND
    assert (
        f'O setor com ID "{non_existent_sector_id}" não foi encontrado ou está inativo.'
        in response.json()['detail']
    )


@pytest.mark.asyncio
async def test_create_location_for_inactive_sector(
    client,
    create_sector,
    create_user,
    create_token,
    session,
    create_legal_guardian,
):
    user = await create_user()
    legal_guardian = await create_legal_guardian()
    token = create_token(user)
    sector = await create_sector(user_id=user.id)

    sector.deleted_at = datetime.now()
    session.add(sector)
    await session.commit()

    response = client.post(
        '/locations',
        json={
            'location_name': 'Localização para Setor Inativo',
            'location_code': 'LSI-01',
            'sector_id': str(sector.id),
            'legal_guardian_id': str(legal_guardian.id),
        },
        headers={'Authorization': f'Bearer {token}'},
    )

    assert response.status_code == HTTPStatus.NOT_FOUND


@pytest.mark.asyncio
async def test_create_location_conflict(
    client, create_location, create_user, create_token
):
    user = await create_user()
    token = create_token(user)
    location = await create_location(
        user_id=user.id, location_name='Sala Precisa'
    )

    response = client.post(
        '/locations',
        json={
            'location_name': 'Sala Precisa',
            'location_code': 'SP-02',
            'sector_id': str(location.sector.id),
            'legal_guardian_id': str(location.legal_guardian.id),
        },
        headers={'Authorization': f'Bearer {token}'},
    )

    assert response.status_code == HTTPStatus.CONFLICT
    assert (
        'Uma localização com este nome já existe para este setor e responsável.'
        in response.json()['detail']
    )


@pytest.mark.asyncio
async def test_read_locations_empty(client):
    response = client.get('/locations')
    assert response.status_code == HTTPStatus.OK
    assert response.json() == {'locations': []}


@pytest.mark.asyncio
async def test_read_locations_with_data(client, create_location):
    location = await create_location()
    location_schema = LocationPublic.model_validate(location).model_dump(
        mode='json'
    )

    response = client.get('/locations')
    assert response.status_code == HTTPStatus.OK
    assert response.json()['locations'] == [location_schema]


@pytest.mark.asyncio
async def test_read_locations_with_text_search(client, create_location):
    await create_location(
        location_name='Prateleira A-10', location_code='PA10'
    )
    await create_location(location_name='Galpão B-20', location_code='GB20')

    response = client.get('/locations?q=Galpão')
    assert response.status_code == HTTPStatus.OK
    locations = response.json()['locations']
    assert len(locations) == 1
    assert locations[0]['location_name'] == 'Galpão B-20'


@pytest.mark.asyncio
async def test_filter_location_by_sector(
    client, create_location, create_sector
):
    EXPECTED_COUNT = 1
    sector1 = await create_sector()
    await create_location(sector_id=sector1.id)
    sector2 = await create_sector()
    await create_location(sector_id=sector2.id)

    response = client.get(f'/locations?sector_id={sector2.id}')
    assert response.status_code == HTTPStatus.OK
    sectors = response.json()['locations']
    assert len(sectors) == EXPECTED_COUNT

    EXPECTED_COUNT = 2
    response = client.get('/locations')
    assert response.status_code == HTTPStatus.OK
    sectors = response.json()['locations']
    assert len(sectors) == EXPECTED_COUNT


@pytest.mark.asyncio
async def test_delete_location_success(
    client, create_location, create_user, create_token, session
):
    user = await create_user()
    token = create_token(user)
    location = await create_location()

    response = client.delete(
        f'/locations/{location.id}',
        headers={'Authorization': f'Bearer {token}'},
    )

    assert response.status_code == HTTPStatus.OK
    assert response.json() == {
        'message': 'Localização desativada com sucesso.'
    }

    response_get = client.get('/locations')
    assert response_get.json()['locations'] == []

    await session.refresh(location)
    assert location.deleted_at is not None


@pytest.mark.asyncio
async def test_delete_location_not_found(client, create_user, create_token):
    user = await create_user()
    token = create_token(user)
    non_existent_id = uuid.uuid4()

    response = client.delete(
        f'/locations/{non_existent_id}',
        headers={'Authorization': f'Bearer {token}'},
    )

    assert response.status_code == HTTPStatus.NOT_FOUND
    assert response.json()['detail'] == 'Localização não encontrada.'


@pytest.mark.asyncio
async def test_delete_location_already_deleted(
    client, create_location, create_user, create_token, session
):
    user = await create_user()
    token = create_token(user)
    location = await create_location()

    location.deleted_at = datetime.now()
    session.add(location)
    await session.commit()

    response = client.delete(
        f'/locations/{location.id}',
        headers={'Authorization': f'Bearer {token}'},
    )

    assert response.status_code == HTTPStatus.BAD_REQUEST
    assert response.json()['detail'] == 'Esta localização já está desativada.'


@pytest.mark.asyncio
async def test_read_my_locations_success(
    client,
    create_user,
    create_token,
    create_legal_guardian,
    create_location,
    create_system_identity,
):
    user_responsible = await create_user(
        username='responsavel', email='resp@example.com'
    )
    token_responsible = create_token(user_responsible)
    guardian_responsible = await create_legal_guardian(
        user_id=user_responsible.id,
        legal_guardians_name='Guardião Responsável',
        legal_guardians_code='GR01',
    )
    await create_system_identity(
        user_id=user_responsible.id, legal_guardian_id=guardian_responsible.id
    )

    location1_resp = await create_location(
        legal_guardian_id=guardian_responsible.id,
        location_name='Sala do Responsável 1',
    )
    location2_resp = await create_location(
        legal_guardian_id=guardian_responsible.id,
        location_name='Sala do Responsável 2',
    )

    user_other = await create_user(username='outro', email='outro@example.com')
    guardian_other = await create_legal_guardian(
        user_id=user_other.id,
        legal_guardians_name='Outro Guardião',
        legal_guardians_code='OG02',
    )
    await create_system_identity(
        user_id=user_other.id, legal_guardian_id=guardian_other.id
    )
    await create_location(
        legal_guardian_id=guardian_other.id, location_name='Sala do Outro'
    )

    response = client.get(
        '/locations/my',
        headers={'Authorization': f'Bearer {token_responsible}'},
    )

    assert response.status_code == HTTPStatus.OK
    data = response.json()
    assert len(data['locations']) == 2

    returned_ids = {loc['id'] for loc in data['locations']}
    expected_ids = {str(location1_resp.id), str(location2_resp.id)}
    assert returned_ids == expected_ids


@pytest.mark.asyncio
async def test_read_my_locations_empty_for_user_with_no_responsibilities(
    client, create_user, create_token, create_location
):
    user_no_locations = await create_user()
    token = create_token(user_no_locations)

    await create_location()

    response = client.get(
        '/locations/my', headers={'Authorization': f'Bearer {token}'}
    )

    assert response.status_code == HTTPStatus.OK
    assert response.json() == {'locations': []}


@pytest.mark.asyncio
async def test_read_my_locations_unauthorized(client):
    response = client.get('/locations/my')
    assert response.status_code == HTTPStatus.UNAUTHORIZED


@pytest.mark.asyncio
async def test_read_my_locations_with_filters(
    client,
    create_user,
    create_token,
    create_legal_guardian,
    create_location,
    create_system_identity,
):
    user = await create_user()
    token = create_token(user)
    guardian = await create_legal_guardian(user_id=user.id)
    await create_system_identity(
        user_id=user.id, legal_guardian_id=guardian.id
    )

    await create_location(
        legal_guardian_id=guardian.id,
        location_name='Sala de Reuniões Grifinória',
    )
    await create_location(
        legal_guardian_id=guardian.id, location_name='Cozinha da Lufa-Lufa'
    )
    await create_location(location_name='Sala de Reuniões Corvinal')

    response = client.get(
        '/locations/my?q=Grifinória',
        headers={'Authorization': f'Bearer {token}'},
    )

    assert response.status_code == HTTPStatus.OK
    data = response.json()
    assert len(data['locations']) == 1
    assert (
        data['locations'][0]['location_name'] == 'Sala de Reuniões Grifinória'
    )


@pytest.mark.asyncio
async def test_read_my_locations_ignores_deleted_ones(
    client,
    session,
    create_user,
    create_token,
    create_legal_guardian,
    create_location,
    create_system_identity,
):
    user = await create_user()
    token = create_token(user)
    guardian = await create_legal_guardian(user_id=user.id)
    await create_system_identity(
        user_id=user.id, legal_guardian_id=guardian.id
    )

    location_active = await create_location(
        legal_guardian_id=guardian.id, location_name='Ativa'
    )
    location_to_delete = await create_location(
        legal_guardian_id=guardian.id, location_name='Inativa'
    )

    location_to_delete.deleted_at = datetime.now()
    session.add(location_to_delete)
    await session.commit()

    response = client.get(
        '/locations/my', headers={'Authorization': f'Bearer {token}'}
    )

    assert response.status_code == HTTPStatus.OK
    data = response.json()
    assert len(data['locations']) == 1
    assert data['locations'][0]['id'] == str(location_active.id)

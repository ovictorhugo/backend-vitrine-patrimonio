import uuid
from http import HTTPStatus

import pytest

from vitrine.schemas import InventoryPublic


@pytest.mark.asyncio
async def test_create_inventory(
    client, create_user, create_token, create_location
):
    user = await create_user()
    token = create_token(user)
    location = await create_location()

    response = client.post(
        '/inventories',
        json={
            'location_id': str(location.id),
            'term': '2025-07',
        },
        headers={'Authorization': f'Bearer {token}'},
    )

    assert response.status_code == HTTPStatus.CREATED
    data = response.json()
    assert data['location_id'] == str(location.id)
    assert data['term'] == '2025-07'
    assert uuid.UUID(data['id'])


@pytest.mark.asyncio
async def test_read_inventories_empty(client):
    response = client.get('/inventories')
    assert response.status_code == HTTPStatus.OK
    assert response.json() == {'inventories': []}


@pytest.mark.asyncio
async def test_read_inventories_with_inventory(client, create_inventory):
    inventory = await create_inventory()
    inventory_schema = InventoryPublic.model_validate(inventory).model_dump(
        mode='json'
    )

    response = client.get('/inventories')

    assert response.status_code == HTTPStatus.OK
    assert response.json() == {'inventories': [inventory_schema]}


@pytest.mark.asyncio
async def test_delete_inventory(
    client, create_inventory, create_user, create_token
):
    user = await create_user()
    token = create_token(user)
    inventory = await create_inventory(user_id=user.id)

    response = client.delete(
        f'/inventories/{inventory.id}',
        headers={'Authorization': f'Bearer {token}'},
    )

    assert response.status_code == HTTPStatus.OK
    assert response.json() == {'message': 'Inventory deactivated successfully'}

    response_get = client.get('/inventories')
    assert response_get.json() == {'inventories': []}


@pytest.mark.asyncio
async def test_create_inventory_conflict(
    client, create_inventory, create_user, create_token
):
    user = await create_user()
    token = create_token(user)
    inventory = await create_inventory(user_id=user.id)

    response = client.post(
        '/inventories',
        json={
            'location_id': str(inventory.location_id),
            'term': inventory.term,
        },
        headers={'Authorization': f'Bearer {token}'},
    )

    assert response.status_code == HTTPStatus.CONFLICT
    assert (
        response.json()['detail']
        == 'An inventory for this location and term already exists.'
    )


@pytest.mark.asyncio
async def test_delete_inventory_not_found(client, create_user, create_token):
    user = await create_user()
    token = create_token(user)
    non_existent_id = uuid.uuid4()

    response = client.delete(
        f'/inventories/{non_existent_id}',
        headers={'Authorization': f'Bearer {token}'},
    )

    assert response.status_code == HTTPStatus.NOT_FOUND
    assert response.json()['detail'] == 'Inventory not found'


@pytest.mark.asyncio
async def test_create_inventory_no_token(client, create_location):
    location = await create_location()
    response = client.post(
        '/inventories',
        json={'location_id': str(location.id), 'term': '2025-08'},
    )
    assert response.status_code == HTTPStatus.UNAUTHORIZED


@pytest.mark.asyncio
async def test_read_inventories_pagination(client, create_inventory):
    inventories = [
        await create_inventory(term=f'2025-0{i + 1}') for i in range(3)
    ]
    inventories.sort(key=lambda inv: inv.created_at)

    response = client.get('/inventories?offset=0&limit=2')
    assert response.status_code == HTTPStatus.OK
    data = response.json()
    assert len(data['inventories']) == 2
    assert data['inventories'][0]['id'] == str(inventories[0].id)

    response = client.get('/inventories?offset=2&limit=2')
    assert response.status_code == HTTPStatus.OK
    data = response.json()
    assert len(data['inventories']) == 1
    assert data['inventories'][0]['id'] == str(inventories[2].id)

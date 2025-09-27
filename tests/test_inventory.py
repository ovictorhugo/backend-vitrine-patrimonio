from http import HTTPStatus
from uuid import uuid4

import pytest

from vitrine.models import Inventory, InventoryAssetStatus


@pytest.mark.asyncio
async def test_create_inventory(
    client, create_user, create_token, create_location
):
    EXPECTED_COUNT = 3
    creator_user = await create_user(email='creator@test.com')
    [await create_location() for _ in range(EXPECTED_COUNT)]

    token = create_token(creator_user)
    inventory_key = f'KEY-{uuid4()}'
    payload = {'key': inventory_key}

    response = client.post(
        '/inventories/',
        json=payload,
        headers={'Authorization': f'Bearer {token}'},
    )

    assert response.status_code == HTTPStatus.CREATED
    data = response.json()
    assert data['key'] == inventory_key
    assert data['created_by']['id'] == str(creator_user.id)


@pytest.mark.asyncio
async def test_create_inventory_conflict_on_duplicate_key(
    client, create_user, create_token, create_inventory
):
    user = await create_user()
    token = create_token(user)

    existing_key = 'DUPLICATE-KEY-123'

    await create_inventory(key=existing_key)

    payload = {'key': existing_key}

    response = client.post(
        '/inventories/',
        json=payload,
        headers={'Authorization': f'Bearer {token}'},
    )

    assert response.status_code == HTTPStatus.CONFLICT
    data = response.json()
    assert data['detail'] == 'Inventory entry already exists'


@pytest.mark.asyncio
async def test_read_inventories(
    client, create_user, create_token, create_inventory
):
    user = await create_user()
    token = create_token(user)

    inv1 = await create_inventory(key=f'KEY-{uuid4()}')
    inv2 = await create_inventory(key=f'KEY-{uuid4()}')

    response = client.get(
        '/inventories/?offset=0&limit=10',
        headers={'Authorization': f'Bearer {token}'},
    )

    assert response.status_code == HTTPStatus.OK
    data = response.json()
    assert len(data['inventories']) >= 2
    keys = [inv['key'] for inv in data['inventories']]
    assert inv1.key in keys
    assert inv2.key in keys


@pytest.mark.asyncio
async def test_update_inventory_success(
    client, session, create_user, create_token, create_inventory
):
    user = await create_user()
    token = create_token(user)

    inv = await create_inventory(key=f'KEY-{uuid4()}', created_by_id=user.id)
    new_key = f'UPDATED-{uuid4()}'

    response = client.put(
        f'/inventories/{inv.id}',
        json={'key': new_key},
        headers={'Authorization': f'Bearer {token}'},
    )

    assert response.status_code == HTTPStatus.OK
    data = response.json()
    assert data['key'] == new_key

    db_inv = await session.get(Inventory, inv.id)
    assert db_inv.key == new_key


@pytest.mark.asyncio
async def test_update_inventory_forbidden(
    client, create_user, create_token, create_inventory
):
    creator = await create_user()
    other = await create_user()

    inv = await create_inventory(
        key=f'KEY-{uuid4()}', created_by_id=creator.id
    )
    token = create_token(other)

    response = client.put(
        f'/inventories/{inv.id}',
        json={'key': 'SHOULD-NOT-WORK'},
        headers={'Authorization': f'Bearer {token}'},
    )

    assert response.status_code == HTTPStatus.FORBIDDEN
    assert response.json()['detail'] == 'Not enough permissions'


@pytest.mark.asyncio
async def test_delete_inventory_success(
    client, session, create_user, create_token, create_inventory
):
    user = await create_user()
    token = create_token(user)

    inv = await create_inventory(key=f'KEY-{uuid4()}', created_by_id=user.id)

    response = client.delete(
        f'/inventories/{inv.id}',
        headers={'Authorization': f'Bearer {token}'},
    )

    assert response.status_code == HTTPStatus.NO_CONTENT

    db_inv = await session.get(Inventory, inv.id)
    assert db_inv.deleted_at is not None


@pytest.mark.asyncio
async def test_delete_inventory_forbidden(
    client, create_user, create_token, create_inventory
):
    creator = await create_user()
    other = await create_user()

    inv = await create_inventory(
        key=f'KEY-{uuid4()}', created_by_id=creator.id
    )
    token = create_token(other)

    response = client.delete(
        f'/inventories/{inv.id}',
        headers={'Authorization': f'Bearer {token}'},
    )

    assert response.status_code == HTTPStatus.FORBIDDEN
    assert response.json()['detail'] == 'Not enough permissions'


@pytest.mark.asyncio
async def test_update_inventory_conflict(
    client, create_user, create_token, create_inventory
):
    user = await create_user()
    token = create_token(user)

    inv1 = await create_inventory(key=f'KEY-{uuid4()}', created_by_id=user.id)
    inv2 = await create_inventory(key=f'KEY-{uuid4()}', created_by_id=user.id)

    response = client.put(
        f'/inventories/{inv2.id}',
        json={'key': inv1.key},
        headers={'Authorization': f'Bearer {token}'},
    )

    assert response.status_code == HTTPStatus.CONFLICT
    assert response.json()['detail'] == 'Inventory key already exists'


@pytest.mark.asyncio
async def test_delete_inventory_not_found(client, create_user, create_token):
    user = await create_user()
    token = create_token(user)

    response = client.delete(
        f'/inventories/{uuid4()}',
        headers={'Authorization': f'Bearer {token}'},
    )

    assert response.status_code == HTTPStatus.NOT_FOUND
    assert response.json()['detail'] == 'Inventory not found'


@pytest.mark.asyncio
async def test_put_inventory_not_found(client, create_user, create_token):
    user = await create_user()
    token = create_token(user)

    response = client.put(
        f'/inventories/{uuid4()}',
        json={'key': 'xpto'},
        headers={'Authorization': f'Bearer {token}'},
    )

    assert response.status_code == HTTPStatus.NOT_FOUND
    assert response.json()['detail'] == 'Inventory not found'


@pytest.mark.asyncio
async def test_read_inventory(
    client, create_user, create_token, create_inventory
):
    user = await create_user()
    token = create_token(user)

    inv1 = await create_inventory(key=f'KEY-{uuid4()}')

    response = client.get(
        f'/inventories/{inv1.id}',
        headers={'Authorization': f'Bearer {token}'},
    )

    assert response.status_code == HTTPStatus.OK
    data = response.json()
    assert inv1.key == data['key']


@pytest.mark.asyncio
async def test_add_asset_to_inventory_success(
    client,
    session,
    create_user,
    create_token,
    create_inventory,
    create_asset,
    create_location,
):
    user = await create_user()
    token = create_token(user)
    location = await create_location(user_id=user.id)
    inventory = await create_inventory(key=f'KEY-{uuid4()}')
    asset = await create_asset()

    payload = {
        'asset_id': str(asset.id),
        'status': InventoryAssetStatus.FOUND,
        'comment': 'Test comment',
        'location_id': str(location.id),
    }

    response = client.post(
        f'/inventories/{inventory.id}/assets',
        json=payload,
        headers={'Authorization': f'Bearer {token}'},
    )

    assert response.status_code == HTTPStatus.CREATED
    data = response.json()
    assert data['status'] == InventoryAssetStatus.FOUND.value
    assert data['comment'] == 'Test comment'


@pytest.mark.asyncio
async def test_add_asset_inventory_not_found(
    client, create_user, create_token
):
    user = await create_user()
    token = create_token(user)

    payload = {
        'asset_id': str(uuid4()),
        'status': InventoryAssetStatus.FOUND,
        'location_id': str(uuid4()),
    }

    response = client.post(
        f'/inventories/{uuid4()}/assets',
        json=payload,
        headers={'Authorization': f'Bearer {token}'},
    )

    assert response.status_code == HTTPStatus.NOT_FOUND
    assert response.json()['detail'] == 'Inventory not found'


@pytest.mark.asyncio
async def test_add_asset_inventory_closed(
    client, create_user, create_token, create_inventory
):
    user = await create_user()
    token = create_token(user)
    inventory = await create_inventory(key=f'KEY-{uuid4()}', avaliable=False)

    payload = {
        'asset_id': str(uuid4()),
        'status': InventoryAssetStatus.FOUND,
        'location_id': str(uuid4()),
    }

    response = client.post(
        f'/inventories/{inventory.id}/assets',
        json=payload,
        headers={'Authorization': f'Bearer {token}'},
    )

    assert response.status_code == HTTPStatus.UNAUTHORIZED
    assert (
        response.json()['detail'] == 'Inventory is no longer accepting assets'
    )


@pytest.mark.asyncio
async def test_add_asset_user_not_location(
    client,
    create_user,
    create_token,
    create_inventory,
    create_asset,
):
    inventory = await create_inventory(key=f'KEY-{uuid4()}')
    user = await create_user()
    token = create_token(user)
    asset = await create_asset()

    payload = {
        'asset_id': str(asset.id),
        'status': InventoryAssetStatus.FOUND,
        'location_id': str(uuid4()),
    }

    response = client.post(
        f'/inventories/{inventory.id}/assets',
        json=payload,
        headers={'Authorization': f'Bearer {token}'},
    )

    assert response.status_code == HTTPStatus.NOT_FOUND
    assert (
        response.json()['detail']
        == 'The specified location is not registered for this inventory.'
    )


@pytest.mark.asyncio
async def test_add_asset_not_found(
    client, create_user, create_token, create_inventory
):
    user = await create_user()
    token = create_token(user)
    inventory = await create_inventory(key=f'KEY-{uuid4()}')

    payload = {
        'asset_id': str(uuid4()),
        'status': InventoryAssetStatus.FOUND,
        'location_id': str(uuid4()),
    }

    response = client.post(
        f'/inventories/{inventory.id}/assets',
        json=payload,
        headers={'Authorization': f'Bearer {token}'},
    )

    assert response.status_code == HTTPStatus.NOT_FOUND
    assert (
        response.json()['detail']
        == 'The specified location is not registered for this inventory.'
    )


@pytest.mark.asyncio
async def test_add_assets_batch_success(
    client,
    create_user,
    create_token,
    create_inventory,
    create_asset,
    create_location,
):
    user = await create_user()
    location = await create_location(user_id=user.id)
    token = create_token(user)
    inventory = await create_inventory(key=f'KEY-{uuid4()}')
    asset1 = await create_asset()
    asset2 = await create_asset()

    payload = [
        {
            'asset_id': str(asset1.id),
            'status': InventoryAssetStatus.FOUND,
            'comment': 'Item 1 ok',
            'location_id': str(location.id),
        },
        {
            'asset_id': str(asset2.id),
            'status': InventoryAssetStatus.NOT_FOUND,
            'comment': 'Item 2 not found',
            'location_id': str(location.id),
        },
    ]

    response = client.post(
        f'/inventories/{inventory.id}/assets/batch',
        json=payload,
        headers={'Authorization': f'Bearer {token}'},
    )

    assert response.status_code == HTTPStatus.CREATED
    data = response.json()
    assert len(data['inventoried_asset']) == 2


@pytest.mark.asyncio
async def test_add_assets_batch_one_asset_not_found(
    client,
    create_user,
    create_token,
    create_inventory,
    create_asset,
    create_location,
):
    user = await create_user()
    token = create_token(user)
    location = await create_location(user_id=user.id)
    inventory = await create_inventory(key=f'KEY-{uuid4()}')
    asset1 = await create_asset()
    non_existent_asset_id = uuid4()

    payload = [
        {
            'asset_id': str(asset1.id),
            'status': InventoryAssetStatus.FOUND,
            'location_id': str(location.id),
        },
        {
            'asset_id': str(non_existent_asset_id),
            'status': InventoryAssetStatus.FOUND,
            'location_id': str(location.id),
        },
    ]

    response = client.post(
        f'/inventories/{inventory.id}/assets/batch',
        json=payload,
        headers={'Authorization': f'Bearer {token}'},
    )

    assert response.status_code == HTTPStatus.NOT_FOUND
    data = response.json()
    assert 'Assets not found' in data['detail']
    assert str(non_existent_asset_id) in data['detail']


@pytest.mark.asyncio
async def test_add_assets_batch_conflict_on_duplicate(
    client,
    create_user,
    create_token,
    create_inventory,
    create_asset,
    create_location,
):
    user = await create_user()
    token = create_token(user)
    location = await create_location()
    inventory = await create_inventory(key=f'KEY-{uuid4()}')
    asset1 = await create_asset()

    payload = [
        {
            'asset_id': str(asset1.id),
            'status': InventoryAssetStatus.FOUND,
            'location_id': str(location.id),
        },
        {
            'asset_id': str(asset1.id),
            'status': InventoryAssetStatus.FOUND,
            'location_id': str(location.id),
        },
    ]

    response = client.post(
        f'/inventories/{inventory.id}/assets/batch',
        json=payload,
        headers={'Authorization': f'Bearer {token}'},
    )

    assert response.status_code == HTTPStatus.CONFLICT
    assert (
        response.json()['detail']
        == 'One or more assets have already been added to your inventory.'
    )


@pytest.mark.asyncio
async def test_add_assets_batch_empty_list(
    client, create_user, create_token, create_inventory
):
    user = await create_user()
    token = create_token(user)
    inventory = await create_inventory(key=f'KEY-{uuid4()}')

    response = client.post(
        f'/inventories/{inventory.id}/assets/batch',
        json=[],
        headers={'Authorization': f'Bearer {token}'},
    )

    assert response.status_code == HTTPStatus.CREATED
    data = response.json()
    assert data['inventoried_asset'] == []


@pytest.mark.asyncio
async def test_add_assets_batch_user_not_location(
    client,
    create_user,
    create_token,
    create_inventory,
    create_asset,
    create_location,
):
    await create_user(email='owner@test.com')
    location = await create_location()
    inventory = await create_inventory(key=f'KEY-{uuid4()}')

    non_owner_user = await create_user(email='non-owner@test.com')
    token = create_token(non_owner_user)
    asset = await create_asset()

    payload = [
        {
            'asset_id': str(asset.id),
            'status': InventoryAssetStatus.FOUND,
            'location_id': str(location.id),
        }
    ]

    response = client.post(
        f'/inventories/{inventory.id}/assets/batch',
        json=payload,
        headers={'Authorization': f'Bearer {token}'},
    )

    assert response.status_code == HTTPStatus.FORBIDDEN
    assert (
        response.json()['detail'] == 'User is not an owner of this inventory'
    )


@pytest.mark.asyncio
async def test_list_inventory_locations_success(
    client,
    create_user,
    create_token,
    create_inventory,
    create_location,
    create_location_inventory,
):
    user = await create_user()
    token = create_token(user)
    inventory = await create_inventory()

    location1 = await create_location(location_name='Bloco A')
    location2 = await create_location(location_name='Bloco B')
    await create_location(location_name='Bloco C')

    await create_location_inventory(
        inventory_id=inventory.id, location_id=location1.id
    )
    await create_location_inventory(
        inventory_id=inventory.id, location_id=location2.id
    )

    response = client.get(
        f'/inventories/{inventory.id}/locations',
        headers={'Authorization': f'Bearer {token}'},
    )

    assert response.status_code == HTTPStatus.OK

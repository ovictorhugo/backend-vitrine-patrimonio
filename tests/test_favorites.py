import uuid
from http import HTTPStatus

import pytest


@pytest.mark.asyncio
async def test_create_favorite(
    client, create_user, create_token, create_catalog_entry
):
    user = await create_user()
    token = create_token(user)
    catalog = await create_catalog_entry()

    response = client.post(
        f'/favorites/{catalog.id}',
        headers={'Authorization': f'Bearer {token}'},
    )

    assert response.status_code == HTTPStatus.CREATED
    assert response.json() == {'message': 'Asset favorited successfully'}


@pytest.mark.asyncio
async def test_create_favorite_unauthenticated(client, create_catalog_entry):
    catalog = await create_catalog_entry()
    response = client.post(f'/favorites/{catalog.id}')
    assert response.status_code == HTTPStatus.UNAUTHORIZED


@pytest.mark.asyncio
async def test_create_favorite_catalog_not_found(
    client, create_user, create_token
):
    user = await create_user()
    token = create_token(user)
    non_existent_id = uuid.uuid4()

    response = client.post(
        f'/favorites/{non_existent_id}',
        headers={'Authorization': f'Bearer {token}'},
    )

    assert response.status_code == HTTPStatus.NOT_FOUND


@pytest.mark.asyncio
async def test_create_favorite_already_exists(
    client,
    create_user,
    create_token,
    create_catalog_entry,
    create_favorite_catalog,
):
    user = await create_user()
    token = create_token(user)
    catalog = await create_catalog_entry()
    await create_favorite_catalog(user_id=user.id, catalog_id=catalog.id)

    response = client.post(
        f'/favorites/{catalog.id}',
        headers={'Authorization': f'Bearer {token}'},
    )

    assert response.status_code == HTTPStatus.CONFLICT


@pytest.mark.asyncio
async def test_read_favorites_empty(client, create_user, create_token):
    user = await create_user()
    token = create_token(user)

    response = client.get(
        '/favorites/',
        headers={'Authorization': f'Bearer {token}'},
    )

    assert response.status_code == HTTPStatus.OK
    assert response.json() == {'favorites': []}


@pytest.mark.asyncio
async def test_read_favorites_with_items(
    client,
    create_user,
    create_token,
    create_catalog_entry,
    create_favorite_catalog,
):
    user = await create_user()
    token = create_token(user)

    catalog1 = await create_catalog_entry()
    catalog2 = await create_catalog_entry()
    await create_favorite_catalog(user_id=user.id, catalog_id=catalog1.id)
    await create_favorite_catalog(user_id=user.id, catalog_id=catalog2.id)

    await create_favorite_catalog()

    response = client.get(
        '/favorites/',
        headers={'Authorization': f'Bearer {token}'},
    )

    assert response.status_code == HTTPStatus.OK
    data = response.json()
    assert len(data['favorites']) == 2
    favorite_ids = {fav['id'] for fav in data['favorites']}
    assert str(catalog1.id) in favorite_ids
    assert str(catalog2.id) in favorite_ids


@pytest.mark.asyncio
async def test_delete_favorite(
    client,
    create_user,
    create_token,
    create_catalog_entry,
    create_favorite_catalog,
):
    user = await create_user()
    token = create_token(user)
    catalog = await create_catalog_entry()
    await create_favorite_catalog(user_id=user.id, catalog_id=catalog.id)

    response = client.delete(
        f'/favorites/{catalog.id}',
        headers={'Authorization': f'Bearer {token}'},
    )

    assert response.status_code == HTTPStatus.OK
    assert response.json() == {'message': 'Favorite removed successfully'}

    response_get = client.get(
        '/favorites/',
        headers={'Authorization': f'Bearer {token}'},
    )
    assert response_get.json() == {'favorites': []}


@pytest.mark.asyncio
async def test_delete_favorite_not_found(client, create_user, create_token):
    user = await create_user()
    token = create_token(user)
    non_existent_id = uuid.uuid4()

    response = client.delete(
        f'/favorites/{non_existent_id}',
        headers={'Authorization': f'Bearer {token}'},
    )

    assert response.status_code == HTTPStatus.NOT_FOUND

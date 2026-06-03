from http import HTTPStatus

import pytest

pytestmark = pytest.mark.asyncio


async def test_create_collection(client, create_user, create_token):
    user = await create_user()
    token = create_token(user)
    payload = {
        'name': 'Meus Itens Favoritos',
        'description': 'Descrição da coleção.',
        'type': 'FAVORITES',
    }

    response = client.post(
        '/collections/',
        json=payload,
        headers={'Authorization': f'Bearer {token}'},
    )

    assert response.status_code == HTTPStatus.CREATED
    data = response.json()
    assert data['name'] == payload['name']
    assert data['description'] == payload['description']
    assert 'id' in data


async def test_create_collection_fails_if_name_exists_for_same_user(
    client, create_user, create_token, create_collection
):
    user = await create_user()
    token = create_token(user)
    await create_collection(user_id=user.id, name='Coleção Repetida')

    payload = {'name': 'Coleção Repetida', 'description': ''}
    response = client.post(
        '/collections/',
        json=payload,
        headers={'Authorization': f'Bearer {token}'},
    )

    assert response.status_code == HTTPStatus.CONFLICT
    assert (
        response.json()['detail']
        == 'You already have a collection with this name.'
    )


async def test_read_my_collections(
    client, create_user, create_token, create_collection
):
    user_a = await create_user()
    token_a = create_token(user_a)
    await create_collection(user_id=user_a.id, name='Coleção 1 de A')
    await create_collection(user_id=user_a.id, name='Coleção 2 de A')

    user_b = await create_user()
    await create_collection(user_id=user_b.id, name='Coleção de B')

    response = client.get(
        '/collections/my', headers={'Authorization': f'Bearer {token_a}'}
    )

    assert response.status_code == HTTPStatus.OK
    data = response.json()
    assert len(data['collections']) == 2
    assert {c['name'] for c in data['collections']} == {
        'Coleção 1 de A',
        'Coleção 2 de A',
    }


async def test_read_collection_by_id(
    client, create_user, create_token, create_collection
):
    user = await create_user()
    token = create_token(user)
    collection = await create_collection(user_id=user.id)

    response = client.get(
        f'/collections/{collection.id}',
        headers={'Authorization': f'Bearer {token}'},
    )

    assert response.status_code == HTTPStatus.OK
    data = response.json()
    assert data['id'] == str(collection.id)
    assert data['name'] == collection.name


async def test_delete_collection(
    client, session, create_user, create_token, create_collection
):
    user = await create_user()
    token = create_token(user)
    collection = await create_collection(user_id=user.id)

    response = client.delete(
        f'/collections/{collection.id}',
        headers={'Authorization': f'Bearer {token}'},
    )

    assert response.status_code == HTTPStatus.OK
    assert response.json() == {
        'message': 'Collection deactivated successfully.'
    }

    get_response = client.get(
        f'/collections/{collection.id}',
        headers={'Authorization': f'Bearer {token}'},
    )
    assert get_response.status_code == HTTPStatus.NOT_FOUND

    await session.refresh(collection)
    assert collection.deleted_at is not None


async def test_read_my_collections_pagination_offset_limit(
    client, create_user, create_token, create_collection
):
    user = await create_user()
    token = create_token(user)

    names = ['Coleção A', 'Coleção B', 'Coleção C']
    for name in sorted(names):
        await create_collection(user_id=user.id, name=name)

    response = client.get(
        '/collections/my?offset=1&limit=1',
        headers={'Authorization': f'Bearer {token}'},
    )

    assert response.status_code == HTTPStatus.OK
    data = response.json()
    assert len(data['collections']) == 1
    assert data['collections'][0]['name'] == 'Coleção B'

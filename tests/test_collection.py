from http import HTTPStatus
from uuid import uuid4

import pytest

pytestmark = pytest.mark.asyncio


async def test_create_collection(client, create_user, create_token):
    user = await create_user()
    token = create_token(user)
    payload = {
        'name': 'Meus Itens Favoritos',
        'description': 'Descrição da coleção.',
    }

    response = client.post(
        '/collections/',
        json=payload,
        headers={'Authorization': f'Bearer {token}'},
    )

    assert response.status_code == HTTPStatus.CREATED
    data = response.json()
    assert data['name'] == payload['name']
    assert data['user_id'] == str(user.id)
    assert 'id' in data


async def test_create_collection_fails_if_name_exists(
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
        == 'Você já possui uma coleção com este nome.'
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
    client, create_user, create_token, create_collection
):
    user = await create_user()
    token = create_token(user)
    collection = await create_collection(user_id=user.id)

    response = client.delete(
        f'/collections/{collection.id}',
        headers={'Authorization': f'Bearer {token}'},
    )

    assert response.status_code == HTTPStatus.OK
    assert response.json() == {'message': 'Coleção desativada com sucesso.'}

    get_response = client.get(
        f'/collections/{collection.id}',
        headers={'Authorization': f'Bearer {token}'},
    )
    assert get_response.status_code == HTTPStatus.NOT_FOUND


async def test_add_item_to_collection(
    client, create_user, create_token, create_collection, create_catalog_entry
):
    user = await create_user()
    token = create_token(user)
    collection = await create_collection(user_id=user.id)
    catalog = await create_catalog_entry(user_id=user.id)

    payload = {
        'catalog_id': str(catalog.id),
        'status': True,
        'comment': 'Item de catálogo interessante.',
    }

    response = client.post(
        f'/collections/{collection.id}/items',
        json=payload,
        headers={'Authorization': f'Bearer {token}'},
    )

    assert response.status_code == HTTPStatus.CREATED
    data = response.json()
    assert data['comment'] == 'Item de catálogo interessante.'
    assert data['status'] is True
    assert 'catalog' in data
    assert data['catalog']['id'] == str(catalog.id)


async def test_add_item_fails_if_item_not_found(
    client, create_user, create_token, create_collection
):
    user = await create_user()
    token = create_token(user)
    collection = await create_collection(user_id=user.id)
    fake_catalog_id = uuid4()

    payload = {
        'catalog_id': str(fake_catalog_id),
        'status': True,
        'comment': 'Item que não existe.',
    }
    response = client.post(
        f'/collections/{collection.id}/items',
        json=payload,
        headers={'Authorization': f'Bearer {token}'},
    )

    assert response.status_code == HTTPStatus.NOT_FOUND
    expected_detail = (
        f'O item de catálogo com ID "{fake_catalog_id}" não foi encontrado.'
    )
    assert response.json()['detail'] == expected_detail


async def test_add_item_fails_if_already_in_collection(
    client, create_user, create_token, create_collection, create_catalog_entry
):
    user = await create_user()
    token = create_token(user)
    collection = await create_collection(user_id=user.id)
    catalog = await create_catalog_entry(user_id=user.id)

    payload = {
        'catalog_id': str(catalog.id),
        'status': True,
        'comment': 'Adicionando item.',
    }
    client.post(
        f'/collections/{collection.id}/items',
        json=payload,
        headers={'Authorization': f'Bearer {token}'},
    )

    response = client.post(
        f'/collections/{collection.id}/items',
        json=payload,
        headers={'Authorization': f'Bearer {token}'},
    )

    assert response.status_code == HTTPStatus.CONFLICT
    assert response.json()['detail'] == 'Este item já está na coleção.'


async def test_remove_item_from_collection(
    client, create_user, create_token, create_collection, create_catalog_entry
):
    user = await create_user()
    token = create_token(user)
    collection = await create_collection(user_id=user.id)
    catalog = await create_catalog_entry(user_id=user.id)

    add_payload = {
        'catalog_id': str(catalog.id),
        'status': True,
        'comment': 'Item para remover.',
    }
    add_response = client.post(
        f'/collections/{collection.id}/items',
        json=add_payload,
        headers={'Authorization': f'Bearer {token}'},
    )
    item_in_collection_id = add_response.json()['id']

    delete_response = client.delete(
        f'/collections/{collection.id}/items/{item_in_collection_id}',
        headers={'Authorization': f'Bearer {token}'},
    )
    assert delete_response.status_code == HTTPStatus.OK
    assert delete_response.json() == {
        'message': 'Item removido da coleção com sucesso.'
    }

    get_response = client.get(
        f'/collections/{collection.id}',
        headers={'Authorization': f'Bearer {token}'},
    )
    assert len(get_response.json()['items']) == 0

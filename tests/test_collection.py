from http import HTTPStatus
from uuid import uuid4

import pytest

from vitrine.models import CollectionItemType

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


async def test_read_collection_fails_if_not_found(
    client, create_user, create_token
):
    user = await create_user()
    token = create_token(user)
    fake_id = uuid4()

    response = client.get(
        f'/collections/{fake_id}', headers={'Authorization': f'Bearer {token}'}
    )

    assert response.status_code == HTTPStatus.NOT_FOUND


async def test_read_collection_fails_if_not_owner(
    client, create_user, create_token, create_collection
):
    owner = await create_user()
    collection = await create_collection(user_id=owner.id)

    other_user = await create_user()
    other_token = create_token(other_user)

    response = client.get(
        f'/collections/{collection.id}',
        headers={'Authorization': f'Bearer {other_token}'},
    )

    assert response.status_code == HTTPStatus.FORBIDDEN


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


async def test_delete_collection_fails_if_not_owner(
    client, create_user, create_token, create_collection
):
    owner = await create_user()
    collection = await create_collection(user_id=owner.id)

    other_user = await create_user()
    other_token = create_token(other_user)

    response = client.delete(
        f'/collections/{collection.id}',
        headers={'Authorization': f'Bearer {other_token}'},
    )

    assert response.status_code == HTTPStatus.FORBIDDEN


async def test_update_collection(
    client, create_user, create_token, create_collection
):
    user = await create_user()
    token = create_token(user)
    collection = await create_collection(user_id=user.id, name='Nome Antigo')

    payload = {'name': 'Nome Novo', 'description': 'Descrição Atualizada'}
    response = client.put(
        f'/collections/{collection.id}',
        json=payload,
        headers={'Authorization': f'Bearer {token}'},
    )

    assert response.status_code == HTTPStatus.OK
    data = response.json()
    assert data['name'] == 'Nome Novo'
    assert data['description'] == 'Descrição Atualizada'


async def test_add_item_to_collection(
    client, create_user, create_token, create_collection, create_asset
):
    user = await create_user()
    token = create_token(user)
    collection = await create_collection(user_id=user.id)
    asset = await create_asset()

    payload = {
        'item_id': str(asset.id),
        'item_type': CollectionItemType.ASSET.value,
        'status': True,
        'comment': 'XPTO',
    }

    response = client.post(
        f'/collections/{collection.id}/items',
        json=payload,
        headers={'Authorization': f'Bearer {token}'},
    )

    assert response.status_code == HTTPStatus.CREATED
    data = response.json()
    assert data['item_id'] == str(asset.id)
    assert data['item_type'] == 'ASSET'


async def test_add_item_fails_if_item_not_found(
    client, create_user, create_token, create_collection
):
    user = await create_user()
    token = create_token(user)
    collection = await create_collection(user_id=user.id)
    fake_asset_id = uuid4()

    payload = {
        'item_id': str(fake_asset_id),
        'item_type': CollectionItemType.ASSET.value,
        'status': True,
        'comment': 'XPTO',
    }
    response = client.post(
        f'/collections/{collection.id}/items',
        json=payload,
        headers={'Authorization': f'Bearer {token}'},
    )

    assert response.status_code == HTTPStatus.NOT_FOUND
    assert (
        f'O item do tipo "ASSET" com ID "{fake_asset_id}" não foi encontrado.'
        in response.json()['detail']
    )


async def test_add_item_fails_if_already_in_collection(
    client, create_user, create_token, create_collection, create_asset
):
    user = await create_user()
    token = create_token(user)
    collection = await create_collection(user_id=user.id)
    asset = await create_asset()

    payload = {
        'item_id': str(asset.id),
        'item_type': CollectionItemType.ASSET.value,
        'status': True,
        'comment': 'XPTO',
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
    client, create_user, create_token, create_collection, create_asset
):
    user = await create_user()
    token = create_token(user)
    collection = await create_collection(user_id=user.id)
    asset = await create_asset()

    add_payload = {
        'item_id': str(asset.id),
        'item_type': CollectionItemType.ASSET.value,
        'status': True,
        'comment': 'XPTO',
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


async def test_remove_item_fails_if_not_owner_of_collection(
    client, create_user, create_token, create_collection, create_asset
):
    owner = await create_user()
    owner_token = create_token(owner)
    collection = await create_collection(user_id=owner.id)
    asset = await create_asset()

    add_payload = {
        'item_id': str(asset.id),
        'item_type': 'ASSET',
        'status': True,
        'comment': 'XPTO',
    }
    add_resp = client.post(
        f'/collections/{collection.id}/items',
        json=add_payload,
        headers={'Authorization': f'Bearer {owner_token}'},
    )
    item_id = add_resp.json()['id']

    other_user = await create_user()
    other_token = create_token(other_user)
    delete_response = client.delete(
        f'/collections/{collection.id}/items/{item_id}',
        headers={'Authorization': f'Bearer {other_token}'},
    )

    assert delete_response.status_code == HTTPStatus.FORBIDDEN

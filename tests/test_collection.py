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
    assert response.json() == {
        'message': 'Collection deactivated successfully.'
    }

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
    expected_detail = f'Catalog item with ID "{fake_catalog_id}" not found.'
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
    assert (
        response.json()['detail'] == 'This item is already in the collection.'
    )


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
    assert delete_response.status_code == HTTPStatus.NOT_FOUND
    assert delete_response.json() == {
        'message': 'Item removed from the collection successfully.'
    }

    get_response = client.get(
        f'/collections/{collection.id}',
        headers={'Authorization': f'Bearer {token}'},
    )
    assert len(get_response.json()['items']) == 0


pytestmark = pytest.mark.asyncio


async def test_read_my_collections_filter_q(
    client, create_user, create_token, create_collection, create_catalog_entry
):
    user = await create_user()
    token = create_token(user)
    collection = await create_collection(user_id=user.id)
    catalog = await create_catalog_entry(
        user_id=user.id, description='Descrição com palavraunica123'
    )
    add_payload = {
        'catalog_id': str(catalog.id),
        'status': True,
        'comment': 'coment',
    }
    client.post(
        f'/collections/{collection.id}/items',
        json=add_payload,
        headers={'Authorization': f'Bearer {token}'},
    )
    response = client.get(
        '/collections/my?q=palavraunica123',
        headers={'Authorization': f'Bearer {token}'},
    )
    assert response.status_code == HTTPStatus.OK
    data = response.json()
    assert len(data['collections']) == 1
    assert data['collections'][0]['id'] == str(collection.id)


async def test_read_my_collections_filter_asset_identifier(
    client,
    create_user,
    create_token,
    create_collection,
    create_catalog_entry,
    create_asset,
):
    user = await create_user()
    token = create_token(user)
    collection = await create_collection(user_id=user.id)
    asset = await create_asset(asset_code='ABC123', asset_check_digit='4')
    catalog = await create_catalog_entry(user_id=user.id, asset_id=asset.id)
    add_payload = {
        'catalog_id': str(catalog.id),
        'status': True,
        'comment': 'coment',
    }
    client.post(
        f'/collections/{collection.id}/items',
        json=add_payload,
        headers={'Authorization': f'Bearer {token}'},
    )
    response = client.get(
        '/collections/my?asset_identifier=ABC123-4',
        headers={'Authorization': f'Bearer {token}'},
    )
    assert response.status_code == HTTPStatus.OK
    data = response.json()
    assert len(data['collections']) == 1
    assert data['collections'][0]['id'] == str(collection.id)


async def test_read_my_collections_filter_atm_number_is_official_and_user_id_and_asset_status_and_csv_code(
    client,
    create_user,
    create_token,
    create_collection,
    create_catalog_entry,
    create_asset,
):
    user = await create_user()
    token = create_token(user)
    collection = await create_collection(user_id=user.id)
    owner = await create_user()
    asset = await create_asset(
        atm_number='ATM-999',
        is_official=True,
        asset_status='operational',
        csv_code='CSV-321',
    )
    catalog = await create_catalog_entry(user_id=owner.id, asset_id=asset.id)
    add_payload = {
        'catalog_id': str(catalog.id),
        'status': True,
        'comment': 'coment',
    }
    client.post(
        f'/collections/{collection.id}/items',
        json=add_payload,
        headers={'Authorization': f'Bearer {token}'},
    )
    params = (
        '/collections/my?atm_number=ATM-999'
        '&is_official=true'
        f'&user_id={owner.id}'
        '&asset_status=operational'
        '&csv_code=CSV-321'
    )
    response = client.get(params, headers={'Authorization': f'Bearer {token}'})
    assert response.status_code == HTTPStatus.OK
    data = response.json()
    assert len(data['collections']) == 1
    assert data['collections'][0]['id'] == str(collection.id)


async def test_read_my_collections_pagination_offset_limit(
    client, create_user, create_token, create_collection, create_catalog_entry
):
    user = await create_user()
    token = create_token(user)
    coll1 = await create_collection(user_id=user.id, name='Pag 1')
    coll2 = await create_collection(user_id=user.id, name='Pag 2')
    cat1 = await create_catalog_entry(user_id=user.id)
    cat2 = await create_catalog_entry(user_id=user.id)
    payload1 = {'catalog_id': str(cat1.id), 'status': True, 'comment': 'c1'}
    payload2 = {'catalog_id': str(cat2.id), 'status': True, 'comment': 'c2'}
    client.post(
        f'/collections/{coll1.id}/items',
        json=payload1,
        headers={'Authorization': f'Bearer {token}'},
    )
    client.post(
        f'/collections/{coll2.id}/items',
        json=payload2,
        headers={'Authorization': f'Bearer {token}'},
    )
    response = client.get(
        '/collections/my?offset=1&limit=1',
        headers={'Authorization': f'Bearer {token}'},
    )
    assert response.status_code == HTTPStatus.OK
    data = response.json()
    assert len(data['collections']) == 1
    assert data['collections'][0]['name'] in {'Pag 1', 'Pag 2'}

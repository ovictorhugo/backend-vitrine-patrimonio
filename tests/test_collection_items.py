from http import HTTPStatus
from uuid import uuid4

import pytest

pytestmark = pytest.mark.asyncio


async def test_add_item_to_collection(
    client, create_user, create_token, create_collection, create_catalog_entry
):
    user = await create_user()
    token = create_token(user)
    collection = await create_collection(user_id=user.id)
    catalog_item = await create_catalog_entry(user_id=user.id)

    payload = {
        'catalog_id': str(catalog_item.id),
        'status': True,
        'comment': 'Item de catálogo interessante.',
    }

    response = client.post(
        f'/collections/{collection.id}/items/',
        json=payload,
        headers={'Authorization': f'Bearer {token}'},
    )

    assert response.status_code == HTTPStatus.CREATED
    data = response.json()
    assert data['comment'] == 'Item de catálogo interessante.'
    assert data['status'] is True
    assert 'catalog' in data
    assert data['catalog']['id'] == str(catalog_item.id)


async def test_add_item_fails_if_catalog_not_found(
    client, create_user, create_token, create_collection
):
    user = await create_user()
    token = create_token(user)
    collection = await create_collection(user_id=user.id)
    fake_catalog_id = uuid4()

    payload = {
        'catalog_id': str(fake_catalog_id),
        'status': True,
    }
    response = client.post(
        f'/collections/{collection.id}/items/',
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
    catalog_item = await create_catalog_entry(user_id=user.id)

    payload = {
        'catalog_id': str(catalog_item.id),
        'status': True,
    }
    first_response = client.post(
        f'/collections/{collection.id}/items/',
        json=payload,
        headers={'Authorization': f'Bearer {token}'},
    )
    assert first_response.status_code == HTTPStatus.CREATED

    second_response = client.post(
        f'/collections/{collection.id}/items/',
        json=payload,
        headers={'Authorization': f'Bearer {token}'},
    )

    assert second_response.status_code == HTTPStatus.CONFLICT
    assert (
        second_response.json()['detail']
        == 'This item is already in the collection.'
    )


async def test_add_item_fails_for_other_user_collection(
    client, create_user, create_token, create_collection, create_catalog_entry
):
    owner_user = await create_user()
    collection = await create_collection(user_id=owner_user.id)
    catalog_item = await create_catalog_entry(user_id=owner_user.id)

    other_user = await create_user()
    other_token = create_token(other_user)

    payload = {'catalog_id': str(catalog_item.id), 'status': True}
    response = client.post(
        f'/collections/{collection.id}/items/',
        json=payload,
        headers={'Authorization': f'Bearer {other_token}'},
    )

    assert response.status_code == HTTPStatus.FORBIDDEN


async def test_remove_item_from_collection(
    client, create_user, create_token, create_collection, create_catalog_entry
):
    user = await create_user()
    token = create_token(user)
    collection = await create_collection(user_id=user.id)
    catalog_item = await create_catalog_entry(user_id=user.id)

    add_payload = {'catalog_id': str(catalog_item.id), 'status': True}
    add_response = client.post(
        f'/collections/{collection.id}/items/',
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
        'message': 'Item removed from the collection successfully.'
    }

    get_response = client.get(
        f'/collections/{collection.id}',
        headers={'Authorization': f'Bearer {token}'},
    )
    assert get_response.status_code == HTTPStatus.OK


async def test_remove_item_fails_if_item_not_in_collection(
    client, create_user, create_token, create_collection
):
    user = await create_user()
    token = create_token(user)
    collection = await create_collection(user_id=user.id)
    fake_item_id = uuid4()

    response = client.delete(
        f'/collections/{collection.id}/items/{fake_item_id}',
        headers={'Authorization': f'Bearer {token}'},
    )

    assert response.status_code == HTTPStatus.NOT_FOUND
    assert response.json()['detail'] == 'Item not found in this collection.'


async def test_list_collection_items_success(
    client, create_user, create_token, create_collection, create_catalog_entry
):
    user = await create_user()
    token = create_token(user)
    collection = await create_collection(user_id=user.id)
    catalog_item1 = await create_catalog_entry(user_id=user.id)
    catalog_item2 = await create_catalog_entry(user_id=user.id)

    client.post(
        f'/collections/{collection.id}/items/',
        json={'catalog_id': str(catalog_item1.id), 'status': True},
        headers={'Authorization': f'Bearer {token}'},
    )
    client.post(
        f'/collections/{collection.id}/items/',
        json={'catalog_id': str(catalog_item2.id), 'status': False},
        headers={'Authorization': f'Bearer {token}'},
    )

    response = client.get(
        f'/collections/{collection.id}/items/',
        headers={'Authorization': f'Bearer {token}'},
    )

    assert response.status_code == HTTPStatus.OK
    data = response.json()
    assert 'collection_items' in data
    assert len(data['collection_items']) == 2

    returned_catalog_ids = {
        item['catalog']['id'] for item in data['collection_items']
    }
    expected_catalog_ids = {str(catalog_item1.id), str(catalog_item2.id)}
    assert returned_catalog_ids == expected_catalog_ids


async def test_list_items_from_empty_collection(
    client, create_user, create_token, create_collection
):
    user = await create_user()
    token = create_token(user)
    collection = await create_collection(user_id=user.id)

    response = client.get(
        f'/collections/{collection.id}/items/',
        headers={'Authorization': f'Bearer {token}'},
    )

    assert response.status_code == HTTPStatus.OK
    data = response.json()
    assert data['collection_items'] == []


async def test_list_items_fails_if_collection_not_found(
    client, create_user, create_token
):
    user = await create_user()
    token = create_token(user)
    non_existent_collection_id = uuid4()

    response = client.get(
        f'/collections/{non_existent_collection_id}/items/',
        headers={'Authorization': f'Bearer {token}'},
    )

    assert response.status_code == HTTPStatus.NOT_FOUND
    assert response.json()['detail'] == 'Collection not found.'

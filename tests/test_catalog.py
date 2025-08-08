import io
import uuid
from http import HTTPStatus

import pytest

from vitrine.schemas import (
    AssetSituation,
    CatalogPublic,
    ConservationStatus,
    WorkFlowStatus,
)


@pytest.mark.asyncio
async def test_create_catalog_entry(
    client, create_user, create_token, create_asset
):
    user = await create_user()
    asset = await create_asset()
    token = create_token(user)

    payload = {
        'asset_id': str(asset.id),
        'situation': AssetSituation.NORMAL.value,
        'conservation_status': ConservationStatus.GOOD.value,
        'description': 'Item em perfeito estado.',
    }

    response = client.post(
        '/catalog',
        json=payload,
        headers={'Authorization': f'Bearer {token}'},
    )

    assert response.status_code == HTTPStatus.CREATED
    data = response.json()
    assert data['asset']['id'] == str(asset.id)
    assert uuid.UUID(data['id'])


@pytest.mark.asyncio
async def test_read_catalog_entries(client, create_catalog_entry):
    entry = await create_catalog_entry()
    entry_schema = CatalogPublic.model_validate(entry).model_dump(mode='json')

    response = client.get('/catalog')

    assert response.status_code == HTTPStatus.OK
    assert response.json() == {'catalog_entries': [entry_schema]}


@pytest.mark.asyncio
async def test_update_catalog_entry(
    client, create_user, create_asset, create_catalog_entry, create_token
):
    owner_user = await create_user()
    asset = await create_asset()

    entry = await create_catalog_entry(
        user_id=owner_user.id, asset_id=asset.id
    )

    token = create_token(owner_user)

    update_payload = {
        'asset_id': str(entry.asset_id),
        'situation': AssetSituation.MOVED.value,
        'conservation_status': ConservationStatus.IDLE.value,
        'description': 'Item foi movimentado e está ocioso.',
    }

    response = client.put(
        f'/catalog/{entry.id}',
        json=update_payload,
        headers={'Authorization': f'Bearer {token}'},
    )

    assert response.status_code == HTTPStatus.OK
    data = response.json()
    assert data['id'] == str(entry.id)
    assert data['situation'] == AssetSituation.MOVED.value


@pytest.mark.asyncio
async def test_delete_catalog_entry(
    client, create_user, create_catalog_entry, create_token
):
    owner_user = await create_user()
    entry = await create_catalog_entry(user_id=owner_user.id)

    action_user = await create_user()
    token = create_token(action_user)

    response = client.delete(
        f'/catalog/{entry.id}',
        headers={'Authorization': f'Bearer {token}'},
    )

    assert response.status_code == HTTPStatus.OK
    assert response.json() == {'message': 'Catalog entry deactivated'}


@pytest.mark.asyncio
async def test_create_catalog_entry_also_creates_workflow(
    client, create_user, create_token, create_asset, create_workflow_step
):
    user = await create_user()
    asset = await create_asset()
    payload = {
        'asset_id': str(asset.id),
        'situation': AssetSituation.NORMAL.value,
        'conservation_status': ConservationStatus.GOOD.value,
        'description': 'Item novo, workflow iniciado.',
    }

    response = client.post(
        '/catalog',
        json=payload,
        headers={'Authorization': f'Bearer {create_token(user)}'},
    )

    assert response.status_code == HTTPStatus.CREATED
    data = response.json()
    assert 'workflow_history' in data
    assert len(data['workflow_history']) == 1

    workflow_step = data['workflow_history'][0]
    assert workflow_step['workflow_status'] == WorkFlowStatus.STARTED.value
    assert uuid.UUID(workflow_step['user_id'])


@pytest.mark.asyncio
async def test_add_workflow_step(
    client, create_catalog_entry, create_user, create_token
):
    user = await create_user()
    catalog_entry = await create_catalog_entry()
    catalog_id = str(catalog_entry.id)

    workflow_payload = {
        'workflow_status': WorkFlowStatus.REVIEW_REQUESTED.value,
        'detail': {'reason': 'Awaiting approval from manager.'},
    }
    response = client.post(
        f'/catalog/{catalog_id}/workflow',
        json=workflow_payload,
        headers={'Authorization': f'Bearer {create_token(user)}'},
    )

    assert response.status_code == HTTPStatus.CREATED

    data = response.json()

    assert data['workflow_status'] == WorkFlowStatus.REVIEW_REQUESTED.value
    assert data['detail']['reason'] == 'Awaiting approval from manager.'
    assert data['catalog_id'] == catalog_id


@pytest.mark.asyncio
async def test_add_workflow_step_for_nonexistent_catalog(
    client, create_user, create_token
):
    user = await create_user()
    non_existent_id = uuid.uuid4()
    workflow_payload = {
        'workflow_status': WorkFlowStatus.COMPLETED.value,
    }

    response = client.post(
        f'/catalog/{non_existent_id}/workflow',
        json=workflow_payload,
        headers={'Authorization': f'Bearer {create_token(user)}'},
    )

    assert response.status_code == HTTPStatus.NOT_FOUND
    assert response.json()['detail'] == 'Catalog entry not found'


@pytest.mark.asyncio
async def test_upload_catalog_image_and_get(
    client, create_user, create_catalog_entry, create_token
):
    owner_user = await create_user()
    entry = await create_catalog_entry(user_id=owner_user.id)

    file_content = b'fake image content'
    file_name = 'test_image.png'

    response_upload = client.post(
        f'/catalog/{entry.id}/images',
        files={'file': (file_name, io.BytesIO(file_content), 'image/png')},
        headers={'Authorization': f'Bearer {create_token(owner_user)}'},
    )

    assert response_upload.status_code == HTTPStatus.CREATED
    image_data = response_upload.json()
    assert image_data['catalog_id'] == str(entry.id)
    assert image_data['file_url'].startswith('/uploads/')

    print(client.get('/catalog/').json())

    response_get = client.get(f'/catalog/{entry.id}')
    assert response_get.status_code == HTTPStatus.OK
    catalog_data = response_get.json()
    assert len(catalog_data['images']) == 1
    assert catalog_data['images'][0]['file_url'] == image_data['file_url']


@pytest.mark.asyncio
async def test_upload_image_to_nonexistent_catalog(
    client, create_user, create_token
):
    user = await create_user()
    token = create_token(user)
    fake_id = uuid.uuid4()

    response = client.post(
        f'/catalog/{fake_id}/images',
        files={'file': ('test.png', io.BytesIO(b'abc'), 'image/png')},
        headers={'Authorization': f'Bearer {token}'},
    )

    assert response.status_code == HTTPStatus.NOT_FOUND
    assert response.json()['detail'] == 'Catalog entry not found'


@pytest.mark.asyncio
async def test_delete_catalog_image(
    client, create_user, create_catalog_entry, create_token
):
    owner_user = await create_user()
    entry = await create_catalog_entry(user_id=owner_user.id)
    token = create_token(owner_user)

    upload_resp = client.post(
        f'/catalog/{entry.id}/images',
        files={'file': ('delete_me.png', io.BytesIO(b'123'), 'image/png')},
        headers={'Authorization': f'Bearer {token}'},
    )
    assert upload_resp.status_code == HTTPStatus.CREATED
    image_id = upload_resp.json()['id']

    delete_resp = client.delete(
        f'/catalog/{entry.id}/images/{image_id}',
        headers={'Authorization': f'Bearer {token}'},
    )
    assert delete_resp.status_code == HTTPStatus.OK
    assert delete_resp.json() == {'message': 'Image deleted'}

    get_resp = client.get(f'/catalog/{entry.id}')
    assert get_resp.status_code == HTTPStatus.OK
    assert len(get_resp.json()['images']) == 0

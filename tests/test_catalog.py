import io
import uuid
from http import HTTPStatus
from uuid import uuid4

import pytest

from vitrine.schemas import (
    AssetSituation,
    CatalogPublic,
    WorkFlowStatus,
)


@pytest.mark.asyncio
async def test_create_catalog_entry(
    client, create_user, create_token, create_asset, create_location
):
    user = await create_user()
    asset = await create_asset()
    location = await create_location()
    token = create_token(user)

    payload = {
        'asset_id': str(asset.id),
        'situation': AssetSituation.UNUSED.value,
        'conservation_status': 'XPTO',
        'description': 'Item em perfeito estado.',
        'location_id': str(location.id),
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
async def test_read_catalog_entry_not_found(client):
    catalog_id = str(uuid4())
    response = client.get(f'/catalog/{catalog_id}')
    assert response.status_code == HTTPStatus.NOT_FOUND
    assert response.json() == {'detail': 'Catalog entry not found'}


@pytest.mark.asyncio
async def test_read_catalog_entries(client, create_catalog_entry):
    entry = await create_catalog_entry()
    entry_schema = CatalogPublic.model_validate(entry).model_dump(mode='json')

    response = client.get('/catalog')
    assert response.status_code == HTTPStatus.OK
    assert response.json() == {'catalog_entries': [entry_schema]}


@pytest.mark.asyncio
async def test_search_catalog_entries_by_asset_data(
    client,
    create_user,
    create_token,
    create_asset,
    create_catalog_entry,
):
    user = await create_user()
    token = create_token(user)
    auth_header = {'Authorization': f'Bearer {token}'}

    asset_notebook = await create_asset(
        asset_description='Notebook Dell de alta performance'
    )
    await create_catalog_entry(user_id=user.id, asset_id=asset_notebook.id)

    asset_chair = await create_asset(
        asset_description='Cadeira de escritório ergonômica'
    )
    await create_catalog_entry(user_id=user.id, asset_id=asset_chair.id)

    response = client.get('/catalog?q=Notebook', headers=auth_header)

    assert response.status_code == HTTPStatus.OK
    data = response.json()

    assert len(data['catalog_entries']) == 1

    returned_entry = data['catalog_entries'][0]
    assert 'Notebook Dell' in returned_entry['asset']['asset_description']
    assert returned_entry['asset']['id'] == str(asset_notebook.id)


@pytest.mark.asyncio
async def test_update_catalog_entry(
    client,
    create_user,
    create_asset,
    create_catalog_entry,
    create_token,
    create_location,
):
    owner_user = await create_user()
    asset = await create_asset()
    location = await create_location()
    entry = await create_catalog_entry(
        user_id=owner_user.id, asset_id=asset.id
    )

    token = create_token(owner_user)

    update_payload = {
        'asset_id': str(entry.asset_id),
        'situation': AssetSituation.BROKEN.value,
        'conservation_status': 'XPTO',
        'description': 'Item foi movimentado e está ocioso.',
        'location_id': str(location.id),
    }

    response = client.put(
        f'/catalog/{entry.id}',
        json=update_payload,
        headers={'Authorization': f'Bearer {token}'},
    )

    assert response.status_code == HTTPStatus.OK
    data = response.json()
    assert data['id'] == str(entry.id)
    assert data['situation'] == AssetSituation.BROKEN.value


@pytest.mark.asyncio
async def test_update_catalog_entry_not_found(
    client,
    create_user,
    create_asset,
    create_catalog_entry,
    create_token,
    create_location,
):
    owner_user = await create_user()
    asset = await create_asset()
    location = await create_location()
    entry = await create_catalog_entry(
        user_id=owner_user.id, asset_id=asset.id
    )

    token = create_token(owner_user)
    fake_id = uuid4()
    update_payload = {
        'asset_id': str(entry.asset_id),
        'situation': AssetSituation.BROKEN.value,
        'conservation_status': 'XPTO',
        'description': 'Item foi movimentado e está ocioso.',
        'location_id': str(location.id),
    }

    response = client.put(
        f'/catalog/{fake_id}',
        json=update_payload,
        headers={'Authorization': f'Bearer {token}'},
    )

    assert response.status_code == HTTPStatus.NOT_FOUND


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
async def test_delete_catalog_entry_not_found(
    client, create_user, create_catalog_entry, create_token
):
    fake_id = uuid4()
    action_user = await create_user()
    token = create_token(action_user)

    response = client.delete(
        f'/catalog/{fake_id}',
        headers={'Authorization': f'Bearer {token}'},
    )

    assert response.status_code == HTTPStatus.NOT_FOUND


@pytest.mark.asyncio
async def test_create_catalog_entry_also_creates_workflow(
    client,
    create_user,
    create_token,
    create_asset,
    create_location,
):
    user = await create_user()
    asset = await create_asset()
    location = await create_location()
    payload = {
        'asset_id': str(asset.id),
        'situation': AssetSituation.UNECONOMICAL.value,
        'conservation_status': 'XPTo',
        'description': 'Item novo, workflow iniciado.',
        'location_id': str(location.id),
    }

    response = client.post(
        '/catalog',
        json=payload,
        headers={'Authorization': f'Bearer {create_token(user)}'},
    )

    assert response.status_code == HTTPStatus.CREATED
    data = response.json()
    print(data)
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
    assert image_data['file_path'].startswith('/uploads/')

    response_get = client.get(f'/catalog/{entry.id}')
    assert response_get.status_code == HTTPStatus.OK
    catalog_data = response_get.json()
    assert len(catalog_data['images']) == 1
    assert catalog_data['images'][0]['file_path'] == image_data['file_path']


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


@pytest.mark.asyncio
async def test_catalog_entry_has_workflow_and_image(
    client,
    create_user,
    create_token,
    create_asset,
    create_location,
):
    user = await create_user()
    asset = await create_asset()
    location = await create_location()
    token = create_token(user)
    auth_headers = {'Authorization': f'Bearer {token}'}

    catalog_payload = {
        'asset_id': str(asset.id),
        'situation': 'UNECONOMICAL',
        'conservation_status': 'Bom',
        'description': 'Entrada de teste com workflow e imagem.',
        'location_id': str(location.id),
    }

    response_create = client.post(
        '/catalog', json=catalog_payload, headers=auth_headers
    )
    assert response_create.status_code == HTTPStatus.CREATED
    created_catalog_data = response_create.json()
    catalog_id = created_catalog_data['id']

    file_content = b'conteudo da imagem de teste'
    file_name = 'imagem_de_teste.png'

    response_upload = client.post(
        f'/catalog/{catalog_id}/images',
        files={'file': (file_name, io.BytesIO(file_content), 'image/jpeg')},
        headers=auth_headers,
    )
    assert response_upload.status_code == HTTPStatus.CREATED
    uploaded_image_data = response_upload.json()

    response_get = client.get(f'/catalog/{catalog_id}')
    assert response_get.status_code == HTTPStatus.OK
    final_data = response_get.json()

    assert 'workflow_history' in final_data
    assert len(final_data['workflow_history']) == 1
    workflow_step = final_data['workflow_history'][0]
    assert workflow_step['workflow_status'] == 'STARTED'
    assert workflow_step['user_id'] == str(user.id)

    assert 'images' in final_data
    assert len(final_data['images']) == 1
    image_info = final_data['images'][0]
    assert image_info['id'] == uploaded_image_data['id']
    assert image_info['file_path'] == uploaded_image_data['file_path']


@pytest.mark.asyncio
async def test_filter_catalog_by_workflow_status(
    client,
    create_user,
    create_catalog_entry,
    create_workflow_step,
):
    user = await create_user()

    entry_started = await create_catalog_entry(user_id=user.id)
    entry_review_requested = await create_catalog_entry(user_id=user.id)
    entry_completed = await create_catalog_entry(user_id=user.id)

    # Adiciona workflow inicial STARTED para cada entry
    await create_workflow_step(
        catalog_id=entry_started.id,
        user_id=user.id,
        workflow_status=WorkFlowStatus.STARTED.value,
    )

    await create_workflow_step(
        catalog_id=entry_review_requested.id,
        user_id=user.id,
        workflow_status=WorkFlowStatus.REVIEW_REQUESTED.value,
    )

    await create_workflow_step(
        catalog_id=entry_completed.id,
        user_id=user.id,
        workflow_status=WorkFlowStatus.REVIEW_REQUESTED.value,
    )
    await create_workflow_step(
        catalog_id=entry_completed.id,
        user_id=user.id,
        workflow_status=WorkFlowStatus.COMPLETED.value,
    )

    # Testa STARTED
    response_started = client.get(
        f'/catalog?workflow_status={WorkFlowStatus.STARTED.value}'
    )
    assert response_started.status_code == HTTPStatus.OK
    data_started = response_started.json()
    assert len(data_started['catalog_entries']) == 1
    assert data_started['catalog_entries'][0]['id'] == str(entry_started.id)

    # Testa REVIEW_REQUESTED
    response_review = client.get(
        f'/catalog?workflow_status={WorkFlowStatus.REVIEW_REQUESTED.value}'
    )
    assert response_review.status_code == HTTPStatus.OK
    data_review = response_review.json()
    assert len(data_review['catalog_entries']) == 1
    assert data_review['catalog_entries'][0]['id'] == str(
        entry_review_requested.id
    )

    # Testa COMPLETED
    response_completed = client.get(
        f'/catalog?workflow_status={WorkFlowStatus.COMPLETED.value}'
    )
    assert response_completed.status_code == HTTPStatus.OK
    data_completed = response_completed.json()
    assert len(data_completed['catalog_entries']) == 1
    assert data_completed['catalog_entries'][0]['id'] == str(
        entry_completed.id
    )

    # Testa workflow inexistente
    response_empty = client.get(
        f'/catalog?workflow_status={WorkFlowStatus.ADJUSTMENT_REQUESTED.value}'
    )
    assert response_empty.status_code == HTTPStatus.OK
    data_empty = response_empty.json()
    assert len(data_empty['catalog_entries']) == 0

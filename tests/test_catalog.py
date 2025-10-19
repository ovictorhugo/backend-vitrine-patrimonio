import io
import uuid
from http import HTTPStatus
from uuid import uuid4

import httpx
import pytest

from vitrine.models import (
    WorkFlowStatus,
    WorkflowTransferStatus,
)
from vitrine.schemas import (
    AssetSituation,
    CatalogPublic,
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
    assert 'workflow_history' in data
    assert len(data['workflow_history']) == 1

    workflow_step = data['workflow_history'][0]
    assert (
        workflow_step['workflow_status']
        == WorkFlowStatus.REVIEW_REQUESTED_DESFAZIMENTO.value
    )
    assert uuid.UUID(workflow_step['user']['id'])


@pytest.mark.asyncio
async def test_add_workflow_step(
    client, create_catalog_entry, create_user, create_token
):
    user = await create_user()
    catalog_entry = await create_catalog_entry()
    catalog_id = str(catalog_entry.id)

    workflow_payload = {
        'workflow_status': WorkFlowStatus.REVIEW_REQUESTED_VITRINE.value,
        'detail': {'reason': 'Awaiting approval from manager.'},
    }
    response = client.post(
        f'/catalog/{catalog_id}/workflow',
        json=workflow_payload,
        headers={'Authorization': f'Bearer {create_token(user)}'},
    )

    assert response.status_code == HTTPStatus.CREATED

    data = response.json()

    assert (
        data['workflow_status']
        == WorkFlowStatus.REVIEW_REQUESTED_VITRINE.value
    )
    assert data['detail']['reason'] == 'Awaiting approval from manager.'
    assert data['catalog_id'] == catalog_id

    response = client.get(
        f'/catalog/{catalog_id}',
        headers={'Authorization': f'Bearer {create_token(user)}'},
    )


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
    assert workflow_step['workflow_status'] == 'REVIEW_REQUESTED_DESFAZIMENTO'
    assert workflow_step['user']['id'] == str(user.id)

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

    await create_workflow_step(
        catalog_id=entry_started.id,
        user_id=user.id,
        workflow_status=WorkFlowStatus.STARTED.value,
    )

    await create_workflow_step(
        catalog_id=entry_review_requested.id,
        user_id=user.id,
        workflow_status=WorkFlowStatus.REVIEW_REQUESTED_VITRINE.value,
    )

    await create_workflow_step(
        catalog_id=entry_completed.id,
        user_id=user.id,
        workflow_status=WorkFlowStatus.REVIEW_REQUESTED_VITRINE.value,
    )
    await create_workflow_step(
        catalog_id=entry_completed.id,
        user_id=user.id,
        workflow_status=WorkFlowStatus.COMPLETED.value,
    )

    response_started = client.get(
        f'/catalog?workflow_status={WorkFlowStatus.STARTED.value}'
    )
    assert response_started.status_code == HTTPStatus.OK
    data_started = response_started.json()
    assert len(data_started['catalog_entries']) == 1
    assert data_started['catalog_entries'][0]['id'] == str(entry_started.id)

    response_review = client.get(
        f'/catalog?workflow_status={WorkFlowStatus.REVIEW_REQUESTED_VITRINE.value}'
    )
    assert response_review.status_code == HTTPStatus.OK
    data_review = response_review.json()
    assert len(data_review['catalog_entries']) == 1
    assert data_review['catalog_entries'][0]['id'] == str(
        entry_review_requested.id
    )

    response_completed = client.get(
        f'/catalog?workflow_status={WorkFlowStatus.COMPLETED.value}'
    )
    assert response_completed.status_code == HTTPStatus.OK
    data_completed = response_completed.json()
    assert len(data_completed['catalog_entries']) == 1
    assert data_completed['catalog_entries'][0]['id'] == str(
        entry_completed.id
    )

    response_empty = client.get(
        f'/catalog?workflow_status={WorkFlowStatus.ADJUSTMENT_REQUESTED.value}'
    )
    assert response_empty.status_code == HTTPStatus.OK
    data_empty = response_empty.json()
    assert len(data_empty['catalog_entries']) == 0


@pytest.mark.asyncio
async def test_list_catalog_materials(
    client, create_catalog_entry, create_asset, create_material
):
    mat1 = await create_material(material_name='Ferro')
    mat2 = await create_material(material_name='Zinco')
    mat3 = await create_material(material_name='Fosforo')

    asset1 = await create_asset(material_id=mat1.id)
    asset2 = await create_asset(material_id=mat2.id)
    asset3 = await create_asset(material_id=mat3.id)

    await create_catalog_entry(asset_id=asset1.id)
    await create_catalog_entry(asset_id=asset2.id)
    await create_catalog_entry(asset_id=asset3.id)

    response = client.get('/catalog/search/materials?q=F')
    assert response.status_code == HTTPStatus.OK
    assert len(response.json()['materials']) == 2

    response = client.get('/catalog/search/materials')
    assert response.status_code == HTTPStatus.OK
    assert len(response.json()['materials']) == 3

    response = client.get('/catalog/search/materials?q=Ferr')
    assert response.status_code == HTTPStatus.OK
    assert len(response.json()['materials']) == 1


@pytest.mark.asyncio
async def test_list_catalog_by_material(
    client, create_catalog_entry, create_asset, create_material
):
    mat1 = await create_material(material_name='Ferro')
    mat2 = await create_material(material_name='Zinco')

    asset1 = await create_asset(material_id=mat1.id)
    asset2 = await create_asset(material_id=mat2.id)
    asset3 = await create_asset(material_id=mat2.id)

    await create_catalog_entry(asset_id=asset1.id)
    await create_catalog_entry(asset_id=asset2.id)
    await create_catalog_entry(asset_id=asset3.id)

    response = client.get(f'/catalog?material_id={mat1.id}')
    assert response.status_code == HTTPStatus.OK
    assert len(response.json()['catalog_entries']) == 1

    response = client.get('/catalog')
    assert response.status_code == HTTPStatus.OK
    assert len(response.json()['catalog_entries']) == 3

    response = client.get(f'/catalog?material_id={mat2.id}')
    assert response.status_code == HTTPStatus.OK
    assert len(response.json()['catalog_entries']) == 2


@pytest.mark.asyncio
async def test_list_catalog_legal_guardians(
    client, create_catalog_entry, create_asset, create_legal_guardian
):
    guardian1 = await create_legal_guardian(legal_guardians_name='João')
    guardian2 = await create_legal_guardian(legal_guardians_name='Jozé')
    guardian3 = await create_legal_guardian(legal_guardians_name='Pedro')

    asset1 = await create_asset(legal_guardian_id=guardian1.id)
    asset2 = await create_asset(legal_guardian_id=guardian2.id)
    asset3 = await create_asset(legal_guardian_id=guardian3.id)

    await create_catalog_entry(asset_id=asset1.id)
    await create_catalog_entry(asset_id=asset2.id)
    await create_catalog_entry(asset_id=asset3.id)

    response = client.get('/catalog/search/legal_guardians?q=J')
    assert response.status_code == HTTPStatus.OK
    assert len(response.json()['legal_guardians']) == 2

    response = client.get('/catalog/search/legal_guardians')
    assert response.status_code == HTTPStatus.OK
    assert len(response.json()['legal_guardians']) == 3

    response = client.get('/catalog/search/legal_guardians?q=Joz')
    assert response.status_code == HTTPStatus.OK
    assert len(response.json()['legal_guardians']) == 1


@pytest.mark.asyncio
async def test_list_catalog_by_legal_guardian_id(
    client, create_catalog_entry, create_asset, create_legal_guardian
):
    guardian1 = await create_legal_guardian(legal_guardians_name='João')
    guardian2 = await create_legal_guardian(legal_guardians_name='Jozé')

    asset1 = await create_asset(legal_guardian_id=guardian1.id)
    asset2 = await create_asset(legal_guardian_id=guardian2.id)
    asset3 = await create_asset(legal_guardian_id=guardian2.id)

    await create_catalog_entry(asset_id=asset1.id)
    await create_catalog_entry(asset_id=asset2.id)
    await create_catalog_entry(asset_id=asset3.id)

    response = client.get(f'/catalog?legal_guardian_id={guardian2.id}')
    assert response.status_code == HTTPStatus.OK
    assert len(response.json()['catalog_entries']) == 2

    response = client.get('/catalog')
    assert response.status_code == HTTPStatus.OK
    assert len(response.json()['catalog_entries']) == 3

    response = client.get(f'/catalog?legal_guardian_id={guardian1.id}')
    assert response.status_code == HTTPStatus.OK
    assert len(response.json()['catalog_entries']) == 1


@pytest.mark.asyncio
async def test_submit_transfer_request_success(
    client, create_workflow_step, create_user, create_token, create_location
):
    workflow_step = await create_workflow_step(
        workflow_status=WorkFlowStatus.VITRINE.value, detail={}
    )
    user = await create_user()
    location = await create_location()
    payload = {'location_id': str(location.id)}
    response = client.post(
        f'/catalog/{workflow_step.catalog_id}/transfer',
        json=payload,
        headers={'Authorization': f'Bearer {create_token(user)}'},
    )
    assert response.json() == {'message': 'transfer requested successfully'}
    response = client.get(
        f'/catalog/{workflow_step.catalog_id}',
        headers={'Authorization': f'Bearer {create_token(user)}'},
    )


@pytest.mark.asyncio
async def test_transfer_request_fails_if_location_not_found(
    client, create_workflow_step, create_user, create_token
):
    workflow_step = await create_workflow_step(
        workflow_status=WorkFlowStatus.VITRINE.value, detail={}
    )
    user = await create_user()
    token = create_token(user)
    fake_location_id = str(uuid.uuid4())

    response = client.post(
        f'/catalog/{workflow_step.catalog_id}/transfer',
        json={'location_id': fake_location_id},
        headers={'Authorization': f'Bearer {token}'},
    )

    assert response.status_code == HTTPStatus.NOT_FOUND
    assert 'Location not found' in response.json()['detail']


@pytest.mark.asyncio
async def test_submit_transfer_request_persists_in_detail(
    client, create_workflow_step, create_user, create_token, create_location
):
    workflow_step = await create_workflow_step(
        workflow_status=WorkFlowStatus.VITRINE.value, detail={}
    )
    user = await create_user()
    location = await create_location()
    payload = {'location_id': str(location.id)}
    client.post(
        f'/catalog/{workflow_step.catalog_id}/transfer',
        json=payload,
        headers={'Authorization': f'Bearer {create_token(user)}'},
    )

    response = client.get(
        f'/catalog/{workflow_step.catalog_id}',
        headers={'Authorization': f'Bearer {create_token(user)}'},
    )

    data = response.json()
    workflow_entry = data['workflow_history'][0]
    assert 'transfer_requests' in workflow_entry['detail']
    transfer = workflow_entry['detail']['transfer_requests'][0]
    assert transfer['location']['id'] == str(location.id)
    assert transfer['user']['id'] == str(user.id)
    assert transfer['status'] == 'PENDING'


@pytest.mark.asyncio
async def test_multiple_users_can_request_transfer(
    client, create_workflow_step, create_user, create_token, create_location
):
    workflow_step = await create_workflow_step(
        workflow_status=WorkFlowStatus.VITRINE.value, detail={}
    )
    location = await create_location()

    user1 = await create_user()
    user2 = await create_user()

    client.post(
        f'/catalog/{workflow_step.catalog_id}/transfer',
        json={'location_id': str(location.id)},
        headers={'Authorization': f'Bearer {create_token(user1)}'},
    )
    client.post(
        f'/catalog/{workflow_step.catalog_id}/transfer',
        json={'location_id': str(location.id)},
        headers={'Authorization': f'Bearer {create_token(user2)}'},
    )

    response = client.get(
        f'/catalog/{workflow_step.catalog_id}',
        headers={'Authorization': f'Bearer {create_token(user1)}'},
    )
    transfers = response.json()['workflow_history'][0]['detail'][
        'transfer_requests'
    ]
    assert len(transfers) == 2
    assert {t['user']['id'] for t in transfers} == {
        str(user1.id),
        str(user2.id),
    }


@pytest.mark.asyncio
async def test_list_transfer_requests(
    client, create_workflow_transfer, create_user, create_token
):
    user = await create_user()
    token = create_token(user)

    pending_transfer = await create_workflow_transfer(status='PENDING')
    acceptable_transfer = await create_workflow_transfer(status='ACCEPTABLE')
    declined_transfer = await create_workflow_transfer(status='DECLINED')

    response_all = client.get(
        '/catalog/transfer', headers={'Authorization': f'Bearer {token}'}
    )
    assert response_all.status_code == HTTPStatus.OK
    data_all = response_all.json()
    assert len(data_all['transfer_requests']) == 3

    response_pending = client.get(
        '/catalog/transfer?status=PENDING',
        headers={'Authorization': f'Bearer {token}'},
    )
    assert response_pending.status_code == HTTPStatus.OK
    data_pending = response_pending.json()
    assert len(data_pending['transfer_requests']) == 1
    assert data_pending['transfer_requests'][0]['id'] == str(
        pending_transfer.id
    )

    response_acceptable = client.get(
        '/catalog/transfer?status=ACCEPTABLE',
        headers={'Authorization': f'Bearer {token}'},
    )
    assert response_acceptable.status_code == HTTPStatus.OK
    data_acceptable = response_acceptable.json()
    assert len(data_acceptable['transfer_requests']) == 1
    assert data_acceptable['transfer_requests'][0]['id'] == str(
        acceptable_transfer.id
    )

    response_declined = client.get(
        '/catalog/transfer?status=DECLINED',
        headers={'Authorization': f'Bearer {token}'},
    )
    assert response_declined.status_code == HTTPStatus.OK
    data_declined = response_declined.json()
    assert len(data_declined['transfer_requests']) == 1
    assert data_declined['transfer_requests'][0]['id'] == str(
        declined_transfer.id
    )


@pytest.mark.asyncio
async def test_update_transfer_request_status_success(
    client, create_workflow_transfer, create_user, create_token
):
    user = await create_user()
    token = create_token(user)

    transfer_request = await create_workflow_transfer(status='PENDING')

    response = client.put(
        f'/catalog/transfer/{transfer_request.id}?new_status=ACCEPTABLE',
        headers={'Authorization': f'Bearer {token}'},
    )

    assert response.status_code == HTTPStatus.OK
    updated_transfer = response.json()
    assert updated_transfer['status'] == 'ACCEPTABLE'
    assert updated_transfer['id'] == str(transfer_request.id)


@pytest.mark.asyncio
async def test_update_transfer_request_status_success_rejected_the_rest(
    client,
    create_workflow_step,
    create_workflow_transfer,
    create_user,
    create_token,
):
    user = await create_user()
    token = create_token(user)
    workflow_step = await create_workflow_step(
        workflow_status=WorkflowTransferStatus.PENDING
    )
    declined_transfer = await create_workflow_transfer(
        workflow_id=workflow_step.id, status='PENDING'
    )
    transfer_request = await create_workflow_transfer(
        workflow_id=workflow_step.id, status='PENDING'
    )

    response = client.put(
        f'/catalog/transfer/{transfer_request.id}?new_status=ACCEPTABLE',
        headers={'Authorization': f'Bearer {token}'},
    )

    assert response.status_code == HTTPStatus.OK
    updated_transfer = response.json()
    assert updated_transfer['status'] == 'ACCEPTABLE'
    assert updated_transfer['id'] == str(transfer_request.id)

    response_declined = client.get(
        '/catalog/transfer?status=DECLINED',
        headers={'Authorization': f'Bearer {token}'},
    )
    assert response_declined.status_code == HTTPStatus.OK
    data_declined = response_declined.json()
    assert len(data_declined['transfer_requests']) == 1

    response_approved = client.get(
        '/catalog/transfer?status=ACCEPTABLE',
        headers={'Authorization': f'Bearer {token}'},
    )
    assert response_approved.status_code == HTTPStatus.OK
    data_approved = response_approved.json()
    assert len(data_approved['transfer_requests']) == 1


@pytest.mark.asyncio
async def test_update_transfer_request_status_not_found(
    client, create_user, create_token
):
    user = await create_user()
    token = create_token(user)
    fake_transfer_id = str(uuid.uuid4())

    response = client.put(
        f'/catalog/transfer/{fake_transfer_id}?new_status=DECLINED',
        headers={'Authorization': f'Bearer {token}'},
    )

    assert response.status_code == HTTPStatus.NOT_FOUND
    assert 'Transfer request not found' in response.json()['detail']


@pytest.mark.asyncio
async def test_search_catalog_by_asset_identifier(
    client, create_asset, create_catalog_entry
):
    asset1 = await create_asset(asset_code='102030', asset_check_digit='1')
    asset2 = await create_asset(asset_code='102040', asset_check_digit='2')
    asset3 = await create_asset(asset_code='555666', asset_check_digit='3')

    await create_catalog_entry(asset_id=asset1.id)
    await create_catalog_entry(asset_id=asset2.id)
    catalog_with_asset3 = await create_catalog_entry(asset_id=asset3.id)

    response = client.get('/catalog/search/asset-identifier?q=1020')
    assert response.status_code == HTTPStatus.OK
    data = response.json()
    assert len(data['catalogs']) == 2

    response = client.get('/catalog/search/asset-identifier')
    assert response.status_code == HTTPStatus.OK
    data = response.json()
    assert len(data['catalogs']) == 3

    response = client.get('/catalog/search/asset-identifier?q=555')
    assert response.status_code == HTTPStatus.OK
    data = response.json()
    assert len(data['catalogs']) == 1
    assert data['catalogs'][0]['asset_identifier'] == '555666-3'
    assert data['catalogs'][0]['catalog_id'] == str(catalog_with_asset3.id)

    response = client.get('/catalog/search/asset-identifier?q=102030-1')
    assert response.status_code == HTTPStatus.OK
    assert len(response.json()['catalogs']) == 1

    response = client.get('/catalog/search/asset-identifier?q=999999')
    assert response.status_code == HTTPStatus.OK
    assert len(response.json()['catalogs']) == 0


@pytest.mark.asyncio
async def test_email_after_update_transfer_request_status_success(
    client, mailpit, create_workflow_transfer, create_user, create_token
):
    user = await create_user()
    token = create_token(user)

    transfer_request = await create_workflow_transfer(status='PENDING')

    client.put(
        f'/catalog/transfer/{transfer_request.id}?new_status=ACCEPTABLE',
        headers={'Authorization': f'Bearer {token}'},
    )

    api_url = f'http://{mailpit["host"]}:{mailpit["ui_port"]}/api/v1/messages'
    async with httpx.AsyncClient() as http_client:
        resp = await http_client.get(api_url)

    data = resp.json()
    assert data['total'] >= 1

    msgs = data['messages']
    assert any(msg['To'][0]['Address'] == user.email for msg in msgs)


@pytest.mark.asyncio
async def test_list_catalog_by_location_id(
    client, create_catalog_entry, create_asset, create_location
):
    # --- Setup ---
    # Criar duas localizações distintas
    loc1 = await create_location()
    loc2 = await create_location()

    # Criar assets em cada localização
    asset1 = await create_asset(location_id=loc1.id)
    asset2 = await create_asset(location_id=loc2.id)
    asset3 = await create_asset(location_id=loc2.id)

    # Criar entradas de catálogo para os assets
    await create_catalog_entry(asset_id=asset1.id)
    await create_catalog_entry(asset_id=asset2.id)
    await create_catalog_entry(asset_id=asset3.id)

    # --- Test ---
    # 1. Filtrar pela localização 1 (deve retornar 1)
    response_loc1 = client.get(f'/catalog?location_id={loc1.id}')
    assert response_loc1.status_code == HTTPStatus.OK
    assert len(response_loc1.json()['catalog_entries']) == 1
    assert response_loc1.json()['catalog_entries'][0]['asset']['id'] == str(
        asset1.id
    )

    # 2. Filtrar pela localização 2 (deve retornar 2)
    response_loc2 = client.get(f'/catalog?location_id={loc2.id}')
    assert response_loc2.status_code == HTTPStatus.OK
    assert len(response_loc2.json()['catalog_entries']) == 2

    # 3. Sem filtro (deve retornar 3)
    response_all = client.get('/catalog')
    assert response_all.status_code == HTTPStatus.OK
    assert len(response_all.json()['catalog_entries']) == 3

    # 4. Filtrar por um ID inexistente (deve retornar 0)
    fake_id = uuid4()
    response_fake = client.get(f'/catalog?location_id={fake_id}')
    assert response_fake.status_code == HTTPStatus.OK
    assert len(response_fake.json()['catalog_entries']) == 0


@pytest.mark.asyncio
async def test_list_catalog_by_sector_id(
    client,
    create_catalog_entry,
    create_asset,
    create_location,
    create_sector,  # Presume que esta fixture existe
):
    # --- Setup ---
    # Criar dois setores distintos
    sector1 = await create_sector()
    sector2 = await create_sector()

    # Criar localizações em cada setor
    loc1 = await create_location(sector_id=sector1.id)
    loc2 = await create_location(sector_id=sector2.id)

    # Criar assets em cada localização
    asset1 = await create_asset(location_id=loc1.id)
    asset2 = await create_asset(location_id=loc2.id)

    # Criar entradas de catálogo
    await create_catalog_entry(asset_id=asset1.id)
    await create_catalog_entry(asset_id=asset2.id)

    # --- Test ---
    # 1. Filtrar pelo setor 1 (deve retornar 1)
    response_sec1 = client.get(f'/catalog?sector_id={sector1.id}')
    assert response_sec1.status_code == HTTPStatus.OK
    assert len(response_sec1.json()['catalog_entries']) == 1
    assert response_sec1.json()['catalog_entries'][0]['asset']['id'] == str(
        asset1.id
    )

    # 2. Filtrar pelo setor 2 (deve retornar 1)
    response_sec2 = client.get(f'/catalog?sector_id={sector2.id}')
    assert response_sec2.status_code == HTTPStatus.OK
    assert len(response_sec2.json()['catalog_entries']) == 1
    assert response_sec2.json()['catalog_entries'][0]['asset']['id'] == str(
        asset2.id
    )

    # 3. Filtrar por um ID inexistente (deve retornar 0)
    fake_id = uuid4()
    response_fake = client.get(f'/catalog?sector_id={fake_id}')
    assert response_fake.status_code == HTTPStatus.OK
    assert len(response_fake.json()['catalog_entries']) == 0


@pytest.mark.asyncio
async def test_list_catalog_by_agency_id(
    client,
    create_catalog_entry,
    create_asset,
    create_location,
    create_sector,
    create_agency,
):
    agency1 = await create_agency()
    agency2 = await create_agency()

    sector1 = await create_sector(agency_id=agency1.id)
    sector2 = await create_sector(agency_id=agency2.id)

    loc1 = await create_location(sector_id=sector1.id)
    loc2 = await create_location(sector_id=sector2.id)

    asset1 = await create_asset(location_id=loc1.id)
    asset2 = await create_asset(location_id=loc2.id)

    await create_catalog_entry(asset_id=asset1.id)
    await create_catalog_entry(asset_id=asset2.id)

    response_age1 = client.get(f'/catalog?agency_id={agency1.id}')
    assert response_age1.status_code == HTTPStatus.OK
    assert len(response_age1.json()['catalog_entries']) == 1
    assert response_age1.json()['catalog_entries'][0]['asset']['id'] == str(
        asset1.id
    )

    response_age2 = client.get(f'/catalog?agency_id={agency2.id}')
    assert response_age2.status_code == HTTPStatus.OK
    assert len(response_age2.json()['catalog_entries']) == 1
    assert response_age2.json()['catalog_entries'][0]['asset']['id'] == str(
        asset2.id
    )

    fake_id = uuid4()
    response_fake = client.get(f'/catalog?agency_id={fake_id}')
    assert response_fake.status_code == HTTPStatus.OK
    assert len(response_fake.json()['catalog_entries']) == 0


@pytest.mark.asyncio
async def test_list_catalog_by_unit_id(
    client,
    create_catalog_entry,
    create_asset,
    create_location,
    create_sector,
    create_agency,
    create_unit,
):
    unit1 = await create_unit()
    unit2 = await create_unit()

    agency1 = await create_agency(unit_id=unit1.id)
    agency2 = await create_agency(unit_id=unit2.id)

    sector1 = await create_sector(agency_id=agency1.id)
    sector2 = await create_sector(agency_id=agency2.id)

    loc1 = await create_location(sector_id=sector1.id)
    loc2 = await create_location(sector_id=sector2.id)

    asset1 = await create_asset(location_id=loc1.id)
    asset2 = await create_asset(location_id=loc2.id)

    await create_catalog_entry(asset_id=asset1.id)
    await create_catalog_entry(asset_id=asset2.id)

    response_unit1 = client.get(f'/catalog?unit_id={unit1.id}')
    assert response_unit1.status_code == HTTPStatus.OK
    assert len(response_unit1.json()['catalog_entries']) == 1
    assert response_unit1.json()['catalog_entries'][0]['asset']['id'] == str(
        asset1.id
    )

    response_unit2 = client.get(f'/catalog?unit_id={unit2.id}')
    assert response_unit2.status_code == HTTPStatus.OK
    assert len(response_unit2.json()['catalog_entries']) == 1
    assert response_unit2.json()['catalog_entries'][0]['asset']['id'] == str(
        asset2.id
    )

    fake_id = uuid4()
    response_fake = client.get(f'/catalog?unit_id={fake_id}')
    assert response_fake.status_code == HTTPStatus.OK
    assert len(response_fake.json()['catalog_entries']) == 0


@pytest.mark.asyncio
async def test_filter_catalog_by_reviewer_id(
    client, create_catalog_entry, create_token, create_user
):
    user1 = await create_user()
    user2 = await create_user()
    catalog1 = await create_catalog_entry()
    await create_catalog_entry()

    role = client.post(
        '/roles/',
        json={'name': 'Comissão de desfazimento'},
    ).json()
    client.post(f'/roles/{role["id"]}/users/{user1.id}')

    workflow_payload = {
        'workflow_status': 'REVIEW_REQUESTED_COMISSION',
        'detail': {'reason': 'Awaiting approval from manager.'},
    }
    response = client.post(
        f'/catalog/{catalog1.id}/workflow',
        json=workflow_payload,
        headers={'Authorization': f'Bearer {create_token(user2)}'},
    )
    response = client.get(f'/catalog/?reviewer_id={user1.id}')
    data = response.json()
    assert len(data['catalog_entries']) == 1


@pytest.mark.asyncio
async def test_update_workflow_reviewers(
    client, create_catalog_entry, create_token, create_user
):
    user1 = await create_user()
    user2 = await create_user()
    user3 = await create_user()
    catalog1 = await create_catalog_entry()

    role = client.post(
        '/roles/',
        json={'name': 'Comissão de desfazimento'},
    ).json()
    client.post(f'/roles/{role["id"]}/users/{user1.id}')
    client.post(f'/roles/{role["id"]}/users/{user2.id}')
    client.post(f'/roles/{role["id"]}/users/{user3.id}')

    workflow_payload = {
        'workflow_status': 'REVIEW_REQUESTED_COMISSION',
        'detail': {'reason': 'Awaiting approval from manager.'},
    }
    response = client.post(
        f'/catalog/{catalog1.id}/workflow',
        json=workflow_payload,
        headers={'Authorization': f'Bearer {create_token(user1)}'},
    )
    workflow = response.json()
    workflow_id = workflow['id']

    new_reviewers = [str(user2.id), str(user3.id)]
    response = client.put(
        f'/catalog/workflow/{workflow_id}/reviewers',
        json=new_reviewers,
        headers={'Authorization': f'Bearer {create_token(user1)}'},
    )
    assert response.status_code == 200
    updated_workflow = response.json()
    assert set(updated_workflow['detail']['reviewers']) == set(new_reviewers)

    response = client.get(f'/catalog/?reviewer_id={user1.id}')
    data = response.json()
    assert len(data['catalog_entries']) == 0

    response = client.get(f'/catalog/?reviewer_id={user3.id}')
    data = response.json()
    assert len(data['catalog_entries']) == 1

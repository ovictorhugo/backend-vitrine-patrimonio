# testes/test_catalog.py

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

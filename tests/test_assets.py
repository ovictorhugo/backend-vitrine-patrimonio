import uuid
from http import HTTPStatus
from uuid import uuid4

import pytest

from vitrine.schemas import AssetPublic, AssetSchema


@pytest.mark.asyncio
async def test_create_asset(
    client,
    create_user,
    create_token,
    create_agency,
    create_sector,
    create_unit,
    create_location,
    create_material,
    create_legal_guardian,
):
    agency = await create_agency()
    sector = await create_sector()
    unit = await create_unit()
    user = await create_user()
    location = await create_location()
    material = await create_material()
    legal_guardian = await create_legal_guardian()
    response = client.post(
        '/assets/',
        json={
            'asset_code': '1234567890',
            'asset_check_digit': 'X',
            'asset_description': 'Test Asset',
            'agency_id': str(agency.id),
            'sector_id': str(sector.id),
            'unit_id': str(unit.id),
            'location_id': str(location.id),
            'material_id': str(material.id),
            'legal_guardian_id': str(legal_guardian.id),
        },
        headers={'Authorization': f'Bearer {create_token(user)}'},
    )
    assert response.status_code == HTTPStatus.CREATED
    data = response.json()
    assert data['asset_code'] == '1234567890'
    assert data['asset_check_digit'] == 'X'
    assert 'id' in data
    assert uuid.UUID(data['id'])


@pytest.mark.asyncio
async def test_create_assets_from_file(client, create_user, create_token):
    user = await create_user()

    with open('tests/storage/asset_mock.xlsx', 'rb') as file:
        content_type = """application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"""  # noqa: E501
        files = {'file': ('asset_mock.xlsx', file, content_type)}
        response = client.post(
            '/assets/upload',
            headers={'Authorization': f'Bearer {create_token(user)}'},
            files=files,
        )
    assert response.status_code == HTTPStatus.CREATED


@pytest.mark.asyncio
async def test_read_assets_empty(client, create_user, create_token):
    user = await create_user()
    response = client.get(
        '/assets/',
        headers={'Authorization': f'Bearer {create_token(user)}'},
    )
    assert response.status_code == HTTPStatus.OK
    assert response.json() == {'assets': []}


@pytest.mark.asyncio
async def test_update_asset(client, create_asset, create_user, create_token):
    user = await create_user()
    asset = await create_asset()

    asset.asset_code = '0987654321'
    asset.asset_check_digit = 'Y'

    asset.unit_id = asset.unit.id
    asset.sector_id = asset.sector.id
    asset.agency_id = asset.agency.id

    asset_schema = AssetSchema.model_validate(asset).model_dump(mode='json')

    response = client.put(
        f'/assets/{asset.id}',
        json=asset_schema,
        headers={'Authorization': f'Bearer {create_token(user)}'},
    )

    assert response.status_code == HTTPStatus.OK
    data = response.json()
    assert data['asset_code'] == '0987654321'
    assert data['asset_check_digit'] == 'Y'
    assert data['id'] == str(asset.id)


@pytest.mark.asyncio
async def test_update_asset_not_found(
    client, create_asset, create_user, create_token
):
    user = await create_user()
    asset = await create_asset()
    asset_schema = AssetSchema.model_validate(asset).model_dump(mode='json')
    response = client.put(
        f'/assets/{uuid4()}',
        json=asset_schema,
        headers={'Authorization': f'Bearer {create_token(user)}'},
    )
    assert response.status_code == HTTPStatus.NOT_FOUND


@pytest.mark.asyncio
async def test_delete_asset(client, create_asset, create_user, create_token):
    user = await create_user()
    asset = await create_asset()
    response = client.delete(
        f'/assets/{asset.id}',
        headers={'Authorization': f'Bearer {create_token(user)}'},
    )
    assert response.status_code == HTTPStatus.OK
    assert response.json() == {'message': 'Asset deleted'}


@pytest.mark.asyncio
async def test_delete_asset_not_found(client, create_user, create_token):
    user = await create_user()
    response = client.delete(
        f'/assets/{uuid4()}',
        headers={'Authorization': f'Bearer {create_token(user)}'},
    )
    assert response.status_code == HTTPStatus.NOT_FOUND


@pytest.mark.asyncio
async def test_read_assets_with_assets(
    client, create_asset, create_user, create_token
):
    user = await create_user()
    asset = await create_asset()
    asset_schema = AssetPublic.model_validate(asset).model_dump(mode='json')
    response = client.get(
        '/assets/',
        headers={'Authorization': f'Bearer {create_token(user)}'},
    )
    assert response.json() == {'assets': [asset_schema]}

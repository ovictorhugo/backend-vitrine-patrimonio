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


@pytest.mark.asyncio
async def test_search_assets_by_description(
    client, create_user, create_token, create_asset
):
    user = await create_user()
    token = create_token(user)

    await create_asset(asset_description='Notebook Dell de alta performance')
    await create_asset(asset_description='Cadeira de escritório ergonômica')

    response = client.get(
        '/assets?q=Notebook',
        headers={'Authorization': f'Bearer {token}'},
    )

    assert response.status_code == HTTPStatus.OK
    data = response.json()
    assert len(data['assets']) == 1
    assert 'Notebook Dell' in data['assets'][0]['asset_description']


@pytest.mark.asyncio
async def test_search_assets_across_multiple_fields(
    client, create_user, create_token, create_asset
):
    user = await create_user()
    token = create_token(user)

    await create_asset(
        item_brand='Logitech', asset_description='Mouse sem fio'
    )
    await create_asset(
        serial_number='XYZ-987-ABC', asset_description='Monitor 4K'
    )
    await create_asset(
        asset_code='PAT-00123', asset_description='Teclado mecânico'
    )

    response_brand = client.get(
        '/assets?q=Logitech', headers={'Authorization': f'Bearer {token}'}
    )
    assert len(response_brand.json()['assets']) == 1
    assert response_brand.json()['assets'][0]['item_brand'] == 'Logitech'

    response_serial = client.get(
        '/assets?q=XYZ-987', headers={'Authorization': f'Bearer {token}'}
    )
    assert len(response_serial.json()['assets']) == 1
    assert (
        response_serial.json()['assets'][0]['serial_number'] == 'XYZ-987-ABC'
    )

    response_code = client.get(
        '/assets?q=PAT-00123', headers={'Authorization': f'Bearer {token}'}
    )
    assert len(response_code.json()['assets']) == 1
    assert response_code.json()['assets'][0]['asset_code'] == 'PAT-00123'


@pytest.mark.asyncio
async def test_search_assets_no_results(
    client, create_user, create_token, create_asset
):
    user = await create_user()
    token = create_token(user)
    await create_asset(asset_description='Mesa de reunião')

    response = client.get(
        '/assets?q=inexistente',
        headers={'Authorization': f'Bearer {token}'},
    )

    assert response.status_code == HTTPStatus.OK
    assert response.json() == {'assets': []}


@pytest.mark.asyncio
async def test_search_assets_with_prefix(
    client, create_user, create_token, create_asset
):
    user = await create_user()
    token = create_token(user)
    await create_asset(asset_description='Notebook Dell Vostro')

    response = client.get(
        '/assets?q=Note',
        headers={'Authorization': f'Bearer {token}'},
    )

    assert response.status_code == HTTPStatus.OK
    data = response.json()
    assert len(data['assets']) == 1
    assert 'Notebook Dell Vostro' in data['assets'][0]['asset_description']


@pytest.mark.asyncio
async def test_filter_assets_by_foreign_key(
    client, create_user, create_token, create_agency, create_asset
):
    user = await create_user()
    token = create_token(user)

    agency_a = await create_agency(agency_name='Agência A')
    agency_b = await create_agency(agency_name='Agência B')
    await create_asset(agency_id=agency_a.id)
    await create_asset(agency_id=agency_b.id)

    response = client.get(
        f'/assets?agency_id={agency_a.id}',
        headers={'Authorization': f'Bearer {token}'},
    )

    assert response.status_code == HTTPStatus.OK
    data = response.json()
    assert len(data['assets']) == 1
    assert data['assets'][0]['agency']['id'] == str(agency_a.id)


@pytest.mark.asyncio
async def test_search_assets_combined_with_fk_filter(
    client, create_user, create_token, create_agency, create_asset
):
    user = await create_user()
    token = create_token(user)

    agency_a = await create_agency(agency_name='TI - Matriz')
    agency_b = await create_agency(agency_name='RH - Filial')

    await create_asset(
        asset_description='Notebook i7',
        agency_id=agency_a.id,
    )
    await create_asset(
        asset_description='Cadeira ergonômica',
        agency_id=agency_a.id,
    )
    await create_asset(
        asset_description='Notebook i5',
        agency_id=agency_b.id,
    )

    response = client.get(
        f'/assets?q=Notebook&agency_id={agency_a.id}',
        headers={'Authorization': f'Bearer {token}'},
    )

    assert response.status_code == HTTPStatus.OK
    data = response.json()

    assert len(data['assets']) == 1
    asset_found = data['assets'][0]
    assert 'Notebook i7' in asset_found['asset_description']
    assert asset_found['agency']['id'] == str(agency_a.id)


@pytest.mark.asyncio
async def test_filter_assets_by_unit_id(
    client, create_user, create_token, create_unit, create_asset
):
    user = await create_user()
    token = create_token(user)

    unit_A = await create_unit(unit_name='Unidade de TI')
    unit_B = await create_unit(unit_name='Unidade Administrativa')

    await create_asset(asset_description='Servidor Blade', unit_id=unit_A.id)
    await create_asset(
        asset_description='Projetor Multimídia', unit_id=unit_B.id
    )

    response = client.get(
        f'/assets?unit_id={unit_A.id}',
        headers={'Authorization': f'Bearer {token}'},
    )

    assert response.status_code == HTTPStatus.OK
    data = response.json()
    assert len(data['assets']) == 1
    assert data['assets'][0]['unit']['id'] == str(unit_A.id)
    assert data['assets'][0]['asset_description'] == 'Servidor Blade'


@pytest.mark.asyncio
async def test_filter_assets_by_sector_id(
    client, create_user, create_token, create_sector, create_asset
):
    user = await create_user()
    token = create_token(user)

    sector_A = await create_sector(sector_name='Setor de Redes')
    sector_B = await create_sector(sector_name='Setor de Compras')

    await create_asset(
        asset_description='Switch 24 Portas', sector_id=sector_A.id
    )
    await create_asset(
        asset_description='Arquivo de Aço', sector_id=sector_B.id
    )

    response = client.get(
        f'/assets?sector_id={sector_A.id}',
        headers={'Authorization': f'Bearer {token}'},
    )

    assert response.status_code == HTTPStatus.OK
    data = response.json()
    assert len(data['assets']) == 1
    assert data['assets'][0]['sector']['id'] == str(sector_A.id)
    assert data['assets'][0]['asset_description'] == 'Switch 24 Portas'


@pytest.mark.asyncio
async def test_filter_assets_by_material_id(
    client, create_user, create_token, create_material, create_asset
):
    user = await create_user()
    token = create_token(user)

    material_notebook = await create_material(
        material_name='Notebook Corporativo'
    )
    material_monitor = await create_material(
        material_name='Monitor 24 polegadas'
    )

    await create_asset(
        asset_description='Notebook Dell i7', material_id=material_notebook.id
    )
    await create_asset(
        asset_description='Monitor LG Ultrawide',
        material_id=material_monitor.id,
    )

    response = client.get(
        f'/assets?material_id={material_notebook.id}',
        headers={'Authorization': f'Bearer {token}'},
    )

    assert response.status_code == HTTPStatus.OK
    data = response.json()
    assert len(data['assets']) == 1
    assert data['assets'][0]['material']['id'] == str(material_notebook.id)
    assert data['assets'][0]['asset_description'] == 'Notebook Dell i7'


@pytest.mark.asyncio
async def test_search_assets_with_asset_identifier(
    client, create_user, create_token, create_asset
):
    user = await create_user()
    token = create_token(user)

    asset_code = '987652'
    asset_check_digit = '2'
    asset_identifier_to_find = asset_code + '-' + asset_check_digit
    await create_asset(
        asset_code=asset_code,
        asset_check_digit=asset_check_digit,
    )
    await create_asset(asset_code='987123654', asset_check_digit='2')

    response = client.get(
        f'/assets?asset_identifier={asset_identifier_to_find}',
        headers={'Authorization': f'Bearer {token}'},
    )

    assert response.status_code == HTTPStatus.OK
    data = response.json()
    assert len(data['assets']) == 1
    assert data['assets'][0]['asset_code'] == asset_code
    assert data['assets'][0]['asset_check_digit'] == asset_check_digit


@pytest.mark.asyncio
async def test_search_assets_with_atm_number(
    client, create_user, create_token, create_asset
):
    user = await create_user()
    token = create_token(user)

    atm_number_to_find = 'ATM007'

    await create_asset(atm_number=atm_number_to_find)
    await create_asset(atm_number='ATM008')

    response = client.get(
        f'/assets?atm_number={atm_number_to_find}',
        headers={'Authorization': f'Bearer {token}'},
    )

    assert response.status_code == HTTPStatus.OK
    data = response.json()
    assert len(data['assets']) == 1
    assert data['assets'][0]['atm_number'] == atm_number_to_find


@pytest.mark.asyncio
async def test_search_assets_by_asset_code(
    client, create_user, create_token, create_asset
):
    user = await create_user()
    token = create_token(user)

    await create_asset(asset_code='ABC123')
    await create_asset(asset_code='ABC999')
    await create_asset(asset_code='XYZ123')

    response = client.get(
        '/assets/search/asset-code?q=ABC',
        headers={'Authorization': f'Bearer {token}'},
    )

    assert response.status_code == HTTPStatus.OK
    data = response.json()
    assert 'asset_code' in data
    assert all(code.startswith('ABC') for code in data['asset_code'])
    assert 'ABC123' in data['asset_code']
    assert 'ABC999' in data['asset_code']
    assert 'XYZ123' not in data['asset_code']


@pytest.mark.asyncio
async def test_search_assets_by_asset_check_digit(
    client, create_user, create_token, create_asset
):
    user = await create_user()
    token = create_token(user)

    await create_asset(asset_check_digit='CD001')
    await create_asset(asset_check_digit='CD002')
    await create_asset(asset_check_digit='ZZ001')

    response = client.get(
        '/assets/search/asset-check-digit?q=CD',
        headers={'Authorization': f'Bearer {token}'},
    )

    assert response.status_code == HTTPStatus.OK
    data = response.json()
    assert 'asset_check_digit' in data
    assert all(cd.startswith('CD') for cd in data['asset_check_digit'])
    assert 'CD001' in data['asset_check_digit']
    assert 'CD002' in data['asset_check_digit']
    assert 'ZZ001' not in data['asset_check_digit']


@pytest.mark.asyncio
async def test_search_assets_by_atm_number(
    client, create_user, create_token, create_asset
):
    user = await create_user()
    token = create_token(user)

    await create_asset(atm_number='ATM100')
    await create_asset(atm_number='ATM101')
    await create_asset(atm_number='BANK001')

    response = client.get(
        '/assets/search/atm-number?q=ATM',
        headers={'Authorization': f'Bearer {token}'},
    )

    assert response.status_code == HTTPStatus.OK
    data = response.json()
    assert 'atm_number' in data
    assert all(num.startswith('ATM') for num in data['atm_number'])
    assert 'ATM100' in data['atm_number']
    assert 'ATM101' in data['atm_number']
    assert 'BANK001' not in data['atm_number']

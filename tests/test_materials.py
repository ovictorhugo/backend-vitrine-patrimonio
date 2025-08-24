import uuid
from http import HTTPStatus

import pytest

from vitrine.schemas import MaterialPublic


@pytest.mark.asyncio
async def test_create_material(client, create_user, create_token):
    user = await create_user()
    token = create_token(user)

    response = client.post(
        '/materials',
        json={
            'material_name': 'Cadeira de Escritório',
            'material_code': 'CAD-01',
        },
        headers={'Authorization': f'Bearer {token}'},
    )

    assert response.status_code == HTTPStatus.CREATED
    data = response.json()
    assert data['material_name'] == 'Cadeira de Escritório'
    assert data['material_code'] == 'CAD-01'
    assert 'id' in data
    assert uuid.UUID(data['id'])


@pytest.mark.asyncio
async def test_read_materials_empty(client):
    response = client.get('/materials')

    assert response.status_code == HTTPStatus.OK
    assert response.json() == {'materials': []}


@pytest.mark.asyncio
async def test_read_materials_with_material(client, create_material):
    material = await create_material()
    material_schema = MaterialPublic.model_validate(material).model_dump(
        mode='json'
    )

    response = client.get('/materials')

    assert response.status_code == HTTPStatus.OK
    assert response.json() == {'materials': [material_schema]}


@pytest.mark.asyncio
async def test_delete_material(
    client, create_material, create_user, create_token
):
    user = await create_user()
    token = create_token(user)
    material = await create_material()

    response = client.delete(
        f'/materials/{material.id}',
        headers={'Authorization': f'Bearer {token}'},
    )

    assert response.status_code == HTTPStatus.OK
    assert response.json() == {'message': 'Material deactivated successfully'}

    response_get = client.get('/materials')
    assert response_get.json() == {'materials': []}


@pytest.mark.asyncio
async def test_read_materials_with_text_search(client, create_material):
    await create_material(
        material_name='Parafuso Sextavado', material_code='PAR-SEX-01'
    )
    await create_material(
        material_name='Tinta Branca', material_code='TIN-BRA-02'
    )
    await create_material(
        material_name='Serra Circular', material_code='SER-CIR-03'
    )

    response = client.get('/materials')
    assert len(response.json()['materials']) == 3

    response = client.get('/materials?q=Sextavado')
    assert response.status_code == HTTPStatus.OK
    materials = response.json()['materials']
    assert len(materials) == 1
    assert materials[0]['material_name'] == 'Parafuso Sextavado'

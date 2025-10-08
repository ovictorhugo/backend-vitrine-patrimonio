import uuid
from http import HTTPStatus

import pytest

from vitrine.schemas import LegalGuardianPublic


@pytest.mark.asyncio
async def test_create_legal_guardian(client, create_user, create_token):
    user = await create_user()
    token = create_token(user)

    response = client.post(
        '/legal-guardians',
        json={
            'legal_guardians_name': 'João da Silva',
            'legal_guardians_code': 'JS001',
        },
        headers={'Authorization': f'Bearer {token}'},
    )

    assert response.status_code == HTTPStatus.CREATED
    data = response.json()
    assert data['legal_guardians_name'] == 'João da Silva'
    assert data['legal_guardians_code'] == 'JS001'
    assert uuid.UUID(data['id'])


@pytest.mark.asyncio
async def test_read_legal_guardians_empty(client):
    response = client.get('/legal-guardians')

    assert response.status_code == HTTPStatus.OK
    assert response.json() == {'legal_guardians': []}


@pytest.mark.asyncio
async def test_read_legal_guardians_with_guardian(
    client, create_legal_guardian
):
    guardian = await create_legal_guardian()
    guardian_schema = LegalGuardianPublic.model_validate(guardian).model_dump(
        mode='json'
    )

    response = client.get('/legal-guardians')

    assert response.status_code == HTTPStatus.OK
    assert response.json() == {'legal_guardians': [guardian_schema]}


@pytest.mark.asyncio
async def test_delete_legal_guardian(
    client, create_legal_guardian, create_user, create_token
):
    user = await create_user()
    token = create_token(user)
    guardian = await create_legal_guardian()

    response = client.delete(
        f'/legal-guardians/{guardian.id}',
        headers={'Authorization': f'Bearer {token}'},
    )

    assert response.status_code == HTTPStatus.OK
    assert response.json() == {
        'message': 'Legal guardian deactivated successfully'
    }

    response_get = client.get('/legal-guardians')
    assert response_get.json() == {'legal_guardians': []}


@pytest.mark.asyncio
async def test_read_legal_guardians_with_text_search(
    client, create_legal_guardian
):
    await create_legal_guardian(
        legal_guardians_name='João da Silva', legal_guardians_code='JS001'
    )
    await create_legal_guardian(
        legal_guardians_name='Maria Oliveira', legal_guardians_code='MO002'
    )
    await create_legal_guardian(
        legal_guardians_name='Carlos Pereira', legal_guardians_code='CP003'
    )

    response = client.get('/legal-guardians')
    assert len(response.json()['legal_guardians']) == 3

    response = client.get('/legal-guardians?q=Maria')
    assert response.status_code == HTTPStatus.OK
    legal_guardians = response.json()['legal_guardians']
    assert len(legal_guardians) == 1
    assert legal_guardians[0]['legal_guardians_name'] == 'Maria Oliveira'

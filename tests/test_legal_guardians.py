# Em tests/test_legal_guardians.py

import uuid
from http import HTTPStatus

import pytest

from vitrine.schemas import LegalGuardianPublic


@pytest.mark.asyncio
async def test_create_legal_guardian(client, create_user, create_token):
    """Testa a criação de um novo responsável legal com sucesso."""
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
    assert 'id' in data and uuid.UUID(data['id'])


@pytest.mark.asyncio
async def test_read_legal_guardians_empty(client):
    """Testa a listagem quando não há responsáveis legais cadastrados."""
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
    """Testa a desativação (soft delete) de um responsável legal."""
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

    # Verifica se o responsável foi realmente "deletado" (soft delete)
    response_get = client.get('/legal-guardians')
    assert response_get.json() == {'legal_guardians': []}

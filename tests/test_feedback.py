import uuid
from http import HTTPStatus
from uuid import uuid4

import pytest

from vitrine.models import Feedback


@pytest.mark.asyncio
async def test_create_feedback_public(client):
    payload = {
        'name': 'Jane Doe',
        'email': 'jane@doe.com',
        'rating': 10,
        'description': 'Excelente plataforma!',
    }
    response = client.post('/feedback/', json=payload)
    assert response.status_code == HTTPStatus.CREATED
    data = response.json()
    assert data['name'] == 'Jane Doe'
    assert data['rating'] == 10
    assert data['description'] == 'Excelente plataforma!'
    assert uuid.UUID(data['id'])


@pytest.mark.asyncio
@pytest.mark.parametrize(
    'field, value, error_type',
    [
        ('rating', 11, 'greater_than_equal'),
        ('rating', -1, 'less_than_equal'),
        ('email', 'not-an-email', 'value_error'),
        ('name', '', 'string_too_short'),
    ],
)
async def test_create_feedback_validation_error(
    client, field, value, error_type
):
    payload = {
        'name': 'Valid Name',
        'email': 'valid@email.com',
        'rating': 5,
        'description': 'Valid description',
    }
    payload[field] = value

    response = client.post('/feedback/', json=payload)

    assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY


@pytest.mark.asyncio
async def test_list_feedbacks_protected(client):
    response = client.get('/feedback/')
    assert response.status_code == HTTPStatus.UNAUTHORIZED


@pytest.mark.asyncio
async def test_list_feedbacks_as_admin(
    client, create_feedback, create_user, create_token
):
    admin_user = await create_user(username='admin', email='admin@test.com')
    token = create_token(admin_user)

    await create_feedback(rating=1)
    await create_feedback(rating=8)

    response = client.get(
        '/feedback/', headers={'Authorization': f'Bearer {token}'}
    )
    assert response.status_code == HTTPStatus.OK
    data = response.json()
    assert len(data['feedbacks']) == 2


@pytest.mark.asyncio
async def test_get_feedback_by_id_as_admin(
    client, create_feedback, create_user, create_token
):
    """Testa GET /feedback/{id} com autenticação."""
    admin_user = await create_user(username='admin', email='admin@test.com')
    token = create_token(admin_user)

    feedback = await create_feedback(description='Detalhe específico')

    response = client.get(
        f'/feedback/{feedback.id}',
        headers={'Authorization': f'Bearer {token}'},
    )

    assert response.status_code == HTTPStatus.OK
    data = response.json()
    assert data['description'] == 'Detalhe específico'
    assert data['id'] == str(feedback.id)


@pytest.mark.asyncio
async def test_get_feedback_not_found(client, create_user, create_token):
    """Testa GET /feedback/{id} para um ID inexistente."""
    admin_user = await create_user(username='admin', email='admin@test.com')
    token = create_token(admin_user)

    response = client.get(
        f'/feedback/{uuid4()}', headers={'Authorization': f'Bearer {token}'}
    )

    assert response.status_code == HTTPStatus.NOT_FOUND
    assert response.json() == {'detail': 'Feedback not found'}


@pytest.mark.asyncio
async def test_update_feedback_as_admin(
    client, create_feedback, create_user, create_token
):
    """Testa PATCH /feedback/{id} com autenticação."""
    admin_user = await create_user(username='admin', email='admin@test.com')
    token = create_token(admin_user)

    feedback = await create_feedback(rating=5)

    response = client.patch(
        f'/feedback/{feedback.id}',
        headers={'Authorization': f'Bearer {token}'},
        json={'rating': 10, 'description': 'Atualizado'},
    )

    assert response.status_code == HTTPStatus.OK
    data = response.json()
    assert data['rating'] == 10
    assert data['description'] == 'Atualizado'
    assert data['name'] == 'Test User'


@pytest.mark.asyncio
async def test_delete_feedback_as_admin(
    client, session, create_feedback, create_user, create_token
):
    """Testa DELETE /feedback/{id} com autenticação."""
    admin_user = await create_user(username='admin', email='admin@test.com')
    token = create_token(admin_user)

    feedback = await create_feedback()

    response = client.delete(
        f'/feedback/{feedback.id}',
        headers={'Authorization': f'Bearer {token}'},
    )

    assert response.status_code == HTTPStatus.OK
    assert response.json() == {'message': 'Feedback deleted'}

    db_feedback = await session.get(Feedback, feedback.id)
    assert db_feedback is not None
    assert db_feedback.deleted_at is not None

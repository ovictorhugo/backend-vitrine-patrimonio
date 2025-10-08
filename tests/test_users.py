import uuid
from http import HTTPStatus
from uuid import uuid4

import pytest

from vitrine.schemas import UserPublic


def test_create_user(client):
    response = client.post(
        '/users/',
        json={
            'username': 'alice',
            'email': 'alice@example.com',
            'password': 'secret',
        },
    )
    assert response.status_code == HTTPStatus.CREATED
    data = response.json()
    assert data['username'] == 'alice'
    assert data['email'] == 'alice@example.com'
    assert uuid.UUID(data['id'])


def test_read_users(client):
    response = client.get('/users')
    assert response.status_code == HTTPStatus.OK
    assert response.json() == {'users': []}


@pytest.mark.asyncio
async def test_update_user(client, create_user, create_token):
    user = await create_user()

    response = client.put(
        f'/users/{user.id}',
        headers={'Authorization': f'Bearer {create_token(user)}'},
        json={
            'username': 'bob',
            'email': 'bob@example.com',
            'password': 'mynewpassword',
        },
    )
    assert response.status_code == HTTPStatus.OK
    data = response.json()
    assert data['username'] == 'bob'
    assert data['email'] == 'bob@example.com'
    assert data['id'] == str(user.id)


@pytest.mark.asyncio
async def test_update_integrity_error(client, create_user, create_token):
    user = await create_user()
    client.post(
        '/users',
        json={
            'username': 'fausto',
            'email': 'fausto@example.com',
            'password': 'secret',
        },
    )
    response_update = client.put(
        f'/users/{user.id}',
        headers={'Authorization': f'Bearer {create_token(user)}'},
        json={
            'username': 'fausto',
            'email': 'bob@example.com',
            'password': 'mynewpassword',
        },
    )
    assert response_update.status_code == HTTPStatus.CONFLICT
    assert response_update.json() == {'detail': 'Username already exists'}


@pytest.mark.asyncio
async def test_delete_user(client, create_user, create_token):
    user = await create_user()
    response = client.delete(
        f'/users/{user.id}',
        headers={'Authorization': f'Bearer {create_token(user)}'},
    )
    assert response.status_code == HTTPStatus.OK
    assert response.json() == {'message': 'User deactivated'}


@pytest.mark.asyncio
async def test_read_users_with_users(client, create_user):
    user = await create_user()
    user_schema = UserPublic.model_validate(user).model_dump(mode='json')
    response = client.get('/users/')
    assert response.json() == {'users': [user_schema]}


@pytest.mark.asyncio
async def test_update_user_with_wrong_user(client, create_user, create_token):
    user = await create_user()
    response = client.put(
        f'/users/{uuid4()}',
        headers={'Authorization': f'Bearer {create_token(user)}'},
        json={
            'username': 'bob',
            'email': 'bob@example.com',
            'password': 'mynewpassword',
        },
    )
    assert response.status_code == HTTPStatus.FORBIDDEN
    assert response.json() == {'detail': 'Not enough permissions'}


@pytest.mark.asyncio
async def test_delete_user_wrong_user(client, create_user, create_token):
    user = await create_user()
    other_user = await create_user()
    response = client.delete(
        f'/users/{other_user.id}',
        headers={'Authorization': f'Bearer {create_token(user)}'},
    )
    assert response.status_code == HTTPStatus.FORBIDDEN
    assert response.json() == {'detail': 'Not enough permissions'}

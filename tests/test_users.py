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
    assert response.json() == {
        'message': 'User deactivated and reviewers reassigned'
    }


@pytest.mark.asyncio
async def test_read_users_with_users(client, create_user):
    user = await create_user()
    user_schema = UserPublic.model_validate(user).model_dump(mode='json')
    response = client.get('/users/')
    assert response.json() == {'users': [user_schema]}


@pytest.mark.asyncio
async def test_read_users_with_system_identity(
    client, create_user, create_system_identity, create_legal_guardian
):
    user = await create_user()
    legal_guardian = await create_legal_guardian(user_id=user.id)
    await create_system_identity(user.id, legal_guardian.id)
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
    assert response.status_code == HTTPStatus.NOT_FOUND
    assert response.json() == {'detail': 'User not found'}


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


@pytest.mark.asyncio
async def test_read_users_multiple(client, create_user):
    await create_user(username='carlos', email='carlos@example.com')
    await create_user(username='daniela', email='daniela@example.com')

    response = client.get('/users/')

    assert response.status_code == HTTPStatus.OK
    data = response.json()

    assert 'users' in data
    users_list = data['users']
    assert isinstance(users_list, list)

    assert len(users_list) == 2

    for user_data in users_list:
        UserPublic.model_validate(user_data)


@pytest.mark.asyncio
async def test_remove_user_from_reviewers_after_deletion(
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
    client.post(f'/roles/{role["id"]}/users/{user2.id}')

    workflow_payload = {
        'workflow_status': 'REVIEW_REQUESTED_COMISSION',
        'detail': {'reason': 'Awaiting approval from manager.'},
    }
    client.post(
        f'/catalog/{catalog1.id}/workflow',
        json=workflow_payload,
        headers={'Authorization': f'Bearer {create_token(user2)}'},
    )
    response = client.delete(
        f'/users/{user1.id}',
        headers={'Authorization': f'Bearer {create_token(user1)}'},
    )
    response = client.get(f'/catalog/?reviewer_id={user1.id}')
    data = response.json()
    assert len(data['catalog_entries']) == 0


@pytest.mark.asyncio
async def test_read_user_happy_path(client, create_user, create_token):
    user = await create_user()
    token = create_token(user)

    response = client.get(
        f'/users/{user.id}',
        headers={'Authorization': f'Bearer {token}'},
    )

    assert response.status_code == HTTPStatus.OK
    data = response.json()
    assert data['id'] == str(user.id)
    assert data['username'] == user.username


@pytest.mark.asyncio
async def test_read_user_non_existent(client, create_user, create_token):
    user = await create_user()
    token = create_token(user)
    non_existent_uuid = uuid4()

    response = client.get(
        f'/users/{non_existent_uuid}',
        headers={'Authorization': f'Bearer {token}'},
    )

    assert response.status_code == HTTPStatus.NOT_FOUND
    assert response.json() == {'detail': 'User not found'}


@pytest.mark.asyncio
async def test_read_user_after_delete_returns_not_found(
    client, create_user, create_token
):
    user = await create_user()
    token = create_token(user)

    delete_response = client.delete(
        f'/users/{user.id}',
        headers={'Authorization': f'Bearer {token}'},
    )
    assert delete_response.status_code == HTTPStatus.OK

    get_response = client.get(
        f'/users/{user.id}',
        headers={'Authorization': f'Bearer {token}'},
    )

    assert get_response.status_code == HTTPStatus.NOT_FOUND
    assert get_response.json() == {'detail': 'User not found'}

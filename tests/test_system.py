import uuid
from http import HTTPStatus

import pytest
import pytest_asyncio


@pytest_asyncio.fixture
async def admin_token(client, create_user, create_token):
    admin_user = await create_user(
        username='test_admin', email='admin@example.com'
    )

    role_response = client.post(
        '/roles/',
        json={'name': 'Administrador', 'description': 'Admin role'},
    )

    if role_response.status_code == HTTPStatus.CREATED:
        role_id = role_response.json()['id']
    elif role_response.status_code == HTTPStatus.CONFLICT:
        roles_list_resp = client.get('/roles/')
        roles = roles_list_resp.json()['roles']
        role_id = next(r['id'] for r in roles if r['name'] == 'Administrador')
    else:
        raise Exception(f'Falha ao criar/buscar role: {role_response.text}')

    client.post(f'/roles/{role_id}/users/{admin_user.id}')

    return create_token(admin_user)


@pytest.mark.asyncio
async def test_create_setting_as_admin(client, admin_token):
    payload = {
        'key': 'WORKFLOW_MAX_AGE_DAYS',
        'value': 30,
        'description': 'Max age in days.',
    }
    response = client.post(
        '/settings/',
        json=payload,
        headers={'Authorization': f'Bearer {admin_token}'},
    )
    assert response.status_code == HTTPStatus.CREATED
    data = response.json()
    assert data['key'] == 'WORKFLOW_MAX_AGE_DAYS'
    assert data['value'] == 30
    assert uuid.UUID(data['id'])


@pytest.mark.asyncio
async def test_create_setting_as_user(client, create_user, create_token):
    user = await create_user(username='normal_user', email='user@example.com')
    token = create_token(user)

    payload = {
        'key': 'NEW_SETTING',
        'value': 'foo',
    }
    response = client.post(
        '/settings/',
        json=payload,
        headers={'Authorization': f'Bearer {token}'},
    )
    assert response.status_code == HTTPStatus.FORBIDDEN
    assert response.json() == {'detail': 'Acesso restrito a administradores'}


@pytest.mark.asyncio
async def test_create_setting_duplicate_key(client, admin_token):
    payload = {
        'key': 'UNIQUE_KEY_TEST',
        'value': 123,
    }
    response1 = client.post(
        '/settings/',
        json=payload,
        headers={'Authorization': f'Bearer {admin_token}'},
    )
    assert response1.status_code == HTTPStatus.CREATED

    payload['value'] = 456
    response2 = client.post(
        '/settings/',
        json=payload,
        headers={'Authorization': f'Bearer {admin_token}'},
    )
    assert response2.status_code == HTTPStatus.CONFLICT
    assert response2.json() == {
        'detail': 'Uma configuração com esta chave (key) já existe'
    }


@pytest.mark.asyncio
async def test_read_settings_empty(client, create_user, create_token):
    user = await create_user(
        username='reader_user', email='reader@example.com'
    )
    token = create_token(user)

    response = client.get(
        '/settings/', headers={'Authorization': f'Bearer {token}'}
    )
    assert response.status_code == HTTPStatus.OK
    assert response.json() == {'settings': []}


@pytest.mark.asyncio
async def test_read_settings_with_data(
    client, admin_token, create_user, create_token
):
    setting_payload = {
        'key': 'READ_TEST_KEY',
        'value': 'test_value',
    }
    create_resp = client.post(
        '/settings/',
        json=setting_payload,
        headers={'Authorization': f'Bearer {admin_token}'},
    )
    assert create_resp.status_code == HTTPStatus.CREATED
    created_setting = create_resp.json()

    user = await create_user(
        username='reader_user_2', email='reader2@example.com'
    )
    token = create_token(user)

    response = client.get(
        '/settings/', headers={'Authorization': f'Bearer {token}'}
    )
    assert response.status_code == HTTPStatus.OK
    data = response.json()

    assert len(data['settings']) >= 1

    found = next(
        (s for s in data['settings'] if s['key'] == 'READ_TEST_KEY'), None
    )
    assert found is not None
    assert found['value'] == 'test_value'
    assert found['id'] == created_setting['id']


@pytest.mark.asyncio
async def test_read_settings_not_authenticated(client):
    response = client.get('/settings/')
    assert response.status_code == HTTPStatus.UNAUTHORIZED
    assert response.json() == {'detail': 'Not authenticated'}


@pytest.mark.asyncio
async def test_update_setting_as_admin(client, admin_token):
    key = 'KEY_TO_UPDATE'
    client.post(
        '/settings/',
        json={'key': key, 'value': 'initial'},
        headers={'Authorization': f'Bearer {admin_token}'},
    )

    update_payload = {
        'value': {'new_value': 'updated'},
        'description': 'Nova descrição',
    }
    response = client.put(
        f'/settings/{key}',
        json=update_payload,
        headers={'Authorization': f'Bearer {admin_token}'},
    )
    assert response.status_code == HTTPStatus.OK
    data = response.json()
    assert data['key'] == key
    assert data['value'] == {'new_value': 'updated'}
    assert data['description'] == 'Nova descrição'


@pytest.mark.asyncio
async def test_update_setting_as_user(
    client, admin_token, create_user, create_token
):
    key = 'KEY_USER_CANNOT_UPDATE'
    client.post(
        '/settings/',
        json={'key': key, 'value': 'secret'},
        headers={'Authorization': f'Bearer {admin_token}'},
    )

    user = await create_user(
        username='updater_user', email='updater@example.com'
    )
    token = create_token(user)

    response = client.put(
        f'/settings/{key}',
        json={'value': 123},
        headers={'Authorization': f'Bearer {token}'},
    )
    assert response.status_code == HTTPStatus.FORBIDDEN
    assert response.json() == {'detail': 'Acesso restrito a administradores'}


@pytest.mark.asyncio
async def test_update_setting_not_found(client, admin_token):
    response = client.put(
        '/settings/NON_EXISTENT_KEY',
        json={'value': 'test'},
        headers={'Authorization': f'Bearer {admin_token}'},
    )
    assert response.status_code == HTTPStatus.NOT_FOUND
    assert response.json() == {'detail': 'Configuração não encontrada'}


@pytest.mark.asyncio
async def test_delete_setting_as_admin(client, admin_token):
    key = 'KEY_TO_DELETE'
    client.post(
        '/settings/',
        json={'key': key, 'value': 1},
        headers={'Authorization': f'Bearer {admin_token}'},
    )

    response = client.delete(
        f'/settings/{key}',
        headers={'Authorization': f'Bearer {admin_token}'},
    )
    assert response.status_code == HTTPStatus.OK
    assert response.json() == {
        'message': 'Configuração desativada com sucesso'
    }

    list_response = client.get(
        '/settings/', headers={'Authorization': f'Bearer {admin_token}'}
    )
    assert list_response.status_code == HTTPStatus.OK

    found = next(
        (s for s in list_response.json()['settings'] if s['key'] == key), None
    )
    assert found is None


@pytest.mark.asyncio
async def test_delete_setting_as_user(
    client, admin_token, create_user, create_token
):
    key = 'KEY_USER_CANNOT_DELETE'
    client.post(
        '/settings/',
        json={'key': key, 'value': 'data'},
        headers={'Authorization': f'Bearer {admin_token}'},
    )

    user = await create_user(
        username='deleter_user', email='deleter@example.com'
    )
    token = create_token(user)

    response = client.delete(
        f'/settings/{key}',
        headers={'Authorization': f'Bearer {token}'},
    )
    assert response.status_code == HTTPStatus.FORBIDDEN
    assert response.json() == {'detail': 'Acesso restrito a administradores'}


@pytest.mark.asyncio
async def test_delete_setting_not_found(client, admin_token):
    response = client.delete(
        '/settings/NON_EXISTENT_KEY_DELETE',
        headers={'Authorization': f'Bearer {admin_token}'},
    )
    assert response.status_code == HTTPStatus.NOT_FOUND
    assert response.json() == {'detail': 'Configuração não encontrada'}


@pytest.mark.asyncio
async def test_delete_setting_already_deleted(client, admin_token):
    key = 'KEY_TO_DELETE_TWICE'
    client.post(
        '/settings/',
        json={'key': key, 'value': 1},
        headers={'Authorization': f'Bearer {admin_token}'},
    )

    response1 = client.delete(
        f'/settings/{key}',
        headers={'Authorization': f'Bearer {admin_token}'},
    )
    assert response1.status_code == HTTPStatus.OK

    response2 = client.delete(
        f'/settings/{key}',
        headers={'Authorization': f'Bearer {admin_token}'},
    )
    assert response2.status_code == HTTPStatus.NOT_FOUND
    assert response2.json() == {'detail': 'Configuração não encontrada'}

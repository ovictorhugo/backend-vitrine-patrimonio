from http import HTTPStatus
from uuid import uuid4

import pytest


def test_create_role(client):
    response = client.post(
        '/roles/',
        json={
            'name': 'Administrador',
            'description': 'Acesso total ao sistema',
        },
    )
    assert response.status_code == HTTPStatus.CREATED
    data = response.json()
    assert data['name'] == 'Administrador'
    assert data['description'] == 'Acesso total ao sistema'


def test_create_duplicate_role(client):
    client.post(
        '/roles/',
        json={'name': 'Gestor', 'description': 'Gerencia usuários'},
    )
    response = client.post(
        '/roles/',
        json={'name': 'Gestor', 'description': 'Outro texto'},
    )
    assert response.status_code == HTTPStatus.CONFLICT
    assert response.json() == {'detail': 'Role already exists'}


@pytest.mark.asyncio
async def test_read_roles_empty(client):
    response = client.get('/roles/')
    assert response.status_code == HTTPStatus.OK
    assert response.json() == {'roles': []}


@pytest.mark.asyncio
async def test_create_permission(client):
    response = client.post(
        '/roles/permissions',
        json={
            'name': 'Editar usuários',
            'code': 'edit_users',
            'description': 'Permite editar usuários',
        },
    )
    assert response.status_code == HTTPStatus.CREATED
    data = response.json()
    assert data['name'] == 'Editar usuários'
    assert data['code'] == 'edit_users'
    assert data['description'] == 'Permite editar usuários'


@pytest.mark.asyncio
async def test_add_permission_to_role(client):
    role = client.post(
        '/roles/',
        json={'name': 'Suporte', 'description': 'Atendimento ao cliente'},
    ).json()
    permission = client.post(
        '/roles/permissions',
        json={'name': 'Visualizar usuários', 'code': 'view_users'},
    ).json()

    response = client.post(
        f'/roles/{role["id"]}/permissions?permission_id={permission["id"]}'
    )
    assert response.status_code == HTTPStatus.OK
    assert response.json() == {'message': 'Permission added to role'}


@pytest.mark.asyncio
async def test_remove_permission_from_role(client):
    role = client.post('/roles/', json={'name': 'Financeiro'}).json()
    permission = client.post(
        '/roles/permissions',
        json={'name': 'Acessar relatórios', 'code': 'view_reports'},
    ).json()

    client.post(
        f'/roles/{role["id"]}/permissions?permission_id={permission["id"]}'
    )

    response = client.delete(
        f'/roles/{role["id"]}/permissions/{permission["id"]}'
    )
    assert response.status_code == HTTPStatus.OK
    assert response.json() == {'message': 'Permission removed from role'}


@pytest.mark.asyncio
async def test_assign_role_to_user(client, create_user):
    user = await create_user()
    role = client.post('/roles/', json={'name': 'Técnico'}).json()

    response = client.post(f'/roles/{role["id"]}/users/{user.id}')
    assert response.status_code == HTTPStatus.OK
    assert response.json() == {'message': 'Role assigned to user'}


@pytest.mark.asyncio
async def test_remove_role_from_user(client, create_user):
    user = await create_user()
    role = client.post('/roles/', json={'name': 'Analista'}).json()

    client.post(f'/roles/{role["id"]}/users/{user.id}')

    response = client.delete(f'/roles/{role["id"]}/users/{user.id}')
    assert response.status_code == HTTPStatus.OK
    assert response.json() == {'message': 'Role removed from user'}


@pytest.mark.asyncio
async def test_assign_nonexistent_permission(client):
    fake_role_id = uuid4()
    fake_permission_id = uuid4()

    response = client.post(
        f'/roles/{fake_role_id}/permissions?permission_id={fake_permission_id}'
    )
    assert response.status_code == HTTPStatus.NOT_FOUND
    assert response.json() == {'detail': 'Role or Permission not found'}


@pytest.mark.asyncio
async def test_assign_duplicate_permission_to_role(client):
    role = client.post('/roles/', json={'name': 'Gestão'}).json()
    permission = client.post(
        '/roles/permissions',
        json={'name': 'Criar usuário', 'code': 'create_users'},
    ).json()

    client.post(
        f'/roles/{role["id"]}/permissions?permission_id={permission["id"]}'
    )
    response = client.post(
        f'/roles/{role["id"]}/permissions?permission_id={permission["id"]}'
    )
    assert response.status_code == HTTPStatus.CONFLICT
    assert response.json() == {'detail': 'Permission already assigned to role'}

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
    assert response.json() == {
        'message': 'Role removed from user and reviewers reassigned if applicable'
    }


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


@pytest.mark.asyncio
async def test_delete_permission_success(client):
    create_response = client.post(
        '/roles/permissions',
        json={
            'name': 'Permissão a ser Deletada',
            'code': 'permission_to_delete',
            'description': 'Esta permissão será desativada',
        },
    )
    assert create_response.status_code == HTTPStatus.CREATED
    permission_id = create_response.json()['id']

    delete_response = client.delete(f'/roles/permissions/{permission_id}')

    assert delete_response.status_code == HTTPStatus.OK
    assert delete_response.json() == {'message': 'Permission deactivated'}


@pytest.mark.asyncio
async def test_delete_permission_not_found(client):
    fake_permission_id = uuid4()

    delete_response = client.delete(f'/roles/permissions/{fake_permission_id}')

    assert delete_response.status_code == HTTPStatus.NOT_FOUND
    assert delete_response.json() == {'detail': 'Permission deactivated'}


@pytest.mark.asyncio
async def test_delete_permission_twice_idempotent(client):
    create_response = client.post(
        '/roles/permissions',
        json={
            'name': 'Permissão Deleção Dupla',
            'code': 'permission_delete_twice',
            'description': 'Testando idempotência',
        },
    )
    assert create_response.status_code == HTTPStatus.CREATED
    permission_id = create_response.json()['id']

    delete_response_1 = client.delete(f'/roles/permissions/{permission_id}')

    assert delete_response_1.status_code == HTTPStatus.OK
    assert delete_response_1.json() == {'message': 'Permission deactivated'}

    delete_response_2 = client.delete(f'/roles/permissions/{permission_id}')

    assert delete_response_2.status_code == HTTPStatus.NOT_FOUND
    assert delete_response_2.json() == {'detail': 'Permission deactivated'}


@pytest.mark.asyncio
async def test_update_role_success(client):
    create_response = client.post(
        '/roles/',
        json={'name': 'Nome Antigo', 'description': 'Descrição Antiga'},
    )
    assert create_response.status_code == HTTPStatus.CREATED
    role_data = create_response.json()
    role_id = role_data['id']

    update_payload = {
        'name': 'Nome Atualizado',
        'description': 'Descrição Atualizada',
    }

    response = client.put(
        f'/roles/{role_id}',
        json=update_payload,
    )

    assert response.status_code == HTTPStatus.OK
    updated_data = response.json()

    assert updated_data['id'] == role_id
    assert updated_data['name'] == 'Nome Atualizado'
    assert updated_data['description'] == 'Descrição Atualizada'


@pytest.mark.asyncio
async def test_update_role_not_found(client):
    fake_role_id = uuid4()
    update_payload = {
        'name': 'Nome Inexistente',
        'description': 'Descrição Inexistente',
    }

    response = client.put(
        f'/roles/{fake_role_id}',
        json=update_payload,
    )

    assert response.status_code == HTTPStatus.NOT_FOUND
    assert response.json() == {'detail': 'Role not found'}


@pytest.mark.asyncio
async def test_update_deleted_role_not_found(client):
    create_response = client.post(
        '/roles/',
        json={'name': 'Role a ser deletado', 'description': '...'},
    )
    assert create_response.status_code == HTTPStatus.CREATED
    role_id = create_response.json()['id']

    delete_response = client.delete(f'/roles/{role_id}')

    assert delete_response.status_code in {
        HTTPStatus.OK,
        HTTPStatus.NOT_FOUND,
    }

    update_payload = {
        'name': 'Tentativa de Update',
        'description': 'Não deve funcionar',
    }
    response = client.put(
        f'/roles/{role_id}',
        json=update_payload,
    )

    assert response.status_code == HTTPStatus.NOT_FOUND
    assert response.json() == {'detail': 'Role not found'}


@pytest.mark.asyncio
async def test_read_permissions_by_role_success(client):
    role_response = client.post(
        '/roles/', json={'name': 'Auditor', 'description': 'Audita o sistema'}
    )
    assert role_response.status_code == HTTPStatus.CREATED
    role_id = role_response.json()['id']

    perm1_response = client.post(
        '/roles/permissions',
        json={'name': 'Visualizar Logs', 'code': 'view_logs'},
    )
    perm2_response = client.post(
        '/roles/permissions',
        json={'name': 'Visualizar Relatórios', 'code': 'view_reports_audit'},
    )
    perm1_id = perm1_response.json()['id']
    perm2_id = perm2_response.json()['id']

    client.post(f'/roles/{role_id}/permissions?permission_id={perm1_id}')
    client.post(f'/roles/{role_id}/permissions?permission_id={perm2_id}')

    response = client.get(f'/roles/{role_id}/permissions')

    assert response.status_code == HTTPStatus.OK
    data = response.json()
    assert len(data) == 2

    codes_in_response = {perm['code'] for perm in data}
    assert 'view_logs' in codes_in_response
    assert 'view_reports_audit' in codes_in_response


@pytest.mark.asyncio
async def test_read_permissions_by_role_empty(client):
    role_response = client.post(
        '/roles/', json={'name': 'Visitante', 'description': 'Sem permissões'}
    )
    assert role_response.status_code == HTTPStatus.CREATED
    role_id = role_response.json()['id']

    response = client.get(f'/roles/{role_id}/permissions')

    assert response.status_code == HTTPStatus.OK
    assert response.json() == []


@pytest.mark.asyncio
async def test_read_permissions_for_nonexistent_role(client):
    fake_role_id = uuid4()
    response = client.get(f'/roles/{fake_role_id}/permissions')

    assert response.status_code == HTTPStatus.OK
    assert response.json() == []


@pytest.mark.asyncio
async def test_read_permissions_by_role_skips_deleted_permissions(client):
    role_response = client.post(
        '/roles/', json={'name': 'Gerente', 'description': 'Gerencia'}
    )
    assert role_response.status_code == HTTPStatus.CREATED
    role_id = role_response.json()['id']

    perm_active_response = client.post(
        '/roles/permissions',
        json={'name': 'Permissão Ativa', 'code': 'perm_active'},
    )
    perm_deleted_response = client.post(
        '/roles/permissions',
        json={'name': 'Permissão Deletada', 'code': 'perm_deleted'},
    )
    perm_active_id = perm_active_response.json()['id']
    perm_deleted_id = perm_deleted_response.json()['id']

    client.post(f'/roles/{role_id}/permissions?permission_id={perm_active_id}')
    client.post(
        f'/roles/{role_id}/permissions?permission_id={perm_deleted_id}'
    )

    delete_resp = client.delete(f'/roles/permissions/{perm_deleted_id}')
    assert (
        delete_resp.status_code == HTTPStatus.OK
    )  # Baseado no seu teste anterior

    response = client.get(f'/roles/{role_id}/permissions')

    assert response.status_code == HTTPStatus.OK
    data = response.json()

    assert len(data) == 1
    assert data[0]['id'] == perm_active_id
    assert data[0]['code'] == 'perm_active'


@pytest.mark.asyncio
async def test_remove_comissao_role_reassigns_reviewers(
    client, create_user, create_token, create_catalog_entry
):
    user1 = await create_user()
    user2 = await create_user()
    catalog = await create_catalog_entry()

    role = client.post(
        '/roles/',
        json={'name': 'Comissão Permanente de Desfazimento'},
    ).json()
    client.post(f'/roles/{role["id"]}/users/{user1.id}')
    client.post(f'/roles/{role["id"]}/users/{user2.id}')

    workflow_payload = {
        'workflow_status': 'REVIEW_REQUESTED_COMISSION',
        'detail': {'reviewers': [str(user1.id), str(user2.id)]},
    }

    client.post(
        f'/catalog/{catalog.id}/workflow',
        json=workflow_payload,
        headers={'Authorization': f'Bearer {create_token(user2)}'},
    )

    response = client.delete(f'/roles/{role["id"]}/users/{user1.id}')
    assert response.status_code == HTTPStatus.OK
    data = response.json()
    assert 'reassigned' in data['message']

    response = client.get(f'/catalog/?reviewer_id={user1.id}')
    assert len(response.json()['catalog_entries']) == 0

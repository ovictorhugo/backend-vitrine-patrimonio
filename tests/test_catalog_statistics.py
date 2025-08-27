from http import HTTPStatus

import pytest

# Adicione WorkFlowStatus aos seus imports, caso ainda não esteja lá.
# Certifique-se que o nome do enum está correto (ex: WorkFlowStatus).
from vitrine.schemas import WorkFlowStatus


@pytest.mark.asyncio
async def test_get_catalog_count_by_workflow_status(
    client,
    create_user,
    create_token,
    create_catalog_entry,
    create_workflow_step,
):
    """
    Testa se o endpoint de estatísticas retorna a contagem correta de catálogos
    agrupados por seu status de workflow mais recente.
    """
    # 1. SETUP: Criar usuários e dados de teste
    user = await create_user()
    token = create_token(user)
    auth_header = {'Authorization': f'Bearer {token}'}

    # Cenário 1: Dois catálogos com status "PENDENTE"
    entry_pending_1 = await create_catalog_entry(user_id=user.id)
    await create_workflow_step(
        catalog_id=entry_pending_1.id,
        user_id=user.id,
        workflow_status=WorkFlowStatus.REVIEW_REQUESTED_VITRINE.value,
    )
    entry_pending_2 = await create_catalog_entry(user_id=user.id)
    await create_workflow_step(
        catalog_id=entry_pending_2.id,
        user_id=user.id,
        workflow_status=WorkFlowStatus.REVIEW_REQUESTED_VITRINE.value,
    )

    # Cenário 2: Um catálogo com status "CONCLUÍDO"
    entry_completed = await create_catalog_entry(user_id=user.id)
    await create_workflow_step(
        catalog_id=entry_completed.id,
        user_id=user.id,
        workflow_status=WorkFlowStatus.COMPLETED.value,
    )

    # Cenário 3: Um catálogo que PASSOU por "PENDENTE" mas seu status
    # MAIS RECENTE é "AJUSTE SOLICITADO". Este é o teste crucial.
    entry_needs_adjustment = await create_catalog_entry(user_id=user.id)
    await create_workflow_step(  # Status antigo (deve ser ignorado)
        catalog_id=entry_needs_adjustment.id,
        user_id=user.id,
        workflow_status=WorkFlowStatus.REVIEW_REQUESTED_VITRINE.value,
    )
    await create_workflow_step(  # Status mais recente (deve ser contado)
        catalog_id=entry_needs_adjustment.id,
        user_id=user.id,
        workflow_status=WorkFlowStatus.ADJUSTMENT_REQUESTED.value,
    )

    # 2. ACTION: Chamar o endpoint de estatísticas
    # Assumindo que o router está em /assets, conforme o código inicial
    response = client.get(
        '/statistics/catalog/count-by-workflow-status', headers=auth_header
    )

    # 3. ASSERT: Verificar os resultados
    assert response.status_code == HTTPStatus.OK
    data = response.json()

    # Converter a lista de resultados em um dicionário para facilitar as asserções
    # Ex: [{'status': 'COMPLETED', 'count': 1}] -> {'COMPLETED': 1}
    stats_map = {item['status']: item['count'] for item in data}

    # Verificações Finais
    assert (
        len(stats_map) == 3
    )  # Apenas 3 status distintos devem ser retornados

    # Deve haver 2 catálogos cujo status mais recente é "REVIEW_REQUESTED_VITRINE"
    assert stats_map[WorkFlowStatus.REVIEW_REQUESTED_VITRINE.value] == 2

    # Deve haver 1 catálogo cujo status mais recente é "COMPLETED"
    assert stats_map[WorkFlowStatus.COMPLETED.value] == 1

    # Deve haver 1 catálogo cujo status mais recente é "ADJUSTMENT_REQUESTED"
    assert stats_map[WorkFlowStatus.ADJUSTMENT_REQUESTED.value] == 1

    # Garante que nenhum outro status foi contado indevidamente
    assert WorkFlowStatus.STARTED.value not in stats_map


@pytest.mark.asyncio
async def test_get_catalog_count_by_workflow_status_empty(
    client, create_user, create_token
):
    """
    Testa se o endpoint retorna uma lista vazia quando não há catálogos no banco.
    """
    user = await create_user()
    token = create_token(user)
    auth_header = {'Authorization': f'Bearer {token}'}

    response = client.get(
        '/statistics/catalog/count-by-workflow-status', headers=auth_header
    )

    assert response.status_code == HTTPStatus.OK
    assert response.json() == []

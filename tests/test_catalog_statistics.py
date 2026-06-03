from http import HTTPStatus

import pytest

from vitrine.models import WorkFlowStatus


@pytest.mark.asyncio
async def test_get_catalog_count_by_workflow_status(
    client,
    create_user,
    create_token,
    create_catalog_entry,
    create_workflow_step,
):
    user = await create_user()
    token = create_token(user)
    auth_header = {'Authorization': f'Bearer {token}'}

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

    entry_completed = await create_catalog_entry(user_id=user.id)
    await create_workflow_step(
        catalog_id=entry_completed.id,
        user_id=user.id,
        workflow_status=WorkFlowStatus.COMPLETED.value,
    )
    entry_needs_adjustment = await create_catalog_entry(user_id=user.id)
    await create_workflow_step(
        catalog_id=entry_needs_adjustment.id,
        user_id=user.id,
        workflow_status=WorkFlowStatus.REVIEW_REQUESTED_VITRINE.value,
    )
    await create_workflow_step(
        catalog_id=entry_needs_adjustment.id,
        user_id=user.id,
        workflow_status=WorkFlowStatus.ADJUSTMENT_REQUESTED.value,
    )

    response = client.get(
        '/statistics/catalog/count-by-workflow-status', headers=auth_header
    )

    assert response.status_code == HTTPStatus.OK
    data = response.json()

    stats_map = {item['status']: item['count'] for item in data}

    assert len(stats_map) == 3
    assert stats_map[WorkFlowStatus.REVIEW_REQUESTED_VITRINE.value] == 2

    assert stats_map[WorkFlowStatus.COMPLETED.value] == 1

    assert stats_map[WorkFlowStatus.ADJUSTMENT_REQUESTED.value] == 1

    assert WorkFlowStatus.STARTED.value not in stats_map


@pytest.mark.asyncio
async def test_get_catalog_count_by_workflow_status_empty(
    client, create_user, create_token
):
    user = await create_user()
    token = create_token(user)
    auth_header = {'Authorization': f'Bearer {token}'}

    response = client.get(
        '/statistics/catalog/count-by-workflow-status', headers=auth_header
    )

    assert response.status_code == HTTPStatus.OK
    assert response.json() == []

from http import HTTPStatus
from uuid import UUID

from fastapi import APIRouter, HTTPException
from sqlalchemy import desc, func, select
from sqlalchemy.orm import joinedload
from sqlalchemy.orm.attributes import flag_modified

from vitrine.core.dependencies import CurrentUser, Session
from vitrine.models import (
    Catalog,
    CatalogWorkFlow,
    Role,
    SystemIdentity,
    SystemSetting,
    User,
)
from vitrine.schemas import (
    CatalogWorkFlowPublic,
    CatalogWorkFlowSchema,
)

router = APIRouter(prefix='/catalog', tags=['Vitrine - Workflow de Anúncios'])


@router.post(
    '/{catalog_id}/workflow',
    status_code=HTTPStatus.CREATED,
    response_model=CatalogWorkFlowPublic,
)
async def add_workflow_step(
    catalog_id: UUID,
    workflow_data: CatalogWorkFlowSchema,
    session: Session,
    current_user: CurrentUser,
):
    if workflow_data.detail and 'reviewers' in workflow_data.detail:
        raise HTTPException(
            status_code=HTTPStatus.FORBIDDEN,
            detail="The 'reviewers' field cannot be set manually.",
        )

    db_catalog = await session.get(Catalog, catalog_id)
    if not db_catalog:
        raise HTTPException(
            status_code=HTTPStatus.NOT_FOUND, detail='Catalog entry not found'
        )

    if workflow_data.workflow_status == 'REVIEW_REQUESTED_COMISSION':
        try:
            stmt = select(SystemSetting.value).where(
                SystemSetting.key == 'COMISSION_SAMPLE_SIZE'
            )
            sample_size = await session.scalar(stmt)
            comission_sample_size = (
                int(sample_size) if sample_size is not None else 5
            )
        except Exception:
            comission_sample_size = 5

        stmt = (
            select(User)
            .where(
                User.roles.any(
                    Role.name == 'Comissão Permanente de Desfazimento'
                )
            )
            .order_by(func.random())
            .limit(comission_sample_size)
        )

        result = await session.execute(stmt)
        random_comission_users = result.scalars().all()

        if workflow_data.detail is None:
            workflow_data.detail = {}

        workflow_data.detail['reviewers'] = [
            {'id': str(user.id), 'username': user.username}
            for user in random_comission_users
        ]

    new_workflow_entry = CatalogWorkFlow(
        catalog_id=catalog_id,
        user_id=current_user.id,
        workflow_status=workflow_data.workflow_status,
        detail=workflow_data.detail,
    )

    session.add(new_workflow_entry)
    await session.commit()
    await session.refresh(
        new_workflow_entry, attribute_names=['transfer_requests']
    )

    return new_workflow_entry


@router.put(
    '/{catalog_id}/reviewers',
    response_model=CatalogWorkFlowPublic,
)
async def update_workflow_reviewers(
    catalog_id: UUID,
    new_reviewers: list[UUID],
    session: Session,
    current_user: CurrentUser,
):
    query = (
        select(CatalogWorkFlow)
        .options(
            joinedload(CatalogWorkFlow.user)
            .joinedload(User.system_identity)
            .joinedload(SystemIdentity.legal_guardian)
        )
        .where(CatalogWorkFlow.catalog_id == catalog_id)
        .order_by(desc(CatalogWorkFlow.created_at))
        .limit(1)
    )
    result = await session.execute(query)
    workflow = result.scalar_one_or_none()

    if not workflow:
        raise HTTPException(
            status_code=HTTPStatus.NOT_FOUND, detail='Workflow not found'
        )

    if workflow.workflow_status != 'REVIEW_REQUESTED_COMISSION':
        raise HTTPException(
            status_code=HTTPStatus.BAD_REQUEST,
            detail='Can only update reviewers for workflows in REVIEW_REQUESTED_COMISSION status',
        )

    stmt = select(User).where(
        User.id.in_(new_reviewers),
        User.roles.any(Role.name == 'Comissão Permanente de Desfazimento'),
        User.deleted_at.is_(None),
    )
    result = await session.execute(stmt)
    valid_reviewers = result.scalars().all()

    valid_reviewer_ids = {user.id for user in valid_reviewers}
    invalid_ids = set(new_reviewers) - valid_reviewer_ids

    if invalid_ids:
        raise HTTPException(
            status_code=HTTPStatus.BAD_REQUEST,
            detail=f'Users {list(invalid_ids)} are not valid reviewers or do not have permission',
        )

    if workflow.detail is None:
        workflow.detail = {}

    workflow.detail['reviewers'] = [
        {'id': str(user.id), 'username': user.username}
        for user in valid_reviewers
    ]

    flag_modified(workflow, 'detail')
    session.add(workflow)
    await session.commit()
    await session.refresh(workflow, attribute_names=['transfer_requests'])

    return workflow

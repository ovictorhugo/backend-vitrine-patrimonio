from http import HTTPStatus
from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import select, update
from sqlalchemy.orm import selectinload

from vitrine.core.dependencies import CurrentUser, Mail, Session
from vitrine.models import (
    Catalog,
    CatalogWorkFlow,
    Location,
    LocationInventory,
    User,
    WorkflowTransfer,
    WorkflowTransferStatus,
)
from vitrine.schemas import (
    FilterTransfer,
    Message,
    RequestTransferList,
    RequestTransferPublic,
    RequestTransferSchema,
)
from vitrine.services import mail_service

router = APIRouter(
    prefix='/catalog', tags=['Vitrine - Transferências de Anúncios']
)


@router.get(
    '/transfer',
    response_model=RequestTransferList,
    status_code=HTTPStatus.OK,
)
async def list_transfer_requests(
    session: Session,
    filters: Annotated[FilterTransfer, Depends()],
):
    query = select(WorkflowTransfer)

    if filters.status:
        query = query.where(WorkflowTransfer.status == filters.status)

    if filters.user_id:
        query = query.where(WorkflowTransfer.user_id == filters.user_id)

    if filters.workflow_id:
        query = query.where(
            WorkflowTransfer.workflow_id == filters.workflow_id
        )

    result = await session.scalars(query)
    return {'transfer_requests': result.all()}


@router.get(
    '/transfer/my',
    response_model=RequestTransferList,
    status_code=HTTPStatus.OK,
)
async def list_my_transfer_requests(
    session: Session,
    filters: Annotated[FilterTransfer, Depends()],
    current_user: CurrentUser,
):
    query = select(WorkflowTransfer)

    query = query.where(WorkflowTransfer.user_id == current_user.id)

    if filters.status:
        query = query.where(WorkflowTransfer.status == filters.status)

    if filters.user_id:
        query = query.where(WorkflowTransfer.user_id == filters.user_id)

    if filters.workflow_id:
        query = query.where(
            WorkflowTransfer.workflow_id == filters.workflow_id
        )

    result = await session.scalars(query)
    return {'transfer_requests': result.all()}


@router.post('/{catalog_id}/transfer', response_model=Message)
async def toggle_transfer_request(
    catalog_id: UUID,
    request: RequestTransferSchema,
    session: Session,
    current_user: CurrentUser,
):
    options = [
        selectinload(Catalog.images),
        selectinload(Catalog.files),
        selectinload(Catalog.location)
        .selectinload(Location.location_inventories)
        .selectinload(LocationInventory.inventory),
        selectinload(Catalog.workflow_history).options(
            selectinload(CatalogWorkFlow.user),
            selectinload(CatalogWorkFlow.transfer_requests).options(
                selectinload(WorkflowTransfer.user),
                selectinload(WorkflowTransfer.location),
            ),
        ),
    ]
    db_catalog = await session.get(Catalog, catalog_id, options=options)

    if not db_catalog or db_catalog.deleted_at:
        raise HTTPException(
            status_code=HTTPStatus.NOT_FOUND, detail='Catalog entry not found'
        )
    db_location = await session.get(Location, request.location_id)
    if not db_location or db_location.deleted_at:
        raise HTTPException(
            status_code=HTTPStatus.NOT_FOUND, detail='Location not found'
        )

    latest_workflow_entry = max(
        db_catalog.workflow_history, key=lambda wf: wf.created_at
    )

    if latest_workflow_entry.workflow_status != 'VITRINE':
        raise HTTPException(
            status_code=HTTPStatus.BAD_REQUEST,
            detail=(
                'Item not available for transfer. Current status is '
                f"'{latest_workflow_entry.workflow_status}'."
            ),
        )

    new_transfer_request = WorkflowTransfer(
        workflow_id=latest_workflow_entry.id,
        user_id=current_user.id,
        location_id=request.location_id,
        status='PENDING',
    )

    session.add(new_transfer_request)
    await session.commit()

    await session.refresh(latest_workflow_entry, ['transfer_requests'])
    transfer_list = RequestTransferList(
        transfer_requests=[
            RequestTransferPublic.model_validate(tr)
            for tr in latest_workflow_entry.transfer_requests
        ]
    )

    latest_workflow_entry.detail = transfer_list.model_dump(mode='json')

    await session.commit()
    await session.refresh(db_catalog)

    return {'message': 'transfer requested successfully'}


@router.put(
    '/transfer/{transfer_id}',
    response_model=RequestTransferPublic,
    status_code=HTTPStatus.OK,
)
async def update_transfer_status(
    transfer_id: UUID,
    new_status: WorkflowTransferStatus,
    session: Session,
    current_user: CurrentUser,
    mail: Mail,
):
    db_transfer = await session.get(
        WorkflowTransfer,
        transfer_id,
        options=[
            selectinload(WorkflowTransfer.workflow),
            selectinload(WorkflowTransfer.user),
            selectinload(WorkflowTransfer.location),
        ],
    )
    if not db_transfer:
        raise HTTPException(
            status_code=HTTPStatus.NOT_FOUND,
            detail='Transfer request not found',
        )
    if new_status == WorkflowTransferStatus.ACCEPTABLE:
        await session.execute(
            update(WorkflowTransfer)
            .where(WorkflowTransfer.workflow_id == db_transfer.workflow_id)
            .where(WorkflowTransfer.id != transfer_id)
            .values(status='DECLINED')
        )
        new_workflow_entry = CatalogWorkFlow(
            catalog_id=db_transfer.workflow.catalog_id,
            user_id=current_user.id,
            workflow_status='AGUARDANDO_ASSINATURAS',
            detail={
                'transfer_request': RequestTransferPublic.model_validate(
                    db_transfer
                ).model_dump(mode='json'),
                'asset_owner_signed': False,
                'requester_signed': False,
                'department_signed': False,
            },
        )
        catalog_owner = await session.get(User, new_workflow_entry.user_id)
        requester = await session.get(
            User, new_workflow_entry.detail['transfer_request']['user']['id']
        )

        await mail_service.send_email(mail, catalog_owner, 'Transferencia_1')
        await mail_service.send_email(mail, requester, 'Transferencia_2')

        session.add(new_workflow_entry)

    db_transfer.status = new_status
    await session.commit()
    await session.refresh(db_transfer)
    return db_transfer




@router.get(
    '/mail',
    status_code=HTTPStatus.OK,
)
async def mail_test(
    session: Session,
    mail: Mail,
):
    catalog_owner = await session.get(User, '3fa85f64-5717-4562-b3fc-2c963f66afa6')
    requester = catalog_owner
    await mail_service.send_email(mail, catalog_owner, 'Transferencia_1')
    await mail_service.send_email(mail, requester, 'Transferencia_2')

    return {"msg":"Tudo ok"}

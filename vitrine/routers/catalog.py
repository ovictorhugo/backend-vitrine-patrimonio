import os
from datetime import datetime
from http import HTTPStatus
from typing import Annotated
from uuid import UUID, uuid4

from fastapi import APIRouter, Depends, HTTPException, UploadFile
from sqlalchemy import func, select, update
from sqlalchemy.orm import selectinload

from vitrine.dependencies import CurrentUser, Mail, Session
from vitrine.models import (
    Asset,
    Catalog,
    CatalogImage,
    CatalogWorkFlow,
    LegalGuardian,
    Location,
    LocationInventory,
    Material,
    SystemIdentity,
    User,
    WorkFlowStatus,
    WorkflowTransfer,
    WorkflowTransferStatus,
)
from vitrine.schemas import (
    CatalogAssetIdentifierList,
    CatalogImagePublic,
    CatalogList,
    CatalogPublic,
    CatalogSchema,
    CatalogWorkFlowPublic,
    CatalogWorkFlowSchema,
    FilterCatalog,
    FilterSearchCatalog,
    FilterTransfer,
    LegalGuardianList,
    MaterialList,
    Message,
    RequestTransferList,
    RequestTransferPublic,
    RequestTransferSchema,
)
from vitrine.services import filter_service, mail_service

router = APIRouter(
    prefix='/catalog', tags=['vitrine - patrimônios anunciados']
)


@router.post('/', status_code=HTTPStatus.CREATED, response_model=CatalogPublic)
async def create_catalog_entry(
    catalog_data: CatalogSchema,
    session: Session,
    current_user: CurrentUser,
):
    query = select(Catalog).where(Catalog.asset_id == catalog_data.asset_id)
    db_catalog_check = await session.scalar(query)
    if db_catalog_check:
        raise HTTPException(
            status_code=HTTPStatus.CONFLICT,
            detail='Catalog entry for this asset already exists',
        )

    db_catalog = Catalog(
        asset_id=catalog_data.asset_id,
        situation=catalog_data.situation,
        conservation_status=catalog_data.conservation_status,
        description=catalog_data.description,
        location_id=catalog_data.location_id,
        user_id=current_user.id,
    )
    session.add(db_catalog)
    await session.flush()

    if catalog_data.situation in {'UNECONOMICAL', 'BROKEN'}:
        status = WorkFlowStatus.REVIEW_REQUESTED_DESFAZIMENTO
    if catalog_data.situation in {'UNUSED', 'RECOVERABLE'}:
        status = WorkFlowStatus.REVIEW_REQUESTED_VITRINE

    workflow = CatalogWorkFlow(
        catalog_id=db_catalog.id,
        user_id=current_user.id,
        workflow_status=status,
        detail={},
    )
    session.add(workflow)
    await session.commit()

    query = (
        select(Catalog)
        .where(Catalog.id == db_catalog.id)
        .options(
            selectinload(Catalog.images),
            selectinload(Catalog.workflow_history).options(
                selectinload(CatalogWorkFlow.user),
                selectinload(CatalogWorkFlow.transfer_requests),
            ),
            selectinload(Catalog.location)
            .selectinload(Location.location_inventories)
            .selectinload(LocationInventory.inventory),
        )
    )
    created_catalog = await session.scalar(query)
    return created_catalog


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
    db_catalog = await session.get(Catalog, catalog_id)
    if not db_catalog:
        raise HTTPException(
            status_code=HTTPStatus.NOT_FOUND, detail='Catalog entry not found'
        )

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


@router.get('/', response_model=CatalogList)
async def read_catalog_entries(
    session: Session, filters: Annotated[FilterCatalog, Depends()]
):
    query = select(Catalog).where(Catalog.deleted_at.is_(None))

    asset_join = filters.q or filters.material_id or filters.legal_guardian_id
    if asset_join:
        query = query.join(Catalog.asset)

    if filters.q:
        prefix_query = ' & '.join(word + ':*' for word in filters.q.split())
        ts_query = func.to_tsquery('portuguese', prefix_query)
        query = query.where(Asset.tsv.op('@@')(ts_query))

    if filters.material_id:
        query = query.where(Asset.material_id == filters.material_id)

    if filters.legal_guardian_id:
        query = query.where(
            Asset.legal_guardian_id == filters.legal_guardian_id
        )

    query = filter_service.apply_catalog_filters(query, filters)

    query = query.options(
        selectinload(Catalog.images),
        selectinload(Catalog.workflow_history).options(
            selectinload(CatalogWorkFlow.user).options(
                selectinload(User.system_identity).options(
                    selectinload(SystemIdentity.legal_guardian)
                )
            ),
            selectinload(CatalogWorkFlow.transfer_requests),
        ),
        selectinload(Catalog.location)
        .selectinload(Location.location_inventories)
        .selectinload(LocationInventory.inventory),
    )

    query = query.offset(filters.offset).limit(filters.limit)

    result = await session.scalars(query)
    entries = result.unique().all()

    return {'catalog_entries': entries}


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


@router.get('/{catalog_id}', response_model=CatalogPublic)
async def read_catalog_entry(catalog_id: UUID, session: Session):
    options = [
        selectinload(Catalog.images),
        selectinload(Catalog.workflow_history).options(
            selectinload(CatalogWorkFlow.user),
            selectinload(CatalogWorkFlow.transfer_requests),
        ),
        selectinload(Catalog.location)
        .selectinload(Location.location_inventories)
        .selectinload(LocationInventory.inventory),
    ]
    db_catalog = await session.get(Catalog, catalog_id, options=options)

    if not db_catalog or db_catalog.deleted_at:
        raise HTTPException(
            status_code=HTTPStatus.NOT_FOUND, detail='Catalog entry not found'
        )
    return db_catalog


@router.put('/{catalog_id}', response_model=CatalogPublic)
async def update_catalog_entry(
    catalog_id: UUID,
    catalog_data: CatalogSchema,
    session: Session,
    current_user: CurrentUser,
):
    options = [
        selectinload(Catalog.images),
        selectinload(Catalog.workflow_history).options(
            selectinload(CatalogWorkFlow.user),
            selectinload(CatalogWorkFlow.transfer_requests),
        ),
        selectinload(Catalog.location)
        .selectinload(Location.location_inventories)
        .selectinload(LocationInventory.inventory),
    ]
    db_catalog = await session.get(Catalog, catalog_id, options=options)

    if not db_catalog:
        raise HTTPException(
            status_code=HTTPStatus.NOT_FOUND, detail='Catalog entry not found'
        )

    db_catalog.asset_id = catalog_data.asset_id
    db_catalog.situation = catalog_data.situation
    db_catalog.conservation_status = catalog_data.conservation_status
    db_catalog.description = catalog_data.description

    await session.commit()
    await session.refresh(db_catalog)

    return db_catalog


@router.post('/{catalog_id}/transfer', response_model=Message)
async def toggle_transfer_request(
    catalog_id: UUID,
    request: RequestTransferSchema,
    session: Session,
    current_user: CurrentUser,
):
    options = [
        selectinload(Catalog.images),
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


@router.delete('/{catalog_id}', response_model=Message)
async def delete_catalog_entry(
    catalog_id: UUID,
    session: Session,
    current_user: CurrentUser,
):
    db_catalog = await session.get(Catalog, catalog_id)
    if not db_catalog:
        raise HTTPException(
            status_code=HTTPStatus.NOT_FOUND, detail='Catalog entry not found'
        )

    db_catalog.deleted_at = datetime.now()
    await session.commit()

    return {'message': 'Catalog entry deactivated'}


@router.post(
    '/{catalog_id}/images',
    status_code=HTTPStatus.CREATED,
    response_model=CatalogImagePublic,
)
async def upload_catalog_image(
    catalog_id: UUID, file: UploadFile, session: Session
):
    db_catalog = await session.get(Catalog, catalog_id)
    if not db_catalog:
        raise HTTPException(status_code=404, detail='Catalog entry not found')

    filename = f'{uuid4()}{os.path.splitext(file.filename)[1]}'
    file_path = os.path.join('vitrine/storage/uploads', filename)

    with open(file_path, 'wb') as buffer:
        buffer.write(await file.read())

    public_path = f'/uploads/{filename}'
    db_image = CatalogImage(catalog_id=catalog_id, file_path=public_path)
    session.add(db_image)
    await session.commit()
    await session.refresh(db_catalog, ['images'])
    return db_image


@router.delete('/{catalog_id}/images/{image_id}', response_model=Message)
async def delete_catalog_image(
    catalog_id: UUID,
    image_id: UUID,
    session: Session,
    current_user: CurrentUser,
):
    db_image = await session.get(CatalogImage, image_id)
    if not db_image or db_image.catalog_id != catalog_id:
        raise HTTPException(status_code=404, detail='Image not found')

    db_catalog = await session.get(Catalog, catalog_id)
    if not db_catalog:
        raise HTTPException(status_code=404, detail='Catalog entry not found')

    file_full_path = os.path.join(
        'vitrine/storage', db_image.file_path.lstrip('/')
    )
    if os.path.exists(file_full_path):
        os.remove(file_full_path)

    await session.delete(db_image)
    await session.commit()

    await session.refresh(db_catalog, ['images'])

    return {'message': 'Image deleted'}


@router.get('/search/materials', response_model=MaterialList)
async def list_catalog_materials(
    session: Session, filters: Annotated[FilterSearchCatalog, Depends()]
):
    query = (
        select(Material)
        .join_from(Catalog, Asset)
        .join(Material)
        .where(Catalog.deleted_at.is_(None))
    )

    query = filter_service.apply_catalog_filters(query, filters)

    if filters.q:
        query = query.where(
            Material.material_name.ilike(f'{filters.q}%')
        ).distinct()

    result = await session.scalars(query)
    materials = result.all()

    return {'materials': materials}


@router.get(
    '/search/legal_guardians',
    response_model=LegalGuardianList,
)
async def list_catalog_legal_guardians(
    session: Session,
    filters: Annotated[FilterSearchCatalog, Depends()],
):
    query = (
        select(LegalGuardian)
        .join_from(Catalog, Asset)
        .join(LegalGuardian)
        .where(Catalog.deleted_at.is_(None))
    )

    query = filter_service.apply_catalog_filters(query, filters)

    if filters.q:
        query = query.where(
            LegalGuardian.legal_guardians_name.ilike(f'{filters.q}%')
        )

    result = await session.scalars(query)
    legal_guardians = result.all()

    return {'legal_guardians': legal_guardians}


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

        await mail_service.send_email(mail, catalog_owner, None)
        await mail_service.send_email(mail, requester, None)

        session.add(new_workflow_entry)

    db_transfer.status = new_status
    await session.commit()
    await session.refresh(db_transfer)
    return db_transfer


@router.get(
    '/search/asset-identifier',
    response_model=CatalogAssetIdentifierList,
)
async def search_catalog_by_asset_identifier(session: Session, q: str = str()):
    q = q.replace('-', '')
    query = (
        select(
            Catalog.id.label('catalog_id'),
            func.concat(Asset.asset_code, '-', Asset.asset_check_digit).label(
                'asset_identifier'
            ),
        )
        .join(Asset, Catalog.asset_id == Asset.id)
        .where(
            Catalog.deleted_at.is_(None),
            Asset.deleted_at.is_(None),
            func.concat(Asset.asset_code, Asset.asset_check_digit).ilike(
                f'{q}%'
            ),
        )
    )
    result = await session.execute(query)
    return {'catalogs': result.mappings().all()}

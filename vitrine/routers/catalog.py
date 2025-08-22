import os
from datetime import datetime
from http import HTTPStatus
from typing import Annotated
from uuid import UUID, uuid4

from fastapi import APIRouter, Depends, HTTPException, UploadFile
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from vitrine.database import get_session
from vitrine.models import (
    Asset,
    Catalog,
    CatalogImage,
    CatalogWorkFlow,
    User,
    WorkFlowStatus,
)
from vitrine.schemas import (
    CatalogImagePublic,
    CatalogList,
    CatalogPublic,
    CatalogSchema,
    CatalogWorkFlowPublic,
    CatalogWorkFlowSchema,
    FilterCatalog,
    Message,
)
from vitrine.security import get_current_user

router = APIRouter(
    prefix='/catalog', tags=['vitrine - patrimônios anunciados']
)

Session = Annotated[AsyncSession, Depends(get_session)]
CurrentUser = Annotated[User, Depends(get_current_user)]


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

    initial_workflow = CatalogWorkFlow(
        catalog_id=db_catalog.id,
        user_id=current_user.id,
        workflow_status=WorkFlowStatus.STARTED,
        detail={'message': 'Catalog entry created and workflow started.'},
    )
    session.add(initial_workflow)
    await session.commit()

    query = (
        select(Catalog)
        .where(Catalog.id == db_catalog.id)
        .options(
            selectinload(Catalog.images),
            selectinload(Catalog.workflow_history).options(
                selectinload(CatalogWorkFlow.user)
            ),
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
    await session.refresh(new_workflow_entry)

    return new_workflow_entry


@router.get('/', response_model=CatalogList)
async def read_catalog_entries(
    session: Session, filters: Annotated[FilterCatalog, Depends()]
):
    query = select(Catalog).where(Catalog.deleted_at.is_(None))

    if filters.q:
        query = query.join(Catalog.asset)
        prefix_query = ' & '.join(word + ':*' for word in filters.q.split())
        ts_query = func.to_tsquery('portuguese', prefix_query)
        query = query.where(Asset.tsv.op('@@')(ts_query))

    if filters.workflow_status:
        latest_workflow_sq = select(
            CatalogWorkFlow.catalog_id,
            CatalogWorkFlow.workflow_status,
            func.row_number()
            .over(
                partition_by=CatalogWorkFlow.catalog_id,
                order_by=CatalogWorkFlow.created_at.desc(),
            )
            .label('rn'),
        ).subquery('latest_workflow_sq')

        query = query.join(
            latest_workflow_sq,
            Catalog.id == latest_workflow_sq.c.catalog_id,
        )

        query = query.where(
            latest_workflow_sq.c.rn == 1,
            latest_workflow_sq.c.workflow_status == filters.workflow_status,
        )

    query = query.options(
        selectinload(Catalog.images),
        selectinload(Catalog.workflow_history).options(
            selectinload(CatalogWorkFlow.user)
        ),
    )

    query = query.offset(filters.offset).limit(filters.limit)

    result = await session.scalars(query)
    entries = result.all()

    return {'catalog_entries': entries}


@router.get('/{catalog_id}', response_model=CatalogPublic)
async def read_catalog_entry(catalog_id: UUID, session: Session):
    options = [
        selectinload(Catalog.images),
        selectinload(Catalog.workflow_history).options(
            selectinload(CatalogWorkFlow.user)
        ),
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
            selectinload(CatalogWorkFlow.user)
        ),
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

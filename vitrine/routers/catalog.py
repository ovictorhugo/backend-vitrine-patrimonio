from datetime import datetime
from http import HTTPStatus
from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from vitrine.database import get_session
from vitrine.models import Catalog, CatalogWorkFlow, User, WorkFlowStatus
from vitrine.schemas import (
    CatalogList,
    CatalogPublic,
    CatalogSchema,
    CatalogWorkFlowPublic,
    CatalogWorkFlowSchema,
    FilterPage,
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
    db_catalog_check = await session.scalar(
        select(Catalog).where(Catalog.asset_id == catalog_data.asset_id)
    )
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
    await session.refresh(db_catalog)

    return db_catalog


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
    session: Session, filters: Annotated[FilterPage, Depends()]
):
    query = await session.scalars(
        select(Catalog)
        .where(Catalog.deleted_at.is_(None))
        .offset(filters.offset)
        .limit(filters.limit)
    )
    entries = query.all()
    return {'catalog_entries': entries}


@router.get('/{catalog_id}', response_model=CatalogPublic)
async def read_catalog_entry(catalog_id: UUID, session: Session):
    stmt = (
        select(Catalog)
        .options(selectinload(Catalog.workflow_history))
        .where(Catalog.id == catalog_id)
    )
    result = await session.execute(stmt)
    db_catalog = result.scalar_one_or_none()

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
    db_catalog = await session.get(Catalog, catalog_id)
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

from datetime import datetime
from http import HTTPStatus
from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import select
from sqlalchemy.orm import selectinload

from vitrine.core.dependencies import CurrentUser, Session
from vitrine.models import (
    Catalog,
    CatalogWorkFlow,
    Location,
    LocationInventory,
    Role,
    SystemIdentity,
    User,
    UserRole,
    WorkFlowStatus,
    WorkflowTransfer,
)
from vitrine.schemas import (
    CatalogList,
    CatalogPublic,
    CatalogSchema,
    FilterAsset,
    FilterCatalog,
    Message,
)
from vitrine.services import filter_service

_ASSET_FIELDS = set(FilterAsset.model_fields.keys())
_NON_JOIN_FIELDS = {'limit', 'offset'}
ASSET_JOIN_TRIGGER_FIELDS = _ASSET_FIELDS - _NON_JOIN_FIELDS


router = APIRouter(prefix='/catalog', tags=['Vitrine - Anúncios'])


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
            selectinload(Catalog.files),
            selectinload(Catalog.workflow_history).options(
                selectinload(CatalogWorkFlow.user).options(
                    selectinload(User.system_identity).options(
                        selectinload(SystemIdentity.legal_guardian)
                    ),
                    selectinload(User.user_role_associations).selectinload(
                        UserRole.role
                    ),
                ),
                selectinload(CatalogWorkFlow.transfer_requests),
            ),
            selectinload(Catalog.location)
            .selectinload(Location.location_inventories)
            .selectinload(LocationInventory.inventory),
        )
    )
    created_catalog = await session.scalar(query)
    return created_catalog


@router.get('/', response_model=CatalogList)
async def read_catalog_entries(
    session: Session, filters: Annotated[FilterCatalog, Depends()]
):
    query = select(Catalog).where(Catalog.deleted_at.is_(None))

    asset_join_needed = any(
        getattr(filters, field_name) is not None
        for field_name in ASSET_JOIN_TRIGGER_FIELDS
    )

    query = filter_service.apply_catalog_filters(query, filters)

    if asset_join_needed:
        query = query.join(Catalog.asset)
        query = filter_service.apply_asset_filters(query, filters)

    query = query.options(
        selectinload(Catalog.images),
        selectinload(Catalog.files),
        selectinload(Catalog.workflow_history).options(
            selectinload(CatalogWorkFlow.user).options(
                selectinload(User.system_identity).options(
                    selectinload(SystemIdentity.legal_guardian)
                ),
                selectinload(User.user_role_associations).selectinload(
                    UserRole.role
                ),
            ),
            selectinload(CatalogWorkFlow.transfer_requests),
        ),
        selectinload(Catalog.location)
        .selectinload(Location.location_inventories)
        .selectinload(LocationInventory.inventory),
    )

    if filters.user_id:
        query = query.where(Catalog.user_id == filters.user_id)

    if filters.role_id:
        users_with_role = (
            select(UserRole.user_id)
            .join(Role, Role.id == UserRole.role_id)
            .where(
                Role.id == filters.role_id,
                UserRole.deleted_at.is_(None),
                Role.deleted_at.is_(None),
            )
            .distinct()
            .cte('users_with_role')
        )
        query = query.join(
            users_with_role, Catalog.user_id == users_with_role.c.user_id
        )

    query = query.offset(filters.offset).limit(filters.limit)
    result = await session.scalars(query)
    entries = result.unique().all()

    return {'catalog_entries': entries}


@router.get('/{catalog_id}', response_model=CatalogPublic)
async def read_catalog_entry(catalog_id: UUID, session: Session):
    options = [
        selectinload(Catalog.images),
        selectinload(Catalog.files),
        selectinload(Catalog.workflow_history).options(
            selectinload(CatalogWorkFlow.user).options(
                selectinload(User.system_identity).options(
                    selectinload(SystemIdentity.legal_guardian)
                ),
                selectinload(User.user_role_associations).selectinload(
                    UserRole.role
                ),
            ),
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
        selectinload(Catalog.files),
        selectinload(Catalog.workflow_history).options(
            selectinload(CatalogWorkFlow.user),
            selectinload(CatalogWorkFlow.transfer_requests).options(
                selectinload(WorkflowTransfer.user),
                selectinload(WorkflowTransfer.location),
            ),
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

    db_catalog_loaded = await session.get(Catalog, catalog_id, options=options)

    if not db_catalog_loaded:
        raise HTTPException(
            status_code=HTTPStatus.NOT_FOUND,
            detail='Catalog entry not found after update',
        )

    return db_catalog_loaded


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

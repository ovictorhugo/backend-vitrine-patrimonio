from http import HTTPStatus
from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import select
from sqlalchemy.orm import joinedload, selectinload

from vitrine.dependencies import CurrentUser, Session
from vitrine.models import (
    Asset,
    Catalog,
    CatalogWorkFlow,
    FavoriteCatalog,
    Location,
    LocationInventory,
    WorkflowTransfer,
)
from vitrine.schemas import FavoriteList, FilterAsset, FilterCatalog, Message
from vitrine.services import filter_service

_ASSET_FIELDS = set(FilterAsset.model_fields.keys())
_NON_JOIN_FIELDS = {'limit', 'offset'}
ASSET_JOIN_TRIGGER_FIELDS = _ASSET_FIELDS - _NON_JOIN_FIELDS

router = APIRouter(prefix='/favorites', tags=['vitrine - favoritos'])


@router.post(
    '/{catalog_id}', status_code=HTTPStatus.CREATED, response_model=Message
)
async def create_favorite(
    catalog_id: UUID,
    session: Session,
    current_user: CurrentUser,
):
    query = select(Catalog).where(Catalog.id == catalog_id)
    db_catalog = await session.scalar(query)
    if not db_catalog:
        raise HTTPException(
            status_code=HTTPStatus.NOT_FOUND,
            detail='Catalog entry not found',
        )

    query_favorite = select(FavoriteCatalog).where(
        FavoriteCatalog.user_id == current_user.id,
        FavoriteCatalog.catalog_id == catalog_id,
    )
    db_favorite_check = await session.scalar(query_favorite)
    if db_favorite_check:
        raise HTTPException(
            status_code=HTTPStatus.CONFLICT,
            detail='Asset already favorited',
        )

    db_favorite = FavoriteCatalog(
        user_id=current_user.id, catalog_id=catalog_id
    )
    session.add(db_favorite)
    await session.commit()

    return {'message': 'Asset favorited successfully'}


@router.get('/', response_model=FavoriteList)
async def read_user_favorites(
    session: Session,
    current_user: CurrentUser,
    filters: Annotated[FilterCatalog, Depends()],
):
    query = (
        select(Catalog)
        .join(FavoriteCatalog, FavoriteCatalog.catalog_id == Catalog.id)
        .where(FavoriteCatalog.user_id == current_user.id)
        .where(Catalog.deleted_at.is_(None))
        .options(
            selectinload(Catalog.images),
            selectinload(Catalog.location).options(
                selectinload(Location.location_inventories).selectinload(
                    LocationInventory.inventory
                )
            ),
            selectinload(Catalog.asset).options(
                joinedload(Asset.material),
                joinedload(Asset.legal_guardian),
                selectinload(Asset.location).options(
                    selectinload(Location.location_inventories).selectinload(
                        LocationInventory.inventory
                    )
                ),
            ),
            selectinload(Catalog.workflow_history).options(
                joinedload(CatalogWorkFlow.user),
                selectinload(CatalogWorkFlow.transfer_requests).options(
                    selectinload(WorkflowTransfer.location).options(
                        selectinload(
                            Location.location_inventories
                        ).selectinload(LocationInventory.inventory)
                    )
                ),
            ),
        )
    )

    asset_join_needed = any(
        getattr(filters, field_name) is not None
        for field_name in ASSET_JOIN_TRIGGER_FIELDS
    )

    query = filter_service.apply_catalog_filters(query, filters)

    if asset_join_needed:
        query = query.join(Catalog.asset)
        query = filter_service.apply_asset_filters(query, filters)

    query = query.offset(filters.offset).limit(filters.limit)

    result = await session.scalars(query)
    entries = result.unique().all()

    return {'favorites': entries}


@router.delete('/{catalog_id}', response_model=Message)
async def delete_favorite(
    catalog_id: UUID,
    session: Session,
    current_user: CurrentUser,
):
    query = select(FavoriteCatalog).where(
        FavoriteCatalog.user_id == current_user.id,
        FavoriteCatalog.catalog_id == catalog_id,
    )
    db_favorite = await session.scalar(query)

    if not db_favorite:
        raise HTTPException(
            status_code=HTTPStatus.NOT_FOUND, detail='Favorite not found'
        )

    await session.delete(db_favorite)
    await session.commit()

    return {'message': 'Favorite removed successfully'}

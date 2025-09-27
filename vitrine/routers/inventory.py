from datetime import datetime
from http import HTTPStatus
from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import and_, func, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import selectinload

from vitrine.dependencies import CurrentUser, Session
from vitrine.models import (
    Asset,
    Inventory,
    InventoryAsset,
    Location,
    LocationInventory,
)
from vitrine.schemas import (
    FilterAsset,
    FilterInventory,
    FilterLocationInventory,
    InventoryAssetList,
    InventoryAssetPublic,
    InventoryAssetSchema,
    InventoryList,
    InventoryPublic,
    InventorySchema,
    LocationInventoryList,
)
from vitrine.services import filter_service

router = APIRouter(prefix='/inventories', tags=['inventário'])


@router.post(
    '/',
    status_code=HTTPStatus.CREATED,
    response_model=InventoryPublic,
)
async def create_inventory(
    inventory: InventorySchema,
    session: Session,
    current_user: CurrentUser,
):
    query = select(Inventory).where(
        Inventory.key == inventory.key, Inventory.deleted_at.is_(None)
    )
    inventory_db = await session.scalar(query)
    if inventory_db:
        raise HTTPException(
            status_code=HTTPStatus.CONFLICT,
            detail='Inventory entry already exists',
        )

    query = select(Location).where(Location.deleted_at.is_(None))
    location_db = await session.scalars(query)
    location_db = location_db.all()

    inventory_db = Inventory(
        key=inventory.key,
        created_by_id=current_user.id,
        avaliable=inventory.avaliable,
    )

    session.add(inventory_db)
    await session.flush()

    locations = []
    for location in location_db:
        i = LocationInventory(
            inventory_id=inventory_db.id,
            location_id=location.id,
        )
        locations.append(i)

    session.add(inventory_db)
    session.add_all(locations)

    await session.commit()
    await session.refresh(inventory_db)
    return inventory_db


@router.get('/', response_model=InventoryList)
async def read_inventories(
    session: Session,
    filters: Annotated[FilterInventory, Depends()],
):
    query = await session.scalars(
        select(Inventory)
        .where(Inventory.deleted_at.is_(None))
        .offset(filters.offset)
        .limit(filters.limit)
    )
    inventories = query.all()
    return {'inventories': inventories}


@router.get('/{inventory_id}', response_model=InventoryPublic)
async def read_inventory(session: Session, inventory_id: UUID):
    inventory = await session.get(Inventory, inventory_id)
    if not inventory or inventory.deleted_at is not None:
        raise HTTPException(status_code=404, detail='Inventory not found')
    return inventory


@router.put('/{inventory_id}', response_model=InventoryPublic)
async def update_inventory(
    inventory_id: UUID,
    data: InventorySchema,
    session: Session,
    current_user: CurrentUser,
):
    inventory = await session.get(Inventory, inventory_id)
    if not inventory or inventory.deleted_at:
        raise HTTPException(
            status_code=HTTPStatus.NOT_FOUND, detail='Inventory not found'
        )
    if inventory.created_by_id != current_user.id:
        raise HTTPException(
            status_code=HTTPStatus.FORBIDDEN, detail='Not enough permissions'
        )

    try:
        inventory.key = data.key
        inventory.avaliable = data.avaliable
        await session.commit()
        await session.refresh(inventory)
        return inventory
    except IntegrityError:
        raise HTTPException(
            status_code=HTTPStatus.CONFLICT,
            detail='Inventory key already exists',
        )


@router.delete('/{inventory_id}', status_code=HTTPStatus.NO_CONTENT)
async def delete_inventory(
    inventory_id: UUID,
    session: Session,
    current_user: CurrentUser,
):
    inventory = await session.get(Inventory, inventory_id)
    if not inventory or inventory.deleted_at:
        raise HTTPException(
            status_code=HTTPStatus.NOT_FOUND, detail='Inventory not found'
        )
    if inventory.created_by_id != current_user.id:
        raise HTTPException(
            status_code=HTTPStatus.FORBIDDEN, detail='Not enough permissions'
        )

    inventory.deleted_at = datetime.now()
    await session.commit()


@router.post(
    '/{inventory_id}/assets/batch',
    status_code=HTTPStatus.CREATED,
    response_model=InventoryAssetList,
)
async def add_assets_to_inventory_batch(
    inventory_id: UUID,
    inventory_assets_data: list[InventoryAssetSchema],
    session: Session,
    current_user: CurrentUser,
):
    if not inventory_assets_data:
        return {'inventoried_asset': []}

    inventory = await session.get(Inventory, inventory_id)
    if not inventory:
        raise HTTPException(HTTPStatus.NOT_FOUND, 'Inventory not found')
    if not inventory.avaliable:
        raise HTTPException(
            HTTPStatus.FORBIDDEN, 'Inventory is no longer accepting assets'
        )

    asset_ids = {d.asset_id for d in inventory_assets_data}
    location_ids = {d.location_id for d in inventory_assets_data}

    if len(asset_ids) != len(inventory_assets_data):
        raise HTTPException(
            HTTPStatus.CONFLICT,
            'One or more assets have already been added to your inventory.',
        )
    if len(location_ids) > 1:
        raise HTTPException(
            HTTPStatus.BAD_REQUEST,
            'All assets in a batch must have the same location_id',
        )

    location_id = next(iter(location_ids))
    location_inventory = await session.scalar(
        select(LocationInventory).where(
            and_(
                LocationInventory.inventory_id == inventory_id,
                LocationInventory.location_id == location_id,
            )
        )
    )
    if not location_inventory:
        raise HTTPException(
            HTTPStatus.NOT_FOUND,
            f'Location {location_id} is not registered for this inventory.',
        )

    location = await session.get(Location, location_id)
    if not location or location.user_id != current_user.id:
        raise HTTPException(
            HTTPStatus.FORBIDDEN,
            'User is not an owner of this inventory',
        )

    found_asset_ids = set(
        await session.scalars(select(Asset.id).where(Asset.id.in_(asset_ids)))
    )
    if not found_asset_ids.issuperset(asset_ids):
        missing = asset_ids - found_asset_ids
        raise HTTPException(
            HTTPStatus.NOT_FOUND,
            f'Assets not found: {", ".join(map(str, missing))}',
        )

    duplicate_asset_ids = await session.scalars(
        select(InventoryAsset.asset_id).where(
            and_(
                InventoryAsset.location_inventory_id == location_inventory.id,
                InventoryAsset.asset_id.in_(asset_ids),
            )
        )
    )
    if duplicate_asset_ids.first():
        raise HTTPException(
            HTTPStatus.CONFLICT,
            'One or more assets have already been added to your inventory.',
        )

    new_inventory_assets = [
        InventoryAsset(
            location_inventory_id=location_inventory.id,
            asset_id=d.asset_id,
            status=d.status,
            comment=d.comment,
            location_id=location_id,
        )
        for d in inventory_assets_data
    ]
    session.add_all(new_inventory_assets)

    location_inventory.filled = True

    await session.commit()

    return {'inventoried_asset': new_inventory_assets}


@router.post(
    '/{inventory_id}/assets',
    status_code=HTTPStatus.CREATED,
    response_model=InventoryAssetPublic,
)
async def add_asset_to_inventory(
    inventory_id: UUID,
    inventory_asset_data: InventoryAssetSchema,
    session: Session,
    current_user: CurrentUser,
):
    inventory = await session.get(Inventory, inventory_id)
    if not inventory:
        raise HTTPException(
            status_code=HTTPStatus.NOT_FOUND,
            detail='Inventory not found',
        )

    if not inventory.avaliable:
        raise HTTPException(
            status_code=HTTPStatus.UNAUTHORIZED,
            detail='Inventory is no longer accepting assets',
        )

    query_location_inventory = select(LocationInventory).where(
        and_(
            LocationInventory.inventory_id == inventory_id,
            LocationInventory.location_id == inventory_asset_data.location_id,
        )
    )
    location_inventory = await session.scalar(query_location_inventory)

    if not location_inventory:
        raise HTTPException(
            status_code=HTTPStatus.NOT_FOUND,
            detail='The specified location is not registered for this inventory.',
        )

    location = await session.get(Location, inventory_asset_data.location_id)
    if not location or location.user_id != current_user.id:
        raise HTTPException(
            status_code=HTTPStatus.FORBIDDEN,
            detail='User is not an owner of this inventory',
        )

    asset = await session.get(Asset, inventory_asset_data.asset_id)
    if not asset:
        raise HTTPException(
            status_code=HTTPStatus.NOT_FOUND,
            detail=f'Asset with id {inventory_asset_data.asset_id} not found.',
        )

    query_existing_asset = select(InventoryAsset).where(
        and_(
            InventoryAsset.location_inventory_id == location_inventory.id,
            InventoryAsset.asset_id == inventory_asset_data.asset_id,
        )
    )
    existing_inventory_asset = await session.scalar(query_existing_asset)

    if existing_inventory_asset:
        raise HTTPException(
            status_code=HTTPStatus.CONFLICT,
            detail='This asset has already been inventoried in this location.',
        )

    inventory_asset = InventoryAsset(
        location_inventory_id=location_inventory.id,
        asset_id=inventory_asset_data.asset_id,
        status=inventory_asset_data.status,
        location_id=inventory_asset_data.location_id,
        comment=inventory_asset_data.comment,
    )
    session.add(inventory_asset)

    location_inventory.filled = True

    await session.commit()
    await session.refresh(inventory_asset)
    return inventory_asset


@router.get('/{inventory_id}/assets', response_model=InventoryAssetList)
async def list_assets_in_inventory(
    inventory_id: UUID,
    session: Session,
    current_user: CurrentUser,
    filters: FilterAsset,
):
    query = (
        select(InventoryAsset)
        .join(LocationInventory)
        .where(
            LocationInventory.inventory_id == inventory_id,
            LocationInventory.user_id == current_user.id,
        )
    )
    if filters.location_id:
        query = query.where(Asset.location_id == filters.location_id)
        filters.location_id = None

    query = filter_service.apply_asset_filters(query, filters)

    assets_db = await session.scalars(query)
    return {'inventoried_asset': assets_db.all()}


@router.delete(
    '/{inventory_id}/assets/{inventory_asset_id}',
    status_code=HTTPStatus.NO_CONTENT,
)
async def remove_asset_from_inventory(
    inventory_id: UUID,
    inventory_asset_id: UUID,
    session: Session,
    current_user: CurrentUser,
):
    query = (
        select(InventoryAsset)
        .join(LocationInventory)
        .where(
            InventoryAsset.id == inventory_asset_id,
            LocationInventory.user_id == current_user.id,
            LocationInventory.inventory_id == inventory_id,
        )
    )
    inventory_asset = await session.scalar(query)

    if not inventory_asset or inventory_asset.deleted_at:
        raise HTTPException(
            status_code=HTTPStatus.NOT_FOUND,
            detail='Inventory asset not found',
        )

    inventory_asset.deleted_at = datetime.now()
    await session.commit()


@router.get('/{inventory_id}/locations', response_model=LocationInventoryList)
async def list_inventory_locations_by_inventory(
    inventory_id: UUID,
    session: Session,
    filters: Annotated[FilterLocationInventory, Depends()],
):
    query = (
        select(LocationInventory)
        .join(LocationInventory.location)
        .where(
            LocationInventory.inventory_id == inventory_id,
            Location.deleted_at.is_(None),
        )
        .options(
            selectinload(LocationInventory.location).selectinload(
                Location.location_inventories
            ),
            selectinload(LocationInventory.assets),
        )
    )

    if filters.q:
        prefix_query = ' & '.join(word + ':*' for word in filters.q.split())
        ts_query = func.to_tsquery('portuguese', prefix_query)
        query = query.where(Location.tsv.op('@@')(ts_query))

    if filters.filled:
        query = query.where(LocationInventory.filled == filters.filled)

    if filters.sector_id:
        query = query.where(Location.sector_id == filters.sector_id)

    if filters.legal_guardian_id:
        query = query.where(
            Location.legal_guardian_id == filters.legal_guardian_id
        )

    query = query.offset(filters.offset).limit(filters.limit)

    result = await session.scalars(query)
    location_inventories = result.all()

    return {'location_inventory': location_inventories}

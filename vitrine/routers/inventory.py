from datetime import datetime
from http import HTTPStatus
from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError

from vitrine.dependencies import CurrentUser, Session
from vitrine.models import (
    Asset,
    Inventory,
    InventoryAsset,
    InventoryOwner,
    User,
)
from vitrine.schemas import (
    FilterAsset,
    FilterInventory,
    InventoryAssetList,
    InventoryAssetPublic,
    InventoryAssetSchema,
    InventoryList,
    InventoryPublic,
    InventorySchema,
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

    query = select(User).where(User.deleted_at.is_(None))
    users_db = await session.scalars(query)
    users_db = users_db.all()

    inventory_db = Inventory(
        key=inventory.key,
        created_by_id=current_user.id,
        avaliable=inventory.avaliable,
    )

    session.add(inventory_db)
    await session.flush()

    owners = []
    for user in users_db:
        i = InventoryOwner(inventory_id=inventory_db.id, user_id=user.id)
        owners.append(i)

    session.add(inventory_db)
    session.add_all(owners)

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
            status_code=HTTPStatus.NOT_FOUND, detail='Inventory not found'
        )
    if not inventory.avaliable:
        raise HTTPException(
            status_code=HTTPStatus.UNAUTHORIZED,
            detail='Inventory is no longer accepting assets',
        )
    query = select(InventoryOwner).where(
        InventoryOwner.inventory_id == inventory_id,
        InventoryOwner.user_id == current_user.id,
    )
    owner = await session.scalar(query)
    if not owner:
        raise HTTPException(
            status_code=HTTPStatus.FORBIDDEN,
            detail='User is not an owner of this inventory',
        )

    asset = await session.get(Asset, inventory_asset_data.asset_id)
    if not asset:
        raise HTTPException(
            status_code=HTTPStatus.NOT_FOUND, detail='Asset not found'
        )

    inventory_asset = InventoryAsset(
        inventory_owner_id=owner.id,
        asset_id=inventory_asset_data.asset_id,
        status=inventory_asset_data.status,
        comment=inventory_asset_data.comment,
        location_id=inventory_asset_data.location_id,
    )

    session.add(inventory_asset)

    try:
        await session.commit()
        await session.refresh(inventory_asset)
    except IntegrityError:
        await session.rollback()
        raise HTTPException(
            status_code=HTTPStatus.CONFLICT,
            detail='This asset has already been added to your inventory',
        )

    return inventory_asset


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
    inventory = await session.get(Inventory, inventory_id)
    if not inventory:
        raise HTTPException(
            status_code=HTTPStatus.NOT_FOUND, detail='Inventory not found'
        )
    if not inventory.avaliable:
        raise HTTPException(
            status_code=HTTPStatus.UNAUTHORIZED,
            detail='Inventory is no longer accepting assets',
        )
    query = select(InventoryOwner).where(
        InventoryOwner.inventory_id == inventory_id,
        InventoryOwner.user_id == current_user.id,
    )
    owner = await session.scalar(query)
    if not owner:
        raise HTTPException(
            status_code=HTTPStatus.FORBIDDEN,
            detail='User is not an owner of this inventory',
        )

    if not inventory_assets_data:
        return {'assets': []}

    asset_ids_to_check = {data.asset_id for data in inventory_assets_data}
    stmt = select(Asset.id).where(Asset.id.in_(asset_ids_to_check))
    result = await session.execute(stmt)
    existing_asset_ids = {res[0] for res in result}

    if len(existing_asset_ids) != len(asset_ids_to_check):
        not_found_ids = asset_ids_to_check - existing_asset_ids
        raise HTTPException(
            status_code=HTTPStatus.NOT_FOUND,
            detail=f'Assets not found: {", ".join(str(uid) for uid in not_found_ids)}',
        )

    new_inventory_assets = [
        InventoryAsset(
            inventory_owner_id=owner.id,
            asset_id=data.asset_id,
            status=data.status,
            comment=data.comment,
            location_id=data.location_id,
        )
        for data in inventory_assets_data
    ]

    session.add_all(new_inventory_assets)

    try:
        await session.commit()
    except IntegrityError:
        await session.rollback()
        raise HTTPException(
            status_code=HTTPStatus.CONFLICT,
            detail='One or more assets have already been added to your inventory.',
        )

    return {'inventoried_asset': new_inventory_assets}


@router.get('/{inventory_id}/assets', response_model=InventoryAssetList)
async def list_assets_in_inventory(
    inventory_id: UUID,
    session: Session,
    current_user: CurrentUser,
    filters: FilterAsset,
):
    query = (
        select(InventoryAsset)
        .join(InventoryOwner)
        .where(
            InventoryOwner.inventory_id == inventory_id,
            InventoryOwner.user_id == current_user.id,
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
        .join(InventoryOwner)
        .where(
            InventoryAsset.id == inventory_asset_id,
            InventoryOwner.user_id == current_user.id,
            InventoryOwner.inventory_id == inventory_id,
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

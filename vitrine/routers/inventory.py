from datetime import datetime
from http import HTTPStatus
from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from vitrine.database import get_session
from vitrine.models import (
    Asset,
    Inventory,
    InventoryAsset,
    InventoryOwner,
    User,
)
from vitrine.schemas import (
    FilterInventory,
    InventoryAssetList,
    InventoryAssetPublic,
    InventoryAssetSchema,
    InventoryList,
    InventoryPublic,
    InventorySchema,
)
from vitrine.security import get_current_user

router = APIRouter(prefix='/inventories', tags=['inventário'])

Session = Annotated[AsyncSession, Depends(get_session)]
CurrentUser = Annotated[User, Depends(get_current_user)]


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


@router.get('/{inventory_id}/assets', response_model=InventoryAssetList)
async def list_assets_in_inventory(
    inventory_id: UUID,
    session: Session,
    current_user: CurrentUser,
):
    query = (
        select(InventoryAsset)
        .join(InventoryOwner)
        .where(
            InventoryOwner.inventory_id == inventory_id,
            InventoryOwner.user_id == current_user.id,
            InventoryAsset.deleted_at.is_(None),
        )
    )
    query = select(InventoryOwner)
    assets_db = await session.scalars(query)
    return {'assets': assets_db.all()}


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

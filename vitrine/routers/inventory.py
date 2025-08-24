from datetime import datetime
from http import HTTPStatus
from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from vitrine.database import get_session
from vitrine.models import Inventory, InventoryOwner, User
from vitrine.schemas import (
    FilterInventory,
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


@router.get('/my', response_model=InventoryList)
async def read_my_inventories(session: Session, current_user: CurrentUser):
    query = await session.scalars(
        select(Inventory).where(
            Inventory.created_by_id == current_user.id,
            Inventory.deleted_at.is_(None),
        )
    )
    inventories = query.all()
    return {'inventories': inventories}


@router.get('/{inventory_id}', response_model=InventoryPublic)
async def read_inventory(inventory_id: UUID, session: Session):
    inventory = await session.get(Inventory, inventory_id)
    if not inventory or inventory.deleted_at:
        raise HTTPException(
            status_code=HTTPStatus.NOT_FOUND, detail='Inventory not found'
        )
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

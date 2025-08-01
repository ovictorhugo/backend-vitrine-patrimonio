# routers/inventory.py

from datetime import datetime
from http import HTTPStatus
from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import and_, select
from sqlalchemy.ext.asyncio import AsyncSession

from vitrine.database import get_session
from vitrine.models import Inventory, User
from vitrine.schemas import (
    FilterPage,
    InventoryList,
    InventoryPublic,
    InventorySchema,
    Message,
)
from vitrine.security import get_current_user

router = APIRouter(prefix='/inventories', tags=['inventário'])


Session = Annotated[AsyncSession, Depends(get_session)]
CurrentUser = Annotated[User, Depends(get_current_user)]


@router.post(
    '/', status_code=HTTPStatus.CREATED, response_model=InventoryPublic
)
async def create_inventory(
    inventory: InventorySchema,
    session: Session,
    current_user: CurrentUser,
):
    query = select(Inventory).where(
        and_(
            Inventory.location_id == inventory.location_id,
            Inventory.term == inventory.term,
            Inventory.deleted_at.is_(None),
        )
    )
    db_inventory = await session.scalar(query)

    if db_inventory:
        raise HTTPException(
            status_code=HTTPStatus.CONFLICT,
            detail='An inventory for this location and term already exists.',
        )

    db_inventory = Inventory(
        location_id=inventory.location_id,
        term=inventory.term,
        user_id=current_user.id,
    )
    session.add(db_inventory)
    await session.commit()
    await session.refresh(db_inventory)

    return db_inventory


@router.get('/', response_model=InventoryList)
async def read_inventories(
    session: Session, filter_page: Annotated[FilterPage, Depends()]
):
    query = (
        select(Inventory)
        .where(Inventory.deleted_at.is_(None))
        .offset(filter_page.offset)
        .limit(filter_page.limit)
    )
    inventories = await session.scalars(query)

    return {'inventories': inventories.all()}


@router.delete('/{inventory_id}', response_model=Message)
async def delete_inventory(
    inventory_id: UUID, session: Session, current_user: CurrentUser
):
    db_inventory = await session.get(Inventory, inventory_id)

    if not db_inventory or db_inventory.deleted_at:
        raise HTTPException(
            status_code=HTTPStatus.NOT_FOUND, detail='Inventory not found'
        )

    db_inventory.deleted_at = datetime.now()
    await session.commit()

    return {'message': 'Inventory deactivated successfully'}

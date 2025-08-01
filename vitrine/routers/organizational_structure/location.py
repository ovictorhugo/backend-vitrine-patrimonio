from datetime import datetime
from http import HTTPStatus
from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from vitrine.database import get_session
from vitrine.models import Location, User
from vitrine.schemas import (
    FilterPage,
    LocationList,
    LocationPublic,
    LocationSchema,
    Message,
)
from vitrine.security import get_current_user

router = APIRouter(
    prefix='/locations', tags=['estrutura organizacional - localização']
)


Session = Annotated[AsyncSession, Depends(get_session)]
CurrentUser = Annotated[User, Depends(get_current_user)]


@router.post(
    '/', status_code=HTTPStatus.CREATED, response_model=LocationPublic
)
async def create_location(
    location: LocationSchema,
    session: Session,
    current_user: CurrentUser,
):
    db_location = await session.scalar(
        select(Location).where(
            (Location.location_name == location.location_name)
            | (Location.location_code == location.location_code)
        )
    )
    if db_location:
        raise HTTPException(
            status_code=HTTPStatus.CONFLICT,
            detail='Location name or code already exists',
        )

    db_location = Location(
        location_name=location.location_name,
        location_code=location.location_code,
        user_id=current_user.id,
    )
    session.add(db_location)
    await session.commit()
    await session.refresh(db_location)

    return db_location


@router.get('/', response_model=LocationList)
async def read_locations(
    session: Session, filter_page: Annotated[FilterPage, Depends()]
):
    """Lista todas as localizações ativas com paginação."""
    query = await session.scalars(
        select(Location)
        .where(Location.deleted_at.is_(None))
        .offset(filter_page.offset)
        .limit(filter_page.limit)
    )
    locations = query.all()

    return {'locations': locations}


@router.delete('/{location_id}', response_model=Message)
async def delete_location(
    location_id: UUID, session: Session, current_user: CurrentUser
):
    """Desativa (soft delete) uma localização."""
    db_location = await session.get(Location, location_id)

    if not db_location:
        raise HTTPException(
            status_code=HTTPStatus.NOT_FOUND, detail='Location not found'
        )

    db_location.deleted_at = datetime.now()
    await session.commit()

    return {'message': 'Location deactivated successfully'}

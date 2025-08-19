from datetime import datetime
from http import HTTPStatus
from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from vitrine.database import get_session
from vitrine.models import Location, Sector, User
from vitrine.schemas import (
    FilterLocation,
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
    db_sector = await session.get(Sector, location.sector_id)
    if not db_sector or db_sector.deleted_at:
        raise HTTPException(
            status_code=HTTPStatus.NOT_FOUND,
            detail=f'O setor com ID "{location.sector_id}" não foi encontrado ou está inativo.',
        )

    query = select(Location).where(
        Location.location_name == location.location_name
    )
    db_location = await session.scalar(query)
    if db_location:
        raise HTTPException(
            status_code=HTTPStatus.CONFLICT,
            detail='Uma localização com este nome já existe.',
        )

    db_location = Location(
        location_name=location.location_name,
        location_code=location.location_code,
        sector_id=location.sector_id,
        user_id=current_user.id,
    )
    session.add(db_location)
    await session.commit()
    await session.refresh(db_location)

    return db_location


@router.get('/', response_model=LocationList)
async def read_locations(
    session: Session, filters: Annotated[FilterLocation, Depends()]
):
    query = select(Location).where(Location.deleted_at.is_(None))

    if filters.q:
        prefix_query = ' & '.join(word + ':*' for word in filters.q.split())
        ts_query = func.to_tsquery('portuguese', prefix_query)
        query = query.where(Location.tsv.op('@@')(ts_query))

    if filters.sector_id:
        query = query.where(Location.sector_id == filters.sector_id)

    query = query.offset(filters.offset).limit(filters.limit)

    result = await session.scalars(query)
    locations = result.all()

    return {'locations': locations}


@router.delete('/{location_id}', response_model=Message)
async def delete_location(
    location_id: UUID, session: Session, current_user: CurrentUser
):
    db_location = await session.get(Location, location_id)
    if not db_location:
        raise HTTPException(
            status_code=HTTPStatus.NOT_FOUND,
            detail='Localização não encontrada.',
        )
    if db_location.deleted_at:
        raise HTTPException(
            status_code=HTTPStatus.BAD_REQUEST,
            detail='Esta localização já está desativada.',
        )

    db_location.deleted_at = datetime.now()
    await session.commit()

    return {'message': 'Localização desativada com sucesso.'}

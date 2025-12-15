from datetime import datetime
from http import HTTPStatus
from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import func, select
from sqlalchemy.orm import selectinload

from vitrine.core.dependencies import CurrentUser, Session
from vitrine.models import (
    Agency,
    LegalGuardian,
    Location,
    LocationInventory,
    Sector,
    SystemIdentity,
)
from vitrine.schemas import (
    FilterLocation,
    LocationList,
    LocationPublic,
    LocationSchema,
    Message,
)

router = APIRouter(
    prefix='/locations', tags=['estrutura organizacional - localização']
)


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
            detail=(
                f'O setor com ID "{location.sector_id}" não foi encontrado'
                ' ou está inativo.'
            ),
        )

    db_legal_guardian = await session.get(
        LegalGuardian, location.legal_guardian_id
    )
    if not db_legal_guardian or db_legal_guardian.deleted_at:
        raise HTTPException(
            status_code=HTTPStatus.NOT_FOUND,
            detail=(
                f'O responsável legal com ID "{location.legal_guardian_id}" '
                'não foi encontrado ou está inativo.'
            ),
        )

    query = select(Location).where(
        Location.location_name == location.location_name,
        Location.sector_id == location.sector_id,
        Location.legal_guardian_id == location.legal_guardian_id,
    )
    db_location = await session.scalar(query)
    if db_location:
        raise HTTPException(
            status_code=HTTPStatus.CONFLICT,
            detail=(
                'Uma localização com este nome já existe para este setor e '
                'responsável.'
            ),
        )

    db_location = Location(
        location_name=location.location_name,
        location_code=location.location_code,
        sector_id=location.sector_id,
        legal_guardian_id=location.legal_guardian_id,
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
    query = (
        select(Location)
        .where(Location.deleted_at.is_(None))
        .options(
            selectinload(Location.location_inventories).selectinload(
                LocationInventory.inventory
            )
        )
    )

    # Filtro de Texto (Full Text Search)
    if filters.q:
        prefix_query = ' & '.join(word + ':*' for word in filters.q.split())
        ts_query = func.to_tsquery('portuguese', prefix_query)
        query = query.where(Location.tsv.op('@@')(ts_query))

    if filters.sector_id:
        query = query.where(Location.sector_id == filters.sector_id)

    if filters.legal_guardian_id:
        query = query.where(
            Location.legal_guardian_id == filters.legal_guardian_id
        )

    if filters.agency_id or filters.unit_id:
        query = query.join(Location.sector)

    if filters.agency_id:
        query = query.where(Sector.agency_id == filters.agency_id)

    if filters.unit_id:
        query = query.join(Sector.agency).where(
            Agency.unit_id == filters.unit_id
        )

    query = query.offset(filters.offset).limit(filters.limit)
    result = await session.scalars(query)
    locations = result.all()

    return {'locations': locations}


@router.get('/my', response_model=LocationList)
async def read_my_locations(
    session: Session,
    current_user: CurrentUser,
    filters: Annotated[FilterLocation, Depends()],
):
    query = (
        select(Location)
        .join(LegalGuardian, Location.legal_guardian_id == LegalGuardian.id)
        .join(
            SystemIdentity,
            LegalGuardian.id == SystemIdentity.legal_guardian_id,
        )
        .where(
            SystemIdentity.user_id == current_user.id,
            Location.deleted_at.is_(None),
        )
        .options(
            selectinload(Location.location_inventories).selectinload(
                LocationInventory.inventory
            )
        )
    )

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


@router.get('/{location_id}', response_model=LocationPublic)
async def read_location(location_id: UUID, session: Session):
    query = (
        select(Location)
        .where(Location.id == location_id, Location.deleted_at.is_(None))
        .options(
            selectinload(Location.location_inventories).selectinload(
                LocationInventory.inventory
            )
        )
    )
    result = await session.scalar(query)

    if not result:
        raise HTTPException(status_code=404, detail='Location not found')

    return result


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

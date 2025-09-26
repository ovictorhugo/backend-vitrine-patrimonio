from datetime import datetime
from http import HTTPStatus
from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import func, select

from vitrine.dependencies import CurrentUser, Session
from vitrine.models import Agency, Location, Sector
from vitrine.schemas import (
    FilterSector,
    Message,
    SectorList,
    SectorPublic,
    SectorSchema,
)

router = APIRouter(
    prefix='/sectors', tags=['estrutura organizacional - setores']
)


@router.post('/', status_code=HTTPStatus.CREATED, response_model=SectorPublic)
async def create_sector(
    sector: SectorSchema,
    session: Session,
    current_user: CurrentUser,
):
    db_agency = await session.get(Agency, sector.agency_id)
    if not db_agency or db_agency.deleted_at:
        raise HTTPException(
            status_code=HTTPStatus.NOT_FOUND,
            detail=f'A organização com ID "{sector.agency_id}" não foi encontrada ou está inativa.',
        )

    query = select(Sector).where(Sector.sector_name == sector.sector_name)
    db_sector = await session.scalar(query)
    if db_sector:
        raise HTTPException(
            status_code=HTTPStatus.CONFLICT,
            detail='Um setor com este nome já existe.',
        )

    db_sector = Sector(
        sector_name=sector.sector_name,
        sector_code=sector.sector_code,
        agency_id=sector.agency_id,
        user_id=current_user.id,
    )
    session.add(db_sector)
    await session.commit()
    await session.refresh(db_sector)

    return db_sector


@router.get('/', response_model=SectorList)
async def read_sectors(
    session: Session, filters: Annotated[FilterSector, Depends()]
):
    query = select(Sector).where(Sector.deleted_at.is_(None))

    if filters.q:
        prefix_query = ' & '.join(word + ':*' for word in filters.q.split())
        ts_query = func.to_tsquery('portuguese', prefix_query)
        query = query.where(Sector.tsv.op('@@')(ts_query))

    if filters.agency_id:
        query = query.where(Sector.agency_id == filters.agency_id)

    query = query.offset(filters.offset).limit(filters.limit)

    result = await session.scalars(query)
    sectors = result.all()

    return {'sectors': sectors}


@router.delete('/{sector_id}', response_model=Message)
async def delete_sector(
    sector_id: UUID, session: Session, current_user: CurrentUser
):
    db_sector = await session.get(Sector, sector_id)
    if not db_sector:
        raise HTTPException(
            status_code=HTTPStatus.NOT_FOUND, detail='Setor não encontrado.'
        )
    if db_sector.deleted_at:
        raise HTTPException(
            status_code=HTTPStatus.BAD_REQUEST,
            detail='Este setor já está desativado.',
        )

    query = select(func.count(Location.id)).where(
        Location.sector_id == sector_id, Location.deleted_at.is_(None)
    )
    active_locations_count = await session.scalar(query)

    if active_locations_count > 0:
        raise HTTPException(
            status_code=HTTPStatus.CONFLICT,
            detail=f'Não é possível desativar o setor pois ele possui {active_locations_count} localização(ões) ativa(s).',
        )

    db_sector.deleted_at = datetime.now()
    await session.commit()

    return {'message': 'Setor desativado com sucesso.'}

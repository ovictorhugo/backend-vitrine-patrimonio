from datetime import datetime
from http import HTTPStatus
from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from vitrine.database import get_session
from vitrine.models import Sector, User
from vitrine.schemas import (
    FilterSector,
    Message,
    SectorList,
    SectorPublic,
    SectorSchema,
)
from vitrine.security import get_current_user

router = APIRouter(
    prefix='/sectors', tags=['estrutura organizacional - setores']
)


Session = Annotated[AsyncSession, Depends(get_session)]
CurrentUser = Annotated[User, Depends(get_current_user)]


@router.post('/', status_code=HTTPStatus.CREATED, response_model=SectorPublic)
async def create_sector(
    sector: SectorSchema,
    session: Session,
    current_user: CurrentUser,
):
    db_sector = await session.scalar(
        select(Sector).where(
            (Sector.sector_name == sector.sector_name)
            & (Sector.sector_code == sector.sector_code)
        )
    )
    if db_sector:
        raise HTTPException(
            status_code=HTTPStatus.CONFLICT,
            detail='Sector name or code already exists',
        )

    db_sector = Sector(
        sector_name=sector.sector_name,
        sector_code=sector.sector_code,
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
            status_code=HTTPStatus.NOT_FOUND, detail='Sector not found'
        )

    db_sector.deleted_at = datetime.now()
    await session.commit()

    return {'message': 'Sector deactivated successfully'}

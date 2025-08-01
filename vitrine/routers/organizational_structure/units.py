from datetime import datetime
from http import HTTPStatus
from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from vitrine.database import get_session
from vitrine.models import Unit, User
from vitrine.schemas import (
    FilterUnit,
    Message,
    UnitList,
    UnitPublic,
    UnitSchema,
)
from vitrine.security import get_current_user

router = APIRouter(
    prefix='/units', tags=['estrutura organizacional - unidades']
)


Session = Annotated[AsyncSession, Depends(get_session)]
CurrentUser = Annotated[User, Depends(get_current_user)]


@router.post('/', status_code=HTTPStatus.CREATED, response_model=UnitPublic)
async def create_unit(
    unit: UnitSchema,
    session: Session,
    current_user: CurrentUser,
):
    db_unit = await session.scalar(
        select(Unit).where(
            (Unit.unit_name == unit.unit_name)
            & (Unit.unit_code == unit.unit_code)
        )
    )
    if db_unit:
        raise HTTPException(
            status_code=HTTPStatus.CONFLICT,
            detail='Unit name or code already exists',
        )

    db_unit = Unit(
        unit_name=unit.unit_name,
        unit_code=unit.unit_code,
        unit_siaf=unit.unit_siaf,
        user_id=current_user.id,
    )
    session.add(db_unit)
    await session.commit()
    await session.refresh(db_unit)

    return db_unit


@router.get('/', response_model=UnitList)
async def read_units(
    session: Session,
    filters: FilterUnit = Depends(),
):
    query = select(Unit).where(Unit.deleted_at.is_(None))

    if filters.q:
        prefix_query = ' & '.join(word + ':*' for word in filters.q.split())
        ts_query = func.to_tsquery('portuguese', prefix_query)
        query = query.where(Unit.tsv.op('@@')(ts_query))

    query = query.offset(filters.offset).limit(filters.limit)

    result = await session.scalars(query)
    units = result.all()

    return {'units': units}


@router.delete('/{unit_id}', response_model=Message)
async def delete_unit(
    unit_id: UUID, session: Session, current_user: CurrentUser
):
    db_unit = await session.get(Unit, unit_id)

    if not db_unit:
        raise HTTPException(
            status_code=HTTPStatus.NOT_FOUND, detail='Unit not found'
        )

    db_unit.deleted_at = datetime.now()
    await session.commit()

    return {'message': 'Unit deactivated successfully'}

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
    query = select(Unit).where(Unit.unit_name == unit.unit_name)
    db_unit = await session.scalar(query)
    if db_unit:
        raise HTTPException(
            status_code=HTTPStatus.CONFLICT,
            detail='Uma unidade com este nome já existe.',
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

    if filters.agency_id:
        query = query.where(Unit.agency_id == filters.agency_id)

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
            status_code=HTTPStatus.NOT_FOUND, detail='Unidade não encontrada.'
        )
    if db_unit.deleted_at:
        raise HTTPException(
            status_code=HTTPStatus.BAD_REQUEST,
            detail='Esta unidade já está desativada.',
        )

    db_unit.deleted_at = datetime.now()
    await session.commit()

    return {'message': 'Unidade desativada com sucesso.'}

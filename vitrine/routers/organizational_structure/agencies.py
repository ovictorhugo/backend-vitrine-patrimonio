from datetime import datetime
from http import HTTPStatus
from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from vitrine.database import get_session
from vitrine.models import Agency, User
from vitrine.schemas import (
    AgencyList,
    AgencyPublic,
    AgencySchema,
    FilterAgency,
    Message,
)
from vitrine.security import (
    get_current_user,
)

router = APIRouter(
    prefix='/agencies', tags=['estrutura organizacional - organização']
)


Session = Annotated[AsyncSession, Depends(get_session)]
CurrentUser = Annotated[User, Depends(get_current_user)]


@router.post('/', status_code=HTTPStatus.CREATED, response_model=AgencyPublic)
async def create_agency(
    agency: AgencySchema,
    session: Session,
    current_user: CurrentUser,
):
    db_agency = await session.scalar(
        select(Agency).where(
            (Agency.agency_name == agency.agency_name)
            & (Agency.agency_code == agency.agency_code)
        )
    )
    if db_agency:
        raise HTTPException(
            status_code=HTTPStatus.CONFLICT,
            detail='Username already exists',
        )
    db_agency = Agency(
        agency_name=agency.agency_name,
        agency_code=agency.agency_code,
        user_id=current_user.id,
    )
    session.add(db_agency)
    await session.commit()
    await session.refresh(db_agency)

    return db_agency


@router.get('/', response_model=AgencyList)
async def read_agency(
    session: Session,
    filters: FilterAgency = Depends(),
):
    query = select(Agency).where(Agency.deleted_at.is_(None))

    if filters.q:
        ts_query = func.plainto_tsquery('portuguese', func.unaccent(filters.q))
        query = query.where(Agency.tsv.op('@@')(ts_query))

    query = query.offset(filters.offset).limit(filters.limit)

    result = await session.scalars(query)
    agencies = result.all()

    return {'agencies': agencies}


@router.delete('/{agency_id}', response_model=Message)
async def delete_agency(
    agency_id: UUID, session: Session, current_user: CurrentUser
):
    db_agency = await session.get(Agency, agency_id)
    db_agency.deleted_at = datetime.now()
    await session.commit()
    return {'message': 'Agency deactivated'}

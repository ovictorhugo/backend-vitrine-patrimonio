from datetime import datetime
from http import HTTPStatus
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import func, select

from vitrine.core.dependencies import CurrentUser, Session
from vitrine.models import Agency, Sector, Unit
from vitrine.schemas import (
    AgencyList,
    AgencyPublic,
    AgencySchema,
    FilterAgency,
    Message,
)

router = APIRouter(
    prefix='/agencies', tags=['estrutura organizacional - organização']
)


@router.post('/', status_code=HTTPStatus.CREATED, response_model=AgencyPublic)
async def create_agency(
    agency: AgencySchema,
    session: Session,
    current_user: CurrentUser,
):
    db_unit = await session.get(Unit, agency.unit_id)
    if not db_unit or db_unit.deleted_at:
        raise HTTPException(
            status_code=HTTPStatus.NOT_FOUND,
            detail=f'A unidade com ID "{agency.unit_id}" não foi encontrado ou está inativa.',
        )

    query = select(Agency).where(Agency.agency_name == agency.agency_name)
    db_agency = await session.scalar(query)

    if db_agency:
        raise HTTPException(
            status_code=HTTPStatus.CONFLICT,
            detail='Um órgão com este nome já existe.',
        )

    db_agency = Agency(
        agency_name=agency.agency_name,
        agency_code=agency.agency_code,
        unit_id=agency.unit_id,
        user_id=current_user.id,
    )
    session.add(db_agency)
    await session.commit()
    await session.refresh(db_agency)

    return db_agency


@router.get('/', response_model=AgencyList)
async def read_agencies(
    session: Session,
    filters: FilterAgency = Depends(),
):
    query = select(Agency).where(Agency.deleted_at.is_(None))

    if filters.q:
        prefix_query = ' & '.join(word + ':*' for word in filters.q.split())
        ts_query = func.to_tsquery('portuguese', prefix_query)
        query = query.where(Agency.tsv.op('@@')(ts_query))

    query = query.offset(filters.offset).limit(filters.limit)

    result = await session.scalars(query)
    agencies = result.all()

    return {'agencies': agencies}


@router.delete('/{agency_id}', response_model=Message)
async def delete_agency(
    agency_id: UUID, session: Session, current_user: CurrentUser
):
    query = select(func.count(Sector.id)).where(
        Sector.agency_id == agency_id,
        Sector.deleted_at.is_(None),
    )
    active_sectors_count = await session.scalar(query)

    if active_sectors_count > 0:
        raise HTTPException(
            status_code=HTTPStatus.CONFLICT,
            detail=f'Não é possível desativar o órgão pois ele possui {active_sectors_count} setores(s) ativa(s).',
        )

    db_agency = await session.get(Agency, agency_id)
    if not db_agency:
        raise HTTPException(
            status_code=HTTPStatus.NOT_FOUND,
            detail='Órgão não encontrado.',
        )

    if db_agency.deleted_at:
        raise HTTPException(
            status_code=HTTPStatus.BAD_REQUEST,
            detail='Este órgão já está desativado.',
        )

    db_agency.deleted_at = datetime.now()
    await session.commit()

    return {'message': 'Órgão desativado com sucesso.'}

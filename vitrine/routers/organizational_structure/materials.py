from datetime import datetime
from http import HTTPStatus
from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from vitrine.database import get_session
from vitrine.models import Material, User
from vitrine.schemas import (
    FilterMaterial,
    MaterialList,
    MaterialPublic,
    MaterialSchema,
    Message,
)
from vitrine.security import get_current_user

router = APIRouter(
    prefix='/materials', tags=['estrutura organizacional - materiais']
)


Session = Annotated[AsyncSession, Depends(get_session)]
CurrentUser = Annotated[User, Depends(get_current_user)]


@router.post(
    '/', status_code=HTTPStatus.CREATED, response_model=MaterialPublic
)
async def create_material(
    material: MaterialSchema,
    session: Session,
    current_user: CurrentUser,
):
    """Cria um novo material."""
    db_material = await session.scalar(
        select(Material).where(
            (Material.material_name == material.material_name)
            | (Material.material_code == material.material_code)
        )
    )
    if db_material:
        raise HTTPException(
            status_code=HTTPStatus.CONFLICT,
            detail='Material name or code already exists',
        )

    db_material = Material(
        material_name=material.material_name,
        material_code=material.material_code,
        user_id=current_user.id,
    )
    session.add(db_material)
    await session.commit()
    await session.refresh(db_material)

    return db_material


@router.get('/', response_model=MaterialList)
async def read_materials(
    session: Session, filters: Annotated[FilterMaterial, Depends()]
):
    query = select(Material).where(Material.deleted_at.is_(None))

    if filters.q:
        prefix_query = ' & '.join(word + ':*' for word in filters.q.split())
        ts_query = func.to_tsquery('portuguese', prefix_query)
        query = query.where(Material.tsv.op('@@')(ts_query))

    query = query.offset(filters.offset).limit(filters.limit)

    result = await session.scalars(query)
    materials = result.all()

    return {'materials': materials}


@router.delete('/{material_id}', response_model=Message)
async def delete_material(
    material_id: UUID, session: Session, current_user: CurrentUser
):
    """Desativa (soft delete) um material."""
    db_material = await session.get(Material, material_id)

    if not db_material:
        raise HTTPException(
            status_code=HTTPStatus.NOT_FOUND, detail='Material not found'
        )

    db_material.deleted_at = datetime.now()
    await session.commit()

    return {'message': 'Material deactivated successfully'}

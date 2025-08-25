from http import HTTPStatus
from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from vitrine.database import get_session
from vitrine.models import Catalog, CatalogWorkFlow, FavoriteCatalog, User
from vitrine.schemas import FavoriteList, Message
from vitrine.security import get_current_user

router = APIRouter(prefix='/favorites', tags=['vitrine - favoritos'])

Session = Annotated[AsyncSession, Depends(get_session)]
CurrentUser = Annotated[User, Depends(get_current_user)]


@router.post(
    '/{catalog_id}', status_code=HTTPStatus.CREATED, response_model=Message
)
async def create_favorite(
    catalog_id: UUID,
    session: Session,
    current_user: CurrentUser,
):
    query = select(Catalog).where(Catalog.id == catalog_id)
    db_catalog = await session.scalar(query)
    if not db_catalog:
        raise HTTPException(
            status_code=HTTPStatus.NOT_FOUND,
            detail='Catalog entry not found',
        )

    query_favorite = select(FavoriteCatalog).where(
        FavoriteCatalog.user_id == current_user.id,
        FavoriteCatalog.catalog_id == catalog_id,
    )
    db_favorite_check = await session.scalar(query_favorite)
    if db_favorite_check:
        raise HTTPException(
            status_code=HTTPStatus.CONFLICT,
            detail='Asset already favorited',
        )

    db_favorite = FavoriteCatalog(
        user_id=current_user.id, catalog_id=catalog_id
    )
    session.add(db_favorite)
    await session.commit()

    return {'message': 'Asset favorited successfully'}


@router.get('/', response_model=FavoriteList)
async def read_user_favorites(
    session: Session,
    current_user: CurrentUser,
):
    query = (
        select(Catalog)
        .join(FavoriteCatalog, FavoriteCatalog.catalog_id == Catalog.id)
        .where(FavoriteCatalog.user_id == current_user.id)
        .where(Catalog.deleted_at.is_(None))
        .options(
            selectinload(Catalog.images),
            selectinload(Catalog.workflow_history).options(
                selectinload(CatalogWorkFlow.user)
            ),
        )
    )

    result = await session.scalars(query)
    user_favorites = result.all()

    return {'favorites': user_favorites}


@router.delete('/{catalog_id}', response_model=Message)
async def delete_favorite(
    catalog_id: UUID,
    session: Session,
    current_user: CurrentUser,
):
    query = select(FavoriteCatalog).where(
        FavoriteCatalog.user_id == current_user.id,
        FavoriteCatalog.catalog_id == catalog_id,
    )
    db_favorite = await session.scalar(query)

    if not db_favorite:
        raise HTTPException(
            status_code=HTTPStatus.NOT_FOUND, detail='Favorite not found'
        )

    await session.delete(db_favorite)
    await session.commit()

    return {'message': 'Favorite removed successfully'}

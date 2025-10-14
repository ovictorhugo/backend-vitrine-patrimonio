from http import HTTPStatus
from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import func, select

from vitrine.dependencies import CurrentUser, Session
from vitrine.models import Collection
from vitrine.schemas import (
    CollectionList,
    CollectionPublic,
    CollectionSchema,
    CollectionUpdateSchema,
    FilterCollection,
    Message,
)

router = APIRouter(prefix='/collections', tags=['coleções - geral'])


@router.post(
    '/', status_code=HTTPStatus.CREATED, response_model=CollectionPublic
)
async def create_collection(
    collection: CollectionSchema,
    session: Session,
    current_user: CurrentUser,
):
    query = select(Collection).where(
        Collection.name == collection.name,
        Collection.user_id == current_user.id,
        Collection.deleted_at.is_(None),
    )
    if await session.scalar(query):
        raise HTTPException(
            status_code=HTTPStatus.CONFLICT,
            detail='You already have a collection with this name.',
        )

    db_collection = Collection(
        name=collection.name,
        description=collection.description,
        type=collection.type,
        user_id=current_user.id,
    )
    session.add(db_collection)
    await session.commit()
    await session.refresh(db_collection)

    db_collection.items = []
    return db_collection


@router.get('/', response_model=CollectionList)
async def read_collections(
    session: Session,
    filters: Annotated[FilterCollection, Depends()],
):
    query = select(Collection).where(Collection.deleted_at.is_(None))
    if filters.type:
        query = query.where(Collection.type == filters.type)
    query = query.offset(filters.offset).limit(filters.limit)
    result = await session.scalars(query)
    collections = result.all()
    return {'collections': collections}


@router.get('/my', response_model=CollectionList)
async def read_my_collections(
    session: Session,
    current_user: CurrentUser,
    filters: Annotated[FilterCollection, Depends()],
):
    query = select(Collection).where(
        Collection.deleted_at.is_(None),
        Collection.user_id == current_user.id,
    )
    if filters.type:
        query = query.where(Collection.type == filters.type)
    query = query.offset(filters.offset).limit(filters.limit)
    result = await session.scalars(query)
    collections = result.all()

    return {'collections': collections}


@router.get('/{collection_id}', response_model=CollectionPublic)
async def read_collection(
    collection_id: UUID, session: Session, current_user: CurrentUser
):
    query = select(Collection).where(
        Collection.id == collection_id,
        Collection.deleted_at.is_(None),
    )

    db_collection = await session.scalar(query)

    if not db_collection:
        raise HTTPException(
            status_code=HTTPStatus.NOT_FOUND, detail='Collection not found.'
        )

    if db_collection.user_id != current_user.id:
        raise HTTPException(
            status_code=HTTPStatus.FORBIDDEN,
            detail='You do not have permission to access this collection.',
        )

    return db_collection


@router.put('/{collection_id}', response_model=CollectionPublic)
async def update_collection(
    collection_id: UUID,
    collection_data: CollectionUpdateSchema,
    session: Session,
    current_user: CurrentUser,
):
    db_collection = await session.get(Collection, collection_id)

    if not db_collection or db_collection.deleted_at:
        raise HTTPException(
            status_code=HTTPStatus.NOT_FOUND, detail='Collection not found.'
        )
    if db_collection.user_id != current_user.id:
        raise HTTPException(
            status_code=HTTPStatus.FORBIDDEN,
            detail='You do not have permission to edit this collection.',
        )

    update_data = collection_data.model_dump(exclude_unset=True)
    for key, value in update_data.items():
        setattr(db_collection, key, value)

    await session.commit()
    await session.refresh(db_collection)
    return db_collection


@router.delete('/{collection_id}', response_model=Message)
async def delete_collection(
    collection_id: UUID, session: Session, current_user: CurrentUser
):
    db_collection = await session.get(Collection, collection_id)
    if not db_collection or db_collection.deleted_at:
        raise HTTPException(
            status_code=HTTPStatus.NOT_FOUND, detail='Collection not found.'
        )
    if db_collection.user_id != current_user.id:
        raise HTTPException(
            status_code=HTTPStatus.FORBIDDEN,
            detail='You do not have permission to delete this collection.',
        )

    db_collection.deleted_at = func.now()
    await session.commit()
    return {'message': 'Collection deactivated successfully.'}

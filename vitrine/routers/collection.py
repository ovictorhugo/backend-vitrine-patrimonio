from http import HTTPStatus
from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Body
from sqlalchemy import func, select
from sqlalchemy.orm import noload


from vitrine.core.dependencies import CurrentUser, Session
from vitrine.models import Collection, CollectionItem
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
        document_path=collection.document_path,
        sei_process=collection.sei_process,
        parecer=collection.parecer,
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

    query = query.options(noload('*'))

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


@router.get(
    '/stats/{collection_id}',
    status_code=HTTPStatus.OK,
)
async def get_collection_summary(
    collection_id: UUID,
    session: Session,
    current_user: CurrentUser,
):
    # 1. Valida se a coleção existe
    db_collection = await session.get(Collection, collection_id)
    if not db_collection or db_collection.deleted_at:
        raise HTTPException(
            status_code=HTTPStatus.NOT_FOUND, detail='Collection not found.'
        )

    # 2. Monta a query otimizada para buscar as duas contagens juntas
    query = select(
        func.count(CollectionItem.id).label('total'),
        func.count(CollectionItem.id).filter(CollectionItem.is_approved == True).label('approved')
    ).where(CollectionItem.collection_id == collection_id)

    # 3. Executa a query
    result = await session.execute(query)
    
    # O result.first() retorna a primeira (e única) linha com os nossos counts
    row = result.first()

    return {
        "total": row.total or 0,
        "approved": row.approved or 0
    }

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

    db_collection.deleted_at = func.now()
    await session.commit()
    return {'message': 'Collection deactivated successfully.'}


@router.post('/add-sei/{collection_id}', response_model=Message)
async def add_sei_process_to_collection(
    collection_id: UUID,
    session: Session,
    current_user: CurrentUser,
    sei_process: str | None = Body(default=None,embed=True)
):
    db_collection = await session.get(Collection, collection_id)
    
    if not db_collection or db_collection.deleted_at:
        raise HTTPException(
            status_code=HTTPStatus.NOT_FOUND, detail='Collection not found.'
        )

    if db_collection.sei_process and db_collection.sei_process.strip() and sei_process is not None:
        raise HTTPException(
            status_code=HTTPStatus.BAD_REQUEST, detail='Essa coleção já possui um Processo SEI vinculado.'
        )
    
    if sei_process is not None and not sei_process.strip():
        sei_process = None

    db_collection.sei_process = sei_process
    await session.commit()
    
    return {'message': 'Processo SEI adicionado com sucesso.'}
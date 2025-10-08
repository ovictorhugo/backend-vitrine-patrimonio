from http import HTTPStatus
from uuid import UUID

from fastapi import APIRouter, HTTPException
from sqlalchemy import func, select
from sqlalchemy.orm import selectinload

from vitrine.dependencies import CurrentUser, Session
from vitrine.models import Catalog, Collection, CollectionItem
from vitrine.schemas import (
    CollectionItemPublic,
    CollectionItemSchema,
    CollectionList,
    CollectionPublic,
    CollectionSchema,
    CollectionUpdateSchema,
    Message,
)

router = APIRouter(prefix='/collections', tags=['collections'])


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
            detail='Você já possui uma coleção com este nome.',
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


@router.get('/my', response_model=CollectionList)
async def read_my_collections(session: Session, current_user: CurrentUser):
    query = (
        select(Collection)
        .where(
            Collection.user_id == current_user.id,
            Collection.deleted_at.is_(None),
        )
        .options(
            selectinload(Collection.items).selectinload(CollectionItem.catalog)
        )
        .order_by(Collection.name)
    )
    result = await session.scalars(query)
    collections = result.all()
    return {'collections': collections}


@router.get('/{collection_id}', response_model=CollectionPublic)
async def read_collection(
    collection_id: UUID, session: Session, current_user: CurrentUser
):
    query = (
        select(Collection)
        .where(
            Collection.id == collection_id,
            Collection.deleted_at.is_(None),
        )
        .options(
            selectinload(Collection.items).selectinload(CollectionItem.catalog)
        )
    )
    db_collection = (await session.scalars(query)).unique().one_or_none()

    if not db_collection:
        raise HTTPException(
            status_code=HTTPStatus.NOT_FOUND, detail='Coleção não encontrada.'
        )
    if db_collection.user_id != current_user.id:
        raise HTTPException(
            status_code=HTTPStatus.FORBIDDEN,
            detail='Você não tem permissão para acessar esta coleção.',
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
            status_code=HTTPStatus.NOT_FOUND, detail='Coleção não encontrada.'
        )
    if db_collection.user_id != current_user.id:
        raise HTTPException(
            status_code=HTTPStatus.FORBIDDEN,
            detail='Você não tem permissão para editar esta coleção.',
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
            status_code=HTTPStatus.NOT_FOUND, detail='Coleção não encontrada.'
        )
    if db_collection.user_id != current_user.id:
        raise HTTPException(
            status_code=HTTPStatus.FORBIDDEN,
            detail='Você não tem permissão para deletar esta coleção.',
        )
    db_collection.deleted_at = func.now()
    await session.commit()
    return {'message': 'Coleção desativada com sucesso.'}


@router.post(
    '/{collection_id}/items',
    status_code=HTTPStatus.CREATED,
    response_model=CollectionItemPublic,
)
async def add_item_to_collection(
    collection_id: UUID,
    item: CollectionItemSchema,
    session: Session,
    current_user: CurrentUser,
):
    db_collection = await session.get(Collection, collection_id)
    if not db_collection or db_collection.deleted_at:
        raise HTTPException(
            status_code=HTTPStatus.NOT_FOUND, detail='Coleção não encontrada.'
        )
    if db_collection.user_id != current_user.id:
        raise HTTPException(
            status_code=HTTPStatus.FORBIDDEN,
            detail='Você não tem permissão para adicionar itens a esta coleção.',
        )

    if not await session.get(Catalog, item.catalog_id):
        raise HTTPException(
            status_code=HTTPStatus.NOT_FOUND,
            detail=f'O item de catálogo com ID "{item.catalog_id}" não foi encontrado.',
        )

    query = select(CollectionItem).where(
        CollectionItem.collection_id == collection_id,
        CollectionItem.catalog_id == item.catalog_id,
    )
    if await session.scalar(query):
        raise HTTPException(
            status_code=HTTPStatus.CONFLICT,
            detail='Este item já está na coleção.',
        )

    db_item = CollectionItem(
        collection_id=collection_id,
        catalog_id=item.catalog_id,
        status=item.status,
        comment=item.comment,
    )
    session.add(db_item)
    await session.commit()
    await session.refresh(db_item)
    return db_item


@router.delete('/{collection_id}/items/{item_id}', response_model=Message)
async def remove_item_from_collection(
    collection_id: UUID,
    item_id: UUID,
    session: Session,
    current_user: CurrentUser,
):
    db_collection = await session.get(Collection, collection_id)
    if not db_collection or db_collection.user_id != current_user.id:
        raise HTTPException(
            status_code=HTTPStatus.FORBIDDEN, detail='Ação não permitida.'
        )

    db_item = await session.get(CollectionItem, item_id)
    if not db_item or db_item.collection_id != collection_id:
        raise HTTPException(
            status_code=HTTPStatus.NOT_FOUND,
            detail='Item não encontrado nesta coleção.',
        )

    await session.delete(db_item)
    await session.commit()
    return {'message': 'Item removido da coleção com sucesso.'}

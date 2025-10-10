from http import HTTPStatus
from uuid import UUID

from fastapi import APIRouter, HTTPException
from sqlalchemy import select

from vitrine.dependencies import CurrentUser, Session
from vitrine.models import Catalog, Collection, CollectionItem
from vitrine.schemas import (
    CollectionItemPublic,
    CollectionItemSchema,
    Message,
)

router = APIRouter(
    prefix='/collections/{collection_id}/items', tags=['collection-items']
)


@router.post(
    '/',
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
            status_code=HTTPStatus.NOT_FOUND, detail='Collection not found.'
        )
    if db_collection.user_id != current_user.id:
        raise HTTPException(
            status_code=HTTPStatus.FORBIDDEN,
            detail='You do not have permission to add items to this collection.',
        )

    if not await session.get(Catalog, item.catalog_id):
        raise HTTPException(
            status_code=HTTPStatus.NOT_FOUND,
            detail=f'Catalog item with ID "{item.catalog_id}" not found.',
        )

    query = select(CollectionItem).where(
        CollectionItem.collection_id == collection_id,
        CollectionItem.catalog_id == item.catalog_id,
    )
    if await session.scalar(query):
        raise HTTPException(
            status_code=HTTPStatus.CONFLICT,
            detail='This item is already in the collection.',
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


@router.delete(
    '/{item_id}',
    status_code=HTTPStatus.OK,  # Ajustado: 200 OK é mais apropriado para um delete bem-sucedido que retorna uma mensagem
    response_model=Message,
)
async def remove_item_from_collection(
    collection_id: UUID,
    item_id: UUID,
    session: Session,
    current_user: CurrentUser,
):
    db_collection = await session.get(Collection, collection_id)
    if not db_collection or db_collection.user_id != current_user.id:
        raise HTTPException(
            status_code=HTTPStatus.FORBIDDEN, detail='Action not allowed.'
        )

    db_item = await session.get(CollectionItem, item_id)
    if not db_item or db_item.collection_id != collection_id:
        raise HTTPException(
            status_code=HTTPStatus.NOT_FOUND,
            detail='Item not found in this collection.',
        )

    await session.delete(db_item)
    await session.commit()
    return {'message': 'Item removed from the collection successfully.'}

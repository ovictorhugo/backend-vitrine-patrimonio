from http import HTTPStatus
from io import BytesIO
from pathlib import Path
from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import StreamingResponse
from sqlalchemy import select
from sqlalchemy.orm import selectinload
from weasyprint import HTML

from vitrine.core.dependencies import CurrentUser, Session
from vitrine.models import (
    Catalog,
    CatalogWorkFlow,
    Collection,
    CollectionItem,
    Location,
    LocationInventory,
    SystemIdentity,
    User,
    UserRole,
)
from vitrine.schemas import (
    CollectionItemPublic,
    CollectionItemSchema,
    CollectionItemsList,
    CollectionItemUpdate,
    FilterAsset,
    FilterCatalog,
    Message,
)
from vitrine.services import filter_service

from .utils import render_item_html

_ASSET_FIELDS = set(FilterAsset.model_fields.keys())
_NON_JOIN_FIELDS = {'limit', 'offset'}
ASSET_JOIN_TRIGGER_FIELDS = _ASSET_FIELDS - _NON_JOIN_FIELDS

router = APIRouter(
    prefix='/collections/{collection_id}/items',
    tags=['coleções - manipulação dos items'],
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

    query = (
        select(Catalog)
        .where(Catalog.id == item.catalog_id)
        .options(
            selectinload(Catalog.images),
            selectinload(Catalog.workflow_history).options(
                selectinload(CatalogWorkFlow.user).options(
                    selectinload(User.system_identity).options(
                        selectinload(SystemIdentity.legal_guardian)
                    ),
                    selectinload(User.user_role_associations).selectinload(
                        UserRole.role
                    ),
                ),
                selectinload(CatalogWorkFlow.transfer_requests),
            ),
            selectinload(Catalog.location)
            .selectinload(Location.location_inventories)
            .selectinload(LocationInventory.inventory),
        )
    )
    db_catalog = await session.scalar(query)

    if not db_catalog:
        raise HTTPException(
            status_code=HTTPStatus.NOT_FOUND,
            detail=f'Catalog item with ID "{item.catalog_id}" not found.',
        )

    query_existing = select(CollectionItem).where(
        CollectionItem.collection_id == collection_id,
        CollectionItem.catalog_id == item.catalog_id,
    )
    if await session.scalar(query_existing):
        raise HTTPException(
            status_code=HTTPStatus.CONFLICT,
            detail='This item is already in the collection.',
        )

    db_item = CollectionItem(
        collection_id=collection_id,
        catalog_id=item.catalog_id,
        status=item.status,
        comment=item.comment,
        is_locked=item.is_locked,
        is_approved=item.is_approved,
    )
    session.add(db_item)
    await session.commit()
    await session.refresh(db_item)

    db_item.catalog = db_catalog
    return db_item


@router.put(
    '/{item_id}',
    status_code=HTTPStatus.OK,
    response_model=CollectionItemPublic,
)
async def update_collection_item(
    collection_id: UUID,
    item_id: UUID,
    item_update: CollectionItemUpdate,
    session: Session,
    current_user: CurrentUser,
):
    db_collection = await session.get(Collection, collection_id)
    if not db_collection or db_collection.deleted_at:
        raise HTTPException(
            status_code=HTTPStatus.NOT_FOUND, detail='Collection not found.'
        )

    db_item = await session.get(CollectionItem, item_id)
    if not db_item or db_item.collection_id != collection_id:
        raise HTTPException(
            status_code=HTTPStatus.NOT_FOUND,
            detail='Item not found in this collection.',
        )

    existing_item_query = select(CollectionItem).where(
        CollectionItem.collection_id == collection_id,
        CollectionItem.id != item_id,
        CollectionItem.catalog_id == db_item.catalog_id,
    )
    if await session.scalar(existing_item_query):
        raise HTTPException(
            status_code=HTTPStatus.CONFLICT,
            detail='Another item with this catalog ID already exists in the collection.',
        )

    db_item.status = item_update.status
    db_item.comment = item_update.comment
    db_item.is_locked = item_update.is_locked
    db_item.is_approved = item_update.is_approved

    await session.commit()
    await session.refresh(db_item)

    query = (
        select(Catalog)
        .options(
            selectinload(Catalog.images),
            selectinload(Catalog.workflow_history).options(
                selectinload(CatalogWorkFlow.user).options(
                    selectinload(User.system_identity).options(
                        selectinload(SystemIdentity.legal_guardian)
                    ),
                    selectinload(User.user_role_associations).selectinload(
                        UserRole.role
                    ),
                ),
                selectinload(CatalogWorkFlow.transfer_requests),
            ),
            selectinload(Catalog.location)
            .selectinload(Location.location_inventories)
            .selectinload(LocationInventory.inventory),
        )
        .where(Catalog.id == db_item.catalog_id)
    )
    result = await session.execute(query)
    db_catalog = result.scalar_one()
    db_item.catalog = db_catalog

    return db_item


@router.get(
    '/',
    status_code=HTTPStatus.OK,
    response_model=CollectionItemsList,
)
async def list_collection_items(
    collection_id: UUID,
    session: Session,
    filters: Annotated[FilterCatalog, Depends()],
):
    db_collection = await session.get(Collection, collection_id)
    if not db_collection or db_collection.deleted_at:
        raise HTTPException(
            status_code=HTTPStatus.NOT_FOUND, detail='Collection not found.'
        )

    query = (
        select(CollectionItem)
        .join(CollectionItem.catalog)
        .where(CollectionItem.collection_id == collection_id)
        .options(
            selectinload(CollectionItem.catalog).options(
                selectinload(Catalog.images),
                selectinload(Catalog.workflow_history).options(
                    selectinload(CatalogWorkFlow.user).options(
                        selectinload(User.system_identity).options(
                            selectinload(SystemIdentity.legal_guardian)
                        ),
                        selectinload(User.user_role_associations).selectinload(
                            UserRole.role
                        ),
                    ),
                    selectinload(CatalogWorkFlow.transfer_requests),
                ),
                selectinload(Catalog.location)
                .selectinload(Location.location_inventories)
                .selectinload(LocationInventory.inventory),
            )
        )
    )

    asset_join_needed = any(
        getattr(filters, field_name) is not None
        for field_name in ASSET_JOIN_TRIGGER_FIELDS
    )

    query = filter_service.apply_catalog_filters(query, filters)

    if asset_join_needed:
        query = query.join(Catalog.asset)
        query = filter_service.apply_asset_filters(query, filters)

    query = query.offset(filters.offset).limit(filters.limit)

    result = await session.execute(query)
    items = result.scalars().all()
    return {'collection_items': items}


@router.delete(
    '/{item_id}',
    status_code=HTTPStatus.OK,
    response_model=Message,
)
async def remove_item_from_collection(
    collection_id: UUID,
    item_id: UUID,
    session: Session,
    current_user: CurrentUser,
):
    db_collection = await session.get(Collection, collection_id)
    if not db_collection:
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


@router.get(
    '/pdf',
    status_code=HTTPStatus.OK,
)
async def list_collection_items(
    collection_id: UUID,
    session: Session,
    filters: Annotated[FilterCatalog, Depends()],
):
    db_collection = await session.get(Collection, collection_id)
    if not db_collection or db_collection.deleted_at:
        raise HTTPException(
            status_code=HTTPStatus.NOT_FOUND, detail='Collection not found.'
        )

    query = (
        select(CollectionItem)
        .join(CollectionItem.catalog)
        .where(CollectionItem.collection_id == collection_id)
        .options(
            selectinload(CollectionItem.catalog).options(
                selectinload(Catalog.images),
                selectinload(Catalog.workflow_history).options(
                    selectinload(CatalogWorkFlow.user).options(
                        selectinload(User.system_identity).options(
                            selectinload(SystemIdentity.legal_guardian)
                        ),
                        selectinload(User.user_role_associations).selectinload(
                            UserRole.role
                        ),
                    ),
                    selectinload(CatalogWorkFlow.transfer_requests),
                ),
                selectinload(Catalog.location)
                .selectinload(Location.location_inventories)
                .selectinload(LocationInventory.inventory),
            )
        )
    )

    asset_join_needed = any(
        getattr(filters, field_name) is not None
        for field_name in ASSET_JOIN_TRIGGER_FIELDS
    )

    query = filter_service.apply_catalog_filters(query, filters)

    if asset_join_needed:
        query = query.join(Catalog.asset)
        query = filter_service.apply_asset_filters(query, filters)

    query = query.offset(filters.offset).limit(filters.limit)

    result = await session.execute(query)
    items = result.scalars().all()

    entries = [item.catalog for item in items]
    total = len(entries)

    items_html = ''.join(
        render_item_html(entry, idx, total)
        for idx, entry in enumerate(entries)
    )

    ASSETS_DIR = (
        Path(__file__).resolve().parent.parent.parent / 'assets'
    ).resolve()
    lexend_regular = (ASSETS_DIR / 'Lexend-Regular.ttf').resolve().as_uri()

    full_html = f"""
              <!DOCTYPE html>
              <html lang="pt-br">
              <head>
                  <meta charset="utf-8" />
                  <title>Relatório</title>
                  <style>
                      @page {{
                        size: A4;
                        margin: 0; /* Remove margens do PDF para controlarmos no HTML */
                      }}
                      
                      html, body {{
                          margin: 0;
                          padding: 0;
                          height: 100%; /* Garante altura total */
                          background-color: #ffffff;
                          font-family: "Lexend","Lexend-Bold", sans-serif;
                          font-size: 10px;
                      }}
                      @font-face {{
                                font-family: Lexend;
                                src: url({lexend_regular})
                            }}                      
                      img {{ max-width: 100%; }}
                  </style>
              </head>
              <body>
                  {items_html}
              </body>
              </html>
        """
    pdf_bytes: bytes = HTML(string=full_html, encoding='utf-8').write_pdf()

    return StreamingResponse(
        BytesIO(pdf_bytes),
        media_type='application/pdf',
        headers={
            'Content-Disposition': 'inline; filename="catalogo.pdf"',
        },
    )

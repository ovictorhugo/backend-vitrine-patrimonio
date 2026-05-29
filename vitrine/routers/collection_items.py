from http import HTTPStatus
from io import BytesIO
from pathlib import Path
from typing import Annotated, List
from uuid import UUID
from pydantic import BaseModel

from fastapi import APIRouter, Depends, HTTPException, Body
from fastapi.responses import StreamingResponse
from sqlalchemy import select, delete, desc
from sqlalchemy.orm import selectinload, noload
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
    prefix='/collection_items',
    tags=['coleções - manipulação dos items'],
)

@router.post(
    '/add_new/{collection_id}',
    status_code=HTTPStatus.OK,
)
async def add_collection_items(
    collection_id: UUID,
    session: Session,
    catalog_ids: list[UUID] = Body(embed=True)
):
    db_collection = await session.get(Collection, collection_id)
    if not db_collection or db_collection.deleted_at:
        raise HTTPException(
            status_code=HTTPStatus.NOT_FOUND, detail='Collection not found.'
        )

    if db_collection.sei_process is not None:
        raise HTTPException(
            status_code=HTTPStatus.BAD_REQUEST, detail='Itens não podem ser adicionados em coleções com Processo SEI vinculado.'
        )

    success_count = 0
    fail_count = 0

    if catalog_ids:
        approved_query = select(CollectionItem.catalog_id).where(
            CollectionItem.catalog_id.in_(catalog_ids),
            CollectionItem.is_approved == True
        )
        approved_result = await session.execute(approved_query)
        approved_catalog_ids = set(approved_result.scalars().all())
    else:
        approved_catalog_ids = set()

    for cid in catalog_ids:
        if cid in approved_catalog_ids:
            fail_count += 1
            continue
        try:
            async with session.begin_nested():
                new_item = CollectionItem(
                    collection_id=collection_id,
                    catalog_id=cid,
                    comment="",
                    status=False,
                    is_approved=False
                )
                session.add(new_item)
            success_count += 1
        except Exception:
            fail_count += 1

    await session.commit()

    if success_count == 0 and len(catalog_ids) > 0:
        return {"message": "Nenhum item foi adicionado."}
    if fail_count > 0:
        return {"message": "Alguns itens não foram adicionados com sucesso."}

    return {"message": "Itens adicionados com sucesso."}

@router.post(
    '/{collection_id}',
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

    if db_collection.sei_process is not None:
        raise HTTPException(
            status_code=HTTPStatus.BAD_REQUEST, detail='Itens não podem ser adicionados em coleções com Processo SEI vinculado.'
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
        is_approved=item.is_approved,
    )
    session.add(db_item)
    await session.commit()
    await session.refresh(db_item)

    db_item.catalog = db_catalog
    return db_item


@router.put(
    '/{collection_id}/{item_id}',
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
    '/{collection_id}',
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
    '/remove_by_filters/{collection_id}',
    status_code=HTTPStatus.OK,
)
async def remove_items_by_filters(
    collection_id: UUID,
    session: Session,
    current_user: CurrentUser,
    filters: Annotated[FilterCatalog, Depends()],
):
    # 1. Valida se a coleção existe
    db_collection = await session.get(Collection, collection_id)
    if not db_collection or db_collection.deleted_at:
        raise HTTPException(
            status_code=HTTPStatus.NOT_FOUND, detail='Collection not found.'
        )

    if db_collection.sei_process is not None:
        raise HTTPException(
            status_code=HTTPStatus.BAD_REQUEST, detail='Itens não podem ser removidos de coleções com Processo SEI vinculado.'
        )

    # 2. Monta a Query buscando os itens DENTRO da coleção atual
    query = (
        select(CollectionItem)
        .join(CollectionItem.catalog)
        .where(CollectionItem.collection_id == collection_id)
    )

    asset_join_needed = any(
        getattr(filters, field_name) is not None
        for field_name in ASSET_JOIN_TRIGGER_FIELDS
    )

    # Aplica os mesmos filtros que você já usa
    query = filter_service.apply_catalog_filters(query, filters)

    if asset_join_needed:
        query = query.join(Catalog.asset)
        query = filter_service.apply_asset_filters(query, filters)

    # 3. Executa a busca
    result = await session.execute(query)
    collection_items = result.scalars().all()

    if not collection_items:
        return {"message": "Nenhum item encontrado na coleção para os filtros informados."}

    # 4. Lógica de remoção individual com tolerância a falhas
    success_count = 0
    fail_count = 0

    for item in collection_items:
        try:
            # O begin_nested protege a transação. Se a deleção de um item 
            # falhar (ex: por alguma trava de chave estrangeira), os outros 
            # ainda serão deletados normalmente.
            async with session.begin_nested():
                await session.delete(item)
            success_count += 1
        except Exception:
            fail_count += 1

    await session.commit()

    # 5. Validação das mensagens de retorno
    if success_count == 0:
        return {"message": "Nenhum item foi removido."}
    
    if fail_count > 0:
        return {"message": "Alguns itens não foram removidos com sucesso."}

    return {"message": "Itens removidos com sucesso."}

@router.delete(
    '/{collection_id}/{item_id}',
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

    if db_collection.sei_process is not None:
        raise HTTPException(
            status_code=HTTPStatus.BAD_REQUEST, detail='Itens não podem ser removidos de coleções com Processo SEI vinculado.'
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
        Path(__file__).resolve().parent.parent / 'assets'
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


@router.post(
    '/add_by_filters/{collection_id}',
    status_code=HTTPStatus.OK,
)
async def add_items_by_filters(
    collection_id: UUID,
    session: Session,
    filters: Annotated[FilterCatalog, Depends()],
):
    # 1. Valida se a coleção existe
    db_collection = await session.get(Collection, collection_id)
    if not db_collection or db_collection.deleted_at:
        raise HTTPException(
            status_code=HTTPStatus.NOT_FOUND, detail='Collection not found.'
        )

    if db_collection.sei_process is not None:
        raise HTTPException(
            status_code=HTTPStatus.BAD_REQUEST, detail='Itens não podem ser adicionados em coleções com Processo SEI vinculado.'
        )

    

    # 2. Monta a Query buscando no Catálogo principal
    query = select(Catalog).where(Catalog.deleted_at.is_(None))

    asset_join_needed = any(
        getattr(filters, field_name) is not None
        for field_name in ASSET_JOIN_TRIGGER_FIELDS
    )

    query = filter_service.apply_catalog_filters(query, filters)

    if asset_join_needed:
        query = query.join(Catalog.asset)
        query = filter_service.apply_asset_filters(query, filters)

    # 3. Executa a busca
    result = await session.execute(query)
    catalogs = result.scalars().all()

    if not catalogs:
        return {"message": "Nenhum item encontrado para os filtros informados."}

    catalog_ids_to_add = [c.id for c in catalogs]
    approved_query = select(CollectionItem.catalog_id).where(
        CollectionItem.catalog_id.in_(catalog_ids_to_add),
        CollectionItem.is_approved == True
    )
    approved_result = await session.execute(approved_query)
    approved_catalog_ids = set(approved_result.scalars().all())

    # 4. Lógica de inserção individual com tolerância a falhas
    success_count = 0
    fail_count = 0

    for catalog in catalogs:
        if catalog.id in approved_catalog_ids:
            fail_count += 1
            continue
        try:
            # O begin_nested cria um "Savepoint". Se o item já existir na coleção
            # e disparar um erro de UniqueConstraint no banco, ele reverte apenas
            # este item e continua o loop normalmente.
            async with session.begin_nested():
                new_item = CollectionItem(
                    collection_id=collection_id,
                    catalog_id=catalog.id,
                    comment="",
                    status=False,
                    is_approved=False
                )
                session.add(new_item)
            success_count += 1
        except Exception:
            fail_count += 1

    await session.commit()

    # 5. Validação das mensagens de retorno
    if success_count == 0:
        return {"message": "Nenhum item foi adicionado."}
    
    if fail_count > 0:
        return {"message": "Alguns itens não foram adicionados com sucesso."}

    return {"message": "Itens adicionados com sucesso."}


@router.post(
    '/approved/{collection_id}',
    status_code=HTTPStatus.OK,
)
async def approve_collection_items(
    collection_id: UUID,
    session: Session,
    catalog_ids: list[UUID] = Body(embed=True)
):
    # 1. Valida a coleção
    db_collection = await session.get(Collection, collection_id)
    if not db_collection or db_collection.deleted_at:
        raise HTTPException(
            status_code=HTTPStatus.NOT_FOUND, detail='Collection not found.'
        )

    success_count = 0
    fail_count = 0

    if not catalog_ids:
        return {"message": "Nenhum item válido ou destrancado foi encontrado para aprovação."}

    query = (
        select(CollectionItem, Catalog)
        .join(Catalog, CollectionItem.catalog_id == Catalog.id)
        .where(
            CollectionItem.collection_id == collection_id,
            CollectionItem.catalog_id.in_(catalog_ids)
        )
        .options(noload('*'))
    )
    result = await session.execute(query)
    rows = result.all()
    
    row_map = {item.catalog_id: (item, catalog) for item, catalog in rows}

    for catalog_id in catalog_ids:
        try:
            async with session.begin_nested():
                if catalog_id in row_map:
                    item, catalog = row_map[catalog_id]

                    # Aplica as regras de negócio
                    if not getattr(item, "is_approved", False):
                        item.is_approved = True
                        
                        # Atualiza o status do workflow no catálogo principal
                        catalog.current_workflow_status = "EM_REMOCAO"
                        
                        # Remove os demais registros deste item
                        delete_query = delete(CollectionItem).where(
                            CollectionItem.catalog_id == catalog_id,
                            CollectionItem.id != item.id
                        )
                        await session.execute(delete_query)

                        success_count += 1
                    
        except Exception as e:
            print(f"Erro ao processar o catalog_id {catalog_id}: {e}")
            fail_count += 1

    # Efetiva todas as alterações (CollectionItem e Catalog)
    await session.commit()

    # Validação do retorno
    if success_count == 0 and fail_count == 0:
        return {"message": "Nenhum item válido ou destrancado foi encontrado para aprovação."}
    
    if fail_count > 0:
        return {"message": f"Operação parcial: {success_count} aprovados, {fail_count} falharam."}

    return {"message": "Itens aprovados e atualizados com sucesso."}

@router.post(
    '/refused/{collection_id}',
    status_code=HTTPStatus.OK,
)
async def refuse_collection_items(
    collection_id: UUID,
    session: Session,
    current_user: CurrentUser,
    catalog_ids: list[UUID] = Body(embed=True)
):
    from datetime import datetime
    
    # 1. Valida a coleção
    db_collection = await session.get(Collection, collection_id)
    if not db_collection or db_collection.deleted_at:
        raise HTTPException(
            status_code=HTTPStatus.NOT_FOUND, detail='Collection not found.'
        )

    success_count = 0
    fail_count = 0

    if not catalog_ids:
        return {"message": "Nenhum item válido foi encontrado para recusa."}

    today_str = datetime.now().strftime("%d/%m/%Y")
    comment_text = f"PRA recusou a remoção deste item em {today_str}"

    query = (
        select(CollectionItem, Catalog)
        .join(Catalog, CollectionItem.catalog_id == Catalog.id)
        .where(
            CollectionItem.collection_id == collection_id,
            CollectionItem.catalog_id.in_(catalog_ids)
        )
        .options(noload('*'))
    )
    result = await session.execute(query)
    rows = result.all()
    
    row_map = {item.catalog_id: (item, catalog) for item, catalog in rows}

    for catalog_id in catalog_ids:
        try:
            async with session.begin_nested():
                if catalog_id in row_map:
                    item, catalog = row_map[catalog_id]
                else:
                    catalog = await session.get(Catalog, catalog_id, options=[noload('*')])
                    item = None

                if not catalog:
                    continue

                if item:
                    # Remover item da coleção
                    await session.delete(item)

                # Alterar status
                catalog.current_workflow_status = "REJEITADOS_COMISSAO"

                # Adicionar workflow
                new_workflow_entry = CatalogWorkFlow(
                    catalog_id=catalog.id,
                    user_id=current_user.id,
                    workflow_status="REJEITADOS_COMISSAO",
                    detail={"justificativa": comment_text, "observation": {"text": comment_text}},
                )
                session.add(new_workflow_entry)

                success_count += 1
                    
        except Exception as e:
            print(f"Erro ao processar o catalog_id {catalog_id}: {e}")
            fail_count += 1

    await session.commit()

    if success_count == 0 and fail_count == 0:
        return {"message": "Nenhum item válido foi encontrado para recusa."}
    
    if fail_count > 0:
        return {"message": f"Operação parcial: {success_count} recusados, {fail_count} falharam."}

    return {"message": "Itens recusados, removidos da coleção e atualizados com sucesso."}
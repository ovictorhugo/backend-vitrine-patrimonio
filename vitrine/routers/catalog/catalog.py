import gc
from datetime import datetime
from http import HTTPStatus
from io import BytesIO
from pathlib import Path
from typing import Annotated
from uuid import UUID
import tempfile
import os
import pikepdf
import math

from fastapi import APIRouter, Depends, HTTPException
from fastapi.concurrency import run_in_threadpool
from fastapi.responses import StreamingResponse
from sqlalchemy import select, func, desc, exists
from sqlalchemy.orm import selectinload, joinedload, contains_eager, noload
from weasyprint import HTML
from playwright.async_api import async_playwright

from vitrine.core.dependencies import CurrentUser, Session
from vitrine.models import (
    Catalog,
    CatalogWorkFlow,
    Location,
    LocationInventory,
    Role,
    SystemIdentity,
    User,
    UserRole,
    WorkFlowStatus,
    WorkflowTransfer,
)
from vitrine.schemas import (
    SimpleCatalogList,
    CatalogList,
    CatalogPublic,
    CatalogSchema,
    FilterAsset,
    FilterCatalog,
    Message,
)
from vitrine.services import filter_service

from ..utils import render_item_html, render_multiple_items

_ASSET_FIELDS = set(FilterAsset.model_fields.keys())
_NON_JOIN_FIELDS = {'limit', 'offset'}
ASSET_JOIN_TRIGGER_FIELDS = _ASSET_FIELDS - _NON_JOIN_FIELDS


router = APIRouter(prefix='/catalog', tags=['Vitrine - Anúncios'])

def render_document_batch(html_content: str):
    return HTML(string=html_content, encoding='utf-8').render()


@router.post('/', status_code=HTTPStatus.CREATED, response_model=CatalogPublic)
async def create_catalog_entry(
    catalog_data: CatalogSchema,
    session: Session,
    current_user: CurrentUser,
):
    query = select(Catalog).where(Catalog.asset_id == catalog_data.asset_id)
    db_catalog_check = await session.scalar(query)
    if db_catalog_check:
        raise HTTPException(
            status_code=HTTPStatus.CONFLICT,
            detail='Catalog entry for this asset already exists',
        )

    db_catalog = Catalog(
        asset_id=catalog_data.asset_id,
        situation=catalog_data.situation,
        conservation_status=catalog_data.conservation_status,
        description=catalog_data.description,
        location_id=catalog_data.location_id,
        user_id=current_user.id,
    )
    session.add(db_catalog)
    await session.flush()

    if catalog_data.situation in {'UNECONOMICAL', 'BROKEN'}:
        status = WorkFlowStatus.REVIEW_REQUESTED_DESFAZIMENTO
    if catalog_data.situation in {'UNUSED', 'RECOVERABLE'}:
        status = WorkFlowStatus.REVIEW_REQUESTED_VITRINE

    workflow = CatalogWorkFlow(
        catalog_id=db_catalog.id,
        user_id=current_user.id,
        workflow_status=status,
        detail={},
    )
    session.add(workflow)
    await session.commit()

    query = (
        select(Catalog)
        .where(Catalog.id == db_catalog.id)
        .options(
            selectinload(Catalog.images),
            selectinload(Catalog.files),
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
    created_catalog = await session.scalar(query)
    return created_catalog

@router.get('/', response_model=CatalogList)
async def read_catalog_entries(
    session: Session, filters: Annotated[FilterCatalog, Depends()]
):
    # Iniciamos a query base
    query = select(Catalog).where(Catalog.deleted_at.is_(None))

    # Verifica se precisamos do join com Asset para os filtros
    asset_join_needed = any(
        getattr(filters, field_name) is not None
        for field_name in ASSET_JOIN_TRIGGER_FIELDS
    )

    # 1. Aplica os filtros de Catálogo (que agora usa a nova coluna workflow_status)
    query = filter_service.apply_catalog_filters(query, filters)

    # 2. Aplica os filtros de Asset (usando joinedload se não for filtrar, ou contains_eager se for)
    if asset_join_needed:
        query = query.join(Catalog.asset)
        query = filter_service.apply_asset_filters(query, filters)
        # Se já fizemos o join para filtrar, usamos contains_eager para carregar os dados
        query = query.options(contains_eager(Catalog.asset))
    else:
        # Se não filtramos por asset, apenas trazemos os dados de forma otimizada
        query = query.options(joinedload(Catalog.asset))

    # 3. Opções de carregamento para os campos aninhados (mantenha apenas o necessário para a resposta)
    query = query.options(
        selectinload(Catalog.workflow_history).options(
            selectinload(CatalogWorkFlow.transfer_requests).options(
                selectinload(WorkflowTransfer.location)
            ),
            selectinload(CatalogWorkFlow.user).options(
                selectinload(User.system_identity).options(
                    selectinload(SystemIdentity.legal_guardian)
                ),
                selectinload(User.user_role_associations).selectinload(
                    UserRole.role
                ),
            ),
        ),
        selectinload(Catalog.user).options(selectinload(User.system_identity)),
    )

    # 4. Filtros de Usuário e Role
    if filters.user_id:
        query = query.where(Catalog.user_id == filters.user_id)

    if filters.role_id:
        # Otimização: Substituindo CTE por EXISTS
        role_subq = (
            select(1)
            .select_from(UserRole)
            .join(Role, Role.id == UserRole.role_id)
            .where(
                UserRole.user_id == Catalog.user_id,
                Role.id == filters.role_id,
                UserRole.deleted_at.is_(None),
                Role.deleted_at.is_(None)
            )
        )
        query = query.where(exists(role_subq))

    # 5. Ordenação e Paginação (CRÍTICO para performance com offset)
    query = query.order_by(desc(Catalog.created_at))
    query = query.offset(filters.offset).limit(filters.limit)

    result = await session.scalars(query)
    # Use .unique() se houver risco de duplicatas devido aos joins de coleções
    entries = result.unique().all()

    return {'catalog_entries': entries}


@router.get('/cards', response_model=SimpleCatalogList)
async def read_catalog_entries(
    session: Session, filters: Annotated[FilterCatalog, Depends()]
):
    query = select(Catalog).where(Catalog.deleted_at.is_(None))

    asset_join_needed = any(
        getattr(filters, field_name) is not None
        for field_name in ASSET_JOIN_TRIGGER_FIELDS
    )

    # 1. Aplica filtros de catálogo
    query = filter_service.apply_catalog_filters(query, filters)

    # 2. Aplica filtros e carregamento do Asset
    if asset_join_needed:
        query = query.join(Catalog.asset)
        query = filter_service.apply_asset_filters(query, filters)
        query = query.options(contains_eager(Catalog.asset))
    else:
        query = query.options(joinedload(Catalog.asset))

    # 3. Opções de Carregamento (O SEGREDO ESTÁ AQUI)
    query = query.options(
        # Garantir o que queremos (já é o padrão, mas fica explícito):
        selectinload(Catalog.images),
        selectinload(Catalog.user),
        
        # DESLIGAR o que o SQLAlchemy tentaria carregar sozinho em background:
        noload(Catalog.location),
        noload(Catalog.files),
        noload(Catalog.workflow_history),
        noload(Catalog.favorited_by),
        noload(Catalog.collection_items),
    )

    # 4. Filtros de Usuário e Role
    if filters.user_id:
        query = query.where(Catalog.user_id == filters.user_id)

    if filters.role_id:
        role_subq = (
            select(1)
            .select_from(UserRole)
            .join(Role, Role.id == UserRole.role_id)
            .where(
                UserRole.user_id == Catalog.user_id,
                Role.id == filters.role_id,
                UserRole.deleted_at.is_(None),
                Role.deleted_at.is_(None)
            )
        )
        query = query.where(exists(role_subq))

    # 5. Ordenação e Paginação
    query = query.order_by(desc(Catalog.created_at))
    query = query.offset(filters.offset).limit(filters.limit)

    result = await session.scalars(query)
    entries = result.unique().all()

    return {'catalog_entries': entries}


@router.get('/pdf_play')
async def export_catalog_pdf(
    session: Session,
    filters: Annotated[FilterCatalog, Depends()],
):
    # --- 1. QUERY E CONTAGEM (Mantido intacto) ---
    base_query = select(Catalog).where(Catalog.deleted_at.is_(None))
    base_query = filter_service.apply_catalog_filters(base_query, filters)

    asset_join_needed = any(
        getattr(filters, field_name) is not None
        for field_name in ASSET_JOIN_TRIGGER_FIELDS
    )

    if asset_join_needed:
        base_query = base_query.join(Catalog.asset)
        base_query = filter_service.apply_asset_filters(base_query, filters)

    if filters.user_id:
        base_query = base_query.where(Catalog.user_id == filters.user_id)

    if filters.role_id:
        users_with_role = (
            select(UserRole.user_id)
            .join(Role, Role.id == UserRole.role_id)
            .where(
                Role.id == filters.role_id,
                UserRole.deleted_at.is_(None),
                Role.deleted_at.is_(None),
            )
            .distinct()
            .cte('users_with_role')
        )
        base_query = base_query.join(
            users_with_role, Catalog.user_id == users_with_role.c.user_id
        )

    count_query = select(func.count()).select_from(base_query.subquery())
    total_items = await session.scalar(count_query) or 0
    print(f'Existe um total de: {total_items} resultados')

    if total_items == 0:
        raise HTTPException(status_code=404, detail='Nenhum catálogo encontrado')

    base_query = base_query.options(
        selectinload(Catalog.images),
        selectinload(Catalog.workflow_history).options(
            selectinload(CatalogWorkFlow.user).options(
                selectinload(User.system_identity).options(
                    selectinload(SystemIdentity.legal_guardian)
                ),
            )
        ),
        selectinload(Catalog.location)
    )

    # --- 2. CÁLCULOS DE PAGINAÇÃO ---
    BATCH_SIZE = 30 # Tem que ser número PAR para fechar folhas completas
    TOTAL_PAGES = math.ceil(total_items / 2) # Como são 2 itens por pág, dividimos por 2

    ASSETS_DIR = (Path(__file__).resolve().parent.parent.parent / 'assets').resolve()
    lexend_regular = (ASSETS_DIR / 'Lexend-Variable.ttf').resolve().as_uri()
    EE_LOGO_URI = (ASSETS_DIR / "ee_logo.png").resolve().as_uri()
    SP_LOGO_URI = (ASSETS_DIR / "sp_logo.png").resolve().as_uri()

    temp_files_to_cleanup = []
    temp_pdf_paths = []

    try:
        # --- 3. GERAÇÃO COM PLAYWRIGHT ---
        async with async_playwright() as p:
            browser = await p.chromium.launch(
                headless=True,
                args=[
                    '--no-sandbox', 
                    '--disable-setuid-sandbox', 
                    '--disable-dev-shm-usage',
                    '--allow-file-access-from-files'
                ]
            )

            for offset in range(0, total_items, BATCH_SIZE):
                batch_query = base_query.offset(offset).limit(BATCH_SIZE)
                result = await session.scalars(batch_query)
                entries = result.unique().all()

                if not entries:
                    break

                batch_pages_html = []
                
                # Agrupa os itens do lote de 2 em 2
                for i in range(0, len(entries), 2):
                    item1 = entries[i]
                    item2 = entries[i+1] if i+1 < len(entries) else None
                    
                    # Calcula qual é a página global atual
                    absolute_idx = offset + i
                    current_page_number = (absolute_idx // 2) + 1

                    # Chama a função que renderiza APENAS O BLOCO DO ITEM
                    item1_html = render_multiple_items(item1)
                    item2_html = render_multiple_items(item2) if item2 else ""

                    # ESTRUTURA DE UMA FOLHA A4 INDIVIDUAL
                    page_html = f"""
                    <div class="folha-a4">
                        <table style="width: 100%; margin-bottom: 15px;padding: 0 12px">
                            <tr>
                                <td style="width: 49%; vertical-align: middle;">
                                    <img src="{SP_LOGO_URI}" style="height: 36px; max-width: 120px; object-fit: contain;" />
                                </td>
                                <td style="width: 49%; vertical-align: middle; text-align: right;">
                                    <img src="{EE_LOGO_URI}" style="height: 36px; max-width: 120px; object-fit: contain;" />
                                </td>
                            </tr>
                        </table>

                        <div class="conteudo-itens">
                            {item1_html}
                            {item2_html}
                        </div>

                        <div class="rodape">
                            <table style="width: 100%; border-collapse: collapse;">
                                <tr>
                                    <td style="text-align: center; padding-bottom: 6px;">
                                         <p style="margin: 0; color: #6b7280; font-size: 11px; font-weight: 500;">
                                            Av. Presidente Antônio Carlos, nº 6.627, Belo Horizonte/MG - CEP: 31.270-901
                                          </p>
                                    </td>
                                </tr>
                                <tr>
                                    <td style="text-align: right; color: #6b7280; font-size: 10px;">
                                        Página {current_page_number} de {TOTAL_PAGES}
                                    </td>
                                </tr>
                            </table>
                        </div>
                    </div>
                    """
                    batch_pages_html.append(page_html)

                # Junta todas as páginas deste lote em um único documento HTML
                final_batch_html_content = "".join(batch_pages_html)

                batch_full_html = f"""
                    <!DOCTYPE html>
                    <html lang="pt-br">
                    <head>
                        <meta charset="utf-8" />
                        <style>
                            @page {{
                                size: A4;
                                margin: 0; /* Remove a margem nativa do Chrome */
                            }}
                            html, body {{
                                margin: 0; padding: 0; background-color: #f9fafb;
                                font-family: "Lexend", sans-serif; font-size: 10px;
                            }}
                            @font-face {{ font-family: Lexend; src: url({lexend_regular}); }}
                            
                            .folha-a4 {{
                                position: relative;
                                width: 210mm;
                                height: 296.5mm;
                                box-sizing: border-box;
                                padding: 20px 0 80px 0;
                                page-break-after: always; /* Força quebra de página */
                                overflow: hidden; /* Corta o que sobrar */
                            }}

                            .conteudo-itens {{
                                width: 100%;
                            }}

                            .rodape {{
                                position: absolute;
                                bottom: 15px;
                                left: 30px;
                                right: 30px;
                                border-top: 1px solid #e5e7eb;
                                padding-top: 10px;
                            }}
                        </style>
                    </head>
                    <body>
                        {final_batch_html_content}
                    </body>
                    </html>
                """

                # Salva o HTML temporário do lote
                with tempfile.NamedTemporaryFile(delete=False, suffix='.html', mode='w', encoding='utf-8') as f:
                    f.write(batch_full_html)
                    temp_html_path = f.name
                
                temp_pdf_path = temp_html_path.replace('.html', '.pdf')
                temp_files_to_cleanup.extend([temp_html_path, temp_pdf_path])

                context = await browser.new_context()
                page = await context.new_page()
                
                await page.goto(f"file://{temp_html_path}", wait_until="networkidle")
                await page.pdf(
                    path=temp_pdf_path,
                    format="A4",
                    print_background=True,
                    margin={"top": "0", "right": "0", "bottom": "0", "left": "0"} # Margem nativa zerada
                )
                
                await page.close()
                await context.close()

                temp_pdf_paths.append(temp_pdf_path)
                print(f"Lote processado (offset: {offset})")

            await browser.close()


        # --- 4. UNIÃO DOS PDFs COM PIKEPDF ---
        final_pdf = pikepdf.Pdf.new()
        opened_pdfs = [] 

        for pdf_path in temp_pdf_paths:
            src_pdf = pikepdf.Pdf.open(pdf_path)
            opened_pdfs.append(src_pdf)
            final_pdf.pages.extend(src_pdf.pages)

        pdf_bytes_io = BytesIO()
        final_pdf.save(pdf_bytes_io)
        pdf_bytes_io.seek(0)

        for src_pdf in opened_pdfs:
            src_pdf.close()

        return StreamingResponse(
            pdf_bytes_io,
            media_type='application/pdf',
            headers={
                'Content-Disposition': 'inline; filename="catalogo_completo.pdf"',
            },
        )

    except Exception as e:
        print(f'Erro PDF: {e}')
        raise HTTPException(
            status_code=500, detail='Erro interno ao gerar PDF'
        )
        
    finally:
        for filepath in temp_files_to_cleanup:
            if os.path.exists(filepath):
                try:
                    os.remove(filepath)
                except Exception as e:
                    pass

@router.get('/pdf')
async def export_catalog_pdf(
    session: Session,
    filters: Annotated[FilterCatalog, Depends()],
):
    query = select(Catalog).where(Catalog.deleted_at.is_(None))

    asset_join_needed = any(
        getattr(filters, field_name) is not None
        for field_name in ASSET_JOIN_TRIGGER_FIELDS
    )

    query = filter_service.apply_catalog_filters(query, filters)

    if asset_join_needed:
        query = query.join(Catalog.asset)
        query = filter_service.apply_asset_filters(query, filters)

    query = query.options(
        selectinload(Catalog.images),
        selectinload(Catalog.files),
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

    if filters.user_id:
        query = query.where(Catalog.user_id == filters.user_id)

    if filters.role_id:
        users_with_role = (
            select(UserRole.user_id)
            .join(Role, Role.id == UserRole.role_id)
            .where(
                Role.id == filters.role_id,
                UserRole.deleted_at.is_(None),
                Role.deleted_at.is_(None),
            )
            .distinct()
            .cte('users_with_role')
        )
        query = query.join(
            users_with_role, Catalog.user_id == users_with_role.c.user_id
        )

    query = query.offset(filters.offset).limit(filters.limit)

    result = await session.scalars(query)
    entries = result.all()
    total_items = len(entries)
    print(f'Existe um total de: {total_items} resultados')

    BATCH_SIZE = 10
    ASSETS_DIR = (
        Path(__file__).resolve().parent.parent.parent / 'assets'
    ).resolve()
    lexend_regular = (ASSETS_DIR / 'Lexend-Variable.ttf').resolve().as_uri()

    main_document = None

    try:
        for offset in range(0, total_items, BATCH_SIZE):
            # --- Query do Lote (Igual) ---
            query = select(Catalog).where(Catalog.deleted_at.is_(None))
            query = filter_service.apply_catalog_filters(query, filters)

            if asset_join_needed:
                query = query.join(Catalog.asset)
                query = filter_service.apply_asset_filters(query, filters)

            query = query.options(
                selectinload(Catalog.images),
                selectinload(Catalog.files),
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

            if filters.user_id:
                query = query.where(Catalog.user_id == filters.user_id)

            if filters.role_id:
                users_with_role = (
                    select(UserRole.user_id)
                    .join(Role, Role.id == UserRole.role_id)
                    .where(
                        Role.id == filters.role_id,
                        UserRole.deleted_at.is_(None),
                        Role.deleted_at.is_(None),
                    )
                    .distinct()
                    .cte('users_with_role')
                )
                query = query.join(
                    users_with_role,
                    Catalog.user_id == users_with_role.c.user_id,
                )

            query = query.offset(offset).limit(BATCH_SIZE)

            result = await session.scalars(query)
            entries = result.unique().all()

            if not entries:
                break

            # Renderiza HTML
            items_html = ''.join(
                render_item_html(entry, offset + idx, total_items)
                for idx, entry in enumerate(entries)
            )

            full_html = f"""
                <!DOCTYPE html>
                <html lang="pt-br">
                <head>
                    <meta charset="utf-8" />
                    <style>
                        @page {{
                            size: A4;
                            margin: 0;
                            counter-increment: page;
                        }}
                        body {{
                            counter-reset: page {offset};
                        }}
                        html, body {{
                            margin: 0; padding: 0; background-color: #ffffff;
                            font-family: "Lexend", sans-serif; font-size: 10px;
                        }}
                        @font-face {{ font-family: Lexend; src: url({lexend_regular}); }}
                        img {{ max-width: 100%; }}
                    </style>
                </head>
                <body>
                    {items_html}
                </body>
                </html>
            """

            # GERA O DOCUMENTO NA MEMÓRIA (Rodando em thread separada para não travar)
            batch_document = await run_in_threadpool(
                render_document_batch, full_html
            )

            # --- MÁGICA DO WEASYPRINT AQUI ---
            if main_document is None:
                # O primeiro lote vira o documento mestre
                main_document = batch_document
            else:
                # Os lotes seguintes apenas injetam suas páginas no mestre
                main_document.pages.extend(batch_document.pages)

            # Limpeza agressiva
            del entries
            del items_html
            del full_html
            del (
                batch_document
            )  # Já copiamos as páginas, podemos deletar o objeto
            gc.collect()

        # 3. Escrita final
        final_buffer = BytesIO()

        # O método write_pdf agora grava o mestre (com todas as páginas acumuladas)
        if main_document:
            # write_pdf também pode ser pesado, rodamos em threadpool
            await run_in_threadpool(main_document.write_pdf, final_buffer)
        else:
            raise HTTPException(status_code=404, detail='Erro na geração')

        final_buffer.seek(0)

        return StreamingResponse(
            final_buffer,
            media_type='application/pdf',
            headers={
                'Content-Disposition': 'inline; filename="catalogo_completo.pdf"',
            },
        )

    except Exception as e:
        print(f'Erro PDF: {e}')
        raise HTTPException(
            status_code=500, detail='Erro interno ao gerar PDF'
        )

@router.get('/{catalog_id}', response_model=CatalogPublic)
async def read_catalog_entry(catalog_id: UUID, session: Session):
    options = [
        selectinload(Catalog.images),
        selectinload(Catalog.files),
        selectinload(Catalog.workflow_history).options(
            selectinload(CatalogWorkFlow.user).options(
                selectinload(User.system_identity).options(
                    selectinload(SystemIdentity.legal_guardian)
                ),
                selectinload(User.user_role_associations).selectinload(
                    UserRole.role
                ),
            ),
            selectinload(CatalogWorkFlow.transfer_requests).options(
                selectinload(WorkflowTransfer.location)
            ),
        ),
        selectinload(Catalog.location)
        .selectinload(Location.location_inventories)
        .selectinload(LocationInventory.inventory),
    ]
    db_catalog = await session.get(Catalog, catalog_id, options=options)

    if not db_catalog or db_catalog.deleted_at:
        raise HTTPException(
            status_code=HTTPStatus.NOT_FOUND, detail='Catalog entry not found'
        )
    return db_catalog

@router.put('/{catalog_id}', response_model=CatalogPublic)
async def update_catalog_entry(
    catalog_id: UUID,
    catalog_data: CatalogSchema,
    session: Session,
    current_user: CurrentUser,
):
    options = [
        selectinload(Catalog.images),
        selectinload(Catalog.files),
        selectinload(Catalog.user).options(
            selectinload(User.system_identity).options(
                selectinload(SystemIdentity.legal_guardian)
            )
        ),
        selectinload(Catalog.workflow_history).options(
            selectinload(CatalogWorkFlow.user).options(
                selectinload(User.system_identity).options(
                    selectinload(SystemIdentity.legal_guardian)
                )
            ),
            selectinload(CatalogWorkFlow.transfer_requests).options(
                selectinload(WorkflowTransfer.user).options(
                    selectinload(User.system_identity).options(
                        selectinload(SystemIdentity.legal_guardian)
                    )
                ),
                selectinload(WorkflowTransfer.location),
            ),
        ),
        selectinload(Catalog.location)
        .selectinload(Location.location_inventories)
        .selectinload(LocationInventory.inventory),
    ]

    db_catalog = await session.get(Catalog, catalog_id, options=options)

    if not db_catalog:
        raise HTTPException(
            status_code=HTTPStatus.NOT_FOUND, detail='Catalog entry not found'
        )

    db_catalog.asset_id = catalog_data.asset_id
    db_catalog.situation = catalog_data.situation
    db_catalog.conservation_status = catalog_data.conservation_status
    db_catalog.description = catalog_data.description

    session.expire_on_commit = False
    await session.commit()

    return db_catalog

@router.delete('/{catalog_id}', response_model=Message)
async def delete_catalog_entry(
    catalog_id: UUID,
    session: Session,
    current_user: CurrentUser,
):
    db_catalog = await session.get(Catalog, catalog_id)
    if not db_catalog:
        raise HTTPException(
            status_code=HTTPStatus.NOT_FOUND, detail='Catalog entry not found'
        )

    db_catalog.deleted_at = datetime.now()
    await session.commit()

    return {'message': 'Catalog entry deactivated'}

@router.get('/pdf/{catalog_id}')
async def export_catalog_pdf(
    session: Session,
    catalog_id: UUID,
    filters: Annotated[FilterCatalog, Depends()],
):
    options = [
        selectinload(Catalog.images),
        selectinload(Catalog.files),
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
    ]

    db_catalog = await session.get(Catalog, catalog_id, options=options)

    if not db_catalog or db_catalog.deleted_at:
        raise HTTPException(
            status_code=HTTPStatus.NOT_FOUND, detail='Catalog entry not found'
        )

    print('Existe 1 resultado')
    print(filters)

    items_html = ''.join(render_item_html(db_catalog, 0, 1))

    ASSETS_DIR = (
        Path(__file__).resolve().parent.parent.parent / 'assets'
    ).resolve()
    lexend_regular = (ASSETS_DIR / 'Lexend-Variable.ttf').resolve().as_uri()

    full_html = f"""
              <!DOCTYPE html>
              <html lang="pt-br">
              <head>
                  <meta charset="utf-8" />
                  <title>Relatório</title>
                  <style>
                      @page {{
                        size: A4;
                        margin: 0;
                      }}
                      
                      html, body {{
                          margin: 0;
                          padding: 0;
                          height: 100%;
                          background-color: #ffffff;
                          font-family: "Lexend", sans-serif;
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

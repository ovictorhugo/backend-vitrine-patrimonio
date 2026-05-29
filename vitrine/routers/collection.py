from http import HTTPStatus
from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Body, UploadFile, File
from fastapi.responses import FileResponse
from pathlib import Path
from sqlalchemy import func, select, desc
from sqlalchemy.orm import noload, selectinload


from vitrine.core.dependencies import CurrentUser, Session
from vitrine.models import (
    Collection, 
    CollectionItem, 
    Catalog, 
    CatalogWorkFlow, 
    User, 
    SystemIdentity, 
    TemporaryFileReference
)
import math
import uuid
import tempfile
import os
from email.message import EmailMessage
from fastapi.concurrency import run_in_threadpool
from playwright.async_api import async_playwright
from fastapi import BackgroundTasks
from vitrine.core.database import async_session
from vitrine.core.mail import get_smtp
from vitrine.core.settings import Settings
from vitrine.routers.utils import render_multiple_items
from vitrine.routers.catalog.catalog import merge_pdfs_sync, limpar_arquivos_antigos
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
        user_id=current_user.id,
        parecer_pdf=""
    )

    session.add(db_collection)
    await session.commit()
    await session.refresh(db_collection)

    db_collection.items = []
    return db_collection


@router.get('/', response_model=CollectionList)
async def read_collections(
    session: Session,
    current_user: CurrentUser,
    filters: Annotated[FilterCollection, Depends()],
    admin: bool = False,
):
    from vitrine.models import UserRole
    from uuid import UUID

    query = select(Collection).where(Collection.deleted_at.is_(None))

    if admin:
        role_id_uuid = UUID('5e07c6df-cb65-46fc-bdb4-1e55483e4848')
        role_query = select(UserRole).where(
            UserRole.user_id == current_user.id,
            UserRole.role_id == role_id_uuid
        )
        is_admin = await session.scalar(role_query)
        if not is_admin:
            query = query.where(Collection.user_id == current_user.id)
    else:
        query = query.where(Collection.user_id == current_user.id)

    query = query.options(noload('*'))

    if filters.type:
        query = query.where(Collection.type == filters.type)

    query = query.order_by(desc(Collection.created_at))
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

@router.post('/enviar-parecer/{collection_id}', response_model=Message)
async def enviar_parecer_pdf(
    collection_id: UUID,
    session: Session,
    current_user: CurrentUser,
    file: UploadFile = File(...)
):
    db_collection = await session.get(Collection, collection_id)
    if not db_collection or db_collection.deleted_at:
        raise HTTPException(
            status_code=HTTPStatus.NOT_FOUND, detail='Collection not found.'
        )

    if file.content_type != "application/pdf":
        raise HTTPException(status_code=400, detail="O arquivo deve ser um PDF.")

    STORAGE_ROOT = Path("vitrine/storage/pdfs_remocao/parecer")
    STORAGE_ROOT.mkdir(parents=True, exist_ok=True)

    filename = f"{collection_id}_{file.filename}"
    full_path = STORAGE_ROOT / filename

    content = await file.read()
    with open(full_path, "wb") as f:
        f.write(content)

    db_collection.parecer_pdf = filename

    if db_collection.type == "REMOCAO_DISPONIVEIS":
        from vitrine.models import Catalog, CatalogWorkFlow
        
        query = select(Catalog).join(CollectionItem, CollectionItem.catalog_id == Catalog.id).where(CollectionItem.collection_id == collection_id)
        catalogs = (await session.scalars(query)).all()
        for catalog in catalogs:
            catalog.current_workflow_status = "REMOVIDO_DESFAZIMENTO"
            new_workflow_entry = CatalogWorkFlow(
                catalog_id=catalog.id,
                user_id=current_user.id,
                workflow_status="REMOVIDO_DESFAZIMENTO",
                detail={"justificativa": "O item chegou ao fim do fluxo de desfazimento.", "observation": {"text": "O item chegou ao fim do fluxo de desfazimento."}},
            )
            session.add(new_workflow_entry)

    await session.commit()

    return {"message": "Parecer enviado com sucesso."}


@router.get('/baixar-parecer/{collection_id}')
async def baixar_parecer_pdf(
    collection_id: UUID,
    session: Session,
    current_user: CurrentUser,
):
    db_collection = await session.get(Collection, collection_id)
    if not db_collection or db_collection.deleted_at:
        raise HTTPException(
            status_code=HTTPStatus.NOT_FOUND, detail='Collection not found.'
        )

    if not db_collection.parecer_pdf:
        raise HTTPException(
            status_code=HTTPStatus.NOT_FOUND, detail='Parecer PDF not found.'
        )

    STORAGE_ROOT = Path("vitrine/storage/pdfs_remocao/parecer")
    file_path = STORAGE_ROOT / db_collection.parecer_pdf

    if not file_path.exists():
        raise HTTPException(
            status_code=HTTPStatus.NOT_FOUND, detail='File not found on server.'
        )

    return FileResponse(
        path=file_path,
        media_type='application/pdf',
        headers={'Content-Disposition': f'attachment; filename="{db_collection.parecer_pdf}"'}
    )

@router.post('/admin/action/{collection_id}', response_model=Message)
async def admin_collection_action(
    collection_id: UUID,
    session: Session,
    current_user: CurrentUser,
    action: int = Body(embed=True)
):
    is_admin = any(role.name == 'Administrador' for role in current_user.roles)
    if not is_admin:
        raise HTTPException(
            status_code=HTTPStatus.FORBIDDEN, detail='Ação restrita a administradores.'
        )

    db_collection = await session.get(Collection, collection_id)
    if not db_collection or db_collection.deleted_at:
        raise HTTPException(
            status_code=HTTPStatus.NOT_FOUND, detail='Collection not found.'
        )

    if action == 1:
        query = select(CollectionItem).where(CollectionItem.collection_id == collection_id)
        items = (await session.scalars(query)).all()
        count = 0
        for item in items:
            if getattr(item, "is_approved", False) is not True:
                await session.delete(item)
                count += 1
        await session.commit()
        return {"message": f"{count} itens não aprovados foram removidos."}

    elif action == 2:
        query = (
            select(CollectionItem)
            .options(selectinload(CollectionItem.catalog))
            .where(
                CollectionItem.collection_id == collection_id,
                CollectionItem.is_approved == True
            )
        )
        items = (await session.scalars(query)).all()
        count = 0
        for item in items:
            item.is_approved = None
            if item.catalog:
                item.catalog.current_workflow_status = "DESFAZIMENTO"
            count += 1
        await session.commit()
        return {"message": f"{count} itens aprovados foram alterados e catálogo atualizado."}
        
    elif action == 4:
        if db_collection.parecer_pdf:
            STORAGE_ROOT = Path("vitrine/storage/pdfs_remocao/parecer")
            file_path = STORAGE_ROOT / db_collection.parecer_pdf
            if file_path.exists():
                file_path.unlink()
            
            db_collection.parecer_pdf = None
            await session.commit()
            return {"message": "Arquivo de parecer removido com sucesso."}
        else:
            return {"message": "Nenhum arquivo de parecer encontrado."}
    
    else:
        raise HTTPException(status_code=400, detail="Ação inválida.")


@router.get('/remocao_pdf/{collection_id}', response_model=Message)
async def export_collection_pdf_play(
    collection_id: UUID, 
    session: Session, 
    background_tasks: BackgroundTasks,
    current_user: CurrentUser
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

    count_query = (
        select(func.count())
        .select_from(CollectionItem)
        .where(CollectionItem.collection_id == collection_id)
    )
    total_items = await session.scalar(count_query) or 0

    if total_items == 0:
        raise HTTPException(status_code=404, detail='Nenhum item na coleção')

    background_tasks.add_task(generate_and_send_collection_pdf_play, current_user, collection_id, total_items)
    background_tasks.add_task(limpar_arquivos_antigos, session)

    return {'message': 'Processamento do PDF iniciado. Você receberá um e-mail em breve com o arquivo.'}

async def generate_and_send_collection_pdf_play(current_user_obj: User, coll_id: UUID, tot_items: int):
    async with async_session() as bg_session:
            base_query = (
                select(Catalog)
                .join(CollectionItem, CollectionItem.catalog_id == Catalog.id)
                .where(CollectionItem.collection_id == coll_id)
            )

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

            TOTAL_PAGES = math.ceil(tot_items / 2)
            BATCH_SIZE = 60

            ASSETS_DIR = (Path(__file__).resolve().parent.parent / 'assets').resolve()
            lexend_regular = (ASSETS_DIR / 'Lexend-Variable.ttf').resolve().as_uri()
            EE_LOGO_URI = (ASSETS_DIR / "ee_logo.png").resolve().as_uri()
            SP_LOGO_URI = (ASSETS_DIR / "sp_logo.png").resolve().as_uri()

            try:
                temp_files_to_cleanup = []
                temp_pdf_paths = []

                async with async_playwright() as p:
                    browser = await p.chromium.launch(
                        headless=True,
                        args=[
                            '--no-sandbox', 
                            '--disable-setuid-sandbox', 
                            '--disable-dev-shm-usage',
                            '--disable-gpu',
                            '--single-process',
                            '--no-zygote',
                            '--disable-software-rasterizer'
                        ]
                    )
                    context = await browser.new_context()
                    page = await context.new_page()

                    for offset in range(0, tot_items, BATCH_SIZE):
                        batch_query = base_query.offset(offset).limit(BATCH_SIZE)
                        result = await bg_session.scalars(batch_query)
                        entries = result.unique().all()

                        if not entries:
                            break

                        batch_pages_html = []
                        for i in range(0, len(entries), 2):
                            item1 = entries[i]
                            item2 = entries[i+1] if i+1 < len(entries) else None
                            
                            absolute_idx = offset + i
                            current_page_number = (absolute_idx // 2) + 1

                            item1_html = render_multiple_items(item1)                    
                            item2_html = render_multiple_items(item2) if item2 else ""
                            
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

                        final_batch_html_content = "".join(batch_pages_html)

                        batch_full_html = f"""
                            <!DOCTYPE html>
                            <html lang="pt-br">
                            <head>
                                <meta charset="utf-8" />
                                <style>
                                    @page {{
                                        size: A4;
                                        margin: 0;
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
                                        page-break-after: always;
                                        overflow: hidden;
                                        display: flex;
                                        flex-direction: column;
                                    }}

                                    .conteudo-itens {{
                                        flex: 1;
                                        display: flex;
                                        flex-direction: column;
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

                        temp_pdf_path = f"/tmp/pdf_batch_{uuid.uuid4().hex}.pdf"
                        
                        with tempfile.NamedTemporaryFile(delete=False, suffix='.html', mode='w', encoding='utf-8') as f:
                            f.write(batch_full_html)
                            temp_html_path = f.name
                            
                        temp_files_to_cleanup.extend([temp_html_path, temp_pdf_path])
                        
                        await page.goto(f"file://{temp_html_path}", wait_until="load")
                        
                        await page.pdf(
                            path=temp_pdf_path,
                            format="A4",
                            print_background=True,
                            margin={"top": "0", "right": "0", "bottom": "0", "left": "0"}
                        )
                        
                        temp_pdf_paths.append(temp_pdf_path)

                    await page.close()
                    await context.close()
                    await browser.close()

                pdf_bytes_io = await run_in_threadpool(merge_pdfs_sync, temp_pdf_paths)
                
                pdf_uuid = uuid.uuid4()
                pdf_filename = f"remocao_{str(pdf_uuid)[-6:]}.pdf"
                pdf_filepath = os.path.join('vitrine/storage/temp', pdf_filename)
                
                os.makedirs(os.path.dirname(pdf_filepath), exist_ok=True)
                with open(pdf_filepath, "wb") as f:
                    f.write(pdf_bytes_io.getvalue())

                new_file_record = TemporaryFileReference(
                    folder_type="catalog_pdf",
                    file_name=pdf_filename
                )
                bg_session.add(new_file_record)
                await bg_session.commit()
                await bg_session.refresh(new_file_record)
                file_token = new_file_record.token

                try:
                    smtp_gen = get_smtp()
                    mail_conn = next(smtp_gen)
                    try:
                        msg = EmailMessage()
                        msg['Subject'] = 'Coleção de Remoção - Vitrine Patrimônio'
                        msg['From'] = Settings().SMTP_USER
                        msg['To'] = current_user_obj.email
                        msg.set_content(
                            f"Prezado(a),\n\n"
                            f"Seu relatório da coleção de remoção do Sistema patrimônio foi gerado com sucesso! Ele estará disponível por 7 dias.\n\n"
                            f"Para acessá-lo, basta acessar o seguinte link: http://sistemapatrimonio.eng.ufmg.br/download-pdf-by?token={file_token}\n\n"
                            f"Atenciosamente,\n"
                            f"Equipe Vitrine Patrimônio"
                        )
                        mail_conn.send_message(msg)
                    finally:
                        try:
                            next(smtp_gen)
                        except StopIteration:
                            pass
                except Exception as e:
                    print(f'Erro PDF Playwright Email: {e}')
                
            finally:
                for filepath in temp_files_to_cleanup:
                    if os.path.exists(filepath):
                        try:
                            os.remove(filepath)
                        except Exception as e:
                            pass


@router.get('/removiveis_pdf/{collection_id}', response_model=Message)
async def export_collection_removiveis_pdf_play(
    collection_id: UUID, 
    session: Session, 
    background_tasks: BackgroundTasks,
    current_user: CurrentUser
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

    count_query = (
        select(func.count())
        .select_from(CollectionItem)
        .where(CollectionItem.collection_id == collection_id)
    )
    total_items = await session.scalar(count_query) or 0

    if total_items == 0:
        raise HTTPException(status_code=404, detail='Nenhum item na coleção')

    background_tasks.add_task(generate_and_send_collection_removiveis_pdf_play, current_user, collection_id, total_items)
    background_tasks.add_task(limpar_arquivos_antigos, session)

    return {'message': 'Processamento do PDF iniciado. Você receberá um e-mail em breve com o arquivo.'}


async def generate_and_send_collection_removiveis_pdf_play(current_user_obj: User, coll_id: UUID, tot_items: int):
    async with async_session() as bg_session:
        base_query = (
            select(Catalog)
            .join(CollectionItem, CollectionItem.catalog_id == Catalog.id)
            .where(CollectionItem.collection_id == coll_id)
        )

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

        TOTAL_PAGES = math.ceil(tot_items / 2)
        BATCH_SIZE = 60

        ASSETS_DIR = (Path(__file__).resolve().parent.parent / 'assets').resolve()
        lexend_regular = (ASSETS_DIR / 'Lexend-Variable.ttf').resolve().as_uri()
        EE_LOGO_URI = (ASSETS_DIR / "ee_logo.png").resolve().as_uri()
        SP_LOGO_URI = (ASSETS_DIR / "sp_logo.png").resolve().as_uri()

        try:
            temp_files_to_cleanup = []
            temp_pdf_paths = []

            async with async_playwright() as p:
                browser = await p.chromium.launch(
                    headless=True,
                    args=[
                        '--no-sandbox', 
                        '--disable-setuid-sandbox', 
                        '--disable-dev-shm-usage',
                        '--disable-gpu',
                        '--single-process',
                        '--no-zygote',
                        '--disable-software-rasterizer'
                    ]
                )
                context = await browser.new_context()
                page = await context.new_page()

                # --------- CAPA ---------
                html_capa = f"""
                <!DOCTYPE html>
                <html lang="pt-br">
                <head>
                    <meta charset="utf-8" />
                    <style>
                        @page {{ size: A4; margin: 0; }}
                        html, body {{ margin: 0; padding: 0; }}
                        @font-face {{ font-family: Lexend; src: url({lexend_regular}); }}
                    </style>
                </head>
                <body>
                    <div
                        style="
                            position: relative;
                            width: 210mm;
                            height: 296.5mm;
                            box-sizing: border-box;
                            background-color: #f9fafb;
                            overflow: hidden;
                            font-family: 'Lexend', sans-serif;
                        "
                    >
                        <table
                            style="
                                width: 100%;
                                border-collapse: collapse;
                                margin: 0;
                                padding: 40px;
                            "
                        >
                            <tr>
                                <td style="width: 150px; padding: 32px 48px 12px 48px; vertical-align: middle;">
                                    <img src="{SP_LOGO_URI}" style="height: 48px; max-width: 120px; object-fit: contain;" />
                                </td>
                                <td style="width: 150px; padding: 32px 48px 12px 48px; text-align: right; vertical-align: middle;">
                                    <img src="{EE_LOGO_URI}" style="height: 48px; max-width: 120px; object-fit: contain;" />
                                </td>
                            </tr>
                        </table>

                        <div style="padding: 0 52px 10px 52px;">
                            <section style="display: block; width: 100%; margin-bottom: 20px;">
                                <h2
                                  style="
                                    font-size: 20px;
                                    font-weight: 700;
                                    margin: 0 0 15px 0;
                                    text-transform: uppercase;
                                    color: #111827;
                                    text-align: center;
                                  "
                                >
                                  Remoção Permanente de itens
                                </h2>                
                            </section>
                        </div>

                        <div style="padding: 0 52px; color: #374151; font-size: 14px; line-height: 1.6;">
                            
                            <h3 style="font-size: 15px; font-weight: 700; color: #111827; margin: 0 0 12px 0;">
                                1. INSTITUIÇÃO DE ORIGEM (CEDENTE / ALIENANTE)
                            </h3>
                            <p style="margin: 0 0 4px 0;"><strong>Nome da Instituição:</strong> ___________________________________</p>
                            <p style="margin: 0 0 4px 0;"><strong>CNPJ:</strong> ___________________________________</p>
                            <p style="margin: 0 0 4px 0;"><strong>Endereço:</strong> ___________________________________</p>
                            <p style="margin: 0 0 24px 0;"><strong>Responsável Autorizador:</strong>___________________________________</p>

                            <h3 style="font-size: 15px; font-weight: 700; color: #111827; margin: 0 0 12px 0;">
                                2. INSTITUIÇÃO RECEPTORA (CESSIONÁRIA / RECEBEDORA)
                            </h3>
                            <p style="margin: 0 0 4px 0;"><strong>Nome da Instituição:</strong> ___________________________________</p>
                            <p style="margin: 0 0 4px 0;"><strong>CNPJ:</strong>___________________________________</p>
                            <p style="margin: 0 0 4px 0;"><strong>Endereço:</strong>___________________________________</p>
                            <p style="margin: 0 0 24px 0;"><strong>Representante Legal:</strong>___________________________________</p>

                            <h3 style="font-size: 15px; font-weight: 700; color: #111827; margin: 0 0 12px 0;">
                                3. OBJETO
                            </h3>
                            <p style="margin: 0 0 24px 0; text-align: justify;">
                                O presente termo tem por objeto a transferência de posse, domínio e responsabilidade do(s) bem(ns) público(s) listado(s) no Anexo I (ou tabela abaixo) deste documento, outrora pertencentes ao patrimônio da _______________________________ caracterizando o seu desfazimento oficial.
                            </p>

                            <h3 style="font-size: 15px; font-weight: 700; color: #111827; margin: 0 0 12px 0;">
                                4. DA TRANSFERÊNCIA DE RESPONSABILIDADE
                            </h3>
                            <p style="margin: 0 0 12px 0; text-align: justify;">
                                A ___________________________________ declara, para os devidos fins legais, que recebe o(s) bem(ns) listado(s) no estado de conservação em que se encontra(m), não cabendo qualquer tipo de reclamação posterior ou exigência de garantia ou manutenção.
                            </p>
                            <p style="margin: 0 0 12px 0; text-align: justify;">
                                Fica expressamente acordado que, a partir da data de assinatura deste Termo, a ___________________________________ exime-se de toda e qualquer responsabilidade administrativa, civil, penal, ambiental ou patrimonial sobre o(s) referido(s) bem(ns).
                            </p>
                            <p style="margin: 0 0 24px 0; text-align: justify;">
                                Caberá exclusivamente à ___________________________________ a responsabilidade pelo transporte, guarda, utilização adequada e, se for o caso, pelo descarte final ecologicamente correto dos itens, assumindo todos os ônus e riscos decorrentes destas operações, em estrita observância à legislação ambiental e patrimonial vigente.
                            </p>

                            <table style="width: 100%; margin-top: 60px; text-align: center;">
                                <tr>
                                    <td style="width: 50%; padding: 0 20px;">
                                        __________________________<br/>
                                        <strong>Assinatura da Instituição Cedente</strong>
                                    </td>
                                    <td style="width: 50%; padding: 0 20px;">
                                        __________________________<br/>
                                        <strong>Assinatura da Instituição Recebedora</strong>
                                    </td>
                                </tr>
                            </table>
                        </div>
                        <div 
                            style="
                                position: absolute;
                                bottom: 0;
                                left: 0;
                                right: 0;
                                height: 50px;
                                padding: 0 24px 20px 24px;
                            "
                        >
                             <div style="border-top: 1px solid #e5e7eb; padding-top: 10px;">
                              <table style="width: 100%; border-collapse: collapse;">
                                <tr>
                                    <td style="text-align: center; padding-bottom: 6px;">
                                         <p
                                            style="
                                              margin: 0;
                                              color: #6b7280;
                                              font-size: 11px;
                                              font-weight: 500;
                                            "
                                          >
                                            Av. Presidente Antônio Carlos, nº 6.627, Belo Horizonte/MG - CEP: 31.270-901
                                          </p>
                                    </td>
                                </tr>
                                <tr>
                                    <td style="text-align: right; color: #6b7280; font-size: 10px;">
                                        Página 1 de {TOTAL_PAGES + 1}
                                    </td>
                                </tr>
                              </table>
                          </div>
                        </div>
                    </div>
                </body>
                </html>
                """
                capa_pdf_path = f"/tmp/pdf_batch_{uuid.uuid4().hex}.pdf"
                with tempfile.NamedTemporaryFile(delete=False, suffix='.html', mode='w', encoding='utf-8') as f:
                    f.write(html_capa)
                    capa_html_path = f.name
                temp_files_to_cleanup.extend([capa_html_path, capa_pdf_path])
                
                await page.goto(f"file://{capa_html_path}", wait_until="load")
                await page.pdf(
                    path=capa_pdf_path,
                    format="A4",
                    print_background=True,
                    margin={"top": "0", "right": "0", "bottom": "0", "left": "0"}
                )
                temp_pdf_paths.append(capa_pdf_path)
                # --------- FIM DA CAPA ---------

                for offset in range(0, tot_items, BATCH_SIZE):
                    batch_query = base_query.offset(offset).limit(BATCH_SIZE)
                    result = await bg_session.scalars(batch_query)
                    entries = result.unique().all()

                    if not entries:
                        break

                    batch_pages_html = []
                    for i in range(0, len(entries), 2):
                        item1 = entries[i]
                        item2 = entries[i+1] if i+1 < len(entries) else None
                        
                        absolute_idx = offset + i
                        current_page_number = (absolute_idx // 2) + 2

                        item1_html = render_multiple_items(item1)                    
                        item2_html = render_multiple_items(item2) if item2 else ""
                        
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
                                            Página {current_page_number} de {TOTAL_PAGES + 1}
                                        </td>
                                    </tr>
                                </table>
                            </div>
                        </div>
                        """
                        batch_pages_html.append(page_html)

                    final_batch_html_content = "".join(batch_pages_html)

                    batch_full_html = f"""
                        <!DOCTYPE html>
                        <html lang="pt-br">
                        <head>
                            <meta charset="utf-8" />
                            <style>
                                @page {{
                                    size: A4;
                                    margin: 0;
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
                                    page-break-after: always;
                                    overflow: hidden;
                                    display: flex;
                                    flex-direction: column;
                                }}

                                .conteudo-itens {{
                                    flex: 1;
                                    display: flex;
                                    flex-direction: column;
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

                    temp_pdf_path = f"/tmp/pdf_batch_{uuid.uuid4().hex}.pdf"
                    
                    with tempfile.NamedTemporaryFile(delete=False, suffix='.html', mode='w', encoding='utf-8') as f:
                        f.write(batch_full_html)
                        temp_html_path = f.name
                        
                    temp_files_to_cleanup.extend([temp_html_path, temp_pdf_path])
                    
                    await page.goto(f"file://{temp_html_path}", wait_until="load")
                    
                    await page.pdf(
                        path=temp_pdf_path,
                        format="A4",
                        print_background=True,
                        margin={"top": "0", "right": "0", "bottom": "0", "left": "0"}
                    )
                    
                    temp_pdf_paths.append(temp_pdf_path)

                await page.close()
                await context.close()
                await browser.close()

            pdf_bytes_io = await run_in_threadpool(merge_pdfs_sync, temp_pdf_paths)
            
            pdf_uuid = uuid.uuid4()
            pdf_filename = f"removiveis_{str(pdf_uuid)[-6:]}.pdf"
            pdf_filepath = os.path.join('vitrine/storage/temp', pdf_filename)
            
            os.makedirs(os.path.dirname(pdf_filepath), exist_ok=True)
            with open(pdf_filepath, "wb") as f:
                f.write(pdf_bytes_io.getvalue())

            new_file_record = TemporaryFileReference(
                folder_type="catalog_pdf",
                file_name=pdf_filename
            )
            bg_session.add(new_file_record)
            await bg_session.commit()
            await bg_session.refresh(new_file_record)
            file_token = new_file_record.token

            try:
                smtp_gen = get_smtp()
                mail_conn = next(smtp_gen)
                try:
                    msg = EmailMessage()
                    msg['Subject'] = 'Coleção de Itens Removíveis - Vitrine Patrimônio'
                    msg['From'] = Settings().SMTP_USER
                    msg['To'] = current_user_obj.email
                    msg.set_content(
                        f"Prezado(a),\n\n"
                        f"Seu relatório da coleção de itens removíveis do Sistema Patrimônio foi gerado com sucesso! Ele estará disponível por 7 dias.\n\n"
                        f"Para acessá-lo, basta acessar o seguinte link: http://sistemapatrimonio.eng.ufmg.br/download-pdf-by?token={file_token}\n\n"
                        f"Atenciosamente,\n"
                        f"Equipe Vitrine Patrimônio"
                    )
                    mail_conn.send_message(msg)
                finally:
                    try:
                        next(smtp_gen)
                    except StopIteration:
                        pass
            except Exception as e:
                print(f'Erro PDF Playwright Email: {e}')
            
        finally:
            for filepath in temp_files_to_cleanup:
                if os.path.exists(filepath):
                    try:
                        os.remove(filepath)
                    except Exception as e:
                        pass

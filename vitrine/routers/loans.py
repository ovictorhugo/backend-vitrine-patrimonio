from http import HTTPStatus
from typing import Optional, List
from uuid import UUID

from fastapi import APIRouter, HTTPException, Query, Depends
from sqlalchemy import select, or_, exists, desc
from sqlalchemy.orm import selectinload, joinedload, noload, contains_eager
from datetime import datetime
from typing import Annotated
from pathlib import Path
from weasyprint import HTML
from io import BytesIO
from fastapi.responses import StreamingResponse

from .utils import render_loanable_item, render_all_loanable_items, render_loan_terms
from ..services import mail_service
from vitrine.services import filter_service

from vitrine.core.dependencies import CurrentUser, Session, Mail
from vitrine.models import (Catalog, 
    Loan, 
    LoanableItem,
    CatalogWorkFlow,
    Location,
    LocationInventory,
    SystemIdentity,
    User,
    Role,
    UserRole,
    Asset,
    Sector,
    Agency, )
from vitrine.schemas import (
    LoanableItemPublic,
    LoanableItemList,
    SimpleLoanableItemList,
    LoanSchema,
    CatalogSchema,
    FilterCatalog,
    FilterAsset,
)

router = APIRouter(prefix='/loans', tags=['empréstimos'])


_ASSET_FIELDS = set(FilterAsset.model_fields.keys())
_NON_JOIN_FIELDS = {'limit', 'offset'}
ASSET_JOIN_TRIGGER_FIELDS = _ASSET_FIELDS - _NON_JOIN_FIELDS


@router.post(
    '/items', 
    status_code=HTTPStatus.CREATED
)
async def create_loanable_item(
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

    workflow = CatalogWorkFlow(
        catalog_id=db_catalog.id,
        user_id=current_user.id,
        workflow_status="AUDIOVISUAL_ANUNCIADO",
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

    # 2. Verifica se já está cadastrado para empréstimo (unique constraint)
    query = select(LoanableItem).where(
        LoanableItem.catalog_id == db_catalog.id,
        LoanableItem.deleted_at.is_(None)
    )
    if await session.scalar(query):
        raise HTTPException(
            status_code=HTTPStatus.CONFLICT,
            detail='This catalog item is already registered for loans.'
        )

    # 3. Cria o item vinculando ao legal_guardian (dono/cadastrador)
    db_item = LoanableItem(
        catalog_id=db_catalog.id,
        last_check= datetime.now(),
        legal_guardian_id=current_user.id, # Definido pelo usuário logado
        owner_notes=None
    )

    session.add(db_item)
    await session.commit()
    await session.refresh(db_item)
    return {"catalog_id" : db_catalog.id}


@router.post(
    '/request', 
    status_code=HTTPStatus.CREATED, 
    response_model=LoanSchema
)
async def request_loan(
    loan_data: LoanSchema,  
    session: Session,
    current_user: CurrentUser,
):
    # 1. Verifica se o item está disponível para empréstimo
    loanable_item = await session.get(LoanableItem, loan_data.loanable_item_id)
    if not loanable_item:
        raise HTTPException(
            status_code=HTTPStatus.NOT_FOUND, 
            detail='Loanable item not found.'
        )
    
    db_loan = Loan(
        loanable_item_id=loan_data.loanable_item_id,
        requester_id=current_user.id,
        temporary_guardian_id=loan_data.temporary_guardian_id,
        start_at=loan_data.start_at,
        end_at= None if loan_data.is_maintenance else loan_data.end_at,
        is_maintenance=loan_data.is_maintenance,
        lend_detail=loan_data.lend_detail,
        returned_detail=None,
        returned_at=None,
        rejection_reason=None,  
    )

    session.add(db_loan)
    await session.commit()
    await session.refresh(db_loan)
    return db_loan


@router.get('/', response_model=LoanableItemList)
async def list_loanable_items(
    session: Session,
    offset: int = Query(0, ge=0),
    limit: int = Query(24, gt=0, le=100),
):
    stmt = (
        select(LoanableItem)
        .where(LoanableItem.deleted_at.is_(None))
        .options(
            # 1. Responsável pelo Item
            selectinload(LoanableItem.legal_guardian),
            selectinload(LoanableItem.loans).options(
                selectinload(Loan.requester),
                selectinload(Loan.temporary_guardian),
                selectinload(Loan.confirmed_by),
                selectinload(Loan.executed_by),
                selectinload(Loan.returned_by)
            ),

            # 3. Catálogo e toda a sua árvore de dependências
            selectinload(LoanableItem.catalog).options(
                selectinload(Catalog.images),
                selectinload(Catalog.files),
                selectinload(Catalog.user), # Usuário que cadastrou
                
                # Asset -> Material e Responsável do Asset
                selectinload(Catalog.asset).options(
                    selectinload(Asset.material),
                    selectinload(Asset.legal_guardian)
                ),
                
                # Location -> Sector -> Agency -> Unit (Cascata completa baseada no seu DTO)
                selectinload(Catalog.location).options(
                    selectinload(Location.sector).options(
                        selectinload(Sector.agency).options(
                            selectinload(Agency.unit)
                        )
                    ),
                    selectinload(Location.location_inventories).selectinload(
                        LocationInventory.inventory
                    )
                ),
                
                # Workflow History e suas dependências internas
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
            )
        )
        .order_by(LoanableItem.created_at.desc())
        .offset(offset)
        .limit(limit)
    )

    result = await session.execute(stmt)
    items = result.unique().scalars().all()

    try:
        # Força a validação manualmente antes de retornar
        LoanableItemList.model_validate({'loanable_items': items})
    except Exception as e:
        print("====== ERRO DE VALIDAÇÃO DO SCHEMA ======")
        print(e)
        raise e

    return {'loanable_items': items}


@router.get('/item/{id}', response_model=LoanableItemList)
async def get_one_loanable_item(
    session: Session,
    id:UUID,
    offset: int = Query(0, ge=0),
    limit: int = Query(24, gt=0, le=100),
):
    stmt = (
        select(LoanableItem)
        .where(LoanableItem.id == id,
               LoanableItem.deleted_at.is_(None))
        .options(
            # 1. Responsável pelo Item
            selectinload(LoanableItem.legal_guardian),
            selectinload(LoanableItem.loans).options(
                selectinload(Loan.requester),
                selectinload(Loan.temporary_guardian),
                selectinload(Loan.confirmed_by),
                selectinload(Loan.executed_by),
                selectinload(Loan.returned_by)
            ),

            # 3. Catálogo e toda a sua árvore de dependências
            selectinload(LoanableItem.catalog).options(
                selectinload(Catalog.images),
                selectinload(Catalog.files),
                selectinload(Catalog.user), # Usuário que cadastrou
                
                # Asset -> Material e Responsável do Asset
                selectinload(Catalog.asset).options(
                    selectinload(Asset.material),
                    selectinload(Asset.legal_guardian)
                ),
                
                # Location -> Sector -> Agency -> Unit (Cascata completa baseada no seu DTO)
                selectinload(Catalog.location).options(
                    selectinload(Location.sector).options(
                        selectinload(Sector.agency).options(
                            selectinload(Agency.unit)
                        )
                    ),
                    selectinload(Location.location_inventories).selectinload(
                        LocationInventory.inventory
                    )
                ),
                
                # Workflow History e suas dependências internas
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
            )
        )
        .order_by(LoanableItem.created_at.desc())
        .offset(offset)
        .limit(limit)
    )

    result = await session.execute(stmt)
    items = result.unique().scalars().all()

    try:
        # Força a validação manualmente antes de retornar
        LoanableItemList.model_validate({'loanable_items': items})
    except Exception as e:
        print("====== ERRO DE VALIDAÇÃO DO SCHEMA ======")
        print(e)
        raise e

    return {'loanable_items': items}

@router.get('/cards', response_model=SimpleLoanableItemList)
async def list_loanable_items_cards(
    session: Session, 
    filters: Annotated[FilterCatalog, Depends()],
    is_visible: Optional[bool] = Query(None)
):
    query = select(LoanableItem).where(LoanableItem.deleted_at.is_(None))
    
    if is_visible is not None:
        query = query.where(LoanableItem.is_visible == is_visible)

    # Sempre fazemos o JOIN com Catalog, então a estratégia base para ele será sempre contains_eager
    query = query.join(LoanableItem.catalog).where(Catalog.deleted_at.is_(None))

    # 1. Aplica filtros diretos do Catálogo
    query = filter_service.apply_catalog_filters(query, filters)

    # Criamos a árvore de carregamento do Catalog a partir do contains_eager
    catalog_loader = contains_eager(LoanableItem.catalog).options(
        selectinload(Catalog.images),
        selectinload(Catalog.user), 
        noload(Catalog.location),
        noload(Catalog.files),
        noload(Catalog.workflow_history),
        noload(Catalog.favorited_by),
        noload(Catalog.collection_items),
    )

    # 2. Verifica se precisa fazer o JOIN com Asset
    asset_join_needed = any(
        getattr(filters, field_name) is not None
        for field_name in ASSET_JOIN_TRIGGER_FIELDS
    )

    if asset_join_needed:
        query = query.join(Catalog.asset)
        query = filter_service.apply_asset_filters(query, filters)
        
        # Como o JOIN de Asset foi feito, anexamos um contains_eager na árvore do catálogo
        catalog_loader = catalog_loader.contains_eager(Catalog.asset).options(
            selectinload(Asset.material)
        )
    else:
        # Se não houve JOIN, ordenamos o SQLAlchemy a buscar o Asset separadamente (selectinload)
        catalog_loader = catalog_loader.selectinload(Catalog.asset).options(
            selectinload(Asset.material)
        )

    # 3. Consolida todas as opções na query principal
    query = query.options(
        selectinload(LoanableItem.legal_guardian),
        selectinload(LoanableItem.loans),
        catalog_loader # <-- Injetamos a árvore inteira aqui, sem conflitos!
    )

    # 4. Ordenação e Paginação
    query = query.order_by(desc(LoanableItem.created_at))
    query = query.offset(filters.offset).limit(filters.limit)

    result = await session.execute(query)
    items = result.unique().scalars().all()

    try:
        SimpleLoanableItemList.model_validate({'loanable_items': items})
    except Exception as e:
        print("====== ERRO DE VALIDAÇÃO DO SCHEMA ======")
        print(e)
        raise e

    return {'loanable_items': items}

    
@router.get('/my', response_model=LoanableItemList)
async def list_my_loanable_items(
    session: Session, # ou AsyncSession
    current_user: CurrentUser,
    offset: int = Query(0, ge=0),
    limit: int = Query(24, gt=0, le=100),
):
    stmt = (
        select(LoanableItem)
        .join(Loan) # Faz a junção das tabelas
        .where(
            LoanableItem.deleted_at.is_(None),
            Loan.is_returned.is_(False), # Garante que o empréstimo não foi finalizado
            or_(
                Loan.requester_id == current_user.id,
                Loan.temporary_guardian_id == current_user.id
            ) # O usuário precisa ser o solicitante ou o guardião temporário
        )
        .options(
            # 1. Responsável pelo Item
            selectinload(LoanableItem.legal_guardian),
            
            # 2. Histórico de Empréstimos
            selectinload(LoanableItem.loans).options(
                selectinload(Loan.requester),
                selectinload(Loan.temporary_guardian),
                selectinload(Loan.confirmed_by),
                selectinload(Loan.executed_by),
                selectinload(Loan.returned_by)
            ),

            # 3. Catálogo e toda a sua árvore de dependências (Cópia exata do original)
            selectinload(LoanableItem.catalog).options(
                selectinload(Catalog.images),
                selectinload(Catalog.files),
                selectinload(Catalog.user),
                selectinload(Catalog.asset).options(
                    selectinload(Asset.material),
                    selectinload(Asset.legal_guardian)
                ),
                
                selectinload(Catalog.location).options(
                    selectinload(Location.sector).options(
                        selectinload(Sector.agency).options(
                            selectinload(Agency.unit)
                        )
                    ),
                    selectinload(Location.location_inventories).selectinload(
                        LocationInventory.inventory
                    )
                ),
                
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
            )
        )
        .order_by(LoanableItem.created_at.desc())
        .offset(offset)
        .limit(limit)
    )

    result = await session.execute(stmt)
    items = result.unique().scalars().all()

    try:
        LoanableItemList.model_validate({'loanable_items': items})
    except Exception as e:
        print("====== ERRO DE VALIDAÇÃO DO SCHEMA ======")
        print(e)
        raise e

    return {'loanable_items': items}

@router.get('/all_pdf')
async def export_all_catalog_pdf(
    session: Session,
):
    stmt = (
        select(LoanableItem)
        .where(
            LoanableItem.deleted_at.is_(None)
        )
        .options(
            selectinload(LoanableItem.legal_guardian),
            selectinload(LoanableItem.loans),
            selectinload(LoanableItem.catalog).options(
                selectinload(Catalog.images),
                selectinload(Catalog.user),
                selectinload(Catalog.asset).options(
                    selectinload(Asset.material),
                    selectinload(Asset.legal_guardian)
                ),
                selectinload(Catalog.location).options(
                    selectinload(Location.sector).options(
                        selectinload(Sector.agency).options(
                            selectinload(Agency.unit)
                        )
                    )
                ),
                selectinload(Catalog.workflow_history).options(
                    selectinload(CatalogWorkFlow.user),
                    selectinload(CatalogWorkFlow.transfer_requests)
                ),
            )
        )
    )

    result = await session.execute(stmt)
    # Extrai todos os itens em forma de lista
    items = result.unique().scalars().all()

    if not items:
        raise HTTPException(    
            status_code=404, 
            detail="Nenhum item de patrimônio disponível no momento."
        )

    # Passa a lista 'items' para a nova função de renderização
    items_html = ''.join(render_all_loanable_items(items))

    ASSETS_DIR = (
        Path(__file__).resolve().parent.parent.parent / 'assets'
    ).resolve()
    lexend_regular = (ASSETS_DIR / 'Lexend-Variable.ttf').resolve().as_uri()

    full_html = f"""
              <!DOCTYPE html>
              <html lang="pt-br">
              <head>
                  <meta charset="utf-8" />
                  <title>Relatório Geral</title>
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
            # Sugestão: Alterado o nome do arquivo para diferenciar do pdf individual
            'Content-Disposition': 'inline; filename="catalogo_geral.pdf"', 
        },
    )


@router.get('/{catalog_id}', response_model=LoanableItemPublic)
async def get_loanable_item_by_catalog(
    session: Session,
    catalog_id: UUID
):    
    stmt = (
        select(LoanableItem)
        .where(
            LoanableItem.catalog_id == catalog_id,
            LoanableItem.deleted_at.is_(None)
        )
        .options(
            selectinload(LoanableItem.legal_guardian),
            selectinload(LoanableItem.loans).options(
                selectinload(Loan.requester),
                selectinload(Loan.temporary_guardian),
                selectinload(Loan.confirmed_by),
                selectinload(Loan.executed_by),
                selectinload(Loan.returned_by)
            ),
            selectinload(LoanableItem.catalog).options(
                selectinload(Catalog.images),
                selectinload(Catalog.user),
                selectinload(Catalog.asset).options(
                    selectinload(Asset.material),
                    selectinload(Asset.legal_guardian)
                ),
                selectinload(Catalog.location).options(
                    selectinload(Location.sector).options(
                        selectinload(Sector.agency).options(
                            selectinload(Agency.unit)
                        )
                    )
                ),
                selectinload(Catalog.workflow_history).options(
                    selectinload(CatalogWorkFlow.user),
                    selectinload(CatalogWorkFlow.transfer_requests)
                ),
            )
        )
    )

    result = await session.execute(stmt)
    item = result.unique().scalar_one_or_none()

    if not item:
        raise HTTPException(    
            status_code=404, 
            detail="Este item de patrimônio não possui um registro de empréstimo (LoanableItem)."
        )
    try:
        return LoanableItemPublic.model_validate(item)
    except Exception as e:
        print(f"Erro de validação no catalog_id {catalog_id}: {e}")
        raise HTTPException(
            status_code=500, 
            detail="Erro na estrutura de dados do item de empréstimo."
        )

@router.patch('/confirm/{loan_id}')
async def confirm_loan(
    loan_id: UUID,
    confirm: bool,
    session: Session,
    mail: Mail,
    current_user: CurrentUser,
    rejection_reason: Optional[str] = None,
):
    query = (
        select(Loan)
        .options(
            selectinload(Loan.loanable_item),
            selectinload(Loan.requester)
        )
        .where(Loan.id == loan_id)
    )
    db_loan = await session.scalar(query)

    if not db_loan:
        raise HTTPException(status_code=HTTPStatus.NOT_FOUND, detail='Loan not found.')

    try:
        
        db_loan.confirmed_by_id = current_user.id
        
        if confirm:
            db_loan.is_confirmed = True
            db_loan.rejection_reason = None

            stmt = (
                select(Loan)
                .where(Loan.id == loan_id)
                .options(
                    selectinload(Loan.requester),
                    selectinload(Loan.temporary_guardian),
                    selectinload(Loan.confirmed_by),
                    selectinload(Loan.executed_by),
                    selectinload(Loan.returned_by),
                    selectinload(Loan.loanable_item).options(
                        selectinload(LoanableItem.legal_guardian),
                        selectinload(LoanableItem.loans),
                        selectinload(LoanableItem.catalog).options(
                            selectinload(Catalog.images),
                            selectinload(Catalog.user),
                            selectinload(Catalog.asset).options(
                                selectinload(Asset.material),
                                selectinload(Asset.legal_guardian)
                            ),
                            selectinload(Catalog.location).options(
                                selectinload(Location.sector).options(
                                    selectinload(Sector.agency).options(
                                        selectinload(Agency.unit)
                                    )
                                )
                            ),
                            selectinload(Catalog.workflow_history).options(
                                selectinload(CatalogWorkFlow.user),
                                selectinload(CatalogWorkFlow.transfer_requests)
                            ),
                        )
                    )
                )
            )

            result = await session.execute(stmt)
            loan = result.unique().scalar_one_or_none()
            item = loan.loanable_item if loan else None

            if not item:
                raise HTTPException(    
                    status_code=404, 
                    detail="O item associado a este empréstimo não foi encontrado ou foi excluído."
                )

            items_html = ''.join(render_loan_terms(item,loan)) 

            ASSETS_DIR = (
                Path(__file__).resolve().parent.parent.parent / 'assets'
            ).resolve()
            lexend_regular = (ASSETS_DIR / 'Lexend-Variable.ttf').resolve().as_uri()

            full_html = f"""
                    <!DOCTYPE html>
                    <html lang="pt-br">
                    <head>
                        <meta charset="utf-8" />
                        <title>Termo de Empréstimo</title>
                        <style>
                            @page {{
                                size: A4;
                                margin: 0;
                            }}
                            
                            html, body {{
                                margin: 0;
                                padding: 0;
                                height: 100%;
                                background-color: #f9fafb;
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
            
            await mail_service.send_custom_email(
                mail=mail,
                user=db_loan.requester, 
                subject='Confirmação de Empréstimo - Vitrine Patrimônio',
                content=(
                    "Prezado(a),\n\n"
                    "Informamos que o seu pedido de empréstimo foi aceito pelo setor responsável.\n\n"
                    "Favor baixar o termo de compromisso, assiná-lo e entregar à equipe do Audiovisual ao pegar seu item.\n\n"
                    "Atenciosamente,\n"
                    "Equipe Vitrine Patrimônio"
                ),
                attachment_filename="Termo_de_compromisso.pdf",
                attachment_bytes=pdf_bytes
            )
        else:
            # Fluxo de Recusa
            db_loan.is_confirmed = False
            db_loan.is_executed = True
            db_loan.is_returned = True
            db_loan.rejection_reason = rejection_reason
            
            await mail_service.send_custom_email(
                mail=mail,
                user=db_loan.requester,
                subject='Empréstimo recusado - Vitrine Patrimônio',
                content=(
                    "Prezado(a),\n\n"
                    "Informamos que o seu pedido de empréstimo foi recusado pelo setor responsável.\n\n"
                    "Caso haja dúvidas, procure a equipe do Audiovisual.\n\n"
                    "Atenciosamente,\n"
                    "Equipe Vitrine Patrimônio"
                )
            )

        await session.commit()
        await session.refresh(db_loan)

        return {"msg": "Updated"}

    except Exception as e:
        await session.rollback()
        print(f"Erro ao confirmar empréstimo: {e}") 
        
        raise HTTPException(
            status_code=HTTPStatus.INTERNAL_SERVER_ERROR,
            detail=f"Não foi possível concluir a ação. Erro interno: {str(e)}"
        )
    
@router.patch('/execute/{loan_id}')
async def execute_loan(
    loan_id: UUID,
    session: Session,
    current_user: CurrentUser,
):
    """Marca que o item foi fisicamente entregue/retirado"""
    db_loan = await session.get(Loan, loan_id)

    if not db_loan:
        raise HTTPException(status_code=HTTPStatus.NOT_FOUND, detail='Loan not found.')
    
    if not db_loan.is_confirmed:
        raise HTTPException(
            status_code=HTTPStatus.BAD_REQUEST, 
            detail='Cannot execute a loan that has not been confirmed.'
        )

    db_loan.is_executed = True
    db_loan.executed_by_id = current_user.id 
    
    await session.commit()
    await session.refresh(db_loan)
    return {"msg": "Updated"}

@router.patch('/return/{loan_id}')
async def return_loan(
    loan_id: UUID,
    session: Session,
    current_user: CurrentUser,
    rejection_reason: Optional[str] = None,
):
    """Finaliza o empréstimo ou a manutenção"""
    db_loan = await session.get(Loan, loan_id)

    if not db_loan:
        raise HTTPException(status_code=HTTPStatus.NOT_FOUND, detail='Loan not found.')

    if not db_loan.is_executed:
         raise HTTPException(
            status_code=HTTPStatus.BAD_REQUEST, 
            detail='Cannot return an item that was never executed/delivered.'
        )

    now = datetime.now()
    db_loan.is_returned = True
    db_loan.rejection_reason = rejection_reason
    db_loan.returned_at = now
    db_loan.returned_by_id = current_user.id 
    
    await session.commit()
    await session.refresh(db_loan)
    return {"msg": "Updated"}

@router.patch('/send_maintenance/{item_id}')
async def send_maintenance(
    item_id: UUID,
    session: Session,
    current_user: CurrentUser, 
):
    """Coloca o item em manutenção e cria o registro de empréstimo sem data final"""
    
    db_item = await session.get(LoanableItem, item_id)
    if not db_item:
        raise HTTPException(status_code=HTTPStatus.NOT_FOUND, detail='Loanable item not found.')
    
    if db_item.in_maintenance:
        raise HTTPException(
            status_code=HTTPStatus.BAD_REQUEST, 
            detail='Este item já está em manutenção.'
        )

    db_item.in_maintenance = True
    
    maintenance_loan = Loan(
        loanable_item_id=db_item.id,
        requester_id=current_user.id,
        temporary_guardian_id=current_user.id, 
        start_at=datetime.now(),
        end_at=None,
        is_maintenance=True,
        is_confirmed=True,
        is_executed=True,
        is_returned=False,
        lend_detail="Manutenção",
        returned_detail=None,
        returned_at=None,
        rejection_reason=None,
        confirmed_by_id=current_user.id, 
        executed_by_id=current_user.id
    )
    
    session.add(maintenance_loan)
    await session.commit()
    
    return {"msg": "Item successfully sent to maintenance."}

@router.patch('/end_maintenance/{item_id}')
async def end_maintenance(
    item_id: UUID,
    session: Session,
    current_user: CurrentUser,
    is_vistoria: bool = False, 
):
    """Retira o item da manutenção e finaliza o empréstimo em aberto"""
    
    db_item = await session.get(LoanableItem, item_id)
    if not db_item:
        raise HTTPException(status_code=HTTPStatus.NOT_FOUND, detail='Loanable item not found.')
    
    if not db_item.in_maintenance:
        raise HTTPException(
            status_code=HTTPStatus.BAD_REQUEST, 
            detail='The item is not currently in maintenance.'
        )

    now = datetime.now()
    db_item.in_maintenance = False
    
    if is_vistoria:
        db_item.last_check = now

    query = select(Loan).where(
        Loan.loanable_item_id == item_id,
        Loan.is_maintenance == True,
        Loan.is_returned == False
    ).order_by(Loan.start_at.desc())
    
    result = await session.execute(query)
    active_maintenance_loan = result.scalars().first()
    
    if active_maintenance_loan:
        active_maintenance_loan.end_at = now
        active_maintenance_loan.returned_at = now
        active_maintenance_loan.is_returned = True
        active_maintenance_loan.returned_by_id = current_user.id 
    
    await session.commit()
    
    return {"msg": "Item successfully returned from maintenance."}

@router.patch('/changeVisibility/{catalog_id}')
async def change_visibility(
    catalog_id: UUID,
    session: Session, 
    current_user: CurrentUser,
):
    query = select(LoanableItem).where(LoanableItem.catalog_id == catalog_id)
    result = await session.execute(query)
    db_item = result.scalar_one_or_none()

    if not db_item:
        raise HTTPException(
            status_code=HTTPStatus.NOT_FOUND, 
            detail='Item de empréstimo não encontrado para este catálogo.'
        )

    db_item.is_visible = not db_item.is_visible
    
    await session.commit()
    
    return {
        "msg": "Visibility updated successfully.",
        "is_visible": db_item.is_visible
    }

@router.get('/pdf/{loan_id}')
async def export_catalog_pdf(
    session: Session,
    loan_id: UUID,
):
    stmt = (
        select(LoanableItem)
        .where(
            LoanableItem.id == loan_id,
            LoanableItem.deleted_at.is_(None)
        )
        .options(
            selectinload(LoanableItem.legal_guardian),
            selectinload(LoanableItem.loans),
            selectinload(LoanableItem.catalog).options(
                selectinload(Catalog.images),
                selectinload(Catalog.user),
                selectinload(Catalog.asset).options(
                    selectinload(Asset.material),
                    selectinload(Asset.legal_guardian)
                ),
                selectinload(Catalog.location).options(
                    selectinload(Location.sector).options(
                        selectinload(Sector.agency).options(
                            selectinload(Agency.unit)
                        )
                    )
                ),
                selectinload(Catalog.workflow_history).options(
                    selectinload(CatalogWorkFlow.user),
                    selectinload(CatalogWorkFlow.transfer_requests)
                ),
            )
        )
    )

    result = await session.execute(stmt)
    item = result.unique().scalar_one_or_none()

    if not item:
        raise HTTPException(    
            status_code=404, 
            detail="Este item de patrimônio não possui um registro de empréstimo (LoanableItem)."
        )

    items_html = ''.join(render_loanable_item(item))

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
            'Content-Disposition': 'inline; filename="Item.pdf"',
        },
    )

@router.get('/terms_pdf/{loan_id}')
async def export_terms_pdf(
    session: Session,
    loan_id: UUID,
):
    # 1. Busca a partir da tabela Loan, não LoanableItem
    stmt = (
        select(Loan)
        .where(
            Loan.id == loan_id
        )
        .options(
            # Carrega as pessoas envolvidas no empréstimo
            selectinload(Loan.requester),
            selectinload(Loan.temporary_guardian),
            
            # Carrega o item e TODA a árvore de dependências dele
            selectinload(Loan.loanable_item).options(
                selectinload(LoanableItem.legal_guardian),
                selectinload(LoanableItem.catalog).options(
                    selectinload(Catalog.images),
                    selectinload(Catalog.user),
                    selectinload(Catalog.asset).options(
                        selectinload(Asset.material),
                        selectinload(Asset.legal_guardian)
                    ),
                    selectinload(Catalog.location).options(
                        selectinload(Location.sector).options(
                            selectinload(Sector.agency).options(
                                selectinload(Agency.unit)
                            )
                        )
                    ),
                    selectinload(Catalog.workflow_history).options(
                        selectinload(CatalogWorkFlow.user),
                        selectinload(CatalogWorkFlow.transfer_requests)
                    ),
                )
            )
        )
    )

    result = await session.execute(stmt)
    loan = result.unique().scalar_one_or_none()

    # 2. Verifica se o empréstimo existe
    if not loan:
        raise HTTPException(    
            status_code=404, 
            detail="Empréstimo não encontrado."
        )

    # 3. Salva o item em uma variável separada
    item = loan.loanable_item

    if not item:
        raise HTTPException(    
            status_code=404, 
            detail="O item associado a este empréstimo não foi encontrado ou foi excluído."
        )

    # 4. Passa as duas variáveis para a função de renderização
    items_html = ''.join(render_loan_terms(item, loan))

    ASSETS_DIR = (
        Path(__file__).resolve().parent.parent.parent / 'assets'
    ).resolve()
    lexend_regular = (ASSETS_DIR / 'Lexend-Variable.ttf').resolve().as_uri()

    full_html = f"""
              <!DOCTYPE html>
              <html lang="pt-br">
              <head>
                  <meta charset="utf-8" />
                  <title>Termo de Empréstimo</title>
                  <style>
                      @page {{
                        size: A4;
                        margin: 0;
                      }}
                      
                      html, body {{
                          margin: 0;
                          padding: 0;
                          height: 100%;
                          background-color: #f9fafb;
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
            # Sugestão: Alterado o nome do arquivo baixado
            'Content-Disposition': 'inline; filename="termo_emprestimo.pdf"',
        },
    )
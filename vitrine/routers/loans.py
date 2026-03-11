from http import HTTPStatus
from typing import Optional, List
from uuid import UUID

from fastapi import APIRouter, HTTPException, Query
from sqlalchemy import select
from sqlalchemy.orm import selectinload
from datetime import datetime
from pathlib import Path
from weasyprint import HTML
from io import BytesIO
from fastapi.responses import StreamingResponse

from .utils import render_loanable_item

from vitrine.core.dependencies import CurrentUser, Session
from vitrine.models import (Catalog, 
    Loan, 
    LoanableItem,
    CatalogWorkFlow,
    Location,
    LocationInventory,
    SystemIdentity,
    User,
    UserRole,
    Asset,
    Sector,
    Agency, )
from vitrine.schemas import (
    LoanableItemPublic,
    LoanableItemList,
    LoanSchema,
    CatalogSchema,
)

router = APIRouter(prefix='/loans', tags=['empréstimos'])

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
    # 2. Cria a solicitação de empréstimo
    # is_confirmed começa como False até o dono aceitar
    db_loan = Loan(
        loanable_item_id=loan_data.loanable_item_id,
        requester_id=current_user.id,
        temporary_guardian_id=loan_data.temporary_guardian_id,
        start_at=loan_data.start_at,
        end_at=loan_data.end_at,
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


@router.get('/one', response_model=List[LoanSchema])
async def list_loans(
    session: Session,
    status: Optional[str] = None, # pendente, ativo, atrasado, concluido
    offset: int = Query(0, ge=0), # Adicionado para suportar a paginação do Kanban
    limit: int = Query(24, gt=0, le=100), # Adicionado para suportar a paginação do Kanban
):
    # Carregamento profundo idêntico ao do GET /items, 
    # garantindo que o Empréstimo devolva a foto e os detalhes completos do equipamento
    stmt = (
        select(Loan)
        .options(
            selectinload(Loan.requester),
            selectinload(Loan.temporary_guardian),
            
            # Mergulhando fundo no Loanable Item atrelado a este empréstimo
            selectinload(Loan.loanable_item).options(
                selectinload(LoanableItem.legal_guardian),
                
                selectinload(LoanableItem.catalog).options(
                    selectinload(Catalog.images),
                    selectinload(Catalog.files),
                    selectinload(Catalog.user),
                    
                    # Asset -> Material e Responsável
                    selectinload(Catalog.asset).options(
                        selectinload(Asset.material),
                        selectinload(Asset.legal_guardian)
                    ),
                    
                    # Location -> Sector -> Agency -> Unit
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
                    
                    # Workflow History
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
                    )
                )
            )
        )
        .order_by(Loan.start_at.desc())
    )

    # Filtros de negócio
    now = datetime.now()
    if status == 'atrasado':
        stmt = stmt.where(
            Loan.is_executed == True,
            Loan.is_returned == False,
            Loan.end_at < now
        )
    elif status == 'pendente':
        stmt = stmt.where(Loan.is_confirmed == False, Loan.rejection_reason.is_(None))
    elif status == 'ativo':
        stmt = stmt.where(Loan.is_executed == True, Loan.is_returned == False)

    # Aplica a paginação que o frontend envia
    stmt = stmt.offset(offset).limit(limit)

    result = await session.execute(stmt)
    
    # Retorna usando o unique() para evitar duplicação por conta das listas (images, workflow, etc)
    return result.unique().scalars().all()


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
            
            # 2. Histórico de Empréstimos (Necessário para a tela)
            selectinload(LoanableItem.loans),

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

    # DICA DE OURO: Bloco de Debug
    # Se ainda faltar algum campo, esse bloco vai "capturar" o erro e imprimir 
    # no terminal exatamente qual campo do Pydantic está reclamando.
    try:
        # Força a validação manualmente antes de retornar
        LoanableItemList.model_validate({'loanable_items': items})
    except Exception as e:
        print("====== ERRO DE VALIDAÇÃO DO SCHEMA ======")
        print(e)
        raise e

    return {'loanable_items': items}


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
    rejection_reason: Optional[str] = None,
):
    # 1. Busca o empréstimo e o item (para verificar quem é o dono)
    query = (
        select(Loan)
        .options(selectinload(Loan.loanable_item))
        .where(Loan.id == loan_id)
    )
    db_loan = await session.scalar(query)

    if not db_loan:
        raise HTTPException(status_code=HTTPStatus.NOT_FOUND, detail='Loan not found.')

    if confirm:
        db_loan.is_confirmed = True
        db_loan.rejection_reason = None
    else:
        db_loan.is_confirmed = False
        db_loan.is_executed = True
        db_loan.is_returned = True
        db_loan.rejection_reason = rejection_reason

    await session.commit()
    await session.refresh(db_loan)
    return {"msg": "Updated"}


@router.patch('/execute/{loan_id}')
async def execute_loan(
    loan_id: UUID,
    session: Session,
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
    await session.commit()
    await session.refresh(db_loan)
    return {"msg": "Updated"}


@router.patch('/return/{loan_id}')
async def return_loan(
    loan_id: UUID,
    session: Session,
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

    db_loan.is_returned = True
    await session.commit()
    await session.refresh(db_loan)
    return {"msg": "Updated"}


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
            'Content-Disposition': 'inline; filename="catalogo.pdf"',
        },
    )

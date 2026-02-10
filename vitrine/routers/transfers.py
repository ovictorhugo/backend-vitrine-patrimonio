from http import HTTPStatus
from fastapi import APIRouter, HTTPException, UploadFile, File
from sqlalchemy import select
from sqlalchemy.orm import selectinload
from pathlib import Path
from datetime import datetime
from uuid import UUID
from typing import Annotated


from vitrine.core.dependencies import Session
from vitrine.models import (
    TransferSigner,
    Catalog,
    User,
    Location,
    Sector,
    TransferDocument, 
)
from vitrine.schemas import (
    TransferCreate, 
    PendingDocumentSignsResponse,
    TransferDocumentSchema,
)

from fastapi.responses import StreamingResponse, FileResponse
from sqlalchemy.orm import selectinload,joinedload
from weasyprint import HTML
from io import BytesIO
from .utils import render_transfer_item, seal_pdf_digitally, verify_pdf_signature

router = APIRouter(prefix="/transfers", tags=["Transferências"])


@router.post('/verify_pdf')
async def verify_uploaded_pdf(
    file: UploadFile = File(...)
):
    """
    Recebe um PDF via upload, verifica a integridade e validade da assinatura digital.
    """
    if file.content_type != "application/pdf":
         raise HTTPException(status_code=400, detail="O arquivo deve ser um PDF.")

    # Lê os bytes do arquivo enviado
    content = await file.read()
    
    # Chama a função de validação criada no utils.py
    result = await verify_pdf_signature(content)
    
    return result


@router.get('/sign/{token}')
async def sign_pdf_with_token(
    token: UUID,
    session: Session,
):
    query = (
        select(TransferSigner)
        .where(TransferSigner.token == token)
        .options(
            selectinload(TransferSigner.transfer_document).options(
                selectinload(TransferDocument.signers).joinedload(TransferSigner.user),
                joinedload(TransferDocument.catalog).joinedload(Catalog.asset),
                joinedload(TransferDocument.location).joinedload(Location.sector).joinedload(Sector.agency)
            )
        )
    )
    
    result = await session.scalars(query)
    db_signer = result.first()

    if not db_signer:
        raise HTTPException(status_code=404, detail="Link inválido.")

    if db_signer.isSigned:
        return {"message": "Já assinado.", "status": "already_signed"}

    # 2. Marca assinatura
    db_signer.isSigned = True
    db_signer.signedAt = datetime.now()

    # 3. Lógica de Finalização
    document = db_signer.transfer_document
    all_signed = all(s.isSigned for s in document.signers)

    if all_signed:
        print(f"Finalizando Documento {document.id}...")
        
        document.status = "COMPLETED"

        try:
            # A) Gera PDF Visual
            raw_pdf_bytes = await generate_transfer_pdf(document)

            # B) Aplica Assinatura Digital (Certificados UFMG)
            final_pdf_bytes = await seal_pdf_digitally(raw_pdf_bytes)

            STORAGE_ROOT = Path("vitrine/storage/transfer_pdfs")
            
            STORAGE_ROOT.mkdir(parents=True, exist_ok=True)
            
            filename = f"{document.id}.pdf"
            full_path = STORAGE_ROOT / filename
            
            # D) Salva no Disco
            with open(full_path, "wb") as f:
                f.write(final_pdf_bytes)
            
            print(f"PDF salvo em: {full_path}")

        except Exception as e:
            print(f"Erro ao salvar PDF final: {e}")
            # O status continua COMPLETED, mas o arquivo pode não ter sido gerado.
            # Idealmente ter um sistema de retry ou log de erro crítico aqui.

    await session.commit()
    
    return {
        "message": "Assinatura realizada com sucesso!",
        "document_status": document.status
    }

async def generate_transfer_pdf(document:TransferDocument) -> str:
    
    catalog = document.catalog
    location = document.location
    signers = document.signers

    ASSETS_DIR = (Path(__file__).resolve().parent.parent / 'assets').resolve()
    lexend_regular = (ASSETS_DIR / 'Lexend-Variable.ttf').resolve().as_uri()    
  
    content_placeholder = render_transfer_item(catalog,signers,location)

    full_html = f"""
              <!DOCTYPE html>
              <html lang="pt-br">
              <head>
                  <meta charset="utf-8" />
                  <title>Transferência</title>
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
                {content_placeholder}
              </body>
              </html>
        """

    pdf_bytes: bytes = HTML(string=full_html, encoding='utf-8').write_pdf()

    return pdf_bytes

@router.post('/pdf/', status_code=HTTPStatus.CREATED)
async def create_transfer_process(
    transfer_data: TransferCreate,
    session: Session,

):
    owner_id = transfer_data.owner
    new_guardian_id = transfer_data.new_guardian

    owner_user = await session.get(User, owner_id)
    new_guardian_user = await session.get(User, new_guardian_id)

    if not owner_user:
        raise HTTPException(status_code=404, detail=f"Owner user {owner_id} not found")
    if not new_guardian_user:
        raise HTTPException(status_code=404, detail=f"New Guardian user {new_guardian_id} not found")

    db_document = TransferDocument(
        catalog_id=transfer_data.catalog_id,
        location_id=transfer_data.location_id,
        file_path=None,
        current_step=0,
        status="PENDING"    
    )

    session.add(db_document)
    await session.flush() # Para gerar o ID do documento

    signer_owner = TransferSigner(
        transfer_document_id=db_document.id,
        user_id=owner_id, # Convertendo UUID para string conforme seu Model
        isSigned=False,
        signedAt=None
    )

    signer_guardian = TransferSigner(
        transfer_document_id=db_document.id,
        user_id=new_guardian_id,
        isSigned=False,
        signedAt=None
    )

    signer_chief_1 = TransferSigner(
        transfer_document_id=db_document.id,
        user_id=None,
        isSigned=False,
        signedAt=None
    )

    signer_chief_2 = TransferSigner(
        transfer_document_id=db_document.id,
        user_id=None,
        isSigned=False,
        signedAt=None
    )

    session.add(signer_owner)
    session.add(signer_guardian)
    session.add(signer_chief_1)
    session.add(signer_chief_2)

    catalog = await session.get(Catalog, db_document.catalog_id)
     
    real_filename = f"transferencia_{catalog.asset.asset_code}_{datetime.now().timestamp()}.pdf"
    
    db_document.file_path = real_filename
    session.add(db_document)
    await session.commit()

    # TODO: Disparar BackgroundTask para enviar email para o assinante de ordem 0
    
    return {"message": "Processo criado", "document_id": db_document.id}

@router.get('/pending/{user_id}', response_model=PendingDocumentSignsResponse)
async def read_pending_entries(
    session: Session,
    user_id: UUID,
):
    query = (
        select(TransferDocument) 
        .join(TransferSigner, TransferSigner.transfer_document_id == TransferDocument.id)
        .where(
            TransferSigner.user_id == user_id, 
            TransferDocument.status == "PENDING"   
        )
        .options(
            joinedload(TransferDocument.catalog).joinedload(Catalog.asset),
            joinedload(TransferDocument.location).joinedload(Location.sector).joinedload(Sector.agency),
            selectinload(TransferDocument.signers).joinedload(TransferSigner.user)
        )
    )
    result = await session.scalars(query)
    entries = result.unique().all()

    return {'pending': entries}

@router.get('/completed/{user_id}', response_model=PendingDocumentSignsResponse)
async def read_completed_entries(
    session: Session,
    user_id: UUID,
):
    query = (
        select(TransferDocument) 
        .join(TransferSigner, TransferSigner.transfer_document_id == TransferDocument.id)
        .where(
            TransferSigner.user_id == user_id, 
            TransferDocument.status == "COMPLETED"   
        )
        .options(
            joinedload(TransferDocument.catalog).joinedload(Catalog.asset),
            joinedload(TransferDocument.location).joinedload(Location.sector).joinedload(Sector.agency),
            selectinload(TransferDocument.signers).joinedload(TransferSigner.user)
        )
    )
    result = await session.scalars(query)
    entries = result.unique().all()

    return {'pending': entries}

@router.get('/pdf/{document_id}')
async def get_pdf_from_entry(
    session: Session,
    document_id: UUID,
):
    options = [
        joinedload(TransferDocument.catalog).joinedload(Catalog.asset),
        joinedload(TransferDocument.location).joinedload(Location.sector).joinedload(Sector.agency),
        selectinload(TransferDocument.signers).joinedload(TransferSigner.user)
    ]
    
    db_document = await session.get(TransferDocument, document_id, options=options)   
    
    if not db_document or db_document.deleted_at:
        raise HTTPException(
            status_code=HTTPStatus.NOT_FOUND, detail='Document entry not found'
        )

    STORAGE_DIR = Path("vitrine/storage/transfer_pdfs")
    filename = f"{document_id}.pdf"
    file_path = STORAGE_DIR / filename

    if db_document.status == "COMPLETED" and file_path.exists():
        return FileResponse(
            path=file_path,
            media_type='application/pdf',
            headers={'Content-Disposition': f'inline; filename="{filename}"'}
        )

    pdf_bytes = await generate_transfer_pdf(db_document)

    # Se o status no banco diz que está COMPLETED, mas o arquivo não existia (caiu aqui),
    # precisamos assinar e salvar agora para corrigir (Auto-healing).
    if db_document.status == "COMPLETED":
        try:
            pdf_bytes_signed = seal_pdf_digitally(pdf_bytes)
            pdf_bytes = pdf_bytes_signed
            STORAGE_DIR.mkdir(parents=True, exist_ok=True) 
            
            with open(file_path, "wb") as f:
                f.write(pdf_bytes)
            
            print(f"♻️ PDF recuperado (assinado e salvo) em: {file_path}")

        except Exception as e:
            print(f"❌ Erro ao tentar salvar fallback do PDF: {e}")
    
    return StreamingResponse(
        BytesIO(pdf_bytes),
        media_type='application/pdf',
        headers={
            'Content-Disposition': 'inline; filename="catalogo.pdf"',
        },
    )

@router.get('/details/{document_id}', response_model=TransferDocumentSchema)
async def get_document_full_details(session: Session,document_id: UUID):
    options = [
            joinedload(TransferDocument.catalog).joinedload(Catalog.asset),
            joinedload(TransferDocument.location).joinedload(Location.sector).joinedload(Sector.agency),
            selectinload(TransferDocument.signers).joinedload(TransferSigner.user)
    ]

    
    db_document = await session.get(TransferDocument, document_id, options=options)   
    
    if not db_document or db_document.deleted_at:
        raise HTTPException(
            status_code=HTTPStatus.NOT_FOUND, detail='Document entry not found'
        )
    
    return db_document

@router.get('/details_by_token/{token}', response_model=TransferDocumentSchema)
async def get_document_details_by_token(
    session: Session,
    token: UUID, 
):
    options = [
        joinedload(TransferDocument.catalog).joinedload(Catalog.asset),
        joinedload(TransferDocument.location).joinedload(Location.sector).joinedload(Sector.agency),
        selectinload(TransferDocument.signers).joinedload(TransferSigner.user)
    ]

    query = (
        select(TransferDocument)
        .join(TransferSigner, TransferSigner.transfer_document_id == TransferDocument.id)
        .where(TransferSigner.token == token)
        .options(*options)
    )
    
    result = await session.scalars(query)
    db_document = result.first()
    
    if not db_document or db_document.deleted_at:
        raise HTTPException(
            status_code=HTTPStatus.NOT_FOUND, 
            detail='Link de assinatura inválido ou documento não encontrado.'
        )
    
    return db_document
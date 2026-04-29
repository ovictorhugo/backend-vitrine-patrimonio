from sqlalchemy import select
from fastapi import HTTPException, APIRouter
from fastapi.responses import FileResponse
from http import HTTPStatus
from pathlib import Path
from uuid import UUID

from vitrine.core.dependencies import Session

from vitrine.models import (
    TemporaryFileReference,
    )

router = APIRouter(prefix="/temporary_files", tags=["Arquivos temporários"])

TEMP_DIR = (Path(__file__).resolve().parent.parent / "storage" / "temp").resolve()

@router.get('/download-by-token/{token}')
async def get_pdf_from_entry(
    session: Session,
    token: UUID,
):
    query = select(TemporaryFileReference).where(
        TemporaryFileReference.token == token,
        TemporaryFileReference.deleted_at.is_(None)
    )
    
    result = await session.execute(query)
    db_document = result.scalar_one_or_none()
    
    if not db_document:
        raise HTTPException(
            status_code=HTTPStatus.NOT_FOUND, 
            detail='Document entry not found or expired'
        )

    file_path = TEMP_DIR / db_document.file_name

    return FileResponse(
        path=file_path,
        media_type='application/pdf',
        headers={'Content-Disposition': f'inline; filename="{db_document.file_name}.pdf"'}
    )
    
    raise HTTPException(
        status_code=HTTPStatus.NOT_FOUND, 
        detail='File is not ready or does not exist on disk'
    )
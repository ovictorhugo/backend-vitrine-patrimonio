import os
from http import HTTPStatus
from typing import Annotated
from uuid import UUID, uuid4

from fastapi import APIRouter, Depends, HTTPException, UploadFile
from sqlalchemy import func, select

from vitrine.core.dependencies import CurrentUser, Session
from vitrine.models import Catalog, CatalogFile
from vitrine.schemas import (
    CatalogFileList,
    CatalogFilePublic,
    FilterCatalogImage,
    Message,
)

router = APIRouter(prefix='/catalog', tags=['Vitrine - Arquivos de Anúncios'])


@router.get('/files', response_model=CatalogFileList)
async def list_catalog_files(
    session: Session,
    filters: Annotated[FilterCatalogImage, Depends()],
):
    query = select(CatalogFile)

    if filters.random:
        query = query.order_by(func.random())
    else:
        query = query.order_by(CatalogFile.created_at.desc())

    query = query.offset(filters.offset).limit(filters.limit)
    result = await session.scalars(query)
    files = result.all()
    return {'files': files}


@router.post(
    '/{catalog_id}/files',
    status_code=HTTPStatus.CREATED,
    response_model=CatalogFilePublic,
)
async def upload_catalog_file(
    catalog_id: UUID,
    file: UploadFile,
    session: Session,
):
    db_catalog = await session.get(Catalog, catalog_id)
    if not db_catalog:
        raise HTTPException(status_code=404, detail='Catalog entry not found')

    ext = os.path.splitext(file.filename)[1]
    filename = f'{uuid4()}{ext}'
    file_path = os.path.join('vitrine/storage/uploads', filename)

    with open(file_path, 'wb') as buffer:
        buffer.write(await file.read())

    public_path = f'/uploads/{filename}'
    db_file = CatalogFile(
        catalog_id=catalog_id,
        file_path=public_path,
        file_name=file.filename,
        content_type=file.content_type,
    )

    session.add(db_file)
    await session.commit()
    await session.refresh(db_file)
    await session.refresh(db_catalog, ['files'])
    return db_file


@router.delete('/{catalog_id}/files/{file_id}', response_model=Message)
async def delete_catalog_file(
    catalog_id: UUID,
    file_id: UUID,
    session: Session,
    current_user: CurrentUser,
):
    db_file = await session.get(CatalogFile, file_id)
    if not db_file or db_file.catalog_id != catalog_id:
        raise HTTPException(status_code=404, detail='File not found')

    db_catalog = await session.get(Catalog, catalog_id)
    if not db_catalog:
        raise HTTPException(status_code=404, detail='Catalog entry not found')

    file_full_path = os.path.join(
        'vitrine/storage', db_file.file_path.lstrip('/')
    )
    if os.path.exists(file_full_path):
        os.remove(file_full_path)

    await session.delete(db_file)
    await session.commit()
    await session.refresh(db_catalog, ['files'])

    return {'message': 'File deleted'}

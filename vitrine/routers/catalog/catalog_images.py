import os
from http import HTTPStatus
from typing import Annotated
from uuid import UUID, uuid4

from fastapi import APIRouter, Depends, HTTPException, UploadFile
from sqlalchemy import func, select

from vitrine.core.dependencies import CurrentUser, Session
from vitrine.models import Catalog, CatalogImage
from vitrine.schemas import (
    CatalogImageList,
    CatalogImagePublic,
    FilterCatalogImage,
    Message,
)

router = APIRouter(prefix='/catalog', tags=['Vitrine - Imagens de Anúncios'])


@router.get('/images', response_model=CatalogImageList)
async def list_catalog_images(
    session: Session,
    filters: Annotated[FilterCatalogImage, Depends()],
):
    query = select(CatalogImage)

    if filters.random:
        query = query.order_by(func.random())
    else:
        query = query.order_by(CatalogImage.created_at.desc())

    query = query.offset(filters.offset).limit(filters.limit)

    result = await session.scalars(query)
    images = result.all()

    return {'images': images}


@router.post(
    '/{catalog_id}/images',
    status_code=HTTPStatus.CREATED,
    response_model=CatalogImagePublic,
)
async def upload_catalog_image(
    catalog_id: UUID, file: UploadFile, session: Session
):
    db_catalog = await session.get(Catalog, catalog_id)
    if not db_catalog:
        raise HTTPException(status_code=404, detail='Catalog entry not found')

    filename = f'{uuid4()}{os.path.splitext(file.filename)[1]}'
    file_path = os.path.join('vitrine/storage/uploads', filename)

    with open(file_path, 'wb') as buffer:
        buffer.write(await file.read())

    public_path = f'/uploads/{filename}'
    db_image = CatalogImage(catalog_id=catalog_id, file_path=public_path)
    session.add(db_image)
    await session.commit()
    await session.refresh(db_catalog, ['images'])
    return db_image


@router.delete('/{catalog_id}/images/{image_id}', response_model=Message)
async def delete_catalog_image(
    catalog_id: UUID,
    image_id: UUID,
    session: Session,
    current_user: CurrentUser,
):
    db_image = await session.get(CatalogImage, image_id)
    if not db_image or db_image.catalog_id != catalog_id:
        raise HTTPException(status_code=404, detail='Image not found')

    db_catalog = await session.get(Catalog, catalog_id)
    if not db_catalog:
        raise HTTPException(status_code=404, detail='Catalog entry not found')

    file_full_path = os.path.join(
        'vitrine/storage', db_image.file_path.lstrip('/')
    )
    if os.path.exists(file_full_path):
        os.remove(file_full_path)

    await session.delete(db_image)
    await session.commit()

    await session.refresh(db_catalog, ['images'])

    return {'message': 'Image deleted'}

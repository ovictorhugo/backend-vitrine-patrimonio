from datetime import datetime
from http import HTTPStatus
from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, File, HTTPException, Query, UploadFile
from pydantic import ValidationError
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from vitrine import service
from vitrine.database import get_session
from vitrine.models import Asset, User
from vitrine.schemas import (
    AssetList,
    AssetPublic,
    AssetSchema,
    FilterPage,
    Message,
)
from vitrine.security import get_current_user

router = APIRouter(prefix='/assets', tags=['vitrine - patrimônio'])
Session = Annotated[AsyncSession, Depends(get_session)]
CurrentUser = Annotated[User, Depends(get_current_user)]


@router.post('/', status_code=HTTPStatus.CREATED, response_model=AssetPublic)
async def create_asset(
    asset: AssetSchema,
    session: Session,
    current_user: CurrentUser,
):
    db_asset = Asset(**asset.model_dump())
    session.add(db_asset)
    await session.commit()
    await session.refresh(db_asset)
    return db_asset


@router.post('/upload', status_code=HTTPStatus.CREATED, response_model=Message)
async def create_assets_from_file(
    session: Session,
    current_user: CurrentUser,
    file: UploadFile = File(...),
):
    assets = service.file_to_list(file)

    if not assets:
        raise HTTPException(
            status_code=HTTPStatus.BAD_REQUEST,
            detail='The file is empty or does not contain valid data.',
        )
    assets = await service.find_relationships(assets, session, current_user.id)

    try:
        db_assets = service.align_assets(assets)
    except ValidationError as E:
        raise HTTPException(
            status_code=HTTPStatus.UNPROCESSABLE_ENTITY,
            detail=f'Validation error in file data: {E.errors()}',
        )

    session.add_all(db_assets)
    await session.commit()
    return {'message': f'{len(db_assets)} ativos criados com sucesso.'}


@router.get('/', response_model=AssetList)
async def read_assets(
    session: Session,
    filter_assets: Annotated[FilterPage, Query()],
):
    query = await session.scalars(
        select(Asset)
        .where(Asset.deleted_at.is_(None))
        .offset(filter_assets.offset)
        .limit(filter_assets.limit)
    )
    assets = query.all()
    return {'assets': assets}


@router.put('/{asset_id}', response_model=AssetPublic)
async def update_asset(
    asset_id: UUID,
    asset: AssetSchema,
    session: Session,
    current_user: CurrentUser,
):
    db_asset = await session.get(Asset, asset_id)
    if not db_asset or db_asset.deleted_at is not None:
        raise HTTPException(
            status_code=HTTPStatus.NOT_FOUND, detail='Asset not found'
        )

    for field, value in asset.model_dump().items():
        setattr(db_asset, field, value)

    await session.commit()
    await session.refresh(db_asset)
    return db_asset


@router.delete('/{asset_id}', response_model=Message)
async def delete_asset(
    asset_id: UUID,
    session: Session,
    current_user: CurrentUser,
):
    db_asset = await session.get(Asset, asset_id)
    if not db_asset or db_asset.deleted_at is not None:
        raise HTTPException(
            status_code=HTTPStatus.NOT_FOUND, detail='Asset not found'
        )

    db_asset.deleted_at = datetime.now()
    await session.commit()
    return {'message': 'Asset deleted'}

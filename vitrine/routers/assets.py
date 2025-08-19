from datetime import datetime
from http import HTTPStatus
from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, File, HTTPException, UploadFile
from pydantic import ValidationError
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from vitrine import service
from vitrine.database import get_session
from vitrine.models import Asset, User
from vitrine.schemas import (
    AssetCheckDigit,
    AssetCode,
    AssetList,
    AssetPublic,
    AssetSchema,
    AtmNumber,
    FilterAsset,
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
    session: Session, filters: Annotated[FilterAsset, Depends()]
):
    query = select(Asset).where(Asset.deleted_at.is_(None))

    if filters.q:
        prefix_query = ' & '.join(word + ':*' for word in filters.q.split())
        ts_query = func.to_tsquery('portuguese', prefix_query)
        query = query.where(Asset.tsv.op('@@')(ts_query))

    if filters.asset_code:
        query = query.where(Asset.asset_code == filters.asset_code)
    if filters.asset_check_digit:
        query = query.where(
            Asset.asset_check_digit == filters.asset_check_digit
        )
    if filters.atm_number:
        query = query.where(Asset.atm_number == filters.atm_number)

    if filters.agency_id:
        query = query.where(Asset.agency_id == filters.agency_id)
    if filters.unit_id:
        query = query.where(Asset.unit_id == filters.unit_id)
    if filters.sector_id:
        query = query.where(Asset.sector_id == filters.sector_id)
    if filters.material_id:
        query = query.where(Asset.material_id == filters.material_id)
    query = query.offset(filters.offset).limit(filters.limit)

    result = await session.scalars(query)
    assets = result.all()

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


@router.get('/search/asset-code', response_model=AssetCode)
async def search_by_asset_code(
    q: str,
    session: Session,
):
    query = (
        select(Asset.asset_code)
        .where(Asset.deleted_at.is_(None))
        .where(Asset.asset_code.ilike(f'{q}%'))
    )
    result = await session.scalars(query)
    return {'asset_code': result.all()}


@router.get('/search/asset-check-digit', response_model=AssetCheckDigit)
async def search_by_asset_check_digit(
    q: str,
    session: Session,
):
    query = (
        select(Asset.asset_check_digit)
        .where(Asset.deleted_at.is_(None))
        .where(Asset.asset_check_digit.ilike(f'{q}%'))
    )
    result = await session.scalars(query)
    return {'asset_check_digit': result.all()}


@router.get('/search/atm-number', response_model=AtmNumber)
async def search_by_atm_number(
    q: str,
    session: Session,
):
    query = (
        select(Asset.atm_number)
        .where(Asset.deleted_at.is_(None))
        .where(Asset.atm_number.ilike(f'{q}%'))
    )
    result = await session.scalars(query)
    return {'atm_number': result.all()}

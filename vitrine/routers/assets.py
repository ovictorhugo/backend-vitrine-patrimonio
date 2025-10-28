import os
import shutil
from datetime import datetime
from http import HTTPStatus
from typing import Annotated
from uuid import UUID, uuid4

from fastapi import (
    APIRouter,
    BackgroundTasks,
    Depends,
    File,
    HTTPException,
    UploadFile,
)
from sqlalchemy import func, select
from sqlalchemy.orm import selectinload

from vitrine.dependencies import CurrentUser, Session
from vitrine.models import Asset, Location, LocationInventory
from vitrine.schemas import (
    AssetCheckDigitList,
    AssetCodeList,
    AssetIdentifierList,
    AssetList,
    AssetPublic,
    AssetSchema,
    AtmNumberList,
    FilterAsset,
    Message,
)
from vitrine.services import filter_service, service

router = APIRouter(prefix='/assets', tags=['vitrine - patrimônio'])


@router.post('/', status_code=HTTPStatus.CREATED, response_model=AssetPublic)
async def create_asset(
    asset: AssetSchema,
    session: Session,
    current_user: CurrentUser,
):
    db_asset = Asset(**asset.model_dump(), user_id=current_user.id)
    session.add(db_asset)
    await session.commit()
    await session.refresh(db_asset)
    return db_asset


async def process_file(filepath: str, session: Session, user_id):
    assets = service.file_to_list_from_path(filepath)
    if not assets:
        return
    assets = await service.find_relationships(assets, session, user_id)
    db_assets = await service.align_assets(session, assets, user_id)
    session.add_all(db_assets)
    await session.commit()
    os.remove(filepath)


@router.post('/upload', status_code=HTTPStatus.CREATED, response_model=Message)
async def create_assets_from_file(
    background_tasks: BackgroundTasks,
    session: Session,
    current_user: CurrentUser,
    file: UploadFile = File(...),
):
    ext = os.path.splitext(file.filename)[-1].lower()
    filename = f'{uuid4().hex}{ext}'
    filepath = os.path.join('vitrine', 'storage', filename)

    with open(filepath, 'wb') as buffer:
        shutil.copyfileobj(file.file, buffer)

    background_tasks.add_task(process_file, filepath, session, current_user.id)

    return {
        'message': 'Arquivo enviado com sucesso. O processamento ocorrerá em segundo plano.'
    }


@router.get('/', response_model=AssetList)
async def read_assets(
    session: Session, filters: Annotated[FilterAsset, Depends()]
):
    query = select(Asset).options(
        selectinload(Asset.location)
        .selectinload(Location.location_inventories)
        .joinedload(LocationInventory.inventory)
    )

    if filters.q:
        prefix_query = ' & '.join(word + ':*' for word in filters.q.split())
        ts_query = func.to_tsquery('portuguese', prefix_query)
        query = query.where(Asset.tsv.op('@@')(ts_query))

    query = filter_service.apply_asset_filters(query, filters)

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


@router.get('/search/asset-code', response_model=AssetCodeList)
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


@router.get('/search/asset-check-digit', response_model=AssetCheckDigitList)
async def search_by_asset_check_digit(
    session: Session,
    q: str = str(),
):
    query = (
        select(Asset.asset_check_digit)
        .where(Asset.deleted_at.is_(None))
        .where(Asset.asset_check_digit.ilike(f'{q}%'))
    )
    result = await session.scalars(query)
    return {'asset_check_digit': result.all()}


@router.get('/search/atm-number', response_model=AtmNumberList)
async def search_by_atm_number(
    session: Session,
    q: str = str(),
):
    query = (
        select(Asset.atm_number)
        .where(Asset.deleted_at.is_(None))
        .where(Asset.atm_number.ilike(f'{q}%'))
    )
    result = await session.scalars(query)
    return {'atm_number': result.all()}


@router.get('/search/asset-identifier', response_model=AssetIdentifierList)
async def search_by_asset_identifier(
    session: Session,
    q: str = str(),
):
    q = q.replace('-', '')
    query = (
        select(func.concat(Asset.asset_code, '-', Asset.asset_check_digit))
        .where(Asset.deleted_at.is_(None))
        .where(
            func.concat(Asset.asset_code, Asset.asset_check_digit).ilike(
                f'{q}%'
            )
        )
    )
    result = await session.scalars(query)
    return {'asset_identifier': result.all()}

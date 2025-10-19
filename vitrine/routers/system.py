from datetime import datetime
from http import HTTPStatus
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import select

from vitrine.dependencies import CurrentUser, Session
from vitrine.models import (
    SystemSetting,
)
from vitrine.schemas import (
    FilterPage,
    Message,
    SystemSettingCreate,
    SystemSettingList,
    SystemSettingPublic,
    SystemSettingUpdate,
)

router = APIRouter(prefix='/settings', tags=['configurações do sistema'])


def check_is_admin(current_user: CurrentUser):
    is_admin = any(role.name == 'Administrador' for role in current_user.roles)
    if not is_admin:
        raise HTTPException(
            status_code=HTTPStatus.FORBIDDEN,
            detail='Acesso restrito a administradores',
        )


@router.post(
    '/', status_code=HTTPStatus.CREATED, response_model=SystemSettingPublic
)
async def create_setting(
    setting: SystemSettingCreate,
    session: Session,
    current_user: CurrentUser,
):
    check_is_admin(current_user)

    db_setting = await session.scalar(
        select(SystemSetting).where(SystemSetting.key == setting.key)
    )

    if db_setting and not db_setting.deleted_at:
        raise HTTPException(
            status_code=HTTPStatus.CONFLICT,
            detail='Uma configuração com esta chave (key) já existe',
        )

    if db_setting and db_setting.deleted_at:
        db_setting.value = setting.value
        db_setting.description = setting.description
        db_setting.deleted_at = None
        db_setting.updated_at = datetime.now()
    else:
        db_setting = SystemSetting(**setting.model_dump())

    session.add(db_setting)
    await session.commit()
    await session.refresh(db_setting)

    return db_setting


@router.get('/', response_model=SystemSettingList)
async def read_settings(
    session: Session,
    current_user: CurrentUser,
    filter_page: Annotated[FilterPage, Depends()],  # Paginação
):
    query = (
        select(SystemSetting)
        .where(SystemSetting.deleted_at.is_(None))
        .order_by(SystemSetting.key)
        .offset(filter_page.offset)
        .limit(filter_page.limit)
    )
    result = await session.scalars(query)
    settings = result.all()

    return {'settings': settings}


@router.put('/{key}', response_model=SystemSettingPublic)
async def update_setting(
    key: str,
    setting_update: SystemSettingUpdate,
    session: Session,
    current_user: CurrentUser,
):
    check_is_admin(current_user)

    db_setting = await session.scalar(
        select(SystemSetting).where(
            SystemSetting.key == key, SystemSetting.deleted_at.is_(None)
        )
    )

    if not db_setting:
        raise HTTPException(
            status_code=HTTPStatus.NOT_FOUND,
            detail='Configuração não encontrada',
        )

    update_data = setting_update.model_dump(exclude_unset=True)

    if not update_data:
        return db_setting

    for field, value in update_data.items():
        setattr(db_setting, field, value)

    session.add(db_setting)
    await session.commit()
    await session.refresh(db_setting)

    return db_setting


@router.delete('/{key}', response_model=Message)
async def delete_setting(
    key: str, session: Session, current_user: CurrentUser
):
    check_is_admin(current_user)

    db_setting = await session.scalar(
        select(SystemSetting).where(SystemSetting.key == key)
    )

    if not db_setting:
        raise HTTPException(
            status_code=HTTPStatus.NOT_FOUND,
            detail='Configuração não encontrada',
        )

    if db_setting.deleted_at:
        raise HTTPException(
            status_code=HTTPStatus.BAD_REQUEST,
            detail='Configuração já está desativada',
        )

    db_setting.deleted_at = datetime.now()
    session.add(db_setting)
    await session.commit()

    return {'message': 'Configuração desativada com sucesso'}

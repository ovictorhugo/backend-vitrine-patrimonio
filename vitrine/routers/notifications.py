from datetime import datetime
from http import HTTPStatus
from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import select
from sqlalchemy.orm import selectinload

from vitrine.dependencies import CurrentUser, Session
from vitrine.models import Notification, User
from vitrine.schemas import (
    FilterNotification,
    Message,
    NotificationCreateSchema,
    NotificationList,
    NotificationPublic,
    NotificationUpdateSchema,
)

router = APIRouter(
    prefix='/notifications',
    tags=['funcionalidades - notificações'],
)


@router.post(
    '/',
    status_code=HTTPStatus.CREATED,
    response_model=NotificationPublic,
)
async def create_notification(
    notification_data: NotificationCreateSchema,
    session: Session,
    current_user: CurrentUser,
):
    target_user = await session.get(User, notification_data.target_user_id)
    if not target_user or target_user.deleted_at:
        raise HTTPException(
            status_code=HTTPStatus.NOT_FOUND,
            detail='Target user not found or has been deactivated',
        )

    db_notification = Notification(
        target_user_id=notification_data.target_user_id,
        source_user_id=current_user.id,
        type=notification_data.type,
        detail=notification_data.detail,
    )

    session.add(db_notification)
    await session.commit()
    await session.refresh(db_notification)

    return db_notification


@router.get('/', response_model=NotificationList)
async def read_notifications(
    session: Session,
    current_user: CurrentUser,
    filters: Annotated[FilterNotification, Depends()],
):
    query = (
        select(Notification)
        .where(
            Notification.target_user_id == current_user.id,
            Notification.deleted_at.is_(None),
        )
        .options(selectinload(Notification.source_user))
        .offset(filters.offset)
        .limit(filters.limit)
        .order_by(Notification.created_at.desc())
    )

    if filters.read is True:
        query = query.where(Notification.read_at.is_not(None))
    elif filters.read is False:
        query = query.where(Notification.read_at.is_(None))

    if filters.type:
        query = query.where(Notification.type == filters.type)

    db_notifications = await session.scalars(query)
    return {'notifications': db_notifications.all()}


@router.patch('/{notification_id}', response_model=NotificationPublic)
async def update_notification_status(
    notification_id: UUID,
    update_data: NotificationUpdateSchema,
    session: Session,
    current_user: CurrentUser,
):
    query = select(Notification).where(
        Notification.id == notification_id,
        Notification.deleted_at.is_(None),
    )
    notification = await session.scalar(query)

    if not notification:
        raise HTTPException(
            status_code=HTTPStatus.NOT_FOUND,
            detail='Notification not found',
        )

    if notification.target_user_id != current_user.id:
        raise HTTPException(
            status_code=HTTPStatus.FORBIDDEN, detail='Not enough permissions'
        )

    if update_data.read and not notification.read_at:
        notification.read_at = datetime.now()
    elif not update_data.read:
        notification.read_at = None

    await session.commit()
    await session.refresh(notification)

    return notification


@router.delete('/{notification_id}', response_model=Message)
async def delete_notification(
    notification_id: UUID,
    session: Session,
    current_user: CurrentUser,
):
    query = select(Notification).where(
        Notification.id == notification_id,
        Notification.deleted_at.is_(None),
    )
    notification = await session.scalar(query)

    if not notification:
        raise HTTPException(
            status_code=HTTPStatus.NOT_FOUND,
            detail='Notification not found',
        )

    if notification.target_user_id != current_user.id:
        raise HTTPException(
            status_code=HTTPStatus.FORBIDDEN, detail='Not enough permissions'
        )

    notification.deleted_at = datetime.now()
    await session.commit()

    return {'message': 'Notification deleted'}

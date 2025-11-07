from datetime import datetime
from http import HTTPStatus
from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import select
from sqlalchemy.orm import selectinload

from vitrine.core.dependencies import CurrentUser, Session
from vitrine.models import Notification, User, UserNotification
from vitrine.schemas import (
    FilterNotification,
    FilterPage,
    Message,
    NotificationCreateSchema,
    NotificationList,
    NotificationPublic,
    NotificationSentList,
    NotificationSentPublic,
    NotificationUpdateSchema,
)

router = APIRouter(
    prefix='/notifications',
    tags=['utilidades - notificações'],
)


@router.post(
    '/',
    status_code=HTTPStatus.CREATED,
    response_model=NotificationSentPublic,
)
async def create_notification(
    notification_data: NotificationCreateSchema,
    session: Session,
    current_user: CurrentUser,
):
    target_ids = (
        notification_data.target_user_id.split(';')
        if notification_data.target_user_id != '*'
        else '*'
    )

    if target_ids == '*':
        users = await session.scalars(
            select(User).where(User.deleted_at.is_(None))
        )
        target_users = users.all()
    else:
        try:
            valid_target_ids = [UUID(uid) for uid in target_ids]
        except ValueError:
            raise HTTPException(
                status_code=HTTPStatus.UNPROCESSABLE_ENTITY,
                detail='Invalid UUID format in target_user_id',
            )
        users = await session.scalars(
            select(User).where(
                User.id.in_(valid_target_ids), User.deleted_at.is_(None)
            )
        )
        target_users = users.all()

    if not target_users:
        raise HTTPException(
            status_code=HTTPStatus.NOT_FOUND,
            detail='Target user(s) not found or deactivated',
        )

    notification = Notification(
        source_user_id=current_user.id,
        type=notification_data.type,
        detail=notification_data.detail,
    )
    session.add(notification)
    await session.flush()

    for user in target_users:
        user_notif = UserNotification(
            notification_id=notification.id,
            target_user_id=user.id,
        )
        session.add(user_notif)

    await session.commit()

    await session.refresh(notification)

    result = await session.scalar(
        select(Notification)
        .where(Notification.id == notification.id)
        .options(
            selectinload(Notification.recipients).selectinload(
                UserNotification.target_user
            )
        )
    )
    return result


@router.get('/my', response_model=NotificationList)
async def read_my_notifications(
    session: Session,
    current_user: CurrentUser,
    filters: Annotated[FilterNotification, Depends()],
):
    query = (
        select(UserNotification)
        .where(
            UserNotification.target_user_id == current_user.id,
            UserNotification.deleted_at.is_(None),
        )
        .join(UserNotification.notification)
        .options(
            selectinload(UserNotification.notification).selectinload(
                Notification.source_user
            )
        )
        .offset(filters.offset)
        .limit(filters.limit)
        .order_by(UserNotification.created_at.desc())
    )

    if filters.read is True:
        query = query.where(UserNotification.read_at.is_not(None))
    elif filters.read is False:
        query = query.where(UserNotification.read_at.is_(None))

    if filters.type:
        query = query.where(Notification.type == filters.type)

    db_notifications = await session.scalars(query)
    return {'notifications': db_notifications.all()}


@router.get('/sent', response_model=NotificationSentList)
async def read_sent_notifications(
    session: Session,
    current_user: CurrentUser,
    filters: Annotated[FilterPage, Depends()],
):
    query = (
        select(Notification)
        .where(
            Notification.source_user_id == current_user.id,
            Notification.deleted_at.is_(None),
        )
        .options(
            selectinload(Notification.recipients).selectinload(
                UserNotification.target_user
            )
        )
        .offset(filters.offset)
        .limit(filters.limit)
        .order_by(Notification.created_at.desc())
    )

    db_notifications = await session.scalars(query)
    return {'notifications': db_notifications.all()}


@router.get('/', response_model=NotificationList)
async def read_all_notifications(
    session: Session,
    filters: Annotated[FilterNotification, Depends()],
):
    query = (
        select(UserNotification)
        .where(UserNotification.deleted_at.is_(None))
        .join(UserNotification.notification)
        .options(
            selectinload(UserNotification.notification).selectinload(
                Notification.source_user
            )
        )
        .offset(filters.offset)
        .limit(filters.limit)
        .order_by(UserNotification.created_at.desc())
    )

    if filters.read is True:
        query = query.where(UserNotification.read_at.is_not(None))
    elif filters.read is False:
        query = query.where(UserNotification.read_at.is_(None))

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
    query = (
        select(UserNotification)
        .where(
            UserNotification.id == notification_id,
            UserNotification.deleted_at.is_(None),
        )
        .options(
            selectinload(UserNotification.notification).selectinload(
                Notification.source_user
            )
        )
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

    notification.deleted_at = datetime.now()
    await session.commit()

    return {'message': 'Notification deleted'}

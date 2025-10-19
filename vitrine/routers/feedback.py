from datetime import datetime
from http import HTTPStatus
from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import select

from vitrine.dependencies import CurrentUser, Session
from vitrine.models import Feedback
from vitrine.schemas import (
    FeedbackCreate,
    FeedbackList,
    FeedbackPublic,
    FeedbackUpdate,
    FilterPage,
    Message,
)

router = APIRouter(
    prefix='/feedback',
    tags=['utilidades - feedbacks'],
)


async def get_feedback_or_404(
    session: Session,
    feedback_id: UUID,
) -> Feedback:
    query = select(Feedback).where(
        Feedback.id == feedback_id,
        Feedback.deleted_at.is_(None),
    )
    feedback = await session.scalar(query)
    if not feedback:
        raise HTTPException(
            status_code=HTTPStatus.NOT_FOUND,
            detail='Feedback not found',
        )
    return feedback


@router.post(
    '/',
    status_code=HTTPStatus.CREATED,
    response_model=FeedbackPublic,
)
async def create_feedback(feedback_data: FeedbackCreate, session: Session):
    feedback = Feedback(**feedback_data.model_dump())
    session.add(feedback)
    await session.commit()
    await session.refresh(feedback)
    return feedback


@router.get('/', response_model=FeedbackList)
async def list_feedbacks(
    session: Session,
    filters: Annotated[FilterPage, Depends()],
    current_user: CurrentUser,
):
    query = (
        select(Feedback)
        .where(Feedback.deleted_at.is_(None))
        .offset(filters.offset)
        .limit(filters.limit)
        .order_by(Feedback.created_at.desc())
    )
    db_feedbacks = await session.scalars(query)
    return {'feedbacks': db_feedbacks.all()}


@router.get('/{feedback_id}', response_model=FeedbackPublic)
async def get_feedback(
    feedback_id: UUID, session: Session, current_user: CurrentUser
):
    feedback = await get_feedback_or_404(session, feedback_id)
    return feedback


@router.patch('/{feedback_id}', response_model=FeedbackPublic)
async def update_feedback(
    feedback_id: UUID,
    update_data: FeedbackUpdate,
    session: Session,
    current_user: CurrentUser,
):
    feedback = await get_feedback_or_404(session, feedback_id)
    payload = update_data.model_dump(exclude_unset=True)
    for key, value in payload.items():
        setattr(feedback, key, value)
    await session.commit()
    await session.refresh(feedback)
    return feedback


@router.delete('/{feedback_id}', response_model=Message)
async def delete_feedback(
    feedback_id: UUID, session: Session, current_user: CurrentUser
):
    feedback = await get_feedback_or_404(session, feedback_id)
    feedback.deleted_at = datetime.now()
    await session.commit()
    return {'message': 'Feedback deleted'}

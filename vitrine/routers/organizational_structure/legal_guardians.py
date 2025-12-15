from datetime import datetime
from http import HTTPStatus
from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import func, select

from vitrine.core.dependencies import CurrentUser, Session
from vitrine.models import LegalGuardian, SystemIdentity, User
from vitrine.schemas import (
    FilterLegalGuardian,
    LegalGuardianList,
    LegalGuardianPublic,
    LegalGuardianSchema,
    Message,
)

router = APIRouter(
    prefix='/legal-guardians',
    tags=['estrutura organizacional - responsáveis'],
)


@router.post(
    '/', status_code=HTTPStatus.CREATED, response_model=LegalGuardianPublic
)
async def create_legal_guardian(
    legal_guardian: LegalGuardianSchema,
    session: Session,
    current_user: CurrentUser,
):
    db_legal_guardian = await session.scalar(
        select(LegalGuardian).where(
            (
                LegalGuardian.legal_guardians_name
                == legal_guardian.legal_guardians_name
            )
            | (
                LegalGuardian.legal_guardians_code
                == legal_guardian.legal_guardians_code
            )
        )
    )
    if db_legal_guardian:
        raise HTTPException(
            status_code=HTTPStatus.CONFLICT,
            detail='Legal guardian name or code already exists',
        )

    db_legal_guardian = LegalGuardian(
        legal_guardians_name=legal_guardian.legal_guardians_name,
        legal_guardians_code=legal_guardian.legal_guardians_code,
        user_id=current_user.id,
    )
    session.add(db_legal_guardian)

    query_user = select(User).where(
        User.email == db_legal_guardian.legal_guardians_code
    )
    found_user = await session.scalar(query_user)

    if found_user:
        new_identity = SystemIdentity(
            user=found_user, legal_guardian=db_legal_guardian
        )
        session.add(new_identity)

    await session.commit()
    await session.refresh(db_legal_guardian)

    return db_legal_guardian


@router.get('/', response_model=LegalGuardianList)
async def read_legal_guardians(
    session: Session, filters: Annotated[FilterLegalGuardian, Depends()]
):
    query = select(LegalGuardian).where(LegalGuardian.deleted_at.is_(None))

    if filters.q:
        prefix_query = ' & '.join(word + ':*' for word in filters.q.split())
        ts_query = func.to_tsquery('portuguese', prefix_query)
        query = query.where(LegalGuardian.tsv.op('@@')(ts_query))

    query = query.offset(filters.offset).limit(filters.limit)

    result = await session.scalars(query)
    legal_guardians = result.all()

    return {'legal_guardians': legal_guardians}


@router.delete('/{legal_guardian_id}', response_model=Message)
async def delete_legal_guardian(
    legal_guardian_id: UUID, session: Session, current_user: CurrentUser
):
    """Desativa (soft delete) um responsável legal."""
    db_legal_guardian = await session.get(LegalGuardian, legal_guardian_id)

    if not db_legal_guardian:
        raise HTTPException(
            status_code=HTTPStatus.NOT_FOUND, detail='Legal guardian not found'
        )

    db_legal_guardian.deleted_at = datetime.now()
    await session.commit()

    return {'message': 'Legal guardian deactivated successfully'}

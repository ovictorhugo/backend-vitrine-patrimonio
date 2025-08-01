from datetime import datetime
from http import HTTPStatus
from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from vitrine.database import get_session
from vitrine.models import LegalGuardian, User
from vitrine.schemas import (
    FilterPage,
    LegalGuardianList,
    LegalGuardianPublic,
    LegalGuardianSchema,
    Message,
)
from vitrine.security import get_current_user

router = APIRouter(
    prefix='/legal-guardians',
    tags=['estrutura organizacional - responsáveis'],
)


Session = Annotated[AsyncSession, Depends(get_session)]
CurrentUser = Annotated[User, Depends(get_current_user)]


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
    await session.commit()
    await session.refresh(db_legal_guardian)

    return db_legal_guardian


@router.get('/', response_model=LegalGuardianList)
async def read_legal_guardians(
    session: Session, filter_page: Annotated[FilterPage, Depends()]
):
    """Lista todos os responsáveis legais ativos com paginação."""
    query = await session.scalars(
        select(LegalGuardian)
        .where(LegalGuardian.deleted_at.is_(None))
        .offset(filter_page.offset)
        .limit(filter_page.limit)
    )
    legal_guardians = query.all()

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

from http import HTTPStatus
from secrets import token_hex
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.security import OAuth2PasswordRequestForm
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from starlette.responses import RedirectResponse

from vitrine.database import get_session
from vitrine.models import LegalGuardian, SystemIdentity, User
from vitrine.schemas import Token
from vitrine.security import (
    create_access_token,
    get_current_user,
    get_password_hash,
    verify_password,
)
from vitrine.settings import Settings

router = APIRouter(prefix='/auth', tags=['autenticação'])

OAuth2Form = Annotated[OAuth2PasswordRequestForm, Depends()]
Session = Annotated[AsyncSession, Depends(get_session)]
CurrentUser = Annotated[User, Depends(get_current_user)]


@router.post('/token', response_model=Token)
async def login_for_access_token(form_data: OAuth2Form, session: Session):
    user = await session.scalar(
        select(User).where(User.email == form_data.username)
    )

    if not user:
        raise HTTPException(
            status_code=HTTPStatus.UNAUTHORIZED,
            detail='Incorrect email or password',
        )

    if not verify_password(form_data.password, user.password):
        raise HTTPException(
            status_code=HTTPStatus.UNAUTHORIZED,
            detail='Incorrect email or password',
        )

    access_token = create_access_token(data={'sub': user.email})

    return {'access_token': access_token, 'token_type': 'bearer'}


@router.post('/refresh_token', response_model=Token)
async def refresh_access_token(user: CurrentUser):
    new_access_token = create_access_token(data={'sub': user.email})
    return {'access_token': new_access_token, 'token_type': 'bearer'}


@router.get('/shibboleth/login')
async def shibboleth_login(request: Request, session: Session):
    shib_data = request.headers
    print(shib_data)
    eppn = shib_data.get('eppn')
    if not eppn:
        raise HTTPException(
            status_code=HTTPStatus.UNAUTHORIZED,
            detail=(
                'Atributo de identificação (eppn) não fornecido pelo '
                'Provedor de Identidade. Acesso negado.'
            ),
        )

    db_user = await session.scalar(
        select(User).where(
            (User.username == shib_data.get('shib-person-commonname'))
            | (User.email == shib_data.get('shib-person-mail'))
        )
    )

    if not db_user:
        hashed_password = get_password_hash(token_hex(256))
        db_user = User(
            username=shib_data.get('shib-person-commonname'),
            password=hashed_password,
            email=shib_data.get('shib-person-mail'),
            provider='SHIB',
        )
        session.add(db_user)

        query_lg = select(LegalGuardian).where(
            LegalGuardian.legal_guardians_code == eppn
        )
        found_legal_guardian = await session.scalar(query_lg)

        if found_legal_guardian:
            new_identity = SystemIdentity(
                user=db_user, legal_guardian=found_legal_guardian
            )
            session.add(new_identity)

        await session.commit()
        await session.refresh(db_user)

    access_token = create_access_token(data={'sub': db_user.email})
    url = f'{Settings().CLIENT}/authentication?token={access_token}'
    return RedirectResponse(url, status_code=302)

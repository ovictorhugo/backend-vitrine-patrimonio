from http import HTTPStatus
from secrets import token_hex

from fastapi import APIRouter, HTTPException, Request
from sqlalchemy import func, or_, select
from starlette.responses import RedirectResponse

from vitrine.dependencies import CurrentUser, OAuth2Form, Session
from vitrine.models import LegalGuardian, SystemIdentity, User
from vitrine.schemas import Token
from vitrine.security import (
    create_access_token,
    get_password_hash,
    verify_password,
)
from vitrine.settings import Settings

router = APIRouter(
    prefix='/auth', tags=['autenticação e autorização - autenticação']
)


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

    eppn = shib_data.get('eppn')

    if not eppn:
        raise HTTPException(
            status_code=HTTPStatus.UNAUTHORIZED,
            detail=(
                'Atributo de identificação (eppn) não fornecido pelo '
                'Provedor de Identidade. Acesso negado.'
            ),
        )

    shib_ep_affiliation = shib_data.get('shib-ep-affiliation', str())
    affiliations = {a.strip().lower() for a in shib_ep_affiliation.split(';')}
    if (
        affiliations == {'student'}
        and shib_data.get('shib-person-mail') != 'victorhugodejesus@ufmg.br'
    ):
        raise HTTPException(
            status_code=HTTPStatus.UNAUTHORIZED,
            detail='Perfil cadastrado apenas como discente pelo Provedor de Identidade. Acesso negado.',
        )

    shib_username = shib_data.get('shib-person-commonname')
    shib_email = shib_data.get('shib-person-mail')

    db_user = await session.scalar(
        select(User).where(
            or_(User.username == shib_username, User.email == shib_email)
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
            func.upper(User.username) == shib_username.upper()
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

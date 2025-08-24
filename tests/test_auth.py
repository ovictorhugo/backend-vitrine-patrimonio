from http import HTTPStatus
from urllib.parse import parse_qs, urlparse

import pytest
from freezegun import freeze_time
from jose import jwt
from sqlalchemy import func
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select

from vitrine.models import User
from vitrine.settings import Settings

MOCK_SHIB_HEADERS = {
    'eppn': 'testuser@example.com',
    'shib-person-commonname': 'Test User',
    'shib-person-mail': 'testuser@example.com',
}


@pytest.mark.asyncio
async def test_get_token(client, create_user):
    user = await create_user()
    response = client.post(
        '/auth/token',
        data={'username': user.email, 'password': user.clean_password},
    )
    token = response.json()

    assert response.status_code == HTTPStatus.OK
    assert 'access_token' in token
    assert 'token_type' in token


@pytest.mark.asyncio
async def test_token_expired_after_time(client, create_user):
    user = await create_user()
    with freeze_time('2023-07-14 12:00:00'):
        response = client.post(
            '/auth/token',
            data={'username': user.email, 'password': user.clean_password},
        )
        assert response.status_code == HTTPStatus.OK
        token = response.json()['access_token']

    with freeze_time('2023-07-21 12:31:00'):
        response = client.put(
            f'/users/{user.id}',
            headers={'Authorization': f'Bearer {token}'},
            json={
                'username': 'wrongwrong',
                'email': 'wrong@wrong.com',
                'password': 'wrong',
            },
        )
        assert response.status_code == HTTPStatus.UNAUTHORIZED
        assert response.json() == {'detail': 'Could not validate credentials'}


def test_token_inexistent_user(client):
    response = client.post(
        '/auth/token',
        data={'username': 'no_user@no_domain.com', 'password': 'testtest'},
    )
    assert response.status_code == HTTPStatus.UNAUTHORIZED
    assert response.json() == {'detail': 'Incorrect email or password'}


@pytest.mark.asyncio
async def test_token_wrong_password(client, create_user):
    user = await create_user()
    response = client.post(
        '/auth/token',
        data={'username': user.email, 'password': 'wrong_password'},
    )
    assert response.status_code == HTTPStatus.UNAUTHORIZED
    assert response.json() == {'detail': 'Incorrect email or password'}


@pytest.mark.asyncio
async def test_refresh_token(client, create_user, create_token):
    user = await create_user()
    response = client.post(
        '/auth/refresh_token',
        headers={'Authorization': f'Bearer {create_token(user)}'},
    )

    data = response.json()

    assert response.status_code == HTTPStatus.OK
    assert 'access_token' in data
    assert 'token_type' in data
    assert data['token_type'] == 'bearer'


@pytest.mark.asyncio
async def test_token_expired_dont_refresh(client, create_user):
    user = await create_user()
    with freeze_time('2023-07-14 12:00:00'):
        response = client.post(
            '/auth/token',
            data={'username': user.email, 'password': user.clean_password},
        )
        assert response.status_code == HTTPStatus.OK
        token = response.json()['access_token']

    with freeze_time('2023-07-21 12:31:00'):
        response = client.post(
            '/auth/refresh_token',
            headers={'Authorization': f'Bearer {token}'},
        )
        assert response.status_code == HTTPStatus.UNAUTHORIZED
        assert response.json() == {'detail': 'Could not validate credentials'}


@pytest.mark.asyncio
async def test_shibboleth_login_new_user(client, session: AsyncSession):
    user_query = await session.execute(
        select(User).where(User.email == MOCK_SHIB_HEADERS['shib-person-mail'])
    )
    assert user_query.scalar_one_or_none() is None

    response = client.get(
        '/auth/shibboleth/login',
        headers=MOCK_SHIB_HEADERS,
        follow_redirects=False,
    )

    assert response.status_code == HTTPStatus.FOUND  # 302

    user_query = await session.execute(
        select(User).where(User.email == MOCK_SHIB_HEADERS['shib-person-mail'])
    )
    db_user = user_query.scalar_one_or_none()
    assert db_user is not None
    assert db_user.username == MOCK_SHIB_HEADERS['shib-person-commonname']
    assert db_user.provider == 'SHIB'

    redirect_location = response.headers.get('location')
    assert redirect_location.startswith(Settings().CLIENT)

    parsed_url = urlparse(redirect_location)
    query_params = parse_qs(parsed_url.query)
    assert 'token' in query_params
    token = query_params['token'][0]

    payload = jwt.decode(token, Settings().SECRET_KEY, algorithms=['HS256'])
    assert payload['sub'] == MOCK_SHIB_HEADERS['shib-person-mail']


@pytest.mark.asyncio
async def test_shibboleth_login_existing_user(
    client, session: AsyncSession, create_user
):
    existing_user = await create_user(
        username=MOCK_SHIB_HEADERS['shib-person-commonname'],
        email=MOCK_SHIB_HEADERS['shib-person-mail'],
    )

    user_count_before = await session.scalar(select(func.count(User.id)))

    response = client.get(
        '/auth/shibboleth/login',
        headers=MOCK_SHIB_HEADERS,
        follow_redirects=False,
    )

    user_count_after = await session.scalar(select(func.count(User.id)))

    assert response.status_code == HTTPStatus.FOUND

    assert user_count_after == user_count_before

    redirect_location = response.headers.get('location')
    parsed_url = urlparse(redirect_location)
    query_params = parse_qs(parsed_url.query)
    token = query_params['token'][0]

    payload = jwt.decode(token, Settings().SECRET_KEY, algorithms=['HS256'])
    assert payload['sub'] == existing_user.email


@pytest.mark.asyncio
async def test_shibboleth_login_fails_without_eppn(client):
    headers_without_eppn = {
        'shib-person-commonname': 'Test User',
        'shib-person-mail': 'testuser@example.com',
    }

    response = client.get(
        '/auth/shibboleth/login',
        headers=headers_without_eppn,
        follow_redirects=False,
    )

    assert response.status_code == HTTPStatus.UNAUTHORIZED
    assert response.json() == {
        'detail': (
            'Atributo de identificação (eppn) não fornecido pelo '
            'Provedor de Identidade. Acesso negado.'
        )
    }

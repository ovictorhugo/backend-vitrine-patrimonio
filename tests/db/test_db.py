from dataclasses import asdict
from uuid import UUID

import pytest
from sqlalchemy import select

from vitrine.models import User


@pytest.mark.asyncio
async def test_create_user(session, mock_db_time):
    with mock_db_time(model=User) as time:
        new_user = User(
            username='alice',
            password='secret',
            email='teste@test',
            provider='LOCAL',
        )
        session.add(new_user)
        await session.commit()

    user = await session.scalar(select(User).where(User.username == 'alice'))

    assert asdict(user) == {
        'id': new_user.id,
        'username': 'alice',
        'password': 'secret',
        'email': 'teste@test',
        'background_url': None,
        'institution_id': UUID('27b3839b-d9b3-43c6-824a-aef738ace101'),
        'lattes_id': None,
        'linkedin': None,
        'matricula': None,
        'orcid': None,
        'photo_url': None,
        'provider': 'LOCAL',
        'ramal': None,
        'verify': False,
        'created_at': time,
        'updated_at': time,
        'deleted_at': None,
    }

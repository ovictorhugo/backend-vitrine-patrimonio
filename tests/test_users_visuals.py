import glob
import io
import os
import shutil
from http import HTTPStatus

import pytest

UPLOAD_DIR = 'vitrine/storage/temp'


@pytest.fixture(autouse=True)
def mock_upload_dir(tmp_path, monkeypatch):
    temp_dir = tmp_path / 'vitrine/storage/temp'
    os.makedirs(temp_dir, exist_ok=True)

    from vitrine.routers import users_visuals  # noqa: PLC0415

    monkeypatch.setattr(users_visuals, 'UPLOAD_DIR', str(temp_dir))

    yield str(temp_dir)

    shutil.rmtree(temp_dir, ignore_errors=True)


@pytest.mark.asyncio
async def test_upload_icon_and_get_it(client, create_user, create_token):
    user = await create_user()
    token = create_token(user)

    file_content = b'fake image data'
    response = client.post(
        '/user/upload/icon',
        headers={'Authorization': f'Bearer {token}'},
        files={'file': ('icon.png', io.BytesIO(file_content), 'image/png')},
    )
    assert response.status_code == HTTPStatus.CREATED
    data = response.json()
    assert 'filename' in data

    response = client.get(
        '/user/upload/my/icon',
        headers={'Authorization': f'Bearer {token}'},
    )
    assert response.status_code == HTTPStatus.OK
    assert response.content == file_content

    for path in glob.glob(os.path.join(UPLOAD_DIR, f'icon_{user.id}.*')):
        os.remove(path)


@pytest.mark.asyncio
async def test_delete_icon(client, create_user, create_token):
    user = await create_user()
    token = create_token(user)

    file_content = b'delete test'
    client.post(
        '/user/upload/icon',
        headers={'Authorization': f'Bearer {token}'},
        files={'file': ('icon.png', io.BytesIO(file_content), 'image/png')},
    )

    response = client.delete(
        '/user/upload/icon',
        headers={'Authorization': f'Bearer {token}'},
    )
    assert response.status_code == HTTPStatus.OK
    assert response.json() == {'message': 'Icon successfully deleted.'}


@pytest.mark.asyncio
async def test_delete_icon_when_none_exists(client, create_user, create_token):
    user = await create_user()
    token = create_token(user)

    response = client.delete(
        '/user/upload/icon',
        headers={'Authorization': f'Bearer {token}'},
    )
    assert response.status_code == HTTPStatus.NOT_FOUND
    assert response.json() == {'detail': 'No icon to delete.'}


@pytest.mark.asyncio
async def test_get_icon_from_other_user(client, create_user, create_token):
    user = await create_user()
    other_user = await create_user()

    file_content = b"other user's icon"
    client.post(
        '/user/upload/icon',
        headers={'Authorization': f'Bearer {create_token(other_user)}'},
        files={'file': ('icon.png', io.BytesIO(file_content), 'image/png')},
    )

    response = client.get(
        f'/user/upload/{other_user.id}/icon',
        headers={'Authorization': f'Bearer {create_token(user)}'},
    )
    assert response.status_code == HTTPStatus.OK
    assert response.content == file_content

    for path in glob.glob(os.path.join(UPLOAD_DIR, f'icon_{other_user.id}.*')):
        os.remove(path)


@pytest.mark.asyncio
async def test_get_nonexistent_cover(client, create_user, create_token):
    user = await create_user()
    token = create_token(user)

    response = client.get(
        '/user/upload/my/cover',
        headers={'Authorization': f'Bearer {token}'},
    )
    assert response.status_code == HTTPStatus.NOT_FOUND
    assert response.json() == {'detail': 'Cover not found.'}

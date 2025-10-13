import glob
import os
from http import HTTPStatus
from uuid import UUID

from fastapi import APIRouter, File, HTTPException, UploadFile
from fastapi.responses import FileResponse
from sqlalchemy import select

from vitrine.dependencies import CurrentUser, Session
from vitrine.models import User

router = APIRouter(
    prefix='/user/upload',
    tags=['autenticação e autorização - visuais dos usuários'],
)

UPLOAD_DIR = 'vitrine/storage/uploads/users_visuals'


async def check_user_existence(user_id: str, session: Session):
    stmt = select(User).where(User.id == user_id)
    result = await session.execute(stmt)
    user = result.scalar_one_or_none()
    if not user:
        raise HTTPException(
            status_code=HTTPStatus.NOT_FOUND,
            detail='User not found.',
        )
    return user


async def _get_generic_file_response(entity_id: str, file_type: str):
    friendly_name_map = {'icon': 'Icon', 'cover': 'Cover'}
    friendly_name = friendly_name_map.get(file_type, 'File')

    search_pattern = os.path.join(UPLOAD_DIR, f'{file_type}_{entity_id}.*')
    found_files = glob.glob(search_pattern)

    if not found_files:
        raise HTTPException(
            status_code=HTTPStatus.NOT_FOUND,
            detail=f'{friendly_name} not found.',
        )

    return FileResponse(path=found_files[0])


async def _delete_generic_file(entity_id: str, file_type: str):
    search_pattern = os.path.join(UPLOAD_DIR, f'{file_type}_{entity_id}.*')
    existing_files = glob.glob(search_pattern)

    if not existing_files:
        return False

    for file_path in existing_files:
        try:
            os.remove(file_path)
        except OSError as e:
            print(f'Error deleting old file {file_path}: {e}')

    return True


async def _upload_generic_file(
    entity_id: str, file_type: str, file: UploadFile
):
    os.makedirs(UPLOAD_DIR, exist_ok=True)
    await _delete_generic_file(entity_id, file_type)

    extension = os.path.splitext(file.filename)[1]
    filename = f'{file_type}_{entity_id}{extension}'
    file_path = os.path.join(UPLOAD_DIR, filename)

    try:
        with open(file_path, 'wb') as f:
            content = await file.read()
            f.write(content)
    except IOError as e:
        raise HTTPException(
            status_code=HTTPStatus.INTERNAL_SERVER_ERROR,
            detail=f'Could not save file: {e}',
        )

    return {'filename': filename}


@router.get('/my/icon', response_class=FileResponse)
async def get_my_icon(current_user: CurrentUser):
    return await _get_generic_file_response(str(current_user.id), 'icon')


@router.get('/{user_id}/icon', response_class=FileResponse)
async def get_user_icon_by_id(user_id: UUID, session: Session):
    await check_user_existence(str(user_id), session)
    return await _get_generic_file_response(str(user_id), 'icon')


@router.post('/icon', status_code=HTTPStatus.CREATED)
async def upload_user_icon(
    current_user: CurrentUser, file: UploadFile = File(...)
):
    return await _upload_generic_file(str(current_user.id), 'icon', file)


@router.delete('/icon', status_code=HTTPStatus.OK)
async def delete_user_icon(current_user: CurrentUser):
    if not await _delete_generic_file(str(current_user.id), 'icon'):
        raise HTTPException(
            status_code=HTTPStatus.NOT_FOUND,
            detail='No icon to delete.',
        )
    return {'message': 'Icon successfully deleted.'}


@router.get('/my/cover', response_class=FileResponse)
async def get_my_cover(current_user: CurrentUser):
    return await _get_generic_file_response(str(current_user.id), 'cover')


@router.get('/{user_id}/cover', response_class=FileResponse)
async def get_user_cover_by_id(user_id: UUID, session: Session):
    await check_user_existence(str(user_id), session)
    return await _get_generic_file_response(str(user_id), 'cover')


@router.post('/cover', status_code=HTTPStatus.CREATED)
async def upload_user_cover(
    current_user: CurrentUser, file: UploadFile = File(...)
):
    return await _upload_generic_file(str(current_user.id), 'cover', file)


@router.delete('/cover', status_code=HTTPStatus.OK)
async def delete_user_cover(current_user: CurrentUser):
    if not await _delete_generic_file(str(current_user.id), 'cover'):
        raise HTTPException(
            status_code=HTTPStatus.NOT_FOUND,
            detail='No cover image to delete.',
        )
    return {'message': 'Cover image successfully deleted.'}

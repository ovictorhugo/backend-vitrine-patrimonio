from datetime import datetime
from http import HTTPStatus
from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import select

from vitrine.dependencies import CurrentUser, Session
from vitrine.models import User
from vitrine.schemas import (
    FilterPage,
    Message,
    UserList,
    UserPublic,
    UserSchema,
    UserUpdateSchema,
)
from vitrine.security import (
    get_current_user,
    get_password_hash,
)

router = APIRouter(prefix='/users', tags=['usuários'])


@router.post('/', status_code=HTTPStatus.CREATED, response_model=UserPublic)
async def create_user(
    user: UserSchema,
    session: Session,
):
    db_user = await session.scalar(
        select(User).where(
            (User.username == user.username) | (User.email == user.email)
        )
    )

    if db_user:
        if db_user.username == user.username:
            raise HTTPException(
                status_code=HTTPStatus.CONFLICT,
                detail='Username or Email already exists',
            )
        elif db_user.email == user.email:
            raise HTTPException(
                status_code=HTTPStatus.CONFLICT,
                detail='Email already exists',
            )
    hashed_password = get_password_hash(user.password)

    db_user = User(
        username=user.username,
        password=hashed_password,
        email=user.email,
        provider='LOCAL',
    )
    session.add(db_user)
    await session.commit()
    await session.refresh(db_user)

    return db_user


@router.get('/', response_model=UserList)
async def read_users(
    session: Session, filter_users: Annotated[FilterPage, Query()]
):
    query = await session.scalars(
        select(User)
        .where(User.deleted_at.is_(None))
        .offset(filter_users.offset)
        .limit(filter_users.limit)
    )
    users = query.all()

    return {'users': users}


@router.get('/my-self', include_in_schema=False)
@router.get('/my', response_model=UserPublic)
async def read_me(current_user: CurrentUser):
    return current_user


@router.put('/{user_id}', response_model=UserPublic)
async def update_user(
    user_id: UUID,
    user_update: UserUpdateSchema,
    session: Session,
    current_user: CurrentUser,
):
    if current_user.id != user_id:
        raise HTTPException(
            status_code=HTTPStatus.FORBIDDEN, detail='Not enough permissions'
        )

    update_data = user_update.model_dump(exclude_unset=True)

    if not update_data:
        return current_user

    if 'username' in update_data:
        query_username = select(User).where(
            User.username == update_data['username'],
            User.id != user_id,
        )
        user_exists = await session.scalar(query_username)
        if user_exists:
            raise HTTPException(
                status_code=HTTPStatus.CONFLICT,
                detail='Username already exists',
            )

    if 'email' in update_data:
        query_email = select(User).where(
            User.email == update_data['email'],
            User.id != user_id,
        )
        email_exists = await session.scalar(query_email)
        if email_exists:
            raise HTTPException(
                status_code=HTTPStatus.CONFLICT, detail='Email already exists'
            )

    for field, value in update_data.items():
        setattr(current_user, field, value)

    await session.commit()
    await session.refresh(current_user)

    return current_user


@router.delete('/{user_id}', response_model=Message)
async def delete_user(
    user_id: UUID,
    session: Session,
    current_user: User = Depends(get_current_user),
):
    if current_user.id != user_id:
        raise HTTPException(
            status_code=HTTPStatus.FORBIDDEN, detail='Not enough permissions'
        )

    current_user.deleted_at = datetime.now()
    await session.commit()
    return {'message': 'User deactivated'}

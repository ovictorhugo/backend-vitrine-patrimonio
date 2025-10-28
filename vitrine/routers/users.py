from datetime import datetime
from http import HTTPStatus
from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import Text, cast, desc, func, select
from sqlalchemy.orm import selectinload
from sqlalchemy.orm.attributes import flag_modified

from vitrine.dependencies import CurrentUser, Session
from vitrine.models import (
    CatalogWorkFlow,
    Role,
    SystemIdentity,
    User,
    UserRole,
)
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

router = APIRouter(
    prefix='/users', tags=['autenticação e autorização - usuários']
)


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
        .options(
            selectinload(User.system_identity).selectinload(
                SystemIdentity.legal_guardian
            ),
            selectinload(User.user_role_associations).selectinload(
                UserRole.role
            ),
        )
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


@router.get('/{user_id}', response_model=UserPublic)
async def read_user(
    session: Session,
    user_id: UUID,
):
    options = [
        selectinload(User.system_identity).selectinload(
            SystemIdentity.legal_guardian
        ),
        selectinload(User.user_role_associations).selectinload(UserRole.role),
    ]
    user = await session.get(User, user_id, options=options)
    if not user or user.deleted_at:
        raise HTTPException(
            status_code=HTTPStatus.NOT_FOUND, detail='User not found'
        )
    return user


@router.put('/{user_id}', response_model=UserPublic)
async def update_user(
    user_id: UUID,
    user_update: UserUpdateSchema,
    session: Session,
    current_user: CurrentUser,
):
    db_user = await session.get(User, user_id)
    if not db_user:
        raise HTTPException(
            status_code=HTTPStatus.NOT_FOUND, detail='User not found'
        )

    is_admin = any(role.name == 'Administrador' for role in current_user.roles)

    if current_user.id != db_user.id and not is_admin:
        raise HTTPException(
            status_code=HTTPStatus.FORBIDDEN, detail='Not enough permissions'
        )

    update_data = user_update.model_dump(exclude_unset=True)

    if not update_data:
        return db_user

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
        setattr(db_user, field, value)

    session.add(db_user)
    await session.commit()
    await session.refresh(db_user)

    return db_user


@router.delete('/{user_id}', response_model=Message)
async def delete_user(
    user_id: UUID,
    session: Session,
    current_user: User = Depends(get_current_user),
):
    is_admin = any(role.name == 'Administrador' for role in current_user.roles)

    if current_user.id != user_id and not is_admin:
        raise HTTPException(
            status_code=HTTPStatus.FORBIDDEN, detail='Not enough permissions'
        )

    user_to_delete = await session.get(User, user_id)

    if not user_to_delete:
        raise HTTPException(
            status_code=HTTPStatus.NOT_FOUND, detail='User not found'
        )

    if user_to_delete.deleted_at:
        raise HTTPException(
            status_code=HTTPStatus.BAD_REQUEST,
            detail='User already deactivated',
        )

    user_to_delete.deleted_at = datetime.now()
    session.add(user_to_delete)

    user_id_str = str(user_id)

    workflow_rank_cte = select(
        CatalogWorkFlow.id,
        func.row_number()
        .over(
            partition_by=CatalogWorkFlow.catalog_id,
            order_by=desc(CatalogWorkFlow.created_at),
        )
        .label('rn'),
    ).cte('workflow_rank_cte')

    search_string = f'%"id": "{user_id_str}"%'

    stmt_find_workflows = (
        select(CatalogWorkFlow)
        .join(
            workflow_rank_cte,
            CatalogWorkFlow.id == workflow_rank_cte.c.id,
        )
        .where(
            workflow_rank_cte.c.rn == 1,
            CatalogWorkFlow.workflow_status == 'REVIEW_REQUESTED_COMISSION',
            cast(CatalogWorkFlow.detail['reviewers'], Text).like(
                search_string
            ),
        )
    )

    result_workflows = await session.execute(stmt_find_workflows)
    workflows_to_update = result_workflows.scalars().all()

    for workflow in workflows_to_update:
        current_reviewer_uuids = [
            UUID(reviewer['id']) for reviewer in workflow.detail['reviewers']
        ]

        stmt_find_replacement = (
            select(User)
            .where(
                User.roles.any(
                    Role.name == 'Comissão Permanente de Desfazimento'
                ),
                User.id.notin_(current_reviewer_uuids),
                User.deleted_at.is_(None),
            )
            .order_by(func.random())
            .limit(1)
        )

        result_replacement = await session.execute(stmt_find_replacement)
        new_reviewer_user = result_replacement.scalar_one_or_none()

        new_reviewers_list = [
            reviewer
            for reviewer in workflow.detail['reviewers']
            if reviewer['id'] != user_id_str
        ]

        if new_reviewer_user:
            new_reviewers_list.append({
                'id': str(new_reviewer_user.id),
                'username': new_reviewer_user.username,
            })

        workflow.detail['reviewers'] = new_reviewers_list
        flag_modified(workflow, 'detail')
        session.add(workflow)

    await session.commit()

    return {'message': 'User deactivated and reviewers reassigned'}

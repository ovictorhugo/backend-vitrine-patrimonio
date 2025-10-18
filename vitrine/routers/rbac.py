import datetime
from http import HTTPStatus
from typing import Annotated, List
from uuid import UUID

from fastapi import APIRouter, HTTPException, Query
from sqlalchemy import select
from sqlalchemy.orm import selectinload

from vitrine.dependencies import Session
from vitrine.models import (
    Permission,
    Role,
    RolePermission,
    SystemIdentity,
    User,
    UserRole,
)
from vitrine.schemas import (
    FilterPage,
    Message,
    PermissionPublic,
    PermissionSchema,
    RoleList,
    RolePublic,
    RoleSchema,
    UserList,
)

router = APIRouter(
    prefix='/roles', tags=['autenticação e autorização - cargos e permissões']
)


@router.post('/', status_code=HTTPStatus.CREATED, response_model=RolePublic)
async def create_role(role: RoleSchema, session: Session):
    existing = await session.scalar(select(Role).where(Role.name == role.name))
    if existing:
        raise HTTPException(
            status_code=HTTPStatus.CONFLICT, detail='Role already exists'
        )
    db_role = Role(name=role.name, description=role.description)
    session.add(db_role)
    await session.commit()
    await session.refresh(db_role)
    return db_role


@router.get('/', response_model=RoleList)
async def read_roles(
    session: Session, filter_roles: Annotated[FilterPage, Query()]
):
    query = await session.scalars(
        select(Role)
        .options(
            selectinload(Role.role_permissions).selectinload(
                RolePermission.permission
            )
        )
        .where(Role.deleted_at.is_(None))
        .offset(filter_roles.offset)
        .limit(filter_roles.limit)
    )
    roles = query.all()
    return RoleList(roles=roles)


@router.post(
    '/permissions',
    status_code=HTTPStatus.CREATED,
    response_model=PermissionPublic,
)
async def create_permission(permission: PermissionSchema, session: Session):
    existing = await session.scalar(
        select(Permission).where(
            (Permission.name == permission.name)
            | (Permission.code == permission.code)
        )
    )
    if existing:
        raise HTTPException(
            status_code=HTTPStatus.CONFLICT, detail='Permission already exists'
        )

    db_permission = Permission(
        name=permission.name,
        code=permission.code,
        description=permission.description,
    )
    session.add(db_permission)
    await session.commit()
    await session.refresh(db_permission)
    return db_permission


@router.get('/permissions', response_model=List[PermissionPublic])
async def read_permissions(session: Session):
    query = await session.scalars(
        select(Permission).where(Permission.deleted_at.is_(None))
    )
    return query.all()


@router.put('/{role_id}', response_model=RolePublic)
async def update_role(session: Session, role_id: UUID, role: RoleSchema):
    role_db = await session.get(Role, role_id)
    if not role_db or role_db.deleted_at:
        raise HTTPException(
            status_code=HTTPStatus.NOT_FOUND,
            detail='Role not found',
        )
    role_db.name = role.name
    role_db.description = role.description
    await session.commit()
    await session.refresh(role_db)
    return role_db


@router.delete('/{role_id}', response_model=Message)
async def delete_role(session: Session, role_id: UUID):
    role_db = await session.get(Role, role_id)
    if not role_db or role_db.deleted_at:
        raise HTTPException(
            status_code=HTTPStatus.NOT_FOUND,
            detail='Role not found',
        )
    role_db.deleted_at = datetime.datetime.now()
    await session.commit()
    return {'message': 'Role deactivated'}


@router.delete('/permissions/{permission_id}')
async def delete_permission(permission_id: UUID, session: Session):
    db_permission = await session.get(Permission, permission_id)

    if not db_permission or db_permission.deleted_at:
        raise HTTPException(
            status_code=HTTPStatus.NOT_FOUND,
            detail='Permission deactivated',
        )

    db_permission.deleted_at = datetime.datetime.now()
    await session.commit()

    return {'message': 'Permission deactivated'}


@router.get('/{role_id}/users', response_model=UserList)
async def get_users_from_roles(
    role_id: UUID, filters: Annotated[FilterPage, Query()], session: Session
):
    query = (
        select(User)
        .join(UserRole, User.id == UserRole.user_id)
        .where(
            UserRole.role_id == role_id,
            User.deleted_at.is_(None),
            UserRole.deleted_at.is_(None),
        )
        .options(
            selectinload(User.system_identity).selectinload(
                SystemIdentity.legal_guardian
            ),
            selectinload(User.user_role_associations).selectinload(
                UserRole.role
            ),
        )
        .offset(filters.offset)
        .limit(filters.limit)
    )

    result = await session.scalars(query)
    users = result.all()

    return {'users': users}


@router.post('/{role_id}/permissions', response_model=Message)
async def add_permission_to_role(
    role_id: UUID, permission_id: UUID, session: Session
):
    role = await session.get(Role, role_id)
    permission = await session.get(Permission, permission_id)

    if not role or not permission:
        raise HTTPException(
            status_code=HTTPStatus.NOT_FOUND,
            detail='Role or Permission not found',
        )

    existing = await session.scalar(
        select(RolePermission).where(
            RolePermission.role_id == role_id,
            RolePermission.permission_id == permission_id,
        )
    )
    if existing:
        raise HTTPException(
            status_code=HTTPStatus.CONFLICT,
            detail='Permission already assigned to role',
        )

    session.add(RolePermission(role_id=role_id, permission_id=permission_id))
    await session.commit()
    return {'message': 'Permission added to role'}


@router.delete(
    '/{role_id}/permissions/{permission_id}', response_model=Message
)
async def remove_permission_from_role(
    role_id: UUID, permission_id: UUID, session: Session
):
    assoc = await session.scalar(
        select(RolePermission).where(
            RolePermission.role_id == role_id,
            RolePermission.permission_id == permission_id,
        )
    )
    if not assoc:
        raise HTTPException(
            status_code=HTTPStatus.NOT_FOUND,
            detail='Permission not assigned to role',
        )

    await session.delete(assoc)
    await session.commit()
    return {'message': 'Permission removed from role'}


@router.post('/{role_id}/users/{user_id}', response_model=Message)
async def assign_role_to_user(role_id: UUID, user_id: UUID, session: Session):
    role = await session.get(Role, role_id)
    user = await session.get(User, user_id)
    if not role or not user:
        raise HTTPException(
            status_code=HTTPStatus.NOT_FOUND, detail='User or Role not found'
        )

    existing = await session.scalar(
        select(UserRole).where(
            UserRole.user_id == user_id,
            UserRole.role_id == role_id,
        )
    )
    if existing:
        raise HTTPException(
            status_code=HTTPStatus.CONFLICT,
            detail='Role already assigned to user',
        )

    session.add(UserRole(user_id=user_id, role_id=role_id))
    await session.commit()
    return {'message': 'Role assigned to user'}


@router.delete('/{role_id}/users/{user_id}', response_model=Message)
async def remove_role_from_user(
    role_id: UUID, user_id: UUID, session: Session
):
    assoc = await session.scalar(
        select(UserRole).where(
            UserRole.user_id == user_id,
            UserRole.role_id == role_id,
        )
    )
    if not assoc:
        raise HTTPException(
            status_code=HTTPStatus.NOT_FOUND,
            detail='Role not assigned to user',
        )

    await session.delete(assoc)
    await session.commit()
    return {'message': 'Role removed from user'}

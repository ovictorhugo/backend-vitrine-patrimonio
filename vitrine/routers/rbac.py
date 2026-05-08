from datetime import datetime
from http import HTTPStatus
from typing import Annotated, List
from uuid import UUID
from pydantic import BaseModel, ConfigDict

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import Text, cast, func, select
from sqlalchemy.orm import selectinload, noload
from sqlalchemy.orm.attributes import flag_modified
from sqlalchemy.ext.asyncio import AsyncSession

from vitrine.core.database import get_session
from vitrine.core.dependencies import Session
from vitrine.models import (
    CatalogWorkFlow,
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
    RoleFilter,
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
    session: Session,
    filters: RoleFilter = Depends(),
):
    query = (
        select(Role)
        .options(
            selectinload(Role.role_permissions).selectinload(
                RolePermission.permission
            )
        )
        .where(Role.deleted_at.is_(None))
    )

    if filters.q:
        prefix_query = ' & '.join(word + ':*' for word in filters.q.split())

        ts_query = func.to_tsquery('portuguese', prefix_query)

        query = query.where(Role.tsv.op('@@')(ts_query))

    query = query.offset(filters.offset).limit(filters.limit)

    result = await session.scalars(query)
    roles = result.all()
    return {'roles': roles}


@router.post(
    '/permissions',
    status_code=HTTPStatus.CREATED,
    response_model=PermissionPublic,
)
async def create_permission(permission: PermissionSchema, session: Session):
    existing = await session.scalar(
        select(Permission).where(
            (Permission.name == permission.name)
            | (Permission.code == permission.code),
            Permission.deleted_at.is_(None),
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


@router.get('/{role_id}/permissions', response_model=List[PermissionPublic])
async def read_permissions_by_role(
    role_id: UUID,
    session: AsyncSession = Depends(get_session)
):
    query = (
        select(Permission)
        # 1. Partimos da tabela RolePermission
        .select_from(RolePermission)
        # 2. Fazemos o join com Permission
        .join(RolePermission.permission)
        .where(
            RolePermission.role_id == role_id,
            RolePermission.deleted_at.is_(None),
            Permission.deleted_at.is_(None)
        )
        .options(
            # 3. Barramos as queries automáticas do lazy='selectin'
            noload(Permission.roles),
            noload(Permission.role_permissions)
        )
    )
    
    result = await session.scalars(query)
    return result.all()


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
    role_db.deleted_at = datetime.now()
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

    db_permission.deleted_at = datetime.now()
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

    role = await session.get(Role, role_id)

    if role and role.name == 'Comissão Permanente de Desfazimento':
        user_id_str = str(user_id)
        search_string = f'%"id": "{user_id_str}"%'
        stmt_find_workflows = select(CatalogWorkFlow).where(
            CatalogWorkFlow.workflow_status == 'REVIEW_REQUESTED_COMISSION',
            cast(CatalogWorkFlow.detail['reviewers'], Text).like(
                search_string
            ),
        )

        result_workflows = await session.execute(stmt_find_workflows)
        workflows_to_update = result_workflows.scalars().all()

        for workflow in workflows_to_update:
            current_reviewer_uuids = [
                UUID(reviewer['id'])
                for reviewer in workflow.detail['reviewers']
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

        if workflows_to_update:
            await session.commit()

    return {
        'message': 'Role removed from user and reviewers reassigned if applicable'
    }


class RoleWithCount(BaseModel):
    id: UUID
    name: str
    user_count: int
    model_config = ConfigDict(from_attributes=True)

# Modelo da lista de resposta
class RoleStatisticsList(BaseModel):
    roles: List[RoleWithCount]


@router.get('/statistics', response_model=RoleStatisticsList)
async def get_roles_statistics(
    session: Session,
):
    query = (
        select(Role, func.count(User.id).label('total_users'))
        # 1. Junta Role com a tabela de associação (UserRole)
        .outerjoin(UserRole, Role.id == UserRole.role_id)
        # 2. Junta com a tabela User, MAS apenas se o usuário não estiver deletado
        .outerjoin(User, (UserRole.user_id == User.id) & (User.deleted_at.is_(None)))
        # 3. Garante que o Role também não esteja deletado
        .where(Role.deleted_at.is_(None))
        # 4. Agrupa pelo ID do Role para contar
        .group_by(Role.id)
    )

    result = await session.execute(query)
    # O resultado vem como uma lista de tuplas: (Role_Instance, int_count)
    
    # Montamos a resposta mesclando os dados do objeto Role com a contagem
    response_data = []
    for role, count in result:
        # Pydantic consegue ler atributos do objeto SQLAlchemy
        # Criamos um dict temporário ou usamos o construtor do Pydantic
        role_data = {
            "id": role.id,
            "name": role.name,
            "description": role.description, # Ajuste conforme seus campos reais
            "user_count": count
        }
        response_data.append(role_data)

    return {'roles': response_data}
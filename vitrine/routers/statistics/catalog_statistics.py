from typing import Annotated

from fastapi import APIRouter, Depends
from pydantic import BaseModel
from sqlalchemy import and_, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from vitrine.database import get_session
from vitrine.models import (
    CatalogWorkFlow,
    User,
)
from vitrine.security import get_current_user

router = APIRouter(prefix='/statistics', tags=['estatisticas - patrimonio'])

Session = Annotated[AsyncSession, Depends(get_session)]
CurrentUser = Annotated[User, Depends(get_current_user)]


class CatalogWorkflowCount(BaseModel):
    status: str
    count: int


@router.get(
    '/catalog/count-by-workflow-status',
    response_model=list[CatalogWorkflowCount],
)
async def get_catalog_count_by_workflow_status(session: Session):
    latest_workflow_subquery = (
        select(
            CatalogWorkFlow.catalog_id,
            func.max(CatalogWorkFlow.created_at).label('max_created_at'),
        )
        .group_by(CatalogWorkFlow.catalog_id)
        .subquery('latest_workflow_sq')
    )

    query = (
        select(
            CatalogWorkFlow.workflow_status,
            func.count(CatalogWorkFlow.catalog_id).label('status_count'),
        )
        .join(
            latest_workflow_subquery,
            and_(
                CatalogWorkFlow.catalog_id
                == latest_workflow_subquery.c.catalog_id,
                CatalogWorkFlow.created_at
                == latest_workflow_subquery.c.max_created_at,
            ),
        )
        .group_by(CatalogWorkFlow.workflow_status)
    )

    result = await session.execute(query)

    counts = [
        {'status': status, 'count': count} for status, count in result.all()
    ]
    return counts

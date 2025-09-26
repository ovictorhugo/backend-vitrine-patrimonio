from typing import Annotated

from fastapi import Depends
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql import Select

from vitrine.database import get_session
from vitrine.models import (
    Catalog,
    CatalogWorkFlow,
)
from vitrine.schemas import (
    FilterCatalog,
)

Session = Annotated[AsyncSession, Depends(get_session)]


def apply_filters(query: Select, filters: FilterCatalog) -> Select:
    if filters.user_id:
        query = query.where(Catalog.user_id == filters.user_id)

    if filters.workflow_status:
        latest_workflow_sq = select(
            CatalogWorkFlow.catalog_id,
            CatalogWorkFlow.workflow_status,
            func.row_number()
            .over(
                partition_by=CatalogWorkFlow.catalog_id,
                order_by=CatalogWorkFlow.created_at.desc(),
            )
            .label('rn'),
        ).subquery('latest_workflow_sq')

        query = query.join(
            latest_workflow_sq,
            Catalog.id == latest_workflow_sq.c.catalog_id,
        )

        query = query.where(
            latest_workflow_sq.c.rn == 1,
            latest_workflow_sq.c.workflow_status == filters.workflow_status,
        )

    return query

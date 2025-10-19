from fastapi import APIRouter
from pydantic import BaseModel
from sqlalchemy import and_, case, func, literal_column, select
from sqlalchemy.sql import union_all

from vitrine.dependencies import Session
from vitrine.models import (
    Catalog,
    CatalogWorkFlow,
    CollectionItem,
)

router = APIRouter(prefix='/statistics', tags=['estatisticas - patrimonio'])


class CollectionStatusCount(BaseModel):
    status: str
    count: int


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


@router.get(
    '/catalog/count-by-collection-status',
    response_model=list[CollectionStatusCount],
)
async def get_catalog_count_by_collection_status(session: Session):
    query_true_false = select(
        case(
            (CollectionItem.status == True, 'TRUE'),
            (CollectionItem.status == False, 'FALSE'),
        ).label('status'),
        func.count(CollectionItem.id).label('count'),
    ).group_by(CollectionItem.status)

    query_not_in_collection = (
        select(
            literal_column("'NOT_IN_COLLECTION'").label('status'),
            func.count(Catalog.id).label('count'),
        )
        .outerjoin(CollectionItem, Catalog.id == CollectionItem.catalog_id)
        .where(CollectionItem.id == None)
    )

    final_query = union_all(query_true_false, query_not_in_collection)

    result = await session.execute(final_query)

    counts = [
        {'status': status, 'count': count} for status, count in result.all()
    ]
    return counts

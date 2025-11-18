from uuid import UUID

from fastapi import APIRouter, Depends
from pydantic import BaseModel
from sqlalchemy import text

from vitrine.core.dependencies import Session
from vitrine.schemas import CatalogStatisticsFilters
from vitrine.services.filter_service import build_catalog_filters

router = APIRouter(prefix='/statistics', tags=['estatisticas - patrimonio'])


class ReviewerWorkflowStats(BaseModel):
    reviewer_id: UUID | None
    reviewer: str
    total: int
    d0: int
    d3: int
    w1: int


class CollectionStatusCount(BaseModel):
    status: str
    count: int


@router.get(
    '/catalog/count-by-workflow-status',
    response_model=list[CollectionStatusCount],
)
async def get_catalog_count_by_workflow_status(
    session: Session,
    filters: CatalogStatisticsFilters = Depends(),
):
    join_clauses, filter_clauses, params = build_catalog_filters(filters)

    SQL = f"""
        WITH wc_status AS (
            SELECT DISTINCT ON (catalog_id)
                catalog_workflow.workflow_status, catalog_id
            FROM catalog_workflow
            INNER JOIN catalog
                ON catalog.id = catalog_workflow.catalog_id
                AND catalog.deleted_at IS NULL
            ORDER BY catalog_workflow.catalog_id,
                catalog_workflow.created_at DESC
        )
        SELECT workflow_status AS status, COUNT(*) AS count
        FROM wc_status
            LEFT JOIN catalog ON catalog.id = wc_status.catalog_id
            {join_clauses}
        WHERE 1 = 1
            {filter_clauses}
        GROUP BY workflow_status
    """
    result = await session.execute(text(SQL), params)
    return result.mappings().all()


@router.get(
    '/catalog/count-by-collection-status',
    response_model=list[CollectionStatusCount],
)
async def get_catalog_count_by_collection_status(
    session: Session,
    workflow_status: str,
    filters: CatalogStatisticsFilters = Depends(),
):
    join_clauses, filter_clauses, params = build_catalog_filters(filters)

    params['workflow_status'] = workflow_status

    SQL = f"""
        WITH wc_status AS (
            SELECT DISTINCT ON (catalog_id)
                catalog_workflow.workflow_status, catalog_id
            FROM catalog_workflow
            INNER JOIN catalog
                ON catalog.id = catalog_workflow.catalog_id
                AND catalog.deleted_at IS NULL
            ORDER BY catalog_workflow.catalog_id,
                catalog_workflow.created_at DESC
        )
        SELECT
            COUNT(*),
            CASE
                WHEN collection_items.status IS NULL THEN 'NOT_IN_COLLECTION'
                ELSE UPPER(CAST(collection_items.status AS TEXT))
            END AS status
        FROM wc_status
            INNER JOIN catalog ON catalog.id = wc_status.catalog_id
            LEFT JOIN collection_items ON catalog.id = collection_items.catalog_id
            {join_clauses}
        WHERE
            wc_status.workflow_status = :workflow_status
            {filter_clauses}
        GROUP BY status;
    """

    result = await session.execute(text(SQL), params)
    return result.mappings().all()


@router.get(
    '/catalog/stats/review-commission',
    response_model=list[ReviewerWorkflowStats],
)
async def get_catalog_review_commission_stats(
    session: Session,
    filters: CatalogStatisticsFilters = Depends(),
):
    join_clauses, filter_clauses, params = build_catalog_filters(filters)

    SQL = f"""
        SELECT
            rid AS reviewer_id, reviewer,
            COUNT(*) AS total,
            COUNT(*) FILTER (WHERE DATE(cw.created_at) = CURRENT_DATE) AS d0,
            COUNT(*) FILTER (WHERE cw.created_at >= CURRENT_DATE - INTERVAL '3 days') AS d3,
            COUNT(*) FILTER (WHERE cw.created_at < CURRENT_DATE - INTERVAL '7 days') AS w1
        FROM (
            SELECT DISTINCT ON (catalog_workflow.catalog_id)
                catalog_workflow.detail -> 'reviewers' -> 0 ->> 'id' AS rid,
                catalog_workflow.detail -> 'reviewers' -> 0 ->> 'username' AS reviewer,
                catalog_workflow.created_at
            FROM catalog_workflow
            INNER JOIN catalog
                ON catalog.id = catalog_workflow.catalog_id
                AND catalog.deleted_at IS NULL
            {join_clauses}
            WHERE
                catalog_workflow.workflow_status = 'REVIEW_REQUESTED_COMISSION'
                AND catalog_workflow.detail -> 'reviewers' -> 0 ->> 'id' IS NOT NULL
                AND catalog_workflow.detail -> 'reviewers' -> 0 ->> 'username' IS NOT NULL
            {filter_clauses}
            ORDER BY
                catalog_workflow.catalog_id,
                catalog_workflow.created_at DESC
        ) cw
        GROUP BY rid, reviewer;
    """

    result = await session.execute(text(SQL), params)
    return result.mappings().all()

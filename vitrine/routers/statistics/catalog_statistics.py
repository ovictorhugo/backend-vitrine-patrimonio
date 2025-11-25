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


class WorkflowStatusGrouped(BaseModel):
    id: UUID | None
    name: str | None
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
                catalog_workflow.workflow_status, catalog_id, detail
            FROM catalog_workflow
            INNER JOIN catalog
                ON catalog.id = catalog_workflow.catalog_id
                AND catalog.deleted_at IS NULL
            ORDER BY catalog_workflow.catalog_id,
                catalog_workflow.created_at DESC
        )
        SELECT workflow_status AS status, COUNT(*) AS count
        FROM wc_status AS ws
            LEFT JOIN catalog ON catalog.id = ws.catalog_id
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
        FROM catalog
            LEFT JOIN wc_status ON catalog.id = wc_status.catalog_id
            LEFT JOIN collection_items ON wc_status.catalog_id = collection_items.catalog_id
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
        WITH wc_status AS (
            SELECT DISTINCT ON (catalog_workflow.catalog_id)
                catalog_workflow.catalog_id,
                catalog_workflow.created_at,
                catalog_workflow.detail,
                catalog_workflow.workflow_status
            FROM catalog_workflow
            INNER JOIN catalog
                ON catalog.id = catalog_workflow.catalog_id
                AND catalog.deleted_at IS NULL
            ORDER BY catalog_workflow.catalog_id,
                    catalog_workflow.created_at DESC
        )
        SELECT
            reviewer_data ->> 'id' AS reviewer_id,
            reviewer_data ->> 'username' AS reviewer,
            COUNT(*) AS total,
            COUNT(*) FILTER (WHERE DATE(ws.created_at) = CURRENT_DATE) AS d0,
            COUNT(*) FILTER (WHERE ws.created_at >= CURRENT_DATE - INTERVAL '3 days') AS d3,
            COUNT(*) FILTER (WHERE ws.created_at < CURRENT_DATE - INTERVAL '7 days') AS w1
        FROM wc_status ws
        INNER JOIN catalog ON catalog.id = ws.catalog_id
        CROSS JOIN LATERAL jsonb_array_elements(ws.detail -> 'reviewers') AS reviewer_data
        {join_clauses}
        WHERE
            1 = 1
            {filter_clauses}
        GROUP BY
            reviewer_data ->> 'id',
            reviewer_data ->> 'username';
    """
    print(SQL)
    result = await session.execute(text(SQL), params)
    return result.mappings().all()


def get_hierarchy_joins(level: str, current_joins_str: str) -> str:
    required_joins = []

    j_assets = 'LEFT JOIN assets ON assets.id = catalog.asset_id'
    j_locations = 'LEFT JOIN locations ON assets.location_id = locations.id'
    j_sectors = 'LEFT JOIN sectors ON locations.sector_id = sectors.id'
    j_agencys = 'LEFT JOIN agencys ON sectors.agency_id = agencys.id'
    j_units = 'LEFT JOIN units ON agencys.unit_id = units.id'

    if level in ['location', 'sector', 'agency', 'unit']:
        if 'JOIN assets' not in current_joins_str:
            required_joins.append(j_assets)
        if 'JOIN locations' not in current_joins_str:
            required_joins.append(j_locations)

    if level in ['sector', 'agency', 'unit']:
        if 'JOIN sectors' not in current_joins_str:
            required_joins.append(j_sectors)

    if level in ['agency', 'unit']:
        if 'JOIN agencys' not in current_joins_str:
            required_joins.append(j_agencys)

    if level == 'unit':
        if 'JOIN units' not in current_joins_str:
            required_joins.append(j_units)

    return '\n'.join(required_joins)


async def _get_grouped_stats(
    session: Session,
    filters: CatalogStatisticsFilters,
    id_column: str,
    name_column: str,
    level_key: str,
):
    filter_joins, filter_clauses, params = build_catalog_filters(filters)

    mandatory_joins = get_hierarchy_joins(level_key, filter_joins)

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
            {id_column} AS id,
            {name_column} AS name,
            wc_status.workflow_status AS status, 
            COUNT(*) AS count
        FROM wc_status
            LEFT JOIN catalog ON catalog.id = wc_status.catalog_id
            {filter_joins}
            {mandatory_joins} -- Adiciona os joins de estrutura se faltarem
        WHERE 1 = 1
            {filter_clauses}
        GROUP BY 
            {id_column}, 
            {name_column}, 
            wc_status.workflow_status
        ORDER BY
            {name_column},
            wc_status.workflow_status
    """

    result = await session.execute(text(SQL), params)
    return result.mappings().all()


@router.get(
    '/catalog/workflow-status-grouped/unit',
    response_model=list[WorkflowStatusGrouped],
)
async def get_catalog_workflow_status_by_unit(
    session: Session,
    filters: CatalogStatisticsFilters = Depends(),
):
    return await _get_grouped_stats(
        session,
        filters,
        id_column='units.id',
        name_column='units.unit_name',
        level_key='unit',
    )


@router.get(
    '/catalog/workflow-status-grouped/agency',
    response_model=list[WorkflowStatusGrouped],
)
async def get_catalog_workflow_status_by_agency(
    session: Session,
    filters: CatalogStatisticsFilters = Depends(),
):
    return await _get_grouped_stats(
        session,
        filters,
        id_column='agencys.id',
        name_column='agencys.agency_name',
        level_key='agency',
    )


@router.get(
    '/catalog/workflow-status-grouped/sector',
    response_model=list[WorkflowStatusGrouped],
)
async def get_catalog_workflow_status_by_sector(
    session: Session,
    filters: CatalogStatisticsFilters = Depends(),
):
    return await _get_grouped_stats(
        session,
        filters,
        id_column='sectors.id',
        name_column='sectors.sector_name',
        level_key='sector',
    )


@router.get(
    '/catalog/workflow-status-grouped/location',
    response_model=list[WorkflowStatusGrouped],
)
async def get_catalog_workflow_status_by_location(
    session: Session,
    filters: CatalogStatisticsFilters = Depends(),
):
    return await _get_grouped_stats(
        session,
        filters,
        id_column='locations.id',
        name_column='locations.location_name',
        level_key='location',
    )

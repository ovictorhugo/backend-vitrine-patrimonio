from fastapi import APIRouter, Depends
from pydantic import BaseModel
from sqlalchemy import case, func, literal_column, select
from sqlalchemy.orm import aliased
from sqlalchemy.sql import union_all

from vitrine.core.dependencies import Session
from vitrine.models import (
    Agency,
    Asset,
    Catalog,
    CatalogWorkFlow,
    CollectionItem,
    Location,
    Sector,
)
from vitrine.schemas import CatalogStatisticsFilters

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
async def get_catalog_count_by_workflow_status(  # noqa: PLR0912, PLR0914, PLR0915
    session: Session,
    filters: CatalogStatisticsFilters = Depends(),
):
    cw = aliased(CatalogWorkFlow)
    c = aliased(Catalog)
    a = aliased(Asset)

    loc = aliased(Location)
    s = aliased(Sector)
    ag = aliased(Agency)

    subquery_select = (
        select(
            cw.catalog_id,
            cw.workflow_status,
        )
        .join(c, c.id == cw.catalog_id)
        .join(a, a.id == c.asset_id)
        .where(c.deleted_at.is_(None))
    )

    joined_location = False
    joined_sector = False
    joined_agency = False

    if filters.q:
        prefix_query = ' & '.join(word + ':*' for word in filters.q.split())
        ts_query = func.to_tsquery('portuguese', prefix_query)
        subquery_select = subquery_select.where(a.tsv.op('@@')(ts_query))

    if filters.asset_identifier:
        subquery_select = subquery_select.where(
            func.concat(a.asset_code, a.asset_check_digit)
            == filters.asset_identifier.replace('-', '')
        )

    if filters.atm_number:
        subquery_select = subquery_select.where(
            a.atm_number == filters.atm_number
        )

    if filters.material_id:
        subquery_select = subquery_select.where(
            a.material_id == filters.material_id
        )

    if filters.location_id:
        subquery_select = subquery_select.where(
            a.location_id == filters.location_id
        )

    if filters.legal_guardian_id:
        subquery_select = subquery_select.where(
            a.legal_guardian_id == filters.legal_guardian_id
        )

    if filters.is_official is not None:
        subquery_select = subquery_select.where(
            a.is_official == filters.is_official
        )

    if filters.asset_status:
        subquery_select = subquery_select.where(
            a.asset_status == filters.asset_status
        )

    if filters.csv_code:
        subquery_select = subquery_select.where(a.csv_code == filters.csv_code)

    if filters.unit_id:
        if not joined_location:
            subquery_select = subquery_select.join(
                loc, a.location_id == loc.id
            )
            joined_location = True
        if not joined_sector:
            subquery_select = subquery_select.join(s, loc.sector_id == s.id)
            joined_sector = True
        if not joined_agency:
            subquery_select = subquery_select.join(ag, s.agency_id == ag.id)
            joined_agency = True
        subquery_select = subquery_select.where(ag.unit_id == filters.unit_id)

    if filters.agency_id:
        if not joined_location:
            subquery_select = subquery_select.join(
                loc, a.location_id == loc.id
            )
            joined_location = True
        if not joined_sector:
            subquery_select = subquery_select.join(s, loc.sector_id == s.id)
            joined_sector = True
        subquery_select = subquery_select.where(
            s.agency_id == filters.agency_id
        )

    if filters.sector_id:
        if not joined_location:
            subquery_select = subquery_select.join(
                loc, a.location_id == loc.id
            )
            joined_location = True
        subquery_select = subquery_select.where(
            loc.sector_id == filters.sector_id
        )

    subquery = (
        subquery_select.distinct(cw.catalog_id)
        .order_by(cw.catalog_id, cw.created_at.desc())
        .subquery()
    )

    query = select(
        subquery.c.workflow_status,
        func.count(subquery.c.catalog_id).label('count'),
    ).group_by(subquery.c.workflow_status)

    result = await session.execute(query)

    counts = [
        {'status': status, 'count': count} for status, count in result.all()
    ]
    return counts


@router.get(
    '/catalog/count-by-collection-status',
    response_model=list[CollectionStatusCount],
)
async def get_catalog_count_by_collection_status(
    session: Session,
    workflow_status: str,
):
    latest_workflow_subquery = (
        select(
            CatalogWorkFlow.catalog_id,
            func.max(CatalogWorkFlow.created_at).label('max_created_at'),
        )
        .group_by(CatalogWorkFlow.catalog_id)
        .subquery()
    )

    query_true_false = (
        select(
            case(
                (CollectionItem.status.is_(True), literal_column("'TRUE'")),
                (CollectionItem.status.is_(False), literal_column("'FALSE'")),
            ).label('status'),
            func.count(CollectionItem.id).label('count'),
        )
        .join(Catalog, CollectionItem.catalog_id == Catalog.id)
        .join(
            latest_workflow_subquery,
            Catalog.id == latest_workflow_subquery.c.catalog_id,
        )
        .join(
            CatalogWorkFlow,
            (Catalog.id == CatalogWorkFlow.catalog_id)
            & (
                CatalogWorkFlow.created_at
                == latest_workflow_subquery.c.max_created_at
            ),
        )
        .where(CatalogWorkFlow.workflow_status == workflow_status)
        .group_by(CollectionItem.status)
    )

    query_not_in_collection = (
        select(
            literal_column("'NOT_IN_COLLECTION'").label('status'),
            func.count(Catalog.id).label('count'),
        )
        .outerjoin(CollectionItem, Catalog.id == CollectionItem.catalog_id)
        .join(
            latest_workflow_subquery,
            Catalog.id == latest_workflow_subquery.c.catalog_id,
        )
        .join(
            CatalogWorkFlow,
            (Catalog.id == CatalogWorkFlow.catalog_id)
            & (
                CatalogWorkFlow.created_at
                == latest_workflow_subquery.c.max_created_at
            ),
        )
        .where(
            CollectionItem.id.is_(None),
            CatalogWorkFlow.workflow_status == workflow_status,
        )
    )

    final_query = union_all(query_true_false, query_not_in_collection)

    result = await session.execute(final_query)

    counts = [
        {'status': status, 'count': count} for status, count in result.all()
    ]
    return counts

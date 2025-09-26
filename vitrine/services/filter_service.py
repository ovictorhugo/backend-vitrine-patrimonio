from typing import Annotated

from fastapi import Depends
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql import Select

from vitrine.database import get_session
from vitrine.models import (
    Agency,
    Asset,
    Catalog,
    CatalogWorkFlow,
    Location,
    Sector,
)
from vitrine.schemas import (
    FilterCatalog,
)

Session = Annotated[AsyncSession, Depends(get_session)]


def apply_catalog_filters(query: Select, filters: FilterCatalog) -> Select:
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


def apply_asset_filters(query, filters):
    query = query.where(Asset.deleted_at.is_(None))

    if filters.q:
        prefix_query = ' & '.join(word + ':*' for word in filters.q.split())
        ts_query = func.to_tsquery('portuguese', prefix_query)
        query = query.where(Asset.tsv.op('@@')(ts_query))

    if filters.asset_identifier:
        query = query.where(
            func.concat(Asset.asset_code, Asset.asset_check_digit)
            == filters.asset_identifier.replace('-', '')
        )

    if filters.atm_number:
        query = query.where(Asset.atm_number == filters.atm_number)

    if filters.material_id:
        query = query.where(Asset.material_id == filters.material_id)

    if filters.unit_id:
        query = (
            query.join(Asset.location)
            .join(Location.sector)
            .join(Sector.agency)
            .where(Agency.unit_id == filters.unit_id)
        )

    if filters.agency_id:
        query = (
            query.join(Asset.location)
            .join(Location.sector)
            .where(Sector.agency_id == filters.agency_id)
        )

    if filters.sector_id:
        query = query.join(Asset.location).where(
            Location.sector_id == filters.sector_id
        )

    if filters.location_id:
        query = query.where(Asset.location_id == filters.location_id)

    if filters.legal_guardian_id:
        query = query.where(
            Asset.legal_guardian_id == filters.legal_guardian_id
        )

    if filters.is_official is not None:
        query = query.where(Asset.is_official == filters.is_official)

    if filters.user_id:
        query = query.where(Asset.user_id == filters.user_id)

    query = query.offset(filters.offset).limit(filters.limit)

    return query

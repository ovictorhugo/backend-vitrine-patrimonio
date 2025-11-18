from typing import Annotated

from fastapi import Depends
from sqlalchemy import Text, and_, cast, func, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql import Select

from vitrine.core.database import get_session
from vitrine.models import (
    Agency,
    Asset,
    Catalog,
    CatalogWorkFlow,
    Collection,
    CollectionItem,
    Location,
    Sector,
)
from vitrine.schemas import (
    FilterCatalog,
)

Session = Annotated[AsyncSession, Depends(get_session)]


def build_catalog_filters(filters):
    joins = {}
    filter_clauses = []
    params = {}

    def ensure_assets_join():
        if 'assets' not in joins:
            joins['assets'] = (
                'LEFT JOIN assets ON assets.id = catalog.asset_id'
            )

    def ensure_locations_join():
        ensure_assets_join()
        if 'locations' not in joins:
            joins['locations'] = (
                'LEFT JOIN locations ON assets.location_id = locations.id'
            )

    def ensure_sectors_join():
        ensure_locations_join()
        if 'sectors' not in joins:
            joins['sectors'] = (
                'LEFT JOIN sectors ON locations.sector_id = sectors.id'
            )

    def ensure_agencys_join():
        ensure_sectors_join()
        if 'agencys' not in joins:
            joins['agencys'] = (
                'LEFT JOIN agencys ON sectors.agency_id = agencys.id'
            )

    def ensure_units_join():
        ensure_agencys_join()
        if 'units' not in joins:
            joins['units'] = 'LEFT JOIN units ON agencys.unit_id = units.id'

    if filters.role_id:
        if 'user_roles' not in joins:
            joins['user_roles'] = (
                'INNER JOIN user_roles ON user_roles.user_id = catalog.user_id '
                'AND user_roles.role_id = :role_id'
            )
        params['role_id'] = filters.role_id

    if filters.user_id:
        filter_clauses.append('catalog.user_id = :user_id')
        params['user_id'] = filters.user_id

    if filters.material_id:
        ensure_assets_join()
        filter_clauses.append('assets.material_id = :material_id')
        params['material_id'] = filters.material_id

    if filters.unit_id:
        ensure_units_join()
        filter_clauses.append('units.id = :unit_id')
        params['unit_id'] = filters.unit_id

    if filters.agency_id:
        ensure_agencys_join()
        filter_clauses.append('agencys.id = :agency_id')
        params['agency_id'] = filters.agency_id

    if filters.sector_id:
        ensure_sectors_join()
        filter_clauses.append('sectors.id = :sector_id')
        params['sector_id'] = filters.sector_id

    if filters.location_id:
        ensure_locations_join()
        filter_clauses.append('locations.id = :location_id')
        params['location_id'] = filters.location_id

    if filters.asset_status:
        ensure_assets_join()
        filter_clauses.append('assets.asset_status = :asset_status')
        params['asset_status'] = filters.asset_status

    if filters.legal_guardian_id:
        ensure_assets_join()
        filter_clauses.append('assets.legal_guardian_id = :legal_guardian_id')
        params['legal_guardian_id'] = filters.legal_guardian_id

    if filters.is_official is not None:
        ensure_assets_join()
        filter_clauses.append('assets.is_official = :is_official')
        params['is_official'] = filters.is_official

    if filters.csv_code:
        ensure_assets_join()
        filter_clauses.append('assets.csv_code ILIKE :csv_code')
        params['csv_code'] = f'%{filters.csv_code}%'

    final_joins = '\n'.join(joins.values())

    final_filters = ''
    if filter_clauses:
        final_filters = ' AND ' + ' AND '.join(filter_clauses)

    return final_joins, final_filters, params


def apply_catalog_filters(query: Select, filters: FilterCatalog) -> Select:
    if filters.only_uncollected:
        query = query.outerjoin(
            CollectionItem,
            and_(
                CollectionItem.catalog_id == Catalog.id,
                CollectionItem.collection.has(Collection.deleted_at.is_(None)),
            ),
        ).where(CollectionItem.id.is_(None))

    needs_latest_workflow = filters.reviewer_id or filters.workflow_status

    if needs_latest_workflow:
        latest_workflow_subquery = (
            select(
                CatalogWorkFlow.catalog_id,
                func.max(CatalogWorkFlow.created_at).label('max_created_at'),
            )
            .group_by(CatalogWorkFlow.catalog_id)
            .subquery()
        )

        query = query.join(
            latest_workflow_subquery,
            Catalog.id == latest_workflow_subquery.c.catalog_id,
        ).join(
            CatalogWorkFlow,
            (Catalog.id == CatalogWorkFlow.catalog_id)
            & (
                CatalogWorkFlow.created_at
                == latest_workflow_subquery.c.max_created_at
            ),
        )

        if filters.reviewer_id:
            search_string = f'%"id": "{str(filters.reviewer_id)}"%'
            query = query.where(
                cast(CatalogWorkFlow.detail['reviewers'], Text).like(
                    search_string
                )
            )

        if filters.workflow_status:
            query = query.where(
                CatalogWorkFlow.workflow_status == filters.workflow_status
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

    if filters.asset_status:
        query = query.where(Asset.asset_status == filters.asset_status)

    if filters.csv_code:
        query = query.where(Asset.csv_code == filters.csv_code)

    return query

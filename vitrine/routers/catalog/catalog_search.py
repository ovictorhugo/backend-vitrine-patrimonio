from typing import Annotated

from fastapi import APIRouter, Depends
from sqlalchemy import func, select

from vitrine.core.dependencies import Session
from vitrine.models import (
    Asset,
    Catalog,
    CatalogWorkFlow,
    LegalGuardian,
    Material,
)
from vitrine.schemas import (
    CatalogAssetIdentifierList,
    FilterSearchCatalog,
    LegalGuardianList,
    MaterialList,
)

router = APIRouter(prefix='/catalog', tags=['Vitrine - Busca em Anúncios'])


@router.get('/search/materials', response_model=MaterialList)
async def list_catalog_materials(
    session: Session, filters: Annotated[FilterSearchCatalog, Depends()]
):
    query = (
        select(Material)
        .join_from(Catalog, Asset)
        .join(Material)
        .where(Catalog.deleted_at.is_(None))
    )

    if filters.q:
        query = query.where(Material.material_name.ilike(f'{filters.q}%'))

    if filters.user_id:
        query = query.where(Catalog.user_id == filters.user_id)

    if filters.workflow_status:
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
        )
        query = query.join(
            CatalogWorkFlow,
            (Catalog.id == CatalogWorkFlow.catalog_id)
            & (
                CatalogWorkFlow.created_at
                == latest_workflow_subquery.c.max_created_at
            ),
        )

        query = query.where(
            CatalogWorkFlow.workflow_status == filters.workflow_status
        )

    query = query.distinct()

    query = query.offset(filters.offset).limit(filters.limit)
    result = await session.scalars(query)
    materials = result.unique().all()

    return {'materials': materials}


@router.get(
    '/search/legal_guardians',
    response_model=LegalGuardianList,
)
async def list_catalog_legal_guardians(
    session: Session,
    filters: Annotated[FilterSearchCatalog, Depends()],
):
    query = (
        select(LegalGuardian)
        .join_from(Catalog, Asset)
        .join(LegalGuardian)
        .where(Catalog.deleted_at.is_(None))
    )

    if filters.q:
        query = query.where(
            LegalGuardian.legal_guardians_name.ilike(f'{filters.q}%')
        )

    if filters.user_id:
        query = query.where(Catalog.user_id == filters.user_id)

    if filters.workflow_status:
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
        )
        query = query.join(
            CatalogWorkFlow,
            (Catalog.id == CatalogWorkFlow.catalog_id)
            & (
                CatalogWorkFlow.created_at
                == latest_workflow_subquery.c.max_created_at
            ),
        )

        query = query.where(
            CatalogWorkFlow.workflow_status == filters.workflow_status
        )

    query = query.distinct()

    query = query.offset(filters.offset).limit(filters.limit)
    result = await session.scalars(query)
    legal_guardians = result.unique().all()
    return {'legal_guardians': legal_guardians}


@router.get(
    '/search/asset-identifier',
    response_model=CatalogAssetIdentifierList,
)
async def search_catalog_by_asset_identifier(session: Session, q: str = str()):
    q = q.replace('-', '')
    query = (
        select(
            Catalog.id.label('catalog_id'),
            func.concat(Asset.asset_code, '-', Asset.asset_check_digit).label(
                'asset_identifier'
            ),
        )
        .join(Asset, Catalog.asset_id == Asset.id)
        .where(
            Catalog.deleted_at.is_(None),
            Asset.deleted_at.is_(None),
            func.concat(Asset.asset_code, Asset.asset_check_digit).ilike(
                f'{q}%'
            ),
        )
    )
    result = await session.execute(query)
    return {'catalogs': result.mappings().all()}

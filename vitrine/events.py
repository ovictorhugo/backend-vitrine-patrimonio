import datetime
from datetime import timedelta

from sqlalchemy import func, select
from sqlalchemy.exc import NoResultFound

from vitrine.core.dependencies import Session
from vitrine.models import (
    Catalog,
    CatalogWorkFlow,
    SystemSetting,
)


async def check_and_update_stale_workflows(session: Session):
    setting_key = 'WORKFLOW_MAX_AGE_DAYS'

    try:
        stmt = select(SystemSetting.value).where(
            SystemSetting.key == setting_key
        )
        max_age_days = await session.scalar(stmt)

        max_age_days_int = (
            int(max_age_days) if max_age_days is not None else 30
        )
    except (NoResultFound, TypeError, ValueError):
        max_age_days_int = 30

    now = datetime.datetime.now(datetime.UTC)
    cutoff_date = now - timedelta(days=max_age_days_int)

    subquery = (
        select(
            CatalogWorkFlow.catalog_id,
            func.max(CatalogWorkFlow.created_at).label('last_created_at'),
        )
        .group_by(CatalogWorkFlow.catalog_id)
        .subquery()
    )

    query = (
        select(CatalogWorkFlow)
        .join(
            subquery,
            (CatalogWorkFlow.catalog_id == subquery.c.catalog_id)
            & (CatalogWorkFlow.created_at == subquery.c.last_created_at),
        )
        .join(Catalog, Catalog.id == CatalogWorkFlow.catalog_id)
        .where(Catalog.deleted_at.is_(None))
        .where(CatalogWorkFlow.created_at < cutoff_date)
    )

    result = await session.scalars(query)
    stale_workflows = result.all()

    updated_count = 0

    for wf in stale_workflows:
        new_workflow = CatalogWorkFlow(
            catalog_id=wf.catalog_id,
            user_id='ca4e6a0b-3d3e-4982-8775-1ef47938e3f5',
            workflow_status='ALIENACAO',
            detail={'previous_workflow_id': str(wf.id)},
        )
        session.add(new_workflow)
        updated_count += 1

    if updated_count > 0:
        await session.commit()

    return {'message': f'{updated_count} workflows updated to ALIENACAO'}

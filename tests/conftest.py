import smtplib
from contextlib import contextmanager
from datetime import datetime

import pytest
import pytest_asyncio
from faker import Faker
from fastapi.testclient import TestClient
from sqlalchemy import event, select
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from testcontainers.mailpit import MailpitContainer
from testcontainers.postgres import PostgresContainer

from tests.factories import (
    AgencyFactory,
    AssetFactory,
    CatalogFactory,
    CatalogWorkFlowFactory,
    FavoriteCatalogFactory,
    InventoryFactory,
    LegalGuardiansFactory,
    LocationFactory,
    MaterialFactory,
    SectorFactory,
    UnitFactory,
    UserFactory,
    WorkflowTransferFactory,
)
from vitrine.app import app
from vitrine.database import get_session
from vitrine.mail import get_smtp
from vitrine.models import (
    CatalogWorkFlow,
    InventoryOwner,
    SystemIdentity,
    User,
    WorkflowTransferStatus,
    table_registry,
)
from vitrine.security import get_password_hash

fake = Faker('pt_BR')


@pytest.fixture
def client(session, mailpit):
    def get_session_override():
        return session

    def get_smtp_override():
        smtp_connection = smtplib.SMTP(mailpit['host'], mailpit['smtp_port'])
        try:
            yield smtp_connection
        finally:
            smtp_connection.quit()

    with TestClient(app) as client:
        app.dependency_overrides[get_session] = get_session_override
        app.dependency_overrides[get_smtp] = get_smtp_override
        yield client

    app.dependency_overrides.clear()


@pytest.fixture(scope='session')
def mailpit():
    with MailpitContainer(image='axllent/mailpit:v1.21') as mailpit:
        host = mailpit.get_container_host_ip()
        smtp_port = mailpit.get_exposed_smtp_port()
        ui_port = mailpit.get_exposed_ui_port()
        yield {'host': host, 'smtp_port': smtp_port, 'ui_port': ui_port}


@pytest.fixture(scope='session')
def engine():
    with PostgresContainer('postgres:16', driver='psycopg') as postgres:
        _engine = create_async_engine(postgres.get_connection_url())
        yield _engine


@pytest_asyncio.fixture
async def session(engine):
    async with engine.begin() as conn:
        await conn.run_sync(table_registry.metadata.create_all)

    async with AsyncSession(engine, expire_on_commit=False) as session:
        yield session

    async with engine.begin() as conn:
        await conn.run_sync(table_registry.metadata.drop_all)


@contextmanager
def _mock_db_time(*, model, time=datetime(2024, 1, 1)):
    def fake_insert_time_hook(mapper, connection, target):
        if hasattr(target, 'created_at'):
            target.created_at = time
        if hasattr(target, 'updated_at'):
            target.updated_at = time

    def fake_update_time_hook(mapper, connection, target):
        if hasattr(target, 'updated_at'):
            target.updated_at = time

    def fake_delete_time_hook(mapper, connection, target):
        if hasattr(target, 'deleted_at'):
            target.deleted_at = time

    event.listen(model, 'before_insert', fake_insert_time_hook)
    event.listen(model, 'before_update', fake_update_time_hook)
    event.listen(model, 'before_update', fake_delete_time_hook)

    yield time

    event.remove(model, 'before_insert', fake_insert_time_hook)
    event.remove(model, 'before_update', fake_update_time_hook)


@pytest.fixture
def mock_db_time():
    return _mock_db_time


@pytest_asyncio.fixture
def create_agency(session, create_user, create_unit):
    async def _create_agency(**kwargs):
        if 'user_id' not in kwargs:
            user = await create_user()
            kwargs['user_id'] = user.id

        if 'unit_id' not in kwargs:
            unit = await create_unit()
            kwargs['unit_id'] = unit.id

        agency = AgencyFactory.build(**kwargs)
        session.add(agency)
        await session.commit()
        await session.refresh(agency)
        return agency

    return _create_agency


@pytest_asyncio.fixture
def create_unit(session, create_user):
    async def _create_unit(**kwargs):
        if 'user_id' not in kwargs:
            user = await create_user()
            kwargs['user_id'] = user.id

        unit = UnitFactory.build(**kwargs)

        session.add(unit)
        await session.commit()
        await session.refresh(unit)
        return unit

    return _create_unit


@pytest_asyncio.fixture
def create_sector(session, create_agency, create_user):
    async def _create_sector(**kwargs):
        if 'user_id' not in kwargs:
            user = await create_user()
            kwargs['user_id'] = user.id

        if 'agency_id' not in kwargs:
            agency = await create_agency(user_id=kwargs['user_id'])
            kwargs['agency_id'] = agency.id

        sector = SectorFactory.build(**kwargs)

        session.add(sector)
        await session.commit()
        await session.refresh(sector)

        return sector

    return _create_sector


@pytest_asyncio.fixture
def create_location(
    session, create_sector, create_user, create_legal_guardian
):
    async def _create_location(**kwargs):
        if 'user_id' not in kwargs:
            user = await create_user()
            kwargs['user_id'] = user.id

        if 'sector_id' not in kwargs:
            legal_guardian = await create_sector(user_id=kwargs['user_id'])
            kwargs['sector_id'] = legal_guardian.id

        if 'legal_guardian_id' not in kwargs:
            legal_guardian = await create_legal_guardian(
                user_id=kwargs['user_id']
            )
            kwargs['legal_guardian_id'] = legal_guardian.id

        location = LocationFactory.build(**kwargs)

        session.add(location)
        await session.commit()
        await session.refresh(location)

        await session.refresh(location, ['sector'])
        return location

    return _create_location


@pytest_asyncio.fixture
def create_legal_guardian(session, create_user):
    async def _create_legal_guardian(**kwargs):
        if 'user_id' not in kwargs:
            user = await create_user()
            kwargs['user_id'] = user.id

        legal_guardian = LegalGuardiansFactory.build(**kwargs)

        session.add(legal_guardian)
        await session.commit()
        await session.refresh(legal_guardian)

        return legal_guardian

    return _create_legal_guardian


@pytest_asyncio.fixture
def create_material(session, create_user):
    async def _create_material(**kwargs):
        if 'user_id' not in kwargs:
            user = await create_user()
            kwargs['user_id'] = user.id

        material = MaterialFactory.build(**kwargs)

        session.add(material)
        await session.commit()
        await session.refresh(material)

        return material

    return _create_material


@pytest_asyncio.fixture
def create_user(session):
    async def _create_user(**kwargs):
        factory_user = UserFactory.build(**kwargs)

        raw_password = kwargs.get('password', 'testtest')
        hashed_password = get_password_hash(raw_password)

        user = User(
            username=factory_user.username,
            email=factory_user.email,
            password=hashed_password,
            provider=factory_user.provider,
        )

        session.add(user)
        await session.commit()
        await session.refresh(user)

        user.clean_password = raw_password

        return user

    return _create_user


@pytest_asyncio.fixture
def create_asset(
    session,
    create_agency,
    create_unit,
    create_sector,
    create_location,
    create_material,
    create_legal_guardian,
):
    async def _create_asset(**kwargs):
        if 'location_id' not in kwargs:
            location = await create_location()
            kwargs['location_id'] = location.id
        if 'material_id' not in kwargs:
            material = await create_material()
            kwargs['material_id'] = material.id
        if 'legal_guardian_id' not in kwargs:
            legal_guardian = await create_legal_guardian()
            kwargs['legal_guardian_id'] = legal_guardian.id

        asset = AssetFactory.build(**kwargs)

        session.add(asset)
        await session.commit()
        await session.refresh(asset)

        return asset

    return _create_asset


@pytest.fixture
def create_token(client):
    def _create_token(user):
        response = client.post(
            '/auth/token',
            data={'username': user.email, 'password': user.clean_password},
        )
        return response.json()['access_token']

    return _create_token


@pytest_asyncio.fixture
async def access_header(create_token, create_user):
    header = {'Authorization': f'Bearer {create_token(await create_user())}'}
    return header


@pytest_asyncio.fixture
def create_catalog_entry(session, create_asset, create_user, create_location):
    async def _create_catalog_entry(**kwargs):
        if 'user_id' not in kwargs:
            user = await create_user()
            kwargs['user_id'] = user.id

        if 'asset_id' not in kwargs:
            asset = await create_asset()
            kwargs['asset_id'] = asset.id

        if 'location_id' not in kwargs:
            location = await create_location()
            kwargs['location_id'] = location.id

        catalog = CatalogFactory.build(**kwargs)

        session.add(catalog)
        await session.commit()
        await session.refresh(catalog)

        return catalog

    return _create_catalog_entry


@pytest_asyncio.fixture
def create_workflow_step(session, create_catalog_entry, create_user):
    async def _create_workflow_step(**kwargs):
        if 'user_id' not in kwargs:
            user = await create_user()
            kwargs['user_id'] = user.id

        if 'catalog_id' not in kwargs:
            catalog_entry = await create_catalog_entry()
            kwargs['catalog_id'] = catalog_entry.id

        factory_workflow = CatalogWorkFlowFactory.build(**kwargs)

        workflow_step = CatalogWorkFlow(
            catalog_id=factory_workflow.catalog_id,
            user_id=factory_workflow.user_id,
            workflow_status=factory_workflow.workflow_status,
            detail=factory_workflow.detail,
        )

        session.add(workflow_step)
        await session.commit()
        await session.refresh(workflow_step)

        return workflow_step

    return _create_workflow_step


@pytest_asyncio.fixture
def create_inventory(session, create_user):
    async def _create(**kwargs):
        if 'created_by_id' not in kwargs:
            user = await create_user()
            kwargs['created_by_id'] = user.id

        query = select(User).where(User.deleted_at.is_(None))
        users_db = await session.scalars(query)
        users_db = users_db.all()

        inventory_db = InventoryFactory(**kwargs)

        session.add(inventory_db)
        await session.flush()

        owners = []
        for user in users_db:
            i = InventoryOwner(inventory_id=inventory_db.id, user_id=user.id)
            owners.append(i)

        session.add(inventory_db)
        session.add_all(owners)

        await session.commit()
        await session.refresh(inventory_db)
        return inventory_db

    return _create


@pytest_asyncio.fixture
def create_favorite_catalog(session, create_user, create_catalog_entry):
    async def _create_favorite_catalog(**kwargs):
        if 'user_id' not in kwargs:
            user = await create_user()
            kwargs['user_id'] = user.id

        if 'catalog_id' not in kwargs:
            catalog = await create_catalog_entry()
            kwargs['catalog_id'] = catalog.id

        favorite = FavoriteCatalogFactory.build(**kwargs)

        session.add(favorite)
        await session.commit()
        await session.refresh(favorite)

        return favorite

    return _create_favorite_catalog


@pytest.fixture
def create_system_identity(session: AsyncSession):
    async def _create_system_identity(
        user_id,
        legal_guardian_id,
    ):
        system_identity = SystemIdentity(
            user_id=user_id,
            legal_guardian_id=legal_guardian_id,
        )
        session.add(system_identity)
        await session.commit()
        await session.refresh(system_identity)
        return system_identity

    return _create_system_identity


@pytest_asyncio.fixture
def create_workflow_transfer(
    session, create_workflow_step, create_user, create_location
):
    async def _create_workflow_transfer(**kwargs):
        if 'workflow_id' not in kwargs:
            workflow_step = await create_workflow_step(
                workflow_status=WorkflowTransferStatus.PENDING
            )
            kwargs['workflow_id'] = workflow_step.id

        if 'user_id' not in kwargs:
            user = await create_user()
            kwargs['user_id'] = user.id

        if 'location_id' not in kwargs:
            location = await create_location()
            kwargs['location_id'] = location.id

        transfer = WorkflowTransferFactory.build(**kwargs)

        session.add(transfer)
        await session.commit()
        await session.refresh(transfer)

        return transfer

    return _create_workflow_transfer

import os
import re
from typing import Annotated
from uuid import UUID, uuid4

import polars as pl
from fastapi import Depends, UploadFile
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from vitrine.database import get_session
from vitrine.models import (
    Agency,
    Asset,
    LegalGuardian,
    Location,
    Material,
    Sector,
    Unit,
)
from vitrine.schemas import (
    AgencySchema,
    AssetSchema,
    LegalGuardianSchema,
    LocationSchema,
    MaterialSchema,
    SectorSchema,
    UnitSchema,
)

Session = Annotated[AsyncSession, Depends(get_session)]


def normalize_dataframe(dataframe: pl.DataFrame):
    dataframe = dataframe.with_columns([
        dataframe[col.name].cast(pl.Utf8).alias(col.name)
        for col in dataframe.get_columns()
    ])
    dataframe = dataframe.with_columns([
        dataframe[col.name].str.strip_chars().alias(col.name)
        for col in dataframe.get_columns()
    ])
    return dataframe


def file_to_list(file: UploadFile):
    ext = os.path.splitext(file.filename)[-1].lower()
    filename = f'{uuid4().hex}{ext}'
    filepath = os.path.join('vitrine', 'storage', filename)

    with open(filepath, 'wb') as buffer:
        buffer.write(file.file.read())

    if ext == '.csv':
        dataframe = pl.read_csv(filepath)
    dataframe = pl.read_excel(filepath)
    dataframe = normalize_dataframe(dataframe)
    os.remove(filepath)
    return dataframe.to_dicts()


def align_assets(assets: list[dict]):
    db_assets = list()
    for dict_asset in assets:
        assets_schema = AssetSchema(**dict_asset)
        db_assets.append(Asset(**assets_schema.model_dump()))
    return db_assets


def clean_string(name: str) -> str:
    return re.sub(r'[^\w\s]', '*', name)


async def get_or_create_material(
    session: Session, material_data: dict, user_id
):
    material = MaterialSchema(**material_data)
    material.material_name = clean_string(material.material_name)
    db_material = await session.scalar(
        select(Material).where(
            (Material.material_name == material.material_name)
        )
    )
    if not db_material:
        db_material = Material(
            material_name=material.material_name,
            material_code=material.material_code,
            user_id=user_id,
        )
        session.add(db_material)
        await session.flush()
    return db_material


async def get_or_create_agency(session: Session, data: dict, user_id):
    agency = AgencySchema(**data)
    agency.agency_name = clean_string(agency.agency_name)

    db_agency = await session.scalar(
        select(Agency).where((Agency.agency_name == agency.agency_name))
    )

    if not db_agency:
        db_agency = Agency(
            agency_name=agency.agency_name,
            agency_code=agency.agency_code,
            user_id=user_id,
        )
        session.add(db_agency)
        await session.flush()

    return db_agency


async def get_or_create_unit(
    session: Session, data: dict, user_id, agency_id: UUID
) -> Unit:
    unit = UnitSchema(**data)
    unit.unit_name = clean_string(unit.unit_name)

    db_unit = await session.scalar(
        select(Unit).where(
            (Unit.unit_name == unit.unit_name), (Unit.agency_id == agency_id)
        )
    )

    if not db_unit:
        db_unit = Unit(
            unit_name=unit.unit_name,
            unit_code=unit.unit_code,
            unit_siaf=unit.unit_siaf,
            user_id=user_id,
            agency_id=agency_id,
        )
        session.add(db_unit)
        await session.flush()

    return db_unit


async def get_or_create_sector(
    session: Session, data: dict, user_id, unit_id: UUID
) -> Sector:
    sector = SectorSchema(**data)
    sector.sector_name = clean_string(sector.sector_name)

    db_sector = await session.scalar(
        select(Sector).where(
            (Sector.sector_name == sector.sector_name),
            (Sector.unit_id == unit_id),
        )
    )

    if not db_sector:
        db_sector = Sector(
            sector_name=sector.sector_name,
            sector_code=sector.sector_code,
            user_id=user_id,
            unit_id=unit_id,
        )
        session.add(db_sector)
        await session.flush()

    return db_sector


async def get_or_create_location(
    session: Session, data: dict, user_id, sector_id: UUID
) -> Location:
    location = LocationSchema(**data)
    location.location_name = clean_string(location.location_name)

    db_location = await session.scalar(
        select(Location).where(
            (Location.location_name == location.location_name),
            (Location.sector_id == sector_id),
        )
    )

    if not db_location:
        db_location = Location(
            location_name=location.location_name,
            location_code=location.location_code,
            user_id=user_id,
            sector_id=sector_id,
        )
        session.add(db_location)
        await session.flush()

    return db_location


async def get_or_create_legal_guardian(
    session: Session, data: dict, user_id
) -> LegalGuardian:
    guardian = LegalGuardianSchema(**data)
    guardian.legal_guardians_name = clean_string(guardian.legal_guardians_name)

    db_guardian = await session.scalar(
        select(LegalGuardian).where(
            (
                LegalGuardian.legal_guardians_name
                == guardian.legal_guardians_name
            )
        )
    )

    if not db_guardian:
        db_guardian = LegalGuardian(
            legal_guardians_name=guardian.legal_guardians_name,
            legal_guardians_code=guardian.legal_guardians_code,
            user_id=user_id,
        )
        session.add(db_guardian)
        await session.flush()

    return db_guardian


async def find_relationships(assets: list[dict], session: Session, user_id):
    for asset in assets:
        db_material = await get_or_create_material(session, asset, user_id)
        asset['material_id'] = db_material.id

        db_guardian = await get_or_create_legal_guardian(
            session, asset, user_id
        )
        asset['legal_guardian_id'] = db_guardian.id

        db_agency = await get_or_create_agency(session, asset, user_id)
        asset['agency_id'] = db_agency.id

        db_unit = await get_or_create_unit(
            session, asset, user_id, agency_id=db_agency.id
        )
        asset['unit_id'] = db_unit.id

        db_sector = await get_or_create_sector(
            session, asset, user_id, unit_id=db_unit.id
        )
        asset['sector_id'] = db_sector.id

        db_location = await get_or_create_location(
            session, asset, user_id, sector_id=db_sector.id
        )
        asset['location_id'] = db_location.id

    return assets

import uuid
from datetime import datetime
from enum import Enum as PythonEnum
from uuid import UUID, uuid4

from sqlalchemy import (
    JSON,
    Computed,
    ForeignKey,
    Index,
    UniqueConstraint,
    func,
)
from sqlalchemy.dialects.postgresql import TSVECTOR
from sqlalchemy.orm import Mapped, mapped_column, registry, relationship

table_registry = registry()


class AssetSituation(str, PythonEnum):
    UNUSED = 'UNUSED'
    RECOVERABLE = 'RECOVERABLE'
    UNECONOMICAL = 'UNECONOMICAL'
    BROKEN = 'BROKEN'


class WorkFlowStatus(str, PythonEnum):
    STARTED = 'STARTED'
    REVIEW_REQUESTED_VITRINE = 'REVIEW_REQUESTED_VITRINE'
    REVIEW_REQUESTED_DESFAZIMENTO = 'REVIEW_REQUESTED_DESFAZIMENTO'
    ADJUSTMENT_REQUESTED = 'ADJUSTMENT_REQUESTED'
    COMPLETED = 'COMPLETED'


class InventoryStatus(str, PythonEnum):
    GOOD = 'GOOD'
    NOT_FOUND = 'NOT_FOUND'
    IRRECOVERABLE = 'IRRECOVERABLE'
    UNECONOMICAL = 'UNECONOMICAL'


@table_registry.mapped_as_dataclass
class User:
    __tablename__ = 'users'

    id: Mapped[UUID] = mapped_column(
        init=False, primary_key=True, default=uuid4
    )

    username: Mapped[str] = mapped_column(unique=True)
    password: Mapped[str]
    email: Mapped[str] = mapped_column(unique=True)
    provider: Mapped[str | None] = mapped_column(nullable=True)

    linkedin: Mapped[str | None] = mapped_column(nullable=True, init=False)
    lattes_id: Mapped[str | None] = mapped_column(nullable=True, init=False)
    orcid: Mapped[str | None] = mapped_column(nullable=True, init=False)
    ramal: Mapped[str | None] = mapped_column(nullable=True, init=False)
    photo_url: Mapped[str | None] = mapped_column(nullable=True, init=False)
    background_url: Mapped[str | None] = mapped_column(
        nullable=True, init=False
    )
    matricula: Mapped[str | None] = mapped_column(nullable=True, init=False)

    verify: Mapped[bool] = mapped_column(default=False)
    institution_id: Mapped[UUID] = mapped_column(
        default=UUID('27b3839b-d9b3-43c6-824a-aef738ace101'), init=False
    )

    created_at: Mapped[datetime] = mapped_column(
        init=False, server_default=func.now()
    )
    updated_at: Mapped[datetime | None] = mapped_column(
        init=False, nullable=True, onupdate=func.now()
    )
    deleted_at: Mapped[datetime | None] = mapped_column(
        init=False, nullable=True
    )


@table_registry.mapped_as_dataclass
class Unit:
    __tablename__ = 'units'
    id: Mapped[UUID] = mapped_column(
        init=False, primary_key=True, default=uuid.uuid4
    )
    unit_name: Mapped[str] = mapped_column(nullable=False)
    unit_code: Mapped[str] = mapped_column(nullable=False)
    unit_siaf: Mapped[str] = mapped_column(nullable=False)

    agencies: Mapped[list['Agency']] = relationship(
        init=False, back_populates='unit'
    )

    user_id: Mapped[uuid.UUID] = mapped_column(ForeignKey('users.id'))
    user: Mapped['User'] = relationship(init=False, lazy='joined')

    created_at: Mapped[datetime] = mapped_column(
        init=False, server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        init=False, nullable=True, onupdate=func.now()
    )
    deleted_at: Mapped[datetime | None] = mapped_column(
        init=False, nullable=True
    )

    tsv: Mapped[TSVECTOR] = mapped_column(
        TSVECTOR,
        Computed(
            "to_tsvector('portuguese', coalesce(unit_name, '') || ' ' || coalesce(unit_code, '') || ' ' || coalesce(unit_siaf, ''))",
            persisted=True,
        ),
        init=False,
        index=False,
    )

    # 2. Adicionada a UniqueConstraint para (unit_name, user_id)
    __table_args__ = (
        UniqueConstraint('unit_name', 'user_id', name='uq_unit_name_user_id'),
        Index('ix_units_tsv', tsv, postgresql_using='gin'),
    )


@table_registry.mapped_as_dataclass
class Agency:
    __tablename__ = 'agencys'

    id: Mapped[UUID] = mapped_column(
        init=False, primary_key=True, default=uuid.uuid4
    )
    agency_name: Mapped[str] = mapped_column(nullable=False)
    agency_code: Mapped[str] = mapped_column(nullable=False)

    unit_id: Mapped[uuid.UUID] = mapped_column(ForeignKey('units.id'))
    unit: Mapped['Unit'] = relationship(
        init=False, lazy='joined', back_populates='agencies'
    )

    sectors: Mapped[list['Sector']] = relationship(
        init=False, back_populates='agency'
    )

    user_id: Mapped[uuid.UUID] = mapped_column(ForeignKey('users.id'))
    user: Mapped['User'] = relationship(init=False, lazy='joined')

    created_at: Mapped[datetime] = mapped_column(
        init=False, server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        init=False, nullable=True, onupdate=func.now()
    )
    deleted_at: Mapped[datetime | None] = mapped_column(
        init=False, nullable=True
    )

    tsv: Mapped[TSVECTOR] = mapped_column(
        TSVECTOR,
        Computed(
            "to_tsvector('portuguese', coalesce(agency_name, '') || ' ' || coalesce(agency_code, ''))",
            persisted=True,
        ),
        init=False,
        index=False,
    )

    # 2. Adicionada a UniqueConstraint para (agency_name, unit_id)
    __table_args__ = (
        UniqueConstraint(
            'agency_name', 'unit_id', name='uq_agency_name_unit_id'
        ),
        Index('ix_agencys_tsv', tsv, postgresql_using='gin'),
    )


@table_registry.mapped_as_dataclass
class Sector:
    __tablename__ = 'sectors'
    id: Mapped[UUID] = mapped_column(
        init=False, primary_key=True, default=uuid.uuid4
    )
    sector_name: Mapped[str] = mapped_column(nullable=False)
    sector_code: Mapped[str] = mapped_column(nullable=False)

    agency_id: Mapped[uuid.UUID] = mapped_column(ForeignKey('agencys.id'))
    agency: Mapped['Agency'] = relationship(
        init=False, lazy='joined', back_populates='sectors'
    )

    user_id: Mapped[uuid.UUID] = mapped_column(ForeignKey('users.id'))
    user: Mapped['User'] = relationship(init=False, lazy='joined')

    locations: Mapped[list['Location']] = relationship(
        init=False, back_populates='sector'
    )

    created_at: Mapped[datetime] = mapped_column(
        init=False, server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        init=False, nullable=True, onupdate=func.now()
    )
    deleted_at: Mapped[datetime | None] = mapped_column(
        init=False, nullable=True
    )

    tsv: Mapped[TSVECTOR] = mapped_column(
        TSVECTOR,
        Computed(
            "to_tsvector('portuguese', coalesce(sector_name, '') || ' ' || coalesce(sector_code, ''))",
            persisted=True,
        ),
        init=False,
        index=False,
    )

    # 2. Adicionada a UniqueConstraint para (sector_name, agency_id)
    __table_args__ = (
        UniqueConstraint(
            'sector_name', 'agency_id', name='uq_sector_name_agency_id'
        ),
        Index('ix_sectors_tsv', tsv, postgresql_using='gin'),
    )


@table_registry.mapped_as_dataclass
class Location:
    __tablename__ = 'locations'
    id: Mapped[UUID] = mapped_column(
        init=False, primary_key=True, default=uuid.uuid4
    )
    location_code: Mapped[str] = mapped_column(nullable=False)
    location_name: Mapped[str] = mapped_column(nullable=False)

    sector_id: Mapped[uuid.UUID] = mapped_column(ForeignKey('sectors.id'))
    sector: Mapped['Sector'] = relationship(
        init=False, lazy='joined', back_populates='locations'
    )

    user_id: Mapped[uuid.UUID] = mapped_column(ForeignKey('users.id'))
    user: Mapped['User'] = relationship(init=False, lazy='joined')

    created_at: Mapped[datetime] = mapped_column(
        init=False, server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        init=False, nullable=True, onupdate=func.now()
    )
    deleted_at: Mapped[datetime | None] = mapped_column(
        init=False, nullable=True
    )

    tsv: Mapped[TSVECTOR] = mapped_column(
        TSVECTOR,
        Computed(
            "to_tsvector('portuguese', coalesce(location_name, '') || ' ' || coalesce(location_code, ''))",
            persisted=True,
        ),
        init=False,
        index=False,
    )

    __table_args__ = (
        UniqueConstraint(
            'location_name', 'sector_id', name='uq_location_name_sector_id'
        ),
        Index('ix_locations_tsv', tsv, postgresql_using='gin'),
    )


@table_registry.mapped_as_dataclass
class LegalGuardian:
    __tablename__ = 'legal_guardians'
    id: Mapped[UUID] = mapped_column(
        init=False, primary_key=True, default=uuid4
    )
    legal_guardians_code: Mapped[str] = mapped_column(nullable=False)
    legal_guardians_name: Mapped[str] = mapped_column(
        nullable=False, unique=True
    )

    user_id: Mapped[uuid.UUID] = mapped_column(ForeignKey('users.id'))
    user: Mapped[User] = relationship(init=False, lazy='joined')

    created_at: Mapped[datetime] = mapped_column(
        init=False, server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        init=False, nullable=True, onupdate=func.now()
    )
    deleted_at: Mapped[datetime | None] = mapped_column(
        init=False, nullable=True
    )

    tsv: Mapped[TSVECTOR] = mapped_column(
        TSVECTOR,
        Computed(
            "to_tsvector('portuguese', coalesce(legal_guardians_name, '') || ' ' || coalesce(legal_guardians_code, ''))",
            persisted=True,
        ),
        init=False,
        index=False,
    )

    __table_args__ = (
        Index('ix_legal_guardians_tsv', tsv, postgresql_using='gin'),
    )


@table_registry.mapped_as_dataclass
class Material:
    __tablename__ = 'materials'
    id: Mapped[UUID] = mapped_column(
        init=False, primary_key=True, default=uuid4
    )
    material_code: Mapped[str] = mapped_column(nullable=False)
    material_name: Mapped[str] = mapped_column(nullable=False, unique=True)

    user_id: Mapped[uuid.UUID] = mapped_column(ForeignKey('users.id'))
    user: Mapped[User] = relationship(init=False, lazy='joined')

    created_at: Mapped[datetime] = mapped_column(
        init=False, server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        init=False, nullable=True, onupdate=func.now()
    )
    deleted_at: Mapped[datetime | None] = mapped_column(
        init=False, nullable=True
    )

    tsv: Mapped[TSVECTOR] = mapped_column(
        TSVECTOR,
        Computed(
            "to_tsvector('portuguese', coalesce(material_name, '') || ' ' || coalesce(material_code, ''))",
            persisted=True,
        ),
        init=False,
        index=False,
    )

    __table_args__ = (Index('ix_materials_tsv', tsv, postgresql_using='gin'),)


@table_registry.mapped_as_dataclass
class Asset:
    __tablename__ = 'assets'

    id: Mapped[UUID] = mapped_column(
        init=False, primary_key=True, default=uuid4
    )

    location_id: Mapped[UUID | None] = mapped_column(
        ForeignKey('locations.id'), nullable=True
    )
    material_id: Mapped[UUID | None] = mapped_column(
        ForeignKey('materials.id'), nullable=True
    )
    legal_guardian_id: Mapped[UUID | None] = mapped_column(
        ForeignKey('legal_guardians.id'), nullable=True
    )

    location: Mapped[Location] = relationship(init=False, lazy='joined')

    material: Mapped[Material] = relationship(init=False, lazy='joined')
    legal_guardian: Mapped[LegalGuardian] = relationship(
        init=False, lazy='joined'
    )

    asset_code: Mapped[str] = mapped_column(nullable=False, index=True)
    asset_check_digit: Mapped[str] = mapped_column(nullable=False, index=True)
    atm_number: Mapped[str | None] = mapped_column(nullable=True, index=True)
    serial_number: Mapped[str | None] = mapped_column(nullable=True)
    asset_description: Mapped[str | None] = mapped_column(nullable=True)
    asset_status: Mapped[str | None] = mapped_column(nullable=True)
    asset_value: Mapped[str | None] = mapped_column(nullable=True)
    csv_code: Mapped[str | None] = mapped_column(nullable=True)
    accounting_entry_code: Mapped[str | None] = mapped_column(nullable=True)

    item_brand: Mapped[str | None] = mapped_column(nullable=True)
    item_model: Mapped[str | None] = mapped_column(nullable=True)

    group_type_code: Mapped[str | None] = mapped_column(nullable=True)
    group_code: Mapped[str | None] = mapped_column(nullable=True)
    expense_element_code: Mapped[str | None] = mapped_column(nullable=True)
    subelement_code: Mapped[str | None] = mapped_column(nullable=True)

    is_official: Mapped[bool] = mapped_column(default=False)

    created_at: Mapped[datetime] = mapped_column(
        init=False, server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        init=False, nullable=True, onupdate=func.now()
    )
    deleted_at: Mapped[datetime | None] = mapped_column(
        init=False, nullable=True
    )

    tsv: Mapped[TSVECTOR] = mapped_column(
        TSVECTOR,
        Computed(
            "to_tsvector('portuguese', "
            "coalesce(serial_number, '') || ' ' || "
            "coalesce(asset_description, '') || ' ' || "
            "coalesce(item_brand, '') || ' ' || "
            "coalesce(item_model, '') || ' ' || "
            "coalesce(atm_number, ''))",
            persisted=True,
        ),
        init=False,
        index=False,
    )
    __table_args__ = (
        Index('ix_assets_tsv', tsv, postgresql_using='gin'),
        UniqueConstraint('asset_code', 'asset_check_digit'),
    )


@table_registry.mapped_as_dataclass
class Catalog:
    __tablename__ = 'catalog'

    id: Mapped[uuid.UUID] = mapped_column(
        init=False, primary_key=True, default=uuid.uuid4
    )

    situation: Mapped[str | None] = mapped_column(nullable=False, index=True)
    conservation_status: Mapped[str | None] = mapped_column(nullable=True)
    description: Mapped[str | None] = mapped_column(nullable=True)

    asset_id: Mapped[uuid.UUID] = mapped_column(ForeignKey('assets.id'))
    user_id: Mapped[uuid.UUID] = mapped_column(ForeignKey('users.id'))

    asset: Mapped[Asset] = relationship(init=False, lazy='joined')
    user: Mapped[User] = relationship(init=False, lazy='joined')

    location_id: Mapped[UUID | None] = mapped_column(
        ForeignKey('locations.id'), nullable=True
    )
    location: Mapped[Location] = relationship(init=False, lazy='joined')
    images: Mapped[list['CatalogImage']] = relationship(
        back_populates='catalog',
        init=False,
        lazy='selectin',
        cascade='all, delete-orphan',
    )

    workflow_history: Mapped[list['CatalogWorkFlow']] = relationship(
        back_populates='catalog',
        init=False,
        lazy='selectin',
        cascade='all, delete-orphan',
    )

    created_at: Mapped[datetime] = mapped_column(
        init=False, server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        init=False, nullable=True, onupdate=func.now()
    )
    deleted_at: Mapped[datetime | None] = mapped_column(
        init=False, nullable=True
    )


@table_registry.mapped_as_dataclass
class CatalogImage:
    __tablename__ = 'catalog_images'

    id: Mapped[uuid.UUID] = mapped_column(
        init=False, primary_key=True, default=uuid.uuid4
    )

    catalog_id: Mapped[uuid.UUID] = mapped_column(ForeignKey('catalog.id'))
    file_path: Mapped[str] = mapped_column(nullable=False)
    catalog: Mapped['Catalog'] = relationship(
        back_populates='images', init=False
    )

    created_at: Mapped[datetime] = mapped_column(
        init=False, server_default=func.now()
    )


@table_registry.mapped_as_dataclass
class CatalogWorkFlow:
    __tablename__ = 'catalog_workflow'

    id: Mapped[uuid.UUID] = mapped_column(
        init=False, primary_key=True, default=uuid.uuid4
    )

    catalog_id: Mapped[uuid.UUID] = mapped_column(ForeignKey('catalog.id'))
    user_id: Mapped[uuid.UUID] = mapped_column(ForeignKey('users.id'))
    user: Mapped[User] = relationship(init=False, lazy='joined')
    catalog: Mapped['Catalog'] = relationship(
        back_populates='workflow_history', init=False
    )
    workflow_status: Mapped[str | None] = mapped_column(
        nullable=False, index=True
    )

    detail: Mapped[dict | None] = mapped_column(JSON, nullable=True)

    created_at: Mapped[datetime] = mapped_column(
        init=False, server_default=func.now()
    )


@table_registry.mapped_as_dataclass
class Inventory:
    __tablename__ = 'inventory'
    __table_args__ = (UniqueConstraint('key', name='uq_inventory_key'),)

    id: Mapped[uuid.UUID] = mapped_column(
        init=False, primary_key=True, default=uuid.uuid4
    )
    key: Mapped[str] = mapped_column(nullable=False)

    owners: Mapped[list['InventoryOwner']] = relationship(
        back_populates='inventory',
        init=False,
        lazy='selectin',
        cascade='all, delete-orphan',
    )

    created_by_id: Mapped[uuid.UUID] = mapped_column(ForeignKey('users.id'))
    created_by: Mapped[User] = relationship(init=False, lazy='joined')
    available: Mapped[bool] = mapped_column(default=True, init=False)
    created_at: Mapped[datetime] = mapped_column(
        init=False, server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        init=False, nullable=True, onupdate=func.now()
    )
    deleted_at: Mapped[datetime | None] = mapped_column(
        init=False, nullable=True
    )


@table_registry.mapped_as_dataclass
class InventoryOwner:
    __tablename__ = 'inventory_owners'
    __table_args__ = (
        UniqueConstraint('inventory_id', 'user_id', name='uq_inventory_user'),
    )
    id: Mapped[uuid.UUID] = mapped_column(
        init=False, primary_key=True, default=uuid.uuid4
    )
    inventory_id: Mapped[uuid.UUID] = mapped_column(ForeignKey('inventory.id'))
    inventory: Mapped['Inventory'] = relationship(
        init=False,
        back_populates='owners',
    )
    user_id: Mapped[uuid.UUID] = mapped_column(ForeignKey('users.id'))
    user: Mapped['User'] = relationship(init=False, lazy='joined')
    created_at: Mapped[datetime] = mapped_column(
        init=False, server_default=func.now()
    )

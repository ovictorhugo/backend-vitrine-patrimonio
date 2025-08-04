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
from sqlalchemy import Enum as SQLAlchemyEnum
from sqlalchemy.dialects.postgresql import TSVECTOR
from sqlalchemy.orm import Mapped, mapped_column, registry, relationship

table_registry = registry()


class ConservationStatus(str, PythonEnum):
    GOOD = 'GOOD'
    UNECONOMICAL = 'UNECONOMICAL'
    IRRECOVERABLE = 'IRRECOVERABLE'
    IDLE = 'IDLE'
    RECOVERABLE = 'RECOVERABLE'


class AssetSituation(str, PythonEnum):
    NORMAL = 'NORMAL'
    NOT_INVENTORIED = 'NOT_INVENTORIED'
    REGISTERED = 'REGISTERED'
    AWAITING_ACCEPTANCE = 'AWAITING_ACCEPTANCE'
    MOVED = 'MOVED'


class WorkFlowStatus(str, PythonEnum):
    STARTED = 'STARTED'
    REVIEW_REQUESTED = 'REVIEW_REQUESTED'
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
class Agency:
    __tablename__ = 'agencys'

    id: Mapped[UUID] = mapped_column(
        init=False, primary_key=True, default=uuid.uuid4
    )
    agency_name: Mapped[str] = mapped_column(nullable=False)
    agency_code: Mapped[str] = mapped_column(nullable=False, unique=True)

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
            "to_tsvector('portuguese', coalesce(agency_name, '') || ' ' || coalesce(agency_code, ''))",  # noqa: E501
            persisted=True,
        ),
        init=False,
        index=False,
    )

    __table_args__ = (Index('ix_agencys_tsv', tsv, postgresql_using='gin'),)


@table_registry.mapped_as_dataclass
class Unit:
    __tablename__ = 'units'
    id: Mapped[UUID] = mapped_column(
        init=False, primary_key=True, default=uuid4
    )
    unit_name: Mapped[str] = mapped_column(nullable=False)
    unit_code: Mapped[str] = mapped_column(nullable=False)
    unit_siaf: Mapped[str] = mapped_column(nullable=False)

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
            "to_tsvector('portuguese', coalesce(unit_name, '') || ' ' || coalesce(unit_code, '') || ' ' || coalesce(unit_siaf, ''))",
            persisted=True,
        ),
        init=False,
        index=False,
    )

    __table_args__ = (
        Index('ix_units_tsv', tsv, postgresql_using='gin'),
        UniqueConstraint('unit_name', 'unit_siaf', name='uq_unit'),
    )


@table_registry.mapped_as_dataclass
class Sector:
    __tablename__ = 'sectors'
    id: Mapped[UUID] = mapped_column(
        init=False, primary_key=True, default=uuid4
    )
    sector_name: Mapped[str] = mapped_column(nullable=False, unique=True)
    sector_code: Mapped[str] = mapped_column(nullable=False)

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
            "to_tsvector('portuguese', coalesce(sector_name, '') || ' ' || coalesce(sector_code, ''))",
            persisted=True,
        ),
        init=False,
        index=False,
    )

    __table_args__ = (Index('ix_sectors_tsv', tsv, postgresql_using='gin'),)


@table_registry.mapped_as_dataclass
class Location:
    __tablename__ = 'locations'
    id: Mapped[UUID] = mapped_column(
        init=False, primary_key=True, default=uuid4
    )
    location_code: Mapped[str] = mapped_column(nullable=False)
    location_name: Mapped[str] = mapped_column(nullable=False, unique=True)

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
            "to_tsvector('portuguese', coalesce(location_name, '') || ' ' || coalesce(location_code, ''))",
            persisted=True,
        ),
        init=False,
        index=False,
    )

    __table_args__ = (Index('ix_locations_tsv', tsv, postgresql_using='gin'),)


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
    __table_args__ = (UniqueConstraint('asset_code', 'asset_check_digit'),)

    id: Mapped[UUID] = mapped_column(
        init=False, primary_key=True, default=uuid4
    )

    agency_id: Mapped[UUID | None] = mapped_column(
        ForeignKey('agencys.id'), nullable=True
    )
    unit_id: Mapped[UUID | None] = mapped_column(
        ForeignKey('units.id'), nullable=True
    )
    sector_id: Mapped[UUID | None] = mapped_column(
        ForeignKey('sectors.id'), nullable=True
    )
    material_id: Mapped[UUID | None] = mapped_column(
        ForeignKey('materials.id'), nullable=True
    )
    legal_guardian_id: Mapped[UUID | None] = mapped_column(
        ForeignKey('legal_guardians.id'), nullable=True
    )
    location_id: Mapped[UUID | None] = mapped_column(
        ForeignKey('locations.id'), nullable=True
    )

    agency: Mapped[Agency] = relationship(init=False, lazy='joined')
    unit: Mapped[Unit] = relationship(init=False, lazy='joined')
    sector: Mapped[Sector] = relationship(init=False, lazy='joined')
    material: Mapped[Material] = relationship(init=False, lazy='joined')
    legal_guardian: Mapped[LegalGuardian] = relationship(
        init=False, lazy='joined'
    )
    location: Mapped[Location] = relationship(init=False, lazy='joined')

    asset_code: Mapped[str] = mapped_column(nullable=False)
    asset_check_digit: Mapped[str] = mapped_column(nullable=False)
    atm_number: Mapped[str | None] = mapped_column(nullable=True)
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
            "coalesce(asset_code, '') || ' ' || "
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
    __table_args__ = (Index('ix_assets_tsv', tsv, postgresql_using='gin'),)


@table_registry.mapped_as_dataclass
class Catalog:
    __tablename__ = 'catalog'

    id: Mapped[uuid.UUID] = mapped_column(
        init=False, primary_key=True, default=uuid.uuid4
    )

    situation: Mapped[AssetSituation] = mapped_column(
        SQLAlchemyEnum(AssetSituation, name='asset_situation_enum'),
        nullable=False,
    )
    conservation_status: Mapped[ConservationStatus] = mapped_column(
        SQLAlchemyEnum(ConservationStatus, name='conservation_status_enum'),
        nullable=False,
    )
    description: Mapped[str | None] = mapped_column(nullable=True)

    asset_id: Mapped[uuid.UUID] = mapped_column(ForeignKey('assets.id'))
    user_id: Mapped[uuid.UUID] = mapped_column(ForeignKey('users.id'))

    asset: Mapped[Asset] = relationship(init=False, lazy='joined')
    user: Mapped[User] = relationship(init=False, lazy='joined')

    workflow_history: Mapped[list['CatalogWorkFlow']] = relationship(
        'CatalogWorkFlow',
        back_populates='catalog',
        lazy='selectin',
        order_by='CatalogWorkFlow.created_at',
        default_factory=list,
        init=False,
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
class CatalogWorkFlow:
    __tablename__ = 'catalog_workflow'

    id: Mapped[uuid.UUID] = mapped_column(
        init=False, primary_key=True, default=uuid.uuid4
    )

    catalog_id: Mapped[uuid.UUID] = mapped_column(ForeignKey('catalog.id'))
    user_id: Mapped[uuid.UUID] = mapped_column(ForeignKey('users.id'))

    catalog: Mapped[Catalog] = relationship(init=False, lazy='joined')
    user: Mapped[User] = relationship(init=False, lazy='joined')

    workflow_status: Mapped[WorkFlowStatus] = mapped_column(
        SQLAlchemyEnum(WorkFlowStatus, name='workflow_status_enum'),
        nullable=False,
    )

    detail: Mapped[dict | None] = mapped_column(JSON, nullable=True)

    created_at: Mapped[datetime] = mapped_column(
        init=False, server_default=func.now()
    )


@table_registry.mapped_as_dataclass
class Inventory:
    __tablename__ = 'inventory'
    __table_args__ = (
        UniqueConstraint(
            'location_id',
            'term',
            name='uq_inventory_location_term',
        ),
    )

    id: Mapped[uuid.UUID] = mapped_column(
        init=False, primary_key=True, default=uuid.uuid4
    )
    location_id: Mapped[uuid.UUID] = mapped_column(ForeignKey('locations.id'))
    term: Mapped[str] = mapped_column(nullable=False)

    user_id: Mapped[uuid.UUID] = mapped_column(ForeignKey('users.id'))
    user: Mapped['User'] = relationship(init=False, lazy='joined')

    assets: Mapped[list['InventoryAsset']] = relationship(
        back_populates='inventory', init=False
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
class InventoryAsset:
    __tablename__ = 'inventory_assets'
    __table_args__ = (
        UniqueConstraint(
            'inventory_id', 'asset_id', name='uq_inventory_asset'
        ),
    )

    id: Mapped[uuid.UUID] = mapped_column(
        init=False, primary_key=True, default=uuid.uuid4
    )
    inventory_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey('inventory.id'), nullable=False
    )
    asset_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey('assets.id'), nullable=False
    )
    inventory_status: Mapped[InventoryStatus] = mapped_column(
        SQLAlchemyEnum(InventoryStatus, name='inventory_status_enum'),
        nullable=False,
    )

    inventory: Mapped['Inventory'] = relationship(
        init=False, lazy='joined', back_populates='assets'
    )
    asset: Mapped['Asset'] = relationship(init=False, lazy='joined')

    created_at: Mapped[datetime] = mapped_column(
        init=False, server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        init=False, nullable=True, onupdate=func.now()
    )
    deleted_at: Mapped[datetime | None] = mapped_column(
        init=False, nullable=True
    )

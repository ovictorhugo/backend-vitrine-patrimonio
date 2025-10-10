import uuid
from datetime import datetime
from enum import Enum as PythonEnum
from typing import Optional
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


class WorkflowTransferStatus(str, PythonEnum):
    PENDING = 'PENDING'
    DECLINED = 'DECLINED'
    ACCEPTABLE = 'ACCEPTABLE'


class InventoryAssetStatus(str, PythonEnum):
    OC = 'OC'
    QB = 'QB'
    NE = 'NE'
    SP = 'SP'


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
    VITRINE = 'VITRINE'
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

    matricula: Mapped[str | None] = mapped_column(nullable=True, init=False)

    verify: Mapped[bool] = mapped_column(default=False)
    institution_id: Mapped[UUID] = mapped_column(
        default=UUID('27b3839b-d9b3-43c6-824a-aef738ace101'), init=False
    )
    favorites: Mapped[list['FavoriteCatalog']] = relationship(
        back_populates='user',
        init=False,
        cascade='all, delete-orphan',
    )
    system_identity: Mapped[Optional['SystemIdentity']] = relationship(
        back_populates='user',
        init=False,
        cascade='all, delete-orphan',
        lazy='selectin',
        uselist=False,
    )
    transfer_requests: Mapped[list['WorkflowTransfer']] = relationship(
        back_populates='user',
        init=False,
        lazy='selectin',
        cascade='all, delete-orphan',
    )
    collections: Mapped[list['Collection']] = relationship(
        back_populates='user',
        init=False,
        cascade='all, delete-orphan',
        lazy='selectin',
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
    user: Mapped['User'] = relationship(init=False, lazy='selectin')

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
        init=False, lazy='selectin', back_populates='agencies'
    )

    sectors: Mapped[list['Sector']] = relationship(
        init=False, back_populates='agency'
    )

    user_id: Mapped[uuid.UUID] = mapped_column(ForeignKey('users.id'))
    user: Mapped['User'] = relationship(init=False, lazy='selectin')

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
        init=False, lazy='selectin', back_populates='sectors'
    )

    user_id: Mapped[uuid.UUID] = mapped_column(ForeignKey('users.id'))
    user: Mapped['User'] = relationship(init=False, lazy='selectin')

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

    legal_guardian_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey('legal_guardians.id'), nullable=False
    )
    legal_guardian: Mapped['LegalGuardian'] = relationship(
        init=False, lazy='selectin', back_populates='locations'
    )

    sector_id: Mapped[uuid.UUID] = mapped_column(ForeignKey('sectors.id'))
    sector: Mapped['Sector'] = relationship(
        init=False, lazy='selectin', back_populates='locations'
    )

    user_id: Mapped[uuid.UUID] = mapped_column(ForeignKey('users.id'))
    user: Mapped['User'] = relationship(init=False, lazy='selectin')

    inventory_assets: Mapped[list['InventoryAsset']] = relationship(
        back_populates='location', init=False
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
    incoming_transfers: Mapped[list['WorkflowTransfer']] = relationship(
        back_populates='location',
        init=False,
        cascade='all, delete-orphan',
    )
    location_inventories: Mapped[list['LocationInventory']] = relationship(
        back_populates='location',
        init=False,
        cascade='all, delete-orphan',
        lazy='selectin',
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
            'location_name',
            'sector_id',
            'legal_guardian_id',
            name='uq_location_name_sector_lg_id',
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
    user: Mapped[User] = relationship(init=False, lazy='selectin')

    created_at: Mapped[datetime] = mapped_column(
        init=False, server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        init=False, nullable=True, onupdate=func.now()
    )
    deleted_at: Mapped[datetime | None] = mapped_column(
        init=False, nullable=True
    )
    system_identity: Mapped[Optional['SystemIdentity']] = relationship(
        back_populates='legal_guardian',
        init=False,
        cascade='all, delete-orphan',
        uselist=False,
        foreign_keys='[SystemIdentity.legal_guardian_id]',
    )
    locations: Mapped[list['Location']] = relationship(
        back_populates='legal_guardian',
        init=False,
        cascade='all, delete-orphan',
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
    user: Mapped[User] = relationship(init=False, lazy='selectin')

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

    material_id: Mapped[UUID | None] = mapped_column(
        ForeignKey('materials.id'), nullable=True
    )
    legal_guardian_id: Mapped[UUID | None] = mapped_column(
        ForeignKey('legal_guardians.id'), nullable=True
    )
    location_id: Mapped[UUID | None] = mapped_column(
        ForeignKey('locations.id'), nullable=True
    )
    location: Mapped[Location] = relationship(init=False, lazy='selectin')
    material: Mapped[Material] = relationship(init=False, lazy='selectin')
    legal_guardian: Mapped[LegalGuardian] = relationship(
        init=False, lazy='selectin'
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

    user_id: Mapped[uuid.UUID] = mapped_column(ForeignKey('users.id'))
    user: Mapped[User] = relationship(init=False, lazy='selectin')

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

    asset: Mapped[Asset] = relationship(init=False, lazy='selectin')
    user: Mapped[User] = relationship(init=False, lazy='selectin')

    location_id: Mapped[UUID | None] = mapped_column(
        ForeignKey('locations.id'), nullable=True
    )
    location: Mapped[Location] = relationship(init=False, lazy='selectin')
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
    favorited_by: Mapped[list['FavoriteCatalog']] = relationship(
        back_populates='catalog',
        init=False,
        lazy='selectin',
        cascade='all, delete-orphan',
    )
    collection_items: Mapped[list['CollectionItem']] = relationship(
        back_populates='catalog',
        init=False,
        cascade='all, delete-orphan',
        lazy='selectin',
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
    user: Mapped[User] = relationship(init=False, lazy='selectin')
    catalog: Mapped['Catalog'] = relationship(
        back_populates='workflow_history', init=False
    )
    workflow_status: Mapped[str | None] = mapped_column(
        nullable=False, index=True
    )

    detail: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    transfer_requests: Mapped[list['WorkflowTransfer']] = relationship(
        back_populates='workflow',
        init=False,
        cascade='all, delete-orphan',
    )
    created_at: Mapped[datetime] = mapped_column(
        init=False, server_default=func.now()
    )


@table_registry.mapped_as_dataclass
class WorkflowTransfer:
    __tablename__ = 'workflow_transfer'

    id: Mapped[uuid.UUID] = mapped_column(
        init=False, primary_key=True, default=uuid.uuid4
    )

    workflow_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey('catalog_workflow.id'), nullable=False, index=True
    )

    user_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey('users.id'), nullable=False, index=True
    )

    location_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey('locations.id'), nullable=False, index=True
    )

    status: Mapped[str] = mapped_column(
        nullable=False, index=True, default='PENDING'
    )

    workflow: Mapped['CatalogWorkFlow'] = relationship(
        back_populates='transfer_requests',
        init=False,
        lazy='selectin',
    )

    user: Mapped['User'] = relationship(
        back_populates='transfer_requests', init=False, lazy='selectin'
    )

    location: Mapped['Location'] = relationship(
        back_populates='incoming_transfers', init=False, lazy='selectin'
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
class Inventory:
    __tablename__ = 'inventory'
    __table_args__ = (UniqueConstraint('key', name='uq_inventory_key'),)

    id: Mapped[uuid.UUID] = mapped_column(
        init=False, primary_key=True, default=uuid.uuid4
    )
    key: Mapped[str] = mapped_column(nullable=False)

    inventoried_locations: Mapped[list['LocationInventory']] = relationship(
        back_populates='inventory',
        init=False,
        lazy='selectin',
        cascade='all, delete-orphan',
    )

    created_by_id: Mapped[uuid.UUID] = mapped_column(ForeignKey('users.id'))
    created_by: Mapped[User] = relationship(init=False, lazy='selectin')
    avaliable: Mapped[bool] = mapped_column(default=True, nullable=True)
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
class LocationInventory:
    __tablename__ = 'location_inventory'
    __table_args__ = (
        UniqueConstraint(
            'inventory_id', 'location_id', name='uq_inventory_location'
        ),
    )
    id: Mapped[uuid.UUID] = mapped_column(
        init=False, primary_key=True, default=uuid.uuid4
    )
    inventory_id: Mapped[uuid.UUID] = mapped_column(ForeignKey('inventory.id'))
    inventory: Mapped['Inventory'] = relationship(
        init=False,
        back_populates='inventoried_locations',
    )

    location_id: Mapped[uuid.UUID] = mapped_column(ForeignKey('locations.id'))

    location: Mapped['Location'] = relationship(
        init=False, back_populates='location_inventories', lazy='selectin'
    )

    assets: Mapped[list['InventoryAsset']] = relationship(
        back_populates='location_inventory',
        init=False,
        cascade='all, delete-orphan',
        lazy='selectin',
    )
    filled: Mapped[bool] = mapped_column(default=False, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        init=False, server_default=func.now()
    )


@table_registry.mapped_as_dataclass
class InventoryAsset:
    __tablename__ = 'inventory_assets'
    __table_args__ = (
        UniqueConstraint(
            'location_inventory_id',
            'asset_id',
            name='uq_location_inventory_asset',
        ),
    )
    id: Mapped[uuid.UUID] = mapped_column(
        init=False, primary_key=True, default=uuid.uuid4
    )

    location_inventory_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey('location_inventory.id')
    )
    location_inventory: Mapped[LocationInventory] = relationship(
        init=False, back_populates='assets'
    )

    asset_id: Mapped[uuid.UUID] = mapped_column(ForeignKey('assets.id'))
    asset: Mapped[Asset] = relationship(init=False, lazy='selectin')

    status: Mapped[str | None] = mapped_column(nullable=False, index=True)
    comment: Mapped[str | None] = mapped_column(nullable=True)

    location_id: Mapped[uuid.UUID] = mapped_column(ForeignKey('locations.id'))
    location: Mapped['Location'] = relationship(
        init=False, lazy='selectin', back_populates='inventory_assets'
    )

    created_at: Mapped[datetime] = mapped_column(
        init=False, server_default=func.now()
    )
    deleted_at: Mapped[datetime | None] = mapped_column(
        init=False, nullable=True
    )


@table_registry.mapped_as_dataclass
class FavoriteCatalog:
    __tablename__ = 'favorite_catalogs'
    __table_args__ = (
        UniqueConstraint('user_id', 'catalog_id', name='uq_favorite_catalog'),
    )

    id: Mapped[uuid.UUID] = mapped_column(
        init=False, primary_key=True, default=uuid.uuid4
    )
    user_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey('users.id'), nullable=False
    )
    catalog_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey('catalog.id'), nullable=False
    )
    created_at: Mapped[datetime] = mapped_column(
        init=False, server_default=func.now()
    )

    user: Mapped['User'] = relationship(back_populates='favorites', init=False)
    catalog: Mapped['Catalog'] = relationship(
        back_populates='favorited_by', init=False, lazy='selectin'
    )


@table_registry.mapped_as_dataclass
class SystemIdentity:
    __tablename__ = 'system_identities'

    id: Mapped[UUID] = mapped_column(
        init=False, primary_key=True, default=uuid4
    )

    user_id: Mapped[UUID] = mapped_column(
        ForeignKey('users.id'), unique=True, nullable=False
    )

    legal_guardian_id: Mapped[UUID] = mapped_column(
        ForeignKey('legal_guardians.id'), unique=True, nullable=False
    )

    user: Mapped['User'] = relationship(
        back_populates='system_identity', init=False
    )
    legal_guardian: Mapped['LegalGuardian'] = relationship(
        back_populates='system_identity', init=False
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
class Collection:
    __tablename__ = 'collections'
    __table_args__ = (
        UniqueConstraint('user_id', 'name', name='uq_collection_user_name'),
    )

    id: Mapped[uuid.UUID] = mapped_column(
        init=False, primary_key=True, default=uuid.uuid4
    )
    name: Mapped[str] = mapped_column(nullable=False)
    description: Mapped[str | None] = mapped_column(nullable=True)
    type: Mapped[str | None] = mapped_column(nullable=True)

    user_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey('users.id'), nullable=False
    )
    user: Mapped['User'] = relationship(
        back_populates='collections', init=False
    )

    items: Mapped[list['CollectionItem']] = relationship(
        back_populates='collection',
        init=False,
        cascade='all, delete-orphan',
        lazy='selectin',
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
class CollectionItem:
    __tablename__ = 'collection_items'
    __table_args__ = (
        UniqueConstraint(
            'collection_id',
            'catalog_id',
            name='uq_collection_catalog_item',
        ),
    )

    id: Mapped[uuid.UUID] = mapped_column(
        init=False, primary_key=True, default=uuid.uuid4
    )
    collection_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey('collections.id'), nullable=False
    )

    catalog_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey('catalog.id'), nullable=False, index=True
    )

    status: Mapped[bool] = mapped_column(nullable=False)
    comment: Mapped[str | None] = mapped_column(nullable=True)

    collection: Mapped['Collection'] = relationship(
        back_populates='items', init=False
    )

    catalog: Mapped['Catalog'] = relationship(
        back_populates='collection_items', init=False, lazy='selectin'
    )

    created_at: Mapped[datetime] = mapped_column(
        init=False, server_default=func.now()
    )

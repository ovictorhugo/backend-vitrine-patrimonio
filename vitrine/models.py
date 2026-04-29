import uuid
from dataclasses import dataclass
from datetime import datetime
from enum import Enum as PythonEnum
from typing import Any, Optional
from uuid import UUID, uuid4

from sqlalchemy import (
    CheckConstraint,
    Computed,
    ForeignKey,
    Index,
    UniqueConstraint,
    and_,
    column,
    func,
)
from sqlalchemy.dialects.postgresql import JSONB, TSVECTOR
from sqlalchemy.orm import (
    Mapped,
    declared_attr,
    mapped_column,
    registry,
    relationship,
)

table_registry = registry()


@dataclass(init=False)
class AuditMixin:
    @declared_attr
    def created_at(cls) -> Mapped[datetime]:
        return mapped_column(init=False, server_default=func.now())

    @declared_attr
    def updated_at(cls) -> Mapped[Optional[datetime]]:
        return mapped_column(init=False, nullable=True, onupdate=func.now())

    @declared_attr
    def deleted_at(cls) -> Mapped[Optional[datetime]]:
        return mapped_column(init=False, nullable=True)

    @declared_attr
    def created_by(cls) -> Mapped[Optional[UUID]]:
        return mapped_column(
            ForeignKey(
                'users.id',
                name=f'fk_{cls.__tablename__}_created_by',
                use_alter=True,
            ),
            nullable=True,
            default=None,
        )

    @declared_attr
    def updated_by(cls) -> Mapped[Optional[UUID]]:
        return mapped_column(
            ForeignKey(
                'users.id',
                name=f'fk_{cls.__tablename__}_updated_by',
                use_alter=True,
            ),
            nullable=True,
            default=None,
        )

    @declared_attr
    def deleted_by(cls) -> Mapped[Optional[UUID]]:
        return mapped_column(
            ForeignKey(
                'users.id',
                name=f'fk_{cls.__tablename__}_deleted_by',
                use_alter=True,
            ),
            nullable=True,
            default=None,
        )

    def set_creation_audit(self, user_id: UUID):
        self.created_at = func.now()
        self.created_by = user_id

    def set_update_audit(self, user_id: UUID):
        self.updated_at = func.now()
        self.updated_by = user_id

    def set_deletion_audit(self, user_id: UUID):
        self.deleted_at = func.now()
        self.deleted_by = user_id


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
class User(AuditMixin):
    __tablename__ = 'users'

    id: Mapped[UUID] = mapped_column(
        init=False, primary_key=True, default=uuid4
    )

    username: Mapped[str]
    password: Mapped[str]

    email: Mapped[str]
    provider: Mapped[str | None] = mapped_column(nullable=True)

    linkedin: Mapped[str | None] = mapped_column(nullable=True, init=False)
    lattes_id: Mapped[str | None] = mapped_column(nullable=True, init=False)
    orcid: Mapped[str | None] = mapped_column(nullable=True, init=False)
    ramal: Mapped[str | None] = mapped_column(nullable=True, init=False)

    matricula: Mapped[str | None] = mapped_column(nullable=True, init=False)

    roles: Mapped[list['Role']] = relationship(
        'Role',
        secondary='user_roles',
        back_populates='users',
        primaryjoin=lambda: and_(
            User.id == UserRole.user_id,
            UserRole.deleted_at.is_(None),
        ),
        secondaryjoin=lambda: and_(
            Role.id == UserRole.role_id,
            Role.deleted_at.is_(None),
        ),
        init=False,
        lazy='selectin',
    )

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
        foreign_keys='[SystemIdentity.user_id]',
    )
    transfer_requests: Mapped[list['WorkflowTransfer']] = relationship(
        back_populates='user',
        init=False,
        lazy='selectin',
        cascade='all, delete-orphan',
        foreign_keys='[WorkflowTransfer.user_id]',
    )
    collections: Mapped[list['Collection']] = relationship(
        back_populates='user',
        init=False,
        cascade='all, delete-orphan',
        lazy='selectin',
        foreign_keys='[Collection.user_id]',
    )
    notifications_received: Mapped[list['UserNotification']] = relationship(
        back_populates='target_user',
        init=False,
        cascade='all, delete-orphan',
        foreign_keys='[UserNotification.target_user_id]',
        lazy='selectin',
    )
    notifications_sent: Mapped[list['Notification']] = relationship(
        back_populates='source_user',
        init=False,
        cascade='all, delete-orphan',
        foreign_keys='[Notification.source_user_id]',
        lazy='selectin',
    )
    user_role_associations: Mapped[list['UserRole']] = relationship(
        back_populates='user',
        init=False,
        cascade='all, delete-orphan',
        lazy='selectin',
        foreign_keys='[UserRole.user_id]',
    )

    tsv: Mapped[TSVECTOR] = mapped_column(
        TSVECTOR,
        Computed(
            "to_tsvector('portuguese', "
            "coalesce(username, '') || ' ' || "
            "regexp_replace(coalesce(split_part(email, '@', 1), ''), '[@._-]', ' ', 'g')"
            ')',
            persisted=True,
        ),
        init=False,
        index=False,
    )

    __table_args__ = (
        Index(
            'ix_uq_users_username_active',
            'username',
            unique=True,
            postgresql_where=(column('deleted_at').is_(None)),
        ),
        Index(
            'ix_uq_users_email_active',
            'email',
            unique=True,
            postgresql_where=(column('deleted_at').is_(None)),
        ),
        Index('ix_user_tsv', tsv, postgresql_using='gin'),
    )


@table_registry.mapped_as_dataclass
class Unit(AuditMixin):
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
    user: Mapped['User'] = relationship(
        init=False, lazy='selectin', foreign_keys=[user_id]
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
        Index(
            'ix_uq_units_unit_name_user_id_active',
            'unit_name',
            'user_id',
            unique=True,
            postgresql_where=(column('deleted_at').is_(None)),
        ),
        Index('ix_units_tsv', tsv, postgresql_using='gin'),
    )


@table_registry.mapped_as_dataclass
class Agency(AuditMixin):
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
    user: Mapped['User'] = relationship(
        init=False, lazy='selectin', foreign_keys=[user_id]
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
        Index(
            'ix_uq_agencys_agency_name_unit_id_active',
            'agency_name',
            'unit_id',
            unique=True,
            postgresql_where=(column('deleted_at').is_(None)),
        ),
        Index('ix_agencys_tsv', tsv, postgresql_using='gin'),
    )


@table_registry.mapped_as_dataclass
class Sector(AuditMixin):
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
    user: Mapped['User'] = relationship(
        init=False, lazy='selectin', foreign_keys=[user_id]
    )

    locations: Mapped[list['Location']] = relationship(
        init=False, back_populates='sector'
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
        Index(
            'ix_uq_sectors_sector_name_agency_id_active',
            'sector_name',
            'agency_id',
            unique=True,
            postgresql_where=(column('deleted_at').is_(None)),
        ),
        Index('ix_sectors_tsv', tsv, postgresql_using='gin'),
    )


@table_registry.mapped_as_dataclass
class Location(AuditMixin):
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
    user: Mapped['User'] = relationship(
        init=False, lazy='selectin', foreign_keys=[user_id]
    )

    inventory_assets: Mapped[list['InventoryAsset']] = relationship(
        back_populates='location', init=False
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
        Index(
            'ix_uq_locations_location_name_sector_lg_id_active',
            'location_name',
            'sector_id',
            'legal_guardian_id',
            unique=True,
            postgresql_where=(column('deleted_at').is_(None)),
        ),
        Index('ix_locations_tsv', tsv, postgresql_using='gin'),
    )


@table_registry.mapped_as_dataclass
class LegalGuardian(AuditMixin):
    __tablename__ = 'legal_guardians'
    id: Mapped[UUID] = mapped_column(
        init=False, primary_key=True, default=uuid4
    )
    legal_guardians_code: Mapped[str] = mapped_column(nullable=False)

    legal_guardians_name: Mapped[str] = mapped_column(nullable=False)

    user_id: Mapped[uuid.UUID] = mapped_column(ForeignKey('users.id'))
    user: Mapped[User] = relationship(
        init=False,
        lazy='selectin',
        foreign_keys=[user_id],
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
        Index(
            'ix_uq_legal_guardians_name_active',
            'legal_guardians_name',
            unique=True,
            postgresql_where=(column('deleted_at').is_(None)),
        ),
    )


@table_registry.mapped_as_dataclass
class Material(AuditMixin):
    __tablename__ = 'materials'
    id: Mapped[UUID] = mapped_column(
        init=False, primary_key=True, default=uuid4
    )
    material_code: Mapped[str] = mapped_column(nullable=False)

    material_name: Mapped[str] = mapped_column(nullable=False)

    user_id: Mapped[uuid.UUID] = mapped_column(ForeignKey('users.id'))
    user: Mapped[User] = relationship(
        init=False,
        lazy='selectin',
        foreign_keys=[user_id],
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

    __table_args__ = (
        Index('ix_materials_tsv', tsv, postgresql_using='gin'),
        Index(
            'ix_uq_materials_material_name_active',
            'material_name',
            unique=True,
            postgresql_where=(column('deleted_at').is_(None)),
        ),
    )


@table_registry.mapped_as_dataclass
class Asset(AuditMixin):
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
    user: Mapped[User] = relationship(
        init=False,
        lazy='selectin',
        foreign_keys=[user_id],
    )

    is_official: Mapped[bool] = mapped_column(default=False)

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
        Index(
            'ix_uq_assets_asset_code_asset_check_digit_active',
            'asset_code',
            'asset_check_digit',
            unique=True,
            postgresql_where=(column('deleted_at').is_(None)),
        ),
    )


@table_registry.mapped_as_dataclass
class Catalog(AuditMixin):
    __tablename__ = 'catalog'

    id: Mapped[uuid.UUID] = mapped_column(
        init=False, primary_key=True, default=uuid.uuid4
    )

    situation: Mapped[str | None] = mapped_column(nullable=False, index=True)
    conservation_status: Mapped[str | None] = mapped_column(nullable=True)
    description: Mapped[str | None] = mapped_column(nullable=True)
    current_workflow_status: Mapped[str | None] = mapped_column(index=True, nullable=True)

    asset_id: Mapped[uuid.UUID] = mapped_column(ForeignKey('assets.id'))
    user_id: Mapped[uuid.UUID] = mapped_column(ForeignKey('users.id'), index=True)

    asset: Mapped[Asset] = relationship(init=False, lazy='selectin')
    user: Mapped[User] = relationship(
        init=False,
        lazy='selectin',
        foreign_keys=[user_id],
    )

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
    files: Mapped[list['CatalogFile']] = relationship(
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
        order_by='CatalogWorkFlow.created_at.desc()',
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


@table_registry.mapped_as_dataclass
class CatalogImage:
    __tablename__ = 'catalog_images'

    id: Mapped[uuid.UUID] = mapped_column(
        init=False, primary_key=True, default=uuid.uuid4
    )

    catalog_id: Mapped[uuid.UUID] = mapped_column(ForeignKey('catalog.id'), index=True)
    file_path: Mapped[str] = mapped_column(nullable=False)
    catalog: Mapped['Catalog'] = relationship(
        back_populates='images', init=False
    )

    created_at: Mapped[datetime] = mapped_column(
        init=False, server_default=func.now()
    )


@table_registry.mapped_as_dataclass
class CatalogFile:
    __tablename__ = 'catalog_files'

    id: Mapped[uuid.UUID] = mapped_column(
        init=False, primary_key=True, default=uuid.uuid4
    )

    catalog_id: Mapped[uuid.UUID] = mapped_column(ForeignKey('catalog.id'), index=True)
    file_path: Mapped[str] = mapped_column(nullable=False)
    file_name: Mapped[str] = mapped_column(nullable=False)
    content_type: Mapped[str | None] = mapped_column(nullable=True)

    catalog: Mapped['Catalog'] = relationship(
        back_populates='files', init=False
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

    catalog_id: Mapped[uuid.UUID] = mapped_column(ForeignKey('catalog.id'), index=True)
    user_id: Mapped[uuid.UUID] = mapped_column(ForeignKey('users.id'))
    user: Mapped[User] = relationship(
        init=False,
        lazy='selectin',
        foreign_keys=[user_id],
    )
    catalog: Mapped['Catalog'] = relationship(
        back_populates='workflow_history', init=False
    )
    workflow_status: Mapped[str | None] = mapped_column(
        nullable=False, index=True
    )
    detail: Mapped[dict | None] = mapped_column(JSONB, nullable=True)
    transfer_requests: Mapped[list['WorkflowTransfer']] = relationship(
        back_populates='workflow',
        init=False,
        cascade='all, delete-orphan',
    )
    created_at: Mapped[datetime] = mapped_column(
        init=False, server_default=func.now()
    )


@table_registry.mapped_as_dataclass
class WorkflowTransfer(AuditMixin):
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
        back_populates='transfer_requests',
        init=False,
        lazy='selectin',
        foreign_keys=[user_id],
    )

    location: Mapped['Location'] = relationship(
        back_populates='incoming_transfers', init=False, lazy='selectin'
    )


@table_registry.mapped_as_dataclass
class Inventory(AuditMixin):
    __tablename__ = 'inventory'

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
    created_by: Mapped[User] = relationship(
        init=False,
        lazy='selectin',
        foreign_keys=[created_by_id],
    )
    avaliable: Mapped[bool] = mapped_column(default=True, nullable=True)

    __table_args__ = (
        Index(
            'ix_uq_inventory_key_active',
            'key',
            unique=True,
            postgresql_where=(column('deleted_at').is_(None)),
        ),
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
class InventoryAsset(AuditMixin):
    __tablename__ = 'inventory_assets'

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

    __table_args__ = (
        Index(
            'ix_uq_inventory_assets_location_inventory_asset_active',
            'location_inventory_id',
            'asset_id',
            unique=True,
            postgresql_where=(column('deleted_at').is_(None)),
        ),
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

    user: Mapped['User'] = relationship(
        back_populates='favorites', init=False, foreign_keys=[user_id]
    )
    catalog: Mapped['Catalog'] = relationship(
        back_populates='favorited_by', init=False, lazy='selectin'
    )


@table_registry.mapped_as_dataclass
class SystemIdentity(AuditMixin):
    __tablename__ = 'system_identities'

    id: Mapped[UUID] = mapped_column(
        init=False, primary_key=True, default=uuid4
    )

    user_id: Mapped[UUID] = mapped_column(
        ForeignKey('users.id'), nullable=False
    )

    legal_guardian_id: Mapped[UUID] = mapped_column(
        ForeignKey('legal_guardians.id'), nullable=False
    )

    user: Mapped['User'] = relationship(
        back_populates='system_identity',
        init=False,
        foreign_keys=[user_id],
    )
    legal_guardian: Mapped['LegalGuardian'] = relationship(
        back_populates='system_identity', init=False
    )

    __table_args__ = (
        Index(
            'ix_uq_system_identities_user_id_active',
            'user_id',
            unique=True,
            postgresql_where=(column('deleted_at').is_(None)),
        ),
        Index(
            'ix_uq_system_identities_legal_guardian_id_active',
            'legal_guardian_id',
            unique=True,
            postgresql_where=(column('deleted_at').is_(None)),
        ),
    )


@table_registry.mapped_as_dataclass
class Collection(AuditMixin):
    __tablename__ = 'collections'

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
        back_populates='collections',
        init=False,
        foreign_keys=[user_id],
    )

    items: Mapped[list['CollectionItem']] = relationship(
        back_populates='collection',
        init=False,
        cascade='all, delete-orphan',
        lazy='selectin',
    )

    __table_args__ = (
        Index(
            'ix_uq_collections_user_id_name_active',
            'user_id',
            'name',
            unique=True,
            postgresql_where=(column('deleted_at').is_(None)),
        ),
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


@table_registry.mapped_as_dataclass
class Notification(AuditMixin):
    __tablename__ = 'notifications'

    id: Mapped[uuid.UUID] = mapped_column(
        init=False, primary_key=True, default=uuid.uuid4
    )

    source_user_id: Mapped[uuid.UUID | None] = mapped_column(
        ForeignKey('users.id'), nullable=True
    )

    type: Mapped[str] = mapped_column(nullable=False, index=True)
    detail: Mapped[dict | None] = mapped_column(JSONB, nullable=True)

    source_user: Mapped[Optional['User']] = relationship(
        init=False,
        back_populates='notifications_sent',
        foreign_keys=[source_user_id],
        lazy='selectin',
    )

    recipients: Mapped[list['UserNotification']] = relationship(
        back_populates='notification',
        init=False,
        cascade='all, delete-orphan',
        lazy='selectin',
    )


@table_registry.mapped_as_dataclass
class UserNotification(AuditMixin):
    __tablename__ = 'user_notifications'

    id: Mapped[uuid.UUID] = mapped_column(
        init=False, primary_key=True, default=uuid.uuid4
    )

    notification_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey('notifications.id'), nullable=False, index=True
    )

    target_user_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey('users.id'), nullable=False, index=True
    )

    read_at: Mapped[datetime | None] = mapped_column(init=False, nullable=True)

    notification: Mapped['Notification'] = relationship(
        back_populates='recipients',
        init=False,
        lazy='selectin',
    )

    target_user: Mapped['User'] = relationship(
        back_populates='notifications_received',
        init=False,
        lazy='selectin',
        foreign_keys=[target_user_id],
    )
    __table_args__ = (
        Index(
            'ix_uq_user_notifications_notification_id_target_user_id_active',
            'notification_id',
            'target_user_id',
            unique=True,
            postgresql_where=(column('deleted_at').is_(None)),
        ),
    )


@table_registry.mapped_as_dataclass
class Permission(AuditMixin):
    __tablename__ = 'permissions'

    id: Mapped[UUID] = mapped_column(
        init=False, primary_key=True, default=uuid4
    )

    name: Mapped[str] = mapped_column(nullable=False)

    code: Mapped[str] = mapped_column(nullable=False)
    description: Mapped[str | None] = mapped_column(nullable=True)

    role_permissions: Mapped[list['RolePermission']] = relationship(
        back_populates='permission',
        init=False,
        cascade='all, delete-orphan',
        lazy='selectin',
    )

    roles: Mapped[list['Role']] = relationship(
        'Role',
        secondary='role_permissions',
        back_populates='permissions',
        primaryjoin=lambda: and_(
            Permission.id == RolePermission.permission_id,
            RolePermission.deleted_at.is_(None),
        ),
        secondaryjoin=lambda: and_(
            Role.id == RolePermission.role_id,
            Role.deleted_at.is_(None),
        ),
        init=False,
        lazy='selectin',
    )

    __table_args__ = (
        Index(
            'ix_uq_permissions_name_active',
            'name',
            unique=True,
            postgresql_where=(column('deleted_at').is_(None)),
        ),
        Index(
            'ix_uq_permissions_code_active',
            'code',
            unique=True,
            postgresql_where=(column('deleted_at').is_(None)),
        ),
    )


@table_registry.mapped_as_dataclass
class Role(AuditMixin):
    __tablename__ = 'roles'

    id: Mapped[UUID] = mapped_column(
        init=False, primary_key=True, default=uuid4
    )

    name: Mapped[str] = mapped_column(nullable=False)
    description: Mapped[str | None] = mapped_column(nullable=True)

    role_permissions: Mapped[list['RolePermission']] = relationship(
        back_populates='role',
        init=False,
        cascade='all, delete-orphan',
        lazy='selectin',
    )

    permissions: Mapped[list['Permission']] = relationship(
        'Permission',
        secondary='role_permissions',
        back_populates='roles',
        primaryjoin=lambda: and_(
            Role.id == RolePermission.role_id,
            RolePermission.deleted_at.is_(None),
        ),
        secondaryjoin=lambda: and_(
            Permission.id == RolePermission.permission_id,
            Permission.deleted_at.is_(None),
        ),
        init=False,
        lazy='selectin',
    )

    user_roles: Mapped[list['UserRole']] = relationship(
        back_populates='role',
        init=False,
        cascade='all, delete-orphan',
        lazy='selectin',
    )
    users: Mapped[list['User']] = relationship(
        'User',
        secondary='user_roles',
        back_populates='roles',
        primaryjoin=lambda: and_(
            Role.id == UserRole.role_id,
            UserRole.deleted_at.is_(None),
        ),
        secondaryjoin=lambda: and_(
            User.id == UserRole.user_id,
            User.deleted_at.is_(None),
        ),
        init=False,
        lazy='selectin',
    )
    tsv: Mapped[TSVECTOR] = mapped_column(
        TSVECTOR,
        Computed(
            "to_tsvector('portuguese', "
            "coalesce(name, '') || ' ' || "
            "coalesce(description, ''))",
            persisted=True,
        ),
        init=False,
        index=False,
    )
    __table_args__ = (
        Index('ix_role_tsv', tsv, postgresql_using='gin'),
        Index(
            'ix_uq_roles_name_active',
            'name',
            unique=True,
            postgresql_where=(column('deleted_at').is_(None)),
        ),
    )


@table_registry.mapped_as_dataclass
class UserRole(AuditMixin):
    __tablename__ = 'user_roles'

    id: Mapped[UUID] = mapped_column(
        init=False, primary_key=True, default=uuid4
    )
    user_id: Mapped[UUID] = mapped_column(
        ForeignKey('users.id'), nullable=False, index=True
    )
    role_id: Mapped[UUID] = mapped_column(
        ForeignKey('roles.id'), nullable=False, index=True
    )

    user: Mapped['User'] = relationship(
        back_populates='user_role_associations',
        init=False,
        lazy='selectin',
        foreign_keys=[user_id],
    )
    role: Mapped['Role'] = relationship(
        back_populates='user_roles', init=False, lazy='selectin'
    )

    __table_args__ = (
        Index(
            'ix_uq_user_roles_user_id_role_id_active',
            'user_id',
            'role_id',
            unique=True,
            postgresql_where=(column('deleted_at').is_(None)),
        ),
    )


@table_registry.mapped_as_dataclass
class RolePermission(AuditMixin):
    __tablename__ = 'role_permissions'

    id: Mapped[UUID] = mapped_column(
        init=False, primary_key=True, default=uuid4
    )
    role_id: Mapped[UUID] = mapped_column(
        ForeignKey('roles.id'), nullable=False
    )
    permission_id: Mapped[UUID] = mapped_column(
        ForeignKey('permissions.id'), nullable=False
    )

    role: Mapped['Role'] = relationship(
        back_populates='role_permissions', init=False, lazy='selectin'
    )
    permission: Mapped['Permission'] = relationship(
        back_populates='role_permissions', init=False, lazy='selectin'
    )

    __table_args__ = (
        Index(
            'ix_uq_role_permissions_role_id_permission_id_active',
            'role_id',
            'permission_id',
            unique=True,
            postgresql_where=(column('deleted_at').is_(None)),
        ),
    )


@table_registry.mapped_as_dataclass
class Feedback(AuditMixin):
    __tablename__ = 'feedbacks'
    __table_args__ = (
        CheckConstraint(
            'rating >= 0 AND rating <= 10', name='chk_feedback_rating'
        ),
    )

    id: Mapped[UUID] = mapped_column(
        init=False, primary_key=True, default=uuid4
    )
    name: Mapped[str]
    email: Mapped[str]
    rating: Mapped[int]
    description: Mapped[str | None] = mapped_column(nullable=True)

    user_id: Mapped[UUID | None] = mapped_column(
        ForeignKey('users.id'), nullable=True, init=False
    )
    user: Mapped[Optional['User']] = relationship(
        init=False,
        lazy='selectin',
        foreign_keys=[user_id],
    )


@table_registry.mapped_as_dataclass
class SystemSetting(AuditMixin):
    __tablename__ = 'system_settings'

    id: Mapped[UUID] = mapped_column(
        init=False, primary_key=True, default=uuid4
    )
    key: Mapped[str] = mapped_column(nullable=False, index=True)
    value: Mapped[Any] = mapped_column(JSONB, nullable=True)
    description: Mapped[str | None] = mapped_column(nullable=True)

    __table_args__ = (
        Index(
            'ix_uq_system_settings_key_active',
            'key',
            unique=True,
            postgresql_where=(column('deleted_at').is_(None)),
        ),
    )

from uuid import UUID, uuid4
from datetime import datetime
from sqlalchemy import ForeignKey, Index, column, select
from sqlalchemy.orm import Mapped, mapped_column, relationship

@table_registry.mapped_as_dataclass
class TransferDocument(AuditMixin):
    __tablename__ = 'transfer_documents'

    id: Mapped[UUID] = mapped_column(
        init=False, primary_key=True, default=uuid4
    )
    
    catalog_id: Mapped[UUID] = mapped_column(ForeignKey("catalog.id"), nullable=False)
    catalog: Mapped["Catalog"] = relationship(init=False)
    location_id: Mapped[UUID] = mapped_column(ForeignKey("locations.id"), nullable=False)
    location: Mapped["Location"] = relationship(init=False)
    signers: Mapped[list["TransferSigner"]] = relationship(
        init=False, 
        back_populates="transfer_document",
        cascade="all, delete-orphan"
    )
    
    file_path: Mapped[str] = mapped_column(nullable=True)
    current_step: Mapped[int] = mapped_column(default=0)
    status: Mapped[str] = mapped_column(default="PENDING", index=True)


@table_registry.mapped_as_dataclass
class TransferSigner(AuditMixin):
    __tablename__ = 'transfer_signers'

    id: Mapped[UUID] = mapped_column(
        init=False, primary_key=True, default=uuid4
    )

    transfer_document_id: Mapped[UUID] = mapped_column(ForeignKey("transfer_documents.id"), nullable=False)
    transfer_document: Mapped["TransferDocument"] = relationship(
        init=False,
        back_populates="signers"
    )
    user_id: Mapped[UUID] = mapped_column(ForeignKey("users.id"), nullable=True)
    user: Mapped["User"] = relationship(
        init=False, 
        foreign_keys=[user_id]
    )
    signedAt: Mapped[datetime | None] = mapped_column(nullable=True)
    isSigned: Mapped[bool] = mapped_column(default=False)
    
    token: Mapped[UUID] = mapped_column(init=False, insert_default=uuid4, index=True, nullable=False)

    __table_args__ = (
        Index(
            'ix_uq_transfer_signers_token_active',
            'token',
            unique=True,
            postgresql_where=(column('deleted_at').is_(None)),
        ),
    )


@table_registry.mapped_as_dataclass
class LoanableItem(AuditMixin):
    __tablename__ = 'loanable_items'

    id: Mapped[uuid.UUID] = mapped_column(
        init=False, primary_key=True, default=uuid.uuid4
    )
    
    catalog_id: Mapped[uuid.UUID] = mapped_column(ForeignKey('catalog.id'), unique=True)
    legal_guardian_id: Mapped[uuid.UUID] = mapped_column(ForeignKey('users.id'))

    
    owner_notes: Mapped[str | None] = mapped_column(nullable=True)
    last_check: Mapped[datetime] = mapped_column(nullable=True)
    in_maintenance: Mapped[bool] = mapped_column(default=False, nullable=True)
    is_visible: Mapped[bool] = mapped_column(
    default=True, 
    server_default='true', 
    nullable=False
)

    # Relacionamentos
    catalog: Mapped['Catalog'] = relationship(init=False, lazy='selectin')
    legal_guardian: Mapped['User'] = relationship(
        init=False, 
        lazy='selectin',
        foreign_keys=[legal_guardian_id] 
    )
    loans: Mapped[list['Loan']] = relationship(
        back_populates='loanable_item',
        init=False,
        lazy='selectin',
        cascade='all, delete-orphan',
        order_by='Loan.start_at.desc()'
    )

@table_registry.mapped_as_dataclass
class Loan(AuditMixin):
    __tablename__ = 'loans'

    id: Mapped[uuid.UUID] = mapped_column(
        init=False, primary_key=True, default=uuid.uuid4
    )   
    
    loanable_item_id: Mapped[uuid.UUID] = mapped_column(ForeignKey('loanable_items.id'))
    
    requester_id: Mapped[uuid.UUID] = mapped_column(ForeignKey('users.id'))  
    temporary_guardian_id: Mapped[uuid.UUID] = mapped_column(ForeignKey('users.id'))

    
    start_at: Mapped[datetime] = mapped_column(nullable=False)
    end_at: Mapped[datetime | None] = mapped_column(nullable=True)
    returned_at: Mapped[datetime | None] = mapped_column(nullable=True)

    # Observações
    lend_detail: Mapped[str | None] = mapped_column(nullable=True)
    returned_detail: Mapped[str | None] = mapped_column(nullable=True) 
    rejection_reason: Mapped[str | None] = mapped_column(nullable=True)
    
    is_confirmed: Mapped[bool] = mapped_column(default=False)
    is_executed: Mapped[bool] = mapped_column(default=False) 
    is_returned: Mapped[bool] = mapped_column(default=False)
    is_maintenance: Mapped[bool] = mapped_column(default=False)
    
    confirmed_by_id: Mapped[uuid.UUID | None] = mapped_column(ForeignKey('users.id'), default=None)
    executed_by_id: Mapped[uuid.UUID | None] = mapped_column(ForeignKey('users.id'), default=None)
    returned_by_id: Mapped[uuid.UUID | None] = mapped_column(ForeignKey('users.id'), default=None)
    
    loanable_item: Mapped['LoanableItem'] = relationship(
        back_populates='loans', 
        init=False
    )
    requester: Mapped['User'] = relationship(
        init=False, 
        foreign_keys=[requester_id] # Especificando quem é o requester
    )
    temporary_guardian: Mapped['User'] = relationship(
        init=False, 
        foreign_keys=[temporary_guardian_id]
    )

    confirmed_by: Mapped['User'] = relationship(
        init=False,
        foreign_keys=[confirmed_by_id]
    )
    
    executed_by: Mapped['User'] = relationship(
        init=False,
        foreign_keys=[executed_by_id]
    )
    
    returned_by: Mapped['User'] = relationship(
        init=False,
        foreign_keys=[returned_by_id]
    )


@table_registry.mapped_as_dataclass
class TemporaryFileReference(AuditMixin):
    __tablename__ = 'temporary_files'

    id: Mapped[UUID] = mapped_column(
        init=False, primary_key=True, default=uuid4
    )
    folder_type: Mapped[str] = mapped_column(nullable=False)
    file_name: Mapped[str] = mapped_column(nullable=False)
    token: Mapped[UUID] = mapped_column(
        init=False, insert_default=uuid4, index=True, nullable=False
    )

from datetime import datetime
from typing import Any, List, Optional
from uuid import UUID

from pydantic import (
    BaseModel,
    ConfigDict,
    EmailStr,
    Field,
)

from vitrine.models import (
    AssetSituation,
    InventoryAssetStatus,
    WorkflowTransferStatus,
)


class Message(BaseModel):
    message: str


class Token(BaseModel):
    access_token: str
    token_type: str


class PermissionSchema(BaseModel):
    name: str
    code: str
    description: Optional[str] = Field(default=None)


class PermissionPublic(BaseModel):
    id: UUID
    name: str
    code: str
    description: Optional[str] = Field(default=None)

    model_config = {'from_attributes': True}


class RoleSchema(BaseModel):
    name: str
    description: Optional[str] = Field(default=None)


class RolePublic(BaseModel):
    id: UUID
    name: str
    description: Optional[str] = Field(default=None)
    permissions: list[PermissionPublic] | None = Field(default=None)

    model_config = {'from_attributes': True}


class RoleList(BaseModel):
    roles: list[RolePublic]


class UserSchema(BaseModel):
    username: str
    email: EmailStr
    password: str


class UserUpdateSchema(BaseModel):
    username: str
    email: EmailStr
    provider: Optional[str] = Field(default=None)
    linkedin: Optional[str] = Field(default=None)
    lattes_id: Optional[str] = Field(default=None)
    orcid: Optional[str] = Field(default=None)
    ramal: Optional[str] = Field(default=None)
    photo_url: Optional[str] = Field(default=None)
    background_url: Optional[str] = Field(default=None)
    matricula: Optional[str] = Field(default=None)
    verify: bool | None = Field(default=None)
    institution_id: Optional[UUID] = Field(default=None)


class SystemIdentityPublic(BaseModel):
    id: UUID
    legal_guardian: 'LegalGuardianPublic'

    model_config = ConfigDict(from_attributes=True)


class UserPublic(BaseModel):
    id: UUID
    username: str
    email: EmailStr
    provider: Optional[str] = Field(default=None)
    linkedin: Optional[str] = Field(default=None)
    lattes_id: Optional[str] = Field(default=None)
    orcid: Optional[str] = Field(default=None)
    ramal: Optional[str] = Field(default=None)
    photo_url: Optional[str] = Field(default=None)
    background_url: Optional[str] = Field(default=None)
    matricula: Optional[str] = Field(default=None)
    verify: bool | None = Field(default=None)
    institution_id: Optional[UUID] = Field(default=None)

    roles: list[RolePublic]
    system_identity: Optional[SystemIdentityPublic]

    model_config = ConfigDict(from_attributes=True)


class UserDB(UserSchema):
    id: UUID


class UserList(BaseModel):
    users: List[UserPublic]


class FilterPage(BaseModel):
    offset: int = Field(0, ge=0)
    limit: int = Field(100, ge=1)


class RoleFilter(FilterPage):
    q: Optional[str] = Field(default=None)


class UnitSchema(BaseModel):
    unit_name: str = Field('NÃO ATRIBUIDO', validation_alias='uge_nom')
    unit_code: str = Field('NÃO ATRIBUIDO', validation_alias='uge_cod')
    unit_siaf: str = Field('NÃO ATRIBUIDO', validation_alias='uge_siaf')

    model_config = ConfigDict(from_attributes=True, populate_by_name=True)


class UnitPublic(UnitSchema):
    id: UUID


class UnitList(BaseModel):
    units: List[UnitPublic]


class FilterUnit(FilterPage):
    q: Optional[str] = Field(default=None)
    agency_id: Optional[UUID] = Field(
        default=None, description='Filtrar por ID da Agência'
    )


class FilterUser(FilterPage):
    q: Optional[str] = Field(default=None)


class AgencySchema(BaseModel):
    agency_name: str = Field('NÃO ATRIBUIDO', validation_alias='org_nom')
    agency_code: str = Field('NÃO ATRIBUIDO', validation_alias='org_cod')
    unit_id: UUID
    model_config = ConfigDict(from_attributes=True, populate_by_name=True)


class AgencyPublic(AgencySchema):
    id: UUID
    unit: UnitPublic


class AgencyList(BaseModel):
    agencies: List[AgencyPublic]


class FilterAgency(FilterPage):
    q: Optional[str] = Field(default=None)


class SectorSchema(BaseModel):
    agency_id: UUID

    sector_name: str = Field('NÃO ATRIBUIDO', validation_alias='set_nom')
    sector_code: str = Field('NÃO ATRIBUIDO', validation_alias='set_cod')

    model_config = ConfigDict(from_attributes=True, populate_by_name=True)


class SectorPublic(SectorSchema):
    id: UUID
    agency: AgencyPublic


class SectorList(BaseModel):
    sectors: List[SectorPublic]


class FilterSector(FilterPage):
    q: Optional[str] = Field(default=None)
    agency_id: Optional[UUID] = Field(
        default=None, description='Filtrar por ID da Organização'
    )


class LocationSchema(BaseModel):
    legal_guardian_id: UUID
    sector_id: UUID

    location_name: str = Field('NÃO ATRIBUIDO', validation_alias='loc_nom')
    location_code: str = Field('NÃO ATRIBUIDO', validation_alias='loc_cod')

    model_config = ConfigDict(from_attributes=True, populate_by_name=True)


class LegalGuardianSchema(BaseModel):
    legal_guardians_code: str = Field(
        'NÃO ATRIBUIDO', validation_alias='pes_cod'
    )
    legal_guardians_name: str = Field(
        'NÃO ATRIBUIDO', validation_alias='pes_nome'
    )

    model_config = ConfigDict(from_attributes=True, populate_by_name=True)


class LegalGuardianPublic(LegalGuardianSchema):
    id: UUID
    model_config = ConfigDict(from_attributes=True)


class LegalGuardianList(BaseModel):
    legal_guardians: List[LegalGuardianPublic]


class FilterLegalGuardian(FilterPage):
    q: Optional[str] = Field(default=None)


class LocationPublic(LocationSchema):
    id: UUID
    sector: SectorPublic
    legal_guardian: LegalGuardianPublic


class LocationList(BaseModel):
    locations: List[LocationPublic]


class MyLocationPublic(LocationPublic): ...


class MyLocationList(BaseModel):
    locations: List[MyLocationPublic]


class FilterLocation(FilterPage):
    q: Optional[str] = Field(default=None)
    sector_id: Optional[UUID] = Field(
        default=None, description='Filtrar por ID do setor'
    )
    legal_guardian_id: Optional[UUID] = Field(
        default=None, description='Filtrar por ID do responsável legal'
    )


class FilterLocationInventory(FilterLocation):
    filled: Optional[bool] = Field(default=None)


class MaterialSchema(BaseModel):
    material_code: Optional[str] = Field(
        'NÃO ATRIBUIDO', validation_alias='mat_cod'
    )
    material_name: Optional[str] = Field(
        'NÃO ATRIBUIDO', validation_alias='mat_nom'
    )

    model_config = ConfigDict(from_attributes=True, populate_by_name=True)


class MaterialPublic(MaterialSchema):
    id: UUID
    model_config = ConfigDict(from_attributes=True)


class MaterialList(BaseModel):
    materials: List[MaterialPublic]


class FilterMaterial(FilterPage):
    q: Optional[str] = Field(default=None)


class AssetSchema(BaseModel):
    asset_code: str = Field('NÃO ATRIBUIDO', validation_alias='bem_cod')
    asset_check_digit: str = Field('NÃO ATRIBUIDO', validation_alias='bem_dgv')
    atm_number: Optional[str] = Field(
        'NÃO ATRIBUIDO', validation_alias='bem_num_atm'
    )
    serial_number: Optional[str] = Field(
        'NÃO ATRIBUIDO', validation_alias='bem_serie'
    )
    asset_status: Optional[str] = Field(
        'NÃO ATRIBUIDO', validation_alias='bem_sta'
    )
    asset_value: Optional[str] = Field(
        'NÃO ATRIBUIDO', validation_alias='bem_val'
    )
    asset_description: Optional[str] = Field(
        'NÃO ATRIBUIDO', validation_alias='bem_dsc_com'
    )
    csv_code: Optional[str] = Field(None, validation_alias='csv_cod')
    accounting_entry_code: Optional[str] = Field(
        'NÃO ATRIBUIDO', validation_alias='tre_cod'
    )

    location_id: UUID
    material_id: UUID
    legal_guardian_id: UUID

    item_brand: Optional[str] = Field(
        'NÃO ATRIBUIDO', validation_alias='ite_mar'
    )
    item_model: Optional[str] = Field(
        'NÃO ATRIBUIDO', validation_alias='ite_mod'
    )

    group_type_code: Optional[str] = Field(
        'NÃO ATRIBUIDO', validation_alias='tgr_cod'
    )
    group_code: Optional[str] = Field(
        'NÃO ATRIBUIDO', validation_alias='grp_cod'
    )
    expense_element_code: Optional[str] = Field(
        'NÃO ATRIBUIDO', validation_alias='ele_cod'
    )
    subelement_code: Optional[str] = Field(
        'NÃO ATRIBUIDO', validation_alias='sbe_cod'
    )

    model_config = ConfigDict(from_attributes=True, populate_by_name=True)


class AssetPublic(AssetSchema):
    id: UUID
    material: MaterialPublic
    legal_guardian: LegalGuardianPublic
    location: LocationPublic
    location_id: UUID = Field(exclude=True)
    material_id: UUID = Field(exclude=True)
    legal_guardian_id: UUID = Field(exclude=True)
    is_official: bool
    model_config = ConfigDict(from_attributes=True)


class AssetList(BaseModel):
    assets: List[AssetPublic]


class AssetCodeList(BaseModel):
    asset_code: List[str]


class AssetCheckDigitList(BaseModel):
    asset_check_digit: List[str]


class AtmNumberList(BaseModel):
    atm_number: List[str]


class AssetIdentifierList(BaseModel):
    asset_identifier: List[str]


class MaterialNameResponseList(BaseModel):
    material_name: List[str] = Field(default=None)


class CatalogAssetIdentifier(BaseModel):
    catalog_id: UUID
    asset_identifier: str


class CatalogAssetIdentifierList(BaseModel):
    catalogs: List[CatalogAssetIdentifier]


class LegalGuardianNameResponseList(BaseModel):
    legal_guardians_name: List[str] = Field(default=None)


class FilterAsset(BaseModel):
    limit: int = 100
    offset: int = 0
    q: Optional[str] = Field(default=None)

    asset_identifier: Optional[str] = Field(default=None)
    atm_number: Optional[str] = Field(default=None)
    csv_code: Optional[str] = Field(default=None)
    asset_status: Optional[str] = Field(default=None)
    agency_id: Optional[UUID] = Field(default=None)
    unit_id: Optional[UUID] = Field(default=None)
    sector_id: Optional[UUID] = Field(default=None)
    location_id: Optional[UUID] = Field(default=None)
    material_id: Optional[UUID] = Field(default=None)
    legal_guardian_id: Optional[UUID] = Field(default=None)
    is_official: Optional[bool] = Field(default=None)


class RequestTransferSchema(BaseModel):
    location_id: UUID


class RequestTransferPublic(RequestTransferSchema):
    id: UUID
    status: str
    location_id: UUID = Field(exclude=True)
    user: UserPublic
    location: LocationPublic
    model_config = ConfigDict(from_attributes=True)


class RequestTransferList(BaseModel):
    transfer_requests: List[RequestTransferPublic]


class FilterTransfer(FilterPage):
    user_id: Optional[UUID] = Field(default=None)
    workflow_id: Optional[UUID] = Field(default=None)
    status: Optional[WorkflowTransferStatus] = Field(default=None)


class CatalogSchema(BaseModel):
    asset_id: UUID
    location_id: UUID
    situation: AssetSituation
    conservation_status: str
    description: Optional[str] = Field(default=None)


class CatalogWorkFlowSchema(BaseModel):
    workflow_status: str
    detail: Optional[dict] = Field(default=None)


class CatalogWorkFlowPublic(CatalogWorkFlowSchema):
    id: UUID
    user_id: UUID = Field(exclude=True)
    user: UserPublic
    transfer_requests: List[RequestTransferPublic]
    catalog_id: UUID
    created_at: datetime

    model_config = ConfigDict(from_attributes=True)


class CatalogImagePublic(BaseModel):
    id: UUID
    catalog_id: UUID
    file_path: str
    model_config = ConfigDict(from_attributes=True)


class CatalogFilePublic(BaseModel):
    id: UUID
    catalog_id: UUID
    file_path: str
    file_name: str
    content_type: Optional[str] = Field(default=None)

    model_config = ConfigDict(from_attributes=True)


class CatalogFileList(BaseModel):
    files: list[CatalogFilePublic]


class CatalogPublic(CatalogSchema):
    id: UUID
    asset_id: UUID = Field(exclude=True)
    user_id: UUID = Field(exclude=True)
    location_id: UUID = Field(exclude=True)

    asset: AssetPublic
    user: UserPublic
    location: LocationPublic
    images: List[CatalogImagePublic]
    files: List[CatalogFilePublic]
    workflow_history: List[CatalogWorkFlowPublic]

    created_at: datetime
    model_config = ConfigDict(from_attributes=True)


class CatalogList(BaseModel):
    catalog_entries: List[CatalogPublic]


class FilterCatalog(FilterAsset):
    only_uncollected: Optional[bool] = False
    reviewer_id: Optional[UUID] = Field(default=None)

    location_id: Optional[UUID] = Field(default=None)
    unit_id: Optional[UUID] = Field(default=None)
    agency_id: Optional[UUID] = Field(default=None)
    sector_id: Optional[UUID] = Field(default=None)

    material_id: Optional[UUID] = Field(default=None)
    legal_guardian_id: Optional[UUID] = Field(default=None)

    workflow_status: Optional[str] = Field(default=None)
    user_id: Optional[UUID] = Field(default=None)
    role_id: Optional[UUID] = Field(default=None)


class FilterSearchCatalog(FilterPage):
    q: Optional[str] = Field(default=None)
    user_id: Optional[UUID] = Field(default=None)
    workflow_status: Optional[str] = Field(default=None)


class InventorySchema(BaseModel):
    key: str
    avaliable: Optional[bool] = True
    model_config = ConfigDict(from_attributes=True)


class InventoryPublic(InventorySchema):
    id: UUID
    created_at: datetime
    created_by: UserPublic
    model_config = ConfigDict(from_attributes=True)


class InventoryList(BaseModel):
    inventories: List[InventoryPublic]


class FilterInventory(FilterPage):
    q: Optional[str] = Field(default=None)


class InventoryAssetSchema(BaseModel):
    asset_id: UUID
    status: InventoryAssetStatus | None = InventoryAssetStatus.OC.value
    location_id: UUID
    comment: Optional[str] = Field(default=None)


class InventoryAssetPublic(BaseModel):
    id: UUID
    status: InventoryAssetStatus | None
    comment: Optional[str]
    asset: AssetPublic

    model_config = ConfigDict(from_attributes=True)


class InventoryAssetList(BaseModel):
    inventoried_asset: List[InventoryAssetPublic]


class FavoriteSchema(BaseModel):
    catalog_id: UUID


class FavoriteList(BaseModel):
    favorites: List[CatalogPublic]


class CollectionItemPublic(BaseModel):
    id: UUID
    status: bool
    comment: Optional[str]
    catalog: CatalogPublic
    model_config = ConfigDict(from_attributes=True)


class CollectionItemsList(BaseModel):
    collection_items: List[CollectionItemPublic]


class CollectionItemSchema(BaseModel):
    catalog_id: UUID
    status: bool
    comment: Optional[str] = Field(default=None)


class CollectionItemUpdate(BaseModel):
    status: bool
    comment: Optional[str] = Field(default=None)


class CollectionSchema(BaseModel):
    name: str
    description: Optional[str] = Field(default=None)
    type: Optional[str] = Field(default=None)


class CollectionUpdateSchema(BaseModel):
    name: Optional[str] = Field(default=None)
    description: Optional[str] = Field(default=None)


class CollectionPublic(BaseModel):
    id: UUID
    name: str
    description: Optional[str]
    created_at: datetime

    model_config = ConfigDict(from_attributes=True)


class CollectionList(BaseModel):
    collections: list[CollectionPublic]


class FilterCollection(FilterPage):
    q: Optional[str] = Field(default=None)
    type: Optional[str] = Field(default=None)


class NotificationCreateSchema(BaseModel):
    target_user_id: str = Field(
        description="IDs de usuário separados por ';' ou '*' para todos."
    )
    type: str
    detail: Optional[dict] = {}


class NotificationUpdateSchema(BaseModel):
    read: bool


class FilterNotification(FilterPage):
    read: Optional[bool] = Field(
        default=None,
        description='Filtrar por notificações lidas (true) ou não lidas (false).',
    )
    type: Optional[str] = Field(
        default=None,
        description='Filtrar por tipo de notificação (ex: NEW_TRANSFER_REQUEST).',
    )


class NotificationContentPublic(BaseModel):
    id: UUID
    type: str
    detail: Optional[dict] = Field(default=None)
    source_user: Optional[UserPublic] = Field(default=None)
    model_config = ConfigDict(from_attributes=True)


class NotificationPublic(BaseModel):
    id: UUID
    read_at: Optional[datetime] = Field(default=None)
    created_at: datetime
    notification: NotificationContentPublic
    model_config = ConfigDict(from_attributes=True)


class NotificationList(BaseModel):
    notifications: List[NotificationPublic]


class UserNotificationRecipientPublic(BaseModel):
    id: UUID
    read_at: Optional[datetime]
    target_user: Optional[UserPublic]
    model_config = ConfigDict(from_attributes=True)


class NotificationSentPublic(BaseModel):
    id: UUID
    type: str
    detail: Optional[dict] = Field(default=None)
    created_at: datetime
    recipients: List[UserNotificationRecipientPublic]
    model_config = ConfigDict(from_attributes=True)


class NotificationSentList(BaseModel):
    notifications: List[NotificationSentPublic]


class FeedbackBase(BaseModel):
    name: str = Field(..., min_length=2, max_length=100)
    email: EmailStr
    rating: int = Field(..., ge=0, le=10)
    description: Optional[str] = Field(default=None)


class FeedbackCreate(FeedbackBase):
    pass


class FeedbackUpdate(BaseModel):
    name: Optional[str] = Field(None, min_length=2, max_length=100)
    email: Optional[EmailStr] = Field(default=None)
    rating: Optional[int] = Field(None, ge=0, le=10)
    description: Optional[str] = Field(default=None)


class FeedbackPublic(FeedbackBase):
    id: UUID
    created_at: datetime
    model_config = ConfigDict(from_attributes=True)


class FeedbackList(BaseModel):
    feedbacks: List[FeedbackPublic]


class SystemSettingBase(BaseModel):
    key: str
    value: Any | None = Field(default=None)
    description: Optional[str] = Field(default=None)

    model_config = ConfigDict(from_attributes=True)


class SystemSettingCreate(SystemSettingBase):
    pass


class SystemSettingUpdate(BaseModel):
    value: Any | None = Field(default=None)
    description: Optional[str] = Field(default=None)


class SystemSettingPublic(SystemSettingBase):
    id: UUID
    created_at: datetime
    updated_at: datetime | None


class SystemSettingList(BaseModel):
    settings: list[SystemSettingPublic]


class FilterCatalogImage(FilterPage):
    random: Optional[bool] = Field(
        False, description='Listar em ordem aleatória'
    )


class CatalogImageList(BaseModel):
    images: list[CatalogImagePublic]


class CatalogStatisticsFilters(BaseModel):
    q: Optional[str] = Field(default=None)
    material_id: Optional[UUID] = Field(default=None)
    unit_id: Optional[UUID] = Field(default=None)
    agency_id: Optional[UUID] = Field(default=None)
    sector_id: Optional[UUID] = Field(default=None)
    location_id: Optional[UUID] = Field(default=None)
    legal_guardian_id: Optional[UUID] = Field(default=None)
    is_official: bool | None = Field(default=None)
    asset_status: Optional[UUID] = Field(default=None)
    csv_code: Optional[str] = Field(default=None)
    role_id: Optional[str] = Field(default=None)
    user_id: Optional[str] = Field(default=None)

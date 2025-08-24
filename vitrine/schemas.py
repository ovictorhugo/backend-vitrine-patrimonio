from typing import List, Optional
from uuid import UUID

from pydantic import BaseModel, ConfigDict, EmailStr, Field

from vitrine.models import (
    AssetSituation,
    WorkFlowStatus,
)


class Message(BaseModel):
    message: str


class Token(BaseModel):
    access_token: str
    token_type: str


class UserSchema(BaseModel):
    username: str
    email: EmailStr
    password: str


class UserPublic(BaseModel):
    id: UUID
    username: str
    email: EmailStr
    provider: str | None = None
    linkedin: str | None = None
    lattes_id: str | None = None
    orcid: str | None = None
    ramal: str | None = None
    photo_url: str | None = None
    background_url: str | None = None
    matricula: str | None = None
    verify: bool | None = None
    institution_id: UUID | None = None

    model_config = ConfigDict(from_attributes=True)


class UserDB(UserSchema):
    id: UUID


class UserList(BaseModel):
    users: list[UserPublic]


class FilterPage(BaseModel):
    offset: int = Field(0, ge=0)
    limit: int = Field(100, ge=1)


class UnitSchema(BaseModel):
    unit_name: str = Field(..., validation_alias='uge_nom')
    unit_code: str = Field(..., validation_alias='uge_cod')
    unit_siaf: str = Field(..., validation_alias='uge_siaf')

    model_config = ConfigDict(from_attributes=True, populate_by_name=True)


class UnitPublic(UnitSchema):
    id: UUID


class UnitList(BaseModel):
    units: list[UnitPublic]


class FilterUnit(FilterPage):
    q: str | None = Field(default=None)
    agency_id: Optional[UUID] = Field(
        default=None, description='Filtrar por ID da Agência'
    )


class AgencySchema(BaseModel):
    agency_name: str = Field(..., validation_alias='org_nom')
    agency_code: str = Field(..., validation_alias='org_cod')
    unit_id: UUID
    model_config = ConfigDict(from_attributes=True, populate_by_name=True)


class AgencyPublic(AgencySchema):
    id: UUID
    unit: UnitPublic


class AgencyList(BaseModel):
    agencies: list[AgencyPublic]


class FilterAgency(FilterPage):
    q: str | None = Field(default=None)


class SectorSchema(BaseModel):
    agency_id: UUID

    sector_name: str = Field(..., validation_alias='set_nom')
    sector_code: str = Field(..., validation_alias='set_cod')

    model_config = ConfigDict(from_attributes=True, populate_by_name=True)


class SectorPublic(SectorSchema):
    id: UUID
    agency: AgencyPublic


class SectorList(BaseModel):
    sectors: list[SectorPublic]


class FilterSector(FilterPage):
    q: str | None = Field(default=None)
    agency_id: Optional[UUID] = Field(
        default=None, description='Filtrar por ID da Organização'
    )


class LocationSchema(BaseModel):
    sector_id: UUID

    location_name: str = Field(..., validation_alias='loc_nom')
    location_code: str = Field(..., validation_alias='loc_cod')

    model_config = ConfigDict(from_attributes=True, populate_by_name=True)


class LocationPublic(LocationSchema):
    id: UUID
    sector: SectorPublic


class LocationList(BaseModel):
    locations: list[LocationPublic]


class FilterLocation(FilterPage):
    q: str | None = Field(default=None)
    sector_id: Optional[UUID] = Field(
        default=None, description='Filtrar por ID do setor'
    )


class LegalGuardianSchema(BaseModel):
    legal_guardians_code: str = Field(..., validation_alias='pes_cod')
    legal_guardians_name: str = Field(..., validation_alias='pes_nome')

    model_config = ConfigDict(from_attributes=True, populate_by_name=True)


class LegalGuardianPublic(LegalGuardianSchema):
    id: UUID
    model_config = ConfigDict(from_attributes=True)


class LegalGuardianList(BaseModel):
    legal_guardians: list[LegalGuardianPublic]


class FilterLegalGuardian(FilterPage):
    q: str | None = Field(default=None)


class MaterialSchema(BaseModel):
    material_code: str | None = Field(..., validation_alias='mat_cod')
    material_name: str | None = Field(..., validation_alias='mat_nom')

    model_config = ConfigDict(from_attributes=True, populate_by_name=True)


class MaterialPublic(MaterialSchema):
    id: UUID
    model_config = ConfigDict(from_attributes=True)


class MaterialList(BaseModel):
    materials: list[MaterialPublic]


class FilterMaterial(FilterPage):
    q: str | None = Field(default=None)


class AssetSchema(BaseModel):
    asset_code: str = Field(..., validation_alias='bem_cod')
    asset_check_digit: str = Field(..., validation_alias='bem_dgv')
    atm_number: Optional[str] = Field(None, validation_alias='bem_num_atm')
    serial_number: Optional[str] = Field(None, validation_alias='bem_serie')
    asset_status: Optional[str] = Field(None, validation_alias='bem_sta')
    asset_value: Optional[str] = Field(None, validation_alias='bem_val')
    asset_description: Optional[str] = Field(
        None, validation_alias='bem_dsc_com'
    )
    csv_code: Optional[str] = Field(None, validation_alias='csv_cod')
    accounting_entry_code: Optional[str] = Field(
        None, validation_alias='tre_cod'
    )

    location_id: UUID
    material_id: UUID
    legal_guardian_id: UUID

    item_brand: Optional[str] = Field(None, validation_alias='ite_mar')
    item_model: Optional[str] = Field(None, validation_alias='ite_mod')

    group_type_code: Optional[str] = Field(None, validation_alias='tgr_cod')
    group_code: Optional[str] = Field(None, validation_alias='grp_cod')
    expense_element_code: Optional[str] = Field(
        None, validation_alias='ele_cod'
    )
    subelement_code: Optional[str] = Field(None, validation_alias='sbe_cod')

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


class AssetCode(BaseModel):
    asset_code: List[str]


class AssetCheckDigit(BaseModel):
    asset_check_digit: List[str]


class AtmNumber(BaseModel):
    atm_number: List[str]


class AssetIdentifier(BaseModel):
    asset_identifier: list[str]


class MaterialNameResponse(BaseModel):
    material_name: List[str]


class FilterAsset(BaseModel):
    limit: int = 100
    offset: int = 0
    q: Optional[str] = Field(
        default=None, description='Termo de busca (full-text search)'
    )

    asset_identifier: Optional[str] = Field(
        default=None,
        description='Filtrar por Asset Code + Check Digit (formato: código-dígito)',
    )
    atm_number: Optional[str] = Field(
        default=None, description='Filtrar por número do ATM'
    )

    agency_id: Optional[UUID] = Field(
        default=None, description='Filtrar por ID da Agência'
    )
    unit_id: Optional[UUID] = Field(
        default=None, description='Filtrar por ID da Unidade'
    )
    sector_id: Optional[UUID] = Field(
        default=None, description='Filtrar por ID do Setor'
    )
    location_id: Optional[UUID] = Field(
        default=None, description='Filtrar por ID da Sala'
    )
    material_id: Optional[UUID] = Field(
        default=None, description='Filtrar por ID do Material'
    )


class CatalogSchema(BaseModel):
    asset_id: UUID
    location_id: UUID
    situation: AssetSituation
    conservation_status: str
    description: Optional[str] = None


class CatalogWorkFlowSchema(BaseModel):
    workflow_status: WorkFlowStatus
    detail: Optional[dict] = None


class CatalogWorkFlowPublic(CatalogWorkFlowSchema):
    id: UUID
    user_id: UUID
    catalog_id: UUID
    model_config = ConfigDict(from_attributes=True)


class CatalogImagePublic(BaseModel):
    id: UUID
    catalog_id: UUID
    file_path: str
    model_config = ConfigDict(from_attributes=True)


class CatalogPublic(CatalogSchema):
    id: UUID
    asset_id: UUID = Field(exclude=True)
    user_id: UUID = Field(exclude=True)
    location_id: UUID = Field(exclude=True)

    asset: AssetPublic
    user: UserPublic
    location: LocationPublic
    images: list[CatalogImagePublic]
    workflow_history: list[CatalogWorkFlowPublic]

    model_config = ConfigDict(from_attributes=True)


class CatalogList(BaseModel):
    catalog_entries: list[CatalogPublic]


class FilterCatalog(FilterPage):
    q: str | None = Field(default=None)
    user_id: UUID | None = None
    workflow_status: Optional[WorkFlowStatus] = None


class InventorySchema(BaseModel):
    key: str
    model_config = ConfigDict(from_attributes=True)


class InventoryOwnerPublic(BaseModel):
    user_id: UUID = Field(exclude=True)
    user: UserPublic
    model_config = ConfigDict(from_attributes=True)


class InventoryPublic(InventorySchema):
    id: UUID
    created_by: UserPublic
    owners: list[InventoryOwnerPublic]
    model_config = ConfigDict(from_attributes=True)


class InventoryList(BaseModel):
    inventories: list[InventoryPublic]


class FilterInventory(FilterPage):
    q: str | None = Field(default=None)

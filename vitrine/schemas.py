from typing import List, Optional
from uuid import UUID

from pydantic import BaseModel, ConfigDict, EmailStr, Field

from vitrine.models import (
    AssetSituation,
    ConservationStatus,
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
    model_config = ConfigDict(from_attributes=True)


class UserDB(UserSchema):
    id: UUID


class UserList(BaseModel):
    users: list[UserPublic]


class FilterPage(BaseModel):
    offset: int = Field(0, ge=0)
    limit: int = Field(100, ge=1)


class AgencySchema(BaseModel):
    agency_name: str = Field(..., validation_alias='org_cod')
    agency_code: str = Field(..., validation_alias='org_nom')

    model_config = ConfigDict(from_attributes=True, populate_by_name=True)


class AgencyPublic(AgencySchema):
    id: UUID
    model_config = ConfigDict(from_attributes=True)


class AgencyList(BaseModel):
    agencies: list[AgencyPublic]


class FilterAgency(FilterPage):
    q: str | None = Field(default=None)


class UnitSchema(BaseModel):
    unit_name: str = Field(..., validation_alias='uge_cod')
    unit_code: str = Field(..., validation_alias='uge_nom')
    unit_siaf: str = Field(..., validation_alias='uge_siaf')

    model_config = ConfigDict(from_attributes=True, populate_by_name=True)


class UnitPublic(UnitSchema):
    id: UUID
    model_config = ConfigDict(from_attributes=True)


class UnitList(BaseModel):
    units: list[UnitPublic]


class FilterUnit(FilterPage):
    q: str | None = Field(default=None)


class SectorSchema(BaseModel):
    sector_name: str = Field(..., validation_alias='set_cod')
    sector_code: str = Field(..., validation_alias='set_nom')

    model_config = ConfigDict(from_attributes=True, populate_by_name=True)


class SectorPublic(SectorSchema):
    id: UUID
    model_config = ConfigDict(from_attributes=True)


class SectorList(BaseModel):
    sectors: list[SectorPublic]


class FilterSector(FilterPage):
    q: str | None = Field(default=None)


class LocationSchema(BaseModel):
    location_code: str = Field(..., validation_alias='loc_cod')
    location_name: str = Field(..., validation_alias='loc_nom')

    model_config = ConfigDict(from_attributes=True, populate_by_name=True)


class LocationPublic(LocationSchema):
    id: UUID
    model_config = ConfigDict(from_attributes=True)


class LocationList(BaseModel):
    locations: list[LocationPublic]


class FilterLocation(FilterPage):
    q: str | None = Field(default=None)


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

    agency_id: UUID
    unit_id: UUID
    sector_id: UUID
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

    agency: AgencyPublic
    unit: UnitPublic
    sector: SectorPublic
    location: LocationPublic
    material: MaterialPublic
    legal_guardian: LegalGuardianPublic

    agency_id: UUID = Field(exclude=True)
    unit_id: UUID = Field(exclude=True)
    sector_id: UUID = Field(exclude=True)
    location_id: UUID = Field(exclude=True)
    material_id: UUID = Field(exclude=True)
    legal_guardian_id: UUID = Field(exclude=True)

    is_official: bool
    model_config = ConfigDict(from_attributes=True)


class AssetList(BaseModel):
    assets: List[AssetPublic]


class CatalogSchema(BaseModel):
    asset_id: UUID
    situation: AssetSituation
    conservation_status: ConservationStatus
    description: Optional[str] = None


class CatalogWorkFlowSchema(BaseModel):
    workflow_status: WorkFlowStatus
    detail: Optional[dict] = None


class CatalogWorkFlowPublic(CatalogWorkFlowSchema):
    id: UUID
    user_id: UUID
    catalog_id: UUID

    model_config = ConfigDict(from_attributes=True)


class CatalogPublic(CatalogSchema):
    id: UUID

    asset_id: UUID = Field(exclude=True)
    user_id: UUID = Field(exclude=True)

    asset: AssetPublic
    user: UserPublic

    workflow_history: list[CatalogWorkFlowPublic] = []
    model_config = ConfigDict(from_attributes=True)


class CatalogList(BaseModel):
    catalog_entries: list[CatalogPublic]


class InventorySchema(BaseModel):
    location_id: UUID
    term: str

    model_config = ConfigDict(from_attributes=True)


class InventoryPublic(InventorySchema):
    id: UUID
    model_config = ConfigDict(from_attributes=True)


class InventoryList(BaseModel):
    inventories: list[InventoryPublic]

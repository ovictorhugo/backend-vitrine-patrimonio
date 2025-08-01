import factory
from faker import Faker

from vitrine.models import (
    Agency,
    Asset,
    Catalog,
    CatalogWorkFlow,
    Inventory,
    LegalGuardian,
    Location,
    Material,
    Sector,
    Unit,
    User,
    WorkFlowStatus,
)
from vitrine.schemas import AssetSituation, ConservationStatus

fake = Faker('pt_BR')


class LocationFactory(factory.Factory):
    class Meta:
        model = Location

    location_name = factory.LazyFunction(fake.street_address)
    location_code = factory.Sequence(lambda n: f'LOC-{n:04}')


class AgencyFactory(factory.Factory):
    class Meta:
        model = Agency

    agency_name = factory.LazyFunction(fake.company)
    agency_code = factory.Sequence(lambda n: f'AGY-{n:03}')


class UnitFactory(factory.Factory):
    class Meta:
        model = Unit

    unit_name = factory.LazyFunction(fake.bs)
    unit_code = factory.Sequence(lambda n: f'UNT-{n:04}')
    unit_siaf = factory.Sequence(lambda n: f'74{n:03}')


class SectorFactory(factory.Factory):
    class Meta:
        model = Sector

    sector_name = factory.LazyFunction(fake.job)
    sector_code = factory.Sequence(lambda n: f'SEC-{n:04}')


class LegalGuardiansFactory(factory.Factory):
    class Meta:
        model = LegalGuardian

    legal_guardians_name = factory.LazyFunction(fake.name)
    legal_guardians_code = factory.Sequence(lambda n: f'LG-{n:04}')


class MaterialFactory(factory.Factory):
    class Meta:
        model = Material

    material_name = factory.LazyFunction(fake.word)
    material_code = factory.Sequence(lambda n: f'MAT-{n:05}')


class UserFactory(factory.Factory):
    class Meta:
        model = User

    username = factory.Sequence(lambda n: f'test{n}')
    email = factory.LazyAttribute(lambda obj: f'{obj.username}@test.com')
    password = factory.LazyAttribute(lambda obj: f'{obj.username}@example.com')
    provider = 'LOCAL'


class AssetFactory(factory.Factory):
    class Meta:
        model = Asset

    asset_code = factory.Sequence(lambda n: f'PAT{2024 + n:04d}')
    asset_check_digit = factory.LazyAttribute(
        lambda obj: str(fake.random_digit_not_null())
    )

    atm_number = factory.Faker('ean', length=8)
    serial_number = factory.Faker('uuid4')
    asset_status = factory.Faker(
        'random_element',
        elements=('Ativo', 'Inativo', 'Manutenção', 'Baixado'),
    )
    asset_value = factory.LazyAttribute(
        lambda o: str(
            fake.pydecimal(left_digits=5, right_digits=2, positive=True)
        )
    )
    asset_description = factory.Faker('sentence', nb_words=6)
    csv_code = factory.Faker('pystr', max_chars=10)
    accounting_entry_code = factory.Faker(
        'pystr_format', string_format='CONT-######'
    )

    item_brand = factory.Faker('company_suffix')
    item_model = factory.Faker('word')

    group_type_code = factory.Faker('numerify', text='##')
    group_code = factory.Faker('numerify', text='####')
    expense_element_code = factory.Faker('numerify', text='########')
    subelement_code = factory.Faker('numerify', text='##')

    is_official = factory.Faker('boolean', chance_of_getting_true=25)


class CatalogFactory(factory.Factory):
    class Meta:
        model = Catalog

    situation = factory.Faker('random_element', elements=list(AssetSituation))
    conservation_status = factory.Faker(
        'random_element', elements=list(ConservationStatus)
    )
    description = factory.LazyFunction(fake.sentence)


class CatalogWorkFlowFactory(factory.Factory):
    class Meta:
        model = CatalogWorkFlow

    workflow_status = WorkFlowStatus.REVIEW_REQUESTED.value
    detail = factory.LazyFunction(
        lambda: {'reason': 'Awaiting approval from manager.'}
    )


class InventoryFactory(factory.Factory):
    class Meta:
        model = Inventory

    term = factory.LazyFunction(lambda: fake.date_object().strftime('%Y-%m'))

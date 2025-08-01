"""add_index_tsv

Revision ID: b99f20d7f9d8
Revises: de1bd630c1b1
Create Date: 2025-07-31 23:13:53.276543

"""
from typing import Sequence, Union
from sqlalchemy.dialects import postgresql
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'b99f20d7f9d8'
down_revision: Union[str, Sequence[str], None] = 'de1bd630c1b1'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


SEARCHABLE_COLUMNS = ['agency_name', 'agency_code']
TRIGGER_FUNCTION_NAME = 'agency_tsvector_update'
TRIGGER_NAME = 'tsvector_update_trigger'
TABLE_NAME = 'agencys'


def create_trigger_function_sql() -> str:
    columns_to_vector = " || ' ' || ".join([f"lower(unaccent(coalesce(NEW.{col}, '')))" for col in SEARCHABLE_COLUMNS])
    return f"""
        CREATE OR REPLACE FUNCTION {TRIGGER_FUNCTION_NAME}() RETURNS trigger AS $$
        BEGIN
            NEW.tsv = to_tsvector('portuguese', {columns_to_vector});
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;
    """

def create_trigger_sql() -> str:
    return f"""
        CREATE TRIGGER {TRIGGER_NAME}
        BEFORE INSERT OR UPDATE ON {TABLE_NAME}
        FOR EACH ROW EXECUTE PROCEDURE {TRIGGER_FUNCTION_NAME}();
    """

def drop_trigger_sql() -> str:
    return f"DROP TRIGGER IF EXISTS {TRIGGER_NAME} ON {TABLE_NAME};"

def drop_trigger_function_sql() -> str:
    return f"DROP FUNCTION IF EXISTS {TRIGGER_FUNCTION_NAME}();"


def upgrade() -> None:
    op.execute("CREATE EXTENSION IF NOT EXISTS unaccent;")

    op.create_index(
        'ix_agencys_tsv',
        TABLE_NAME,
        ['tsv'],
        unique=False,
        postgresql_using='gin'
    )
    
    op.execute(create_trigger_function_sql())
    op.execute(create_trigger_sql())

    columns_to_vector_update = " || ' ' || ".join([f"lower(unaccent(coalesce({col}, '')))" for col in SEARCHABLE_COLUMNS])
    op.execute(
        f"UPDATE {TABLE_NAME} SET tsv = to_tsvector('portuguese', {columns_to_vector_update});"
    )

def downgrade() -> None:
    op.execute(drop_trigger_sql())
    op.execute(drop_trigger_function_sql())
    op.drop_index('ix_agencys_tsv', table_name=TABLE_NAME, postgresql_using='gin')
    op.execute("DROP EXTENSION IF EXISTS unaccent;")
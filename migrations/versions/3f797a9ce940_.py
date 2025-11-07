from typing import Sequence, Union
from alembic import op, context
import sqlalchemy as sa

revision: str = '3f797a9ce940'
down_revision: Union[str, Sequence[str], None] = '95e12b0fcb3b'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

def upgrade() -> None:
    op.add_column('assets', sa.Column('user_id', sa.Uuid(), nullable=True))
    op.create_foreign_key(None, 'assets', 'users', ['user_id'], ['id'])

    if not context.is_offline_mode():
        conn = op.get_bind()
        assets_count = conn.execute(sa.text("SELECT COUNT(*) FROM assets")).scalar()
        if assets_count > 0:
            default_user_id = conn.execute(sa.text("SELECT id FROM users LIMIT 1")).scalar()
            if default_user_id:
                conn.execute(
                    sa.text("UPDATE assets SET user_id = :uid WHERE user_id IS NULL"),
                    {"uid": default_user_id},
                )
            else:
                raise Exception("Nenhum usuário encontrado para preencher user_id em assets")

    op.alter_column('assets', 'user_id', nullable=False)
    op.add_column('inventory', sa.Column('avaliable', sa.Boolean(), nullable=True))
    op.drop_column('inventory', 'available')
    op.drop_column('users', 'photo_url')
    op.drop_column('users', 'background_url')

def downgrade() -> None:
    op.add_column('users', sa.Column('background_url', sa.VARCHAR(), autoincrement=False, nullable=True))
    op.add_column('users', sa.Column('photo_url', sa.VARCHAR(), autoincrement=False, nullable=True))
    op.add_column('inventory', sa.Column('available', sa.BOOLEAN(), autoincrement=False, nullable=False))
    op.drop_column('inventory', 'avaliable')
    op.drop_constraint(None, 'assets', type_='foreignkey')
    op.drop_column('assets', 'user_id')

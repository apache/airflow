"""create is_encrypted

Revision ID: 1507a7289a2f
Revises: e3a246e0dc1
Create Date: 2015-08-18 18:57:51.927315

"""

# revision identifiers, used by Alembic.
revision = '1507a7289a2f'
down_revision = 'e3a246e0dc1'
branch_labels = None
depends_on = None

from alembic import op
import sqlalchemy as sa


connectionhelper = sa.Table(
    'connection',
    sa.MetaData(),
    sa.Column('id', sa.Integer, primary_key=True),
    sa.Column('is_encrypted')
)


def upgrade():
    op.add_column('connection',
                  sa.Column('is_encrypted', sa.Boolean,
                            unique=False, default=False))

    conn = op.get_bind()
    conn.execute(
        connectionhelper.update().values(is_encrypted=False)
    )


def downgrade():
    op.drop_column('connection', 'is_encrypted')

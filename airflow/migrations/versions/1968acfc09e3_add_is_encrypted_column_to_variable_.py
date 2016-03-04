"""add is_encrypted column to variable table

Revision ID: 1968acfc09e3
Revises: bba5a7cfc896
Create Date: 2016-02-02 17:20:55.692295

"""

# revision identifiers, used by Alembic.
revision = '1968acfc09e3'
down_revision = 'bba5a7cfc896'
branch_labels = None
depends_on = None

from alembic import op
import sqlalchemy as sa


def upgrade():
    op.add_column('variable', sa.Column('is_encrypted', sa.Boolean,default=False))


def downgrade():
    op.drop_column('variable', 'is_encrypted')

"""Add a column to track the encryption state of the 'Extra' field in connection

Revision ID: bba5a7cfc896
Revises: bbc73705a13e
Create Date: 2016-01-29 15:10:32.656425

"""

# revision identifiers, used by Alembic.
revision = 'bba5a7cfc896'
down_revision = 'bbc73705a13e'
branch_labels = None
depends_on = None

from alembic import op
import sqlalchemy as sa


def upgrade():
    op.add_column('connection', sa.Column('is_extra_encrypted', sa.Boolean,default=False))


def downgrade():
    op.drop_column('connection', 'is_extra_encrypted')

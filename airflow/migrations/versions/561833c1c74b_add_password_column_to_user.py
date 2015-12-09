"""add password column to user

Revision ID: 561833c1c74b
Revises: 40e67319e3a9
Create Date: 2015-11-30 06:51:25.872557

"""

# revision identifiers, used by Alembic.
revision = '561833c1c74b'
down_revision = '40e67319e3a9'
branch_labels = None
depends_on = None

from alembic import op
import sqlalchemy as sa


def upgrade():
    op.add_column('user', sa.Column('password', sa.String(255)))


def downgrade():
    op.drop_column('user', 'password')

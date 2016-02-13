"""Adding extra to Log

Revision ID: 502898887f84
Revises: 52d714495f0
Create Date: 2015-11-03 22:50:49.794097

"""

# revision identifiers, used by Alembic.
revision = '502898887f84'
down_revision = '52d714495f0'
branch_labels = None
depends_on = None

from alembic import op
import sqlalchemy as sa


def upgrade():
    op.add_column('log', sa.Column('extra', sa.Text(), nullable=True))


def downgrade():
    op.drop_column('log', 'extra')

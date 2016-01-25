"""dagrun_config

Revision ID: 40e67319e3a9
Revises: 2e541a1dcfed
Create Date: 2015-10-29 08:36:31.726728

"""

# revision identifiers, used by Alembic.
revision = '40e67319e3a9'
down_revision = '2e541a1dcfed'
branch_labels = None
depends_on = None

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import mysql


def upgrade():
    op.add_column('dag_run', sa.Column('conf', sa.PickleType(), nullable=True))


def downgrade():
    op.drop_column('dag_run', 'conf')

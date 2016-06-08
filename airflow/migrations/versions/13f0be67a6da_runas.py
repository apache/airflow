"""runas

Revision ID: 13f0be67a6da
Revises: 2e82aab8ef20
Create Date: 2016-06-07 22:34:09.923825

"""

# revision identifiers, used by Alembic.
revision = '13f0be67a6da'
down_revision = '2e82aab8ef20'
branch_labels = None
depends_on = None

from alembic import op
import sqlalchemy as sa


def upgrade():
    op.add_column('task_instance', sa.Column('run_as_user', sa.String(length=1000), nullable=True))


def downgrade():
    op.drop_column('task_instance', 'run_as_user')

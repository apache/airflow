"""add TI state index

Revision ID: 211e584da130
Revises: 2e82aab8ef20
Create Date: 2016-06-30 10:54:24.323588

"""

# revision identifiers, used by Alembic.
revision = '211e584da130'
down_revision = '2e82aab8ef20'
branch_labels = None
depends_on = None

from alembic import op
import sqlalchemy as sa


def upgrade():
    op.create_index('ti_state', 'task_instance', ['state'], unique=False)

def downgrade():
    op.drop_index('ti_state', table_name='task_instance')

"""Add notification_sent column to sla_miss

Revision ID: bbc73705a13e
Revises: 4446e08588
Create Date: 2016-01-14 18:05:54.871682

"""

# revision identifiers, used by Alembic.
revision = 'bbc73705a13e'
down_revision = '4446e08588'
branch_labels = None
depends_on = None

from alembic import op
import sqlalchemy as sa


def upgrade():
    op.add_column('sla_miss', sa.Column('notification_sent', sa.Boolean,default=False))


def downgrade():
    op.drop_column('sla_miss', 'notification_sent')

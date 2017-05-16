"""create cluster status table

Revision ID: c731a94965e9
Revises: 1968acfc09e3
Create Date: 2016-03-23 00:06:33.922982

"""

# revision identifiers, used by Alembic.
revision = 'c731a94965e9'
down_revision = '1968acfc09e3'
branch_labels = None
depends_on = None

from alembic import op
import sqlalchemy as sa
from sqlalchemy.engine.reflection import Inspector

from airflow import settings


def upgrade():
    inspector = Inspector.from_engine(settings.engine)
    tables = inspector.get_table_names()
    if 'cluster_status' not in tables:
        op.create_table(
            'cluster_status',
            sa.Column('service_id', sa.Integer(), nullable=False),
            sa.Column('service_type', sa.String(length=50), nullable=True),
            sa.Column('hostname', sa.String(length=500), nullable=True),
            sa.Column('listen_port', sa.Integer(), nullable=True),
            sa.Column('last_heartbeat_at_epoch_second', sa.BIGINT(), nullable=True),
            sa.Column('last_restarted_at_epoch_second', sa.BIGINT(), nullable=True),
            sa.Column('created_at_epoch_second', sa.BIGINT(), nullable=True),
            sa.PrimaryKeyConstraint('service_id'),
            sa.UniqueConstraint('service_type', 'hostname', 'listen_port')
        )


def downgrade():
    op.drop_table('cluster_status')

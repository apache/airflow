"""job_id indices

Revision ID: 52d714495f0
Revises: 338e90f54d61
Create Date: 2015-10-20 03:17:01.962542

"""

# revision identifiers, used by Alembic.
revision = '52d714495f0'
down_revision = '338e90f54d61'
branch_labels = None
depends_on = None

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import mysql


def upgrade():
    op.create_index('idx_job_state_heartbeat', 'job', ['state', 'latest_heartbeat'], unique=False)


def downgrade():
    op.drop_index('idx_job_state_heartbeat', table_name='job')

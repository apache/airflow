"""add DagRun

Revision ID: 19054f4ff36
Revises: 338e90f54d61
Create Date: 2015-10-12 09:55:52.475712

"""

# revision identifiers, used by Alembic.
revision = '19054f4ff36'
down_revision = '338e90f54d61'
branch_labels = None
depends_on = None

from alembic import op
import sqlalchemy as sa


def upgrade():
    op.create_table(
        'dag_run',
        sa.Column('dag_id', sa.String(length=250), nullable=False),
        sa.Column('execution_date', sa.DateTime(), nullable=False),
        sa.Column('run_id', sa.String(length=250), nullable=False),
        sa.PrimaryKeyConstraint('dag_id', 'execution_date')
    )


def downgrade():
    op.drop_table('dag_run')

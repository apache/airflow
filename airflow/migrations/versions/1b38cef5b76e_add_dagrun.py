"""add dagrun

Revision ID: 1b38cef5b76e
Revises: 52d714495f0
Create Date: 2015-10-27 08:31:48.475140

"""

# revision identifiers, used by Alembic.
revision = '1b38cef5b76e'
down_revision = '502898887f84'
branch_labels = None
depends_on = None

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import mysql


def upgrade():
    op.create_table('dag_run',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('dag_id', sa.String(length=250), nullable=True),
        sa.Column('execution_date', sa.DateTime(), nullable=True),
        sa.Column('state', sa.String(length=50), nullable=True),
        sa.Column('run_id', sa.String(length=250), nullable=True),
        sa.Column('external_trigger', sa.Boolean(), nullable=True),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('dag_id', 'execution_date'),
        sa.UniqueConstraint('dag_id', 'run_id'),
    )


def downgrade():
    op.drop_table('dag_run')

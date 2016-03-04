"""dagrun start end

Revision ID: 4446e08588
Revises: 561833c1c74b
Create Date: 2015-12-10 11:26:18.439223

"""

# revision identifiers, used by Alembic.
revision = '4446e08588'
down_revision = '561833c1c74b'
branch_labels = None
depends_on = None

from alembic import op
import sqlalchemy as sa

def upgrade():
    op.add_column('dag_run', sa.Column('end_date', sa.DateTime(), nullable=True))
    op.add_column('dag_run', sa.Column('start_date', sa.DateTime(), nullable=True))


def downgrade():
    op.drop_column('dag_run', 'start_date')
    op.drop_column('dag_run', 'end_date')

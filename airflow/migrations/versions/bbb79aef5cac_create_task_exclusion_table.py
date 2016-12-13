#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""create task_exclusion table

Revision ID: bbb79aef5cac
Revises: f2ca10b85618
Create Date: 2016-11-18 13:38:34.653202

"""

# revision identifiers, used by Alembic.
revision = 'bbb79aef5cac'
down_revision = 'f2ca10b85618'
branch_labels = None
depends_on = None

from alembic import op, context
import sqlalchemy as sa
from sqlalchemy.dialects import mysql


def upgrade():
    if context.config.get_main_option('sqlalchemy.url').startswith('mysql'):
        op.create_table(
            'task_exclusion',
            sa.Column('id', sa.Integer(), nullable=False),
            sa.Column('dag_id', sa.String(length=250), nullable=False),
            sa.Column('task_id', sa.String(length=250), nullable=False),
            sa.Column('exclusion_type', sa.String(length=32), nullable=False),
            sa.Column('exclusion_start_date', mysql.DATETIME(fsp=6),
                      nullable=False),
            sa.Column('exclusion_end_date', mysql.DATETIME(fsp=6),
                      nullable=False),
            sa.Column('created_by', sa.String(length=256), nullable=False),
            sa.Column('created_on', mysql.DATETIME(fsp=6), nullable=False),
            sa.PrimaryKeyConstraint('id'))
    else:
        op.create_table(
            'task_exclusion',
            sa.Column('id', sa.Integer(), nullable=False),
            sa.Column('dag_id', sa.String(length=250), nullable=False),
            sa.Column('task_id', sa.String(length=250), nullable=False),
            sa.Column('exclusion_type', sa.String(length=32), nullable=False),
            sa.Column('exclusion_start_date', sa.DateTime(), nullable=False),
            sa.Column('exclusion_end_date', sa.DateTime(), nullable=False),
            sa.Column('created_by', sa.String(length=256), nullable=False),
            sa.Column('created_on', sa.DateTime(), nullable=False),
            sa.PrimaryKeyConstraint('id'))

def downgrade():
    op.drop_table('task_exclusion')

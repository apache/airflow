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

"""Add task_instance.dag_version column

Revision ID: 785ece547642
Revises: 211e584da130
Create Date: 2016-07-14 15:38:39.571101

"""

# revision identifiers, used by Alembic.
revision = '785ece547642'
down_revision = '211e584da130'
branch_labels = None
depends_on = None

from alembic import op
import sqlalchemy as sa


def upgrade():
    op.add_column('task_instance', sa.Column('dag_version', sa.String(1000)))


def downgrade():
    op.drop_column('task_instance', 'dag_version')

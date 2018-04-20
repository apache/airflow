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

"""add uuid field to task instance

Revision ID: d02246ccf39b
Revises: 0e2a74e0fc9f
Create Date: 2018-04-09 23:12:29.317366

"""

# revision identifiers, used by Alembic.
revision = 'd02246ccf39b'
down_revision = '0e2a74e0fc9f'
branch_labels = None
depends_on = None

from alembic import op
import sqlalchemy as sa


def upgrade():
    op.add_column('task_instance', sa.Column('uuid', sa.String(length=250), nullable=True))
    op.create_index('ti_uuid', 'task_instance', ['uuid'], unique=False)


def downgrade():
    op.drop_column('task_instance', 'uuid')
    op.drop_index('ti_uuid', table_name='task_instance')

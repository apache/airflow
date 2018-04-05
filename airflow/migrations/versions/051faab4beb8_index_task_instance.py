# flake8: noqa
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

"""index task instance

Revision ID: 051faab4beb8
Revises: 0e2a74e0fc9f
Create Date: 2018-03-26 22:42:07.351366

"""

# revision identifiers, used by Alembic.
revision = '051faab4beb8'
down_revision = '0e2a74e0fc9f'
branch_labels = None
depends_on = None

from alembic import op
import sqlalchemy as sa


def upgrade():
    op.create_index('ti_job_id', 'task_instance', ['job_id'], unique=False)


def downgrade():
    op.drop_index('ti_job_id', table_name='task_instance')
